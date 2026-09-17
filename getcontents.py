"""Filter locally stored Pushshift Reddit dumps (RC_*.zst / RS_*.zst).

The dumps are newline-delimited JSON compressed with zstd (long window). A month
of comments is ~250 GB of JSON, so the work done per record has to be tiny:

  1. decompress in chunks of a few MB in a background thread (zstandard
     releases the GIL while decompressing, so it overlaps with the scanning
     below);
  2. scan each chunk as raw bytes to find the records that can possibly match:
     for `match_mode="exact"` (e.g. `filter_type="subreddit"`) every
     `"subreddit":"..."` value in the chunk is located with `bytes.find` and
     looked up in a set; for `match_mode="contains"` the chunk is lower-cased
     once and searched for every keyword with `bytes.find`. Both run at C speed
     and may give false positives but never false negatives;
  3. JSON-parse (orjson when available) only the candidate records, verify the
     match on the parsed value and keep the requested attributes.

The original implementation parsed every record with `json.loads` and appended
matches through `exec()`, which is where all the time went. Files are
independent, so a folder of dumps can be processed by several worker processes
(`filter_files(..., workers=N)`).
"""
import gc
import glob
import json
import os
import queue
import re
import shlex
import string
import subprocess
import sys
import threading
import time
from multiprocessing import Pool

import pandas as pd
import zstandard

from utilities import get_filename, get_stringname, single_df_to_file

try:
    import orjson
    _json_loads = orjson.loads
    _json_dumps = orjson.dumps
except ImportError:  # optional, but several times faster than the stdlib
    _json_loads = json.loads

    def _json_dumps(obj):
        return json.dumps(obj, ensure_ascii=False).encode("utf-8")

# Attributes holding free text. Filters on these default to substring matching
# and, on the command line, underscores in the filter values are read as spaces.
TEXT_FIELDS = {"body", "title", "selftext"}

DEFAULT_ATTRIBUTES = {
    "comment": ["id", "subreddit", "body"],
    "submission": ["id", "subreddit", "title"],
}

CHUNK_SIZE = 1 << 22  # target size of the decompressed chunks handed to the scanner
READ_SIZE = 1 << 19  # bytes of compressed data read from the file at a time
SWITCH_INTERVAL = 0.00005  # interpreter thread switch interval while a file is read (see _iter_chunks_threaded)
PROGRESS_EVERY = 30  # seconds between progress lines
SCHEMA_SAMPLE = 1000  # records inspected up front to validate the requested attributes
SAVE_TYPES = ("xlsx", "csv", "pickle", "jsonl")

# Matching ignores the case of ASCII letters only, on both the raw bytes and the
# parsed strings, so the byte-level pre-filter and the verification agree exactly.
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)
_LOWER_TABLE = bytes(range(256)).lower()


def detect_reddit_type(file_path):
    """'comment' for RC_* files, 'submission' for RS_* files."""
    name = os.path.basename(file_path)
    if "RC" in name:
        return "comment"
    if "RS" in name:
        return "submission"
    raise ValueError(
        f"Cannot tell whether {file_path} holds comments or submissions: the file name "
        "should contain 'RC' (comments) or 'RS' (submissions), e.g. RC_2022-10.zst."
    )


# scp-style path of a file on another machine: [user@]host:/absolute/path
_REMOTE_PATH = re.compile(r"^((?:[\w.-]+@)?[\w.-]+):(?!//)(/.*)$")


def split_remote(path):
    """('[user@]host', '/remote/path') for a path like user@server:/data/RC_2022-10.zst, else None."""
    match = _REMOTE_PATH.match(path)
    return (match.group(1), match.group(2)) if match else None


class _RemoteFile:
    """Read-only stream of a file on another machine, through `ssh host cat path`.

    Nothing is installed or written on the other machine; the compressed bytes
    are decompressed and filtered here. Use SSH keys or a ControlMaster
    connection so that no password is asked for every file.
    """

    def __init__(self, host, path):
        self.name = f"{host}:{path}"
        self._eof = False
        self._proc = subprocess.Popen(
            ["ssh", host, "cat " + shlex.quote(path)],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

    def read(self, n):
        data = self._proc.stdout.read(n)
        if not data:
            self._eof = True
        return data

    def close(self):
        proc = self._proc
        if not self._eof:  # the reader stopped early: just stop the transfer
            proc.kill()
        proc.stdout.close()
        error = proc.stderr.read().decode("utf-8", "replace").strip()
        proc.stderr.close()
        code = proc.wait()
        if self._eof and code != 0:
            raise IOError(f"Cannot read {self.name} over ssh (exit code {code}): {error or 'no error message'}")


def resolve_input_files(input_path):
    """Expand a .zst file, a folder holding .zst files, or a glob pattern into a sorted file list.

    The path may be on another machine reachable with ssh: user@server:/data/RC_2022-10.zst,
    user@server:/data/dumps/ or 'user@server:/data/RC_2022-*.zst'. A local .txt file lists
    inputs of any of these kinds, one per line (empty lines and lines starting with # are skipped).
    """
    if input_path.endswith(".txt") and os.path.isfile(input_path):
        files = []
        with open(input_path, encoding="utf-8") as fh:
            for line in fh:
                entry = line.strip()
                if entry and not entry.startswith("#"):
                    files.extend(f for f in resolve_input_files(entry) if f not in files)
        if not files:
            raise ValueError(f"The list {input_path} does not name any .zst file.")
        return files
    remote = split_remote(input_path)
    if remote:
        host, path = remote
        if any(ch in path for ch in "*?["):
            command = "ls -1d " + path  # the remote shell expands the pattern
        elif path.endswith(".zst"):
            return [input_path]
        else:
            command = "ls -1d " + shlex.quote(path.rstrip("/")) + "/*.zst"
        result = subprocess.run(["ssh", host, command], stdin=subprocess.DEVNULL, capture_output=True, text=True)
        files = sorted(line.strip() for line in result.stdout.splitlines() if line.strip().endswith(".zst"))
        if result.returncode != 0 or not files:
            raise ValueError(f"No .zst file found for {input_path} on {host}: {result.stderr.strip() or 'nothing listed'}")
        return [f"{host}:{f}" for f in files]
    if os.path.isdir(input_path):
        files = sorted(glob.glob(os.path.join(input_path, "*.zst")))
        if not files:
            raise ValueError(f"No .zst file found in the folder {input_path}.")
    elif any(ch in input_path for ch in "*?["):
        files = sorted(glob.glob(input_path))
        if not files:
            raise ValueError(f"No file matches the pattern {input_path}.")
    else:
        if not os.path.isfile(input_path):
            raise ValueError(f"The input file {input_path} does not exist.")
        files = [input_path]
    return files


def _raw_forms(text):
    """Byte strings a JSON serializer may have written for `text` inside a string value.

    Covers raw UTF-8 and \\uXXXX-escaped output, with or without escaped slashes,
    so the byte-level pre-filter never misses a record that really contains `text`.
    """
    forms = set()
    for ensure_ascii in (False, True):
        form = json.dumps(text, ensure_ascii=ensure_ascii)[1:-1]
        forms.add(form)
        if "/" in form:
            forms.add(form.replace("/", "\\/"))
    return [form.encode("utf-8") for form in forms]


def _quoted_value(buf, i, hi):
    """Raw bytes of the JSON string starting at buf[i:hi] (after optional whitespace), or None."""
    while i < hi and buf[i] in b" \t\r":
        i += 1
    if i >= hi or buf[i] != 34:  # not a '"'
        return None
    start = i + 1
    j = buf.find(b'"', start, hi)
    while j >= 0:
        # a quote preceded by an odd number of backslashes is escaped
        k = j - 1
        while k >= start and buf[k] == 92:
            k -= 1
        if (j - 1 - k) % 2 == 0:
            return buf[start:j]
        j = buf.find(b'"', j + 1, hi)
    return None


class _Matcher:
    """Assigns records to filters.

    `candidates(buf, lo, hi)` scans the complete lines held in buf[lo:hi] and
    returns the (start, end) spans of the lines that may match. `match(obj)` is
    run on the parsed candidates only and returns the index of the first filter
    (in the order of `filter_list`) the record satisfies, or -1.
    """

    def __init__(self, filter_list, field, mode):
        if mode not in ("exact", "contains"):
            raise ValueError("Wrong match_mode. Only 'exact' or 'contains' are allowed.")
        self.field = field
        self.exact = mode == "exact"
        self._keywords = [flt.translate(_ASCII_LOWER) for flt in filter_list]
        self._index = {}
        for i, flt in enumerate(self._keywords):
            self._index.setdefault(flt, i)
        raw = sorted({form.lower() for flt in filter_list for form in _raw_forms(flt)})
        if self.exact:
            self._key = f'"{field}":'.encode("utf-8")
            self._raw_targets = set(raw)
            self.candidates = self._candidates_exact
        else:
            self._forms = raw
            self.candidates = self._candidates_contains

    def _candidates_exact(self, buf, lo, hi):
        # Every occurrence of the key is checked (nested objects such as crosspost
        # parents can carry the same key), so a real match is never skipped.
        key = self._key
        klen = len(key)
        targets = self._raw_targets
        find = buf.find
        spans = []
        pos = find(key, lo, hi)
        while pos >= 0:
            i = pos + klen
            if i < hi and buf[i] == 34:  # the common case: "key":"value" with no escapes
                j = find(b'"', i + 1, hi)
                if j > 0 and buf[j - 1] != 92:
                    value = buf[i + 1:j]
                    nxt = j + 1
                else:
                    value = _quoted_value(buf, i, hi)
                    nxt = i
            else:
                value = _quoted_value(buf, i, hi)
                nxt = i
            if value is not None and value.lower() in targets:
                start = buf.rfind(b"\n", lo, pos)
                start = lo if start < 0 else start + 1
                end = find(b"\n", pos, hi)
                end = hi if end < 0 else end
                spans.append((start, end))
                nxt = end  # the rest of this line needs no further look
            pos = find(key, nxt, hi)
        return spans

    def _candidates_contains(self, buf, lo, hi):
        low = buf.translate(_LOWER_TABLE)
        find = low.find
        rfind = low.rfind
        spans = {}
        for form in self._forms:
            pos = find(form, lo, hi)
            while pos >= 0:
                start = rfind(b"\n", lo, pos)
                start = lo if start < 0 else start + 1
                end = find(b"\n", pos, hi)
                end = hi if end < 0 else end
                spans[start] = end
                pos = find(form, end, hi)
        return sorted(spans.items())

    def match(self, obj):
        value = obj.get(self.field)
        if not isinstance(value, str):
            return -1
        value = value.translate(_ASCII_LOWER)
        if self.exact:
            return self._index.get(value, -1)
        for i, keyword in enumerate(self._keywords):
            if keyword in value:
                return i
        return -1


def _iter_chunks(file_path, chunk_size):
    """Yield the decompressed content of a zstd file in chunks of roughly `chunk_size` bytes.

    A file may hold several frames one after the other (newer dumps do), so a
    new decompressor is started on the bytes left after each frame. A file that
    ends in the middle of a frame (an unfinished download) raises instead of
    silently returning a part of the data.
    """
    dctx = zstandard.ZstdDecompressor(max_window_size=2 ** 31)
    remote = split_remote(file_path)
    source = _RemoteFile(*remote) if remote else open(file_path, "rb")
    try:
        dobj = dctx.decompressobj(write_size=chunk_size)
        in_frame = False
        while True:
            data = source.read(READ_SIZE)
            if not data:
                break
            while data:
                out = dobj.decompress(data)
                in_frame = True
                if out:
                    yield out
                if not dobj.eof:
                    break
                data = dobj.unused_data  # start of the next frame, if any
                dobj = dctx.decompressobj(write_size=chunk_size)
                in_frame = False
    finally:
        source.close()  # for a remote file this also reports a failed transfer
    if in_frame:
        raise ValueError(f"{file_path} is truncated or corrupt: the compressed data ends in the middle of a zstd frame.")


def _iter_chunks_threaded(file_path, chunk_size, depth=4):
    """Like `_iter_chunks`, but decompresses in a background thread.

    zstandard releases the GIL while decompressing, but the reader thread needs
    it back after every call and by default waits up to 5 ms (the interpreter's
    switch interval) for the scanning thread to hand it over, which serializes
    the two. A much shorter interval lets decompression and scanning overlap
    almost completely; it is restored when the reader finishes.
    """
    q = queue.Queue(maxsize=depth)
    stop = threading.Event()
    done = object()
    switch_interval = sys.getswitchinterval()
    sys.setswitchinterval(SWITCH_INTERVAL)

    def put(item):
        while not stop.is_set():
            try:
                q.put(item, timeout=0.5)
                return True
            except queue.Full:
                pass
        return False  # the consumer went away

    def producer():
        try:
            for chunk in _iter_chunks(file_path, chunk_size):
                if not put(chunk):
                    return
        except BaseException as exc:  # re-raised in the consumer
            put(exc)
            return
        put(done)

    thread = threading.Thread(target=producer, name="zstd-reader", daemon=True)
    thread.start()
    try:
        while True:
            item = q.get()
            if item is done:
                return
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        stop.set()
        thread.join()
        sys.setswitchinterval(switch_interval)


def iter_line_buffers(file_path, chunk_size=CHUNK_SIZE, threaded=True):
    """Yield (buf, lo, hi) such that buf[lo:hi] holds only complete lines of the decompressed file.

    Lines cut by a chunk boundary are carried over and yielded whole with the
    next chunk, so a record is never seen partially or twice.
    """
    chunks = _iter_chunks_threaded(file_path, chunk_size) if threaded else _iter_chunks(file_path, chunk_size)
    carry = b""
    for chunk in chunks:
        first = chunk.find(b"\n")
        if first < 0:
            carry += chunk
            continue
        lo = 0
        if carry:
            head = carry + chunk[:first]
            yield head, 0, len(head)
            lo = first + 1
        last = chunk.rfind(b"\n")
        if last > lo:
            yield chunk, lo, last
        carry = chunk[last + 1:]
    if carry:
        yield carry, 0, len(carry)


def _normalize_filters(filter_list, filter_type):
    filters = []
    for flt in filter_list:
        if flt is None or (isinstance(flt, float) and flt != flt):  # NaN from a spreadsheet
            continue
        flt = str(flt).strip()
        if filter_type == "subreddit" and flt[:2].lower() == "r/":
            flt = flt[2:]
        if flt:
            filters.append(flt)
    return filters


def _sanitize_name(flt):
    """Filter value as used in the output file name (same scheme as the original code)."""
    name = str(flt)
    for old, new in ((".", "_DOT_"), (",", "_COMA_"), (":", "_COLON_"), (";", "_SEMICOLON_")):
        name = name.replace(old, new)
    return name.replace("/", "_").replace("\\", "_")


def _check_schema(lines, filter_type, attribute, file_path):
    """Fail fast on a mistyped filter attribute instead of after a long scan.

    Several records are inspected because not every record carries every
    attribute (e.g. promoted posts in submission dumps have no 'subreddit').
    """
    seen = set()
    for line in lines:
        try:
            obj = _json_loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            seen.update(obj)
    if not seen:
        return
    if filter_type is not None and filter_type not in seen:
        raise ValueError(
            f"The attribute '{filter_type}' (filter_type) does not exist in the first {len(lines)} records of "
            f"{file_path}. Available attributes: {', '.join(sorted(seen))}."
        )
    missing = [att for att in attribute if att not in seen]
    if missing:
        print(
            f"Warning: the attribute(s) {', '.join(missing)} are not present in the first {len(lines)} records "
            f"of {file_path}; they will be empty (None) where missing.", flush=True
        )


def _save_jsonl(records, encode, saved_file_name):
    with open(saved_file_name, "wb") as fh:
        for record in records:
            fh.write(encode(record))
            fh.write(b"\n")


def get_contents(reddit_type, file_path, filter_list, filter_type, attribute, add_detail,
                 output_path, save_type, return_type="merged_df", match_mode=None, verbose=True):
    """Filter one dump file.

    reddit_type: 'comment', 'submission', or None to detect it from the file name.
    filter_list: values to filter on; [] keeps every record.
    filter_type: the attribute the filter is applied to (e.g. 'subreddit', 'body').
    attribute:   attributes to keep; None for the defaults, [] or ['all'] for all of them.
    match_mode:  'exact' or 'contains'; None picks 'contains' for text attributes and 'exact' otherwise.
    return_type: 'save_file' writes one file per filter to output_path and returns a summary dict;
                 'merged_df' returns the list of DataFrames (one per filter) instead.
    """
    if return_type not in ("save_file", "merged_df"):
        raise ValueError("Wrong 'return_type' parameter. Only 'save_file' or 'merged_df' are allowed.")
    if return_type == "save_file":
        # Checked before the scan so that a wrong option does not surface after a long run
        if save_type not in SAVE_TYPES:
            raise ValueError("Wrong save_type. Only 'xlsx', 'csv', 'pickle', or 'jsonl' are allowed to be used as save_type.")
        if output_path:
            os.makedirs(output_path, exist_ok=True)
    if reddit_type is None:
        reddit_type = detect_reddit_type(file_path)
    if attribute is None:
        attribute = DEFAULT_ATTRIBUTES[reddit_type]
    elif attribute == "all" or list(attribute) == ["all"]:
        attribute = []
    attribute = list(attribute)
    keep_all = attribute == []
    filters = _normalize_filters(filter_list, filter_type)
    if match_mode is None:
        match_mode = "contains" if filter_type in TEXT_FIELDS else "exact"
    matcher = _Matcher(filters, filter_type, match_mode) if filters else None
    buckets = [[] for _ in filters] or [[]]
    name = os.path.basename(file_path)
    # With jsonl output and all attributes the raw lines of the dump are kept and written back
    # untouched (also lighter in memory than parsed records); otherwise records are parsed.
    to_jsonl = return_type == "save_file" and save_type == "jsonl"
    raw_lines = to_jsonl and keep_all and add_detail != "yes"

    if verbose:
        if filters:
            print(f"[{name}] filtering {len(filters)} {filter_type} value(s), match_mode={match_mode} ...", flush=True)
        else:
            print(f"[{name}] collecting all records ...", flush=True)

    loads = _json_loads
    t0 = last_report = time.monotonic()
    n_bytes = n_matched = 0
    first = True
    # The kept records are plain lists/dicts that never form cycles; with millions
    # of them the cyclic garbage collector would otherwise rescan them repeatedly.
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        for buf, lo, hi in iter_line_buffers(file_path):
            if first:
                sample = buf[lo:hi].split(b"\n", SCHEMA_SAMPLE)[:SCHEMA_SAMPLE]
                _check_schema(sample, filter_type if filters else None, attribute, file_path)
                first = False
            n_bytes += hi - lo
            if matcher is None:
                bucket = buckets[0]
                for line in buf[lo:hi].split(b"\n"):
                    if not line:
                        continue
                    if raw_lines:
                        bucket.append(line)
                        continue
                    obj = loads(line)
                    bucket.append(obj if keep_all else [obj.get(att) for att in attribute])
            else:
                match = matcher.match
                for start, end in matcher.candidates(buf, lo, hi):
                    line = buf[start:end]
                    obj = loads(line)
                    i = match(obj)
                    if i >= 0:
                        buckets[i].append(line if raw_lines else obj if keep_all else [obj.get(att) for att in attribute])
            if verbose:
                now = time.monotonic()
                if now - last_report >= PROGRESS_EVERY:
                    last_report = now
                    n_matched = sum(map(len, buckets))
                    print(f"[{name}] {n_bytes / 1e9:.1f} GB scanned, {n_matched:,} records matched, "
                          f"{n_bytes / 1e6 / (now - t0):.0f} MB/s", flush=True)
    finally:
        if gc_was_enabled:
            gc.enable()
    n_matched = sum(map(len, buckets))
    elapsed = time.monotonic() - t0
    if verbose:
        rate = n_bytes / 1e6 / elapsed if elapsed else 0
        print(f"[{name}] done: {n_bytes / 1e9:.1f} GB scanned, {n_matched:,} records matched "
              f"in {elapsed:.0f}s ({rate:.0f} MB/s)", flush=True)

    # Save one file per filter (or return one DataFrame per filter)
    frames = []
    counts = {}
    for i, flt in enumerate(filters or ["alldata"]):
        records = buckets[i]
        buckets[i] = None  # release the records while the output is built
        counts[flt] = len(records)
        filter_name = filter_type + "_" + _sanitize_name(flt) if filters else "alldata"
        if to_jsonl:
            if not records:
                print(f"There is no Reddit {filter_type} data that contains {flt} in the input_path {file_path}.", flush=True)
                continue
            detail = {"type": reddit_type, "filter": flt if filters else "no_filter"} if add_detail == "yes" else {}
            if raw_lines:
                encode = lambda line: line
            elif keep_all:
                encode = lambda obj: _json_dumps({**detail, **obj})
            else:
                encode = lambda values: _json_dumps({**detail, **dict(zip(attribute, values))})
            saved = output_path + filter_name + "_" + get_stringname(get_filename(file_path)) + ".jsonl"
            _save_jsonl(records, encode, saved)
            if verbose:
                print(f"[{name}] {len(records):,} records for {filter_type} '{flt}' saved in {saved}", flush=True)
            del records
            continue
        df = pd.DataFrame(records) if keep_all else pd.DataFrame(records, columns=attribute)
        del records
        if add_detail == "yes":
            df.insert(0, "type", reddit_type)
            df.insert(1, "filter", flt if filters else "no_filter")
        if return_type == "save_file":
            saved = single_df_to_file(df, filter_type, flt, filter_name, file_path, output_path, save_type)
            if verbose and saved:
                print(f"[{name}] {len(df):,} records for {filter_type} '{flt}' saved in {saved}", flush=True)
        else:
            frames.append(df)
    if return_type == "merged_df":
        return frames
    return {"file": file_path, "bytes": n_bytes, "matched": n_matched, "seconds": elapsed, "counts": counts}


def _run_job(job):
    try:
        return get_contents(*job)
    except Exception as exc:  # keep the other files going; reported at the end
        return {"file": job[1], "error": f"{type(exc).__name__}: {exc}"}


def filter_files(files, filter_list, filter_type, attribute, add_detail, output_path, save_type,
                 match_mode=None, workers=1, verbose=True):
    """Filter several dump files, `workers` of them at a time, saving one output file per filter and dump."""
    files = list(files)
    jobs = [(None, f, filter_list, filter_type, attribute, add_detail, output_path, save_type,
             "save_file", match_mode, verbose) for f in files]
    workers = max(1, min(int(workers), len(jobs)))
    if verbose:
        print(f"Filtering {len(jobs)} file(s) with {workers} worker(s) ...", flush=True)
    t0 = time.monotonic()
    if workers == 1:
        results = [_run_job(job) for job in jobs]
    else:
        with Pool(workers) as pool:
            results = list(pool.imap_unordered(_run_job, jobs))
    failed = [r for r in results if "error" in r]
    done = [r for r in results if "error" not in r]
    if verbose:
        total_bytes = sum(r["bytes"] for r in done)
        total_matched = sum(r["matched"] for r in done)
        print(f"Filtered {len(done)} file(s): {total_bytes / 1e9:.1f} GB scanned, {total_matched:,} records "
              f"kept in {time.monotonic() - t0:.0f}s.", flush=True)
    if failed:
        details = "\n".join(f"  {r['file']}: {r['error']}" for r in failed)
        raise RuntimeError(f"{len(failed)} file(s) could not be filtered:\n{details}")
    return results
