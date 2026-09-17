# GetReddit
A simple Python library to collect and filter Reddit data (submissions or comments) without Reddit API Access Token for re-training language model needs. `Update`: Since the Pushshift service changed, we cannot use this script to download Reddit data unless you modify the script.
## About this repository
Training/re-training language models for domain adaption or discourse analysis in specific domains sometimes needs a huge raw dataset. Reddit (https://www.reddit.com/) is one of the best places to collect dataset for domain adaption of the language model since there are various topic discussed there which is collected in one place called a "Subreddit". Pushshift (https://github.com/pushshift/api) are providing a great API to collect Reddit datasets based on your need but it is limited to a maximum of 1000 submissions/comments per hit. To get more data, they provide a link to download full submissions (https://files.pushshift.io/reddit/submissions/) or comments (https://files.pushshift.io/reddit/comments/) from each month. This Python library is created to make us easily download, filter, and split to sentence the Reddit submissions/comments provided by Pushshift. 
## Main use
In this Python library, we provide 3 different `mode` that you can use: <br />
1. `download`: This `mode` type is used to download and filter the Reddit submissions/comments data from a particular month in one run (`Update`: Since the Pushshift service changed, we cannot use this mode).
2. `filter`: This `mode` type is used to filter the downloaded Reddit submissions/comments data from a particular month in one run.
3. `split`: This `mode` type is used to split a specific attribute Reddit submissions/comments from into a separate sentence.
## Requirements
This Python library is implemented in `Python 3` and requires a number of packages. To install all needed packages, simply run `$ pip install -r requirements.txt` in your virtual environment. To be able to use the `split` `mode`, you need to download the model `en_core_web_sm` from Spacy, simply by scripting `$ python -m spacy download en_core_web_sm` on your virtual environment. Not that, this may only work in English. For other languages, you need to edit the Spacy model that is used to split the data. We recommend you to use `Python 3.10.2` version as we use it to develop it. Other `Python 3` version may need several modification in the package used.

# How to use as an instant library which directly called from the terminal
In this section, I just give examples of best practice use for each mode. For more functionality, you can explore how to set the parameters to use this library by scripting `$ python getreddit.py -h` on your virtual environment.
## Example to use the `download` `mode`
Suppose that you want to download all Reddit comments from the `olympics` and `programming` subreddit for October 2022 month (https://files.pushshift.io/reddit/comments/RC_2022-10.zst), where you only want to collect attributes `id`, `subreddit`, `author`, and `body`, and then save them to `/Users/username/folder/filtered/`, here is the minimum script that you need to run:
```
$ python getreddit.py --url_path https://files.pushshift.io/reddit/comments/RC_2022-10.zst --output_path /Users/username/folder/filtered/ --filter_list olympics,programming --attribute_list id,subreddit,author,body
```
## Example to use the `filter` `mode`
Suppose that you have downloaded the October 2022 Reddit comment (https://files.pushshift.io/reddit/comments/RC_2022-10.zst) and you saved it in `/Users/username/folder/input_folder/RC_2022-10.zst`. Then, you want to filter the `olympics` and `programming` subreddit from that file where you only want to collect attributes `id`, `subreddit`, `author`, and `body`, and then save them to `/Users/username/folder/filtered/`, here is the minimum script that you need to run:
```
$ python getreddit.py --input_path /Users/username/folder/input_folder/RC_2022-10.zst --output_path /Users/username/folder/filtered/ --filter_list olympics,programming --attribute_list id,subreddit,author,body
```
The `input_path` can also be a folder that contains several `.zst` files, a glob pattern such as `'/Users/username/folder/input_folder/RC_2022-*.zst'`, or a `.txt` file listing the dumps to filter, one path per line. Every file is filtered independently and produces its own output files (e.g. `subreddit_olympics_RC_2022-10.pickle`), so you can process several files at the same time with `--workers`:
```
$ python getreddit.py --input_path /Users/username/folder/input_folder/ --output_path /Users/username/folder/filtered/ --filter_list olympics,programming --workers 4
```
Each worker needs up to 2 GB of memory to decompress a Reddit dump (they are compressed with a 2 GB zstd window), plus the memory of the records it keeps.

The `.zst` files may also be on another machine that you can reach with `ssh`, without installing anything there: give the `input_path` as `user@server:/path/to/dumps/` (a file, a folder, or a glob pattern; absolute paths). Each file is then streamed with `ssh user@server cat ...` and decompressed and filtered on your machine, where the output files are written. Set up SSH keys or a `ControlMaster` connection so that you are not asked for a password for every file, and keep `--workers` low (1 or 2) since the connection bandwidth is shared.

By default only `id`, `subreddit`, and `body` (comments) or `title` (submissions) are kept. To keep every attribute of the matching records exactly as they are in the dump (including nested ones such as `media` or `all_awardings`), use `--attribute_list all --save_type jsonl`: the output is then one JSON record per line, which you can read with `pandas.read_json(path, lines=True)` or any JSON tool. `csv` and `xlsx` flatten nested attributes into text and lose the types, and `pickle` files depend on the pandas version that wrote them.

Subreddit names are matched exactly (`programming` does not match `learnprogramming`), ignoring the case of ASCII letters; a leading `r/` is accepted. Filters on text attributes (`body`, `title`, `selftext`) match substrings instead, also ignoring the case, and underscores in the `filter_list` stand for spaces (`climate_change`). Use `--match_mode exact` or `--match_mode contains` to override these defaults. The original `.zst` files that you saved are kept unless you pass `--delete_file yes`.

Filtering is done on the compressed file directly and it does not need much memory: a month of comments (~250 GB of JSON once decompressed) is scanned at a few hundred MB/s on a single core, so a subreddit filter takes roughly 10-20 minutes per file instead of hours. Installing `orjson` (in `requirements.txt`) makes parsing the matching records faster; the script works without it.
## Example to use the `split` `mode`
Suppose that you already collect your filtered Reddit comments (e.g. filtered by subreddit or by keyword applied on attribute `body`), saved in `/Users/username/folder/filtered/`, and you want to split all those comments files sentence by sentence then saved the splitted comments in `/Users/username/folder/splitted/`, here is the minimum script that you need to run:
```
$ python getreddit.py --input_path /Users/username/folder/filtered/ --output_path /Users/username/folder/splitted/
```

# How to use as a package library to be integrated in your python file
To use `GetReddit` as package library, simply call them by scripting `from getreddit import *` in your python file. Specify `*` with the specific function you need to be integrated with your python script.

# Limitation
This library is created for research needs, where the main purpose is just to collect the dataset. The `filter` mode scans the raw bytes of each record and only parses the JSON of the records that can match, so it is limited by the zstd decompression speed of one core per file; filtering on a text attribute (`body`, `title`, `selftext`) additionally scans every record for each keyword, so it gets slower with long keyword lists. All records matched by a filter are kept in memory until the output file is written, so filtering a very popular subreddit from a month of comments needs a few GB of memory. The `split` mode is still sequential. Any modification and contribution to improving this library is more than welcome :) 

# Credit
I'll be happy if you put a credit for this work. If you use this Python library, you must also put credit to the Pushshift team that provides the Reddit submissions (https://files.pushshift.io/reddit/submissions/) and comments (https://files.pushshift.io/reddit/comments/) data. If you processing a huge Reddit dataset using their provided dataset, you can also consider giving them donations: https://pushshift.io/donations/.
