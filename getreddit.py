from utilities import *
from getcontents import filter_files, resolve_input_files, split_remote, TEXT_FIELDS
# getsubreddits (browser scraping) and splitcontents (spaCy) are imported only by the modes
# that need them, so filtering does not require those dependencies and worker processes stay light.
import argparse
import glob
import os
import re
import pandas as pd

# Whether input_path points at Reddit dump(s): a .zst file, a folder with .zst files, or a glob pattern,
# on this machine or on another one reachable with ssh (user@server:/path)
def is_zst_input(input_path):
    if '.zst' in input_path or split_remote(input_path):
        return True
    return os.path.isdir(input_path) and len(glob.glob(os.path.join(input_path, "*.zst"))) > 0

# Default attribute to split when 'filter_type' is not set: 'body' for a file filtered from a Reddit
# Comment (RC) dump and 'title' for a file filtered from a Reddit Submission (RS) dump. The dump name
# is kept in the name of the filtered file, e.g. subreddit_olympics_RC_2022-10.pickle
def default_split_column(filename, mode):
    match = re.search(r'(?:^|_)(RC|RS)_\d{4}-\d{2}', filename)
    if match:
        reddit_type = match.group(1)
    elif "RC" in filename:
        reddit_type = "RC"
    elif "RS" in filename:
        reddit_type = "RS"
    else:
        raise ValueError(f"Cannot tell whether {filename} contains Reddit comments or submissions from its name. Please set the 'filter_type' parameter to the attribute that you want to split (e.g. 'body' or 'title').")
    if reddit_type == "RC":
        print(f"You are running {mode} mode but you do not set the 'filter_type' parameter and we detect that you are trying to split a Reddit Comment (RC) file so that it automatically to be set as 'body'.")
        return "body"
    print(f"You are running {mode} mode but you do not set the 'filter_type' parameter and we detect that you are trying to split a Reddit Submission (RS) file so that it automatically to be set as 'title'.")
    return "title"

# The main function for end-to-end used that can be called instantly from the terminal
def main(args):
    # Set argument for mode
    mode = args.mode
    if mode == "":
        if args.url_path != "":
            mode = "download"
        elif is_zst_input(args.input_path):
            mode = "filter"
        elif args.input_path != "":
            mode = "split"
        else:
            raise ValueError("Please set the 'mode' argument. Choose 'download', 'filter', 'split', or 'subreddit_list'.")
    url_path = args.url_path
    # Set argument for delete_file: files that you saved yourself ('filter' mode) are kept by default
    delete_file = args.delete_file
    if delete_file == "":
        delete_file = "no" if mode == "filter" else "yes"
    # Set default filter_type
    filter_type = args.filter_type
    if filter_type == "":
        if mode == "download" or mode == "filter":
            filter_type = "subreddit"
            print(f"You are running {mode} mode but you do not set the 'filter_type' parameter so that it automatically to be set as 'subreddit'.")
        elif mode == "subreddit_list":
            filter_type = "subreddit_list"
            print(f"You are running {mode} mode but you do not set the 'filter_type' parameter so that it automatically to be set as 'subreddit_list'.")
        # 'split' mode: the default is decided per file inside the split loop, see default_split_column()
    # Set argument for filter_list
    filter_list = args.filter_list
    if filter_list == "":
        if mode == "subreddit_list":
            raise ValueError("You try to run 'subreddit_list' mode. You should define the query list on 'filter_list' parameter. Run 'python getreddit.py -h' for the help.")
        else:
            filter_list = [] # This setting will set to collect all Reddit data
    elif '.xlsx' in filter_list:
        df_filter_list = pd.read_excel(filter_list)
        filter_list = list(df_filter_list[filter_type])
    elif '.csv' in filter_list:
        df_filter_list = pd.read_csv(filter_list)
        filter_list = list(df_filter_list[filter_type])
    elif '.pickle' in filter_list:
        df_filter_list = pd.read_pickle(filter_list)
        filter_list = list(df_filter_list[filter_type])
    else:
        filter_list = [flt.strip() for flt in filter_list.split(',') if flt.strip()]
        # Underscores stand for spaces in multi-word phrases; names such as subreddits keep them
        if filter_type in TEXT_FIELDS:
            filter_list = [flt.replace('_',' ') for flt in filter_list]
    # Set argument for input_path
    input_path = args.input_path
    # Set argument for output_path
    output_path = args.output_path
    if output_path != "":
        if output_path[-1] != "/":
            output_path = output_path+"/"

    # Process for 'download' or 'filter' mode
    if mode == "download" or mode == "filter":
        # Set argument for attribute (None: the default attributes for comments or submissions,
        # decided per file, since the type of each file is detected from its name)
        attribute_list = args.attribute_list
        if attribute_list == "":
            attribute_list = None
        else:
            attribute_list = [att.strip() for att in attribute_list.split(',') if att.strip()]
        # Download the Reddit data for 'download' mode
        if mode == "download":
            if input_path != "":
                if input_path[-1] != "/":
                    input_path = input_path+"/"
            download(url_path,input_path)
            files = [input_path+get_filename(url_path)]
        else:
            files = resolve_input_files(input_path)
        filter_files(files,filter_list,filter_type,attribute_list,args.add_detail,output_path,args.save_type,
                     match_mode=args.match_mode or None,workers=args.workers,verbose=args.verbose == "yes")
        if delete_file == "yes":
            for file_path in files:
                if split_remote(file_path):
                    print(f"{file_path} is on another machine and is not removed.")
                else:
                    remove(file_path)
            print("The entire process to collect and/or filter the Reddit data has been done.")
        else:
            print("The entire process to collect and/or filter the Reddit data has been done.")
            print("The original Reddit data file(s) are kept. Beware that the original file may be very big and make your storage full.")

    # Process for 'subreddit_list' mode
    elif mode == "subreddit_list":
        from getsubreddits import get_subreddit_list
        get_top_subreddit = args.get_top_subreddit
        if get_top_subreddit == 0:
            get_top_subreddit = "all"
        get_subreddit_list(filter_list,filter_type,output_path,args.save_type,"save_file", get_top_subreddit, args.time_sleep, args.time_sleep_mode, args.last_day_sample, args.sample, args.threshold_sample, args.threshold_member,args.verbose,args.headless)            

    # Process for 'split' mode
    else:
        from splitcontents import split_contents
        if (input_path == "") or (".zst" in input_path):
            raise ValueError("You are in 'split' mode. Please re-check your input_path argument. The input_path argument must be the path to the folder that contains the list of filtered Reddit file.")
        if input_path[-1] == "/":
            input_path = input_path[:-1]
        num_of_files = len(os.listdir(input_path))
        file_idx = 1
        for filename in os.listdir(input_path):
            f = os.path.join(input_path, filename)
            if os.path.isfile(f):
                if args.save_type in f:
                    # Set the attribute to split, from the file name when 'filter_type' is not set
                    split_column = filter_type
                    if split_column == "":
                        split_column = default_split_column(filename, mode)
                    if args.save_type == "pickle":
                        dataset = pd.read_pickle(f)
                    elif args.save_type == "excel":
                        dataset = pd.read_excel(f)
                    elif args.save_type == "csv":
                        dataset = pd.read_csv(f)
                    else:
                        raise ValueError("Wrong save_type argument. Only 'pickle', 'excel', or 'csv' that is allowed to be used as save_type argument.")
                    print(f'Processing the file {file_idx} of {num_of_files} i.e. file {f} ...')
                    df = split_contents(dataset,split_column,args.verbose,file_idx,num_of_files)
                    sentences_to_file(df,f,output_path,args.save_type)
                    if delete_file == "yes":
                        remove(f)
                        print("The original filtered data {f} is removed.")
                    print(f'Processing the file {file_idx} of {num_of_files} i.e. file {f} is done. The splitted sentence file is saved in {output_path}splitted_{get_filename(f)}')
            file_idx += 1
        print("The entire process to split the filtered Reddit data has been done.")
        if delete_file == "no":
            print("You choose to not remove the original filtered Reddit data. Beware that the original filtered file may be very big and make your storage full.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", help="The mode of use this library i.e. whether you want to download and filter ('download'), filter the downloaded dataset ('filter'), split based on sentences the filtered Reddit file ('split'), or collecting subreddit list from a certain query list ('subreddit_list').",
        type=str, default=""
    )
    parser.add_argument(
        "--verbose", help="The option ('yes' or 'no') whether you want to print the progress or not.",
        type=str, default="yes"
    )
    parser.add_argument(
        "--url_path", help="The URL link to download the reddit submissions (e.g. https://files.pushshift.io/reddit/submissions/) or comments (https://files.pushshift.io/reddit/comments/) from a particular month.",
        type=str, default=""
    )
    parser.add_argument(
        "--input_path", help="The folder path to store the original downloaded file (mode 'download'), or the downloaded .zst file that you want to filter, a folder containing several .zst files, or a glob pattern such as '/data/RC_2022-*.zst' (mode 'filter'; the files may be on another machine reachable with ssh, e.g. 'user@server:/data/dumps/', they are then read over ssh and filtered here without installing anything there), or the folder path which contain the list of filtered data that you want to split based on the sentence (mode 'split').",
        type=str, default=""
    )
    parser.add_argument(
        "--output_path", help="The folder path to store the output files.",
        type=str, default=""
    )
    parser.add_argument(
        "--save_type", help="The file extension type that you want to store your output files.",
        type=str, default="pickle"
    )
    parser.add_argument(
        "--add_detail", help="The option ('yes' or 'no') whether you want to add 'type' and 'filter' information into your collected Reddit data.",
        type=str, default="no"
    )
    parser.add_argument(
        "--filter_list", help="The words/phrases list that used to filter the Reddit data or to collect the subreddit list. Replace space with underscore, e.g. 'energy,climate_change,waste'. If you have a huge filter (query) list, you can save it in an excel, csv, or pickle with the name column is the same with the 'filter_type' parameter. Do not set this parameter if you want to collect all Reddit data in 'download' or 'filter' mode.",
        type=str, default=""
    )
    parser.add_argument(
        "--filter_type", help="The Reddit attribute that you want to filter using your filter list (for 'download' or 'filter' mode) or that you want to split based on sentences (for 'split' mode). If you run 'subreddit_list' mode to collect a subreddit list from a list of query ('filter_list'), the 'filter_type' should be set as the column name that contain your query list.",
        type=str, default=""
    )
    parser.add_argument(
        "--attribute_list", help="The Reddit attributes that you want to collect, e.g. 'id,subreddit,body'. Type 'all' if you want to collect all Reddit attributes (this will make the data size may very huge).",
        type=str, default=""
    )
    parser.add_argument(
        "--delete_file", help="The option to delete ('yes') the Reddit original file or not ('no'). By default the original file is deleted only in 'download' mode; files that you saved yourself ('filter' mode) are kept.",
        type=str, default=""
    )
    parser.add_argument(
        "--match_mode", help="How the filter values are compared with the 'filter_type' attribute (mode 'download' or 'filter'): 'exact' means the attribute must be equal to a filter value (the default for 'subreddit' and any other non-text attribute), 'contains' means the attribute must contain a filter value as a substring (the default for 'body', 'title', and 'selftext'). The comparison ignores the case of ASCII letters.",
        choices=["exact","contains"], type=str, default=""
    )
    parser.add_argument(
        "--workers", help="The number of .zst files that are filtered in parallel when input_path is a folder or a glob pattern (mode 'filter'). Each worker needs up to 2 GB of memory to decompress a Reddit dump, plus the memory of the records it keeps.",
        type=int, default=1
    )
    parser.add_argument(
        "--get_top_subreddit", help="The top subreddit list that you want to collect for each query. This option only used for 'subreddit_list' mode. Leave this parameter blank or set 0 to collect all subreddit list from each query",
        type=int, default=0
    )
    parser.add_argument(
        "--time_sleep", help="The time (in seconds) to sleep for the scraping process. If your internet is slow, set a higher time to sleep e.g. 5 or 10 (only used for 'subreddit_list' mode). This option only used for 'subreddit_list' mode.",
        type=int, default=2
    )
    parser.add_argument(
        "--time_sleep_mode", help="The option to set whether the time_sleep only applied for the subreddit list scrapping process (set 'only_subreddit_scrapper' for this option) or all processes including for the subreddit statistic collection process (set 'all' for this option). Set 'all' if your internet speed is slow and you highly consider collecting the subreddit statistic. This option only used for 'subreddit_list' mode.",
        choices=["only_subreddit_scrapper","all"], type=str, default="only_subreddit_scrapper"
    )
    parser.add_argument(
        "--last_day_sample", help="The range of the last day that you want to get the subreddit comments/submissions that mention the query. This option only used for 'subreddit_list' mode.",
        type=int, default=90
    )
    parser.add_argument(
        "--sample", help="The number of total samples that you want to calculate the comments/submissions that mention the query. The maximum is 1,000 following the Pushift API limitation. This option only used for 'subreddit_list' mode.",
        type=int, default=1000
    )
    parser.add_argument(
        "--threshold_sample", help="The number of thresholds for minimum comments/submissions in a subreddit that mentions the query. This option only used for 'subreddit_list' mode.",
        type=int, default=100
    )
    parser.add_argument(
        "--threshold_member", help="The number of thresholds for total members in a subreddit. This option only used for 'subreddit_list' mode.",
        type=int, default=10000
    )
    parser.add_argument(
        "--headless", help="The option to show the browser scrapping process or not. It should be set as 'yes' if you run this script in a server that does not has a browser GUI. This option only used for 'subreddit_list' mode.",
        type=str, default="yes"
    )
    args = parser.parse_args()
    main(args)