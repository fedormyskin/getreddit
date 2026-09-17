import urllib.request as ur
import os
import pandas as pd
import pickle

# Function to get file name with extension
def get_filename(file_path):
    try:
        return file_path.split('/')[-1]
    except:
        raise ValueError("Cannot parsing the URL to get the file name with the extention. Please re-check the URL.")

# Function to get file name without extension
def get_stringname(file_name):
    try:
        return file_name.split('.')[0]
    except:
        raise ValueError("Cannot parsing the URL to get the file name without the extention. Please re-check the URL.")

# Function for downloading the file
def download(url_path,input_folder_path):
    try:
        print("Download process is started ...")
        file_path = input_folder_path+get_filename(url_path)
        ur.urlretrieve(url_path, file_path)
        print(f'Reddit data saved into {file_path}.')
    except:
        raise ValueError("Cannot download the Reddit data file. Please re-check the URL and input_folder_path.")

# Function for deleting the downloaded file
def remove(file_path):
    try:
        os.remove(file_path)
        print(f'{file_path} has been removed.')
    except:
        raise ValueError("Cannot remove the file. Make sure the file_path is correct.")

def save_to_file(merged_dataframe,filter_list,filter_type,input_path,output_folder_path,save_type):
    for i in range(0,len(merged_dataframe)):
        df = merged_dataframe[i]
        if filter_list == []:
            filter_name = "alldata_"
        else:
            flt = filter_list[i]
            flt = str(flt).replace(".","_DOT_")
            flt = str(flt).replace(",","_COMA_")
            flt = str(flt).replace(":","_COLON_")
            flt = str(flt).replace(";","_SEMICOLON_")
            filter_name = filter_type+"_"+flt+"_"
        name_with_path = output_folder_path+filter_name+get_stringname(get_filename(input_path))
        if len(df) != 0:
            try:
                if save_type == "xlsx":
                    saved_file_name = name_with_path+".xlsx"
                    df.to_excel(saved_file_name, index=False)
                elif save_type == "csv":
                    saved_file_name = name_with_path+".csv"
                    df.to_csv(saved_file_name, index=False)
                elif save_type == "pickle":
                    saved_file_name = name_with_path+".pickle"
                    with open(saved_file_name, 'wb') as handle:
                        pickle.dump(df, handle, protocol=pickle.HIGHEST_PROTOCOL)
                else:
                    raise ValueError("Wrong save_type. Only 'xlsx', 'csv', or 'pickle' are allowed to be used as save_type.")
                if filter_list == []:
                    print(f'The Reddit data (all data without filtering) has been saved in {saved_file_name}.')
                else:
                    print(f'The Reddit data for {flt} {filter_type} has been saved in {saved_file_name}.')
                del df
            except:
                raise ValueError("Cannot save the file into the output_folder_path defined. Make sure the output_folder_path is correct.")
        else:
            print(f'There is no Reddit {filter_type} data that contains {flt} in the input_path {input_path}.')

def single_df_to_file(df,filter_type,flt,filter_name,input_path,output_folder_path,save_type):
    # Returns the path of the saved file, or None when there was nothing to save
    if len(df) != 0:
        if filter_type == "subreddit_list":
            saved_file_name = output_folder_path+filter_name+"."+save_type
        else:
            saved_file_name = output_folder_path+filter_name+"_"+get_stringname(get_filename(input_path))+"."+save_type
        if save_type not in ("xlsx","csv","pickle"):
            raise ValueError("Wrong save_type. Only 'xlsx', 'csv', or 'pickle' are allowed to be used as save_type.")
        try:
            if save_type == "xlsx":
                df.to_excel(saved_file_name, index=False)
            elif save_type == "csv":
                df.to_csv(saved_file_name, index=False)
            else:
                with open(saved_file_name, 'wb') as handle:
                    pickle.dump(df, handle, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            raise ValueError(f"Cannot save the file {saved_file_name}. Make sure the output_folder_path is correct ({e}).") from e
        return saved_file_name
    else:
        if filter_type == "subreddit_list":
            print(f'Cannot get subreddit that contains/discusses {flt}.This maybe because there is no subreddit that contains/discusses {flt}, internet problem connection problem, the scrapper getting blocked, or the Reddit HTML sctructure has been changed.')
        else:
            print(f'There is no Reddit {filter_type} data that contains {flt} in the input_path {input_path}.')

def sentences_to_file(df,file_path,output_folder_path,save_type):
    try:
        saved_file_name = output_folder_path+"splitted_"+get_filename(file_path)+"."+save_type
        if save_type == "xlsx":
            df.to_excel(saved_file_name, index=False)
        elif save_type == "csv":
            df.to_csv(saved_file_name, index=False)
        elif save_type == "pickle":
            with open(saved_file_name, 'wb') as handle:
                pickle.dump(df, handle, protocol=pickle.HIGHEST_PROTOCOL)
        else:
            raise ValueError("Wrong save_type. Only 'xlsx', 'csv', or 'pickle' are allowed to be used as save_type.")
    except:
        raise ValueError("Cannot save the file into the output_folder_path defined. Make sure the output_folder_path is correct.")