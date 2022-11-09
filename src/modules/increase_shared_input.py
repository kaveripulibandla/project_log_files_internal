import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))
sys.path.append(str(Path.cwd().parent.parent))
import pandas as pd
import math
import random
import env
from helpers.s3_helper import S3Helper
import os
import logging
import random

logging.basicConfig(filename='log.log', filemode='w',
                    format='%(asctime)s.%(msecs)03d : %(name)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
logging.getLogger().setLevel(logging.DEBUG)


def get_data_df(input_file_path):
    """
    The get_data_df function reads in the data from a csv file and returns a pandas DataFrame.
    The function also adds two new columns to the DataFrame, one for each of the two features we will be using.

    :param input_file_path: Specify the path to the input file
    :return: A dataframe with the following columns:
    """
    df = pd.read_csv(input_file_path, sep=' ', header=None)
    df[8] = df[8].apply(lambda x: add_str(x))
    df[10] = df[10].apply(lambda x: add_str(x))
    return df


def add_str(val):
    """
    The add_str function adds a string to the end of a value.

    :param val: Add a string to the end of the input
    :return: A string with a leading space
    """
    return r" {}".format(val)


def get_required_var_for_iterations(data_df_row_size, rows_to_be_populated):
    """
    The get_required_var_for_iterations function takes in the size of the dataframe and
    the number of rows to be populated into the dataframe. It returns a tuple containing:
        1) The number of times that we need to iterate over our dataframe (loop_iterations_to_df).
        2) A list containing all rows that are divisible by 10, which will be used as indices for populating our df.
           This is done because it's easier to populate a df with values from 0-9 rather than 1-10, since there are no values between 9 and 10.
           We avoid index

    :param data_df_row_size: Determine how many rows to be populated in each iteration
    :param rows_to_be_populated: Determine how many rows to be populated in the dataframe
    :return: The number of iterations required to populate the dataframe, rows to be picked and rows to be avoided
    """
    df_row_size = data_df_row_size
    loop_iterations_to_df = math.ceil(rows_to_be_populated / df_row_size)
    rows_to_pick = list(filter(lambda val: val % 10 == 0, [*range(0, df_row_size)]))[1:]
    rows_to_avoid = [0] + list(filter(lambda val: val % 10 != 0, [*range(0, df_row_size)]))
    return loop_iterations_to_df, rows_to_pick, rows_to_avoid


def increase_ip(ip_val):
    """
    The increase_ip function takes an IP address and increments the last octet by 1.
    For example, if the input is 192.168.0.2, then it will return 192.168.0.3.

    :param ip_val: Pass the ip address to be incremented
    :return: The next ip address in the subnet
    """
    ip_list = ip_val.split(".")
    ip_max_val = 255
    if int(ip_list[-1])+1 <= ip_max_val:
        ip_list[-1] = str(int(ip_list[-1])+1)
    else:
        if int(ip_list[-2])+1 <= ip_max_val:
            ip_list[-2] = str(int(ip_list[-2])+1)
        else:
            if int(ip_list[-3])+1 <= ip_max_val:
                ip_list[-3] = str(int(ip_list[-3])+1)
            else:
                if int(ip_list[-4])+1 <= ip_max_val:
                    ip_list[-4] = str(int(ip_list[-4])+1)
                else:
                    ip_list = [str(10), str(0), str(0), str(0)]
    return ".".join(ip_list)


def clean_request(request_val):
    """
    The clean_request function takes a request value and replaces the method with a random choice from
    a list of variations. This is done to avoid any bias in the data.

    :param request_val: Pass in the request value that is being passed into the function
    :return: A random choice from the list of method variations
    """
    method_variations_list = ["GET", "POST", "HEAD"]
    split_request_val = request_val.split(" ")
    split_request_val[0] = random.choice(method_variations_list)
    split_request_val = " ".join(split_request_val)
    return split_request_val


def replace_rows(df):
    """
    The replace_rows function takes a dataframe as an argument and returns the same dataframe with
    the following changes:
        1. The IP address is converted to an integer by using the ip2int function.
        2. The timestamp is converted to a datetime object by using pd.to_datetime and then adding one hour (3600 seconds).  This accounts for timezone differences between local machine and server, which was causing issues when trying to merge on date/time fields later in this notebook.  It also makes it easier for us to do analysis on each day separately since we can easily filter out times that are not

    :param df: Pass the dataframe to be modified
    :return: A new dataframe with the following modifications:
    """
    df[0] = df[0].apply(lambda x: increase_ip(x))
    df[3] = pd.to_datetime(df[3], format="[%d/%b/%Y:%H:%M:%S") + pd.Timedelta(hours=1)
    df[3] = df[3].dt.strftime('[%d/%b/%Y:%H:%M:%S')
    df[5] = df[5].apply(lambda x: clean_request(x))
    return df

def remove_space(save_to_path):
    """
    The remove_space function removes the space between the first and second line of each
    paragraph. This is done to make it easier for me to read through my generated ReST files.

    :param save_to_dummy_path: Save the original file with spaces
    :param save_to_path: Save the file to a specific path
    :return: A list of lines from the dummy file
    """
    all_lines = []
    dummy_file_lines = open(save_to_path, 'r').readlines()
    for line in dummy_file_lines:
        temp_line = line.split('"')
        temp_line[3] = temp_line[3].strip()
        temp_line[-2] = temp_line[-2].strip()
        changed_line = '"'.join(temp_line)
        all_lines.append(changed_line)
    save_original_file = open(save_to_path, 'w')
    save_original_file.writelines(all_lines)


def populate_file(loop_iterations_to_df, rows_to_pick, rows_to_avoid, data_df, save_to_path):
    """
    The populate_file function takes in the following parameters:
        1. loop_iterations_to_df - The number of times to run through the dataframe and replace rows with random values
        2. rows_to_pick - The number of rows to replace with random values per iteration
        3. rows_to_avoid - The number of original, unaltered, non-randomized data points that should be avoided per iteration
            (this is done by randomly selecting a subset from the original dataset)
            This parameter is used so that we don't end up replacing all our randomized data points with new randomized

    :param loop_iterations_to_df: Specify the number of times the function should loop through and populate a file
    :param rows_to_pick: Specify the number of rows to be replaced in each iteration
    :param rows_to_avoid: Avoid replacing rows that are already replaced
    :param data_df: Store the dataframe that is to be populated
    :param save_to_path: Specify the path where the modified dataframe will be saved
    :param save_to_dummy_path: Save the dataframe to a temporary file
    :return: A dataframe with the rows replaced
    """
    modified_df = data_df
    df_row_count = 0
    _ = os.remove(save_to_path) if os.path.exists(save_to_path) else True
    _ = True if os.path.isdir(r"/".join(save_to_path.split(r"/")[:-1])) else os.mkdir(
        r"/".join(save_to_path.split(r"/")[:-1]))
    loop_count=0
    while df_row_count<=env.processed_input_count:
        # if loop_count > 0:
        shuffled_df = modified_df.sample(frac=1)
        rows_to_pick = list(filter(lambda val: val % 10 == 0, [*range(1, shuffled_df.shape[0])]))
        replaced_df = replace_rows(shuffled_df.iloc[rows_to_pick])
        modified_df = modified_df.append(replaced_df).sample(frac=1).drop_duplicates()
        df_row_count = modified_df.shape[0]
        loop_count += 1
        logging.debug("{} iterations completed and {} rows populated".format(loop_count, df_row_count))
        duplicate_rows_count = (modified_df[:env.processed_input_count].shape[0]*env.duplicate_percentage)//100
    fin_clean_df = modified_df[:(env.processed_input_count-duplicate_rows_count)]
    modified_df = fin_clean_df.append(fin_clean_df.sample(n=duplicate_rows_count)).sample(frac=1)
    modified_df.to_csv(save_to_path, header=None, index=None, sep=' ')
    remove_space(save_to_path)

if __name__ == "__main__":
    # S3Helper.read_file_from_s3(S3Helper.get_boto3_session(),
    #                            env.s3_bucket,
    #                            "{}/{}".format(os.getcwd(), env.shared_input_path),
    #                            env.s3_shared_input_path,
    #                            True)
    input_file_path = r"{}/{}".format(os.getcwd(), env.shared_input_path)
    rows_to_be_populated = env.processed_input_count
    save_to_path = r"{}/{}".format(os.getcwd(), env.processed_input_path)
    data_df = get_data_df(input_file_path)
    loop_iterations_to_df, rows_to_pick, rows_to_avoid = get_required_var_for_iterations(data_df.shape[0],
                                                                                         rows_to_be_populated)
    populate_file(loop_iterations_to_df, rows_to_pick, rows_to_avoid, data_df, save_to_path)