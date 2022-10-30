import pandas as pd
import math
import random
import env
from helpers.s3_helper import S3Session
import os
import csv

def get_data_df(input_file_path):
    # Reads file as dataframe
    df = pd.read_csv(input_file_path, sep=' ', header = None)
    df[8] = df[8].apply(lambda x: add_str(x))
    df[10] = df[10].apply(lambda x: add_str(x))
    return df.iloc[:300]

def add_str(val):
    return r" {}".format(val)

def read_file_from_s3(session, bucket_name, download_path, s3_file_path, run_once):
    s3_resource = session.resource('s3')
    s3_client = session.client('s3')
    bucket = s3_resource.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=s3_file_path))
    for obj in objs:
      _ = True if os.path.isdir(download_path) else os.mkdir(download_path)
      s3_client.download_file(bucket_name, obj.key, r"{}/{}".format(download_path, obj.key.split(r"/")[-1]))
      if run_once:
        break

def get_required_var_for_iterations(data_df_row_size, rows_to_be_populated):
    # Gives inputs required for iteration
    df_row_size = data_df_row_size
    loop_iterations_to_df = math.ceil(rows_to_be_populated/df_row_size)
    rows_to_pick = list(filter(lambda val: val%10==0 , [*range(0, df_row_size)]))[1:]
    rows_to_avoid = [0] + list(filter(lambda val: val%10!=0 , [*range(0, df_row_size)]))
    return loop_iterations_to_df, rows_to_pick, rows_to_avoid

def increase_ip(ip_val):
  # Increases ip value by 1
    ip_len = ip_val.split(".")
    return ".".join([str(int(val)+1) if (index == len(ip_len)-1) else val for index, val in enumerate(ip_len)])

def clean_request(request_val):
  # Cleans the reuest column
    remove_char_list = ['%', '-', '|', ","]
    method_variations_list = ["GET", "POST", "HEAD"]
    # " ".join(["GET", "POST", "HEAD", "new"]).replace("HEAD", "").replace("  "," ").strip().split(" ")
    #values should not contain spaces
    splitted_request_val = request_val.split(" ")
    splitted_request_val[0] = random.choice(method_variations_list)
    splitted_request_val = " ".join(splitted_request_val)
    return splitted_request_val
  # return "".join(["" if request_char in remove_char_list else request_char for request_char in splitted_request_val])

def clean_referer(referer_val):
    remove_char_list = ['%', '-', '|', ","]
    return "".join(["" if request_char in remove_char_list else request_char for request_char in referer_val])

def replace_rows(df):
  # Replaces the original rows
    df[0] = df[0].apply(lambda x: increase_ip(x))
    df[3] = pd.to_datetime(df[3], format="[%d/%b/%Y:%H:%M:%S") + pd.Timedelta(hours=1)
    df[3] = df[3].dt.strftime('[%d/%b/%Y:%H:%M:%S')
    df[5] = df[5].apply(lambda x: clean_request(x))

    # df[8] = df[8].apply(lambda x: clean_referer(x))
    return df

def remove_space(save_to_dummy_path,save_to_path):
    all_lines = []
    dummy_file_lines = open(save_to_dummy_path, 'r').readlines()
    for line in dummy_file_lines:
        temp_line = line.split('"')
        temp_line[3] = temp_line[3].strip()
        temp_line[-2] = temp_line[-2].strip()
        changed_line = '"'.join(temp_line)
        all_lines.append(changed_line)
    save_original_file = open(save_to_path, 'w')
    save_original_file.writelines(all_lines)

def populate_file(loop_iterations_to_df, rows_to_pick, rows_to_avoid, data_df, save_to_path, save_to_dummy_path):
  # Populates the ouput file 
    modified_df = data_df
    df_row_count = 0
    _ = os.remove(save_to_path) if os.path.exists(save_to_path) else True
    _ = os.remove(save_to_dummy_path) if os.path.exists(save_to_dummy_path) else True
    _ = True if os.path.isdir(r"/".join(save_to_path.split(r"/")[:-1])) else os.mkdir(r"/".join(save_to_path.split(r"/")[:-1]))
    _ = True if os.path.isdir(r"/".join(save_to_dummy_path.split(r"/")[:-1])) else os.mkdir(r"/".join(save_to_dummy_path.split(r"/")[:-1]))
    for file_index in range(0,loop_iterations_to_df):
      if file_index>0:
        shuffled_df = modified_df.sample(frac=1)
        replaced_df = replace_rows(shuffled_df.iloc[rows_to_pick])
        unchanged_shuffled_df = shuffled_df.iloc[rows_to_avoid]
        modified_df = pd.concat([unchanged_shuffled_df, replaced_df], axis=0).sample(frac=1)
        # print(modified_df)
        # print(modified_df.dtypes)
        modified_df.to_csv(save_to_dummy_path, header=None, index=None, sep=' ', mode = "a")
        remove_space(save_to_dummy_path,save_to_path)
        df_row_count += modified_df.shape[0]
        print("{} iterations completed and {} rows populated".format(file_index, df_row_count))
      else:
        modified_df.to_csv(save_to_dummy_path, header=None, index=None, sep=' ', mode = "a")
        remove_space(save_to_dummy_path,save_to_path)
        df_row_count += modified_df.shape[0]
        print("{} iterations completed and {} rows populated".format(file_index, df_row_count))

if __name__ == "__main__":
    # read_file_from_s3(S3Session.get_boto3_session(), 
    #                   env.s3_bucket, 
    #                   "{}/{}".format(os.getcwd(), env.shared_input_path), 
    #                   env.s3_shared_input_path, 
    #                   True)
    # input log file path
    input_file_path = r"{}/{}/{}".format(os.getcwd(), env.shared_input_path, env.shared_input_file)
    # # rows required
    rows_to_be_populated = env.processed_input_count
    # output log file path
    save_to_path = r"{}/{}/{}".format(os.getcwd(), env.processed_input_path, env.processed_input_file)
    save_to_dummy_path = r"{}/{}/{}".format(os.getcwd(), env.processed_input_path, env.dummy_processed_input_file)
    data_df = get_data_df(input_file_path)
    loop_iterations_to_df, rows_to_pick, rows_to_avoid = get_required_var_for_iterations(data_df.shape[0], rows_to_be_populated)
    populate_file(loop_iterations_to_df, rows_to_pick, rows_to_avoid, data_df, save_to_path, save_to_dummy_path)