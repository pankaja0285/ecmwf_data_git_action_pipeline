import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time

# import custom script
from ecmwf_data_processing_scripts import *
from s3_scripts import *
# *************************************************************************************************

def main_process_ecmwf_data(download_path="", prepped_path="", prepped_suffix="",
                            filter_levels=[], level=2,
                            number_of_days=0, step_counter=6,
                            push_destination="", yaml_file="",
                            delete_s3_files=False
                            ):
    # number_of_days = 5   # 10
    # step_counter = 6

    # ********** NOTE: Always assumed today's date, hence not passing to the function call below
    # start_date_obj = date.today()
    # start_date = start_date_obj.strftime("%Y%m%d")
    # print(f"start_date: {start_date}")
    # *********************************************** 
    
    fmt_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"ECMNWF Data download and processing started at: {fmt_date}")
    start_t = time.time()

    # clean data on s3 first
    if delete_s3_files:
        s3c, _, s3s = connect_to_s3_resource(yaml_file=yaml_file)  # "gribcfg.yaml")
        bucket_name = s3s['bucket_name']
        print(f"bucket_name: {bucket_name}")
        flist = list_bucket_objects(bucket=bucket_name, s3_client=s3c)
        print(f"list of files in s3: {flist}")
        if len(flist) > 0:
            del_status = remove_files_on_s3(file_list=flist, bucket=bucket_name, s3_client=s3c)

    # download and process
    overall_status = download_and_process_ecmwf_data(download_path=download_path, prepped_path=prepped_path,
                                                     prepped_suffix=prepped_suffix,
                                                     filter_levels=filter_levels, level=level,
                                                     number_of_days=number_of_days, step_size=step_counter,
                                                     push_destination=push_destination, 
                                                     yaml_file=yaml_file
                                                    )
    end_t = time.time()
    fmt_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"ECMNWF Data download and processing completed at: {fmt_date}")

    time_diff = round(end_t - start_t)
    # find the difference and print the total time taken
    if time_diff <= 60:
        print(f"Total time taken for ECMWF Data download and process: {time_diff} seconds")
    else:
        total_time = timedelta(seconds = time_diff)
        print(f"Total time taken for ECMWF Data download and process: {total_time}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Downloading ECMWF data and processing to convert to .CSV...')
    parser.add_argument('--download_path', type=str, default='./download',
                        help='main download path for Grib2 files')
    parser.add_argument('--prepped_path', type=str, default='./prepped',
                        help='prepped data main path')
    parser.add_argument('--prepped_suffix', type=str, default='temp',
                        help='temp folder for individually converted (by step hour)grib2 to csv file')
    parser.add_argument('--filter_levels', type=str, default="surface, heightAboveGround",
                        help='a comma separated filter-level strings e.g. "surface, heightAboveGround"')
    parser.add_argument('--level', type=str, default='2',
                        help='level value for extraction if applicable, to specific filter_level')
    parser.add_argument('--number_of_days', type=str, default='5',
                        help='total number of days to get the ECMWF data for')  
    parser.add_argument('--step_counter', type=str, default='6',
                        help='step size')
    parser.add_argument('--push_destination', type=str, default='local',
                        help='push destination where the final prepped ECMWF data csv file will be stored')
    # parser.add_argument('--bucket_name', type=str, default='',
    #                     help='bucket name has to be present if push_destination="s3"')
    parser.add_argument('--yaml_file', type=str, default='gribcfg.yaml',
                        help='settings required for download')
    parser.add_argument('--delete_s3_files_flag', type=str, default='Y',
                        help='flag to indicate to delete all files on S3')
    
    parse_args = parser.parse_args()
    print(f'\nRun args for downloading ECMWF data and processing to convert to .csv --> {parse_args}')

    download_path = ""
    prepped_path = ""
    prepped_suffix = ""
    filter_levels_str = ""
    filter_levels = []  # e.g. filter_levels=['surface', 'heightAboveGround']
    level = 2
    number_of_days=5
    step_counter=6

    # push destination related
    push_destination = ""
    # bucket_name = ""
    yaml_file = ""
    delete_s3_files = False

    if parse_args.download_path:
        download_path = parse_args.download_path
    if parse_args.prepped_path is not None:
        prepped_path = parse_args.prepped_path
    if parse_args.prepped_suffix is not None:
        prepped_suffix = parse_args.prepped_suffix
    if parse_args.filter_levels is not None:
        filter_levels_str = parse_args.filter_levels
    if parse_args.level is not None:
        level_s = parse_args.level
        level = int(level_s)
    if parse_args.number_of_days is not None:
        number_of_days_s = parse_args.number_of_days
        number_of_days = int(number_of_days_s)
    if parse_args.step_counter is not None:
        step_counter_s = parse_args.step_counter
        step_counter = int(step_counter_s)

    # push destination related
    if parse_args.push_destination is not None:
        push_destination = parse_args.push_destination
    # if parse_args.bucket_name is not None:
    #     bucket_name = parse_args.bucket_name
    if parse_args.yaml_file is not None:
        yaml_file = parse_args.yaml_file
    if parse_args.delete_s3_files_flag is not None:
        delete_s3_files = True if parse_args.delete_s3_files_flag=="Y" else False
        
    # prepare filter levels array
    if len(filter_levels_str.strip()) > 0:
        filter_levels = [item.strip() for item in filter_levels_str.split(",")]
    print(f"filter_levels (split): {filter_levels}")
    # kickoff the main processing function
    main_process_ecmwf_data(download_path=download_path, prepped_path=prepped_path, 
                            prepped_suffix=prepped_suffix, filter_levels=filter_levels, level=level,
                            number_of_days=number_of_days, step_counter=step_counter,
                            push_destination=push_destination, 
                            yaml_file=yaml_file, delete_s3_files=delete_s3_files
                            )
    