import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import logging
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# import custom script
from ecmwf_data_processing_scripts import *
from s3_scripts import *
# *************************************************************************************************

def main_process_ecmwf_data(download_path="", prepped_path="", prepped_suffix="",
                            filter_levels=[], level=2,
                            number_of_days=0, step_counter=6,
                            push_destination="", push_data_path="",
                            yaml_file="", 
                            delete_s3_files=False
                            ):
    object_prefix = ""

    # ********** NOTE: Always assumed today's date, hence not passing to the function call below
    # start_date_obj = date.today()
    # start_date = start_date_obj.strftime("%Y%m%d")
    # print(f"start_date: {start_date}")
    # *********************************************** 
    
    fmt_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"ECMWF Data download and processing started at: {fmt_date}")
    logging.info(f"ECMWF Data download and processing started at: {fmt_date}")
    start_t = time.time()

    # clean data on s3 first
    if delete_s3_files:
        s3c, bucket_name = get_s3_client()
        # print(f"bucket_name: {bucket_name}")
        object_prefix = f"{push_data_path}/"
        flist = list_bucket_objects(bucket=bucket_name, s3_client=s3c, object_prefix=object_prefix)
        logging.info(f"list of files in s3 with object_prefix - {object_prefix}: {flist}")
        # check if file count is > 0, delete else proceed to refresh data
        if len(flist) > 0:
            del_status = remove_files_on_s3(file_list=flist, bucket=bucket_name, s3_client=s3c)
            print(f"ECMWF data files deleted on S3, to prep for data refresh.")
        else:
            print(f"No ECMWF data files found on S3, no action taken before data refresh.")
        s3c.close()
        
    # download and process
    overall_status = download_and_process_ecmwf_data(download_path=download_path, prepped_path=prepped_path,
                                                     prepped_suffix=prepped_suffix,
                                                     filter_levels=filter_levels, level=level,
                                                     number_of_days=number_of_days, step_size=step_counter,                                                     
                                                     push_destination=push_destination, 
                                                     push_data_path=push_data_path,
                                                     yaml_file=yaml_file
                                                    )
    
    # list the files after push to s3 as well for audit purposes
    s3c, bucket_name = get_s3_client()
    # print(f"bucket_name: {bucket_name}")
    object_prefix = f"{push_data_path}/"
    f_postlist = list_bucket_objects(bucket=bucket_name, s3_client=s3c, object_prefix=object_prefix)
    # user friendly format the list and show in the log
    logging.info(f"ECMWF data refresh on S3, for requested date of {fmt_date} for {number_of_days} days:")
    if f_postlist:
        fmt_flist = "\n".join(map(str, f_postlist))
        logging.info(f"{fmt_flist}")
    else:
        logging.warn(" Unable to locate files after the ECMWF data push to S3")
    s3c.close()

    # record end of ECMWF data processing
    end_t = time.time()
    fmt_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    print(f"ECMWF Data download and processing completed at: {fmt_date}")
    logging.info(f"ECMWF Data download and processing completed at: {fmt_date}")

    time_diff = round(end_t - start_t)
    msg = ""
    # find the difference and print the total time taken
    if time_diff <= 60:
        msg = f"Total time taken for ECMWF Data download and process: {time_diff} seconds"
    else:
        total_time = timedelta(seconds = time_diff)
        msg = f"Total time taken for ECMWF Data download and process: {total_time}"
    print(msg)
    logging.info(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Downloading ECMWF data and processing to convert to .CSV...')
    parser.add_argument('--download_path', type=str, default='download',
                        help='main download path for Grib2 files')
    parser.add_argument('--prepped_path', type=str, default='prepped',
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
    parser.add_argument('--push_data_path', type=str, default='ecmwfdata',
                        help='push destination data path prefix where the final prepped ECMWF data csv file will be stored')
    
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
    number_of_days = 5
    step_counter = 6
    
    # push destination related
    push_destination = ""
    push_data_path = ""
    yaml_file = ""
    # env = ""
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
    if parse_args.push_data_path is not None:
        push_data_path = parse_args.push_data_path
    # if parse_args.env is not None:
    #     env = parse_args.env
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
                            push_destination=push_destination, push_data_path=push_data_path,
                            yaml_file=yaml_file, 
                            delete_s3_files=delete_s3_files
                            )
    