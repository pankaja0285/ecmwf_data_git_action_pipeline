import os
from pathlib import Path
from ecmwf.opendata import Client
import re
import numpy as np
import pandas as pd 
import time
from datetime import datetime, timedelta, date
from tqdm import tqdm
from glob import glob

import xarray as xr
import yaml
from geopy.geocoders import Nominatim
import functools as ft

import logging
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ***************** DO NOT CHANGE THESE IMPORTS ************************
from s3_scripts import *
# **********************************************************************

# *************  Scripts - common
def get_ecmwf_client():
    ecmwf_client = Client(source="ecmwf")
    return ecmwf_client


def convert_coordinate_to_numeric(coord_string):
    # Find all sequences of one or more digits
    numbers_as_strings = re.findall(r'\d+', coord_string)

    # Convert the extracted strings to integers
    coord_values = [int(num) for num in numbers_as_strings]
    return coord_values


def convert_degrees_to_decimal(degrees, minutes=None, seconds=None, direction=None):
    """
    Converts Degrees, Minutes, Seconds (DMS) to Decimal Degrees (DD).

    Args:
        degrees (float or int): The degree component.
        minutes (float or int): The minute component.
        seconds (float or int): The second component.
        direction (str, optional): The cardinal direction ('N', 'S', 'E', 'W').
                                   If 'S' or 'W', the decimal degrees will be negative.
                                   Defaults to None.

    Returns:
        float: The equivalent value in decimal degrees.
    """
    conv_mins = (float(minutes) / 60) if minutes is not None else 0
    conv_secs = (float(seconds) / 3600) if seconds is not None else 0
    # deg_decimal = float(degrees) + (float(minutes) / 60) + (float(seconds) / 3600)
    deg_decimal = float(degrees) + conv_mins + conv_secs

    # further process if cardinal direction has been provided
    if direction and direction.upper() in ['S', 'W']:
        deg_decimal *= -1

    return deg_decimal

def set_coords_as_decimal(yaml_file=""):
    result = {}
    data = None
    vals = None

    try:
        # load config for coords
        with open(yaml_file, 'r') as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        # print(data['coords']['north'])
        # fetch the north, west, south, east coords
        coords_keys = data['coords'].keys()
        coord_keys_map = data["coords_map"]

        for key in coords_keys:
            # print(key)
            # convert coords to numeric
            vals = convert_coordinate_to_numeric(data['coords'][key])
            # convert coords to decimal
            disp_key = coord_keys_map[key]
            match len(vals):
                case 2:
                    result[disp_key] = convert_degrees_to_decimal(degrees=vals[0], minutes=vals[1])
                case 3:
                    result[disp_key] = convert_degrees_to_decimal(degrees=vals[0], minutes=vals[1], seconds=vals[2])
                case _:  # Default case
                    result[disp_key] = convert_degrees_to_decimal(degrees=vals[0], minutes=vals[1])
            

    except Exception as ex:
        logging.error(f"Error occurred as exception: {ex}")
    return result


def re_arrange_df(df, cols=[]):
    # get all current columns
    all_columns = df.columns.tolist()
    # new column order
    new_column_order = cols + [col for col in all_columns if col not in cols]
    # rearrange the DataFrame columns and return
    rearranged_df = df[new_column_order]
    return rearranged_df

# *************  Scripts - GRIB2 related
       
def load_grib2_to_dataframe(file_path, filter_level="", level=0):
    
    df = None
    ds = None
    backend_kwargs = {}
    
    try:
        match filter_level:
            case "heightAboveGround":
                backend_kwargs={
                    'filter_by_keys': {
                        'typeOfLevel': f"{filter_level}",   # 'heightAboveGround',
                        'level': level, # for temperature at 2 meters above ground
                        # 'shortName': 't' # for temperature
                        # 'errors': 'ignore',
                        # 'indexpath': ''
                    }
                }
            case "surface":
                backend_kwargs={
                    'filter_by_keys': {
                        'typeOfLevel': 'surface',
                        # 'errors': 'ignore',
                        # 'indexpath': ''
                    }
                }
            case "isobaricInhPa":
                backend_kwargs={
                    'filter_by_keys': {
                        'typeOfLevel': 'isobaricInhPa',
                        # 'errors': 'ignore',
                        # 'indexpath': ''
                    }
                }
            
            case _:
                backend_kwargs={
                    'filter_by_keys': {
                        'typeOfLevel': 'surface',
                        # 'errors': 'ignore',
                        # 'indexpath': ''
                    }
                }  
        
        print(f"backend_kwargs:\n{backend_kwargs}")
        # load the grib2 to a xarray dataset
        # ds = xr.open_dataset(file_path, engine='cfgrib') # <-- this errors out
        # ******  WORKS When we give the backend_kwargs as it is needed for heterogeneous datasets
        # ds = xr.open_dataset(file_path, engine='cfgrib', 
        #                      backend_kwargs={'filter_by_keys': {'typeOfLevel': f"{typeoflevel}"}}, # 'surface'}},
        #                      decode_timedelta=True
        #                      )

        ds = xr.open_dataset(file_path, engine='cfgrib', 
                             backend_kwargs=backend_kwargs, 
                             decode_timedelta=True
                             )       
               
        # load ds to dataframe
        if ds is not None:
            df = ds.to_dataframe()
            df = df.reset_index()
    except Exception as e:
        logging.error(f"Error opening GRIB2 file: {e}")
        # Handle the error, e.g., exit or try another engine
    return df

def load_combine_filter_ecmwf_grib_data(file_path="", filter_levels=[], level=2, k2cvalue=273.15,
                                        yaml_file=""):
    status = False
    step = ""
    df_flevel = None
    df_flevels = []
    filtered_df = None
    cols_dict = {
        "surface": ['longitude', 'latitude', 'surface', 'tp', 'tprate'],
        "heightAboveGround": ['longitude', 'latitude', 'time', 't2m']
    }
    
    try:
        step = " filter levels "
        for filter_level in filter_levels:
            df_flevel_curr = load_grib2_to_dataframe(file_path=file_path, filter_level=filter_level,
                                                      level=level)
            df_flevel = df_flevel_curr[cols_dict[filter_level]].copy(deep=True)
            df_flevels.append(df_flevel)

        step = " combine data "
        df_cmb_initial = ft.reduce(lambda left, right: pd.merge(left, right, on=['latitude', 'longitude'], how="inner"), df_flevels)
        # print(f"df_cmb_initial: {df_cmb_initial.shape}")
        # print(f"df_cmb_initial cols: {df_cmb_initial.columns}")
        # print(df_cmb_initial.head(2))

        # apply to convert to celcius
        df_cmb_k2c = df_cmb_initial.copy(deep=True)
        # print("Before k to c conversion")
        # print(df_cmb_k2c.head(2))
        # convert kelvin to celcius
        df_cmb_k2c['t2m_cel'] = df_cmb_k2c['t2m'].apply(lambda x: x - k2cvalue) # 273.15)

        step = " filter for lats "
        min_max_coords = set_coords_as_decimal(yaml_file=yaml_file)   
        filtered_df = (df_cmb_k2c[
            (df_cmb_k2c['latitude'] >= min_max_coords["min_lat_bhutan"]) 
            & (df_cmb_k2c['latitude'] <= min_max_coords["max_lat_bhutan"]) &
            (df_cmb_k2c['longitude'] >= min_max_coords["min_lon_bhutan"]) 
            & (df_cmb_k2c['longitude'] <= min_max_coords["max_lon_bhutan"])
        ])
        status = True
        
    except Exception as ex:
        logging.error(f"Error occurred as exception: {ex} at step: {step}")
    
    return status, filtered_df


def load_grib2_to_csv(filter_levels=[], input_dir="", prepped_dir="",
                      prepped_suffix="temp", level=2, yaml_file=""):  
    # input_dir = "./download"
    prepped_suffix_dir = f"{prepped_dir}/{prepped_suffix}"

    current_dir = Path(input_dir)

    # file_patterns
    glob_ecmwf_filenames = current_dir.glob("*.grib2")
    ecmwf_filenames = [file_path.name for file_path in glob_ecmwf_filenames]
    print(f"ecmwf_filenames[0:1] - {ecmwf_filenames[0:1]}")

    # Process all the relevant grib2 files    
    os.makedirs(prepped_suffix_dir, exist_ok=True, mode=0o777)
    for filename in ecmwf_filenames:
        print(f"\nProcessing grib2 file: {filename}")
        logging.info(f"\nProcessing grib2 file: {filename}")
        inpath_filename = f"{input_dir}/{filename}"
        load_status, comb_df = load_combine_filter_ecmwf_grib_data(file_path=inpath_filename,
                                                                filter_levels=filter_levels,
                                                                level=level, yaml_file=yaml_file)
        
        if load_status:
            # save file
            save_filename = filename.split(".grib2")[0]
            save_filename = f"{prepped_suffix_dir}/{save_filename}.csv"
            comb_df.to_csv(save_filename, index=None)


# *************  Scripts - Other processing related
def assign_param_by_tag(row):    
    param_value = "temperature_celcius" if row['param_tag']=="t2m_cel" else "surface_area" if row['param_tag']=="surface" else "precipitation"
    return param_value

def format_date_final(row, date_format="%Y-%m-%d"):
    # Parse the original string into a datetime object
    # The format code "%Y%m%d" matches the input string "YYYYMMDD"
    date_object = datetime.strptime(row['forecast_date'], "%Y%m%d")

    # Format the datetime object into the desired string format
    date_value = date_object.strftime(date_format)

    return date_value

def combine_csvs_for_one_day(prepped_path="", prepped_suffix="temp", hour_array=[]):
    combined_1day_df = None
     
    step = ""

    print(f"\nCombining csvs to one csv - for current range of forecast_hours...")
    logging.info(f"\nCombining csvs to one csv - for current range of forecast_hours...")
    try:
        # set up file paths
        p_pattern = f"{prepped_path}/{prepped_suffix}/*.csv"
        prepped_files = glob(p_pattern)
        # e.g.: hr_arr = ['6h', '12h', '18h', '24h'] as hr_arr2 = [f"{t}h" for t in [6,12,18,24]]
        hr_arr = [f"{t}h" for t in hour_array]
        step = " matched_dict "
        matched_dict = {item1: item2 for item1 in hr_arr for item2 in prepped_files if item1 in item2}        
        print(f"matched_dict: {matched_dict}")

        # create a dictionary to hold the dataframes for easy access
        step = " load df " 
        dfs_dict = {}
        for hr_s in hr_arr:
            fname = matched_dict[hr_s]
            dfs_dict[hr_s] = pd.read_csv(fname)
        
        # identify the common columns and the forecast variables
        common_cols = ['latitude', 'longitude', 'time']
        forecast_vars = ['t2m_cel', 'surface', 'tp']

        # create a list to store the final, long-format dataframes for each variable
        combined_list = []

        # loop through each forecast variable and each dataframe to build the long format
        step = " forecastvars loop "
        first_key = list(dfs_dict.keys())[0]
        for var in forecast_vars:
            # Initialize a new DataFrame with the common columns
            combined_df_for_var = dfs_dict[first_key][common_cols].copy()
            combined_df_for_var['param_tag'] = var

            # Add the forecast data from each hourly dataframe
            for hour_key, df in dfs_dict.items():
                combined_df_for_var[hour_key] = df[var]

            combined_list.append(combined_df_for_var)

        # concatenate all the variable-specific dataframes into a single final dataframe
        step = " concatcombined "
        # print(f"len combined_list: {len(combined_list)}")
        merged_df_loop = pd.concat(combined_list, ignore_index=True)

        # print(f"Total rows in merged csv: {merged_df_loop.shape[0]}")
        # print(f"merged_df_loop cols: {merged_df_loop.columns}")
        
        # rename the columns 
        final_df = merged_df_loop.copy(deep=True)
        step = " choosecols "
        # e.g. ['latitude', 'longitude', 'time', 'param_tag', '6h', '12h', '18h', '24h']
        f_cols = ['latitude', 'longitude', 'time', 'param_tag']
        f_cols.extend(hr_arr)
        final_df = final_df[f_cols]
        step = " combrename "
        final_df.rename(columns={"time": "forecast_date", "t2m_cel": "temperature", "tp": "precipitation"}, inplace=True)
        # create param column
        final_df['param'] = final_df.apply(lambda r: assign_param_by_tag(r), axis=1)
        # # modify date column formatted as yyyy-mm-dd
        # final_df['forecast_date'] = final_df.apply(lambda r: format_date_final(r), axis=1)
        
        # rearrange columns
        step = " combrearr "
        cols_order = ['longitude', 'latitude', "forecast_date", "param", "param_tag"]
        combined_1day_df = re_arrange_df(final_df, cols=cols_order)
        # print(combined_1day_df.columns)
    except Exception as ex:
        logging.error(f"Error with exception: {ex} at step: {step}")
    return combined_1day_df


# *************  Scripts - Main driver function
def get_forecast_hours_for_total_days(num_days=0, step_size=6, start=6):
    hours_per_day = 24
    hours_array = list(range(start, (num_days * hours_per_day) + 1, step_size))
    return hours_array


def download_and_process_ecmwf_data(download_path="", prepped_path="", prepped_suffix="",
                                    filter_levels=[], level=2,                                    
                                    number_of_days=5, step_size=6,
                                    push_destination="", push_data_path="",
                                    yaml_file=""):
    step = ""
    dp_status = False
    bucket_name = ""

    try:
        root_temp_dir = os.getenv('TEMP_DIR', '/tmp')
        os.makedirs(root_temp_dir, exist_ok=True, mode=0o777)

        # get ecmwf client
        client = get_ecmwf_client()

        # param set up
        step = " initial param set up "
        
        # # Get today's date as a datetime.date object
        # today_date_object = date.today()

        # # Format today's date as a string in 'YYYY-MM-DD' format
        # formatted_date_string = today_date_object.strftime('%Y-%m-%d')

        # NOTE: Instead of using today's date (time and moment), let's account from one day prior...
        use_date_object = date.today() - timedelta(days=1)

        # Format today's date as a string in 'YYYY-MM-DD' format
        formatted_date_string = use_date_object.strftime('%Y-%m-%d')

        # Convert the formatted date string back to a datetime.date object
        # Use datetime.strptime() to parse the string, then extract the date part
        start_date = datetime.strptime(formatted_date_string, '%Y-%m-%d').date()
        # print(f"start_date type: {type(start_date)}")
        print(f"ECMWF Data Refresh -- Start date: {start_date}")
        logging.info(f"ECMWF Data Refresh -- Start date: {start_date}")

        step = " fhours "
        start_hour = step_size
        current_date = start_date
        # hours_per_day = 24
        step_hours = get_forecast_hours_for_total_days(num_days=number_of_days, 
                                                       step_size=step_size, start=start_hour)

        # loop
        cnt = 0        
        chunk_step_size = 24 // step_size  # 24 // 6 = 4
        # get the 4 chunked bins
        chunks = [step_hours[i:i + chunk_step_size] for i in range(0, len(step_hours), chunk_step_size)]
        step = " main processing loop(days) "
        uploaded_file_list = []

        for chunk in chunks:
            print(f"Processing chunk: {chunk}")
            logging.info(f"Processing chunk: {chunk}")
            # loop through for each day
            step = f" download {cnt+1} "
            logging.info(f"\nDownload process for Day {cnt+1}...")
            download_dir = f"{root_temp_dir}/{download_path}"
            os.makedirs(download_dir, exist_ok=True, mode=0o777)
            for i in range(len(chunk)):
                step_hour = chunk[i]
                print(f"step_hour: {step_hour}")
                # set target filename 
                # NOTE: FYI, here we are closely mimicking to the server filename              
                target_filename = f"{download_dir}/ecmwf_data_{start_date.strftime('%Y%m%d')}000000_{step_hour}h_oper_fc.grib2"
                if not os.path.exists(target_filename):
                    client.download(
                        date=current_date.strftime("%Y%m%d"), # Specify the date
                        time=0,  # Assuming you want the 00 UTC forecast for each day
                        step=step_hour,
                        stream="oper", # Example stream, adjust if needed, we are allowed 'oper' ONLY
                        type="fc", # Forecast data
                        target=target_filename,
                    )
                    logging.info(f"Downloaded data for {current_date} to {target_filename}")
                else:
                    logging.info(f"⚠️ Already exists: {target_filename}")
                        
            # process (load, combine, save to prepped) before resuming the while loop
            # load
            step = f" loadgribtocsv cnt {cnt+1} "
            # print(f"filter_levels: {filter_levels}")
            prepped_dir = f"{root_temp_dir}/{prepped_path}"
            os.makedirs(prepped_dir, exist_ok=True, mode=0o777)
            load_grib2_to_csv(filter_levels=filter_levels, input_dir=download_dir,
                              prepped_dir=prepped_dir, level=level, yaml_file=yaml_file)
            
            # combine period csv s for one day to one common csv
            step = f" cmbcsv cnt {cnt+1} "
            df_comb_csv = combine_csvs_for_one_day(prepped_path=prepped_dir, hour_array=chunk)
            print(df_comb_csv.head(2))

            # delete the grib2 and grib2.idx files
            # NOTE: each time a grib2 file is opened, an index file i.e. idx file gets created,
            #       so *.grib2.* pattern is needed, as it will delete all grib2 related files.
            step = f" del gribdate {cnt+1} "
            del_path = f"{download_dir}/*.grib2*"
            files_to_del = glob(del_path)
            for f_del in files_to_del:
                os.remove(f_del)

            # delete the grib files
            step = f" del prepdate {cnt+1} "
            p_del_path = f"{prepped_dir}/{prepped_suffix}/*.csv"
            p_files_to_del = glob(p_del_path)
            for f_del in p_files_to_del:
                os.remove(f_del)

            # save the combined csv-dataframe to a csv file
            step = f" save cmbcsvdate {cnt+1} "
            curr_cmb_hrs = "".join([str(t) for t in chunk])
            save_file =f"ecmwf_data_{start_date.strftime('%Y%m%d')}000000_{curr_cmb_hrs}h_oper_fc_{cnt+1}.csv"            
            cmb_file = ""
            s3c = None
            s3s = {}
            match push_destination: 
                case "local":
                    # sample: cmb_file = f"{prepped_path}/ecmwf_data_{start_date.strftime('%Y%m%d')}000000_{curr_cmb_hrs}h_oper_fc.csv"
                    cmb_file = f"{prepped_path}/{save_file}"
                    print(f"✅Saved for day {cnt+1}, the completely processed/prepped grib2-csv file as: {cmb_file}\n")
                    df_comb_csv.to_csv(cmb_file, index=False)
                case "s3":
                    # save dataframe as a csv file on AWS s3
                    print(f"Saving data on s3 as file: {save_file}")
                    logging.info(f"Saving data on s3 as file: {save_file}")
                    # get s3 client details
                    s3c, bucket_name = get_s3_client()
                    
                    key = f"{push_data_path}/{save_file}"
                    save_status = upload_dataframe_as_csv(df_comb_csv, 
                                                          bucket=bucket_name, 
                                                          key=key,
                                                          s3_client=s3c)
                    if save_status:
                        logging.info(f"✅Push to s3 - the completely processed/prepped grib2-csv file{save_file} for day {cnt+1} succeeded.")
                        uploaded_file_list.append(save_file)
                    else:
                        logging.warning(f"❌Push to s3 - the completely processed/prepped grib2-csv file{save_file} for day {cnt+1} failed.")
                case _: # default
                    # save to local as default                    
                    cmb_file = f"{prepped_path}/{save_file}"
                    df_comb_csv.to_csv(cmb_file, index=False)
                    logging.info(f"✅Saved for day {cnt+1}, the completely processed/prepped grib2-csv file as: {cmb_file}\n")
            
            # status
            dp_status = True

            # update setup bwfore proceeding with next in the while loop...
            step = " next in while loop "            
            cnt += 1
        
        if push_destination == "s3":
            print(f"\nSaved csv file on s3 are: {','.join(uploaded_file_list)}")
            logging.info(f"\nSaved csv file on s3 are: {','.join(uploaded_file_list)}")
    except Exception as ex:
        logging.error(f"Error with exception: {ex} at step: {step}")
    return dp_status
