import os
import boto3
import pickle
import yaml
import json
import pandas as pd 
import logging
from io import StringIO, BytesIO

from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
# ******************************************************************************************

def get_s3_settings(src="yaml", yaml_file="gribcfg.yaml"):
    s3_settings = {}
    data = None

    print(f"Source of access data: {src}")
    if src=="env":
        s3_region = "us-east-2"
        # bucket_name --> this would also be set in env, especially we have access to only one bucket
        bucket_name = "bhutan-climatesense" 
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        s3_settings = {
            "bucket_name": bucket_name,
            "s3_region": s3_region,
            "AWS_ACCESS_KEY_ID": aws_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret        
        }
    else:
        # load config for coords
        with open(yaml_file, 'r') as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        
        if data:
            s3_settings = data["s3"]
    
    return s3_settings

def connect_to_s3_resource(s3_settings=None, yaml_file=""):
    if s3_settings is None:
        s3_settings = get_s3_settings(yaml_file=yaml_file)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_settings["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=s3_settings['AWS_SECRET_ACCESS_KEY'],
        region_name=s3_settings['s3_region']  # e.g., 'us-east-1'
    )
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=s3_settings["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=s3_settings['AWS_SECRET_ACCESS_KEY'],
        region_name=s3_settings['s3_region']  # e.g., 'us-east-1'
    )
    return s3_client, s3_resource, s3_settings


def verify_object_exists(bucket: str=None, key: str=None, s3_client=None) -> bool:
    """Verify object exists and return True (calls head_object)."""
    verify_status = False
    try:
        # NOTE: key is filename here...
        s3_client.head_object(Bucket=bucket, Key=key)
        logging.info(f"Verified: s3://{bucket}/{key} exists and is accessible.")
        verify_status = True
    except ClientError as e:
        # 404 or AccessDenied will raise ClientError
        logging.error(f"head_object failed: {e}")
    return verify_status

def list_bucket_objects(bucket:str="", s3_client=None) -> list:
    lst_objects = []
    try:
        response = s3_client.list_objects_v2(Bucket=bucket)

        if 'Contents' in response:
            for obj in response['Contents']:
                lst_objects.append(f"{obj['Key']}")
    except Exception as ex:
        logging.error(f"Error with exception: {ex}")
    return lst_objects
    
def remove_files_on_s3(file_list=None, bucket:str="", s3_client=None):
    status = False

    try:
        files_to_delete = [{"Key":fl} for fl in file_list]
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={'Objects': files_to_delete, 'Quiet': False}
        )
        print(f"Files deleted successfully from bucket '{bucket}'.")
        if 'Errors' in response:
            print("Errors encountered during deletion:")
            for error in response['Errors']:
                print(f"  Code: {error['Code']}, Key: {error['Key']}, Message: {error['Message']}")
        else:
            status = True
    except Exception as e:
        print(f"Error deleting files: {e}")
    return status


def upload_as_file(local_path: str="", bucket: str="", key: str="", s3_client=None):
    """Upload a local file to s3://{bucket}/{key} using multipart upload (robust)."""
    upl_f_status = False

    try:
        # NOTE: key is filename here...
        logging.info(f"Uploading local file {local_path} -> s3://{bucket}/{key}")
        s3_client.upload_file(local_path, bucket, key)
        logging.info("Upload complete.")
        upl_f_status = True
    except (ClientError, FileNotFoundError) as e:
        logging.error(f"Upload failed: {e}")
    return upl_f_status


def upload_dataframe_as_csv(df: pd.DataFrame, bucket: str, key: str, s3_client=None):
    """Upload a DataFrame to S3 as CSV (in-memory)"""
    buf = StringIO()
    upl_d_status = False

    try:
        df.to_csv(buf, index=False)
        buf.seek(0)
        # NOTE: key is filename here...
        logging.info(f"Uploading DataFrame as CSV -> s3://{bucket}/{key}")
        # put_object is fine for reasonably sized CSVs
        s3_client.put_object(Body=buf.getvalue().encode("utf-8"), Bucket=bucket, Key=key)
        logging.info("CSV upload complete.")
        upl_d_status = True
    except ClientError as e:
        logging.error(f"CSV upload failed: {e}")
    return upl_d_status


def upload_dataframe_as_parquet(df: pd.DataFrame, bucket: str="", key: str="", s3_client=None):
    """Upload a DataFrame to S3 as Parquet (in-memory). Requires pyarrow or fastparquet."""
    buf = BytesIO()
    upl_p_status = False
    upl_d_status = False

    try:
        # pandas will pick pyarrow or fastparquet if installed
        df.to_parquet(buf, index=False)
        upl_d_status = True
    except Exception as e:
        logging.error(f"Failed to convert DataFrame to parquet: {e}")
        return upl_d_status
    
    # If no error above, then proceed 
    buf.seek(0)
    try:
        # NOTE: key is filename here...
        logging.info(f"Uploading DataFrame as Parquet -> s3://{bucket}/{key}")
        # Use upload_fileobj for streaming bytes
        s3_client.upload_fileobj(buf, bucket, key)
        logging.info("Parquet upload complete.")
        upl_p_status = True
    except ClientError as e:
        logging.error(f"Parquet upload failed: {e}")
    return upl_p_status


def download_file(file_type:str="csv", download_path:str="",
                   bucket:str="", key:str="", s3_client=None):
    dwl_f_status= False

    try:
        match file_type:            
            case "csv" | "parquet" | "pickle":
                s3_client.download_file(bucket, key, download_path)
                print(f"{file_type.capitalize()} file '{key}' downloaded to '{download_path}' successfully.")
                dwl_f_status = True
            case _:
                s3_client.download_file(bucket, key, download_path)
                print(f"{file_type.capitalize()} file '{key}' downloaded to '{download_path}' successfully.")
                dwl_f_status = True
    except FileNotFoundError:
        print(f"Error: File '{key}' not found in bucket '{bucket}'. Download failed.")
    except Exception as e:
        print(f"Error downloading {file_type} file: {e}")
    return dwl_f_status


# def download_model(file_type:str="pkl", bucket:str="", key:str="", s3_client=None):
#     # pickle_data = None
#     pickle_obj = None
#     dwl_f_status= False

#     try:
#         match file_type:
#             case "pkl":
#                 # response = s3_client.get_object(Bucket=bucket, Key=key)
#                 # body_data = response["Body"].read()
#                 # if body_data:
#                 #     pickle_data = pickle.loads(body_data)
#                 #     # save pickle file locally
                
#                 # create an in-memory binary stream to store the downloaded file
#                 bytes_stream = BytesIO()

#                 # download object from s3 to io-stream buffer
#                 s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=bytes_stream)

#                 # seek to the beginning of the io-stream before loading with pickle
#                 bytes_stream.seek(0)

#                 # load pickled object from the io-stream
#                 pickle_obj = pickle.load(bytes_stream)
#                 dwl_f_status = True
            
#     except FileNotFoundError:
#         print(f"Error: File '{key}' not found in bucket '{bucket}'. Download failed.")
#     except Exception as e:
#         print(f"Error downloading {file_type} file: {e}")
#     return dwl_f_status, pickle_obj
