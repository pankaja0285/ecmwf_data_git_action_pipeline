#!/bin/bash
pip install --upgrade pip
pip install -r requirements.txt

# ecmwf data processing main kick-off
# local
python main_ecmwf_data_pipeline.py --download_path="download" --prepped_path="prepped" \
--prepped_suffix="temp" \
--filter_levels="surface, heightAboveGround" --number_of_days='2' --step_counter=6 \
--push_destination="s3" --push_data_path="ecmwfdata" --delete_s3_files_flag="Y"

