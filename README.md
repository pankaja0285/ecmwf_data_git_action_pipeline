# ECMWF Data Pipeline Automation

This repository automates the process of downloading ECMWF data at regular intervals, converting it to CSV, and uploading it to AWS S3 using GitHub Actions.

## Overview
This pipeline consists of three main components:
1. **Python Script (`main_ecmwf_data_pipeline.py`)**
   - Handles downloading ECMWF data, converting GRIB files to CSV, and uploading to S3.
   - Should be placed at the root of your repository.
   - Downloads data to the GitHub Actions runner's temporary directory (`RUNNER_TEMP` or `/tmp`).
   - Converts GRIB to CSV using `cfgrib`, `xarray`, and `pandas`.
   - Uploads CSV to S3 using `boto3`.
2. **Requirements File (`requirements.txt`)**
   - Lists all necessary Python packages:
     - `ecmwf-opendata`
     - `cfgrib`
     - `xarray`
     - `pandas`
     - `boto3`
     .... 
   - Use your own `requirements.txt` as needed.
3. **GitHub Actions Workflow (`.github/workflows/ecmwf_data_pipeline.yml`)**
   - Schedules and runs the pipeline automatically (default: daily at 00:05 UTC).
   - Can also be triggered manually from the GitHub Actions tab.
   - Installs system dependencies (`libeccodes-tools`, `libeccodes-dev`) and Python packages.
   - Passes AWS credentials and S3 bucket name securely via repository secrets.

## Example Workflow File
```yaml
name: ECMWF Data Pipeline to S3
on:
  schedule:
    - cron: '5 0 * * *' # Runs at 00:05 UTC every day
  workflow_dispatch:
jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.11'
    - name: Install dependencies
      run: |
        sudo apt-get update
        sudo apt-get install -y libeccodes-tools libeccodes-dev
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    - name: Run Python script
      env:
        AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
        AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        S3_BUCKET_NAME: ${{ secrets.S3_BUCKET_NAME }}
        TEMP_DIR: '/tmp'
      # run: python main_ecmwf_data_pipeline.py
      # Instead use the shell script below, so we can pass params -->
      run: |
        chmod +x ./ecmwf_data_refresh_on_s3.sh
        ./ecmwf_data_refresh_on_s3.sh
```

## Secrets Setup
Add the following secrets in your GitHub repository under `Settings > Secrets and variables > Actions`:
- `AWS_ACCESS_KEY_ID`: Your AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key
- `S3_BUCKET_NAME`: The name of your S3 bucket
- `AWS_REGION`: The AWS region for your S3 bucket (e.g., `us-east-1`)

## File Locations
- **ECMWF file download location:**
  - Files are downloaded to a path constructed as `os.path.join(f"{TEMP_DIR}{download_path}", grib_filename)`, where `TEMP_DIR` is set from the environment variable (default `/tmp`).
- **`main_ecmwf_data_pipeline.py` location:**
  - Place at the root of your repository for direct access by the workflow.

## Notes
- The workflow checks out the repository and runs the pipeline script directly.
- Temporary files are cleaned up after each job completes.
- All necessary Python code and requirements are already available in this repository.
- The workflow can be scheduled or manually triggered for testing.

---
