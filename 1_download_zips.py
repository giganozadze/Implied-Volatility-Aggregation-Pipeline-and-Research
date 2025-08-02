#!/usr/bin/env python3
"""
SpiderRock AWS Data Download Script - SANITIZED VERSION

This script downloads ZIP files from SpiderRock AWS S3 bucket.
Before using this script:
1. Set up your AWS credentials in .env file
2. Update config.yaml with your bucket names and paths
3. Ensure you have proper AWS permissions

Usage:
    python 1_download_zips.py [latest_date]
"""

import boto3
import os
import yaml
from pathlib import Path
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
from tqdm import tqdm
import logging
import argparse
from datetime import datetime
import re

parser = argparse.ArgumentParser()
parser.add_argument('latest_date', nargs='?', help='Latest local zip date in YYYY-MM-DD')
args = parser.parse_args()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

def load_config():
    """Load configuration from config.yaml."""
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Replace "TODAY" with current date
    from datetime import date
    today = date.today().strftime('%Y-%m-%d')
    
    # Update end_date in datasets if it's set to "TODAY"
    for dataset_key, dataset_info in config['datasets'].items():
        if dataset_key != 'process_list' and isinstance(dataset_info, dict):
            if dataset_info.get('end_date') == 'TODAY':
                dataset_info['end_date'] = today
                print(f"Updated {dataset_key} end_date to: {today}")
    
    return config

# Load configuration
config = load_config()

def get_datasets_from_config(config):
    """Build datasets dictionary from config."""
    datasets = {}
    for dataset_key, dataset_info in config['datasets'].items():
        if dataset_key == 'process_list':
            continue
        if dataset_info.get('enabled', True):
            datasets[dataset_key] = {
                'name': dataset_info['name'],
                'bucket': dataset_info['bucket'],
                'path': dataset_info['path'],
                'local_path': config['paths']['options']['zip_input'],
                'download_mode': dataset_info.get('download_mode', 'most_recent'),
                'start_date': dataset_info.get('start_date'),
                'end_date': dataset_info.get('end_date')
            }
    return datasets

DATASETS = get_datasets_from_config(config)

# Get credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

if not aws_access_key_id or not aws_secret_access_key:
    raise ValueError("AWS credentials not found in .env file. Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")

def verify_aws_credentials():
    """Verify AWS credentials by attempting to list buckets."""
    try:
        print("Verifying AWS credentials...")
        s3 = boto3.client('s3',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)
        s3.list_buckets()
        print("AWS credentials verified successfully!")
        return True
    except (NoCredentialsError, ClientError) as e:
        print(f"Error verifying AWS credentials: {e}")
        return False

def format_size(size_bytes):
    """Format file size in human readable format."""
    if size_bytes == 0:
        return "0B"
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.1f}{size_names[i]}"

def list_available_files(s3_client, bucket, folder, dataset):
    """List available files in S3 bucket for a specific dataset."""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=folder)
        
        files = []
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.zip'):
                        filename = obj['Key'].split('/')[-1]
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
                        if date_match:
                            file_date = datetime.strptime(date_match.group(1), '%Y-%m-%d').date()
                            files.append({
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'date': file_date,
                                'filename': filename
                            })
        return files
    except ClientError as e:
        logger.error(f"Error listing files in bucket {bucket}: {e}")
        return []

def download_file(s3_client, bucket, file_info, dataset):
    """Download a single file from S3."""
    local_path = Path(dataset['local_path'])
    local_path.mkdir(parents=True, exist_ok=True)
    
    local_file = local_path / file_info['filename']
    
    if local_file.exists():
        logger.info(f"File already exists: {local_file}")
        return True
    
    try:
        logger.info(f"Downloading {file_info['filename']} ({format_size(file_info['size'])})")
        
        with tqdm(total=file_info['size'], unit='B', unit_scale=True, desc=file_info['filename']) as pbar:
            def callback(bytes_transferred):
                pbar.update(bytes_transferred - pbar.n)
            
            s3_client.download_file(
                bucket, 
                file_info['key'], 
                str(local_file),
                Callback=callback
            )
        
        logger.info(f"Successfully downloaded {file_info['filename']}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {file_info['filename']}: {e}")
        return False

def process_dataset(config, dataset_key, dataset_info):
    """Process a single dataset."""
    logger.info(f"Processing dataset: {dataset_info['name']}")
    
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    
    files = list_available_files(s3, dataset_info['bucket'], dataset_info['path'], dataset_info)
    
    if not files:
        logger.warning(f"No files found for dataset {dataset_key}")
        return
    
    # Filter files based on date range if specified
    if dataset_info.get('start_date') and dataset_info.get('end_date'):
        start_date = datetime.strptime(dataset_info['start_date'], '%Y-%m-%d').date()
        end_date = datetime.strptime(dataset_info['end_date'], '%Y-%m-%d').date()
        files = [f for f in files if start_date <= f['date'] <= end_date]
        logger.info(f"Filtered to {len(files)} files in date range {start_date} to {end_date}")
    
    successful_downloads = 0
    total_size = sum(f['size'] for f in files)
    
    logger.info(f"Found {len(files)} files to download (total size: {format_size(total_size)})")
    
    for file_info in files:
        if download_file(s3, dataset_info['bucket'], file_info, dataset_info):
            successful_downloads += 1
    
    logger.info(f"Successfully downloaded {successful_downloads}/{len(files)} files")

def main():
    """Main function."""
    print("=== SpiderRock AWS Data Download Script ===")
    
    if not verify_aws_credentials():
        print("Failed to verify AWS credentials. Please check your .env file.")
        return
    
    for dataset_key, dataset_info in DATASETS.items():
        try:
            process_dataset(config, dataset_key, dataset_info)
        except Exception as e:
            logger.error(f"Error processing dataset {dataset_key}: {e}")
    
    print("Download process completed!")

if __name__ == "__main__":
    main() 