#!/usr/bin/env python3
"""
Memory-efficient surface curve history ZIP to Zarr converter - SANITIZED VERSION

This script converts ZIP files in the surfacecurvehist/zips directory into a single
Zarr dataset with a flat structure.

Key features:
- Processes files in small batches to manage memory
- Uses flat dataset structure during processing
- Properly handles existing zarr structure where teo, ticker, years are data variables
- Uses efficient memory management and garbage collection
- Handles large datasets without memory issues
- Only processes new ZIP files that contain dates not already in the zarr store
"""

import os
import pandas as pd
import numpy as np
import xarray as xr
import zarr
from zipfile import ZipFile
from pathlib import Path
import logging
from tqdm import tqdm
import gc
import time
import re
from datetime import datetime
from numcodecs import Blosc
import warnings
import yaml
warnings.filterwarnings("ignore", message=".*vlen-utf8.*")

def load_config():
    """Load configuration from config.yaml."""
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config

# Load configuration
config = load_config()

# Configuration - SANITIZED VERSION
INPUT_DIR = config['paths']['options']['zip_input']
OUTPUT_DIR = config['paths']['options']['zarr_output']
OUTPUT_ZARR = os.path.join(OUTPUT_DIR, "surface_curve_history_combined_data_vars.zarr")

# Data columns to keep (matching config.yaml)
KEEP_COLUMNS = ["ticker_tk", "tradingDate", "rate", "years", "atmVol", "atmCen", "atmVega", "slope", "cCnt", "pCnt", "vwidth"]

# Column mapping for final dataset
COLUMN_MAPPING = {
    "tradingDate": "teo",
    "ticker_tk": "ticker"
}

# Processing parameters - much smaller batches to manage memory
BATCH_SIZE = 10  # Process this many files before writing to zarr
CHUNK_SIZE = 5000  # Process this many rows at a time within each file
MAX_MEMORY_ROWS = 100000  # Maximum rows to keep in memory at once

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def extract_date_from_filename(filename):
    """Extract date from ZIP filename."""
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        return pd.to_datetime(match.group(1))
    return None

def get_column_names(header_line):
    """Extract column names from header line."""
    return header_line.strip().split('\t')

def process_values(line):
    """Process a line of tab-separated values."""
    return line.strip().split('\t')

def infer_column_types(df, date_columns={'tradingDate'}, string_columns={'ticker_tk'}):
    """Infer and set proper data types for columns."""
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # Convert numeric columns
    numeric_cols = ['rate', 'years', 'atmVol', 'atmCen', 'atmVega', 'slope', 'cCnt', 'pCnt', 'vwidth']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def process_zip_file(zip_path):
    """Process a single ZIP file and return DataFrame."""
    try:
        with ZipFile(zip_path) as zf:
            txt_files = [f for f in zf.filelist if f.filename.endswith('.txt')]
            if not txt_files:
                return None
            
            # Read the first txt file
            txt_file = txt_files[0]
            with zf.open(txt_file) as f:
                lines = f.readlines()
            
            if len(lines) < 2:
                return None
            
            # Get column names from header
            header_line = lines[0].decode('utf-8')
            columns = get_column_names(header_line)
            
            # Process data lines
            data = []
            for line in lines[1:]:
                values = process_values(line.decode('utf-8'))
                if len(values) == len(columns):
                    data.append(values)
            
            if not data:
                return None
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # Keep only specified columns
            available_columns = [col for col in KEEP_COLUMNS if col in df.columns]
            df = df[available_columns]
            
            # Infer and set data types
            df = infer_column_types(df)
            
            return df
            
    except Exception as e:
        logger.error(f"Error processing {zip_path}: {e}")
        return None

def save_flat_dataset_to_zarr(dataframes, output_path):
    """Save a list of DataFrames to a flat zarr dataset."""
    if not dataframes:
        return
    
    # Combine all DataFrames
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Rename columns according to mapping
    for old_name, new_name in COLUMN_MAPPING.items():
        if old_name in combined_df.columns:
            combined_df = combined_df.rename(columns={old_name: new_name})
    
    # Convert to xarray Dataset
    ds = combined_df.to_xarray()
    
    # Save to zarr with compression
    ds.to_zarr(output_path, mode='w', consolidated=True, 
               encoding={var: {'compressor': Blosc(cname='zstd', clevel=3, shuffle=Blosc.SHUFFLE)} 
                        for var in ds.data_vars})
    
    logger.info(f"Saved {len(combined_df)} rows to {output_path}")

def get_existing_zarr_dates(zarr_path):
    """Get dates already present in the zarr store."""
    if not os.path.exists(zarr_path):
        return set()
    
    try:
        ds = xr.open_zarr(zarr_path)
        
        # Try to get dates from the dataset
        if 'teo' in ds.data_vars:
            dates = pd.to_datetime(ds['teo'].values)
            return set(dates.date())
        elif 'tradingDate' in ds.data_vars:
            dates = pd.to_datetime(ds['tradingDate'].values)
            return set(dates.date())
        else:
            logger.warning("Could not find date column in existing zarr")
            return set()
            
    except Exception as e:
        logger.warning(f"Error reading existing zarr: {e}")
        return set()

def get_new_zip_files(existing_zarr_dates):
    """Get ZIP files that contain dates not in the existing zarr."""
    input_path = Path(INPUT_DIR)
    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_path}")
        return []
    
    zip_files = list(input_path.glob('*.zip'))
    new_files = []
    
    for zip_file in zip_files:
        file_date = extract_date_from_filename(zip_file.name)
        if file_date and file_date.date() not in existing_zarr_dates:
            new_files.append(zip_file)
    
    return sorted(new_files)

def append_to_existing_zarr_fixed(new_dataframes, existing_zarr_path):
    """Append new data to existing zarr store."""
    if not new_dataframes:
        return
    
    # Combine new DataFrames
    new_combined_df = pd.concat(new_dataframes, ignore_index=True)
    
    # Rename columns according to mapping
    for old_name, new_name in COLUMN_MAPPING.items():
        if old_name in new_combined_df.columns:
            new_combined_df = new_combined_df.rename(columns={old_name: new_name})
    
    # Load existing data
    existing_ds = xr.open_zarr(existing_zarr_path)
    existing_df = existing_ds.to_dataframe().reset_index()
    
    # Combine with new data
    combined_df = pd.concat([existing_df, new_combined_df], ignore_index=True)
    
    # Remove duplicates if any
    combined_df = combined_df.drop_duplicates()
    
    # Convert back to xarray
    new_ds = combined_df.to_xarray()
    
    # Save with compression
    new_ds.to_zarr(existing_zarr_path, mode='w', consolidated=True,
                   encoding={var: {'compressor': Blosc(cname='zstd', clevel=3, shuffle=Blosc.SHUFFLE)} 
                            for var in new_ds.data_vars})
    
    logger.info(f"Appended {len(new_combined_df)} new rows to existing zarr")

def main():
    """Main processing function."""
    logger.info("Starting ZIP to Zarr conversion process")
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Get existing zarr dates
    existing_dates = get_existing_zarr_dates(OUTPUT_ZARR)
    logger.info(f"Found {len(existing_dates)} existing dates in zarr")
    
    # Get new ZIP files
    new_zip_files = get_new_zip_files(existing_dates)
    logger.info(f"Found {len(new_zip_files)} new ZIP files to process")
    
    if not new_zip_files:
        logger.info("No new files to process")
        return
    
    # Process files in batches
    all_dataframes = []
    batch_count = 0
    
    for i in tqdm(range(0, len(new_zip_files), BATCH_SIZE), desc="Processing batches"):
        batch_files = new_zip_files[i:i + BATCH_SIZE]
        batch_dataframes = []
        
        for zip_file in tqdm(batch_files, desc=f"Batch {batch_count + 1}"):
            df = process_zip_file(zip_file)
            if df is not None:
                batch_dataframes.append(df)
        
        if batch_dataframes:
            all_dataframes.extend(batch_dataframes)
        
        batch_count += 1
        
        # Clear memory
        gc.collect()
    
    # Save or append to zarr
    if os.path.exists(OUTPUT_ZARR):
        logger.info("Appending to existing zarr store")
        append_to_existing_zarr_fixed(all_dataframes, OUTPUT_ZARR)
    else:
        logger.info("Creating new zarr store")
        save_flat_dataset_to_zarr(all_dataframes, OUTPUT_ZARR)
    
    logger.info("ZIP to Zarr conversion completed!")

def get_sorted_zip_files():
    """Get sorted list of ZIP files in input directory."""
    input_path = Path(INPUT_DIR)
    if not input_path.exists():
        return []
    
    zip_files = list(input_path.glob('*.zip'))
    return sorted(zip_files)

if __name__ == "__main__":

    main() 
