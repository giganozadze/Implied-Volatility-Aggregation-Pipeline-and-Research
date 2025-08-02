#!/usr/bin/env python3
"""
Incremental IV (Implied Volatility) Data Pipeline Orchestrator

This script runs the complete pipeline in incremental mode:
1. Download new ZIP files (1_download_zips.py) - ONLY missing dates
2. Process new ZIPs to Zarr format and APPEND to existing combined zarr (2_zips_to_zarrs_combined.py) 
3. Download daily ticker dataset from GCS (gsutil command)
4. Produce IV dataset (3_produce_iv_dataset_full.py)
5. Upload to Google Cloud Platform (4_upload_to_gc.py)

The pipeline is optimized for efficiency:
- Only downloads files for dates not already in the zips directory
- Appends new data to existing combined zarr store
- Downloads latest daily ticker dataset for IV processing
- Produces updated IV dataset
- Efficiently uploads updated zarr to GCP

Usage:
    python run_update_iv.py [--dry-run] [--force] [--verbose]

Options:
    --dry-run    Show what would be done without actually running
    --force      Force reprocessing even if no new files detected
    --verbose    Enable detailed logging
"""

import subprocess
import sys
import logging
import argparse
import yaml
from pathlib import Path
import os
from datetime import datetime
import re
import glob
import pandas as pd

def setup_logging(verbose=False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def load_config():
    """Load configuration from config.yaml."""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    config_path = script_dir / 'config.yaml'
    
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found at {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

def extract_date_from_filename(filename):
    """Extract date from filename using various patterns."""
    base_name = filename.replace('.zip', '')
    
    patterns = [
        r'(\d{4}-\d{2}-\d{2})$',  # YYYY-MM-DD at end
        r'_(\d{4}-\d{2}-\d{2})$',  # _YYYY-MM-DD at end
        r'(\d{4}_\d{2}_\d{2})$',  # YYYY_MM_DD at end
        r'_(\d{4}_\d{2}_\d{2})$',  # _YYYY_MM_DD at end
    ]
    
    for pattern in patterns:
        match = re.search(pattern, base_name)
        if match:
            date_str = match.group(1).replace('_', '-')
            try:
                return datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                continue
    
    return None

def get_latest_zip_date(zip_dir):
    """Get the latest date from the ZIP filenames."""
    zip_path = Path(zip_dir)
    if not zip_path.exists():
        return None

    zip_files = list(zip_path.glob('*.zip'))
    dates = []

    for zip_file in zip_files:
        match = re.search(r'_(\d{4}-\d{2}-\d{2})$', zip_file.stem)
        if match:
            try:
                date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                dates.append(date)
            except ValueError:
                continue

    return max(dates) if dates else None


def get_latest_zarr_date(zarr_path):
    """Get the latest date from a Zarr store."""
    try:
        import xarray as xr
        
        zarr_path = Path(zarr_path)
        if not zarr_path.exists():
            return None
        
        ds = xr.open_zarr(str(zarr_path))
        
        # Try different possible time dimension names
        time_dims = []
        for dim in ds.dims:
            if any(time_name in dim.lower() for time_name in ['time', 'date', 'teo']):
                time_dims.append(dim)
        
        if not time_dims:
            return None
        
        # Use the first time dimension found
        time_dim = time_dims[0]
        time_data = ds[time_dim].values
        
        if len(time_data) == 0:
            return None
        
        # Convert to datetime and get the latest
        if hasattr(time_data, 'max'):
            latest_timestamp = time_data.max()
        else:
            latest_timestamp = max(time_data)
        
        # Convert numpy datetime64 to Python datetime if needed
        if hasattr(latest_timestamp, 'astype'):
            latest_timestamp = pd.to_datetime(latest_timestamp)
        
        return latest_timestamp
        
    except Exception as e:
        logging.getLogger(__name__).warning(f"Could not read Zarr date from {zarr_path}: {e}")
        return None

def run_script(script_name, logger, dry_run=False, cwd=None, extra_args=None):
    """Run a Python script and return success status."""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    script_path = script_dir / script_name
    
    if not script_path.exists():
        logger.error(f"Script {script_name} not found at {script_path}")
        return False
    
    logger.info(f"{'[DRY RUN] Would run' if dry_run else 'Running'}: {script_name}")
    
    if dry_run:
        return True
    
    try:
        # Use specified working directory or script directory
        working_dir = cwd if cwd else str(script_dir)
        
        # Build command with extra arguments if provided
        cmd = [sys.executable, str(script_path)]
        if extra_args:
            cmd.extend(extra_args)
        
        # Run the script in the same Python environment
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=working_dir)
        
        if result.returncode == 0:
            logger.info(f"✓ {script_name} completed successfully")
            if result.stdout:
                logger.debug(f"STDOUT: {result.stdout}")
            return True
        else:
            logger.error(f"✗ {script_name} failed with return code {result.returncode}")
            if result.stderr:
                logger.error(f"STDERR: {result.stderr}")
            if result.stdout:
                logger.error(f"STDOUT: {result.stdout}")
            return False
            
    except Exception as e:
        logger.error(f"✗ Error running {script_name}: {str(e)}")
        return False

def check_pipeline_status(config, logger):
    """Check the current status of the pipeline and determine what needs to be run."""
    status = {
        'needs_download': False,
        'needs_processing': False,
        'needs_iv_dataset': False,
        'needs_upload': False,
        'existing_zip_dates': set(),
        'latest_zarr_date': None,
        'zip_count': 0,
        'zarr_exists': False
    }
    
    # Check ZIP files
    zip_dir = r"C:\Users\gigan\OneDrive\Desktop\BlueWaterMacro\data\sr_options\data\surfacecurvehist\zips"
    latest_zip_date = get_latest_zip_date(zip_dir)
    zip_files = list(Path(zip_dir).glob('*.zip')) if Path(zip_dir).exists() else []
    
    status['latest_zip_date'] = latest_zip_date
    status['zip_count'] = len(list(Path(zip_dir).glob('*.zip')))
    
    # Check Zarr store
    zarr_path = r"C:\Users\gigan\OneDrive\Desktop\BlueWaterMacro\data\sr_options\data\surfacecurvehist\surface_curve_history_combined.zarr"
    zarr_exists = Path(zarr_path).exists()
    latest_zarr_date = get_latest_zarr_date(zarr_path) if zarr_exists else None
    
    status['zarr_exists'] = zarr_exists
    status['latest_zarr_date'] = latest_zarr_date
    
    # Determine what needs to be run
    # Always try to download new files (script will determine if any are actually new)
    status['needs_download'] = True
    
    # Need processing if:
    # 1. No Zarr store exists, OR
    # 2. We have ZIP files but no Zarr store, OR
    # 3. We have new ZIP files that aren't in the Zarr store
    if not zarr_exists:
        status['needs_processing'] = True
        logger.info("Processing needed: No Zarr store exists")
    elif zip_files:
        status['needs_processing'] = True
        logger.info("Processing needed: Have ZIP files to process")
    else:
        logger.info("Processing not needed: No new files to process")
    
    # Need IV dataset if we have a Zarr store (regardless of processing)
    status['needs_iv_dataset'] = zarr_exists
    
    # Need upload if we processed new data or have a Zarr store
    status['needs_upload'] = status['needs_processing'] or zarr_exists
    
    return status

def check_daily_ticker_dataset():
    """Check if the daily ticker dataset exists and is recent."""
    daily_ticker_path = Path('ds_daily_ticker.zarr')
    
    if not daily_ticker_path.exists():
        return False, "Dataset doesn't exist"
    
    # Check if the dataset is recent (less than 1 day old)
    try:
        # Get the modification time of the dataset
        mtime = daily_ticker_path.stat().st_mtime
        mtime_dt = datetime.fromtimestamp(mtime)
        age_hours = (datetime.now() - mtime_dt).total_seconds() / 3600
        
        if age_hours < 24:
            return True, f"Dataset is {age_hours:.1f} hours old"
        else:
            return False, f"Dataset is {age_hours:.1f} hours old (older than 24 hours)"
    except Exception as e:
        return False, f"Error checking dataset age: {e}"

def run_gsutil_command(cmd, logger, dry_run=False):
    """Run a gsutil command and return success status."""
    logger.info(f"{'[DRY RUN] Would run' if dry_run else 'Running'}: {cmd}")
    
    if dry_run:
        return True
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"✓ gsutil command completed successfully")
            if result.stdout:
                logger.debug(f"STDOUT: {result.stdout}")
            if result.stderr:
                logger.debug(f"STDERR: {result.stderr}")
            return True
        else:
            logger.error(f"✗ gsutil command failed with return code {result.returncode}")
            if result.stderr:
                logger.error(f"STDERR: {result.stderr}")
            if result.stdout:
                logger.error(f"STDOUT: {result.stdout}")
            return False
            
    except Exception as e:
        logger.error(f"✗ Error running gsutil command: {str(e)}")
        return False

def main():
    """Main pipeline orchestrator function."""
    parser = argparse.ArgumentParser(description='Run incremental IV data pipeline')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be done without actually running')
    parser.add_argument('--force', action='store_true',
                       help='Force reprocessing even if no new files detected')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable detailed logging')
    
    args = parser.parse_args()
    
    logger = setup_logging(args.verbose)
    
    try:
        # Load configuration
        config = load_config()
        logger.info("=" * 60)
        logger.info("INCREMENTAL IV DATA PIPELINE")
        logger.info("=" * 60)
        
        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - No actual changes will be made")
        
        # Check current pipeline status
        logger.info("📊 Checking pipeline status...")
        status = check_pipeline_status(config, logger)
        
        logger.info(f"ZIP files: {status['zip_count']} (latest date: {status['latest_zip_date']})")
        logger.info(f"Zarr store: {'exists' if status['zarr_exists'] else 'missing'} (latest: {status['latest_zarr_date'] if status['latest_zarr_date'] else 'None'})")
        
        # Check daily ticker dataset status
        daily_ticker_exists, daily_ticker_status = check_daily_ticker_dataset()
        logger.info(f"Daily ticker dataset: {daily_ticker_status}")
        
        if args.force:
            logger.info("🔄 FORCE MODE - Will run all steps regardless of status")
            status['needs_download'] = True
            status['needs_processing'] = True
            status['needs_iv_dataset'] = True
            status['needs_upload'] = True
        
        # Track overall success
        overall_success = True
        steps_run = 0
        
        # Step 1: Download new ZIP files
        if status['needs_download']:
            logger.info("\n📥 STEP 1: Downloading new ZIP files...")
            latest_date_str = status.get('latest_zip_date').isoformat() if status.get('latest_zip_date') else ''
            success = run_script('1_download_zips.py', logger, args.dry_run, cwd=None, extra_args=[latest_date_str])

            if not success:
                logger.error("❌ Download step failed - stopping pipeline")
                return 1
            steps_run += 1
        else:
            logger.info("\n⏭️  STEP 1: Skipping download - no new files expected")
        
        # Re-check status after download to see if we got new files
        if not args.dry_run and status['needs_download']:
            logger.info("🔄 Re-checking status after download...")
            new_status = check_pipeline_status(config, logger)
            if new_status['latest_zip_date'] != status['latest_zip_date']:
                logger.info(f"✓ New files downloaded! Latest ZIP date changed from {status['latest_zip_date']} to {new_status['latest_zip_date']}")
                status['needs_processing'] = True
            else:
                logger.info("ℹ️  No new files were downloaded")
                if not args.force:
                    status['needs_processing'] = False
        
        # Step 2: Process ZIP files to Zarr (append to existing)
        if status['needs_processing']:
            logger.info("\n⚙️  STEP 2: Processing ZIP files to Zarr format (appending to existing)...")
            success = run_script('2_zips_to_zarrs_combined.py', logger, args.dry_run)
            if not success:
                logger.error("❌ Processing step failed - stopping pipeline")
                return 1
            steps_run += 1
        else:
            logger.info("\n⏭️  STEP 2: Skipping processing - no new data to process")
        
        # Step 3: Download daily ticker dataset from GCS
        if status['needs_iv_dataset']:
            logger.info("\n📥 STEP 3: Downloading daily ticker dataset from GCS...")
            
            # Check if we need to download the dataset
            dataset_exists, dataset_status = check_daily_ticker_dataset()
            logger.info(f"Daily ticker dataset status: {dataset_status}")
            
            if dataset_exists and not args.force:
                logger.info("✓ Daily ticker dataset is recent - skipping download")
            else:
                # Download the dataset
                gsutil_cmd = "gsutil -m cp -r gs://rm_api_data/ds_daily_ticker.zarr ."
                success = run_gsutil_command(gsutil_cmd, logger, args.dry_run)
                if not success:
                    logger.error("❌ Daily ticker dataset download failed")
                    overall_success = False
                else:
                    steps_run += 1
        else:
            logger.info("\n⏭️  STEP 3: Skipping daily ticker dataset download - no IV processing needed")
        
        # Step 4: Produce IV dataset
        if status['needs_iv_dataset']:
            logger.info("\n📊 STEP 4: Producing IV dataset...")
            # Pass force argument if needed
            extra_args = []
            if args.force:
                extra_args.append('--force')
            if args.verbose:
                extra_args.append('--verbose')
            
            success = run_script('3_produce_iv_dataset_full.py', logger, args.dry_run, extra_args=extra_args)
            if not success:
                logger.error("❌ IV dataset production failed")
                overall_success = False
            else:
                steps_run += 1
        else:
            logger.info("\n⏭️  STEP 4: Skipping IV dataset production - no Zarr store available")
        
        # Step 5: Upload to GCP
        if status['needs_upload']:
            logger.info("\n☁️  STEP 5: Uploading to Google Cloud Platform...")
            success = run_script('4_upload_to_gc.py', logger, args.dry_run)
            if not success:
                logger.error("❌ Upload step failed")
                overall_success = False
            else:
                steps_run += 1
        else:
            logger.info("\n⏭️  STEP 5: Skipping upload - no new data to upload")
        
        # Summary
        logger.info("\n" + "=" * 60)
        if overall_success:
            if steps_run > 0:
                logger.info(f"✅ PIPELINE COMPLETED SUCCESSFULLY - {steps_run} steps executed")
            else:
                logger.info("✅ PIPELINE UP TO DATE - No steps needed")
        else:
            logger.error("❌ PIPELINE COMPLETED WITH ERRORS")
        logger.info("=" * 60)
        
        return 0 if overall_success else 1
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())