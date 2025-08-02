#!/usr/bin/env python3
"""
Google Cloud Platform Upload Script for IV Data Pipeline

This script uploads the processed zarr store to Google Cloud Platform.
It replaces the existing GCP store with the updated local store.

Usage:
    python 2b_upload_to_gc.py

Dependencies:
    - gsutil (Google Cloud SDK)
    - gcsfs: For GCP filesystem operations
    - xarray: For data verification
"""

import subprocess
import logging
from pathlib import Path
import yaml
import gcsfs
import xarray as xr

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from config.yaml."""
    config_path = Path('config.yaml')
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found at {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

def get_local_zarr_path():
    """Get the local zarr path for the surface curve history dataset."""
    return Path("surfacecurvehist/surface_curve_history_combined.zarr")

def get_aggregated_iv_zarr_path():
    """Get the local zarr path for the aggregated IV cross totals dataset."""
    return Path("output/aggregated_iv_cross_with_totals.zarr")

def upload_surface_curve_to_gcp():
    """Upload the surface curve history zarr store to GCP."""
    try:
        config = load_config()
        
        # Get paths
        local_zarr_path = get_local_zarr_path()
        bucket_name = config['gcs']['output']['bucket_name']
        gcp_zarr_path = f"{config['gcs']['output']['base_path']}/surface_curve_history_combined.zarr"
        gcp_url = f"gs://{bucket_name}/{gcp_zarr_path}"
        
        # Check if local zarr exists
        if not local_zarr_path.exists():
            logger.error(f"Local zarr file not found at {local_zarr_path}")
            logger.info("Please run 2a_zips_to_zarrs_combined.py first to create the local zarr file.")
            return False
        
        logger.info(f"Local zarr path: {local_zarr_path}")
        logger.info(f"GCP destination: {gcp_url}")
        
        # Check if GCP zarr exists
        gcs = gcsfs.GCSFileSystem()
        try:
            gcs.ls(gcp_url)
            gcp_exists = True
            logger.info("Found existing zarr store in GCP - will replace it")
        except (FileNotFoundError, KeyError):
            gcp_exists = False
            logger.info("No existing zarr store found in GCP - will create new one")
        
        # Use gsutil to sync the local zarr to GCP (this will replace existing)
        cmd = f"gsutil -m rsync -r {local_zarr_path} {gcp_url}"
        logger.info(f"Running: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error during gsutil rsync: {result.stderr}")
            logger.error(f"Command output: {result.stdout}")
            raise RuntimeError("Failed to sync zarr store to GCP")
        
        logger.info("Successfully synced surface curve history zarr store to GCP")
        
        # Verify the upload
        try:
            gcp_ds = xr.open_zarr(gcp_url, storage_options={'token': gcs})
            logger.info(f"GCP dataset shape: {gcp_ds.dims}")
            logger.info(f"GCP dataset variables: {list(gcp_ds.data_vars.keys())}")
            logger.info(f"GCP dataset coordinates: {list(gcp_ds.coords.keys())}")
        except Exception as e:
            logger.warning(f"Could not verify GCP dataset: {e}")
        
        return True
            
    except Exception as e:
        logger.error(f"Error uploading surface curve history to GCP: {str(e)}")
        return False

def upload_aggregated_iv_to_gcp():
    """Upload the aggregated IV cross totals zarr store to GCP."""
    try:
        config = load_config()
        
        # Get paths
        local_zarr_path = get_aggregated_iv_zarr_path()
        bucket_name = config['gcs']['output']['bucket_name']
        gcp_zarr_path = f"{config['gcs']['output']['base_path']}/aggregated_iv_cross_with_totals.zarr"
        gcp_url = f"gs://{bucket_name}/{gcp_zarr_path}"
        
        # Check if local zarr exists
        if not local_zarr_path.exists():
            logger.error(f"Local aggregated IV zarr file not found at {local_zarr_path}")
            logger.info("Please run 3_produce_iv_dataset_full.py first to create the aggregated IV zarr file.")
            return False
        
        logger.info(f"Local aggregated IV zarr path: {local_zarr_path}")
        logger.info(f"GCP destination: {gcp_url}")
        
        # Check if GCP zarr exists
        gcs = gcsfs.GCSFileSystem()
        try:
            gcs.ls(gcp_url)
            gcp_exists = True
            logger.info("Found existing aggregated IV zarr store in GCP - will replace it")
        except (FileNotFoundError, KeyError):
            gcp_exists = False
            logger.info("No existing aggregated IV zarr store found in GCP - will create new one")
        
        # Use gsutil to sync the local zarr to GCP (this will replace existing)
        cmd = f"gsutil -m rsync -r {local_zarr_path} {gcp_url}"
        logger.info(f"Running: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error during gsutil rsync: {result.stderr}")
            logger.error(f"Command output: {result.stdout}")
            raise RuntimeError("Failed to sync aggregated IV zarr store to GCP")
        
        logger.info("Successfully synced aggregated IV zarr store to GCP")
        
        # Verify the upload
        try:
            gcp_ds = xr.open_zarr(gcp_url, storage_options={'token': gcs})
            logger.info(f"GCP aggregated IV dataset shape: {gcp_ds.dims}")
            logger.info(f"GCP aggregated IV dataset variables: {list(gcp_ds.data_vars.keys())}")
            logger.info(f"GCP aggregated IV dataset coordinates: {list(gcp_ds.coords.keys())}")
        except Exception as e:
            logger.warning(f"Could not verify GCP aggregated IV dataset: {e}")
        
        return True
            
    except Exception as e:
        logger.error(f"Error uploading aggregated IV to GCP: {str(e)}")
        return False

def upload_to_gcp():
    """Upload both zarr stores to GCP."""
    logger.info("Uploading surface curve history dataset...")
    surface_success = upload_surface_curve_to_gcp()
    
    logger.info("Uploading aggregated IV cross totals dataset...")
    aggregated_success = upload_aggregated_iv_to_gcp()
    
    return surface_success and aggregated_success

def main():
    """Main processing function."""
    try:
        logger.info("=" * 60)
        logger.info("GCP UPLOAD SCRIPT")
        logger.info("=" * 60)
        
        if upload_to_gcp():
            logger.info("✅ Upload completed successfully!")
            return 0
        else:
            logger.error("❌ Upload failed!")
            return 1
            
    except Exception as e:
        logger.error(f"Error during upload: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main()) 