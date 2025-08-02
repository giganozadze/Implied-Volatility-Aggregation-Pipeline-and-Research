# SpiderRock Options Data Pipeline - Sanitized Repository

This repository contains a complete pipeline for downloading, processing, and analyzing SpiderRock options implied volatility data. The pipeline is designed to work with the last 1.5 year of data for personal use.

## Overview

This pipeline processes SpiderRock AWS S3 data through the following steps:

1. **Download ZIP files** from SpiderRock AWS S3 bucket
2. **Convert ZIP files to Zarr format** for efficient storage and processing
3. **Process and aggregate implied volatility data** with sector and size classifications
4. **Upload processed data** to Google Cloud Storage (optional)
5. **Visualize results** using Streamlit dashboard

## Prerequisites

- Python 3.8+
- AWS credentials with access to SpiderRock S3 bucket
- Google Cloud Storage credentials (optional, for upload functionality)
- Sufficient disk space for data processing (recommended: 50GB+)

## Installation

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up credentials:
   - Copy `env_template.txt` to `.env`
   - Update `.env` with your AWS credentials:
     ```
     AWS_ACCESS_KEY_ID=your_aws_access_key_id_here
     AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key_here
     ```

4. Update `config.yaml` with your specific paths and bucket names:
   - Replace `YOUR_AWS_BUCKET_NAME` with your SpiderRock AWS bucket
   - Replace `YOUR_GCS_BUCKET_NAME` with your Google Cloud Storage bucket (if using)
   - Update local paths as needed

## Directory Structure

```
sanitized_repo/
├── 1_download_zips.py              # Download ZIP files from AWS S3
├── 2_zips_to_zarrs_combined.py     # Convert ZIP files to Zarr format
├── 3_produce_iv_dataset_full.py    # Process and aggregate IV data
├── 4_upload_to_gc.py              # Upload to Google Cloud Storage
├── 5_streamlit.py                  # Streamlit dashboard
├── run_full_pipeline.py            # Orchestrate entire pipeline
├── fs_industry_to_gics_sector_map.py # Sector mapping utilities
├── config.yaml                     # Configuration file
├── requirements.txt                 # Python dependencies
├── env_template.txt                # Environment variables template
└── README.md                       # This file
```

## Usage

### Quick Start

Run the complete pipeline:
```bash
python run_full_pipeline.py
```

### Individual Steps

1. **Download data from AWS S3:**
   ```bash
   python 1_download_zips.py
   ```

2. **Convert ZIP files to Zarr:**
   ```bash
   python 2_zips_to_zarrs_combined.py
   ```

3. **Process and aggregate IV data:**
   ```bash
   python 3_produce_iv_dataset_full.py
   ```

4. **Upload to Google Cloud Storage (optional):**
   ```bash
   python 4_upload_to_gc.py
   ```

5. **Launch Streamlit dashboard:**
   ```bash
   streamlit run 5_streamlit.py
   ```

## Configuration

### config.yaml

The main configuration file contains:

- **Paths**: Local directory paths for data storage
- **AWS Settings**: S3 bucket names and data paths
- **GCS Settings**: Google Cloud Storage configuration
- **Data Processing**: Parameters for IV calculation and aggregation
- **Date Ranges**: Currently set to last 1.5 year of data

### Key Parameters

- `start_date`: "2024-01-01" (last 1.5 year)
- `end_date`: "TODAY" (automatically updated)
- `download_mode`: "date_range" (download specific date range)
- `keep_data_vars`: List of columns to retain from source data

## Data Processing Details

### Step 1: Download (1_download_zips.py)
- Downloads ZIP files from SpiderRock AWS S3
- Filters by date range (last 1.5 year)
- Supports incremental downloads (only new files)
- Progress tracking and error handling

### Step 2: ZIP to Zarr Conversion (2_zips_to_zarrs_combined.py)
- Converts tab-separated text files in ZIPs to Zarr format
- Memory-efficient processing with batching
- Handles large datasets without memory issues
- Incremental processing (only new dates)

### Step 3: IV Processing (3_produce_iv_dataset_full.py)
- Aggregates implied volatility data by:
  - GICS sectors (using FactSet industry mapping)
  - Market cap size categories
  - Investment styles (growth/value)
- Calculates weighted averages and totals
- Creates cross-sectional analysis datasets

### Step 4: Upload (4_upload_to_gc.py)
- Uploads processed Zarr datasets to Google Cloud Storage
- Supports both surface curve history and aggregated IV data
- Verification of upload integrity

### Step 5: Visualization (5_streamlit.py)
- Interactive dashboard for exploring IV data
- Time series plots with filtering options
- Sector and size category analysis
- Export functionality

## Data Schema

### Input Data (SpiderRock)
- `ticker_tk`: Stock ticker
- `tradingDate`: Trading date
- `rate`: Risk-free rate
- `years`: Time to expiration
- `atmVol`: At-the-money implied volatility
- `atmCen`: Censored implied volatility
- `atmVega`: Vega sensitivity
- `slope`: Volatility slope
- `cCnt`: Call option count
- `pCnt`: Put option count
- `vwidth`: Volatility width

### Output Data (Aggregated)
- `teo`: Trading date
- `ticker`: Stock ticker
- `gics_sector`: GICS sector classification
- `size_category`: Market cap size category
- `style`: Investment style (growth/value)
- `value_type`: Type of IV measure
- `expiry_label`: Expiration time bucket
- `weighted_value`: Weighted IV value
- `total_value`: Total IV value

## Troubleshooting

### Common Issues

1. **AWS Credentials Error**
   - Ensure `.env` file exists with correct credentials
   - Verify AWS permissions for S3 bucket access

2. **Memory Issues**
   - Reduce `BATCH_SIZE` in 2_zips_to_zarrs_combined.py
   - Process smaller date ranges

3. **Missing Dependencies**
   - Run `pip install -r requirements.txt`
   - Install additional packages as needed

4. **Path Issues**
   - Update paths in `config.yaml`
   - Ensure directories exist and are writable

### Performance Optimization

- Use SSD storage for better I/O performance
- Increase memory allocation for large datasets
- Consider using cloud compute for processing

## Security Notes

- **Never commit `.env` file** with real credentials
- Use IAM roles with minimal required permissions
- Regularly rotate AWS access keys
- Consider using AWS Secrets Manager for production

## Data Privacy

This repository is configured for personal use with:
- Last 1.5 year of data only
- Sanitized configuration files
- No sensitive credentials included
- Placeholder bucket names

## License

This repository is for personal use only. SpiderRock data is proprietary and subject to their terms of service.

## Support

For issues with:
- **SpiderRock data access**: Contact SpiderRock support
- **AWS credentials**: Check AWS IAM console
- **Pipeline processing**: Check logs and configuration
- **Streamlit dashboard**: Verify data files exist

## Contributing

This is a personal repository. For modifications:
1. Update configuration files as needed
2. Test with small data samples first
3. Ensure all paths are relative or configurable

4. Document any changes to data processing logic 
