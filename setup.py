#!/usr/bin/env python3
"""
Setup script for SpiderRock Options Data Pipeline

This script helps users configure the repository for their environment.
"""

import os
import shutil
from pathlib import Path
import yaml

def create_directories():
    """Create necessary directories."""
    directories = [
        'surfacecurvehist/zips',
        'surfacecurvehist/zarr',
        'output',
        'data/stock_data/zarr'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {directory}")

def setup_env_file():
    """Create .env file from template."""
    if not os.path.exists('.env'):
        if os.path.exists('env_template.txt'):
            shutil.copy('env_template.txt', '.env')
            print("Created .env file from template")
            print("Please edit .env file with your AWS credentials")
        else:
            print("Warning: env_template.txt not found")
    else:
        print(".env file already exists")

def validate_config():
    """Validate config.yaml file."""
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Check for placeholder values
        issues = []
        
        if 'YOUR_AWS_BUCKET_NAME' in str(config):
            issues.append("AWS bucket name not configured")
        
        if 'YOUR_GCS_BUCKET_NAME' in str(config):
            issues.append("GCS bucket name not configured")
        
        if issues:
            print("Configuration issues found:")
            for issue in issues:
                print(f"  - {issue}")
            print("\nPlease update config.yaml with your actual values")
        else:
            print("Configuration looks good!")
            
    except Exception as e:
        print(f"Error reading config.yaml: {e}")

def check_dependencies():
    """Check if required packages are installed."""
    required_packages = [
        'boto3',
        'xarray',
        'pandas',
        'numpy',
        'zarr',
        'streamlit',
        'plotly',
        'yfinance',
        'tqdm',
        'python-dotenv',
        'pyyaml',
        'gcsfs'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("Missing packages:")
        for package in missing_packages:
            print(f"  - {package}")
        print("\nRun: pip install -r requirements.txt")
    else:
        print("All required packages are installed!")

def main():
    """Main setup function."""
    print("=== SpiderRock Options Data Pipeline Setup ===\n")
    
    print("1. Creating directories...")
    create_directories()
    
    print("\n2. Setting up environment file...")
    setup_env_file()
    
    print("\n3. Validating configuration...")
    validate_config()
    
    print("\n4. Checking dependencies...")
    check_dependencies()
    
    print("\n=== Setup Complete ===")
    print("\nNext steps:")
    print("1. Edit .env file with your AWS credentials")
    print("2. Update config.yaml with your bucket names")
    print("3. Add your 1 year of ZIP files to surfacecurvehist/zips/")
    print("4. Run: python run_full_pipeline.py")

if __name__ == "__main__":
    main() 