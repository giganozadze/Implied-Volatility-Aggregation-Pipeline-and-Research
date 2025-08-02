# Quick Setup Instructions

## 1. Initial Setup

Run the setup script to create directories and check dependencies:
```bash
python setup.py
```

## 2. Configure Credentials

1. Edit `.env` file with your AWS credentials:
   ```
   AWS_ACCESS_KEY_ID=your_actual_access_key
   AWS_SECRET_ACCESS_KEY=your_actual_secret_key
   ```

2. Update `config.yaml` with your bucket names:
   - Replace `YOUR_AWS_BUCKET_NAME` with your SpiderRock AWS bucket
   - Replace `YOUR_GCS_BUCKET_NAME` with your GCS bucket (optional)

## 3. Add Your Data

Place your 1 year of SpiderRock ZIP files in:
```
surfacecurvehist/zips/
```

## 4. Run the Pipeline

Execute the complete pipeline:
```bash
python run_full_pipeline.py
```

## 5. View Results

Launch the Streamlit dashboard:
```bash
streamlit run 5_streamlit.py
```

## File Structure After Setup

```
sanitized_repo/
├── surfacecurvehist/
│   ├── zips/           # Your ZIP files here
│   └── zarr/           # Generated Zarr files
├── output/              # Processed IV data
├── data/                # Additional data files
├── .env                 # Your credentials (not in repo)
└── [pipeline scripts]   # All processing scripts
```

## Troubleshooting

- **AWS Credentials Error**: Check `.env` file and AWS permissions
- **Memory Issues**: Reduce batch sizes in config
- **Missing Data**: Ensure ZIP files are in correct directory
- **Path Errors**: Run `python setup.py` to create directories

## Security Notes

- Never commit `.env` file to version control
- Use minimal AWS permissions
- Rotate credentials regularly
- This repo is for personal use only 