import os
import pandas as pd
import numpy as np
import xarray as xr
import yfinance as yf
import dask.dataframe as dd
from itertools import product
from fs_industry_to_gics_sector_map import fs_industry_to_gic_code, gic_code_to_gic_name
import argparse
import logging
from pathlib import Path

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def get_latest_date_in_zarr(zarr_path, time_dim='teo'):
    """Get the latest date from a Zarr store."""
    try:
        ds = xr.open_zarr(zarr_path)
        
        # Check if time_dim is a coordinate (dimension)
        if time_dim in ds.dims:
            time_data = ds[time_dim].values
            if len(time_data) > 0:
                latest_timestamp = pd.to_datetime(time_data.max())
                return latest_timestamp
        
        # Check if time_dim is a data variable
        if time_dim in ds.data_vars:
            time_data = ds[time_dim].values
            if len(time_data) > 0:
                latest_timestamp = pd.to_datetime(time_data.max())
                return latest_timestamp
        
        # For flat datasets, convert to dataframe and find max date
        if 'index' in ds.dims:
            df = ds.to_dataframe().reset_index()
            if time_dim in df.columns:
                time_data = pd.to_datetime(df[time_dim])
                if len(time_data) > 0:
                    latest_timestamp = time_data.max()
                    return latest_timestamp
        
        return None
    except Exception as e:
        logging.warning(f"Could not read latest date from {zarr_path}: {e}")
        return None

def check_incremental_processing_needed():
    """Check if incremental processing is needed by comparing latest dates."""
    logger = logging.getLogger(__name__)
    
    # Paths - SANITIZED VERSION
    input_zarr_path = 'surfacecurvehist/surface_curve_history_combined_data_vars.zarr'
    output_zarr_path = 'output/aggregated_iv_cross_with_totals_flat.zarr'
    
    # Check if output exists
    output_exists = Path(output_zarr_path).exists()
    
    if not output_exists:
        logger.info("Output dataset doesn't exist - will process all data")
        return True, None, None
    
    # Get latest dates
    input_latest = get_latest_date_in_zarr(input_zarr_path)
    output_latest = get_latest_date_in_zarr(output_zarr_path)
    
    logger.info(f"Input dataset latest date: {input_latest}")
    logger.info(f"Output dataset latest date: {output_latest}")
    
    if input_latest is None:
        logger.warning("Could not determine input dataset latest date - will process all data")
        return True, None, None
    
    if output_latest is None:
        logger.warning("Could not determine output dataset latest date - will process all data")
        return True, None, None
    
    # Check if input has newer data
    if input_latest > output_latest:
        logger.info(f"New data available: {input_latest} > {output_latest}")
        logger.info(f"Will process incremental data from {output_latest + pd.Timedelta(days=1)} to {input_latest}")
        return True, output_latest, input_latest
    else:
        logger.info("No new data to process - input and output datasets are up to date")
        return False, None, None

def load_and_filter_data(zarr_path, start_date=None):
    """Load and filter data from zarr, optionally starting from a specific date."""
    logger = logging.getLogger(__name__)
    
    # Load single combined zarr file
    ds = xr.open_zarr(zarr_path)
    
    # Load and apply liquidity filters
    desired_order = ['teo', 'ticker', 'years', 'atmCen', 'atmVol', 'atmVega', 'slope','cCnt','pCnt','vwidth']
    df_all = ds.to_dataframe().dropna().reset_index()[desired_order]

    # Filter by date if start_date is provided
    if start_date is not None:
        logger.info(f"Filtering data from {start_date} onwards")
        df_all = df_all[df_all['teo'] >= start_date]

    df_all = df_all[
        (df_all['years'] > 0.01) & (df_all['years'] < 2) &
        (df_all['atmVol'] < 1.5) &
        ((df_all['cCnt'] + df_all['pCnt']) >= 20) &
        (df_all['vwidth'] <= 0.2)
    ]
    
    logger.info(f"Loaded {len(df_all)} records after filtering")
    return df_all

def append_to_existing_output(new_data, output_path):
    """Append new data to existing output dataset."""
    logger = logging.getLogger(__name__)
    
    # Check if output exists
    if Path(output_path).exists():
        logger.info("Appending to existing flat output dataset")
        try:
            # Load existing dataset
            ds_existing = xr.open_zarr(output_path)
            
            # Convert existing to dataframe
            df_existing = ds_existing.to_dataframe().reset_index()
            
            # Convert new data to dataframe (it's already a dataframe)
            df_new = new_data.copy()
            
            # Add index to new data
            if 'index' not in df_new.columns:
                df_new['index'] = range(len(df_existing), len(df_existing) + len(df_new))
            
            # Combine dataframes
            logger.info("Combining datasets...")
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            
            # Remove duplicates if any
            df_combined = df_combined.drop_duplicates(subset=['teo', 'gics_sector', 'size_category', 'style', 'expiry_label', 'value_type'])
            
            # Reset index
            df_combined['index'] = range(len(df_combined))
            df_combined = df_combined.set_index('index')
            
            # Convert back to xarray
            ds_combined = df_combined.to_xarray()
            
            # Save combined dataset
            ds_combined.to_zarr(output_path, mode='w')
            logger.info("Successfully appended new data to existing flat output")
            
        except Exception as e:
            logger.error(f"Error appending to existing dataset: {e}")
            logger.info("Falling back to creating new dataset with only new data")
            # Create flat structure for new data only (don't overwrite existing)
            df_new = new_data.copy()
            df_new['index'] = range(len(df_new))
            df_new = df_new.set_index('index')
            ds_new = df_new.to_xarray()
            # Use a temporary path to avoid overwriting existing data
            temp_output_path = output_path + '.temp'
            ds_new.to_zarr(temp_output_path, mode='w')
            logger.warning(f"Created temporary dataset at {temp_output_path} - manual intervention may be needed")
            logger.warning("Original dataset preserved at original location")
    else:
        logger.info("Creating new flat output dataset")
        # Create flat structure for new data
        df_new = new_data.copy()
        df_new['index'] = range(len(df_new))
        df_new = df_new.set_index('index')
        ds_new = df_new.to_xarray()
        ds_new.to_zarr(output_path, mode='w')
        logger.info("Successfully created new flat output dataset")

def main():
    """Main function with incremental processing logic."""
    parser = argparse.ArgumentParser(description='Produce IV dataset with incremental processing')
    parser.add_argument('--force', action='store_true', 
                       help='Force reprocessing of all data')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable detailed logging')
    
    args = parser.parse_args()
    logger = setup_logging()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting IV dataset production with incremental processing")
    
    # Check if incremental processing is needed
    if not args.force:
        needs_processing, output_latest, input_latest = check_incremental_processing_needed()
        if not needs_processing:
            logger.info("No new data to process - exiting")
            return 0
        else:
            logger.info("Incremental processing mode - will append new data to existing dataset")
    else:
        logger.info("Force mode - will process all data and append to existing dataset if it exists")
        logger.info("Note: --force flag will process all data but will NOT overwrite existing output dataset")
        needs_processing = True
        output_latest = None
        input_latest = None
    
    # Load and filter data
    zarr_path = 'C:/Users/gigan/OneDrive/Desktop/BlueWaterMacro/data/sr_options/data/surfacecurvehist/surface_curve_history_combined_data_vars.zarr'
    
    # If we have a start date, use it for filtering
    start_date = output_latest + pd.Timedelta(days=1) if output_latest is not None else None
    
    if start_date is not None:
        logger.info(f"Filtering data from {start_date} onwards for incremental processing")
    else:
        logger.info("Processing all available data")
    
    df_all = load_and_filter_data(zarr_path, start_date)
    
    if len(df_all) == 0:
        logger.info("No data to process after filtering")
        return 0
    
    logger.info(f"Processing {len(df_all)} records")

    # 1. Filter to expiries closest to target days
    TARGET_DAYS = [30, 60, 90, 120, 180, 270, 360, 540, 720]
    def assign_closest_bucket(years):
        days = years * 365.0
        diffs = [abs(days - tgt) for tgt in TARGET_DAYS]
        idx = int(np.argmin(diffs))
        return TARGET_DAYS[idx], diffs[idx]

    df_all[['target_days', 'days_diff']] = pd.DataFrame(
        df_all['years'].apply(assign_closest_bucket).tolist(),
        index=df_all.index
    )

    df_all = (
        df_all.sort_values('days_diff')
        .groupby(['teo', 'ticker', 'target_days'], as_index=False)
        .first()
        .drop(columns=['days_diff'])
    )

    # Pivot both atmVol and atmCen
    pivot_vol = df_all.pivot(index=['teo', 'ticker'], columns='target_days', values='atmVol').reset_index()
    pivot_cen = df_all.pivot(index=['teo', 'ticker'], columns='target_days', values='atmCen').reset_index()

    # Define forward pairs
    TARGET_PAIRS = list(zip(TARGET_DAYS[:-1], TARGET_DAYS[1:]))

    # Calculate forward vols for atmVol
    for T1d, T2d in TARGET_PAIRS:
        T1 = T1d / 365.0
        T2 = T2d / 365.0
        col1 = T1d
        col2 = T2d
        new_col = f'fwd_{T1d}_{T2d}'
        pivot_vol[new_col] = np.sqrt(
            (pivot_vol[col2]**2 * T2 - pivot_vol[col1]**2 * T1) / (T2 - T1)
        )

    # Calculate forward vols for atmCen (same logic for symmetry)
    for T1d, T2d in TARGET_PAIRS:
        T1 = T1d / 365.0
        T2 = T2d / 365.0
        col1 = T1d
        col2 = T2d
        new_col = f'fwd_{T1d}_{T2d}'
        pivot_cen[new_col] = np.sqrt(
            (pivot_cen[col2]**2 * T2 - pivot_cen[col1]**2 * T1) / (T2 - T1)
        )

    # Define column lists
    vol_cols = TARGET_DAYS
    fwd_cols = [f'fwd_{T1}_{T2}' for T1, T2 in TARGET_PAIRS]

    # Melt atmVol (including forward vols)
    vol_melt = pivot_vol.melt(
        id_vars=['teo', 'ticker'],
        value_vars=vol_cols + fwd_cols,
        var_name='expiry_label',
        value_name='value'
    )
    vol_melt['value_type'] = 'atmVol'

    # Melt atmCen (including forward vols)
    cen_melt = pivot_cen.melt(
        id_vars=['teo', 'ticker'],
        value_vars=vol_cols + fwd_cols,
        var_name='expiry_label',
        value_name='value'
    )
    cen_melt['value_type'] = 'atmCen'

    # Combine and clean
    df_melt = pd.concat([vol_melt, cen_melt], ignore_index=True).dropna()

    ds_all = df_melt.set_index(['teo', 'ticker', 'expiry_label', 'value_type'])[['value']].to_xarray()

    # First run this in terminal: gsutil -m cp -r gs://rm_api_data/ds_daily_ticker.zarr .
    # Load daily ticker dataset
    ds_daily_ticker = xr.open_zarr('C:/Users/gigan/OneDrive/Desktop/BlueWaterMacro/data/stock_data/zarr/ds_daily_ticker.zarr').compute()

    ds_all['teo'] = ds_all['teo'].astype('datetime64[D]')
    ds_daily_ticker['teo'] = ds_daily_ticker['teo'].astype('datetime64[D]')

    ds_dates = np.unique(ds_all.teo.values)
    teo_dates = np.unique(ds_daily_ticker.teo.values)
    common_teo = np.intersect1d(ds_dates, teo_dates)

    tickers_ds = np.unique(ds_all.ticker.values)
    tickers_daily = np.unique(ds_daily_ticker.ticker.values)
    common_tickers = np.intersect1d(tickers_ds, tickers_daily)

    ds_trimmed = ds_all.sel(
        teo=common_teo,
        ticker=common_tickers
    )

    ds_ticker_trimmed = ds_daily_ticker.sel(
        teo=common_teo,
        ticker=common_tickers
    )

    industry_codes = ds_ticker_trimmed['fs_industry_code']
    ds_trimmed = ds_trimmed.assign_coords(fs_industry_code=(('teo', 'ticker'), industry_codes.values))
    df = ds_trimmed.to_dataframe().reset_index().dropna(subset=['fs_industry_code', 'value'])

    df['fs_industry_code'] = df['fs_industry_code'].astype(int)
    df['gics_sector_code'] = df['fs_industry_code'].map(fs_industry_to_gic_code)
    df['gics_sector'] = df['gics_sector_code'].map(gic_code_to_gic_name)
    df = df.dropna(subset=['gics_sector_code', 'gics_sector'])

    mktcap_df = ds_daily_ticker['mktcap'].to_series().reset_index()
    mktcap_df.columns = ['teo', 'ticker', 'market_cap']
    df['teo'] = pd.to_datetime(df['teo'])
    mktcap_df['teo'] = pd.to_datetime(mktcap_df['teo'])
    df = df.merge(mktcap_df, on=['teo', 'ticker'], how='left')

    def classify_size(market_cap):
        if market_cap >= 10e9:
            return 'Large Cap'
        elif market_cap >= 2e9:
            return 'Mid Cap'
        else:
            return 'Small Cap'

    df['size_category'] = df['market_cap'].apply(classify_size)

    style_zarr_path = 'gs://rm_api_data/symbol_style_time.zarr'
    style_ds = xr.open_zarr(style_zarr_path, storage_options={"token": None})

    # Step 1: Extract raw ticker values (symbol x teo)
    ticker_2d = style_ds['ticker'].values

    # Step 2: Extract first valid ticker per symbol
    def first_valid_ticker(row):
        for val in row:
            if isinstance(val, str) and val.strip() != '':
                return val
        return None

    ticker_1d = np.array([first_valid_ticker(row) for row in ticker_2d])

    # Step 3: Drop rows with missing ticker
    valid_mask = pd.notnull(ticker_1d)
    style_ds = style_ds.isel(symbol=valid_mask)
    ticker_1d = ticker_1d[valid_mask]

    # Step 4: Assign ticker as coordinate and swap dimensions
    style_ds = style_ds.assign_coords(ticker=('symbol', ticker_1d))
    style_ds = style_ds.swap_dims({'symbol': 'ticker'}).drop_vars('symbol')

    style_df = style_ds[['Aggressive_Growth', 'Growth', 'Value', 'Deep_Value', 'GARP', 'Yield']].to_dataframe().reset_index()

    # Melt style columns into long format
    style_melted = style_df.melt(
        id_vars=['teo', 'ticker'],
        value_vars=['Aggressive_Growth', 'Growth', 'Value', 'Deep_Value', 'GARP', 'Yield'],
        var_name='style',
        value_name='style_score'
    )

    # Drop NaNs and select highest score per (ticker, teo)
    style_ranked = (
        style_melted.dropna(subset=['style_score'])
        .sort_values(['ticker', 'teo', 'style_score'], ascending=[True, True, False])
        .drop_duplicates(subset=['ticker', 'teo'])
    )

    # Step 1: Drop style_score
    style_ranked = style_ranked.drop(columns=['style_score'])

    # Step 2: Ensure teo is datetime and sorted
    style_ranked['teo'] = pd.to_datetime(style_ranked['teo'])
    style_ranked = style_ranked.sort_values(['ticker', 'teo'])

    # Step 3: Reindex to daily frequency per ticker
    all_days = pd.date_range(style_ranked['teo'].min(), style_ranked['teo'].max(), freq='D')

    # Create a MultiIndex of all (ticker, date) combinations
    tickers = style_ranked['ticker'].unique()
    full_index = pd.MultiIndex.from_product([tickers, all_days], names=['ticker', 'teo'])

    # Reindex and forward-fill
    style_daily = (
        style_ranked
        .set_index(['ticker', 'teo'])
        .reindex(full_index)
        .groupby(level=0)
        .ffill()
        .reset_index()
    )

    df['teo'] = pd.to_datetime(df['teo'])
    style_daily['teo'] = pd.to_datetime(style_daily['teo'])
    df = df.merge(style_daily, on=['ticker', 'teo'], how='left')

    # Step 1: Daskify the data
    cols = ['teo', 'gics_sector', 'size_category', 'style', 'expiry_label', 'value_type', 'value', 'market_cap']
    ddf = dd.from_pandas(df[cols], npartitions=32)

    # Step 2: Add weighted numerator
    ddf['weighted_value_num'] = ddf['value'] * ddf['market_cap']

    # Step 3: Full-detail group
    group_cols = ['teo', 'gics_sector', 'size_category', 'style', 'expiry_label', 'value_type']
    grouped = ddf.groupby(group_cols)
    agg_main = grouped[['weighted_value_num', 'market_cap']].sum().reset_index()
    agg_main['weighted_value'] = agg_main['weighted_value_num'] / agg_main['market_cap']
    agg_main = agg_main.drop(columns=['weighted_value_num', 'market_cap'])

    '''
    Example:

    On 2025-07-01
    GICS Sector: Technology
    Size: Large Cap
    Style: Growth
    Expiry: 90
    Value Type: atmVol

    It computes market cap–weighted avg of IV across all tech large-cap growth
    stocks with that expiry on that day.
    '''

    # Step 4: Compute true totals for each axis (by dropping one at a time)
    agg_totals = []

    # Add full total (across all 3: sector, size, style)
    total_all = (
        ddf.groupby(['teo', 'expiry_label', 'value_type'])[['weighted_value_num', 'market_cap']]
        .sum()
        .reset_index()
    )
    total_all['gics_sector'] = 'Total'
    total_all['size_category'] = 'Total'
    total_all['style'] = 'Total'
    total_all['weighted_value'] = total_all['weighted_value_num'] / total_all['market_cap']
    total_all = total_all.drop(columns=['weighted_value_num', 'market_cap'])
    agg_totals.append(total_all)

    # Add partial totals - sector total (size and style as-is)
    total_sector = (
        ddf.groupby(['teo', 'size_category', 'style', 'expiry_label', 'value_type'])[['weighted_value_num', 'market_cap']]
        .sum()
        .reset_index()
    )
    total_sector['gics_sector'] = 'Total'
    total_sector['weighted_value'] = total_sector['weighted_value_num'] / total_sector['market_cap']
    total_sector = total_sector.drop(columns=['weighted_value_num', 'market_cap'])
    agg_totals.append(total_sector)

    # Add partial totals - size total (sector and style as-is)
    total_size = (
        ddf.groupby(['teo', 'gics_sector', 'style', 'expiry_label', 'value_type'])[['weighted_value_num', 'market_cap']]
        .sum()
        .reset_index()
    )
    total_size['size_category'] = 'Total'
    total_size['weighted_value'] = total_size['weighted_value_num'] / total_size['market_cap']
    total_size = total_size.drop(columns=['weighted_value_num', 'market_cap'])
    agg_totals.append(total_size)

    # Add partial totals - style total (sector and size as-is)
    total_style = (
        ddf.groupby(['teo', 'gics_sector', 'size_category', 'expiry_label', 'value_type'])[['weighted_value_num', 'market_cap']]
        .sum()
        .reset_index()
    )
    total_style['style'] = 'Total'
    total_style['weighted_value'] = total_style['weighted_value_num'] / total_style['market_cap']
    total_style = total_style.drop(columns=['weighted_value_num', 'market_cap'])
    agg_totals.append(total_style)

    # Add partial totals - sector and size total (style as-is)
    total_sector_size = (
        ddf.groupby(['teo', 'style', 'expiry_label', 'value_type'])[['weighted_value_num', 'market_cap']]
        .sum()
        .reset_index()
    )
    total_sector_size['gics_sector'] = 'Total'
    total_sector_size['size_category'] = 'Total'
    total_sector_size['weighted_value'] = total_sector_size['weighted_value_num'] / total_sector_size['market_cap']
    total_sector_size = total_sector_size.drop(columns=['weighted_value_num', 'market_cap'])
    agg_totals.append(total_sector_size)

    # Add partial totals - sector and style total (size as-is)
    total_sector_style = (
        ddf.groupby(['teo', 'size_category', 'expiry_label', 'value_type'])[['weighted_value_num', 'market_cap']]
        .sum()
        .reset_index()
    )
    total_sector_style['gics_sector'] = 'Total'
    total_sector_style['style'] = 'Total'
    total_sector_style['weighted_value'] = total_sector_style['weighted_value_num'] / total_sector_style['market_cap']
    total_sector_style = total_sector_style.drop(columns=['weighted_value_num', 'market_cap'])
    agg_totals.append(total_sector_style)

    # Add partial totals - size and style total (sector as-is)
    total_size_style = (
        ddf.groupby(['teo', 'gics_sector', 'expiry_label', 'value_type'])[['weighted_value_num', 'market_cap']]
        .sum()
        .reset_index()
    )
    total_size_style['size_category'] = 'Total'
    total_size_style['style'] = 'Total'
    total_size_style['weighted_value'] = total_size_style['weighted_value_num'] / total_size_style['market_cap']
    total_size_style = total_size_style.drop(columns=['weighted_value_num', 'market_cap'])
    agg_totals.append(total_size_style)

    # Step 5: Combine all Dask pieces and compute
    agg_all = dd.concat([agg_main] + agg_totals)
    agg_all = agg_all.compute()

    # Step 6: Final prep and save
    for col in ['gics_sector', 'size_category', 'style', 'expiry_label', 'value_type']:
        agg_all[col] = agg_all[col].astype(str)

    # Save to output
    output_path = 'output/aggregated_iv_cross_with_totals_flat.zarr'
    
    # Check if output exists
    output_exists = Path(output_path).exists()
    
    if not output_exists:
        # Create new flat dataset only if output doesn't exist
        logger.info(f"Creating new output dataset at {output_path}")
        df_new = agg_all.copy()
        df_new['index'] = range(len(df_new))
        df_new = df_new.set_index('index')
        ds_total = df_new.to_xarray()
        ds_total.to_zarr(output_path, mode='w')
        logger.info("Created new flat output dataset")
    else:
        # Always append to existing dataset if it exists (regardless of --force flag)
        logger.info(f"Appending {len(agg_all)} new records to existing dataset at {output_path}")
        append_to_existing_output(agg_all, output_path)
    
    logger.info("IV dataset production completed successfully")
    return 0

if __name__ == "__main__":
    exit(main())

