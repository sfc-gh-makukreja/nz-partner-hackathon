#!/usr/bin/env python3
"""
Process NZ Airfares Data for HIWA_I_TE_RANGI schema
Handles: Kaggle airfares dataset with route, pricing, and seasonal data
Source: https://www.kaggle.com/datasets/shashwatwork/airfares-in-new-zealand
Theme: HIWA_I_TE_RANGI (Travel & Tourism)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import zipfile
from datetime import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path('data')
PROCESSED_DATA_DIR = Path('processed_data')
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

def extract_airfares_data():
    """Extract airfares data from zip file"""
    zip_file = RAW_DATA_DIR / 'NZ airfares.csv.zip'
    
    if not zip_file.exists():
        logger.error(f"Zip file not found: {zip_file}")
        return None
        
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            logger.info(f"Files in zip: {file_list}")
            
            # Extract all files to data directory
            zip_ref.extractall(RAW_DATA_DIR)
            
            # Find the main CSV file
            csv_files = [f for f in file_list if f.endswith('.csv')]
            if csv_files:
                main_csv = csv_files[0]
                logger.info(f"Processing main CSV: {main_csv}")
                return RAW_DATA_DIR / main_csv
            else:
                logger.error("No CSV files found in zip")
                return None
                
    except Exception as e:
        logger.error(f"Error extracting zip file: {e}")
        return None

def clean_column_names(columns):
    """Clean column names for Snowflake compatibility"""
    cleaned = []
    for col in columns:
        clean_col = str(col).strip().lower()
        clean_col = re.sub(r'[^\w\s]', '_', clean_col)
        clean_col = re.sub(r'\s+', '_', clean_col)
        clean_col = re.sub(r'_+', '_', clean_col)
        clean_col = clean_col.strip('_')
        cleaned.append(clean_col)
    return cleaned

def standardize_location_names(location_series):
    """Standardize NZ location names for consistency"""
    if location_series.dtype == 'object':
        # Common NZ location mappings
        location_map = {
            'akl': 'Auckland',
            'auckland': 'Auckland',
            'wlg': 'Wellington', 
            'wellington': 'Wellington',
            'chc': 'Christchurch',
            'christchurch': 'Christchurch',
            'dud': 'Dunedin',
            'dunedin': 'Dunedin',
            'qtn': 'Queenstown',
            'queenstown': 'Queenstown',
            'npm': 'New Plymouth',
            'new plymouth': 'New Plymouth',
            'rot': 'Rotorua',
            'rotorua': 'Rotorua',
            'tau': 'Tauranga',
            'tauranga': 'Tauranga',
            'nsn': 'Nelson',
            'nelson': 'Nelson',
            'pmr': 'Palmerston North',
            'palmerston north': 'Palmerston North'
        }
        
        # Apply mappings
        standardized = location_series.str.lower().str.strip()
        for key, value in location_map.items():
            standardized = standardized.replace(key, value.lower())
        return standardized.str.title()
    return location_series

def process_airfares_data(csv_file):
    """Process the airfares CSV data"""
    try:
        logger.info(f"Reading airfares data from: {csv_file}")
        df = pd.read_csv(csv_file)
        
        logger.info(f"Original shape: {df.shape}")
        logger.info(f"Original columns: {list(df.columns)}")
        logger.info(f"Sample data:")
        logger.info(df.head())
        
        # Clean data
        df = df.dropna(how='all')
        original_columns = df.columns.tolist()
        df.columns = clean_column_names(df.columns)
        
        logger.info(f"Column mapping:")
        for orig, clean in zip(original_columns, df.columns):
            logger.info(f"  {orig} → {clean}")
        
        # Handle date columns
        date_cols = [col for col in df.columns if any(term in col for term in ['date', 'time', 'year', 'month', 'day'])]
        for col in date_cols:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.info(f"✓ Converted {col} to datetime")
            except:
                logger.warning(f"⚠ Could not convert {col} to datetime")
        
        # Handle price columns - convert to numeric
        price_cols = [col for col in df.columns if any(term in col for term in ['price', 'fare', 'cost', 'amount', 'nzd'])]
        for col in price_cols:
            try:
                if col in df.columns:
                    # Remove currency symbols and convert to numeric
                    df[col] = df[col].astype(str).str.replace(r'[^\d.]', '', regex=True)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    logger.info(f"✓ Converted {col} to numeric")
            except:
                logger.warning(f"⚠ Could not convert {col} to numeric")
        
        # Standardize route/location information
        route_cols = [col for col in df.columns if any(term in col for term in ['origin', 'destination', 'route', 'from', 'to', 'departure', 'arrival'])]
        for col in route_cols:
            if col in df.columns:
                df[col] = standardize_location_names(df[col])
                logger.info(f"✓ Standardized {col} location names")
        
        # Handle airline information
        airline_cols = [col for col in df.columns if any(term in col for term in ['airline', 'carrier', 'operator'])]
        for col in airline_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()
        
        # Add audit columns
        df['load_timestamp'] = datetime.now()
        df['data_source'] = 'Kaggle - shashwatwork/airfares-in-new-zealand'
        df['dataset_description'] = 'New Zealand domestic and international airfares dataset'
        df['data_freshness'] = 'Historical airfare data for tourism analysis'
        
        # Save processed data
        output_file = PROCESSED_DATA_DIR / 'nz_airfares.csv'
        df.to_csv(output_file, index=False, date_format='%Y-%m-%d')
        
        logger.info(f"✅ Processed airfares data saved to: {output_file}")
        logger.info(f"Final shape: {df.shape}")
        
        # Show comprehensive data summary
        logger.info("="*60)
        logger.info("AIRFARES DATA SUMMARY")
        logger.info("="*60)
        logger.info(f"Total records: {len(df):,}")
        logger.info(f"Total columns: {len(df.columns)}")
        
        # Route analysis
        if route_cols:
            logger.info(f"\n📍 ROUTE INFORMATION:")
            for col in route_cols[:3]:  # Show first 3 route columns
                if col in df.columns:
                    unique_count = df[col].nunique()
                    unique_values = df[col].unique()[:8]
                    logger.info(f"  {col}: {unique_count} unique values")
                    logger.info(f"    Sample: {list(unique_values)}")
        
        # Price analysis
        if price_cols:
            logger.info(f"\n💰 PRICING INFORMATION:")
            for col in price_cols[:3]:  # Show first 3 price columns
                if col in df.columns:
                    price_stats = df[col].describe()
                    logger.info(f"  {col}:")
                    logger.info(f"    Min: ${price_stats['min']:.2f}")
                    logger.info(f"    Max: ${price_stats['max']:.2f}")
                    logger.info(f"    Mean: ${price_stats['mean']:.2f}")
                    logger.info(f"    Median: ${price_stats['50%']:.2f}")
        
        # Date analysis
        if date_cols:
            logger.info(f"\n📅 DATE INFORMATION:")
            for col in date_cols[:2]:
                if col in df.columns and df[col].notna().any():
                    logger.info(f"  {col}: {df[col].min()} to {df[col].max()}")
        
        # Airline analysis
        if airline_cols:
            logger.info(f"\n✈️ AIRLINE INFORMATION:")
            for col in airline_cols[:2]:
                if col in df.columns:
                    top_airlines = df[col].value_counts().head(5)
                    logger.info(f"  Top airlines in {col}:")
                    for airline, count in top_airlines.items():
                        logger.info(f"    {airline}: {count} flights")
        
        # Data quality assessment
        null_cols = df.isnull().sum()
        high_null_cols = null_cols[null_cols > len(df) * 0.1]  # >10% null
        if len(high_null_cols) > 0:
            logger.info(f"\n⚠️ COLUMNS WITH >10% NULL VALUES:")
            for col, null_count in high_null_cols.items():
                pct = (null_count / len(df)) * 100
                logger.info(f"  {col}: {null_count} nulls ({pct:.1f}%)")
        
        return output_file
        
    except Exception as e:
        logger.error(f"❌ Error processing airfares data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def main():
    """Main processing pipeline"""
    logger.info("="*80)
    logger.info("🛫 NZ AIRFARES DATA PROCESSING - HIWA_I_TE_RANGI")
    logger.info("="*80)
    
    # Extract airfares data
    logger.info("📦 Extracting airfares data from ZIP...")
    csv_file = extract_airfares_data()
    if not csv_file:
        logger.error("❌ Failed to extract airfares data")
        return 1
    
    # Process the data
    logger.info("🔄 Processing airfares data...")
    processed_file = process_airfares_data(csv_file)
    if not processed_file:
        logger.error("❌ Failed to process airfares data")
        return 1
    
    logger.info("="*80)
    logger.info("✅ AIRFARES DATA PROCESSING COMPLETE!")
    logger.info(f"✅ Output: {processed_file}")
    logger.info("="*80)
    logger.info("")
    logger.info("🎯 TOURISM INTELLIGENCE ENHANCED:")
    logger.info("• Event-driven airfare analysis")
    logger.info("• Regional accessibility insights")
    logger.info("• Seasonal pricing patterns")
    logger.info("• Tourism affordability index")
    logger.info("")
    logger.info("📋 NEXT STEPS:")
    logger.info("1. Update setup_hiwa_i_te_rangi.sql to include airfares table")
    logger.info("2. Load data: snow sql -f scripts/setup_hiwa_i_te_rangi.sql")
    logger.info("3. Create advanced queries: airfares + events + tourism stats")
    logger.info("4. Build tourism price intelligence features")
    logger.info("")
    logger.info("🚀 Ready to revolutionize NZ tourism analytics!")
    
    return 0

if __name__ == "__main__":
    exit(main())