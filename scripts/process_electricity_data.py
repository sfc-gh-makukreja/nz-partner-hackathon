#!/usr/bin/env python3
"""
Process electricity data for NZ Partner Hackathon
Converts Excel and CSV files to clean CSV format for Snowflake ingestion
"""

import pandas as pd
import os
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_directories():
    """Create necessary directories"""
    processed_dir = Path("processed_data")
    processed_dir.mkdir(exist_ok=True)
    return processed_dir

def process_electricity_excel(file_path: str, output_dir: Path):
    """
    Process electricity Excel file with multiple sheets
    Convert to clean CSV format suitable for Snowflake
    """
    logger.info(f"Processing Excel file: {file_path}")
    
    try:
        # Read Excel file to examine structure
        xl_file = pd.ExcelFile(file_path)
        logger.info(f"Found sheets: {xl_file.sheet_names}")
        
        processed_files = []
        
        for sheet_name in xl_file.sheet_names:
            logger.info(f"Processing sheet: {sheet_name}")
            
            # Read the sheet
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Basic data cleaning
            # Remove completely empty rows/columns
            df = df.dropna(how='all').dropna(axis=1, how='all')
            
            # Skip if sheet is too small (likely metadata)
            if len(df) < 2:
                logger.warning(f"Skipping sheet {sheet_name} - too few rows")
                continue
            
            # Clean column names
            df.columns = [str(col).strip().replace(' ', '_').replace('(', '').replace(')', '').upper() 
                         for col in df.columns]
            
            # Add metadata columns
            df['SOURCE_SHEET'] = sheet_name
            df['LOAD_TIMESTAMP'] = pd.Timestamp.now()
            
            # Save to CSV
            output_file = output_dir / f"electricity_{sheet_name.lower().replace(' ', '_')}.csv"
            df.to_csv(output_file, index=False)
            processed_files.append(output_file)
            
            logger.info(f"Saved {len(df)} rows to {output_file}")
            
        return processed_files
        
    except Exception as e:
        logger.error(f"Error processing Excel file: {e}")
        raise

def process_zone_data_csv(file_path: str, output_dir: Path):
    """
    Process zone data CSV with 5-minute intervals
    Clean and standardize for Snowflake
    """
    logger.info(f"Processing CSV file: {file_path}")
    
    try:
        # Read CSV
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows with columns: {list(df.columns)}")
        
        # Clean column names
        df.columns = [str(col).strip().replace(' ', '_').replace('(', '').replace(')', '').upper() 
                     for col in df.columns]
        
        # Try to parse datetime columns (common patterns)
        datetime_columns = [col for col in df.columns if any(x in col.lower() for x in ['date', 'time', 'timestamp'])]
        
        for col in datetime_columns:
            try:
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
                logger.info(f"Converted {col} to datetime")
            except Exception as e:
                logger.warning(f"Could not convert {col} to datetime: {e}")
        
        # Add metadata
        df['LOAD_TIMESTAMP'] = pd.Timestamp.now()
        
        # Save processed file
        output_file = output_dir / "electricity_zone_data_5min.csv"
        df.to_csv(output_file, index=False)
        
        logger.info(f"Saved {len(df)} rows to {output_file}")
        return [output_file]
        
    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")
        raise

def main():
    """Main processing function"""
    try:
        # Setup
        output_dir = setup_directories()
        logger.info("Starting electricity data processing...")
        
        # Process files
        excel_files = []
        csv_files = []
        
        # Process Excel file
        excel_path = "data/electricity-2025-q1.xlsx"
        if os.path.exists(excel_path):
            excel_files = process_electricity_excel(excel_path, output_dir)
        else:
            logger.warning(f"Excel file not found: {excel_path}")
        
        # Process CSV file
        csv_path = "data/Zone Data (01 Jul - 29 Jul) [5 intervals] (1).csv"
        if os.path.exists(csv_path):
            csv_files = process_zone_data_csv(csv_path, output_dir)
        else:
            logger.warning(f"CSV file not found: {csv_path}")
        
        # Summary
        all_files = excel_files + csv_files
        logger.info(f"Processing complete! Generated {len(all_files)} files:")
        for file in all_files:
            logger.info(f"  - {file}")
            
        return all_files
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()