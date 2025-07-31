#!/usr/bin/env python3
"""
Enhanced electricity data processor with proper table structure transformation
Processes Excel sheets into normalized database-ready CSV files
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_directories():
    """Create necessary directories"""
    processed_dir = Path("processed_data")
    processed_dir.mkdir(exist_ok=True)
    return processed_dir

def process_zone_data_5min(file_path: str, output_dir: Path):
    """Process 5-minute zone data - this is already in good format"""
    logger.info(f"Processing zone data: {file_path}")
    
    df = pd.read_csv(file_path)
    
    # Clean column names - remove spaces and special characters
    df.columns = [col.strip().replace(' ', '_').replace('(', '').replace(')', '').upper() 
                 for col in df.columns]
    
    # Convert date column
    df['DATE'] = pd.to_datetime(df['DATE'])
    df = df.rename(columns={'DATE': 'TIMESTAMP_NZ'})
    
    # Add metadata
    df['LOAD_TIMESTAMP'] = datetime.now()
    
    # Save
    output_file = output_dir / "electricity_zone_data_5min_final.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Saved {len(df)} rows to {output_file}")
    
    return output_file

def process_fuel_type_data(excel_path: str, output_dir: Path):
    """Process fuel type data from Excel sheet"""
    logger.info("Processing fuel type data for renewable analysis")
    
    # Read the fuel type sheet
    df = pd.read_excel(excel_path, sheet_name='6 - Fuel type (GWh)', skiprows=3)
    
    # The data has years in first column and fuel types in subsequent columns
    # Clean up the structure
    df = df.dropna(subset=[df.columns[0]])  # Remove rows without year
    
    # Rename columns based on expected structure
    expected_cols = [
        'CALENDAR_YEAR', 'HYDRO_GWH', 'GEOTHERMAL_GWH', 'BIOGAS_GWH', 
        'WIND_GWH', 'SOLAR_PV_GWH', 'OIL_GWH', 'COAL_GWH', 'GAS_GWH',
        'ELECTRICITY_ONLY_SUBTOTAL_GWH', 'COGENERATION_GWH', 'TOTAL_GENERATION_GWH'
    ]
    
    # Take only the columns we need (first 12 columns typically)
    df_clean = df.iloc[:, :len(expected_cols)].copy()
    df_clean.columns = expected_cols
    
    # Filter out non-numeric years and convert to int
    df_clean = df_clean[pd.to_numeric(df_clean['CALENDAR_YEAR'], errors='coerce').notna()]
    df_clean['CALENDAR_YEAR'] = df_clean['CALENDAR_YEAR'].astype(int)
    
    # Convert numeric columns to float, handling any non-numeric values
    numeric_cols = ['HYDRO_GWH', 'GEOTHERMAL_GWH', 'BIOGAS_GWH', 'WIND_GWH', 'SOLAR_PV_GWH', 
                   'OIL_GWH', 'COAL_GWH', 'GAS_GWH', 'ELECTRICITY_ONLY_SUBTOTAL_GWH', 
                   'COGENERATION_GWH', 'TOTAL_GENERATION_GWH']
    
    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Calculate renewable vs fossil fuel percentages
    renewable_cols = ['HYDRO_GWH', 'GEOTHERMAL_GWH', 'BIOGAS_GWH', 'WIND_GWH', 'SOLAR_PV_GWH']
    fossil_cols = ['OIL_GWH', 'COAL_GWH', 'GAS_GWH']
    
    # Only include columns that exist and are numeric
    available_renewable = [col for col in renewable_cols if col in df_clean.columns]
    available_fossil = [col for col in fossil_cols if col in df_clean.columns]
    
    df_clean['RENEWABLE_GWH'] = df_clean[available_renewable].sum(axis=1)
    df_clean['FOSSIL_FUEL_GWH'] = df_clean[available_fossil].sum(axis=1)
    
    # Calculate percentages, handling division by zero
    df_clean['RENEWABLE_PERCENTAGE'] = np.where(
        df_clean['TOTAL_GENERATION_GWH'] > 0,
        (df_clean['RENEWABLE_GWH'] / df_clean['TOTAL_GENERATION_GWH'] * 100).round(2),
        0
    )
    df_clean['FOSSIL_FUEL_PERCENTAGE'] = np.where(
        df_clean['TOTAL_GENERATION_GWH'] > 0,
        (df_clean['FOSSIL_FUEL_GWH'] / df_clean['TOTAL_GENERATION_GWH'] * 100).round(2),
        0
    )
    
    # Add metadata
    df_clean['SOURCE_SHEET'] = '6 - Fuel type (GWh)'
    df_clean['LOAD_TIMESTAMP'] = datetime.now()
    
    # Save
    output_file = output_dir / "electricity_generation_by_fuel_final.csv"
    df_clean.to_csv(output_file, index=False)
    logger.info(f"Saved {len(df_clean)} rows to {output_file}")
    
    return output_file

def process_quarterly_data(excel_path: str, output_dir: Path):
    """Process quarterly generation data and reshape from wide to long format"""
    logger.info("Processing quarterly generation data")
    
    # Read quarterly data sheet
    df = pd.read_excel(excel_path, sheet_name='1 - Quarterly GWh', skiprows=3)
    
    # The data structure has quarters as columns from 1974 onwards
    # First column should be the metric name, then quarters
    
    # Find the generation row (usually first data row)
    gen_row_idx = None
    for idx, row in df.iterrows():
        if pd.notna(row.iloc[0]) and 'generation' in str(row.iloc[0]).lower():
            gen_row_idx = idx
            break
    
    if gen_row_idx is None:
        # Take first row with numeric data
        for idx, row in df.iterrows():
            if pd.notna(row.iloc[1]) and pd.to_numeric(row.iloc[1], errors='coerce') is not None:
                gen_row_idx = idx
                break
    
    if gen_row_idx is not None:
        # Extract the generation data row
        gen_data = df.iloc[gen_row_idx, 1:].dropna()  # Skip first column (label)
        
        # Create quarterly data
        quarterly_data = []
        for i, value in enumerate(gen_data):
            if pd.notna(value) and pd.to_numeric(value, errors='coerce') is not None:
                # Calculate quarter and year (starting from 1974 Q1)
                base_year = 1974
                quarter_offset = i
                year = base_year + (quarter_offset // 4)
                quarter = (quarter_offset % 4) + 1
                
                # Create date for the quarter
                quarter_date = pd.Timestamp(year=year, month=(quarter-1)*3+1, day=1)
                
                quarterly_data.append({
                    'CALENDAR_QUARTER': quarter_date.strftime('%Y-%m-%d'),
                    'NET_GENERATION_GWH': float(value),
                    'QUARTER_YEAR': year,
                    'QUARTER_NUMBER': quarter,
                    'SOURCE_SHEET': '1 - Quarterly GWh',
                    'LOAD_TIMESTAMP': datetime.now()
                })
        
        df_quarterly = pd.DataFrame(quarterly_data)
        
        # Calculate year-over-year change
        df_quarterly = df_quarterly.sort_values(['QUARTER_YEAR', 'QUARTER_NUMBER'])
        df_quarterly['YEAR_OVER_YEAR_CHANGE_PERCENT'] = (
            df_quarterly.groupby('QUARTER_NUMBER')['NET_GENERATION_GWH'].pct_change(4) * 100
        ).round(4)
        
        # Save
        output_file = output_dir / "electricity_quarterly_generation_final.csv"
        df_quarterly.to_csv(output_file, index=False)
        logger.info(f"Saved {len(df_quarterly)} rows to {output_file}")
        
        return output_file
    
    logger.warning("Could not process quarterly data - structure not as expected")
    return None

def main():
    """Main processing function"""
    try:
        output_dir = setup_directories()
        logger.info("Starting enhanced electricity data processing...")
        
        generated_files = []
        
        # 1. Process zone data (5-minute intervals)
        csv_path = "data/Zone Data (01 Jul - 29 Jul) [5 intervals] (1).csv"
        if Path(csv_path).exists():
            zone_file = process_zone_data_5min(csv_path, output_dir)
            generated_files.append(zone_file)
        
        # 2. Process fuel type data
        excel_path = "data/electricity-2025-q1.xlsx"
        if Path(excel_path).exists():
            fuel_file = process_fuel_type_data(excel_path, output_dir)
            if fuel_file:
                generated_files.append(fuel_file)
            
            # 3. Process quarterly data
            quarterly_file = process_quarterly_data(excel_path, output_dir)
            if quarterly_file:
                generated_files.append(quarterly_file)
        
        # Summary
        logger.info(f"Processing complete! Generated {len(generated_files)} final files:")
        for file in generated_files:
            logger.info(f"  - {file}")
        
        # Display sample data for verification
        for file in generated_files:
            logger.info(f"\nSample from {file.name}:")
            sample_df = pd.read_csv(file, nrows=3)
            logger.info(f"Columns: {list(sample_df.columns)}")
            logger.info(f"Sample data:\n{sample_df.head(2)}")
        
        return generated_files
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()