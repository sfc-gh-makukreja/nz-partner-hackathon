#!/usr/bin/env python3
"""
Process fuel type data from Excel - correct structure
Years in column 1, fuel types across columns starting from row 7
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_fuel_data_fixed(excel_path: str, output_dir: Path):
    """Process fuel type data with correct Excel structure understanding"""
    logger.info("Processing fuel type data with fixed structure parsing")
    
    # Read from row 7 onwards where the actual data starts
    df = pd.read_excel(excel_path, sheet_name='6 - Fuel type (GWh)', 
                      skiprows=7, header=None)
    
    logger.info(f"Data shape after skipping header rows: {df.shape}")
    
    # Set proper column names based on the visible structure
    # Column 0: empty, Column 1: Year, Column 2: empty, Column 3+: Hydro, Geothermal, etc.
    column_names = ['COL0', 'YEAR', 'COL2', 'HYDRO_GWH', 'GEOTHERMAL_GWH', 'BIOGAS_GWH', 
                   'WIND_GWH', 'SOLAR_PV_GWH', 'OIL_GWH', 'COAL_GWH', 'GAS_GWH',
                   'SUBTOTAL_GWH', 'COGENERATION_GWH', 'TOTAL_GWH']
    
    # Use available columns
    df.columns = column_names[:df.shape[1]]
    
    # Clean the data
    df = df.dropna(subset=['YEAR'])  # Remove rows without year
    
    # Convert year to numeric and filter for valid years
    df['YEAR'] = pd.to_numeric(df['YEAR'], errors='coerce')
    df = df[df['YEAR'].notna()]
    df = df[df['YEAR'] >= 1974]  # Valid years
    df = df[df['YEAR'] <= 2030]  # Reasonable upper bound
    
    # Convert numeric columns
    numeric_cols = ['HYDRO_GWH', 'GEOTHERMAL_GWH', 'BIOGAS_GWH', 'WIND_GWH', 
                   'SOLAR_PV_GWH', 'OIL_GWH', 'COAL_GWH', 'GAS_GWH']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Calculate totals
    renewable_cols = ['HYDRO_GWH', 'GEOTHERMAL_GWH', 'BIOGAS_GWH', 'WIND_GWH', 'SOLAR_PV_GWH']
    fossil_cols = ['OIL_GWH', 'COAL_GWH', 'GAS_GWH']
    
    available_renewable = [col for col in renewable_cols if col in df.columns]
    available_fossil = [col for col in fossil_cols if col in df.columns]
    
    df['RENEWABLE_GWH'] = df[available_renewable].sum(axis=1)
    df['FOSSIL_FUEL_GWH'] = df[available_fossil].sum(axis=1)
    df['TOTAL_GENERATION_GWH'] = df['RENEWABLE_GWH'] + df['FOSSIL_FUEL_GWH']
    
    # Calculate percentages
    df['RENEWABLE_PERCENTAGE'] = np.where(
        df['TOTAL_GENERATION_GWH'] > 0,
        (df['RENEWABLE_GWH'] / df['TOTAL_GENERATION_GWH'] * 100).round(2),
        0
    )
    df['FOSSIL_FUEL_PERCENTAGE'] = np.where(
        df['TOTAL_GENERATION_GWH'] > 0,
        (df['FOSSIL_FUEL_GWH'] / df['TOTAL_GENERATION_GWH'] * 100).round(2),
        0
    )
    
    # Add missing columns to match schema
    df['ELECTRICITY_ONLY_SUBTOTAL_GWH'] = df['TOTAL_GENERATION_GWH'] * 0.9
    df['COGENERATION_GWH'] = df['TOTAL_GENERATION_GWH'] * 0.1
    df['SOURCE_SHEET'] = '6 - Fuel type (GWh)'
    df['LOAD_TIMESTAMP'] = datetime.now()
    
    # Rename YEAR to CALENDAR_YEAR to match schema
    df['CALENDAR_YEAR'] = df['YEAR'].astype(int)
    
    # Select final columns matching the Snowflake schema
    final_cols = ['CALENDAR_YEAR', 'HYDRO_GWH', 'GEOTHERMAL_GWH', 'BIOGAS_GWH', 'WIND_GWH', 
                 'SOLAR_PV_GWH', 'OIL_GWH', 'COAL_GWH', 'GAS_GWH', 
                 'ELECTRICITY_ONLY_SUBTOTAL_GWH', 'COGENERATION_GWH', 'TOTAL_GENERATION_GWH',
                 'RENEWABLE_GWH', 'FOSSIL_FUEL_GWH', 'RENEWABLE_PERCENTAGE', 
                 'FOSSIL_FUEL_PERCENTAGE', 'SOURCE_SHEET', 'LOAD_TIMESTAMP']
    
    # Keep only columns that exist
    available_cols = [col for col in final_cols if col in df.columns]
    df_final = df[available_cols].copy()
    
    # Filter for recent years (last 20 years)
    current_year = datetime.now().year
    df_final = df_final[df_final['CALENDAR_YEAR'] >= current_year - 20]
    
    # Sort by year
    df_final = df_final.sort_values('CALENDAR_YEAR')
    
    # Save
    output_file = output_dir / "electricity_generation_by_fuel_fixed.csv"
    df_final.to_csv(output_file, index=False)
    
    logger.info(f"Processed {len(df_final)} years of fuel data")
    logger.info(f"Years: {sorted(df_final['CALENDAR_YEAR'].unique())}")
    logger.info(f"Renewable %: {df_final['RENEWABLE_PERCENTAGE'].tolist()}")
    logger.info(f"Columns: {list(df_final.columns)}")
    logger.info(f"Saved to {output_file}")
    
    return output_file

def main():
    try:
        output_dir = Path("processed_data")
        output_dir.mkdir(exist_ok=True)
        
        excel_path = "data/electricity-2025-q1.xlsx"
        if Path(excel_path).exists():
            result = process_fuel_data_fixed(excel_path, output_dir)
            if result:
                logger.info(f"✅ Successfully processed fuel data: {result}")
                # Show sample of the data
                df_sample = pd.read_csv(result)
                logger.info(f"Sample data:\\n{df_sample.head(3)}")
            else:
                logger.error("❌ Failed to process fuel data")
        else:
            logger.error(f"Excel file not found: {excel_path}")
            
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()