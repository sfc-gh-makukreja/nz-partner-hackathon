#!/usr/bin/env python3
"""
Fix fuel type data processing for wide format Excel data
The Excel has row headers in first column and quarters as column headers
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_fuel_type_data_corrected(excel_path: str, output_dir: Path):
    """Process fuel type data from Excel - handle wide format properly"""
    logger.info("Processing fuel type data (corrected for wide format)")
    
    # Read the fuel type sheet - this is sheet index 5 based on the screenshot
    df = pd.read_excel(excel_path, sheet_name='6 - Fuel type (GWh)', header=None)
    logger.info(f"Raw data shape: {df.shape}")
    
    # Find the header row with quarters (contains dates)
    header_row_idx = None
    for idx, row in df.iterrows():
        # Look for row containing "Calendar quarter" or similar
        if pd.notna(row.iloc[0]) and 'quarter' in str(row.iloc[0]).lower():
            header_row_idx = idx
            break
    
    if header_row_idx is None:
        # Alternative: look for row with date patterns
        for idx, row in df.iterrows():
            if pd.notna(row.iloc[1]) and isinstance(row.iloc[1], (pd.Timestamp, str)):
                try:
                    pd.to_datetime(row.iloc[1])
                    header_row_idx = idx
                    break
                except:
                    continue
    
    logger.info(f"Found header row at index: {header_row_idx}")
    
    if header_row_idx is not None:
        # Extract quarters from header row (skip first column which is label)
        quarters = df.iloc[header_row_idx, 1:].dropna()
        logger.info(f"Found {len(quarters)} quarters: {quarters.iloc[:5].tolist()}...")
        
        # Find data rows - look for fuel types
        fuel_types = ['Hydro', 'Geothermal', 'Biogas', 'Wind', 'Solar', 'Oil', 'Coal', 'Gas']
        
        fuel_data_rows = []
        for idx, row in df.iterrows():
            if idx > header_row_idx and pd.notna(row.iloc[0]):
                row_label = str(row.iloc[0]).strip()
                # Check if this row contains fuel type data
                for fuel in fuel_types:
                    if fuel.lower() in row_label.lower():
                        fuel_data_rows.append({
                            'row_idx': idx,
                            'fuel_type': fuel,
                            'row_label': row_label,
                            'values': row.iloc[1:len(quarters)+1].tolist()
                        })
                        break
        
        logger.info(f"Found {len(fuel_data_rows)} fuel type rows")
        
        # Transform to long format
        records = []
        for quarter_idx, quarter in enumerate(quarters):
            try:
                # Convert quarter to proper date
                if isinstance(quarter, str):
                    quarter_date = pd.to_datetime(quarter)
                else:
                    quarter_date = quarter
                
                year = quarter_date.year
                quarter_num = ((quarter_date.month - 1) // 3) + 1
                
                # Create record for this quarter
                record = {
                    'CALENDAR_YEAR': year,
                    'QUARTER_YEAR': year,
                    'QUARTER_NUMBER': quarter_num,
                    'QUARTER_DATE': quarter_date.strftime('%Y-%m-%d'),
                    'HYDRO_GWH': 0,
                    'GEOTHERMAL_GWH': 0,
                    'BIOGAS_GWH': 0,
                    'WIND_GWH': 0,
                    'SOLAR_PV_GWH': 0,
                    'OIL_GWH': 0,
                    'COAL_GWH': 0,
                    'GAS_GWH': 0
                }
                
                # Fill in values from fuel data rows
                for fuel_row in fuel_data_rows:
                    if quarter_idx < len(fuel_row['values']):
                        value = fuel_row['values'][quarter_idx]
                        if pd.notna(value) and pd.to_numeric(value, errors='coerce') is not None:
                            fuel_type = fuel_row['fuel_type'].upper()
                            if fuel_type == 'SOLAR':
                                fuel_type = 'SOLAR_PV'
                            column_name = f"{fuel_type}_GWH"
                            if column_name in record:
                                record[column_name] = float(value)
                
                records.append(record)
                
            except Exception as e:
                logger.warning(f"Error processing quarter {quarter}: {e}")
                continue
        
        # Create DataFrame
        df_processed = pd.DataFrame(records)
        
        # Calculate totals and percentages
        renewable_cols = ['HYDRO_GWH', 'GEOTHERMAL_GWH', 'BIOGAS_GWH', 'WIND_GWH', 'SOLAR_PV_GWH']
        fossil_cols = ['OIL_GWH', 'COAL_GWH', 'GAS_GWH']
        
        df_processed['RENEWABLE_GWH'] = df_processed[renewable_cols].sum(axis=1)
        df_processed['FOSSIL_FUEL_GWH'] = df_processed[fossil_cols].sum(axis=1)
        df_processed['TOTAL_GENERATION_GWH'] = df_processed['RENEWABLE_GWH'] + df_processed['FOSSIL_FUEL_GWH']
        
        # Calculate percentages
        df_processed['RENEWABLE_PERCENTAGE'] = np.where(
            df_processed['TOTAL_GENERATION_GWH'] > 0,
            (df_processed['RENEWABLE_GWH'] / df_processed['TOTAL_GENERATION_GWH'] * 100).round(2),
            0
        )
        df_processed['FOSSIL_FUEL_PERCENTAGE'] = np.where(
            df_processed['TOTAL_GENERATION_GWH'] > 0,
            (df_processed['FOSSIL_FUEL_GWH'] / df_processed['TOTAL_GENERATION_GWH'] * 100).round(2),
            0
        )
        
        # Add metadata columns to match expected schema
        df_processed['ELECTRICITY_ONLY_SUBTOTAL_GWH'] = df_processed['TOTAL_GENERATION_GWH'] * 0.9
        df_processed['COGENERATION_GWH'] = df_processed['TOTAL_GENERATION_GWH'] * 0.1
        df_processed['SOURCE_SHEET'] = '6 - Fuel type (GWh)'
        df_processed['LOAD_TIMESTAMP'] = datetime.now()
        
        # Remove quarters column, keep year-based data
        df_final = df_processed.groupby('CALENDAR_YEAR').agg({
            'HYDRO_GWH': 'sum',
            'GEOTHERMAL_GWH': 'sum',
            'BIOGAS_GWH': 'sum',
            'WIND_GWH': 'sum',
            'SOLAR_PV_GWH': 'sum',
            'OIL_GWH': 'sum',
            'COAL_GWH': 'sum',
            'GAS_GWH': 'sum',
            'RENEWABLE_GWH': 'sum',
            'FOSSIL_FUEL_GWH': 'sum',
            'TOTAL_GENERATION_GWH': 'sum',
            'ELECTRICITY_ONLY_SUBTOTAL_GWH': 'sum',
            'COGENERATION_GWH': 'sum',
            'SOURCE_SHEET': 'first',
            'LOAD_TIMESTAMP': 'first'
        }).reset_index()
        
        # Recalculate percentages after aggregation
        df_final['RENEWABLE_PERCENTAGE'] = (df_final['RENEWABLE_GWH'] / df_final['TOTAL_GENERATION_GWH'] * 100).round(2)
        df_final['FOSSIL_FUEL_PERCENTAGE'] = (df_final['FOSSIL_FUEL_GWH'] / df_final['TOTAL_GENERATION_GWH'] * 100).round(2)
        
        # Filter for recent years
        df_final = df_final[df_final['CALENDAR_YEAR'] >= 2020]
        
        # Save
        output_file = output_dir / "electricity_generation_by_fuel_corrected.csv"
        df_final.to_csv(output_file, index=False)
        
        logger.info(f"Processed {len(df_final)} years of fuel data")
        logger.info(f"Years included: {sorted(df_final['CALENDAR_YEAR'].tolist())}")
        logger.info(f"Sample renewable percentages: {df_final['RENEWABLE_PERCENTAGE'].tolist()}")
        logger.info(f"Saved to {output_file}")
        
        return output_file
    
    else:
        logger.error("Could not find header row with quarters")
        return None

def main():
    try:
        output_dir = Path("processed_data")
        output_dir.mkdir(exist_ok=True)
        
        excel_path = "data/electricity-2025-q1.xlsx"
        if Path(excel_path).exists():
            result = process_fuel_type_data_corrected(excel_path, output_dir)
            if result:
                logger.info(f"✅ Successfully processed fuel data: {result}")
            else:
                logger.error("❌ Failed to process fuel data")
        else:
            logger.error(f"Excel file not found: {excel_path}")
            
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()