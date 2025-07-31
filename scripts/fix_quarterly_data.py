#!/usr/bin/env python3
"""
Fix quarterly generation data processing
Row 8: Quarter dates, Row 10: Net Generation values
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_quarterly_data_fixed(excel_path: str, output_dir: Path):
    """Process quarterly data with correct structure understanding"""
    logger.info("Processing quarterly generation data")
    
    # Read the quarterly sheet
    df = pd.read_excel(excel_path, sheet_name='1 - Quarterly GWh', header=None)
    logger.info(f"Raw data shape: {df.shape}")
    
    # Get quarter dates from row 8 (index 8), starting from column 1
    quarter_dates = df.iloc[8, 1:].dropna()
    logger.info(f"Found {len(quarter_dates)} quarters")
    
    # Get net generation values from row 10 (index 10), starting from column 1
    generation_values = df.iloc[10, 1:len(quarter_dates)+1]
    logger.info(f"Found {len(generation_values)} generation values")
    
    # Create records
    records = []
    for i, (quarter_date, gen_value) in enumerate(zip(quarter_dates, generation_values)):
        try:
            # Convert to proper date
            if isinstance(quarter_date, str):
                quarter_dt = pd.to_datetime(quarter_date)
            else:
                quarter_dt = quarter_date
            
            # Extract year and quarter
            year = quarter_dt.year
            quarter_num = ((quarter_dt.month - 1) // 3) + 1
            
            # Convert generation value
            if pd.notna(gen_value):
                gen_gwh = float(gen_value)
                
                records.append({
                    'CALENDAR_QUARTER': quarter_dt.strftime('%Y-%m-%d'),
                    'NET_GENERATION_GWH': gen_gwh,
                    'QUARTER_YEAR': year,
                    'QUARTER_NUMBER': quarter_num,
                    'SOURCE_SHEET': '1 - Quarterly GWh',
                    'LOAD_TIMESTAMP': datetime.now()
                })
        except Exception as e:
            logger.warning(f"Error processing quarter {i}: {e}")
            continue
    
    # Create DataFrame
    df_quarterly = pd.DataFrame(records)
    logger.info(f"Created {len(df_quarterly)} quarterly records")
    
    # Calculate year-over-year change
    df_quarterly = df_quarterly.sort_values(['QUARTER_YEAR', 'QUARTER_NUMBER'])
    df_quarterly['YEAR_OVER_YEAR_CHANGE_PERCENT'] = (
        df_quarterly.groupby('QUARTER_NUMBER')['NET_GENERATION_GWH'].pct_change(4) * 100
    ).round(4)
    
    # Filter for recent years (last 25 years)
    current_year = datetime.now().year
    df_quarterly = df_quarterly[df_quarterly['QUARTER_YEAR'] >= current_year - 25]
    
    # Save
    output_file = output_dir / "electricity_quarterly_generation_fixed.csv"
    df_quarterly.to_csv(output_file, index=False)
    
    logger.info(f"Processed {len(df_quarterly)} quarters")
    logger.info(f"Year range: {df_quarterly['QUARTER_YEAR'].min()} - {df_quarterly['QUARTER_YEAR'].max()}")
    logger.info(f"Sample generation values: {df_quarterly['NET_GENERATION_GWH'].head(5).tolist()}")
    logger.info(f"Saved to {output_file}")
    
    return output_file

def main():
    try:
        output_dir = Path("processed_data")
        output_dir.mkdir(exist_ok=True)
        
        excel_path = "data/electricity-2025-q1.xlsx"
        if Path(excel_path).exists():
            result = process_quarterly_data_fixed(excel_path, output_dir)
            if result:
                logger.info(f"✅ Successfully processed quarterly data: {result}")
                # Show sample
                df_sample = pd.read_csv(result)
                logger.info(f"Sample data:\\n{df_sample.head(3)}")
            else:
                logger.error("❌ Failed to process quarterly data")
        else:
            logger.error(f"Excel file not found: {excel_path}")
            
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()