#!/usr/bin/env python3
"""
Simple electricity data processor focusing on the 5-minute zone data
Creates a working foundation that can be enhanced later
"""

import pandas as pd
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

def process_zone_data_final(file_path: str, output_dir: Path):
    """Process 5-minute zone data - our main dataset"""
    logger.info(f"Processing zone data: {file_path}")
    
    df = pd.read_csv(file_path)
    logger.info(f"Original data: {len(df)} rows, {len(df.columns)} columns")
    
    # Clean column names - remove spaces and special characters
    df.columns = [col.strip().replace(' ', '_').replace('(', '').replace(')', '').upper() 
                 for col in df.columns]
    
    # Convert date column
    df['DATE'] = pd.to_datetime(df['DATE'])
    df = df.rename(columns={'DATE': 'TIMESTAMP_NZ'})
    
    # Add metadata
    df['LOAD_TIMESTAMP'] = datetime.now()
    
    # Show data info
    logger.info(f"Processed columns: {list(df.columns)}")
    logger.info(f"Date range: {df['TIMESTAMP_NZ'].min()} to {df['TIMESTAMP_NZ'].max()}")
    logger.info(f"Sample NZ total MW: {df['NZ_TOTALMW'].describe()}")
    
    # Save final file
    output_file = output_dir / "electricity_zone_data_5min_final.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Saved {len(df)} rows to {output_file}")
    
    return output_file

def create_sample_fuel_data(output_dir: Path):
    """Create sample fuel type data for demonstration"""
    logger.info("Creating sample fuel type data...")
    
    # Create sample data showing NZ's renewable energy growth
    sample_data = [
        {'CALENDAR_YEAR': 2020, 'HYDRO_GWH': 24000, 'GEOTHERMAL_GWH': 7500, 'WIND_GWH': 2800, 'SOLAR_PV_GWH': 150, 'OIL_GWH': 100, 'COAL_GWH': 2000, 'GAS_GWH': 4500},
        {'CALENDAR_YEAR': 2021, 'HYDRO_GWH': 25200, 'GEOTHERMAL_GWH': 7800, 'WIND_GWH': 3200, 'SOLAR_PV_GWH': 200, 'OIL_GWH': 80, 'COAL_GWH': 1800, 'GAS_GWH': 4200},
        {'CALENDAR_YEAR': 2022, 'HYDRO_GWH': 24800, 'GEOTHERMAL_GWH': 8000, 'WIND_GWH': 3600, 'SOLAR_PV_GWH': 280, 'OIL_GWH': 60, 'COAL_GWH': 1500, 'GAS_GWH': 3900},
        {'CALENDAR_YEAR': 2023, 'HYDRO_GWH': 26000, 'GEOTHERMAL_GWH': 8200, 'WIND_GWH': 4100, 'SOLAR_PV_GWH': 400, 'OIL_GWH': 40, 'COAL_GWH': 1200, 'GAS_GWH': 3600},
        {'CALENDAR_YEAR': 2024, 'HYDRO_GWH': 26500, 'GEOTHERMAL_GWH': 8500, 'WIND_GWH': 4800, 'SOLAR_PV_GWH': 550, 'OIL_GWH': 30, 'COAL_GWH': 900, 'GAS_GWH': 3200},
    ]
    
    df = pd.DataFrame(sample_data)
    
    # Calculate totals and percentages
    renewable_cols = ['HYDRO_GWH', 'GEOTHERMAL_GWH', 'WIND_GWH', 'SOLAR_PV_GWH']
    fossil_cols = ['OIL_GWH', 'COAL_GWH', 'GAS_GWH']
    
    df['RENEWABLE_GWH'] = df[renewable_cols].sum(axis=1)
    df['FOSSIL_FUEL_GWH'] = df[fossil_cols].sum(axis=1) 
    df['TOTAL_GENERATION_GWH'] = df['RENEWABLE_GWH'] + df['FOSSIL_FUEL_GWH']
    
    df['RENEWABLE_PERCENTAGE'] = (df['RENEWABLE_GWH'] / df['TOTAL_GENERATION_GWH'] * 100).round(2)
    df['FOSSIL_FUEL_PERCENTAGE'] = (df['FOSSIL_FUEL_GWH'] / df['TOTAL_GENERATION_GWH'] * 100).round(2)
    
    # Add other columns to match expected schema
    df['BIOGAS_GWH'] = 100  # Small constant for now
    df['ELECTRICITY_ONLY_SUBTOTAL_GWH'] = df['TOTAL_GENERATION_GWH'] * 0.9
    df['COGENERATION_GWH'] = df['TOTAL_GENERATION_GWH'] * 0.1
    df['SOURCE_SHEET'] = 'Sample Data'
    df['LOAD_TIMESTAMP'] = datetime.now()
    
    output_file = output_dir / "electricity_generation_by_fuel_final.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Created sample fuel data: {len(df)} rows saved to {output_file}")
    
    return output_file

def create_sample_quarterly_data(output_dir: Path):
    """Create sample quarterly data for demonstration"""
    logger.info("Creating sample quarterly data...")
    
    # Create sample quarterly data for recent years
    quarters = []
    base_generation = 10000
    
    for year in range(2022, 2025):
        for quarter in range(1, 5):
            # Add some seasonal variation
            seasonal_factor = [0.95, 1.02, 1.05, 0.98][quarter-1]
            generation = base_generation * seasonal_factor * (1 + (year-2022) * 0.02)
            
            quarters.append({
                'CALENDAR_QUARTER': f"{year}-{(quarter-1)*3+1:02d}-01",
                'NET_GENERATION_GWH': round(generation, 2),
                'QUARTER_YEAR': year,
                'QUARTER_NUMBER': quarter,
                'SOURCE_SHEET': 'Sample Data',
                'LOAD_TIMESTAMP': datetime.now()
            })
    
    df = pd.DataFrame(quarters)
    
    # Calculate year-over-year change
    df = df.sort_values(['QUARTER_YEAR', 'QUARTER_NUMBER'])
    df['YEAR_OVER_YEAR_CHANGE_PERCENT'] = (
        df.groupby('QUARTER_NUMBER')['NET_GENERATION_GWH'].pct_change(4) * 100
    ).round(4)
    
    output_file = output_dir / "electricity_quarterly_generation_final.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Created sample quarterly data: {len(df)} rows saved to {output_file}")
    
    return output_file

def main():
    """Main processing function"""
    try:
        output_dir = setup_directories()
        logger.info("Starting simple electricity data processing...")
        logger.info("Focus: High-quality zone data + sample tables for other themes")
        
        generated_files = []
        
        # 1. Process the real zone data (our primary dataset)
        csv_path = "data/Zone Data (01 Jul - 29 Jul) [5 intervals] (1).csv"
        if Path(csv_path).exists():
            zone_file = process_zone_data_final(csv_path, output_dir)
            generated_files.append(zone_file)
        else:
            logger.error(f"Zone data file not found: {csv_path}")
        
        # 2. Create sample fuel type data (participants can enhance this)
        fuel_file = create_sample_fuel_data(output_dir)
        generated_files.append(fuel_file)
        
        # 3. Create sample quarterly data (participants can enhance this)
        quarterly_file = create_sample_quarterly_data(output_dir)
        generated_files.append(quarterly_file)
        
        # Summary
        logger.info(f"\n🎉 Processing complete! Generated {len(generated_files)} files:")
        for file in generated_files:
            logger.info(f"✅ {file.name}")
            
            # Show sample of each file
            df_sample = pd.read_csv(file, nrows=2)
            logger.info(f"   Columns ({len(df_sample.columns)}): {list(df_sample.columns)[:5]}...")
            logger.info(f"   Sample: {len(pd.read_csv(file))} total rows")
        
        logger.info(f"\n📊 Ready for Snowflake loading!")
        logger.info(f"🚀 Next: Run 'scripts/run_full_setup.sh' to load into Snowflake")
        
        return generated_files
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()