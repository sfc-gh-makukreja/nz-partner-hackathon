#!/usr/bin/env python3
"""
Process NIWA Climate Station Data for WAIPUNA_RANGI Schema
Handles rainfall and temperature data from multiple stations
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import zipfile
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_station_data(zip_path: Path, output_dir: Path):
    """Extract and organize NIWA station data from zip files"""
    station_id = zip_path.stem.split('_')[0]  # Extract station ID from filename
    data_type = zip_path.stem.split('_')[1].lower()  # 'rain' or 'temperature'
    
    logger.info(f"Processing Station {station_id} - {data_type} data")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir / f"station_{station_id}_{data_type}")
    
    return station_id, data_type

def process_rainfall_data(station_id: str, data_dir: Path, output_dir: Path):
    """Process rainfall data for a station"""
    logger.info(f"Processing rainfall data for station {station_id}")
    
    # Process annual rainfall data
    annual_files = {
        'total_rainfall': f"{station_id}__annual__Total_rainfall__mm.csv",
        'rain_days': f"{station_id}__annual__Rain_Days__with_0_1mm_or_more___days.csv",
        'runoff': f"{station_id}__annual__Total_Runoff__WBal_AWC_150mm___mm.csv",
        'deficit': f"{station_id}__annual__Total_Deficit__WBal_AWC_150mm___mm.csv"
    }
    
    annual_data = {}
    for param, filename in annual_files.items():
        file_path = data_dir / filename
        if file_path.exists():
            df = pd.read_csv(file_path)
            annual_data[param] = df.set_index('YEAR')['STATS_VALUE']
    
    # Combine annual data
    if annual_data:
        annual_df = pd.DataFrame(annual_data).reset_index()
        annual_df['station_id'] = int(station_id)
        annual_df['station_name'] = f"NIWA Station {station_id}"
        annual_df['load_timestamp'] = datetime.now()
        
        # Rename columns to match schema
        annual_df = annual_df.rename(columns={
            'YEAR': 'year',
            'total_rainfall': 'total_rainfall_mm',
            'rain_days': 'rain_days_count',
            'runoff': 'total_runoff_mm',
            'deficit': 'total_deficit_mm'
        })
        
        # Save annual rainfall data
        annual_output = output_dir / f"rainfall_annual_station_{station_id}.csv"
        annual_df.to_csv(annual_output, index=False)
        logger.info(f"Saved annual rainfall data: {annual_output}")
    
    # Process monthly rainfall data
    monthly_files = {
        'total_rainfall': f"{station_id}__monthly__Total_rainfall__mm.csv",
        'rain_days': f"{station_id}__monthly__Rain_Days__with_0_1mm_or_more___days.csv",
        'runoff': f"{station_id}__monthly__Total_Runoff__WBal_AWC_150mm___mm.csv",
        'deficit': f"{station_id}__monthly__Total_Deficit__WBal_AWC_150mm___mm.csv"
    }
    
    monthly_data = {}
    for param, filename in monthly_files.items():
        file_path = data_dir / filename
        if file_path.exists():
            df = pd.read_csv(file_path)
            df['param'] = param
            monthly_data[param] = df
    
    # Combine monthly data
    if monthly_data:
        monthly_df = pd.concat(monthly_data.values(), ignore_index=True)
        
        # Pivot to get parameters as columns
        monthly_pivot = monthly_df.pivot_table(
            index=['PERIOD', 'YEAR'], 
            columns='param', 
            values='STATS_VALUE'
        ).reset_index()
        
        # Add metadata
        monthly_pivot['station_id'] = int(station_id)
        monthly_pivot['station_name'] = f"NIWA Station {station_id}"
        monthly_pivot['month_name'] = monthly_pivot['PERIOD']
        monthly_pivot['load_timestamp'] = datetime.now()
        
        # Convert month names to numbers
        month_map = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        monthly_pivot['month_number'] = monthly_pivot['month_name'].map(month_map)
        
        # Rename columns to match schema
        monthly_pivot = monthly_pivot.rename(columns={
            'YEAR': 'year',
            'total_rainfall': 'total_rainfall_mm',
            'rain_days': 'rain_days_count',
            'runoff': 'total_runoff_mm',
            'deficit': 'total_deficit_mm'
        })
        
        # Save monthly rainfall data
        monthly_output = output_dir / f"rainfall_monthly_station_{station_id}.csv"
        monthly_pivot.to_csv(monthly_output, index=False)
        logger.info(f"Saved monthly rainfall data: {monthly_output}")

def process_temperature_data(station_id: str, data_dir: Path, output_dir: Path):
    """Process temperature data for a station"""
    logger.info(f"Processing temperature data for station {station_id}")
    
    # Process annual temperature data
    annual_files = {
        'mean_temp': f"{station_id}__annual__Mean_Air_Temperature__Deg_C.csv",
        'mean_max': f"{station_id}__annual__Mean_Daily_Maximum_Air_Temperatures__Deg_C.csv",
        'mean_min': f"{station_id}__annual__Mean_Daily_Minimum_Air_Temperatures__Deg_C.csv",
        'grass_min': f"{station_id}__annual__Extreme_Grass_Minimum_Temperature__DegC.csv",
        'earth_temp': f"{station_id}__annual__Mean_10cm_Earth_Temperature__DegC.csv",
        'temp_std': f"{station_id}__annual__Standard_Deviation_of_Mean_Air_Temperature__DegC.csv",
        'frost_days': f"{station_id}__annual__Days_of_occurrence__Ground_Frost__days.csv"
    }
    
    annual_data = {}
    for param, filename in annual_files.items():
        file_path = data_dir / filename
        if file_path.exists():
            df = pd.read_csv(file_path)
            if not df.empty:
                annual_data[param] = df.set_index('YEAR')['STATS_VALUE']
    
    # Combine annual data
    if annual_data:
        annual_df = pd.DataFrame(annual_data).reset_index()
        annual_df['station_id'] = int(station_id)
        annual_df['station_name'] = f"NIWA Station {station_id}"
        annual_df['load_timestamp'] = datetime.now()
        
        # Rename columns to match schema
        annual_df = annual_df.rename(columns={
            'YEAR': 'year',
            'mean_temp': 'mean_temperature_c',
            'mean_max': 'mean_max_temperature_c',
            'mean_min': 'mean_min_temperature_c',
            'grass_min': 'extreme_grass_min_c',
            'earth_temp': 'earth_temperature_10cm_c',
            'temp_std': 'temperature_std_dev',
            'frost_days': 'ground_frost_days'
        })
        
        # Save annual temperature data
        annual_output = output_dir / f"temperature_annual_station_{station_id}.csv"
        annual_df.to_csv(annual_output, index=False)
        logger.info(f"Saved annual temperature data: {annual_output}")
    
    # Process monthly temperature data (simplified - just main parameters)
    monthly_files = {
        'mean_temp': f"{station_id}__monthly__Mean_Air_Temperature__Deg_C.csv",
        'mean_max': f"{station_id}__monthly__Mean_Daily_Maximum_Air_Temperatures__Deg_C.csv",
        'mean_min': f"{station_id}__monthly__Mean_Daily_Minimum_Air_Temperatures__Deg_C.csv"
    }
    
    monthly_data = {}
    for param, filename in monthly_files.items():
        file_path = data_dir / filename
        if file_path.exists():
            df = pd.read_csv(file_path)
            if not df.empty:
                df['param'] = param
                monthly_data[param] = df
    
    # Combine monthly data
    if monthly_data:
        monthly_df = pd.concat(monthly_data.values(), ignore_index=True)
        
        # Pivot to get parameters as columns
        monthly_pivot = monthly_df.pivot_table(
            index=['PERIOD', 'YEAR'], 
            columns='param', 
            values='STATS_VALUE'
        ).reset_index()
        
        # Add metadata
        monthly_pivot['station_id'] = int(station_id)
        monthly_pivot['station_name'] = f"NIWA Station {station_id}"
        monthly_pivot['month_name'] = monthly_pivot['PERIOD']
        monthly_pivot['load_timestamp'] = datetime.now()
        
        # Convert month names to numbers
        month_map = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        monthly_pivot['month_number'] = monthly_pivot['month_name'].map(month_map)
        
        # Rename columns to match schema
        monthly_pivot = monthly_pivot.rename(columns={
            'YEAR': 'year',
            'mean_temp': 'mean_temperature_c',
            'mean_max': 'mean_max_temperature_c',
            'mean_min': 'mean_min_temperature_c'
        })
        
        # Save monthly temperature data
        monthly_output = output_dir / f"temperature_monthly_station_{station_id}.csv"
        monthly_pivot.to_csv(monthly_output, index=False)
        logger.info(f"Saved monthly temperature data: {monthly_output}")

def main():
    """Main processing function"""
    try:
        # Setup directories
        data_dir = Path("data")
        output_dir = Path("processed_data")
        temp_dir = Path("temp_niwa")
        temp_dir.mkdir(exist_ok=True)
        output_dir.mkdir(exist_ok=True)
        
        # Find all NIWA zip files
        zip_files = list(data_dir.glob("*_Rain.zip")) + list(data_dir.glob("*_Temperature.zip"))
        logger.info(f"Found {len(zip_files)} NIWA climate data files")
        
        # Extract all zip files
        for zip_path in zip_files:
            station_id, data_type = extract_station_data(zip_path, temp_dir)
            
            station_data_dir = temp_dir / f"station_{station_id}_{data_type}"
            
            if data_type == 'rain':
                process_rainfall_data(station_id, station_data_dir, output_dir)
            elif data_type == 'temperature':
                process_temperature_data(station_id, station_data_dir, output_dir)
        
        # Create combined files for Snowflake loading
        logger.info("Creating combined CSV files for Snowflake...")
        
        # Combine all rainfall annual data
        rainfall_annual_files = list(output_dir.glob("rainfall_annual_station_*.csv"))
        if rainfall_annual_files:
            combined_rainfall_annual = pd.concat([pd.read_csv(f) for f in rainfall_annual_files], ignore_index=True)
            combined_rainfall_annual.to_csv(output_dir / "rainfall_annual_combined.csv", index=False)
            logger.info("Created rainfall_annual_combined.csv")
        
        # Combine all rainfall monthly data
        rainfall_monthly_files = list(output_dir.glob("rainfall_monthly_station_*.csv"))
        if rainfall_monthly_files:
            combined_rainfall_monthly = pd.concat([pd.read_csv(f) for f in rainfall_monthly_files], ignore_index=True)
            combined_rainfall_monthly.to_csv(output_dir / "rainfall_monthly_combined.csv", index=False)
            logger.info("Created rainfall_monthly_combined.csv")
        
        # Combine all temperature annual data
        temperature_annual_files = list(output_dir.glob("temperature_annual_station_*.csv"))
        if temperature_annual_files:
            combined_temperature_annual = pd.concat([pd.read_csv(f) for f in temperature_annual_files], ignore_index=True)
            combined_temperature_annual.to_csv(output_dir / "temperature_annual_combined.csv", index=False)
            logger.info("Created temperature_annual_combined.csv")
        
        # Combine all temperature monthly data
        temperature_monthly_files = list(output_dir.glob("temperature_monthly_station_*.csv"))
        if temperature_monthly_files:
            combined_temperature_monthly = pd.concat([pd.read_csv(f) for f in temperature_monthly_files], ignore_index=True)
            combined_temperature_monthly.to_csv(output_dir / "temperature_monthly_combined.csv", index=False)
            logger.info("Created temperature_monthly_combined.csv")
        
        # Cleanup temp directory
        import shutil
        shutil.rmtree(temp_dir)
        
        logger.info("✅ NIWA climate data processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()