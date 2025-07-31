#!/usr/bin/env python3
"""
Complete WAIPUNA_RANGI Data Processing Script
Processes all water, rain, and flood-related datasets for the WAIPUNA_RANGI theme:
- NIWA Climate Data (rainfall & temperature)
- Waipa District Flood Zones (Waikato Regional Hazards Portal)  
- ICNZ Natural Disaster Cost Data

Data Sources:
- NIWA: https://niwa.co.nz/climate-and-weather/climate-data/national-climate-database/climate-stations-statistics
- Waikato: https://www.waikatoregion.govt.nz/services/regional-hazards-and-emergency-management/regional-hazards-portal/
- ICNZ: https://www.icnz.org.nz/industry/cost-of-natural-disasters/
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import zipfile
import json
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_waipa_flood_data(data_dir: Path, processed_dir: Path):
    """Process Waipa District flood zone data from Waikato Regional Hazards Portal"""
    logger.info("🗺️  Processing Waipa District flood zone data...")
    
    # Find flood CSV and GeoJSON files
    flood_csv_files = list(data_dir.glob('WaipaDistrictPlan_SpecialFeature_Area_Flood_*.csv'))
    flood_geojson_files = list(data_dir.glob('WaipaDistrictPlan_SpecialFeature_Area_Flood_*.geojson'))
    
    if not flood_csv_files or not flood_geojson_files:
        logger.warning("No Waipa flood data files found")
        return None, None
    
    # Process CSV metadata
    df_flood_zones = pd.read_csv(flood_csv_files[0])
    df_flood_zones['load_timestamp'] = datetime.now()
    df_flood_zones['data_source'] = 'Waikato Regional Hazards Portal'
    df_flood_zones['source_url'] = 'https://www.waikatoregion.govt.nz/services/regional-hazards-and-emergency-management/regional-hazards-portal/'
    
    # Clean column names for Snowflake
    df_flood_zones = df_flood_zones.rename(columns={
        'Shape__Area': 'shape_area_sqm',
        'Shape__Length': 'shape_length_m'
    })
    
    output_file = processed_dir / "waipa_flood_zones.csv"
    df_flood_zones.to_csv(output_file, index=False)
    logger.info(f"✅ Processed flood zones: {output_file} ({len(df_flood_zones)} zones)")
    
    # Process GeoJSON boundaries (extract key properties for simplified table)
    with open(flood_geojson_files[0], 'r') as f:
        geojson_data = json.load(f)
    
    boundaries_data = []
    for feature in geojson_data['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        
        boundaries_data.append({
            'fid': properties.get('FID'),
            'flood_zone_id': properties.get('id'),
            'geometry_type': geometry['type'],
            'coordinate_count': len(geometry['coordinates'][0]) if geometry['type'] == 'Polygon' else 0,
            'geometry_json': json.dumps(geometry),
            'load_timestamp': datetime.now(),
            'data_source': 'Waikato Regional Hazards Portal'
        })
    
    df_boundaries = pd.DataFrame(boundaries_data)
    boundaries_output = processed_dir / "waipa_flood_boundaries.csv"
    df_boundaries.to_csv(boundaries_output, index=False)
    logger.info(f"✅ Processed flood boundaries: {boundaries_output} ({len(df_boundaries)} polygons)")
    
    return output_file, boundaries_output

def process_icnz_disaster_costs(data_dir: Path, processed_dir: Path):
    """Process ICNZ Natural Disaster Cost data"""
    logger.info("💰 Processing ICNZ Natural Disaster Cost data...")
    
    cost_file = data_dir / "Cost Of Natural Disasters Table (NZ).csv"
    if not cost_file.exists():
        logger.warning("ICNZ cost data file not found")
        return None
    
    df_costs = pd.read_csv(cost_file)
    
    # Clean the data
    df_costs = df_costs.rename(columns={
        'Cost ($m)': 'cost_millions_nzd',
        'Inflation adjusted cost ($m)': 'inflation_adjusted_cost_millions_nzd'
    })
    
    # Parse dates
    df_costs['event_date'] = pd.to_datetime(df_costs['Date'], errors='coerce')
    df_costs['event_year'] = df_costs['event_date'].dt.year
    
    # Extract event type from Categories
    df_costs['primary_category'] = df_costs['Categories'].fillna('').str.split(',').str[0].str.strip()
    
    # Classify water-related disasters
    water_related_keywords = ['flood', 'storm', 'rain', 'cyclone', 'water']
    df_costs['is_water_related'] = df_costs['Event'].str.lower().str.contains('|'.join(water_related_keywords), na=False) | \
                                   df_costs['Categories'].str.lower().str.contains('|'.join(water_related_keywords), na=False)
    
    # Add metadata
    df_costs['load_timestamp'] = datetime.now()
    df_costs['data_source'] = 'Insurance Council of New Zealand (ICNZ)'
    df_costs['source_url'] = 'https://www.icnz.org.nz/industry/cost-of-natural-disasters/'
    
    # Remove HTML content from More Info column
    df_costs['more_info_available'] = df_costs['More Info'].str.contains('btn', na=False)
    
    # Select and order columns
    df_final = df_costs[[
        'Date', 'event_date', 'event_year', 'Event', 'Categories', 'primary_category',
        'cost_millions_nzd', 'inflation_adjusted_cost_millions_nzd', 'is_water_related',
        'more_info_available', 'data_source', 'source_url', 'load_timestamp'
    ]]
    
    # Filter out invalid years and costs
    df_final = df_final[df_final['event_year'].notna()]
    df_final = df_final[df_final['cost_millions_nzd'].notna()]
    
    output_file = processed_dir / "icnz_disaster_costs.csv"
    df_final.to_csv(output_file, index=False)
    
    water_related_count = df_final['is_water_related'].sum()
    total_water_cost = pd.to_numeric(df_final[df_final['is_water_related']]['inflation_adjusted_cost_millions_nzd'], errors='coerce').sum()
    
    logger.info(f"✅ Processed disaster costs: {output_file} ({len(df_final)} events)")
    logger.info(f"Year range: {df_final['event_year'].min()} - {df_final['event_year'].max()}")
    logger.info(f"Water-related events: {water_related_count}/{len(df_final)} (${total_water_cost:.1f}M NZD)")
    
    return output_file

def process_niwa_climate_data(data_dir: Path, processed_dir: Path):
    """Process NIWA climate data (using existing logic)"""
    logger.info("🌧️  Processing NIWA climate data...")
    
    zip_files = list(data_dir.glob('*_Rain.zip')) + list(data_dir.glob('*_Temperature.zip'))
    if not zip_files:
        logger.warning("No NIWA climate zip files found")
        return []
    
    logger.info(f"Found {len(zip_files)} NIWA climate data files")

    all_annual_rainfall_data = []
    all_monthly_rainfall_data = []
    all_annual_temperature_data = []
    all_monthly_temperature_data = []

    for zip_file in zip_files:
        station_id = int(re.search(r'(\d+)_', zip_file.name).group(1))
        station_name = f"NIWA Station {station_id}"
        data_type = 'rain' if 'Rain' in zip_file.name else 'temperature'
        logger.info(f"Processing Station {station_id} - {data_type} data")

        with zipfile.ZipFile(zip_file, 'r') as zf:
            for member in zf.namelist():
                if member.endswith('.csv'):
                    with zf.open(member) as f:
                        df = pd.read_csv(f)
                        
                        # Add common columns
                        df['station_id'] = station_id
                        df['station_name'] = station_name
                        df['load_timestamp'] = datetime.now()

                        if data_type == 'rain':
                            if '__annual__' in member and 'Total_rainfall' in member:
                                df = df.rename(columns={'STATS_VALUE': 'total_rainfall_mm'})
                                all_annual_rainfall_data.append(df)
                            elif '__monthly__' in member and 'Total_rainfall' in member:
                                df = df.rename(columns={'STATS_VALUE': 'total_rainfall_mm'})
                                all_monthly_rainfall_data.append(df)
                            elif '__annual__' in member and 'Rain_Days' in member:
                                df = df.rename(columns={'STATS_VALUE': 'rain_days_count'})
                                # Merge with existing annual data or create new entries
                                all_annual_rainfall_data.append(df)
                            elif '__monthly__' in member and 'Rain_Days' in member:
                                df = df.rename(columns={'STATS_VALUE': 'rain_days_count'})
                                all_monthly_rainfall_data.append(df)
                            elif '__annual__' in member and 'Total_Runoff' in member:
                                df = df.rename(columns={'STATS_VALUE': 'total_runoff_mm'})
                                all_annual_rainfall_data.append(df)
                            elif '__monthly__' in member and 'Total_Runoff' in member:
                                df = df.rename(columns={'STATS_VALUE': 'total_runoff_mm'})
                                all_monthly_rainfall_data.append(df)
                            elif '__annual__' in member and 'Total_Deficit' in member:
                                df = df.rename(columns={'STATS_VALUE': 'total_deficit_mm'})
                                all_annual_rainfall_data.append(df)
                            elif '__monthly__' in member and 'Total_Deficit' in member:
                                df = df.rename(columns={'STATS_VALUE': 'total_deficit_mm'})
                                all_monthly_rainfall_data.append(df)
                        elif data_type == 'temperature':
                            if '__annual__' in member and 'Mean_Air_Temperature' in member:
                                df = df.rename(columns={'STATS_VALUE': 'mean_temperature_c'})
                                all_annual_temperature_data.append(df)
                            elif '__monthly__' in member and 'Mean_Air_Temperature' in member:
                                df = df.rename(columns={'STATS_VALUE': 'mean_temperature_c'})
                                all_monthly_temperature_data.append(df)
                            elif '__annual__' in member and 'Mean_daily_maximum_air_temperature' in member:
                                df = df.rename(columns={'STATS_VALUE': 'mean_max_temperature_c'})
                                all_annual_temperature_data.append(df)
                            elif '__annual__' in member and 'Mean_daily_minimum_air_temperature' in member:
                                df = df.rename(columns={'STATS_VALUE': 'mean_min_temperature_c'})
                                all_annual_temperature_data.append(df)

    logger.info("Creating combined CSV files for Snowflake...")
    output_files = []

    # Combine and save annual rainfall
    if all_annual_rainfall_data:
        df_annual_rainfall = pd.concat(all_annual_rainfall_data, ignore_index=True)
        df_annual_rainfall = df_annual_rainfall.pivot_table(
            index=['YEAR', 'station_id', 'station_name', 'load_timestamp'],
            values=['total_rainfall_mm', 'rain_days_count', 'total_runoff_mm', 'total_deficit_mm'],
            aggfunc='first'
        ).reset_index()
        df_annual_rainfall.columns.name = None
        df_annual_rainfall = df_annual_rainfall.rename(columns={'YEAR': 'year'})
        
        output_file = processed_dir / "rainfall_annual_combined.csv"
        df_annual_rainfall.to_csv(output_file, index=False)
        output_files.append(output_file)
        logger.info(f"✅ Created rainfall_annual_combined.csv ({len(df_annual_rainfall)} records)")

    # Combine and save monthly rainfall
    if all_monthly_rainfall_data:
        df_monthly_rainfall = pd.concat(all_monthly_rainfall_data, ignore_index=True)
        df_monthly_rainfall['month_number'] = df_monthly_rainfall['PERIOD'].apply(
            lambda x: datetime.strptime(x, '%B').month if isinstance(x, str) else None)
        df_monthly_rainfall = df_monthly_rainfall.pivot_table(
            index=['PERIOD', 'YEAR', 'station_id', 'station_name', 'load_timestamp', 'month_number'],
            values=['total_rainfall_mm', 'rain_days_count', 'total_runoff_mm', 'total_deficit_mm'],
            aggfunc='first'
        ).reset_index()
        df_monthly_rainfall.columns.name = None
        df_monthly_rainfall = df_monthly_rainfall.rename(columns={'PERIOD': 'month_name', 'YEAR': 'year'})
        
        output_file = processed_dir / "rainfall_monthly_combined.csv"
        df_monthly_rainfall.to_csv(output_file, index=False)
        output_files.append(output_file)
        logger.info(f"✅ Created rainfall_monthly_combined.csv ({len(df_monthly_rainfall)} records)")

    # Combine and save annual temperature
    if all_annual_temperature_data:
        df_annual_temperature = pd.concat(all_annual_temperature_data, ignore_index=True)
        df_annual_temperature = df_annual_temperature.pivot_table(
            index=['YEAR', 'station_id', 'station_name', 'load_timestamp'],
            values=['mean_temperature_c', 'mean_max_temperature_c', 'mean_min_temperature_c'],
            aggfunc='first'
        ).reset_index()
        df_annual_temperature.columns.name = None
        df_annual_temperature = df_annual_temperature.rename(columns={'YEAR': 'year'})
        
        output_file = processed_dir / "temperature_annual_combined.csv"
        df_annual_temperature.to_csv(output_file, index=False)
        output_files.append(output_file)
        logger.info(f"✅ Created temperature_annual_combined.csv ({len(df_annual_temperature)} records)")

    # Combine and save monthly temperature  
    if all_monthly_temperature_data:
        df_monthly_temperature = pd.concat(all_monthly_temperature_data, ignore_index=True)
        df_monthly_temperature['month_number'] = df_monthly_temperature['PERIOD'].apply(
            lambda x: datetime.strptime(x, '%B').month if isinstance(x, str) else None)
        
        # Get available temperature columns
        available_temp_cols = [col for col in ['mean_temperature_c', 'mean_max_temperature_c', 'mean_min_temperature_c'] 
                              if col in df_monthly_temperature.columns]
        
        if available_temp_cols:
            df_monthly_temperature = df_monthly_temperature.pivot_table(
                index=['PERIOD', 'YEAR', 'station_id', 'station_name', 'load_timestamp', 'month_number'],
                values=available_temp_cols,
                aggfunc='first'
            ).reset_index()
            df_monthly_temperature.columns.name = None
            df_monthly_temperature = df_monthly_temperature.rename(columns={'PERIOD': 'month_name', 'YEAR': 'year'})
            
            output_file = processed_dir / "temperature_monthly_combined.csv"
            df_monthly_temperature.to_csv(output_file, index=False)
            output_files.append(output_file)
            logger.info(f"✅ Created temperature_monthly_combined.csv ({len(df_monthly_temperature)} records)")
        else:
            logger.warning("No valid monthly temperature columns found")

    return output_files

def main():
    """Main processing function for all WAIPUNA_RANGI datasets"""
    logger.info("🌊 Starting Complete WAIPUNA_RANGI Data Processing")
    logger.info("=" * 60)
    
    data_dir = Path('data')
    processed_dir = Path('processed_data')
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    all_outputs = []
    
    # Process Waipa flood data
    flood_outputs = process_waipa_flood_data(data_dir, processed_dir)
    if flood_outputs[0]:
        all_outputs.extend([f for f in flood_outputs if f])
    
    # Process ICNZ disaster costs
    cost_output = process_icnz_disaster_costs(data_dir, processed_dir)
    if cost_output:
        all_outputs.append(cost_output)
    
    # Process NIWA climate data
    climate_outputs = process_niwa_climate_data(data_dir, processed_dir)
    all_outputs.extend(climate_outputs)
    
    logger.info("=" * 60)
    logger.info(f"✅ WAIPUNA_RANGI processing completed successfully!")
    logger.info(f"Generated {len(all_outputs)} processed data files:")
    for output in all_outputs:
        logger.info(f"  📄 {output}")
    
    return all_outputs

if __name__ == "__main__":
    main()