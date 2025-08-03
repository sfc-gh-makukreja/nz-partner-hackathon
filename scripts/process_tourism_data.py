#!/usr/bin/env python3
"""
Process Tourism Statistics for HIWA_I_TE_RANGI schema
Handles: Stats NZ tourism data with complex header structures, multiple regions, time series
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import re
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_csv_robust(file_path):
    """Read CSV with fallback encoding"""
    try:
        return pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        logger.warning(f"UTF-8 failed for {file_path}, trying windows-1252")
        return pd.read_csv(file_path, encoding='windows-1252')

def parse_period_to_date(period_str):
    """Convert period strings like '1996M07', '2023' to proper dates"""
    if pd.isna(period_str) or period_str == '' or period_str == ' ':
        return None
    
    period_str = str(period_str).strip()
    
    # Handle monthly format: '1996M07'
    if 'M' in period_str:
        try:
            year, month = period_str.split('M')
            return pd.to_datetime(f"{year}-{month.zfill(2)}-01")
        except:
            return None
    
    # Handle annual format: '2023'
    try:
        year = int(period_str)
        if 1800 <= year <= 2100:  # Reasonable year range
            return pd.to_datetime(f"{year}-01-01")
    except:
        pass
    
    return None

def process_visitor_arrivals(file_path):
    """Process ITM475712 - Visitor arrival total - month ended annuals"""
    logger.info("Processing visitor arrivals data...")
    
    df = read_csv_robust(file_path)
    
    # Skip header rows and get data starting from row 2 (0-indexed)
    data_rows = df.iloc[2:].reset_index(drop=True)
    
    # Extract year and visitor count
    processed_data = []
    for _, row in data_rows.iterrows():
        try:
            year = int(row.iloc[0])
            visitor_count = pd.to_numeric(row.iloc[1], errors='coerce')
            
            if pd.notna(visitor_count) and 1800 <= year <= 2100:
                processed_data.append({
                    'report_year': year,
                    'report_date': pd.to_datetime(f"{year}-12-31"),  # Annual-Dec
                    'period_type': 'Annual',
                    'visitor_arrivals': int(visitor_count),
                    'data_source': 'Stats NZ ITM475712',
                    'dataset_description': 'Visitor arrival total - month ended annuals',
                    'load_timestamp': datetime.now()
                })
        except (ValueError, TypeError):
            continue
    
    result_df = pd.DataFrame(processed_data)
    logger.info(f"Processed {len(result_df)} visitor arrival records")
    return result_df

def process_passenger_movements(file_path):
    """Process ITM332206 - Total passenger movements"""
    logger.info("Processing passenger movements data...")
    
    df = read_csv_robust(file_path)
    
    # Skip header rows and get data starting from row 3 (0-indexed)
    data_rows = df.iloc[3:].reset_index(drop=True)
    
    processed_data = []
    for _, row in data_rows.iterrows():
        try:
            year = int(row.iloc[0])
            arrivals_actual = pd.to_numeric(row.iloc[1], errors='coerce')
            arrivals_sample = pd.to_numeric(row.iloc[2], errors='coerce')
            departures_actual = pd.to_numeric(row.iloc[3], errors='coerce')
            departures_sample = pd.to_numeric(row.iloc[4], errors='coerce')
            total_actual = pd.to_numeric(row.iloc[5], errors='coerce')
            total_sample = pd.to_numeric(row.iloc[6], errors='coerce')
            
            if 1800 <= year <= 2100:
                processed_data.append({
                    'report_year': year,
                    'report_date': pd.to_datetime(f"{year}-12-31"),  # Annual-Dec
                    'period_type': 'Annual',
                    'arrivals_actual': int(arrivals_actual) if pd.notna(arrivals_actual) else None,
                    'arrivals_sample': int(arrivals_sample) if pd.notna(arrivals_sample) else None,
                    'departures_actual': int(departures_actual) if pd.notna(departures_actual) else None,
                    'departures_sample': int(departures_sample) if pd.notna(departures_sample) else None,
                    'total_actual': int(total_actual) if pd.notna(total_actual) else None,
                    'total_sample': int(total_sample) if pd.notna(total_sample) else None,
                    'data_source': 'Stats NZ ITM332206',
                    'dataset_description': 'Total passenger movements',
                    'load_timestamp': datetime.now()
                })
        except (ValueError, TypeError):
            continue
    
    result_df = pd.DataFrame(processed_data)
    logger.info(f"Processed {len(result_df)} passenger movement records")
    return result_df

def extract_region_columns(header_row, metric_row):
    """Extract region and metric information from complex Stats NZ headers"""
    regions = []
    metrics = []
    
    current_region = None
    for i, (region_cell, metric_cell) in enumerate(zip(header_row, metric_row)):
        # Skip empty cells or period column
        if pd.isna(region_cell) or region_cell == '' or str(region_cell).strip() in [' ', '']:
            if pd.notna(metric_cell) and metric_cell != '':
                # Use metric info when region is empty
                if current_region:
                    regions.append(current_region)
                    metrics.append(str(metric_cell).strip())
                else:
                    # Skip if no region context
                    regions.append('Unknown')
                    metrics.append(str(metric_cell).strip())
            continue
        
        region_name = str(region_cell).strip()
        if region_name and region_name not in [' ', '']:
            current_region = region_name
        
        if pd.notna(metric_cell) and metric_cell != '':
            regions.append(current_region if current_region else 'Unknown')
            metrics.append(str(metric_cell).strip())
    
    return regions, metrics

def process_guest_nights(file_path):
    """Process ACS348801 - Guest Nights by Region (Monthly)"""
    logger.info("Processing guest nights data...")
    
    df = read_csv_robust(file_path)
    
    # Extract headers from rows 1 and 2 (regions and metrics)
    header_regions = df.iloc[1].values  # Regional headers
    header_metrics = df.iloc[2].values  # Metric headers
    
    # Get data starting from row 3
    data_rows = df.iloc[3:].reset_index(drop=True)
    
    # Extract region and metric mappings
    regions, metrics = extract_region_columns(header_regions, header_metrics)
    
    processed_data = []
    
    for _, row in data_rows.iterrows():
        period_str = str(row.iloc[0]).strip()
        report_date = parse_period_to_date(period_str)
        
        if report_date is None:
            continue
        
        # Process each data column
        for i, (region, metric) in enumerate(zip(regions, metrics)):
            if i + 1 >= len(row):  # Skip if no data column
                continue
                
            value = pd.to_numeric(row.iloc[i + 1], errors='coerce')
            
            if pd.notna(value) and region != 'Unknown':
                processed_data.append({
                    'report_date': report_date,
                    'period_type': 'Monthly',
                    'region': region,
                    'metric_type': metric,
                    'value': float(value),
                    'data_source': 'Stats NZ ACS348801',
                    'dataset_description': 'Guest Nights by Region (Monthly)',
                    'load_timestamp': datetime.now()
                })
    
    result_df = pd.DataFrame(processed_data)
    
    # Pivot to create separate columns for different metrics
    if len(result_df) > 0:
        result_df = result_df.pivot_table(
            index=['report_date', 'period_type', 'region', 'data_source', 'dataset_description', 'load_timestamp'],
            columns='metric_type',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Flatten column names
        result_df.columns.name = None
        
    logger.info(f"Processed {len(result_df)} guest nights records")
    return result_df

def process_occupancy_rates(file_path):
    """Process ACS348401 - Occupancy Rate by Region (Monthly)"""
    logger.info("Processing occupancy rates data...")
    
    df = read_csv_robust(file_path)
    
    # Extract headers from rows 1 and 2 (regions and metrics)
    header_regions = df.iloc[1].values
    header_metrics = df.iloc[2].values
    
    # Get data starting from row 3
    data_rows = df.iloc[3:].reset_index(drop=True)
    
    # Extract region and metric mappings
    regions, metrics = extract_region_columns(header_regions, header_metrics)
    
    processed_data = []
    
    for _, row in data_rows.iterrows():
        period_str = str(row.iloc[0]).strip()
        report_date = parse_period_to_date(period_str)
        
        if report_date is None:
            continue
        
        # Process each data column
        for i, (region, metric) in enumerate(zip(regions, metrics)):
            if i + 1 >= len(row):
                continue
                
            value = pd.to_numeric(row.iloc[i + 1], errors='coerce')
            
            if pd.notna(value) and region != 'Unknown':
                processed_data.append({
                    'report_date': report_date,
                    'period_type': 'Monthly',
                    'region': region,
                    'metric_type': metric,
                    'value': float(value),
                    'data_source': 'Stats NZ ACS348401',
                    'dataset_description': 'Occupancy Rate by Region (Monthly)',
                    'load_timestamp': datetime.now()
                })
    
    result_df = pd.DataFrame(processed_data)
    
    # Pivot to create separate columns for different metrics
    if len(result_df) > 0:
        result_df = result_df.pivot_table(
            index=['report_date', 'period_type', 'region', 'data_source', 'dataset_description', 'load_timestamp'],
            columns='metric_type',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Flatten column names
        result_df.columns.name = None
        
    logger.info(f"Processed {len(result_df)} occupancy rate records")
    return result_df

def process_migrant_arrivals(file_path):
    """Process ITM553006 - Estimated migrant arrivals (simplified version)"""
    logger.info("Processing migrant arrivals data...")
    
    df = read_csv_robust(file_path)
    
    # This dataset is extremely complex - for now, extract basic totals
    # TODO: Full implementation would parse all visa types and countries
    
    # Look for total columns (usually at the end)
    data_rows = df.iloc[6:].reset_index(drop=True)  # Skip complex headers
    
    processed_data = []
    
    for _, row in data_rows.iterrows():
        try:
            year = int(row.iloc[0])
            if 2000 <= year <= 2100:  # Focus on recent data
                # Look for total columns - usually the last meaningful data
                row_values = row.iloc[1:].values
                # Find the last non-null numeric value as total
                for val in reversed(row_values):
                    total_estimate = pd.to_numeric(val, errors='coerce')
                    if pd.notna(total_estimate) and total_estimate > 0:
                        processed_data.append({
                            'report_year': year,
                            'report_date': pd.to_datetime(f"{year}-04-30"),  # Annual-Apr
                            'period_type': 'Annual',
                            'total_migrant_arrivals': int(total_estimate),
                            'data_source': 'Stats NZ ITM553006',
                            'dataset_description': 'Estimated migrant arrivals by citizenship, visa type (Total)',
                            'load_timestamp': datetime.now()
                        })
                        break
        except (ValueError, TypeError):
            continue
    
    result_df = pd.DataFrame(processed_data)
    logger.info(f"Processed {len(result_df)} migrant arrival records")
    return result_df

def clean_region_names(df, region_column='region'):
    """Standardize region names for consistency"""
    if region_column not in df.columns:
        return df
    
    region_mapping = {
        'Auckland': 'Auckland',
        'Wellington': 'Wellington',
        'Canterbury': 'Canterbury',
        'Waikato': 'Waikato',
        'Bay of Plenty': 'Bay of Plenty',
        'Hawke\'s Bay, Gisborne': 'Hawke\'s Bay',
        'Taranaki, Manawatu, Wanganui': 'Taranaki-Manawatu-Whanganui',
        'Nelson, Marlborough, Tasman': 'Tasman-Nelson-Marlborough',
        'West Coast': 'West Coast',
        'Otago': 'Otago',
        'Southland': 'Southland',
        'Northland': 'Northland',
        'North Island': 'North Island',
        'South Island': 'South Island',
        'New Zealand': 'New Zealand'
    }
    
    # Apply mapping with fallback to original value
    df[region_column] = df[region_column].map(region_mapping).fillna(df[region_column])
    
    return df

def save_processed_data(df, filename, description):
    """Save processed data to CSV with consistent formatting"""
    if len(df) == 0:
        logger.warning(f"No data to save for {filename}")
        return None
        
    try:
        # Create output directory if it doesn't exist
        output_dir = Path('processed_data')
        output_dir.mkdir(exist_ok=True)
        
        output_path = output_dir / filename
        
        # Save to CSV with date formatting
        df.to_csv(output_path, index=False, date_format='%Y-%m-%d')
        
        logger.info(f"Saved {len(df)} {description} records to {output_path}")
        
        # Print summary statistics
        print(f"\n{description.upper()} SUMMARY:")
        print(f"Records: {len(df)}")
        if 'report_date' in df.columns:
            print(f"Date Range: {df['report_date'].min()} to {df['report_date'].max()}")
        elif 'report_year' in df.columns:
            print(f"Year Range: {df['report_year'].min()} to {df['report_year'].max()}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")
        raise

def main():
    """Main processing pipeline"""
    
    tourism_dir = Path('data/tourism')
    
    # File mappings
    files_to_process = {
        'ITM475712_20250803_062609_43.csv': ('visitor_arrivals.csv', 'visitor arrivals', process_visitor_arrivals),
        'ITM332206_20250803_062455_0.csv': ('passenger_movements.csv', 'passenger movements', process_passenger_movements),
        'ACS348801_20250803_062820_30.csv': ('guest_nights_by_region.csv', 'guest nights', process_guest_nights),
        'ACS348401_20250803_062914_74.csv': ('occupancy_rates_by_region.csv', 'occupancy rates', process_occupancy_rates),
        'ITM553006_20250803_062257_32.csv': ('migrant_arrivals.csv', 'migrant arrivals', process_migrant_arrivals)
    }
    
    print("="*80)
    print("TOURISM DATA PROCESSING - HIWA_I_TE_RANGI")
    print("="*80)
    
    processed_files = []
    total_records = 0
    
    for input_file, (output_file, description, processor_func) in files_to_process.items():
        input_path = tourism_dir / input_file
        
        if not input_path.exists():
            logger.warning(f"File not found: {input_path}")
            continue
        
        try:
            logger.info(f"Processing {input_file}...")
            
            # Process the data
            df = processor_func(input_path)
            
            # Clean region names if applicable
            if 'region' in df.columns:
                df = clean_region_names(df)
            
            # Save processed data
            output_path = save_processed_data(df, output_file, description)
            
            if output_path:
                processed_files.append(output_path)
                total_records += len(df)
            
        except Exception as e:
            logger.error(f"Error processing {input_file}: {e}")
            continue
    
    print(f"\n{'='*80}")
    print("PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"Files Processed: {len(processed_files)}")
    print(f"Total Records: {total_records:,}")
    print(f"\nProcessed Files:")
    for file_path in processed_files:
        print(f"  • {file_path}")
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"1. Run: snow sql -f scripts/setup_hiwa_i_te_rangi.sql")
    print(f"2. Load data: COPY INTO tables FROM @tourism_data_stage/")
    print(f"3. Test queries: snow sql -f sample_queries/HIWA_I_TE_RANGI_tourism_events_queries.sql")
    
    return 0

if __name__ == "__main__":
    exit(main())