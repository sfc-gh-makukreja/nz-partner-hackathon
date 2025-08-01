#!/usr/bin/env python3
"""
Process LINZ tide prediction CSV files into clean, structured data for Snowflake
Based on LINZ format: https://www.linz.govt.nz/guidance/marine-information/tide-prediction-guidance/tide-prediction-formats
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_tide_csv(file_path: Path):
    """
    Parse a single LINZ tide prediction CSV file
    
    CSV Format according to LINZ:
    - Line 1: Port code, name, coordinates
    - Line 2: Reference date info
    - Line 3: Time zone and units info
    - Line 4+: Day, DayOfWeek, Month, Year, Time1, Height1, Time2, Height2, Time3, Height3, Time4, Height4
    """
    logger.info(f"Processing {file_path.name}")
    
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
    
    # Parse header information and clean encoding issues
    header_line = lines[0].strip()
    # Clean BOM and encoding issues
    header_line = header_line.replace('ï»¿', '').replace('Â°', '°')
    
    # Extract port code, name, and coordinates using regex
    header_match = re.match(r'(\d+),([^,]+),([^,]+),([^,]+)', header_line)
    
    if header_match:
        port_code = header_match.group(1)
        port_name = header_match.group(2).strip()
        latitude = header_match.group(3).strip()
        longitude = header_match.group(4).strip()
    else:
        # Fallback parsing
        header_parts = header_line.split(',')
        port_code = header_parts[0].replace('ï»¿', '') if len(header_parts) > 0 else 'Unknown'
        port_name = header_parts[1] if len(header_parts) > 1 else file_path.stem.split('_')[0]
        latitude = header_parts[2].replace('Â°', '°') if len(header_parts) > 2 else 'Unknown'
        longitude = header_parts[3].replace('Â°', '°') if len(header_parts) > 3 else 'Unknown'
    
    # Extract reference date info and clean encoding
    reference_info = lines[1].strip().replace('Â°', '°') if len(lines) > 1 else "Unknown reference date"
    timezone_info = lines[2].strip().replace('Â°', '°') if len(lines) > 2 else "Local time, heights in metres"
    
    # Extract year from filename for metadata
    filename_parts = file_path.stem.split('_')
    year = filename_parts[1] if len(filename_parts) > 1 else '2024'
    
    # Parse tide data (skip header lines)
    tide_data = []
    data_start_line = 3  # Skip first 3 header lines
    
    for line_num, line in enumerate(lines[data_start_line:], start=data_start_line + 1):
        line = line.strip()
        if not line:
            continue
            
        parts = line.split(',')
        if len(parts) < 4:
            continue
            
        try:
            day = int(parts[0])
            day_of_week = parts[1]
            month = int(parts[2])
            year_val = int(parts[3])
            
            # Create base date
            try:
                date = datetime(year_val, month, day)
                date_str = date.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid date in {file_path.name}, line {line_num}: {day}/{month}/{year_val}")
                continue
            
            # Process up to 4 tides per day (E,F - G,H - I,J - K,L)
            tides_per_day = []
            for i in range(4):
                time_idx = 4 + (i * 2)  # Time columns: 4, 6, 8, 10
                height_idx = 5 + (i * 2)  # Height columns: 5, 7, 9, 11
                
                if time_idx < len(parts) and height_idx < len(parts):
                    time_str = parts[time_idx].strip()
                    height_str = parts[height_idx].strip()
                    
                    if time_str and height_str:
                        try:
                            # Parse time (format: HH:MM)
                            time_parts = time_str.split(':')
                            if len(time_parts) == 2:
                                hour = int(time_parts[0])
                                minute = int(time_parts[1])
                                
                                # Create datetime with proper time
                                tide_datetime = date.replace(hour=hour, minute=minute)
                                
                                height = float(height_str)
                                
                                # Determine tide type based on surrounding tides
                                tide_sequence = i + 1  # 1st, 2nd, 3rd, 4th tide of day
                                
                                tides_per_day.append({
                                    'port_code': port_code,
                                    'port_name': port_name,
                                    'latitude': latitude,
                                    'longitude': longitude,
                                    'date': date_str,
                                    'day_of_week': day_of_week,
                                    'tide_datetime': tide_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                                    'tide_time': time_str,
                                    'tide_height_m': height,
                                    'tide_sequence': tide_sequence,
                                    'year': year_val,
                                    'month': month,
                                    'day': day,
                                    'reference_info': reference_info,
                                    'timezone_info': timezone_info,
                                    'data_source': 'LINZ Tide Predictions',
                                    'source_file': file_path.name
                                })
                        except (ValueError, IndexError) as e:
                            # Skip invalid time/height data
                            continue
            
            tide_data.extend(tides_per_day)
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error parsing line {line_num} in {file_path.name}: {e}")
            continue
    
    logger.info(f"Processed {len(tide_data)} tide records from {file_path.name}")
    return tide_data

def process_all_tide_files():
    """
    Process all tide prediction files and create combined datasets
    """
    data_dir = Path('data')
    processed_dir = Path('processed_data')
    processed_dir.mkdir(exist_ok=True)
    
    # Find all tide prediction files
    tide_files = list(data_dir.glob('*_tide_predictions.csv'))
    
    logger.info(f"🌊 Processing {len(tide_files)} tide prediction files")
    
    all_tide_data = []
    port_metadata = []
    
    for file_path in sorted(tide_files):
        tide_data = parse_tide_csv(file_path)
        all_tide_data.extend(tide_data)
        
        # Extract unique port metadata
        if tide_data:
            port_info = {
                'port_code': tide_data[0]['port_code'],
                'port_name': tide_data[0]['port_name'],
                'latitude': tide_data[0]['latitude'], 
                'longitude': tide_data[0]['longitude'],
                'reference_info': tide_data[0]['reference_info'],
                'timezone_info': tide_data[0]['timezone_info'],
                'data_source': 'LINZ Tide Predictions',
                'load_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Check if port already exists in metadata
            if not any(p['port_code'] == port_info['port_code'] for p in port_metadata):
                port_metadata.append(port_info)
    
    # Create comprehensive tide dataset
    if all_tide_data:
        df_tides = pd.DataFrame(all_tide_data)
        df_tides['load_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Sort by port, date, and sequence
        df_tides = df_tides.sort_values(['port_name', 'date', 'tide_sequence'])
        
        # Save main tide data
        tide_output = processed_dir / 'tide_predictions_combined.csv'
        df_tides.to_csv(tide_output, index=False)
        logger.info(f"✅ Saved {len(df_tides)} tide records to {tide_output}")
        
        # Calculate tide statistics by port and year
        tide_stats = df_tides.groupby(['port_code', 'port_name', 'year']).agg({
            'tide_height_m': ['count', 'min', 'max', 'mean', 'std'],
            'date': ['min', 'max']
        }).round(3)
        
        # Flatten column names
        tide_stats.columns = ['_'.join(col).strip() for col in tide_stats.columns]
        tide_stats = tide_stats.reset_index()
        tide_stats['load_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Save tide statistics
        stats_output = processed_dir / 'tide_statistics_by_port.csv'
        tide_stats.to_csv(stats_output, index=False)
        logger.info(f"✅ Saved tide statistics to {stats_output}")
    
    # Create port metadata dataset
    if port_metadata:
        df_ports = pd.DataFrame(port_metadata)
        ports_output = processed_dir / 'tide_ports_metadata.csv'
        df_ports.to_csv(ports_output, index=False)
        logger.info(f"✅ Saved {len(df_ports)} port records to {ports_output}")
    
    # Summary
    logger.info("=" * 60)
    logger.info(f"🎯 Tide Data Processing Summary:")
    logger.info(f"   📁 Processed {len(tide_files)} source files")
    logger.info(f"   🌊 Generated {len(all_tide_data):,} total tide records")
    logger.info(f"   📍 Covered {len(port_metadata)} unique ports")
    logger.info(f"   📅 Years: {sorted(set(d['year'] for d in all_tide_data))}")
    
    # Show port coverage
    if port_metadata:
        logger.info(f"   🏙️  Ports: {', '.join([p['port_name'] for p in port_metadata])}")
    
    return len(all_tide_data), len(port_metadata)

if __name__ == "__main__":
    total_tides, total_ports = process_all_tide_files()
    logger.info(f"🌊 Tide data processing completed: {total_tides:,} records across {total_ports} ports")