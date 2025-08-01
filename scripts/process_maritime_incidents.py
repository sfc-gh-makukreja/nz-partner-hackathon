#!/usr/bin/env python3
"""
Process Maritime NZ accident/incident reporting data for WAITĀ schema
Data source: https://maritimenz.govt.nz/media/accacvzc/accident-incident-reporting-data.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_numeric_field(value):
    """Clean numeric fields, handle NULL strings"""
    if pd.isna(value) or str(value).upper() == 'NULL':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def clean_date_field(date_str):
    """Parse various date formats from Maritime NZ data"""
    if pd.isna(date_str) or str(date_str).upper() == 'NULL':
        return None
    
    try:
        # Handle d/m/yyyy format
        if '/' in str(date_str):
            return pd.to_datetime(date_str, format='%d/%m/%Y')
        else:
            return pd.to_datetime(date_str)
    except:
        logger.warning(f"Could not parse date: {date_str}")
        return None

def categorize_incident_severity(what_happened, injured_count, description):
    """Categorize incident severity based on type and injuries"""
    what_happened = str(what_happened).lower()
    description = str(description).lower()
    
    # Critical incidents
    if any(word in what_happened for word in ['fatal', 'death', 'foundered', 'sinking']):
        return 'Critical'
    elif any(word in description for word in ['fatal', 'death', 'foundered']):
        return 'Critical'
    
    # Major incidents
    if injured_count and injured_count > 0:
        return 'Major'
    elif any(word in what_happened for word in ['collision', 'grounding', 'fire', 'explosion']):
        return 'Major'
    elif any(word in what_happened for word in ['capsize', 'flooding', 'structural']):
        return 'Major'
    
    # Moderate incidents
    elif any(word in what_happened for word in ['contact', 'near miss', 'mechanical']):
        return 'Moderate'
    elif any(word in what_happened for word in ['propulsion', 'equipment']):
        return 'Moderate'
    
    # Minor incidents
    else:
        return 'Minor'

def process_maritime_incidents(input_file: Path, output_dir: Path):
    """Process the Maritime NZ incidents data"""
    logger.info("🚢 Processing Maritime NZ incident data...")
    
    # Read the CSV with proper encoding handling
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
    except UnicodeDecodeError:
        # Fallback to windows-1252 encoding for special characters
        logger.info("UTF-8 failed, trying windows-1252 encoding...")
        df = pd.read_csv(input_file, encoding='windows-1252')
    
    logger.info(f"Loaded {len(df)} incident records")
    
    # Clean and process the data
    processed_df = df.copy()
    
    # Clean event date
    processed_df['event_date_parsed'] = processed_df['Event Date'].apply(clean_date_field)
    processed_df['event_year'] = processed_df['event_date_parsed'].dt.year
    processed_df['event_month'] = processed_df['event_date_parsed'].dt.month
    processed_df['event_quarter'] = processed_df['event_date_parsed'].dt.quarter
    
    # Clean numeric fields
    processed_df['latitude_decimal'] = processed_df['Latitude'].apply(clean_numeric_field)
    processed_df['longitude_decimal'] = processed_df['Longitude'].apply(clean_numeric_field)
    processed_df['injured_persons'] = processed_df['Number of Injured Persons'].apply(clean_numeric_field)
    processed_df['gross_tonnage'] = processed_df['Gross Tonnage'].apply(clean_numeric_field)
    processed_df['length_overall'] = processed_df['Length Overall'].apply(clean_numeric_field)
    processed_df['year_of_build'] = processed_df['Year of Build'].apply(clean_numeric_field)
    
    # Calculate vessel age at time of incident
    processed_df['vessel_age_at_incident'] = np.where(
        (processed_df['event_year'].notna()) & (processed_df['year_of_build'].notna()),
        processed_df['event_year'] - processed_df['year_of_build'],
        None
    )
    
    # Categorize incident severity
    processed_df['incident_severity'] = processed_df.apply(
        lambda row: categorize_incident_severity(
            row['What happened'], 
            row['injured_persons'], 
            row['Brief Description']
        ), axis=1
    )
    
    # Clean text fields
    processed_df['brief_description_clean'] = processed_df['Brief Description'].str.strip()
    processed_df['what_happened_clean'] = processed_df['What happened'].str.strip()
    processed_df['event_location_clean'] = processed_df['Event Location'].str.strip()
    
    # Add data source and load timestamp
    processed_df['data_source'] = 'Maritime NZ'
    processed_df['source_url'] = 'https://maritimenz.govt.nz/media/accacvzc/accident-incident-reporting-data.csv'
    processed_df['load_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Create final output with clean column names
    output_df = pd.DataFrame({
        'event_id': processed_df['Event ID'],
        'event_date_original': processed_df['Event Date'],
        'event_date': processed_df['event_date_parsed'],
        'event_year': processed_df['event_year'],
        'event_month': processed_df['event_month'],
        'event_quarter': processed_df['event_quarter'],
        'brief_description': processed_df['brief_description_clean'],
        'what_happened': processed_df['what_happened_clean'],
        'incident_severity': processed_df['incident_severity'],
        'event_location': processed_df['event_location_clean'],
        'latitude_decimal': processed_df['latitude_decimal'],
        'longitude_decimal': processed_df['longitude_decimal'],
        'nz_region': processed_df['NZ Region'],
        'where_happened': processed_df['Where Happened'],
        'sector': processed_df['Sector'],
        'injured_persons': processed_df['injured_persons'],
        'vessel_type': processed_df['Vessel Type'],
        'safety_system': processed_df['Safety System'],
        'country_flag': processed_df['Country Flag'],
        'gross_tonnage': processed_df['gross_tonnage'],
        'length_overall': processed_df['length_overall'],
        'year_of_build': processed_df['year_of_build'],
        'vessel_age_at_incident': processed_df['vessel_age_at_incident'],
        'data_source': processed_df['data_source'],
        'source_url': processed_df['source_url'],
        'load_timestamp': processed_df['load_timestamp']
    })
    
    # Remove records with no valid coordinates
    output_df = output_df[output_df['latitude_decimal'].notna() & output_df['longitude_decimal'].notna()]
    
    # Save processed data
    output_file = output_dir / 'maritime_incidents_processed.csv'
    output_df.to_csv(output_file, index=False)
    
    # Generate summary statistics
    stats = {
        'total_incidents': len(output_df),
        'date_range': f"{output_df['event_date'].min()} to {output_df['event_date'].max()}",
        'severity_breakdown': output_df['incident_severity'].value_counts().to_dict(),
        'sector_breakdown': output_df['sector'].value_counts().to_dict(),
        'region_breakdown': output_df['nz_region'].value_counts().to_dict(),
        'total_injuries': output_df['injured_persons'].sum(),
        'incidents_with_injuries': len(output_df[output_df['injured_persons'] > 0])
    }
    
    logger.info("✅ Maritime incidents processing completed!")
    logger.info(f"📊 Processed {stats['total_incidents']} incidents")
    logger.info(f"📅 Date range: {stats['date_range']}")
    logger.info(f"🚨 Severity breakdown: {stats['severity_breakdown']}")
    logger.info(f"⚕️ Total injuries: {stats['total_injuries']} across {stats['incidents_with_injuries']} incidents")
    logger.info(f"💾 Saved to: {output_file}")
    
    return output_file, stats

def main():
    """Main processing function"""
    data_dir = Path('data')
    processed_dir = Path('processed_data')
    processed_dir.mkdir(exist_ok=True)
    
    input_file = data_dir / 'accident-incident-reporting-data.csv'
    
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return
    
    output_file, stats = process_maritime_incidents(input_file, processed_dir)
    
    logger.info("🌊 Maritime incidents data ready for Snowflake loading!")

if __name__ == "__main__":
    main()