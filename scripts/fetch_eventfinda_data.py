#!/usr/bin/env python3
"""
Fetch EventFinda RSS Feed for HIWA_I_TE_RANGI Tourism Events
Processes RSS feed into structured CSV for Snowflake loading

Data Source: https://www.eventfinda.co.nz/feed/events/new-zealand/whatson/upcoming.rss
Theme: HIWA_I_TE_RANGI (Travel & Tourism)
"""

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import re
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_eventfinda_rss(rss_url: str) -> str:
    """Fetch RSS feed content from EventFinda"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; NZ-Hackathon-Data-Processor/1.0)',
            'Accept': 'application/rss+xml, application/xml, text/xml'
        }
        
        response = requests.get(rss_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Successfully fetched RSS feed: {len(response.content)} bytes")
        return response.content.decode('utf-8')
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch RSS feed: {e}")
        raise

def parse_event_description(description: str) -> dict:
    """Extract structured data from event description HTML"""
    
    # Remove HTML tags and clean up text
    clean_desc = re.sub(r'<[^>]+>', '', description)
    clean_desc = re.sub(r'&[a-zA-Z]+;', ' ', clean_desc)  # Remove HTML entities
    clean_desc = re.sub(r'\s+', ' ', clean_desc).strip()
    
    # Extract location and date info (format: "Location | Date")
    location_match = re.search(r'([^|]+)\|\s*([^|]+)$', clean_desc)
    
    location = None
    date_info = None
    
    if location_match:
        location = location_match.group(1).strip()
        date_info = location_match.group(2).strip()
        # Remove location/date from description
        description_text = clean_desc[:location_match.start()].strip()
    else:
        description_text = clean_desc
    
    return {
        'description_text': description_text,
        'location': location,
        'date_info': date_info
    }

def parse_event_dates(date_str: str) -> dict:
    """Parse date strings to extract start/end dates"""
    
    if not date_str:
        return {'start_date': None, 'end_date': None, 'is_recurring': False}
    
    # Look for date patterns
    date_patterns = [
        r'(\w+day),\s*(\d{1,2})\s+(\w+)\s+(\d{4})',  # "Sunday, 3 August 2025"
        r'(\d{1,2})\s+(\w+)\s+(\d{4})',               # "3 August 2025"
    ]
    
    start_date = None
    end_date = None
    is_recurring = 'every' in date_str.lower() or ' - ' in date_str
    
    for pattern in date_patterns:
        matches = re.findall(pattern, date_str)
        if matches:
            try:
                # Parse first date as start date
                if len(matches[0]) == 4:  # With day name
                    day, month, year = matches[0][1], matches[0][2], matches[0][3]
                else:  # Without day name
                    day, month, year = matches[0]
                
                start_date = pd.to_datetime(f"{day} {month} {year}", format="%d %B %Y")
                
                # If multiple dates, use last as end date
                if len(matches) > 1:
                    last_match = matches[-1]
                    if len(last_match) == 4:
                        day, month, year = last_match[1], last_match[2], last_match[3]
                    else:
                        day, month, year = last_match
                    end_date = pd.to_datetime(f"{day} {month} {year}", format="%d %B %Y")
                else:
                    end_date = start_date
                    
                break
                    
            except Exception as e:
                logger.warning(f"Could not parse date '{date_str}': {e}")
                continue
    
    return {
        'start_date': start_date,
        'end_date': end_date, 
        'is_recurring': is_recurring
    }

def extract_event_category(title: str, description: str) -> str:
    """Categorize events based on title and description"""
    
    content = f"{title} {description}".lower()
    
    # Event categories based on EventFinda data patterns
    categories = {
        'Music & Performance': ['jazz', 'concert', 'music', 'performance', 'orchestra', 'band', 'singing'],
        'Arts & Culture': ['art', 'craft', 'exhibition', 'gallery', 'culture', 'museum', 'drawing', 'painting'],
        'Sports & Recreation': ['sport', 'volleyball', 'skateboard', 'training', 'fitness', 'gym', 'recreation'],
        'Comedy & Entertainment': ['comedy', 'comedian', 'laugh', 'entertainment', 'magic', 'illusionist'],
        'Food & Dining': ['food', 'dining', 'restaurant', 'cuisine', 'chef', 'cooking', 'market'],
        'Education & Workshops': ['workshop', 'class', 'learn', 'training', 'education', 'course', 'tutorial'],
        'Family & Children': ['children', 'kids', 'family', 'youth', 'teens', 'playground'],
        'Health & Wellness': ['yoga', 'wellness', 'health', 'meditation', 'therapy', 'healing'],
        'Business & Professional': ['business', 'professional', 'networking', 'conference', 'meeting'],
        'Film & Cinema': ['film', 'movie', 'cinema', 'screening', 'documentary'],
        'Other': []
    }
    
    for category, keywords in categories.items():
        if any(keyword in content for keyword in keywords):
            return category
    
    return 'Other'

def process_rss_to_dataframe(rss_content: str, months_ahead: int = 3) -> pd.DataFrame:
    """Process RSS XML content into structured DataFrame"""
    
    try:
        root = ET.fromstring(rss_content)
        
        events = []
        cutoff_date = datetime.now() + timedelta(days=months_ahead * 30)
        
        # Parse RSS items
        for item in root.findall('.//item'):
            title = item.find('title')
            link = item.find('link') 
            description = item.find('description')
            pub_date = item.find('pubDate')
            guid = item.find('guid')
            
            if title is not None and description is not None:
                
                # Extract event ID from GUID or link
                event_id = None
                if guid is not None and guid.text:
                    # Extract numeric ID from GUID
                    id_match = re.search(r'(\d+)', guid.text)
                    if id_match:
                        event_id = id_match.group(1)
                
                # Parse description for location and dates
                desc_data = parse_event_description(description.text or '')
                
                # Parse dates
                date_data = parse_event_dates(desc_data['date_info'])
                
                # Only include events within the specified timeframe
                if date_data['start_date'] and date_data['start_date'] <= cutoff_date:
                    
                    # Determine event category
                    category = extract_event_category(title.text or '', desc_data['description_text'])
                    
                    event = {
                        'event_id': event_id,
                        'title': title.text,
                        'description': desc_data['description_text'],
                        'location_text': desc_data['location'],
                        'date_info_original': desc_data['date_info'],
                        'start_date': date_data['start_date'],
                        'end_date': date_data['end_date'],
                        'is_recurring': date_data['is_recurring'],
                        'category': category,
                        'event_url': link.text if link is not None else None,
                        'publication_date': pub_date.text if pub_date is not None else None,
                        'fetch_timestamp': datetime.now(),
                        'data_source': 'EventFinda RSS',
                        'rss_feed_url': 'https://www.eventfinda.co.nz/feed/events/new-zealand/whatson/upcoming.rss'
                    }
                    
                    events.append(event)
        
        df = pd.DataFrame(events)
        logger.info(f"Processed {len(df)} events from RSS feed")
        
        return df
        
    except ET.ParseError as e:
        logger.error(f"Failed to parse RSS XML: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing RSS data: {e}")
        raise

def enhance_location_data(df: pd.DataFrame) -> pd.DataFrame:
    """Add structured location data and region mapping"""
    
    # NZ region mapping based on major cities/areas
    region_mapping = {
        'Auckland': 'Auckland',
        'Wellington': 'Wellington', 
        'Christchurch': 'Canterbury',
        'Hamilton': 'Waikato',
        'Tauranga': 'Bay of Plenty',
        'Dunedin': 'Otago',
        'Palmerston North': 'Manawatu-Whanganui',
        'Napier': 'Hawke\'s Bay',
        'Nelson': 'Nelson',
        'New Plymouth': 'Taranaki',
        'Rotorua': 'Bay of Plenty',
        'Whangarei': 'Northland',
        'Invercargill': 'Southland',
        'Lower Hutt': 'Wellington',
        'Upper Hutt': 'Wellington',
        'Gisborne': 'Gisborne',
        'Timaru': 'Canterbury',
        'Taupo': 'Waikato',
        'Hastings': 'Hawke\'s Bay',
        'Levin': 'Manawatu-Whanganui'
    }
    
    def map_location_to_region(location_text):
        if not location_text:
            return None
            
        # Extract city from location text
        for city, region in region_mapping.items():
            if city.lower() in location_text.lower():
                return region
        
        return 'Other'
    
    # Add region and clean location data
    df['region'] = df['location_text'].apply(map_location_to_region)
    df['city'] = df['location_text'].str.extract(r'([A-Za-z\s]+)')[0].str.strip()
    
    return df

def save_processed_data(df: pd.DataFrame, output_path: str = 'processed_data/eventfinda_events.csv'):
    """Save processed events data to CSV"""
    
    try:
        # Create output directory if it doesn't exist
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_path, index=False, date_format='%Y-%m-%d')
        logger.info(f"Saved {len(df)} events to {output_path}")
        
        # Print summary statistics
        print("\n" + "="*60)
        print("EVENTFINDA DATA PROCESSING SUMMARY")
        print("="*60)
        print(f"Total Events Processed: {len(df)}")
        print(f"Date Range: {df['start_date'].min()} to {df['start_date'].max()}")
        print(f"Output File: {output_path}")
        
        print(f"\nEvents by Category:")
        category_counts = df['category'].value_counts()
        for category, count in category_counts.items():
            print(f"  {category}: {count}")
        
        print(f"\nEvents by Region:")
        region_counts = df['region'].value_counts()
        for region, count in region_counts.head(10).items():
            print(f"  {region}: {count}")
        
        print(f"\nSample Events:")
        for _, event in df.head(3).iterrows():
            print(f"  • {event['title']} - {event['location_text']} ({event['start_date'].strftime('%Y-%m-%d') if pd.notna(event['start_date']) else 'TBD'})")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to save data: {e}")
        raise

def main():
    """Main execution function"""
    
    rss_url = "https://www.eventfinda.co.nz/feed/events/new-zealand/whatson/upcoming.rss"
    
    try:
        print("Fetching EventFinda RSS feed for HIWA_I_TE_RANGI...")
        
        # Fetch RSS content
        rss_content = fetch_eventfinda_rss(rss_url)
        
        # Process into DataFrame
        df = process_rss_to_dataframe(rss_content, months_ahead=3)
        
        # Enhance with location data
        df = enhance_location_data(df)
        
        # Save processed data
        output_path = save_processed_data(df)
        
        print(f"\n✅ SUCCESS: EventFinda data ready for Snowflake!")
        print(f"Next steps:")
        print(f"1. Review data: {output_path}")
        print(f"2. Run: snow sql -f scripts/setup_hiwa_i_te_rangi.sql")
        print(f"3. Load data: COPY INTO events FROM @event_stage/{output_path}")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        print(f"\n❌ ERROR: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())