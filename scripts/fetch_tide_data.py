#!/usr/bin/env python3
"""
Fetch tide prediction data from LINZ for major NZ cities
Downloads CSV files for 6 major ports/cities across 3 years (2024-2026)
"""

import requests
import os
from pathlib import Path
import urllib.parse
import time

def fetch_tide_data():
    """
    Fetch tide prediction CSV files from LINZ for major NZ cities
    """
    
    # Configuration
    base_url = "https://static.charts.linz.govt.nz/tide-tables/maj-ports/csv/"
    cities = [
        "Auckland",
        "Wellington", 
        "Tauranga",
        "Lyttelton",
        "Dunedin",
        "Napier"
    ]
    years = [2024, 2025, 2026]
    
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Track statistics
    total_files = len(cities) * len(years)
    downloaded = 0
    failed = 0
    
    print(f"🌊 Starting LINZ Tide Data Download")
    print(f"📍 Cities: {', '.join(cities)}")
    print(f"📅 Years: {', '.join(map(str, years))}")
    print(f"📁 Target: {total_files} CSV files")
    print("=" * 60)
    
    for city in cities:
        print(f"\n🏙️  Processing {city}...")
        
        for year in years:
            # Construct filename and URL
            filename = f"{city}%20{year}.csv"
            url = base_url + filename
            
            # Local file path (decoded name for storage)
            local_filename = f"{city}_{year}_tide_predictions.csv"
            local_path = data_dir / local_filename
            
            try:
                print(f"   📥 Downloading {city} {year}...", end=" ")
                
                # Make HTTP request
                response = requests.get(url, timeout=30)
                response.raise_for_status()  # Raise exception for bad status codes
                
                # Save to file
                with open(local_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                file_size = len(response.text)
                downloaded += 1
                print(f"✅ Success ({file_size:,} bytes)")
                
                # Brief pause to be respectful to the server
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                failed += 1
                print(f"❌ Failed: {e}")
                
            except Exception as e:
                failed += 1
                print(f"❌ Error: {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print(f"🎯 Download Summary:")
    print(f"   ✅ Successfully downloaded: {downloaded}/{total_files} files")
    print(f"   ❌ Failed downloads: {failed}/{total_files} files")
    
    if downloaded > 0:
        print(f"   📁 Files saved to: {data_dir.absolute()}")
        
        # List downloaded files
        tide_files = list(data_dir.glob("*_tide_predictions.csv"))
        print(f"   📊 Downloaded tide data files:")
        for file in sorted(tide_files):
            size = file.stat().st_size
            print(f"      • {file.name} ({size:,} bytes)")
    
    if failed == 0:
        print("🌊 All tide prediction data downloaded successfully!")
    else:
        print(f"⚠️  {failed} files failed to download. Check URLs or network connection.")

if __name__ == "__main__":
    fetch_tide_data()