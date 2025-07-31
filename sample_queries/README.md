# NZ Partner Hackathon - Sample Queries

This directory contains sample SQL queries for each Matariki theme, organized by available datasets.

## Available Themes & Data

### ✅ URU_RANGI (Wind & Energy) - **DATA AVAILABLE**
- **File**: `URU_RANGI_wind_energy_queries.sql`
- **Data**: Complete electricity dataset with real NZ government data
- **Tables**: 
  - `electricity_zone_data_5min` - 5-minute interval demand by 14 geographic zones (8,345 rows, July 2024)
    - **Source**: [Transpower Live Load Data](https://www.transpower.co.nz/system-operator/live-system-and-market-data/live-load-data#download)
    - **File**: `Zone Data (01 Jul - 29 Jul) [5 intervals] (1).csv`
  - `electricity_generation_by_fuel` - Annual renewable vs fossil fuel breakdown (19 years, 1974-2030)
    - **Source**: [MBIE Electricity Statistics](https://www.mbie.govt.nz/building-and-energy/energy-and-natural-resources/energy-statistics-and-modelling/energy-statistics/electricity-statistics)
    - **File**: `electricity-2025-q1.xlsx` (processed)
  - `electricity_quarterly_generation` - Historical quarterly generation trends (101 quarters, 2000-2025)
    - **Source**: [MBIE Electricity Statistics](https://www.mbie.govt.nz/building-and-energy/energy-and-natural-resources/energy-statistics-and-modelling/energy-statistics/electricity-statistics)
    - **File**: `electricity-2025-q1.xlsx` (processed)
- **Use Cases**: Peak demand prediction, renewable optimization, grid planning

### 🔄 TIPUĀNUKU (Food & Agriculture) - **COMING SOON**
- **Status**: Awaiting food/agriculture dataset upload
- **Planned Data**: Production data, restaurant reviews, supply chain, nutrition
- **Planned Sources**: Stats NZ agriculture data, MPI food safety data, local council data
- **Sample Queries**: Will be created once data is loaded

### 🔄 WAITĀ (Ocean & Marine) - **COMING SOON**  
- **Status**: Awaiting ocean/marine dataset upload
- **Planned Data**: Tide data, fishing conditions, marine weather
- **Planned Sources**: [LINZ Tide Predictions](https://www.linz.govt.nz/products-services/tides-and-tidal-streams/tide-predictions), NIWA marine data, Fisheries NZ
- **Sample Queries**: Will be created once data is loaded

### 🔄 WAIPUNA_RANGI (Rain & Water) - **COMING SOON**
- **Status**: Awaiting water/weather dataset upload  
- **Planned Data**: Rainfall, flooding, insurance claims
- **Planned Sources**: NIWA climate data, regional council flood data, insurance industry data
- **Sample Queries**: Will be created once data is loaded

### 🔄 HIWA_I_TE_RANGI (Travel & Tourism) - **COMING SOON**
- **Status**: Awaiting travel/tourism dataset upload
- **Planned Data**: Flight data, events, tourism patterns
- **Planned Sources**: Tourism NZ, Stats NZ tourism data, airport/airline APIs
- **Sample Queries**: Will be created once data is loaded

## How to Use

1. **For URU_RANGI**: Use the provided queries immediately with real data
2. **For Other Themes**: Sample query files will be created as datasets are added

## Database Access

```sql
-- Connect to the shared database
USE DATABASE NZ_HACKATHON_DATA;

-- Explore available schemas
SHOW SCHEMAS;

-- For electricity data (ready now):
USE SCHEMA URU_RANGI;
SHOW TABLES;
```

## Data Sources & Attribution

All datasets include proper attribution to their original sources:

### Government Data Sources
- **MBIE (Ministry of Business, Innovation & Employment)**: Electricity generation, fuel mix, and historical trends
- **Transpower**: Real-time electricity demand and grid load data
- **LINZ (Land Information New Zealand)**: Tide and marine data (planned)
- **MetService/NIWA**: Weather and climate data (planned)

### Data Processing
- Raw data files are processed into clean, analysis-ready tables
- Original source URLs and collection dates are preserved in metadata
- Data transformations are documented in processing scripts

## Adding New Datasets

When adding data for other themes:
1. Load data into appropriate schema (TIPUANUKU, WAITA, etc.)
2. Document data source URLs and attribution
3. Create corresponding sample query file
4. Update this README with available tables, sources, and use cases