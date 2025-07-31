# NZ Partner Hackathon - Sample Queries

This directory contains sample SQL queries for each Matariki theme, organized by available datasets.
## 📈 Current Hackathon Status

| Theme | Status | Records | Ready For |
|-------|---------|---------|-----------|
| ⚡ URU_RANGI | ✅ Complete | 8,345 + 19 + 101 | Energy AI projects |
| 🌧️ WAIPUNA_RANGI | ✅ Complete | 103 rainfall records | Climate AI projects |
| 🌾 TIPUĀNUKU | 🔄 Pending | - | Food/agriculture data needed |
| 🌊 WAITĀ | 🔄 Pending | - | Ocean/marine data needed |
| ✈️ HIWA_I_TE_RANGI | 🔄 Pending | - | Travel/tourism data needed |

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
- **Sample Queries**:
  | Category | Status | Sample Results |
  | -------- | ------ | -------------- |
  | Exploratory Queries | ✅ Working | 8,345 zone records, 19 fuel years, 101 quarters |
  | Peak Demand Analysis | ✅ Working | Hourly patterns, weekend vs weekday trends |
  | Regional Analysis | ✅ Working | Auckland: 930MW avg, Wellington: 413MW avg |
  | Renewable Trends | ✅ Working | 90.48% renewable in 2023, 4.96% improvement in 2022 |
  | AI/ML Features | ✅ Working | Lag features, rolling averages, time features |
  | Wind Energy Analysis | ✅ Working | Wellington 23.3% variability, Taranaki 17.4% |
  | Cortex AI | ✅ Working | Generated comprehensive optimization strategies |
  | Business Insights | ✅ Working | Grid stability, load forecasting, correlations |

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

### ✅ WAIPUNA_RANGI (Rain & Water) - **COMPLETE WATER RISK INTELLIGENCE PLATFORM**
- **File**: `WAIPUNA_RANGI_climate_queries.sql`
- **Data**: Comprehensive water risk analysis combining climate, flood mapping, and disaster costs
- **Tables**:
  - **Climate Data (NIWA)**:
    - `rainfall_annual` - Annual rainfall statistics (277 records, 3 stations, 1933-2022)
    - `rainfall_monthly` - Monthly rainfall patterns (4,245 records)
    - `temperature_annual` - Annual temperature data (173 records)
    - `temperature_monthly` - Monthly temperature patterns (910 records)
    - `climate_stations` - Station metadata (Historic 1464, Primary 2109, Modern 4960)
    - **Source**: [NIWA Climate Station Statistics](https://niwa.co.nz/climate-and-weather/climate-data/national-climate-database/climate-stations-statistics)
  - **Flood Risk Data (Waikato Regional Hazards Portal)**:
    - `waipa_flood_zones` - Flood zone metadata (13 zones with area/perimeter data)
    - `waipa_flood_boundaries` - GeoJSON polygon boundaries for spatial analysis
    - **Source**: [Waikato Regional Hazards Portal](https://www.waikatoregion.govt.nz/services/regional-hazards-and-emergency-management/regional-hazards-portal/)
    - **Coverage**: Waipa River, Puniu River, Mangapiko Stream flood areas
  - **Disaster Cost Data (ICNZ)**:
    - `icnz_disaster_costs` - Natural disaster insurance costs (141 events, 1968-2025)
    - **Source**: [ICNZ Cost of Natural Disasters](https://www.icnz.org.nz/industry/cost-of-natural-disasters/)
    - **Financial Impact**: 97 water-related events totaling $1,955M NZD (inflation-adjusted)
- **Use Cases**: Flood prediction & early warning, disaster cost modeling, spatial risk assessment, climate adaptation planning, emergency response optimization, AI-powered flood insurance pricing

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
USE DATABASE nz_partner_hackathon;

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