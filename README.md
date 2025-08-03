# NZ Partner Hackathon 🇳🇿 - AI Data Platform

**Complete water risk intelligence and energy analytics platform for AI innovation**

This repository contains production-ready datasets and sample queries for Snowflake AI hackathon participants, organized by Matariki themes.

## 📈 Current Hackathon Status

| Theme | Status | Total Records | Key Datasets | Ready For |
|-------|---------|---------------|--------------|-----------|
| ⚡ **URU_RANGI** | ✅ **COMPLETE** | **8,465** | 5-min electricity demand, fuel mix, quarterly trends | Energy AI, grid optimization, renewable forecasting |
| 🌊 **WAIPUNA_RANGI** | ✅ **COMPLETE** | **5,776** | Climate data (89 years), flood mapping, disaster costs | Water risk AI, flood prediction, climate adaptation |
| 🌾 **TIPUĀNUKU** | 🔄 Pending | - | Food/agriculture data needed | Agricultural AI projects |
| 🌊 **WAITA** | ✅ **COMPLETE** | **31,314** | LINZ tide predictions + Maritime NZ incidents + PDF document processing | Marine AI, safety analytics, intelligent document Q&A |
| ✈️ **HIWA_I_TE_RANGI** | 🔄 Pending | - | Travel/tourism data needed | Tourism AI projects |

### 🚀 **Ready for AI Innovation:**
- **3 Complete Themes** with production data
- **45,158 Total Records** across climate, energy, marine safety, and financial datasets  
- **Real Government Data** from MBIE, Transpower, NIWA, ICNZ, LINZ, Maritime NZ
- **Snowflake Cortex AI** examples with verified Asia Pacific availability

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

### ✅ WAIPUNA_RANGI (Rain & Water) - **🌊 COMPLETE WATER RISK INTELLIGENCE PLATFORM**
- **File**: `sample_queries/WAIPUNA_RANGI_climate_queries.sql`
- **Status**: ✅ **FULLY LOADED & TESTED** - All sample queries verified working
- **Total Records**: **5,776** across climate, flood, and disaster datasets
- **Data Coverage**: 89 years of climate data (1933-2022) + real-time flood risk mapping + 57 years of disaster costs

#### **📊 Available Tables:**

**🌧️ Climate Data (NIWA)**:
- `rainfall_annual` - **277 records** (3 stations, 1933-2022) 
- `rainfall_monthly` - **4,245 records** (monthly patterns, 89 years)
- `temperature_annual` - **173 records** (1947-1984)
- `temperature_monthly` - **910 records** (monthly temperature trends)
- `climate_stations` - Station metadata (Historic 1464, Primary 2109, Modern 4960)
- **Source**: [NIWA Climate Station Statistics](https://niwa.co.nz/climate-and-weather/climate-data/national-climate-database/climate-stations-statistics)

**🗺️ Flood Risk Data (Waikato Regional Hazards Portal)**:
- `waipa_flood_zones` - **13 zones** covering **23.9 km²** of flood-prone areas
- `waipa_flood_boundaries` - **13 GeoJSON polygons** for spatial analysis (complexity: simple → highly complex)
- **Source**: [Waikato Regional Hazards Portal](https://www.waikatoregion.govt.nz/services/regional-hazards-and-emergency-management/regional-hazards-portal/)
- **Coverage**: Waipa River, Puniu River, Mangapiko Stream + flood detention dams

**💰 Disaster Cost Data (ICNZ)**:
- `icnz_disaster_costs` - **141 disaster events** (1968-2025, $4.5B+ total impact)
- **Water-related events**: **97 events** totaling **$1,956M NZD** (inflation-adjusted)
- **Source**: [ICNZ Cost of Natural Disasters](https://www.icnz.org.nz/industry/cost-of-natural-disasters/)
- **Major events**: Cyclone Gabrielle ($1,970M), Timaru Hail ($213M), Canterbury Storms ($86M)

#### **🤖 AI-Ready Features:**
- **Verified Cortex AISQL Functions**: `AI_COMPLETE`, `AI_CLASSIFY`, `AI_FILTER`, `AI_AGG`, `AI_SENTIMENT`
- **Available Models**: llama3.1-8b, llama3.1-70b, mistral-large2, mixtral-8x7b, mistral-7b
- **Regions**: AWS AP Southeast 2 (Sydney), AWS AP Northeast 1 (Tokyo)

#### **📈 Sample Query Categories (All Tested ✅):**
- **Climate Trend Analysis**: Rolling averages, extreme weather detection, seasonal patterns
- **Flood Risk Assessment**: Spatial analysis, complexity mapping, watercourse classification  
- **Disaster Cost Modeling**: Financial impact analysis, water-related event correlation
- **Integrated Risk Intelligence**: Multi-source risk scoring, extreme weather-disaster correlation
- **AI-Powered Insights**: Automated risk reports, severity classification, pattern detection

**🎯 Use Cases**: Flood prediction & early warning, disaster cost modeling, spatial risk assessment, climate adaptation planning, emergency response optimization, AI-powered flood insurance pricing, water resource management

### 🔄 HIWA_I_TE_RANGI (Travel & Tourism) - **COMING SOON**
- **Status**: Awaiting travel/tourism dataset upload
- **Planned Data**: Flight data, events, tourism patterns
- **Planned Sources**: Tourism NZ, Stats NZ tourism data, airport/airline APIs
- **Sample Queries**: Will be created once data is loaded

## 🚀 How to Use

### **Ready for AI Innovation:**

**1. URU_RANGI (Energy Analytics)**:
```sql
USE DATABASE nz_partner_hackathon;
USE SCHEMA URU_RANGI;
-- 8,465 records: 5-min demand, fuel mix, quarterly trends
```

**2. WAIPUNA_RANGI (Water Risk Intelligence)**:
```sql
USE DATABASE nz_partner_hackathon;
USE SCHEMA WAIPUNA_RANGI;
-- 5,776 records: climate data, flood mapping, disaster costs
```

**3. WAITA (Ocean & Marine)**:
```sql
USE DATABASE nz_partner_hackathon;
USE SCHEMA WAITA;
-- 31,314 records: LINZ tide predictions, maritime incidents, + PDF document processing
-- NEW: Cortex Search on fishing regulation PDFs for intelligent Q&A
-- FEATURED: "Is this the right time to go fishing?" Streamlit app template
```

**4. Other Themes**: Sample query files will be created as datasets are added

### **Database Access**

```sql
-- Connect to the shared hackathon database
USE DATABASE nz_partner_hackathon;

-- Explore all available schemas
SHOW SCHEMAS;

-- Energy AI projects:
USE SCHEMA URU_RANGI;
SHOW TABLES; -- electricity_zone_data_5min, electricity_generation_by_fuel, etc.

-- Water risk AI projects:
USE SCHEMA WAIPUNA_RANGI;
SHOW TABLES; -- rainfall_annual, waipa_flood_zones, icnz_disaster_costs, etc.

-- Marine AI projects:
USE SCHEMA WAITA;
SHOW TABLES; -- tide_predictions, tide_ports, tidal_range_analysis, etc.

-- Sample query examples:
SELECT * FROM URU_RANGI.electricity_zone_data_5min LIMIT 10;
SELECT * FROM WAIPUNA_RANGI.rainfall_annual WHERE year >= 2020;
SELECT * FROM WAITA.tide_predictions WHERE date >= '2024-01-01' LIMIT 10;
```

## 📋 Data Sources & Attribution

All datasets include proper attribution to their original sources and are ready for commercial AI applications:

### **✅ Active Government Data Sources**
- **MBIE (Ministry of Business, Innovation & Employment)**: Electricity generation, fuel mix, and historical trends
- **Transpower**: Real-time electricity demand and grid load data (5-minute intervals)
- **NIWA (National Institute of Water & Atmospheric Research)**: Climate station statistics (89 years, 1933-2022)
- **Waikato Regional Council**: Flood hazard mapping and spatial risk data
- **ICNZ (Insurance Council of New Zealand)**: Natural disaster insurance costs and financial impact data
- **LINZ (Land Information New Zealand)**: Tide predictions for 6 major NZ ports (2024-2026) 
- **Maritime NZ**: Accident and incident reports with safety analytics (2018-2024)
- **Fisheries NZ**: PDF regulations ready for Cortex Search document processing

### **🆕 AI Document Processing Capabilities**
- **Snowflake Cortex PARSE_DOCUMENT**: Extract text and layout from PDF regulations
- **SPLIT_TEXT_RECURSIVE_CHARACTER**: Intelligent text chunking for semantic search
- **Cortex Search Service**: Natural language Q&A on fishing regulations and marine safety documents
- **Multi-modal AI**: Ready for images, PDFs, and structured data integration

### **🔄 Planned Data Sources**
- **Stats NZ**: Agriculture, tourism, and socio-economic data
- **MPI (Ministry for Primary Industries)**: Food safety and agricultural production data

### **Data Processing & Quality**
- ✅ Raw data files processed into **clean, analysis-ready tables**
- ✅ Original source URLs and collection dates **preserved in metadata**
- ✅ Data transformations **documented in processing scripts**
- ✅ All sample queries **tested and verified working**
- ✅ **Snowflake Cortex AI functions** verified for Asia Pacific regions

## 📁 Repository Structure

```
nz-partner-hackathon/
├── README.md                          # This file - complete project overview
├── setup.sql                          # Database and schema creation
├── data/                              # Raw data files (CSV, Excel, GeoJSON)
├── processed_data/                    # Clean, processed CSV files  
├── scripts/                          # Data processing and loading scripts
│   ├── process_waipuna_rangi_complete.py   # NIWA climate + flood + disaster processing
│   ├── setup_waipuna_rangi.sql            # WAIPUNA_RANGI database setup
│   ├── complete_setup.sql                 # URU_RANGI database setup
│   └── data_sharing_setup.sql             # Participant access management
└── sample_queries/                    # Production-ready SQL examples
    ├── URU_RANGI_wind_energy_queries.sql      # Energy AI examples (✅ tested)
    ├── WAIPUNA_RANGI_climate_queries.sql      # Water risk AI examples (✅ tested)
    ├── TIPUANUKU_food_agriculture_queries.sql # Food AI examples (🔄 pending data)
    └── query_template.sql                     # Template for new themes
```

## 🎯 **Hackathon Ready Status**

### **🚀 READY FOR IMMEDIATE USE:**
1. **URU_RANGI (Energy Analytics)**: 8,465 records across 3 tables
2. **WAIPUNA_RANGI (Water Risk Intelligence)**: 5,776 records across 8 tables
3. **Total**: 14,241 production records + verified AI examples

### **🔧 Adding New Datasets**

When adding data for remaining themes:
1. Load data into appropriate schema (`TIPUANUKU`, `WAITA`, `HIWA_I_TE_RANGI`)
2. Document data source URLs and attribution in README
3. Create corresponding sample query file using `query_template.sql`
4. Test all queries and verify Cortex AI functions work
5. Update this README with available tables, sources, and use cases

---

**Ready to build innovative AI solutions with real New Zealand government data! 🇳🇿🤖**