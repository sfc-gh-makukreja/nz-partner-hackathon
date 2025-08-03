# NZ Partner Hackathon 🇳🇿 - AI Data Platform

This repository contains production-ready datasets and sample queries for Snowflake AI hackathon participants, organized by Matariki themes.

## 📈 Current Status

| Theme | Status | Total Records | Data Coverage | Sample Queries |
|-------|---------|---------------|---------------|----------------|
| ⚡ **URU_RANGI** | ✅ **COMPLETE** | **8,465** | 5-min electricity demand, fuel mix (1974-2030), quarterly trends | [`URU_RANGI_wind_energy_queries.sql`](sample_queries/URU_RANGI_wind_energy_queries.sql) |
| ⛈️ **WAIPUNA_RANGI** | ✅ **COMPLETE** | **5,776** | Climate data (89 years), flood mapping, disaster costs | [`WAIPUNA_RANGI_climate_queries.sql`](sample_queries/WAIPUNA_RANGI_climate_queries.sql) |
| 🌊 **WAITA** | ✅ **COMPLETE** | **31,314** | LINZ tide predictions, maritime incidents, PDF RAG system | [`WAITA_marine_tide_queries.sql`](sample_queries/WAITA_marine_tide_queries.sql) |
| 🌾 **TIPUĀNUKU** | ✅ **COMPLETE** | **4,100,000+** | Open Food Facts global database + NZ/AU product focus + AI image analysis | [`TIPUANUKU_food_agriculture_queries.sql`](sample_queries/TIPUANUKU_food_agriculture_queries.sql) |
| ✈️ **HIWA_I_TE_RANGI** | ✅ **COMPLETE** | **183,142** | EventFinda events + Stats NZ tourism + NZ airfares (162K flights) | [`HIWA_I_TE_RANGI_tourism_events_queries.sql`](sample_queries/HIWA_I_TE_RANGI_tourism_events_queries.sql) |
| 🏛️ **FOUNDATIONAL** | ✅ **COMPLETE** | **62,218** | Stats NZ income & productivity data (1979-2024) + AI economic analysis | [`FOUNDATIONAL_socioeconomic_queries.sql`](sample_queries/FOUNDATIONAL_socioeconomic_queries.sql) |

### 🚀 **Production Ready:**
- **6 Complete Themes** with verified data and working queries
- **4,390,915+ Total Records** across climate, energy, marine safety, events, tourism, airfares, food safety, and socio-economic datasets  
- **Real Government Data** from MBIE, Transpower, NIWA, ICNZ, LINZ, Maritime NZ, Fisheries NZ, Stats NZ
- **Global Food Database** with 4.1M+ products including 12,819 NZ/AU products
- **Comprehensive Economic Data** with 45+ years of income, productivity, and regional development indicators
- **Snowflake Cortex AI** examples with verified Asia Pacific availability + multimodal image analysis
- **RAG Document Processing** operational with PDF fishing regulations

---

## 🎯 Theme Details & Infrastructure

### ⚡ URU_RANGI (Wind & Energy) - **✅ PRODUCTION READY**

**Raw Data Sources:**
- [Transpower Live Load Data](https://www.transpower.co.nz/system-operator/live-system-and-market-data/live-load-data#download) → `Zone Data (01 Jul - 29 Jul) [5 intervals] (1).csv`
- [MBIE Electricity Statistics](https://www.mbie.govt.nz/building-and-energy/energy-and-natural-resources/energy-statistics-and-modelling/energy-statistics/electricity-statistics) → `electricity-2025-q1.xlsx`

**Database Infrastructure:**
- **Schema:** `URU_RANGI`
- **Stage:** `electricity_data_stage` (CSV format)
- **File Format:** `electricity_csv_format`

**Tables:**
- `electricity_zone_data_5min` - **8,345 records** - 5-minute demand by 14 geographic zones (July 2024)
- `electricity_generation_by_fuel` - **19 records** - Annual renewable vs fossil fuel breakdown (1974-2030)  
- `electricity_quarterly_generation` - **101 records** - Historical quarterly generation trends (2000-2025)

**Sample Queries:** [`URU_RANGI_wind_energy_queries.sql`](sample_queries/URU_RANGI_wind_energy_queries.sql)
- ✅ Peak demand analysis, regional patterns, renewable trends
- ✅ AI/ML features with lag functions and rolling averages  
- ✅ Cortex AI optimization strategies
- ✅ Wind energy variability analysis

---

### ⛈️ WAIPUNA_RANGI (Rain & Water) - **✅ PRODUCTION READY**

**Raw Data Sources:**
- [NIWA Climate Station Statistics](https://niwa.co.nz/climate-and-weather/climate-data/national-climate-database/climate-stations-statistics) → `1464_*.csv`, `2109_*.csv`, `4960_*.csv` (3 stations)
- [Waikato Regional Hazards Portal](https://www.waikatoregion.govt.nz/services/regional-hazards-and-emergency-management/regional-hazards-portal/) → `WaipaDistrictPlan_SpecialFeature_Area_Flood_*.csv/.geojson`
- [ICNZ Cost of Natural Disasters](https://www.icnz.org.nz/industry/cost-of-natural-disasters/) → `Cost Of Natural Disasters Table (NZ).csv`

**Database Infrastructure:**
- **Schema:** `WAIPUNA_RANGI`
- **Stage:** `climate_data_stage` (NIWA CSV format)
- **File Format:** `niwa_csv_format`

**Tables:**
- `rainfall_annual` - **277 records** - Annual rainfall (3 stations, 1933-2022)
- `rainfall_monthly` - **4,245 records** - Monthly patterns (89 years)
- `temperature_annual` - **173 records** - Annual temperature trends (1947-1984)
- `temperature_monthly` - **910 records** - Monthly temperature data
- `climate_stations` - Station metadata (Historic 1464, Primary 2109, Modern 4960)
- `waipa_flood_zones` - **13 zones** covering **23.9 km²** of flood-prone areas
- `waipa_flood_boundaries` - **13 GeoJSON polygons** for spatial analysis
- `icnz_disaster_costs` - **141 disaster events** (1968-2025, $4.5B+ total impact)

**Sample Queries:** [`WAIPUNA_RANGI_climate_queries.sql`](sample_queries/WAIPUNA_RANGI_climate_queries.sql)
- ✅ Climate trend analysis with rolling averages
- ✅ Flood risk assessment with spatial analysis
- ✅ Disaster cost modeling and correlation
- ✅ AI-powered risk reports and pattern detection

---

### 🌊 WAITA (Ocean & Marine) - **✅ PRODUCTION READY + RAG**

**Raw Data Sources:**
- [LINZ Tide Predictions](https://www.linz.govt.nz/products-services/tides-and-tidal-streams/tide-predictions) → `Auckland_*.csv`, `Wellington_*.csv`, `Christchurch_*.csv`, etc. (6 ports, 2024-2026)
- [Maritime NZ](https://maritimenz.govt.nz/media/accacvzc/accident-incident-reporting-data.csv) → `accident-incident-reporting-data.csv`
- [Fisheries NZ](https://www.mpi.govt.nz/fishing-aquaculture/recreational-fishing/fishing-rules/) → `fish-pdf/*.pdf` (8 regional fishing regulation documents)

**Database Infrastructure:**
- **Schema:** `WAITA`
- **Stages:** 
  - `marine_data_stage` (CSV format)
  - `fishing_documents_stage` (PDF documents with encryption)
- **File Formats:** 
  - `waita_csv_format` (Marine CSV data)
  - `waita_pdf_format` (Document processing)
- **Cortex Search Service:** `fishing_documents_search_service` (RAG-enabled)

**Tables:**
- `tide_predictions` - **29,218 records** - LINZ tide data (6 ports, 2024-2026) with GEOGRAPHY points
- `tide_ports` - **6 records** - Port metadata with WGS 84 coordinates
- `tide_range_analysis` - **View** - Statistical analysis of tidal ranges
- `maritime_incidents` - **2,096 records** - Accident/incident reports (2018-2024)
- `fishing_documents` - **8 records** - Parsed PDF regulations with Cortex
- `fishing_document_chunks` - **Document chunks** - Optimized for semantic search (≤512 tokens)

**Advanced Features:**
- ✅ **Cortex PARSE_DOCUMENT** - Extract text and layout from PDF regulations
- ✅ **SPLIT_TEXT_RECURSIVE_CHARACTER** - Intelligent text chunking
- ✅ **Cortex Search Service** - Natural language Q&A on fishing regulations
- ✅ **RAG Fishing Trip Planner** - Multi-source data integration with AI synthesis

**Sample Queries:** [`WAITA_marine_tide_queries.sql`](sample_queries/WAITA_marine_tide_queries.sql)
- ✅ Tide prediction analysis with geographic functions
- ✅ Maritime incident safety analytics
- ✅ **Comprehensive RAG implementation** - "Is this the right time to go fishing?"
- ✅ Document Q&A with semantic search

---

### 🌾 TIPUĀNUKU (Food & Agriculture) - **✅ PRODUCTION READY + AI VISION**

**Raw Data Sources:**
- [Open Food Facts](https://world.openfoodfacts.org/data) → `en.openfoodfacts.org.products.csv` (4.1M+ global products)
- Food images for AI analysis → `food_image_table` (multimodal analysis capability)

**Database Infrastructure:**
- **Schema:** `TIPUANUKU`
- **Stage:** `openfoodfacts_stage` (CSV format)
- **File Format:** `openfoodfacts_csv_format` (tab-separated)

**Tables:**
- `openfoodfacts_raw` - **3,970,000+ records** - Complete global food product database
- `food_products_allergy_focus` - **1,130,000+ records** - Products with allergen/ingredient data
- `food_image_table` - **Image analysis capability** - FILE objects for multimodal AI processing

**Analytical Views:**
- `products_with_allergens` - **580,000+ products** - Filtered products with allergen/trace warnings
- `oceania_food_products` - **12,819 products** - NZ/Australia market focus

**Advanced Features:**
- ✅ **Multimodal AI Image Analysis** - Food identification, ingredient detection, allergen analysis
- ✅ **Global Product Database** - 1.13M+ products with comprehensive allergen information
- ✅ **Regional Focus** - 12,819 products specifically for NZ/Australian market
- ✅ **AI-Powered Food Safety** - Ingredient analysis with product database cross-referencing

**Sample Queries:** [`TIPUANUKU_food_agriculture_queries.sql`](sample_queries/TIPUANUKU_food_agriculture_queries.sql)
- ✅ Food safety analysis with allergen detection
- ✅ AI image analysis → ingredient suggestion → Oceania product matching
- ✅ Regional food market analysis (NZ/AU focus)
- ✅ Nutritional data exploration and health scoring
- ✅ Smart Allergy Scanner App foundation with 1.1M+ product safety database

---

### ✈️ HIWA_I_TE_RANGI (Travel & Tourism) - **✅ COMPLETE TOURISM PLATFORM**

**Raw Data Sources:**
- [EventFinda RSS Feed](https://www.eventfinda.co.nz/feed/events/new-zealand/whatson/upcoming.rss) → `eventfinda_events.csv` (20 current events)
- [Kaggle NZ Airfares](https://www.kaggle.com/datasets/shashwatwork/airfares-in-new-zealand) → `NZ airfares.csv` (162,833 flights, Sep-Dec 2019)
- [Stats NZ Visitor Arrivals](https://infoshare.stats.govt.nz/SelectVariables.aspx?pxID=67ad24ca-aa48-48a1-9183-c36ba84d15f9) → `ITM475712_*.csv` (102 years, 1923-2024)
- [Stats NZ Passenger Movements](https://infoshare.stats.govt.nz/SelectVariables.aspx?pxID=8b80adf1-4cd5-4b98-a06e-7ef5c934b2a0) → `ITM332206_*.csv` (164 years, 1861-2024)
- [Stats NZ Guest Nights](https://infoshare.stats.govt.nz/SelectVariables.aspx?pxID=d44894b7-7ee0-43f6-b98a-200a0c9120ac) → `ACS348801_*.csv` (8,800 regional records, 1996-2019)
- [Stats NZ Occupancy Rates](https://infoshare.stats.govt.nz/SelectVariables.aspx?pxID=a8d045fa-36bd-40b4-84ca-33afa1b04e46) → `ACS348401_*.csv` (11,120 regional records, 1996-2019)
- [Stats NZ Migrant Arrivals](https://infoshare.stats.govt.nz/SelectVariables.aspx?pxID=8b80adf1-4cd5-4b98-a06e-7ef5c934b2a0) → `ITM553006_*.csv` (23 years, 2003-2025)

**Database Infrastructure:**
- **Schema:** `HIWA_I_TE_RANGI`
- **Stage:** `tourism_data_stage`
- **File Format:** `tourism_csv_format`
- **Processing Scripts:** `process_tourism_data.py`, `fetch_eventfinda_data.py`, `process_airfares_data.py`
- **Complete Setup:** `setup_complete_hiwa_i_te_rangi.sh`

**Tables:**
- `eventfinda_events` - **20 records** - Real-time events from RSS feed (July-August 2025)
- `nz_airfares` - **162,833 records** - Domestic flight pricing data (Sep-Dec 2019)
- `visitor_arrivals` - **102 records** - Annual visitor arrival totals (1923-2024)
- `passenger_movements` - **164 records** - Annual arrivals/departures data (1861-2024)  
- `guest_nights_by_region` - **8,800 records** - Monthly accommodation demand by region (1996-2019)
- `occupancy_rates_by_region` - **11,120 records** - Monthly occupancy rates by region (1996-2019)
- `migrant_arrivals` - **23 records** - Annual migrant arrival estimates (2003-2025)

**Analytical Views:**
- `events_monthly_summary` - Event distribution by month/category/region
- `tourism_events_correlation` - Events vs accommodation demand correlation
- `regional_tourism_performance` - Regional tourism KPI analysis
- `tourism_cost_analysis` - **NEW** Events + airfares tourism value scoring
- `regional_airfare_analysis` - **NEW** Regional flight accessibility and pricing analysis

**Event Categories (Current):**
- Sports & Recreation (5 events)
- Music & Performance (3 events) 
- Arts & Culture (3 events)
- Comedy & Entertainment (2 events)
- Education & Workshops (1 event)
- Other (6 events)

**Regional Coverage:**
- Wellington (6 events), Auckland (5 events), Taranaki (2 events), Canterbury (1 event), Hawke's Bay (1 event)

**Historical Data Coverage:**
- **100+ Years** of visitor arrival trends
- **160+ Years** of passenger movement data
- **20+ Years** of regional accommodation statistics
- **Real-time** event monitoring via RSS feed

**Sample Queries:** [`HIWA_I_TE_RANGI_tourism_events_queries.sql`](sample_queries/HIWA_I_TE_RANGI_tourism_events_queries.sql)
- ✅ Event tourism impact analysis with historical context
- ✅ Regional event distribution and demand patterns
- ✅ AI-powered tourism demand forecasting using 100+ years of data
- ✅ Cross-theme integration (events + weather/marine data)
- ✅ Event-driven accommodation demand prediction
- ✅ Long-term tourism trend analysis (1861-2024)
- ✅ Migration pattern analysis and tourism correlation

---

### 🏛️ FOUNDATIONAL (Socio-Economic) - **✅ PRODUCTION READY + AI ECONOMICS**

**Raw Data Sources:**
- [Stats NZ Income Statistics](https://explore.data.stats.govt.nz/) → `STATSNZ,INC_INC_005,1.0+all.csv` (Earnings by occupation, sex, qualification 2013-2024)
- [Stats NZ Household Income](https://explore.data.stats.govt.nz/) → `STATSNZ,INC_INC_011,1.0+all.csv` (Regional household income 1998-2024)
- [Stats NZ Productivity Statistics](https://explore.data.stats.govt.nz/) → `STATSNZ,PRD_PRD_002,1.0+all.csv` (Industry productivity 1979-2024)

**Database Infrastructure:**
- **Schema:** `FOUNDATIONAL`
- **Stage:** `foundational_data_stage` (CSV format)
- **File Format:** `foundational_csv_format`

**Tables:**
- `earnings_by_occupation` - **15,012 records** - Earnings by occupation, sex, and highest qualification (2013-2024)
- `household_income_by_region` - **42,806 records** - Regional household income by source and type (1998-2024)
- `productivity_by_industry` - **4,400 records** - Industry productivity growth accounting (1979-2024)

**Analytical Views:**
- `income_gender_analysis` - **Comprehensive pay gap analysis** - Earnings by occupation, gender, and qualification
- `regional_income_trends` - **Regional economic patterns** - Income composition and growth trends by region
- `industry_productivity_trends` - **Productivity performance** - Multi-factor productivity analysis with YoY changes
- `economic_indicators_summary` - **Data coverage overview** - Summary of all economic datasets

**Advanced Features:**
- ✅ **45+ Years of Economic Data** - Comprehensive long-term trend analysis from 1979-2024
- ✅ **Gender Pay Gap Analysis** - Detailed occupational equity assessment with AI insights
- ✅ **Regional Economic Profiling** - Income composition, growth rates, and resilience scoring
- ✅ **Industry Productivity Tracking** - Labour productivity, multifactor productivity, and capital contribution analysis
- ✅ **AI-Powered Economic Assessment** - Cortex AI for regional development recommendations and policy insights

**Sample Queries:** [`FOUNDATIONAL_socioeconomic_queries.sql`](sample_queries/FOUNDATIONAL_socioeconomic_queries.sql)
- ✅ Gender pay gap analysis across all occupations with AI equity insights
- ✅ Regional economic health monitoring and resilience scoring
- ✅ Industry productivity trends and performance profiling
- ✅ Economic forecasting and policy impact simulation
- ✅ Cross-dataset integration with tourism, energy, and climate data
- ✅ AI-powered economic development recommendations

---

## 📁 Repository Structure

```
nz-partner-hackathon/
├── README.md                          # This file - complete project overview
├── setup.sql                          # Database and schema creation
├── data/                              # Raw data files (CSV, Excel, GeoJSON, PDF)
│   ├── fish-pdf/                      # 8 PDF fishing regulation documents
│   ├── *_tide_predictions.csv         # LINZ tide data (6 ports × 3 years)
│   ├── accident-incident-reporting-data.csv  # Maritime NZ incidents
│   ├── electricity-2025-q1.xlsx       # MBIE electricity statistics
│   ├── Zone Data (01 Jul - 29 Jul).csv    # Transpower load data
│   ├── Cost Of Natural Disasters Table.csv # ICNZ disaster costs
│   ├── 1464_*.csv, 2109_*.csv, 4960_*.csv # NIWA climate data (3 stations)
│   └── WaipaDistrictPlan_*.csv/.geojson    # Flood mapping data
├── processed_data/                    # Clean, processed CSV files ready for loading
│   ├── tide_predictions_combined.csv  # Combined LINZ tide data
│   ├── maritime_incidents_processed.csv   # Cleaned incident reports
│   ├── electricity_*_final.csv        # Processed electricity data
│   ├── rainfall_*_combined.csv        # Merged rainfall datasets
│   ├── temperature_*_combined.csv     # Merged temperature datasets
│   └── icnz_disaster_costs.csv        # Processed disaster cost data
├── scripts/                          # Data processing and loading scripts
│   ├── process_waipuna_rangi_complete.py   # NIWA climate + flood + disaster processing
│   ├── setup_waipuna_rangi.sql            # WAIPUNA_RANGI database setup
│   ├── complete_setup.sql                 # URU_RANGI database setup
│   ├── setup_waita_marine.sql             # WAITA database setup
│   ├── setup_cortex_fishing_documents.sql # RAG and document processing setup
│   ├── setup_openfoodfacts.sql            # TIPUANUKU food database setup
│   ├── load_openfoodfacts_simple.sql      # Open Food Facts data loading
│   ├── setup_foundational.sql             # FOUNDATIONAL socio-economic database setup
│   ├── load_foundational_data.sql         # Stats NZ income & productivity data loading
│   ├── process_tide_data.py               # LINZ tide data processing
│   ├── process_maritime_incidents.py      # Maritime incident data cleaning
│   └── data_sharing_setup.sql             # Participant access management
└── sample_queries/                    # Production-ready SQL examples
    ├── URU_RANGI_wind_energy_queries.sql      # Energy AI examples (✅ tested)
    ├── WAIPUNA_RANGI_climate_queries.sql      # Water risk AI examples (✅ tested)
    ├── WAITA_marine_tide_queries.sql          # Marine AI + RAG examples (✅ tested)
    ├── TIPUANUKU_food_agriculture_queries.sql # Food AI + image analysis (✅ tested)
    ├── HIWA_I_TE_RANGI_tourism_events_queries.sql # Tourism AI examples (✅ tested)
    ├── FOUNDATIONAL_socioeconomic_queries.sql # Economic AI + policy analysis (✅ tested)
    └── query_template.sql                     # Template for new themes
```

## 🤖 AI & Technology Features

### **Snowflake Cortex AI Functions (Verified Working)**
- **Models Available:** llama3.1-8b, llama3.1-70b, mistral-large2, mixtral-8x7b, mistral-7b
- **Regions:** AWS AP Southeast 2 (Sydney), AWS AP Northeast 1 (Tokyo)
- **Functions:** `AI_COMPLETE`, `AI_CLASSIFY`, `AI_FILTER`, `AI_AGG`, `AI_SENTIMENT`

### **Document Processing & RAG**
- **PARSE_DOCUMENT:** Extract text and layout from PDFs
- **SPLIT_TEXT_RECURSIVE_CHARACTER:** Intelligent chunking for semantic search
- **Cortex Search Service:** Natural language Q&A on documents
- **Multi-modal AI:** Ready for images, PDFs, and structured data integration

### **Geographic & Spatial Features**
- **GEOGRAPHY Data Type:** WGS 84 coordinate system for tide predictions
- **GeoJSON Support:** Flood boundary polygons for spatial analysis
- **Spatial Functions:** Distance calculations, area measurements, boundary analysis

### **Data Processing Excellence**
- ✅ All sample queries tested and verified working
- ✅ Raw data preserved with original source URLs and collection dates
- ✅ Data transformations documented in processing scripts  
- ✅ Clean, analysis-ready tables with proper data types
- ✅ Error handling and data validation implemented

---

**Ready to build innovative AI solutions with real New Zealand government data! 🇳🇿🤖**