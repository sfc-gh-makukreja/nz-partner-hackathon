# NZ Partner Hackathon 🇳🇿 - AI Data Platform

This repository contains production-ready datasets and sample queries for Snowflake AI hackathon participants, organized by Matariki themes.

## 📈 Current Status

| Theme | Status | Total Records | Data Coverage | Sample Queries |
|-------|---------|---------------|---------------|----------------|
| ⚡ **URU_RANGI** | ✅ **COMPLETE** | **8,465** | 5-min electricity demand, fuel mix (1974-2030), quarterly trends | [`URU_RANGI_wind_energy_queries.sql`](sample_queries/URU_RANGI_wind_energy_queries.sql) |
| 🌊 **WAIPUNA_RANGI** | ✅ **COMPLETE** | **5,776** | Climate data (89 years), flood mapping, disaster costs | [`WAIPUNA_RANGI_climate_queries.sql`](sample_queries/WAIPUNA_RANGI_climate_queries.sql) |
| 🌊 **WAITA** | ✅ **COMPLETE** | **31,314** | LINZ tide predictions, maritime incidents, PDF RAG system | [`WAITA_marine_tide_queries.sql`](sample_queries/WAITA_marine_tide_queries.sql) |
| 🌾 **TIPUĀNUKU** | 🔄 **Template Ready** | - | Template queries prepared for agriculture data | [`TIPUANUKU_food_agriculture_queries.sql`](sample_queries/TIPUANUKU_food_agriculture_queries.sql) |
| ✈️ **HIWA_I_TE_RANGI** | 🔄 **Schema Only** | - | Schema created, awaiting tourism/travel data | *No queries yet* |
| 🏛️ **FOUNDATIONAL** | 🔄 **Schema Only** | - | Schema created, awaiting socio-economic data | *No queries yet* |

### 🚀 **Production Ready:**
- **3 Complete Themes** with verified data and working queries
- **45,555 Total Records** across climate, energy, marine safety, and financial datasets  
- **Real Government Data** from MBIE, Transpower, NIWA, ICNZ, LINZ, Maritime NZ, Fisheries NZ
- **Snowflake Cortex AI** examples with verified Asia Pacific availability
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

### 🌊 WAIPUNA_RANGI (Rain & Water) - **✅ PRODUCTION READY**

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

### 🌾 TIPUĀNUKU (Food & Agriculture) - **🔄 TEMPLATE READY**

**Status:** Schema created with comprehensive template queries prepared for food/agriculture datasets

**Database Infrastructure:**
- **Schema:** `TIPUANUKU` *(created but no data loaded)*

**Planned Data Sources:**
- Stats NZ agriculture production data
- MPI food safety datasets  
- Local council restaurant/food service data
- Nutrition and dietary information

**Template Queries:** [`TIPUANUKU_food_agriculture_queries.sql`](sample_queries/TIPUANUKU_food_agriculture_queries.sql)
- 🔄 Food production analysis templates
- 🔄 Restaurant review processing examples
- 🔄 Supply chain tracking patterns  
- 🔄 Nutrition and dietary analysis templates
- 🔄 AI-powered food recommendation systems

---

### ✈️ HIWA_I_TE_RANGI (Travel & Tourism) - **🔄 SCHEMA READY**

**Status:** Schema created, awaiting tourism/travel datasets

**Database Infrastructure:**
- **Schema:** `HIWA_I_TE_RANGI` *(created but no tables or data)*

**Planned Data Sources:**
- Tourism New Zealand visitor data
- Stats NZ tourism statistics
- Airport/airline arrival data
- Event and attraction information

---

### 🏛️ FOUNDATIONAL (Socio-Economic) - **🔄 SCHEMA READY**

**Status:** Schema created, awaiting foundational socio-economic datasets

**Database Infrastructure:**
- **Schema:** `FOUNDATIONAL` *(created but no tables or data)*

**Planned Data Sources:**
- Stats NZ population and demographic data
- Household economic survey data
- Regional development indicators
- Infrastructure and connectivity metrics

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
│   ├── process_tide_data.py               # LINZ tide data processing
│   ├── process_maritime_incidents.py      # Maritime incident data cleaning
│   └── data_sharing_setup.sql             # Participant access management
└── sample_queries/                    # Production-ready SQL examples
    ├── URU_RANGI_wind_energy_queries.sql      # Energy AI examples (✅ tested)
    ├── WAIPUNA_RANGI_climate_queries.sql      # Water risk AI examples (✅ tested)
    ├── WAITA_marine_tide_queries.sql          # Marine AI + RAG examples (✅ tested)
    ├── TIPUANUKU_food_agriculture_queries.sql # Food AI templates (🔄 ready for data)
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