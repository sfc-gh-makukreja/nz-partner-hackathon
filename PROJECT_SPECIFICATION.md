# NZ Partner Hackathon Project Specification

## Objective
Create a Snowflake-based hackathon foundation with ready-to-use datasets and AI capabilities for New Zealand data themes.

## Project Scope
- **Database**: `nz_partner_hackathon` 
- **Schemas**: 6 Matariki-themed schemas (`URU_RANGI`, `WAIPUNA_RANGI`, `WAITĀ`, `HIWA_I_TE_RANGI`, `TIPUĀNUKU`, `FOUNDATIONAL`)
- **Data Sources**: Government datasets (LINZ, NIWA, Maritime NZ, Fisheries NZ, ICNZ)
- **AI Features**: Snowflake Cortex functions for RAG, text processing, and intelligent queries

## Deliverables
1. **Processed datasets** loaded into Snowflake tables
2. **Sample queries** demonstrating AI/data integration capabilities
3. **Python processing scripts** for data cleaning and transformation  
4. **SQL setup scripts** for schema creation and data loading
5. **Documentation** for hackathon participants

## Technical Requirements
- **Database Platform**: Snowflake
- **Processing**: Python with pandas, requests, snowflake-connector-python
- **AI Capabilities**: Cortex AISQL functions (COMPLETE, SEARCH_PREVIEW, PARSE_DOCUMENT)
- **Data Sharing**: Snowflake native data sharing for participant access

## Development Rules
- **Always test code before commit** - No exceptions
- **Never include data/query results in commit messages**
- **Use semantic commit messages** with type prefixes (feat:, fix:, docs:, etc.)
- **Focus on fixing requested issues only** - No creative additions
- **Follow Snowflake SQL best practices** (explicit column names, TRY_CAST, proper error handling)

## Target Audience
Hackathon participants who will:
- Build Streamlit applications using the provided data
- Create AI/ML models with Cortex functions
- Develop dashboards and visualizations
- Focus on application logic rather than data preparation

## Success Criteria
- All datasets successfully loaded and queryable
- Sample queries execute without errors
- RAG functionality operational with PDF documents
- Documentation clear and complete
- Data sharing mechanism functional