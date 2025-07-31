use role accountadmin;

use database nz_partner_hackathon;
-- Schema for the Uru Rangi (Wind & Atmosphere) theme
CREATE SCHEMA IF NOT EXISTS URU_RANGI COMMENT = 'Schema for datasets related to wind, atmosphere, energy, and climate.';

-- Schema for the Tipuānuku (Food & The Land) theme
CREATE SCHEMA IF NOT EXISTS TIPUANUKU COMMENT = 'Schema for datasets related to food, agriculture, and land use.';

-- Schema for the Waitā (The Ocean) theme
CREATE SCHEMA IF NOT EXISTS WAITA COMMENT = 'Schema for datasets related to the ocean, marine life, and coastal data.';

-- Schema for the Waipuna Rangi (Rain & Water Cycles) theme
CREATE SCHEMA IF NOT EXISTS WAIPUNA_RANGI COMMENT = 'Schema for datasets related to rain, water cycles, and hydrology.';

-- Schema for the Hiwa-i-te-Rangi (Aspirations & Travel) theme
CREATE SCHEMA IF NOT EXISTS HIWA_I_TE_RANGI COMMENT = 'Schema for datasets related to travel, transport, and tourism.';

-- Schema for the Foundational datasets
CREATE SCHEMA IF NOT EXISTS FOUNDATIONAL COMMENT = 'Schema for foundational socio-economic datasets like population and household economics.';