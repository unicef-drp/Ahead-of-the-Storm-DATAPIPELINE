# Ahead of the Storm – Data Processing Setup Guide

## Related Repositories
- **[Ahead-of-the-Storm](https://github.com/unicef-drp/Ahead-of-the-Storm)**: Dash web application for visualizing hurricane impact forecasts. The application displays interactive maps, probabilistic analysis, and impact reports based on pre-processed hurricane data
- **[TC-ECMWF-Forecast-Pipeline](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline)**: Pipeline for processing ECMWF BUFR tropical cyclone and wind forecast data

## GitHub Action Workflows
- **[GitHub Actions Workflows](README_GITHUB_ACTIONS.md)**: Guide for using GitHub Actions to manage countries and trigger pipeline runs

### 1. Manage Country Status
[![Manage Country Status](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/manage-country-status.yml/badge.svg)](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/manage-country-status.yml)

### 2. Initialize New Country
[![Initialize New Country](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/initialize-country.yml/badge.svg)](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/initialize-country.yml)

### 3. Process Past Storms
[![Process Past Storms](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/process-past-storms.yml/badge.svg)](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/process-past-storms.yml)


## Prerequisites

1. **Python 3.11+** installed
2. **Virtual environment** activated (`.venv`)
3. **Environment variables** configured in `.env` file
   - Start from the provided example: `cp example_env.txt .env`
   - Edit values to match the environment (Snowflake, optional Azure)

### Environment setup (recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Required environment variables

- RESULTS_DIR (default: `project_results/climate/lacro_project`)
- STORMS_FILE (default: `storms.json`)
- ROOT_DATA_DIR (default: `geodb`)
- VIEWS_DIR (default: `aos_views`)
- SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
- DATA_PIPELINE_DB (`LOCAL` by default; use `BLOB` for Azure, `SNOWFLAKE` for Snowflake stage)
  - If `BLOB`, also set: `ACCOUNT_URL`, `SAS_TOKEN`
  - If `SNOWFLAKE`, also set: `SNOWFLAKE_STAGE_NAME` (stage must exist in the database/schema)

### API tokens
- GIGA_SCHOOL_LOCATION_API_KEY (required to fetch school locations)
- HEALTHSITES_API_KEY (required to fetch health center locations)
- GEOREPO_API_KEY (optional, preferred)
- GEOREPO_USER_EMAIL (optional, required if using GEOREPO_API_KEY)


## Step 1: Initialize Base Data

This step creates the mercator tile views and admin level views with demographic and infrastructure data for each country. The pipeline uses direct country boundary checks with a 1500km buffer, eliminating the need for a separate bounding box file. Countries are specified when initializing.

**Command:**
```bash
python main_pipeline.py --type initialize
```

**Parameters:**
- `--zoom` (default: 14): Zoom level for mercator tiles
- `--rewrite` (default: 0): Set to 1 to rewrite existing views, 0 to skip if they exist
- `--countries`: List of country codes (e.g., `TWN` for Taiwan, or `DOM VNM` for multiple countries)

**What it does:**
- Creates mercator tiles for each country at specified zoom level
- Creates admin level 1 views for each country
- Downloads and aggregates demographic data (WorldPop population, school age, infants)
- Downloads and aggregates infrastructure data (GHSL built surface, SMOD settlement)
- Fetches school locations (via GIGA API)
- Fetches health center locations (via HealthSites API) - cached after first fetch
- Saves base views to `geodb/aos_views/mercator_views/` and `geodb/aos_views/admin_views/`

**Note:** This process can take 30-60 minutes and downloads several GB of data. It only needs to be run once, or when adding new countries.

**Requirements:**
- GIGA_SCHOOL_LOCATION_API_KEY and HEALTHSITES_API_KEY must be set
- Network access to data sources (WorldPop, GHSL, SMOD, GIGA, HealthSites)


## Step 2: Process Storm Data

Once initialized, run the pipeline to process hurricane data from Snowflake.

**Command:**
```bash
python main_pipeline.py --type update
```

**Parameters:**
- `--type` (default: update): Pipeline mode (initialize or update)
- `--time_delta` (default: 9): Number of days in the past to consider storms for analysis
- `--date` (optional): Process only storms on a specific date (YYYY-MM-DD format, e.g., `2025-11-10`). Overrides `time_delta`.
- `--storm` (optional): Process only a specific storm (e.g., `FUNG-WONG`). Can be combined with `--date`.
- `--rewrite` (default: 0): Set to 1 to force reprocessing of existing storms, 0 to skip already processed
- `--countries`: List of country codes to process (e.g., `TWN` or `DOM VNM`)
- `--zoom` (default: 14): Zoom level for tiles
- `--skip-analysis`: Skip analysis step (for testing)

**What it does:**
- Connects to Snowflake and retrieves available storm data
- Filters storms by time delta or specific date/storm (if provided)
- For each storm/forecast combination:
  - Loads hurricane envelope data
  - **Per-country filtering**: Checks each country individually with a 1500km buffer
  - **Only processes affected countries**: If a storm affects Taiwan but not Vietnam, only Taiwan is processed
  - Creates impact views for schools, health centers, tiles, tracks, and admin levels
  - Calculates Child Cyclone Index (CCI) values
  - Generates JSON impact reports
  - Saves views to the configured data store (local/Azure/Snowflake)
  - Records processed storms in `storms.json`

**Process Flow:**
1. Reads `storms.json` to track which storms have been processed
2. Queries Snowflake for new storms
3. Filters by time delta or specific date/storm (if provided)
4. For each storm not yet processed (or if `rewrite=1`):
   - Loads envelope data (wind impact areas at different thresholds)
   - **Per-country intersection check**: For each specified country:
     - Creates a 1500km buffer around the country boundary
     - Checks if storm envelopes intersect the buffered zone
     - Only processes countries that are actually affected
   - Creates impact views for each affected country:
     - School impact views (probability per wind threshold)
     - Health center impact views (probability per wind threshold)
     - Tile impact views (expected impacts per tile)
     - Admin level impact views (aggregated by admin level 1)
     - CCI views (Child Cyclone Index values)
     - Track views (severity metrics per ensemble member)
   - Generates JSON impact reports
   - Marks storm as processed in `storms.json`

Requirements:
- Valid Snowflake credentials (`SNOWFLAKE_*`)
- Network access to Snowflake

Note:
- `--skip-analysis` does not bypass Snowflake fetching in the current `update` flow.

**Output Example:**
```
======================================================================
HURRICANE IMPACT ANALYSIS PIPELINE
======================================================================
Storm: LORENZO
Forecast Time: 20251015120000
Countries: ATG, BLZ, NIC, DOM, DMA, GRD, MSR, KNA, LCA, VCT, AIA, VGB
Skip Analysis: False
======================================================================
STEP 1: Impact Analysis
--------------------------------------------------
Running impact analysis for LORENZO at 20251015120000
Countries: ATG, BLZ, NIC, DOM, DMA, GRD, MSR, KNA, LCA, VCT, AIA, VGB
Loading envelope data from Snowflake...
Loaded 23 envelope records
Envelopes already converted to GeoDataFrame
Checking which countries are affected (1500km buffer per country)...
  TWN: Affected
Processing 1 affected country/countries: TWN
Creating impact views for affected countries...
  Processing ATG...
    Processing schools...
    Created 3 school views
    Processing health centers...
    Created 3 health center views
    Processing tiles...
    Created 3 tile views
    Processing tracks...
    Created 3 track views
Impact analysis completed successfully
======================================================================
```

## How Country Filtering Works

The pipeline uses **per-country filtering** with a **1500km buffer**:

1. **For each country** specified (e.g., `--countries TWN DOM VNM`):
   - Fetches the actual country boundary from UNICEF GeoRepo
   - Applies a 1500km buffer around the boundary
   - Checks if storm envelopes intersect this buffered zone

2. **Only affected countries are processed**:
   - If a storm affects Taiwan but not Vietnam, only Taiwan is processed
   - This saves time and resources by avoiding unnecessary processing

3. **Benefits**:
   - More efficient: Only processes countries actually at risk
   - More accurate: Uses actual country boundaries, not a bounding box
   - Flexible: Easy to add/remove countries without file management


## Quick Start Summary

```bash
# 1. Initialize base data for countries (one-time setup, takes 30-60 min)
python main_pipeline.py --type initialize --countries TWN

# 2. Process storms (run regularly to update with new storm data)
python main_pipeline.py --type update --countries TWN

# 3. Process a specific storm on a specific date
python main_pipeline.py --type update --countries TWN --date 2025-11-10 --storm FUNG-WONG
```


## Data Storage

The pipeline supports three storage backends (configured via `DATA_PIPELINE_DB`):
- **LOCAL**: Files stored on local filesystem
- **BLOB**: Files stored in Azure Blob Storage (requires `ACCOUNT_URL` and `SAS_TOKEN`)
- **SNOWFLAKE**: Files stored in Snowflake internal stage (requires `SNOWFLAKE_STAGE_NAME`)

**Storage locations:**
- **Base mercator views:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/` (e.g., `geodb/aos_views/mercator_views/`)
- **Base admin views:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/` (e.g., `geodb/aos_views/admin_views/`)
- **Impact views:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/`, `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views/`, etc.
- **CCI views:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/` and `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/` (with `_cci.csv` suffix)
- **Impact reports:** `{RESULTS_DIR}/jsons/` (JSON files per country/storm/forecast)
- **Processed storms:** `{RESULTS_DIR}/{STORMS_FILE}` (e.g., `project_results/climate/lacro_project/storms.json`)
- **Raw data:** `{ROOT_DATA_DIR}/bronze/` (downloaded automatically)

See `FILE_STRUCTURE.md` for detailed file structure and naming conventions.

See `README_GITHUB_ACTIONS.md` for GitHub Actions workflows.

---

## Country Management System

The pipeline uses a Snowflake-based country management system to track which countries are active and their initialization status.

### Database Tables

**PIPELINE_COUNTRIES** - Stores country information:
- `COUNTRY_CODE` (VARCHAR(3)): ISO3 country code (e.g., 'TWN')
- `COUNTRY_NAME` (VARCHAR(100)): Full country name
- `ZOOM_LEVEL` (INTEGER): Default zoom level (usually 14)
- `ACTIVE` (BOOLEAN): Whether the country is active
- `LAST_INITIALIZED` (TIMESTAMP_NTZ): Last initialization timestamp
- `ADDED_DATE` (TIMESTAMP_NTZ): When country was added
- `NOTES` (VARCHAR(500)): Optional notes

**PIPELINE_COUNTRY_ZOOM_LEVELS** - Tracks initialization per zoom level:
- `COUNTRY_CODE` (VARCHAR(3)): ISO3 country code
- `ZOOM_LEVEL` (INTEGER): Zoom level
- `LAST_INITIALIZED` (TIMESTAMP_NTZ): When this zoom level was initialized
- Primary key: (COUNTRY_CODE, ZOOM_LEVEL)

### Quick Start: Managing Countries

**Option 1: Initialize via Command Line**
```bash
# Initialize Taiwan at zoom level 14 (tracking happens automatically)
python main_pipeline.py --type initialize --countries TWN --zoom 14 --rewrite 0
```

**Option 2: Initialize via GitHub Actions** (Recommended for production)
1. Go to GitHub repository → Actions tab
2. Select "Initialize New Country" workflow
3. Click "Run workflow"
4. Fill in: Country Code, Country Name, Zoom Level, Rewrite
5. Click "Run workflow"

**Option 3: Activate/Deactivate Countries**
```bash
# Via GitHub Actions: Use "Manage Country Status" workflow
# Or via Python:
python -c "from country_utils import activate_country; activate_country('TWN')"
python -c "from country_utils import deactivate_country; deactivate_country('VNM')"
```

### Automatic Country Detection

If `--countries` is not provided, the pipeline automatically fetches active countries from the `PIPELINE_COUNTRIES` Snowflake table:

```bash
# Uses active countries from Snowflake table
python main_pipeline.py --type update
```

### Verifying Initialization

```bash
# Check which countries are initialized
python -c "
from country_utils import get_initialized_zoom_levels, get_all_countries_from_snowflake
import pandas as pd

df = get_all_countries_from_snowflake()
print(df[['COUNTRY_CODE', 'COUNTRY_NAME', 'ZOOM_LEVEL', 'LAST_INITIALIZED']].to_string(index=False))
"
```

Or query directly in Snowflake:
```sql
-- View all initialized zoom levels
SELECT * FROM PIPELINE_COUNTRY_ZOOM_LEVELS 
ORDER BY COUNTRY_CODE, ZOOM_LEVEL;

-- View countries and their initialization status
SELECT 
    c.COUNTRY_CODE,
    c.COUNTRY_NAME,
    c.ZOOM_LEVEL as DEFAULT_ZOOM,
    c.LAST_INITIALIZED,
    z.ZOOM_LEVEL as INITIALIZED_ZOOM,
    z.LAST_INITIALIZED as ZOOM_INITIALIZED
FROM PIPELINE_COUNTRIES c
LEFT JOIN PIPELINE_COUNTRY_ZOOM_LEVELS z 
    ON c.COUNTRY_CODE = z.COUNTRY_CODE
WHERE c.ACTIVE = TRUE
ORDER BY c.COUNTRY_CODE, z.ZOOM_LEVEL;
```

---

## Multiple Zoom Levels

The pipeline supports multiple zoom levels per country. Each zoom level creates separate files:

- Base mercator views: `{country}_{zoom_level}.parquet`
- Impact views: `{country}_{storm}_{date}_{wind_threshold}_{zoom_level}.csv`
- CCI views: `{country}_{storm}_{date}_{zoom_level}_cci.csv`

### Adding New Zoom Levels

**Via Command Line:**
```bash
# Initialize Taiwan at zoom level 15
python main_pipeline.py --type initialize --countries TWN --zoom 15 --rewrite 0
# Tracking happens automatically
```

**Via GitHub Actions:**
Use the "Initialize New Country" workflow with an existing country code but a new zoom level.

### Processing with Different Zoom Levels

```bash
# Process with zoom level 14
python main_pipeline.py --type update --countries TWN --zoom 14

# Process with zoom level 15
python main_pipeline.py --type update --countries TWN --zoom 15
```

**Important:** The pipeline will load the base mercator view for the specified zoom level. If it doesn't exist, initialization will be triggered automatically (if `rewrite=0`).
