# Ahead of the Storm – Data Processing Setup Guide

## Related Repositories
- **[Ahead-of-the-Storm](https://github.com/unicef-drp/Ahead-of-the-Storm)**: Dash web application for visualizing hurricane impact forecasts. The application displays interactive maps, probabilistic analysis, and impact reports based on pre-processed hurricane data
- **[TC-ECMWF-Forecast-Pipeline](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline)**: Pipeline for processing ECMWF BUFR tropical cyclone and wind forecast data


## Prerequisites

1. **Python 3.11+** installed
2. **Virtual environment** activated (`.venv`)
3. **Environment variables** configured in `.env` file
   - Start from the provided example: `cp example_env.txt .env`
   - Edit values to match your environment (Snowflake, optional Azure)

### Environment setup (recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Required environment variables

- RESULTS_DIR (default: `results`)
- BBOX_FILE (e.g., `bbox.parquet`)
- STORMS_FILE (e.g., `storms.json`)
- ROOT_DATA_DIR (e.g., `geodb`)
- VIEWS_DIR (e.g., `aos_views`)
- SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
- DATA_PIPELINE_DB (`LOCAL` by default; use `BLOB` or `RO_BLOB` for Azure)
  - If `BLOB` or `RO_BLOB`, also set: `ACCOUNT_URL`, `SAS_TOKEN`

### API tokens
- GIGA_SCHOOL_LOCATION_API_KEY (required to fetch school locations)
- HEALTHSITES_API_KEY (required to fetch health center locations)
- GEOREPO_TOKEN (preferred/required if your org restricts GeoRepo access)


## Step 1: Set Up Bounding Box (Required Only Once)

The bounding box defines the geographic region of interest for hurricane impact analysis.

Using here countries in the Caribbean + Central America: ['ATG','BLZ','NIC','DOM','DMA','GRD','MSR','KNA','LCA','VCT','AIA','VGB']

**Command:**
```bash
python -c "from impact_analysis import save_bounding_box; countries = ['ATG','BLZ','NIC','DOM','DMA','GRD','MSR','KNA','LCA','VCT','AIA','VGB']; save_bounding_box(countries); print('Bounding box created!')"
```

**What it does:**
- Fetches administrative boundaries for all specified countries from UNICEF GeoRepo
- Creates a padded bounding box (1000km padding) that encompasses all countries
- Saves the bounding box to `project_results/climate/lacro_project/bbox.parquet`


## Step 2: Initialize Base Data (Required Only Once or After Adding Countries)

This step creates the mercator tile views and admin level views with demographic and infrastructure data for each country.

**Command:**
```bash
python main_pipeline.py --type initialize --zoom 14 --rewrite 0
```

**Parameters:**
- `--zoom` (default: 14): Zoom level for mercator tiles
- `--rewrite` (default: 0): Set to 1 to rewrite existing views, 0 to skip if they exist
- `--countries`: List of country codes (default: Caribbean + Central America countries)

**What it does:**
- Creates mercator tiles for each country at specified zoom level
- Creates admin level 1 views for each country
- Downloads and aggregates demographic data (WorldPop population, school age, infants)
- Downloads and aggregates infrastructure data (GHSL built surface, SMOD settlement)
- Fetches school locations (via GIGA API)
- Fetches health center locations (via HealthSites API) - cached after first fetch
- Saves base views to `geodb/aos_views/mercator_views/` and `geodb/aos_views/admin_views/`

**Note:** This process can take 30-60 minutes and downloads several GB of data. It only needs to be run once, or when you add new countries.

Requirements:
- GIGA_SCHOOL_LOCATION_API_KEY and HEALTHSITES_API_KEY must be set
- Network access to data sources (WorldPop, GHSL, SMOD, GIGA, HealthSites)


## Step 3: Process Storm Data

Once initialized, you can run the pipeline to process hurricane data from Snowflake.

**Command:**
```bash
python main_pipeline.py --type update --time_delta 9 --rewrite 0
```

**Parameters:**
- `--type` (default: update): Pipeline mode (initialize or update)
- `--time_delta` (default: 9): Number of days in the past to consider storms for analysis
- `--rewrite` (default: 0): Set to 1 to force reprocessing of existing storms, 0 to skip already processed
- `--countries`: List of country codes to process
- `--zoom` (default: 14): Zoom level for tiles
- `--skip-analysis`: Skip analysis step (for testing)

**What it does:**
- Connects to Snowflake and retrieves available storm data
- Filters storms by time delta (only processes storms within N days of today)
- For each storm/forecast combination that intersects the bounding box:
  - Loads hurricane envelope data
  - Creates impact views for schools, health centers, tiles, tracks, and admin levels
  - Calculates Cumulative Climate Impact (CCI) indices
  - Generates JSON impact reports
  - Saves views to `geodb/aos_views/hc_views/`, `geodb/aos_views/school_views/`, etc.
  - Records processed storms in `storms.json`

**Process Flow:**
1. Reads `storms.json` to track which storms have been processed
2. Queries Snowflake for new storms
3. Filters by time delta (only recent storms)
4. For each storm not yet processed (or if `rewrite=1`):
   - Loads envelope data (wind impact areas at different thresholds)
   - Checks if envelopes intersect with region of interest
   - Creates impact views for each country in the region:
     - School impact views (probability per wind threshold)
     - Health center impact views (probability per wind threshold)
     - Tile impact views (expected impacts per tile)
     - Admin level impact views (aggregated by admin level 1)
     - CCI views (cumulative climate impact indices)
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
Getting bounding box...
Envelopes intersect with region
Creating impact views...
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

## Troubleshooting

### "Bounding box not found" error
- Run **Step 1** to create the bounding box

### "No envelope data found" error
- This is normal for storms that don't affect the region
- Check that envelopes intersect with the bounding box

### "401 Unauthorized" for school data
- Verify `GIGA_SCHOOL_LOCATION_API_KEY` is set correctly in `.env`

### "SSL Certificate" errors
- Install: `pip install pip-system-certs`

### Pipeline fails on specific countries
- Some smaller Caribbean islands may have issues
- You can skip problematic countries or run with only working ones:
  ```bash
  python main_pipeline.py --countries ATG BLZ DOM
  ```

### "Operation not permitted" during initialize (WorldPop/GHSL downloads)
- Ensure your output/base directories exist and are writable:
  ```bash
  mkdir -p "${RESULTS_DIR:-results}" "${ROOT_DATA_DIR:-geodb}"/aos_views/{mercator_views,school_views,hc_views,track_views}
  ```
- If you are working from a protected/synced folder (e.g., OneDrive), consider running the project from a regular local path (e.g., `~/Projects/...`) or set your paths to a local writable directory.
- On macOS, you may need to grant Terminal/IDE Full Disk Access.
- If SSL issues appear during downloads, install system certs:
  ```bash
  pip install pip-system-certs
  ```

## Quick Start Summary

```bash
# 1. Create bounding box (one-time setup)
python -c "from impact_analysis import save_bounding_box; save_bounding_box(['ATG','BLZ','NIC','DOM','DMA','GRD','MSR','KNA','LCA','VCT','AIA','VGB'])"

# 2. Initialize base data (one-time setup, takes 30-60 min)
python main_pipeline.py --type initialize

# 3. Process storms (run regularly to update with new storm data)
python main_pipeline.py --type update
```

## Data Storage

- **Bounding box:** `{RESULTS_DIR}/{BBOX_FILE}` (e.g., `project_results/climate/lacro_project/bbox.parquet`)
- **Base mercator views:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/` (e.g., `geodb/aos_views/mercator_views/`)
- **Base admin views:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/` (e.g., `geodb/aos_views/admin_views/`)
- **Impact views:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/`, `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views/`, etc.
- **CCI views:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/` and `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/` (with `_cci.csv` suffix)
- **Impact reports:** `{RESULTS_DIR}/jsons/` (JSON files per country/storm/forecast)
- **Processed storms:** `{RESULTS_DIR}/{STORMS_FILE}` (e.g., `project_results/climate/lacro_project/storms.json`)
- **Raw data:** `{ROOT_DATA_DIR}/bronze/` (downloaded automatically)

See `FILE_STRUCTURE.md` for detailed file structure and naming conventions.

