# GitHub Actions Workflows

This document explains how to use GitHub Actions to manage countries and trigger pipeline runs.

## Overview

GitHub Actions workflows allow you to:
- **Add new countries** and automatically initialize them
- **Process past storms** for historical analysis
- **Patch specific columns** in existing country data without full re-initialization
- **Trigger pipeline runs** without manual command-line execution

## Prerequisites

**GitHub Secrets** must be configured:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA`
   - `SNOWFLAKE_STAGE_NAME`
   - `DATA_PIPELINE_DB` (should be `SNOWFLAKE`)
   - `GIGA_SCHOOL_LOCATION_API_KEY`
   - `HEALTHSITES_API_KEY`
   - `GEOREPO_API_KEY` (optional)
   - `GEOREPO_USER_EMAIL` (optional, required if using GEOREPO_API_KEY)


## Workflows

### 1. Manage Country Status

[![Manage Country Status](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/manage-country-status.yml/badge.svg)](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/manage-country-status.yml)

**Workflow:** `.github/workflows/manage-country-status.yml`

**Purpose:** Activate or deactivate countries in the pipeline.

**How to use:**
1. Go to **Actions** tab in GitHub
2. Select **"Manage Country Status"** workflow
3. Click **"Run workflow"**
4. Fill in the form:
   - **Country Code**: ISO3 code (e.g., `VNM`, `TWN`)
   - **Action**: Choose `activate` or `deactivate`
5. Click **"Run workflow"**

**What it does:**
1. Updates `ACTIVE` flag in `PIPELINE_COUNTRIES` table
2. Verifies the status change

**Example:**
```
Country Code: VNM
Action: deactivate
```

This will set Vietnam to inactive, so it won't be processed by the pipeline.

**Note:** Deactivated countries remain in the table but won't be included when the pipeline reads active countries from Snowflake.

### 2. Initialize New Country

[![Initialize New Country](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/initialize-country.yml/badge.svg)](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/initialize-country.yml)

**Workflow:** `.github/workflows/initialize-country.yml`

**Purpose:** Add a new country to the pipeline and initialize its base data.

**How to use:**
1. Go to **Actions** tab in GitHub
2. Select **"Initialize New Country"** workflow
3. Click **"Run workflow"**
4. Fill in the form:
   - **Country Code**: ISO3 code (e.g., `TWN`, `DOM`, `VNM`)
   - **Country Name**: Full name (e.g., `Taiwan`)
   - **Zoom Level**: Default is `14` (for analysis tiles)
   - **Center Lat**: Latitude for map center (e.g., `23.50`)
   - **Center Lon**: Longitude for map center (e.g., `121.00`)
   - **View Zoom**: Zoom level for visualization map (e.g., `8`)
   - **Admin Levels**: Space-separated admin levels to initialize (default: `1`). Use `1 2` to also create admin2 base parquets. Logs an error and skips gracefully if a requested level is unavailable in GeoRepo.
   - **Rewrite**: `0` to skip existing, `1` to overwrite
5. Click **"Run workflow"**

**What it does:**
1. Adds country to `PIPELINE_COUNTRIES` table in Snowflake (with map configuration)
2. Runs initialization pipeline for the country
3. Updates `LAST_INITIALIZED` timestamp

**Example:**
```
Country Code: TWN
Country Name: Taiwan
Zoom Level: 14
Center Lat: 23.50
Center Lon: 121.00
View Zoom: 8
Admin Levels: 1 2
Rewrite: 0
```

### 3. Update Country Map Config

[![Update Country Map Config](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/update-country-map-config.yml/badge.svg)](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/update-country-map-config.yml)

**Workflow:** `.github/workflows/update-country-map-config.yml`

**Purpose:** Update the map configuration (center coordinates and/or view zoom) for an existing country.

**How to use:**
1. Go to **Actions** tab in GitHub
2. Select **"Update Country Map Config"** workflow
3. Click **"Run workflow"**
4. Fill in the form:
   - **Country Code**: ISO3 code (e.g., `TWN`, `DOM`, `VNM`) - **Required**
   - **Center Lat**: Latitude for map center (e.g., `23.50`) - **Optional**
   - **Center Lon**: Longitude for map center (e.g., `121.00`) - **Optional**
   - **View Zoom**: Zoom level for visualization map (e.g., `8`) - **Optional**
5. Click **"Run workflow"**

**What it does:**
1. Updates only the fields you provide (leaves others unchanged)
2. Verifies the update was successful

**Examples:**

Update only the view zoom:
```
Country Code: TWN
Center Lat: (leave empty)
Center Lon: (leave empty)
View Zoom: 9
```

Update only coordinates:
```
Country Code: TWN
Center Lat: 23.50
Center Lon: 121.00
View Zoom: (leave empty)
```

Update all fields:
```
Country Code: TWN
Center Lat: 23.50
Center Lon: 121.00
View Zoom: 8
```

**Note:** 
- At least one field (center_lat, center_lon, or view_zoom) must be provided
- Fields left empty will not be updated (existing values preserved)
- The country must already exist in the `PIPELINE_COUNTRIES` table

### 4. Process Past Storms

[![Process Past Storms](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/process-past-storms.yml/badge.svg)](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/process-past-storms.yml)

**Workflow:** `.github/workflows/process-past-storms.yml`

**Purpose:** Process historical storm data for a date range.

**How to use:**
1. Go to **Actions** tab in GitHub
2. Select **"Process Past Storms"** workflow
3. Click **"Run workflow"**
4. Fill in the form:
   - **Start Date**: `YYYY-MM-DD` (e.g., `2025-11-01`)
   - **End Date**: `YYYY-MM-DD` (e.g., `2025-11-10`)
   - **Countries**: Comma-separated (e.g., `TWN,DOM,VNM`) or leave empty for all active countries
   - **Storm**: Optional specific storm name (e.g., `FUNG-WONG`)
   - **Rewrite**: `0` to skip existing, `1` to overwrite
5. Click **"Run workflow"**

**What it does:**
1. Gets list of countries (from input or Snowflake table)
2. For each date in the range:
   - Runs pipeline with `--date` parameter
   - Processes all storms for that date (or specific storm if provided)
   - Creates impact views and reports

**Example:**
```
Start Date: 2025-11-01
End Date: 2025-11-10
Countries: TWN,DOM
Storm: (leave empty for all storms)
Rewrite: 0
```

This will process all storms from November 1-10, 2025 for Taiwan and Dominican Republic.

### 5. Patch Country Columns

[![Patch Country Columns](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/patch-columns.yml/badge.svg)](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE/actions/workflows/patch-columns.yml)

**Workflow:** `.github/workflows/patch-columns.yml`

**Purpose:** Backfill specific columns in existing country mercator parquets without full re-initialization. Use this when a data source becomes available after a country was first initialized (e.g. new WorldPop dataset, custom shelter registry, RWI data now available for a country).

**How to use:**
1. Go to **Actions** tab in GitHub
2. Select **"Patch Country Columns"** workflow
3. Click **"Run workflow"**
4. Fill in the form:
   - **Countries**: Comma-separated (e.g., `PNG,FJI`) or leave empty for all active countries
   - **Columns**: Space-separated column names to patch (required — see supported columns below)
   - **Zoom Level**: Must match the existing mercator parquet (default: `14`)
5. Click **"Run workflow"**

**Supported columns:**

| Column | Source |
|--------|--------|
| `population` | WorldPop (total, 1km) |
| `school_age_population` | WorldPop GR2/2025 (ages 5–14, 100m) |
| `infant_population` | WorldPop (ages 0–4, 100m) |
| `adolescent_population` | WorldPop GR2/2025 (ages 15–19y, 100m) |
| `built_surface_m2` | GHSL built surface |
| `smod_class` | GHSL SMOD L2 settlement class |
| `smod_class_l1` | Derived from `smod_class` (always updated together) |
| `rwi` | HDX Relative Wealth Index |
| `schools` | GIGA school location API → updates `num_schools` column |
| `hcs` | HealthSites API → updates `num_hcs` column |
| `shelters` | OSM Overpass / custom CSV → updates `num_shelters` column |
| `wash` | OSM Overpass / custom CSV → updates `num_wash` column |
| `admin<N>` (e.g. `admin2`) | Adds a new admin level base parquet without re-initializing |

**Notes:**
- `schools`, `hcs`, `shelters`, `wash` re-fetch the full facility location cache and recompute per-tile counts; the parquet columns they update are `num_schools`, `num_hcs`, `num_shelters`, `num_wash`
- Patching `smod_class` always updates `smod_class_l1` at the same time (derived field)
- Patching any regular column updates the mercator parquet and **all initialized admin parquets** (re-aggregated automatically for every admin level found)
- Patching `admin<N>` creates a new admin level base parquet from the existing mercator tiles — no GeoRepo re-fetch of existing levels needed
- Custom CSVs in `geodb/custom/` take priority over API/raster re-processing
- The country must already be initialized (base mercator parquet must exist)
- Population columns can be patched individually — useful when a new WorldPop dataset is released

**Examples:**
```
# Backfill shelter and WASH data after adding custom data files
Countries: PNG
Columns: shelters wash
Zoom Level: 14

# Update wealth and settlement data for all active countries
Countries: (leave empty)
Columns: rwi smod_class
Zoom Level: 14

# Update population from a new WorldPop dataset
Countries: TWN,DOM
Columns: population school_age_population infant_population adolescent_population
Zoom Level: 14
```

## Country Management

### Countries Stored in Snowflake

Countries are stored in the `PIPELINE_COUNTRIES` table:

| Column | Description |
|--------|-------------|
| `COUNTRY_CODE` | ISO3 code (e.g., `TWN`) |
| `COUNTRY_NAME` | Full name (e.g., `Taiwan`) |
| `ACTIVE` | Whether country is active (boolean) |
| `ADDED_DATE` | When country was added |
| `LAST_INITIALIZED` | Last initialization timestamp |
| `ZOOM_LEVEL` | Zoom level for analysis tiles (usually 14) |
| `CENTER_LAT` | Latitude for visualization map center |
| `CENTER_LON` | Longitude for visualization map center |
| `VIEW_ZOOM` | Zoom level for visualization map (different from analysis ZOOM_LEVEL) |
| `NOTES` | Optional notes |

### Viewing Countries

```sql
-- View all active countries
SELECT * FROM PIPELINE_COUNTRIES 
WHERE ACTIVE = TRUE 
ORDER BY COUNTRY_CODE;

-- View countries needing initialization
SELECT * FROM PIPELINE_COUNTRIES 
WHERE ACTIVE = TRUE 
  AND LAST_INITIALIZED IS NULL;
```

### Adding Countries Manually

```sql
-- Add a new country with map configuration
INSERT INTO PIPELINE_COUNTRIES (COUNTRY_CODE, COUNTRY_NAME, ZOOM_LEVEL, CENTER_LAT, CENTER_LON, VIEW_ZOOM)
VALUES ('PHL', 'Philippines', 14, 12.88, 121.77, 6);

-- Then initialize via GitHub Actions or manually
```

**Note:** Map configuration (`CENTER_LAT`, `CENTER_LON`, `VIEW_ZOOM`) is required for visualization. Use the "Update Country Map Config" workflow to update these values if needed.

### Activating/Deactivating Countries

**Via GitHub Actions (Recommended):**
1. Use the **"Manage Country Status"** workflow
2. Select country code and action (activate/deactivate)

**Via SQL (Manual):**
```sql
-- Deactivate a country (won't be processed)
UPDATE PIPELINE_COUNTRIES
SET ACTIVE = FALSE
WHERE COUNTRY_CODE = 'OLD';

-- Activate a country
UPDATE PIPELINE_COUNTRIES
SET ACTIVE = TRUE
WHERE COUNTRY_CODE = 'OLD';
```

## Pipeline Behavior

### Automatic Country Detection

If `--countries` is not provided, the pipeline will:
1. Try to read from `PIPELINE_COUNTRIES` table
2. Use all active countries
3. Fall back to default list if table doesn't exist

This allows:
- **GitHub Actions** to use Snowflake as source of truth
- **SPCS jobs** to process all active countries automatically
- **Manual runs** to still override with `--countries`

### Command-Line Override

You can still override countries via command line:
```bash
python main_pipeline.py --type update --countries TWN DOM
```

This takes precedence over the Snowflake table.

## Setting Up GitHub Secrets

1. Go to your GitHub repository
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **"New repository secret"**
4. Add each secret:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA`
   - `SNOWFLAKE_STAGE_NAME`
   - `DATA_PIPELINE_DB` (set to `SNOWFLAKE`)
   - `GIGA_SCHOOL_LOCATION_API_KEY`
   - `HEALTHSITES_API_KEY`
   - `GEOREPO_API_KEY` (optional)
   - `GEOREPO_USER_EMAIL` (optional, required if using GEOREPO_API_KEY)

## Troubleshooting

### "No active countries found in Snowflake table"

- Run `snowflake/setup_countries_table.sql` to create the table
- Add countries using the GitHub Actions workflow or SQL

### "Error retrieving countries from Snowflake"

- Check GitHub secrets are configured correctly
- Verify Snowflake credentials have access to the table
- Check that table exists: `SELECT * FROM PIPELINE_COUNTRIES LIMIT 1;`

### Workflow fails with authentication error

- Verify all GitHub secrets are set
- Check that `SNOWFLAKE_PASSWORD` is correct
- Ensure user has permissions to read/write the table