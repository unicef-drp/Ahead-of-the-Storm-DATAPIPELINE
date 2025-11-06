# [NOT UPDATED YET] File Structure and Output Directories

This document lists all files produced and downloaded by the Ahead of the Storm pipeline, organized by directory structure.

## Directory Structure Overview

The pipeline uses environment variables to configure base directories:
- `RESULTS_DIR` - Results and configuration files (default: `results`)
- `ROOT_DATA_DIR` - Base data directory (default: `geodb`)
- `VIEWS_DIR` - Subdirectory for views (default: `aos_views`)
- `BBOX_FILE` - Bounding box filename (e.g., `bbox.parquet`)
- `STORMS_FILE` - Processed storms tracking file (e.g., `storms.json`)

---

## Files Produced During Initialize (`--type initialize`)

### 1. Bounding Box File
**Location:** `{RESULTS_DIR}/{BBOX_FILE}`
- **Example:** `results/bbox.parquet` or `project_results/climate/lacro_project/bbox.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Single polygon representing the padded bounding box for all countries
- **Created by:** `save_bounding_box()`

### 2. Base Mercator Views (per country)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/{country}_{zoom_level}.parquet`
- **Example:** `geodb/aos_views/mercator_views/DOM_14.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Mercator tiles at specified zoom level with:
  - Population data (WorldPop)
  - School age population
  - Infant population
  - Built surface area (GHSL)
  - Settlement classification (SMOD)
  - School locations (count per tile)
  - Health center locations (count per tile)
  - Relative Wealth Index (RWI)
  - Administrative boundary IDs
- **Created by:** `save_mercator_view()`
- **Note:** One file per country per zoom level

### 3. Base Admin Views (per country)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_admin1.parquet`
- **Example:** `geodb/aos_views/admin_views/DOM_admin1.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Administrative level 1 boundaries with aggregated:
  - Population totals
  - School age population totals
  - Infant population totals
  - Built surface totals
  - School counts
  - Health center counts
  - Average RWI
  - Average SMOD class
  - Administrative names and geometries
- **Created by:** `save_admin_view()`
- **Note:** One file per country

### 4. Health Center Locations (per country, cached)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/{country}_health_centers.parquet`
- **Example:** `geodb/aos_views/hc_views/DOM_health_centers.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Health center locations fetched from HealthSites API
- **Created by:** `save_hc_locations()`
- **Note:** Cached after first fetch to avoid repeated API calls

---

## Files Produced During Update (`--type update`)

For each storm/forecast combination processed, the following files are created:

### 5. School Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/school_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** School locations with impact probability for each wind threshold
- **Created by:** `save_school_view()`
- **Note:** Multiple files per storm (one per wind threshold: 34, 40, 50, 64, 83, 96, 113, 137)

### 6. Health Center Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/hc_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Health center locations with impact probability for each wind threshold
- **Created by:** `save_hc_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 7. Mercator Tile Impact Views (per country, per storm, per forecast, per wind threshold, per zoom)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/{country}_{storm}_{date}_{wind_threshold}_{zoom_level}.csv`
- **Example:** `geodb/aos_views/mercator_views/DOM_LORENZO_20251015120000_34_14.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** Expected impact values per tile:
  - `E_population`
  - `E_school_age_population`
  - `E_infant_population`
  - `E_built_surface_m2`
  - `E_num_schools`
  - `E_num_hcs`
  - `E_rwi`
  - `E_smod_class`
  - `probability`
- **Created by:** `save_tiles_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 8. CCI (Cumulative Climate Impact) Tile Views (per country, per storm, per forecast, per zoom)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/{country}_{storm}_{date}_{zoom_level}_cci.csv`
- **Example:** `geodb/aos_views/mercator_views/DOM_LORENZO_20251015120000_14_cci.csv`
- **Format:** CSV (DataFrame)
- **Content:** Cumulative Climate Impact indices:
  - `CCI_children`
  - `E_CCI_children`
  - `CCI_school_age`
  - `E_CCI_school_age`
  - `CCI_infants`
  - `E_CCI_infants`
  - `CCI_pop`
  - `E_CCI_pop`
- **Created by:** `save_cci_tiles()`
- **Note:** One file per storm (aggregates all wind thresholds)

### 9. Admin Level Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_{storm}_{date}_{wind_threshold}_admin1.csv`
- **Example:** `geodb/aos_views/admin_views/DOM_LORENZO_20251015120000_34_admin1.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** Expected impact values aggregated by admin level 1:
  - `E_population`
  - `E_school_age_population`
  - `E_infant_population`
  - `E_built_surface_m2`
  - `E_num_schools`
  - `E_num_hcs`
  - `E_rwi`
  - `E_smod_class`
  - `probability`
  - `name` (admin name)
- **Created by:** `save_admin_tiles_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 10. CCI Admin Views (per country, per storm, per forecast)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_{storm}_{date}_admin1_cci.csv`
- **Example:** `geodb/aos_views/admin_views/DOM_LORENZO_20251015120000_admin1_cci.csv`
- **Format:** CSV (DataFrame)
- **Content:** Cumulative Climate Impact indices aggregated by admin level 1
- **Created by:** `save_cci_admin()`
- **Note:** One file per storm

### 11. Track Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/track_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/track_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Ensemble member tracks with severity metrics:
  - `severity_schools`
  - `severity_hcs`
  - `severity_population`
  - `severity_school_age_population`
  - `severity_infant_population`
  - `severity_built_surface_m2`
- **Created by:** `save_tracks_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 12. JSON Impact Reports (per country, per storm, per forecast)
**Location:** `{RESULTS_DIR}/jsons/{country}_{storm}_{date}.json`
- **Example:** `results/jsons/DOM_LORENZO_20251015120000.json`
- **Format:** JSON
- **Content:** Comprehensive impact report data including:
  - Expected impacts by wind threshold
  - Top 5 schools at risk
  - Top 5 health centers at risk
  - Administrative breakdowns
  - Vulnerability indicators (poverty, urban/rural)
  - Change indicators (compared to previous forecast)
- **Created by:** `save_json_report()`
- **Note:** One file per country per storm per forecast

### 13. Processed Storms Tracking File
**Location:** `{RESULTS_DIR}/{STORMS_FILE}`
- **Example:** `results/storms.json` or `project_results/climate/lacro_project/storms.json`
- **Format:** JSON or Parquet (currently inconsistent - see bug in documentation review)
- **Content:** Dictionary tracking which storm/forecast combinations have been processed
- **Created by:** `save_json_storms()`
- **Note:** Updated after each successful storm processing

---

## Files Downloaded from External Sources

These files are downloaded automatically by the GigaSpatial library and stored in the data store. The exact location depends on the data store configuration (LOCAL vs BLOB).

### 14. WorldPop Population Data
**Location:** `{ROOT_DATA_DIR}/bronze/` (or equivalent in data store)
- **Source:** WorldPop API
- **Downloaded by:** `MercatorViewGenerator.map_wp_pop()`
- **Note:** Downloaded on-demand during initialize, cached for reuse

### 15. GHSL Built Surface Data
**Location:** `{ROOT_DATA_DIR}/bronze/` (or equivalent in data store)
- **Source:** Global Human Settlement Layer (GHSL)
- **Downloaded by:** `MercatorViewGenerator.map_built_s()`
- **Note:** Downloaded on-demand during initialize, cached for reuse

### 16. SMOD Settlement Classification Data
**Location:** `{ROOT_DATA_DIR}/bronze/` (or equivalent in data store)
- **Source:** GHSL Settlement Model (SMOD)
- **Downloaded by:** `MercatorViewGenerator.map_smod()`
- **Note:** Downloaded on-demand during initialize, cached for reuse

### 17. Relative Wealth Index (RWI) Data
**Location:** `{ROOT_DATA_DIR}/bronze/` (or equivalent in data store)
- **Source:** Facebook/Meta RWI dataset
- **Downloaded by:** `RWIHandler.load_data()`
- **Note:** Downloaded on-demand during initialize, cached for reuse

### 18. School Locations
**Source:** GIGA School Location API
- **Fetched by:** `GigaSchoolLocationFetcher.fetch_locations()`
- **Note:** Fetched via API, not cached to disk (fetched each time)
- **Requires:** `GIGA_SCHOOL_LOCATION_API_KEY` environment variable

### 19. Health Center Locations
**Source:** HealthSites API
- **Fetched by:** `HealthSitesFetcher.fetch_facilities()`
- **Cached to:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/{country}_health_centers.parquet`
- **Note:** Cached after first fetch to avoid repeated API calls
- **Requires:** `HEALTHSITES_API_KEY` environment variable

### 20. Administrative Boundaries
**Source:** UNICEF GeoRepo (via GigaSpatial)
- **Fetched by:** `AdminBoundaries.create()`
- **Note:** Fetched via API, not cached to disk (fetched each time)
- **Optional:** `GEOREPO_TOKEN` environment variable

---

## Complete Directory Structure Example

```
{RESULTS_DIR}/                          # e.g., results/ or project_results/climate/lacro_project/
├── {BBOX_FILE}                         # bbox.parquet
├── {STORMS_FILE}                       # storms.json
└── jsons/
    ├── {country}_{storm}_{date}.json   # Impact reports

{ROOT_DATA_DIR}/                        # e.g., geodb/
├── bronze/                             # Raw downloaded data (WorldPop, GHSL, SMOD, RWI)
│   └── [various downloaded files]
└── {VIEWS_DIR}/                        # e.g., aos_views/
    ├── mercator_views/
    │   ├── {country}_{zoom}.parquet                    # Base mercator views
    │   ├── {country}_{storm}_{date}_{wind}_{zoom}.csv  # Impact tile views
    │   └── {country}_{storm}_{date}_{zoom}_cci.csv     # CCI tile views
    ├── admin_views/
    │   ├── {country}_admin1.parquet                     # Base admin views
    │   ├── {country}_{storm}_{date}_{wind}_admin1.csv # Impact admin views
    │   └── {country}_{storm}_{date}_admin1_cci.csv    # CCI admin views
    ├── school_views/
    │   └── {country}_{storm}_{date}_{wind}.parquet      # School impact views
    ├── hc_views/
    │   ├── {country}_health_centers.parquet             # Cached health center locations
    │   └── {country}_{storm}_{date}_{wind}.parquet      # Health center impact views
    └── track_views/
        └── {country}_{storm}_{date}_{wind}.parquet      # Track impact views
```

---

## File Naming Conventions

### Date Format
- All dates in filenames use format: `YYYYMMDDHHMMSS`
- Example: `20251015120000` = October 15, 2025, 12:00:00 UTC

### Wind Threshold Values
- Common thresholds: `34`, `40`, `50`, `64`, `83`, `96`, `113`, `137`
- Represent wind speeds in knots

### Country Codes
- ISO3 country codes (e.g., `DOM`, `ATG`, `BLZ`)

### Storm Names
- Uppercase storm names (e.g., `LORENZO`, `JERRY`)

---

## Notes

1. **Data Store Backend:** All files are written through the `data_store` abstraction, which can be:
   - `LocalDataStore` - Files written to local filesystem
   - `ADLSDataStore` - Files written to Azure Blob Storage
   - Controlled by `DATA_PIPELINE_DB` environment variable

2. **File Formats:**
   - `.parquet` files contain GeoDataFrames (with geometry)
   - `.csv` files contain DataFrames (no geometry, just data)


3. **Storage Location:**
   - If using Azure Blob Storage, paths are relative to the blob container
   - If using local storage, paths are relative to the project root or configured base directory