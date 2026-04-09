# File Structure and Output Directories

This document lists all files produced and downloaded by the Ahead of the Storm pipeline, organized by directory structure.

## Directory Structure Overview

The pipeline uses environment variables to configure base directories:
- `RESULTS_DIR` - Results and configuration files (default: `project_results/climate/lacro_project`)
- `ROOT_DATA_DIR` - Base data directory (default: `geodb`)
- `VIEWS_DIR` - Subdirectory for views (default: `aos_views`)
- `STORMS_FILE` - Processed storms tracking file (default: `storms.json`)

---

## Files Produced During Initialize (`--type initialize`)

**Note:** The pipeline no longer requires a separate bounding box file. Country boundaries are checked directly with a 1500km buffer during processing.

### 1. Base Mercator Views (per country)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/{country}_{zoom_level}.parquet`
- **Example:** `geodb/aos_views/mercator_views/DOM_14.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Mercator tiles at specified zoom level with:
  - Population data (WorldPop): `population`, `school_age_population`, `infant_population`, `adolescent_population`
  - Built surface area (GHSL): `built_surface_m2`
  - Settlement classification (SMOD): `smod_class` (L2 raw), `smod_class_l1` (derived 3-class)
  - Relative Wealth Index: `rwi`
  - Facility counts per tile: `num_schools`, `num_hcs`, `num_shelters`, `num_wash`
  - Administrative boundary ID (admin level 1): `id`
- **Created by:** `create_mercator_country_layer()` via `save_mercator_and_admin_views()`
- **Note:** One file per country per zoom level

### 2. Base Admin Views (per country)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_admin1.parquet`
- **Example:** `geodb/aos_views/admin_views/DOM_admin1.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Administrative level 1 boundaries with aggregated:
  - Population totals: `population`, `school_age_population`, `infant_population`, `adolescent_population`
  - Built surface total: `built_surface_m2`
  - Facility counts: `num_schools`, `num_hcs`, `num_shelters`, `num_wash`
  - Average wealth/settlement: `rwi`, `smod_class`, `smod_class_l1`
  - Administrative names and geometries
- **Created by:** `create_admin_country_layer()` via `save_mercator_and_admin_views()`
- **Note:** One file per country, created during initialize

### 3. School Locations (per country, cached)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views/{country}_schools.parquet`
- **Example:** `geodb/aos_views/school_views/DOM_schools.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** School locations fetched from GIGA School Location API
- **Created by:** `save_school_locations()`
- **Note:** Cached after first fetch to avoid repeated API calls. Replaced by `geodb/custom/{country}_schools.csv` if present.

### 4. Health Center Locations (per country, cached)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/{country}_hcs.parquet`
- **Example:** `geodb/aos_views/hc_views/DOM_hcs.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Health center locations fetched from HealthSites API
- **Created by:** `save_hc_locations()`
- **Note:** Cached after first fetch to avoid repeated API calls. Replaced by `geodb/custom/{country}_health_centers.csv` if present.

### 5. Shelter Locations (per country, cached)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/shelter_views/{country}_shelters.parquet`
- **Example:** `geodb/aos_views/shelter_views/DOM_shelters.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Emergency shelter locations fetched from OSM Overpass (`social_facility=shelter`)
- **Created by:** `save_shelter_locations()`
- **Note:** Cached after first fetch. Replaced by `geodb/custom/{country}_shelters.csv` if present. OSM coverage is sparse — providing a custom government shelter registry is recommended.

### 6. WASH Locations (per country, cached)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/wash_views/{country}_wash.parquet`
- **Example:** `geodb/aos_views/wash_views/DOM_wash.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** WASH infrastructure locations fetched from OSM Overpass (drinking water, toilets, water works, pumping stations, etc.)
- **Created by:** `save_wash_locations()`
- **Note:** Cached after first fetch. Replaced by `geodb/custom/{country}_wash.csv` if present.

---

## Files Produced During Update (`--type update`)

For each storm/forecast combination processed, the following files are created:

### 7. School Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/school_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** School locations with impact probability for each wind threshold
- **Created by:** `save_school_view()`
- **Note:** Multiple files per storm (one per wind threshold: 34, 40, 50, 64, 83, 96, 113, 137)

### 8. Health Center Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/hc_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Health center locations with impact probability for each wind threshold
- **Created by:** `save_hc_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 9. Shelter Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/shelter_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/shelter_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Shelter locations with impact probability for each wind threshold
- **Created by:** `save_shelter_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 10. WASH Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/wash_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/wash_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** WASH facility locations with impact probability for each wind threshold
- **Created by:** `save_wash_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 11. Mercator Tile Impact Views (per country, per storm, per forecast, per wind threshold, per zoom)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/{country}_{storm}_{date}_{wind_threshold}_{zoom_level}.csv`
- **Example:** `geodb/aos_views/mercator_views/DOM_LORENZO_20251015120000_34_14.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** Expected impact values per tile:
  - `E_population`, `E_school_age_population`, `E_infant_population`, `E_adolescent_population`
  - `E_built_surface_m2`
  - `E_num_schools`, `E_num_hcs`, `E_num_shelters`, `E_num_wash`
  - `E_rwi`, `E_smod_class`, `E_smod_class_l1`
  - `probability`
- **Created by:** `save_tiles_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 12. CCI (Child Cyclone Index) Tile Views (per country, per storm, per forecast, per zoom)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/{country}_{storm}_{date}_{zoom_level}_cci.csv`
- **Example:** `geodb/aos_views/mercator_views/DOM_LORENZO_20251015120000_14_cci.csv`
- **Format:** CSV (DataFrame)
- **Content:** Child Cyclone Index (CCI) values:
  - `CCI_children`, `E_CCI_children`
  - `CCI_school_age`, `E_CCI_school_age`
  - `CCI_infants`, `E_CCI_infants`
  - `CCI_adolescent`, `E_CCI_adolescent`
  - `CCI_pop`, `E_CCI_pop`
- **Created by:** `save_cci_tiles()`
- **Note:** One file per storm (aggregates all wind thresholds)

### 13. Admin Level Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_{storm}_{date}_{wind_threshold}_admin1.csv`
- **Example:** `geodb/aos_views/admin_views/DOM_LORENZO_20251015120000_34_admin1.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** Expected impact values aggregated by admin level 1:
  - `E_population`, `E_school_age_population`, `E_infant_population`, `E_adolescent_population`
  - `E_built_surface_m2`
  - `E_num_schools`, `E_num_hcs`, `E_num_shelters`, `E_num_wash`
  - `E_rwi`, `E_smod_class`, `E_smod_class_l1`
  - `probability`
  - `name` (admin name)
- **Created by:** `save_admin_tiles_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 14. CCI Admin Views (per country, per storm, per forecast)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_{storm}_{date}_admin1_cci.csv`
- **Example:** `geodb/aos_views/admin_views/DOM_LORENZO_20251015120000_admin1_cci.csv`
- **Format:** CSV (DataFrame)
- **Content:** Child Cyclone Index (CCI) values aggregated by admin level 1
- **Created by:** `save_cci_admin()`
- **Note:** One file per storm

### 15. Track Views (per country, per storm, per forecast, per wind threshold)
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

### 16. JSON Impact Reports (per country, per storm, per forecast)
**Location:** `{RESULTS_DIR}/jsons/{country}_{storm}_{date}.json`
- **Example:** `project_results/climate/lacro_project/jsons/DOM_LORENZO_20251015120000.json`
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

### 17. Processed Storms Tracking File
**Location:** `{RESULTS_DIR}/{STORMS_FILE}`
- **Example:** `project_results/climate/lacro_project/storms.json`
- **Format:** JSON
- **Content:** Dictionary tracking which storm/forecast combinations have been processed
- **Created by:** `save_json_storms()`
- **Note:** Updated after each successful storm processing

---

## Files Downloaded from External Sources

These files are downloaded automatically by the GigaSpatial library and stored in the data store. The exact location depends on the data store configuration (LOCAL vs BLOB).

> **Note on raster data storage:** The raw raster files (WorldPop, GHSL, SMOD, RWI) are downloaded
> and cached internally by giga-spatial in its own local cache directory. They are **not** written to
> the pipeline's data store or Snowflake stage. However, the aggregated per-tile values derived from
> these rasters **are** permanently stored in the base mercator view parquet
> (`mercator_views/{country}_{zoom}.parquet`). This means the spatial distribution of all metrics
> below can be visualized directly from that parquet — each tile has a geometry and the corresponding
> aggregated value — without needing access to the original rasters.

### 18. WorldPop Population Data
- **Source:** WorldPop API (GR2, year=2025)
- **Downloaded by:** `MercatorViewGenerator` (giga-spatial internal)
- **Raw cache:** `geodb/bronze/` (subdirectory managed by giga-spatial) — written to the active data store (local filesystem or Snowflake stage). On first init for a country all 62 age-band files (~175 MB) are downloaded and cached; subsequent runs reuse the cache.
- **Stored in mercator parquet as:** `population` (100m res, sum per tile), `school_age_population`, `infant_population`, `adolescent_population` (all 100m res, sum per tile)

### 19. GHSL Built Surface Data
- **Source:** Global Human Settlement Layer (GHSL), year=2020, 100m resolution
- **Downloaded by:** `MercatorViewGenerator` (giga-spatial internal)
- **Raw cache:** `geodb/bronze/` (subdirectory managed by giga-spatial) — written to the active data store on first use, reused on subsequent runs
- **Stored in mercator parquet as:** `built_surface_m2` (sum per tile)

### 20. SMOD Settlement Classification Data
- **Source:** GHSL Settlement Model (SMOD), year=2020, 1km resolution
- **Downloaded by:** `MercatorViewGenerator` (giga-spatial internal)
- **Raw cache:** `geodb/bronze/` (subdirectory managed by giga-spatial) — written to the active data store on first use, reused on subsequent runs
- **Stored in mercator parquet as:** `smod_class` (raw L2 median per tile, values 10–30) and `smod_class_l1` (derived 3-class: 1=rural, 2=suburban, 3=urban)

### 21. Relative Wealth Index (RWI) Data
- **Source:** Facebook/Meta RWI dataset via HDX
- **Downloaded by:** `RWIHandler` (giga-spatial internal)
- **Raw cache:** `geodb/bronze/` (subdirectory managed by giga-spatial) — written to the active data store on first use, reused on subsequent runs
- **Stored in mercator parquet as:** `rwi` (mean per tile)
- **Note:** Not available for all countries. Tiles will have NaN for `rwi` where data is unavailable — no error raised.

### 22. School Locations
**Source:** GIGA School Location API
- **Fetched by:** `GigaSchoolLocationFetcher.fetch_locations()`
- **Cached to:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views/{country}_schools.parquet`
- **Note:** Cached after first fetch to avoid repeated API calls
- **Requires:** `GIGA_SCHOOL_LOCATION_API_KEY` environment variable

### 23. Health Center Locations
**Source:** HealthSites API
- **Fetched by:** `HealthSitesFetcher.fetch_facilities()`
- **Cached to:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/{country}_hcs.parquet`
- **Note:** Cached after first fetch to avoid repeated API calls
- **Requires:** `HEALTHSITES_API_KEY` environment variable

### 24. Administrative Boundaries
**Source:** UNICEF GeoRepo (via GigaSpatial)
- **Fetched by:** `AdminBoundaries.create()`
- **Note:** Fetched via API, not cached to disk (fetched each time)
- **Optional:** `GEOREPO_TOKEN` environment variable

---

## Complete Directory Structure Example

```
{RESULTS_DIR}/                          # e.g., results/
├── {STORMS_FILE}                       # storms.json
└── jsons/
    └── {country}_{storm}_{date}.json   # Impact reports

{ROOT_DATA_DIR}/                        # e.g., geodb/
├── custom/                             # Custom data overrides (never overwritten by pipeline)
│   ├── {country}_schools.csv           # Replaces GIGA school API
│   ├── {country}_health_centers.csv    # Replaces HealthSites API
│   ├── {country}_shelters.csv          # Replaces OSM shelter query
│   ├── {country}_wash.csv              # Replaces OSM WASH query
│   ├── {country}_population_z{zoom}.csv
│   ├── {country}_built_surface_z{zoom}.csv
│   ├── {country}_smod_z{zoom}.csv
│   └── {country}_rwi_z{zoom}.csv
└── {VIEWS_DIR}/                        # e.g., aos_views/
    ├── mercator_views/
    │   ├── {country}_{zoom}.parquet                    # Base mercator views
    │   ├── {country}_{storm}_{date}_{wind}_{zoom}.csv  # Impact tile views
    │   └── {country}_{storm}_{date}_{zoom}_cci.csv     # CCI tile views
    ├── admin_views/
    │   ├── {country}_admin1.parquet                     # Base admin views
    │   ├── {country}_{storm}_{date}_{wind}_admin1.csv   # Impact admin views
    │   └── {country}_{storm}_{date}_admin1_cci.csv      # CCI admin views
    ├── school_views/
    │   ├── {country}_schools.parquet                    # Cached school locations
    │   └── {country}_{storm}_{date}_{wind}.parquet      # School impact views
    ├── hc_views/
    │   ├── {country}_hcs.parquet                        # Cached health center locations
    │   └── {country}_{storm}_{date}_{wind}.parquet      # Health center impact views
    ├── shelter_views/
    │   ├── {country}_shelters.parquet                   # Cached shelter locations
    │   └── {country}_{storm}_{date}_{wind}.parquet      # Shelter impact views
    ├── wash_views/
    │   ├── {country}_wash.parquet                       # Cached WASH locations
    │   └── {country}_{storm}_{date}_{wind}.parquet      # WASH impact views
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
   - `LocalDataStore` - Files written to local filesystem (`DATA_PIPELINE_DB=LOCAL`)
   - `ADLSDataStore` - Files written to Azure Blob Storage (`DATA_PIPELINE_DB=BLOB`)
   - `SnowflakeDataStore` - Files written to Snowflake internal stage (`DATA_PIPELINE_DB=SNOWFLAKE`)
   - Controlled by `DATA_PIPELINE_DB` environment variable

2. **File Formats:**
   - `.parquet` files contain GeoDataFrames (with geometry)
   - `.csv` files contain DataFrames (no geometry, just data)


3. **Storage Location:**
   - If using Azure Blob Storage (`DATA_PIPELINE_DB=BLOB`), paths are relative to the blob container
   - If using Snowflake stage (`DATA_PIPELINE_DB=SNOWFLAKE`), paths are relative to the stage (e.g., `@stage_name/geodb/...`)
   - If using local storage (`DATA_PIPELINE_DB=LOCAL`), paths are relative to the project root or configured base directory