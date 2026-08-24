# File Structure and Output Directories

This document lists all files produced and downloaded by the Ahead of the Storm pipeline, organized by directory structure.

## Directory Structure Overview

The pipeline uses environment variables to configure base directories:
- `RESULTS_DIR` - Results and configuration files (default: `results`)
- `ROOT_DATA_DIR` - Base data directory (default: `geodb`)
- `VIEWS_DIR` - Subdirectory for views (default: `aos_views`)
- `STORMS_FILE` - Processed storms tracking file (default: `storms.json`)

---

## Files Produced During Initialize (`--type initialize`)

**Note:** The pipeline no longer requires a separate bounding box file. Country boundaries are checked directly with a 500km buffer during processing.

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
  - *(Optional, after `--type patch --columns vulnerability`)* `moderate_poverty_prob`, `severe_poverty_prob`: PCHIP-interpolated child poverty rates per tile from DHS/RWI disaggregation
- **Created by:** `create_mercator_country_layer()` via `save_mercator_and_admin_views()`
- **Note:** One file per country per zoom level. This parquet is the single source of truth for all base spatial data including poverty rates. Admin/facility per-threshold impact CSVs never carry poverty columns; the mercator tile view (item 12) is an exception, see its own note.

### 2. Base Admin Views (per country, per admin level)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_admin{N}.parquet`
- **Example:** `geodb/aos_views/admin_views/DOM_admin1.parquet`, `geodb/aos_views/admin_views/PNG_admin2.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Administrative level N boundaries with aggregated:
  - Population totals: `population`, `school_age_population`, `infant_population`, `adolescent_population`
  - Built surface total: `built_surface_m2`
  - Facility counts: `num_schools`, `num_hcs`, `num_shelters`, `num_wash`
  - Average wealth/settlement: `rwi`, `smod_class`, `smod_class_l1`
  - *(Optional, after `--type patch --columns vulnerability`)* `moderate_poverty_prob`, `severe_poverty_prob`: **population-weighted mean** poverty rate across tiles in the admin unit: `Σ(pop_tile × rate_tile) / Σ(pop_tile)`. Tiles without RWI coverage (NaN poverty rate) are excluded from both numerator and denominator. When all admin tiles lack RWI the column is NaN. Aggregating all admin units population-weighted back to national level exactly recovers the calibrated national target: calibration drops NaN-RWI tiles before quintile assignment and scales PCHIP outputs so `Σ(rate × pop)` matches the DHS-implied count for covered tiles. NaN areas are absent from both the calibration and the aggregation by design, not a source of additional error.
  - Administrative names and geometries
- **Created by:** `create_admin_country_layer()` via `save_mercator_and_admin_views()`
- **Note:** One file per country per admin level. Default is admin1; use `--admin 1 2` during initialize (or `--type patch --columns admin2`) to create admin2. Admin parquets are automatically re-synced whenever `--type patch` runs, so they pick up any newly patched columns including poverty rates.

### 3. School Locations (per country, cached)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views/{country}_schools.parquet`
- **Example:** `geodb/aos_views/school_views/DOM_schools.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** School locations fetched from GIGA School Location API
- **Created by:** `save_school_locations()`
- **Note:** Cached after first fetch to avoid repeated API calls. Replaced by `geodb/custom/{country}_schools.csv` if present.

### 4. Health Center Locations (per country, cached)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/{country}_health_centers.parquet`
- **Example:** `geodb/aos_views/hc_views/DOM_health_centers.parquet`
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
- **Note:** Cached after first fetch. Replaced by `geodb/custom/{country}_shelters.csv` if present. OSM coverage is sparse: providing a custom government shelter registry is recommended.

### 6. WASH Locations (per country, cached)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/wash_views/{country}_wash.parquet`
- **Example:** `geodb/aos_views/wash_views/DOM_wash.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** WASH infrastructure locations fetched from OSM Overpass (drinking water, toilets, water works, pumping stations, etc.)
- **Created by:** `save_wash_locations()`
- **Note:** Cached after first fetch. Replaced by `geodb/custom/{country}_wash.csv` if present.

---

## Files Produced During Patch (`--type patch --columns vulnerability`)

### 7. Vulnerability Source CSV (per country, per zoom level)
**Location:** `{ROOT_DATA_DIR}/vulnerability/{country}_vulnerability_z{zoom_level}.csv`
- **Example:** `geodb/vulnerability/PHL_vulnerability_z14.csv`
- **Format:** CSV (DataFrame)
- **Content:** Per-tile child poverty rates from DHS/RWI disaggregation:
  - `tile_id`: mercator quadkey at the specified zoom level
  - `moderate_poverty_prob`: moderate child poverty rate per tile (0–1), DHS threshold: ≥2 deprivations
  - `severe_poverty_prob`: severe child poverty rate per tile (0–1), DHS threshold: ≥3 deprivations
- **Created by:** `vulnerability/fetch_vulnerability_probs.py` (downloads from Azure Blob)
- **Note:** Pre-computed using Meta RWI + DHS quintile anchors + PCHIP interpolation. Tiles without RWI coverage have NaN poverty rates (~30% for PHL). This file is the source for patching the base parquet.

After fetching, patching the base parquet writes `moderate_poverty_prob` and `severe_poverty_prob` into `{country}_{zoom}.parquet`. This is the only location where raw poverty rates live at tile level. Admin/facility per-threshold impact CSVs never carry these; the mercator tile view (item 12) is an exception, see its own note.

---

## Files Produced During Update (`--type update`)

For each storm/forecast combination processed, the following files are created:

### 8. School Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/school_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** School locations with impact probability for each wind threshold
- **Created by:** `save_school_view()`
- **Note:** Multiple files per storm (one per wind threshold: 34, 40, 50, 64, 83, 96, 113, 137)

### 9. Health Center Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/hc_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Health center locations with impact probability for each wind threshold
- **Created by:** `save_hc_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 10. Shelter Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/shelter_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/shelter_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Shelter locations with impact probability for each wind threshold
- **Created by:** `save_shelter_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 11. WASH Impact Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/wash_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/wash_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** WASH facility locations with impact probability for each wind threshold
- **Created by:** `save_wash_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 12. Mercator Tile Impact Views (per country, per storm, per forecast, per wind threshold, per zoom)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/{country}_{storm}_{date}_{wind_threshold}_{zoom_level}.csv`
- **Example:** `geodb/aos_views/mercator_views/DOM_LORENZO_20251015120000_34_14.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** Expected impact values per tile at the given wind threshold:
  - `E_population`, `E_school_age_population`, `E_infant_population`, `E_adolescent_population`
  - `E_built_surface_m2`
  - `E_num_schools`, `E_num_hcs`, `E_num_shelters`, `E_num_wash`
  - `E_rwi`, `E_smod_class`, `E_smod_class_l1`
  - `probability`
- **Created by:** `save_tiles_view()`
- **Note:** Multiple files per storm (one per wind threshold). Wind-integrated people-in-need estimates live only in the vulnerability output file (item 15), never here. Raw poverty rates (`moderate_poverty_prob`, `severe_poverty_prob`) are appended as trailing columns ($17/$18) when the country has been patched with vulnerability data `create_mercator_view_from_envelopes()` deliberately keeps them last so the file's first 16 columns stay positionally identical to the pre-poverty format. Wind's own MAT-loading SQL ignores these trailing columns; gust's own MAT table does capture them (see item 33 / `MERCATOR_TILE_GUST_MAT`). Countries without vulnerability data simply don't have these columns in the file at all.

### 13. CCI (Child Cyclone Index) Tile Views (per country, per storm, per forecast, per zoom)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/{country}_{storm}_{date}_{zoom_level}_cci.csv`
- **Example:** `geodb/aos_views/mercator_views/DOM_LORENZO_20251015120000_14_cci.csv`
- **Format:** CSV (DataFrame)
- **Content:** Child Cyclone Index (CCI) values:
  - `CCI_children`, `E_CCI_children`
  - `CCI_school_age`, `E_CCI_school_age`
  - `CCI_infants`, `E_CCI_infants`
  - `CCI_adolescents`, `E_CCI_adolescents`
  - `CCI_pop`, `E_CCI_pop`
- **Created by:** `save_cci_tiles()`
- **Note:** One file per storm per forecast (aggregates all wind thresholds)

### 14. Admin Level Impact Views (per country, per storm, per forecast, per wind threshold, per admin level)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_{storm}_{date}_{wind_threshold}_admin{N}.csv`
- **Example:** `geodb/aos_views/admin_views/DOM_LORENZO_20251015120000_34_admin1.csv`, `geodb/aos_views/admin_views/PNG_FUNG-WONG_20251110120000_34_admin2.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** Expected impact values aggregated by admin level N:
  - `E_population`, `E_school_age_population`, `E_infant_population`, `E_adolescent_population`
  - `E_built_surface_m2`
  - `E_num_schools`, `E_num_hcs`, `E_num_shelters`, `E_num_wash`
  - `E_rwi`, `E_smod_class`, `E_smod_class_l1`
  - `probability`
  - `name` (admin name)
- **Created by:** `save_admin_tiles_view()`
- **Note:** Multiple files per storm per wind threshold per initialized admin level. Auto-detected from existing base admin parquets, no configuration needed at update time. **Poverty rate columns are not included**: people-in-need estimates aggregated by admin are in the vulnerability admin views (item 16).

### 15. CCI Admin Views (per country, per storm, per forecast, per admin level)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_{storm}_{date}_admin{N}_cci.csv`
- **Example:** `geodb/aos_views/admin_views/DOM_LORENZO_20251015120000_admin1_cci.csv`, `geodb/aos_views/admin_views/PNG_FUNG-WONG_20251110120000_admin2_cci.csv`
- **Format:** CSV (DataFrame)
- **Content:** Child Cyclone Index (CCI) values aggregated by admin level N
- **Created by:** `save_cci_admin()`
- **Note:** One file per storm per forecast per initialized admin level

### 16. Vulnerability Tile Views (per country, per storm, per forecast, per zoom)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views/{country}_{storm}_{date}_{zoom_level}_vulnerability.csv`
- **Example:** `geodb/aos_views/mercator_views/PHL_FUNG-WONG_20251109060000_14_vulnerability.csv`
- **Format:** CSV (DataFrame)
- **Content:** Wind-integrated people/children in need per tile (CHIN methodology):
  - `zone_id`: mercator quadkey (= tile_id)
  - `id`: admin1 unit ID (for admin aggregation)
  - `E_infant_in_need`: expected infants (0–4y) in need
  - `E_school_age_in_need`: expected school-age children (5–14y) in need
  - `E_adolescent_in_need`: expected adolescents (15–19y) in need
  - `E_children_in_need`: expected children (0–19y) in need
  - `E_people_in_need`: expected total people in need
- **Created by:** `save_vulnerability_tiles()` (called automatically during `--type update` for patched countries)
- **Note:** One file per storm per forecast. This is the sole output with E_people_in_need estimates: these are not included in the per-threshold CSVs (item 12). Tiles without RWI/poverty coverage produce NaN and are excluded. Countries not patched with vulnerability data produce an empty file (NaN columns, no error).
- **Methodology:** Vulnerability weight = Σ P(band k) × rate(k), where bands are mutually exclusive (P(band k) = P(≥k) − P(≥k+1)) and rates follow the CHIN formula: <50kt → severe rate; 50–96kt → `moderate × (1−t) + t, t = (kt−50)/46`; ≥96kt → 1.0.

### 17. Vulnerability Admin Views (per country, per storm, per forecast, per admin level)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views/{country}_{storm}_{date}_admin{N}_vulnerability.csv`
- **Example:** `geodb/aos_views/admin_views/PHL_FUNG-WONG_20251109060000_admin1_vulnerability.csv`
- **Format:** CSV (DataFrame)
- **Content:** Same columns as vulnerability tile views (item 16), aggregated (summed) by admin level N
- **Created by:** `save_vulnerability_admin()` (called automatically during `--type update`)
- **Note:** One file per storm per forecast per initialized admin level

### 18. Track Views (per country, per storm, per forecast, per wind threshold)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/track_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/track_views/DOM_LORENZO_20251015120000_34.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** Ensemble member tracks with severity metrics:
  - `severity_schools`
  - `severity_hcs`
  - `severity_num_shelters`
  - `severity_num_wash`
  - `severity_population`
  - `severity_school_age_population`
  - `severity_infant_population`
  - `severity_adolescent_population`
  - `severity_built_surface_m2`
- **Created by:** `save_tracks_view()`
- **Note:** Multiple files per storm (one per wind threshold)

### 19. Vulnerability Track Views (per country, per storm, per forecast, per zoom)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/track_views/{country}_{storm}_{date}_{zoom_level}_vulnerability_tracks.parquet`
- **Example:** `geodb/aos_views/track_views/PHL_FUNG-WONG_20251109060000_14_vulnerability_tracks.parquet`
- **Format:** Parquet (DataFrame)
- **Content:** Per-ensemble-member people/children in need totals:
  - `zone_id`: ensemble member number
  - `severity_people_in_need`
  - `severity_children_in_need`
  - `severity_infant_in_need`
  - `severity_school_age_in_need`
  - `severity_adolescent_in_need`
- **Created by:** `calculate_vulnerability_tracks()` → `save_vulnerability_tracks()`
- **Note:** One file per storm per forecast (not per wind threshold, vulnerability integrates across all thresholds per member). All `severity_*` columns are NaN for countries not patched with vulnerability data. Loaded into **`TRACK_VULNERABILITY_MAT`** in Snowflake by `REFRESH_MATERIALIZED_VIEWS()`. The dashboard joins this onto `TRACK_MAT` via `get_track_impacts()` in `snowflake_utils.py` to include in-need columns alongside wind-threshold severity metrics.
- **Methodology:** For each member, tiles intersecting that member's cumulative wind envelopes are identified via spatial join. Each tile is assigned the rate of its *highest* wind band reached by that member (exclusive assignment, same rate formula as item 16): `severe_poverty_prob` below 50kt, a linear blend from `moderate_poverty_prob` toward 1.0 between 50-96kt, and 1.0 (catastrophic, all people need assistance) at/above 96kt. The per-tile `population × rate` values are then summed. This is the per-member analogue of item 16: item 16 uses ensemble-probability weights to produce one expected value per tile (a spatial map of vulnerability concentration); this file uses binary member coverage to produce one scenario total per member instead (enabling DET/#51 and worst-case display in the dashboard).

### 19b. Tile-Member Bitmask Views (per country, per storm/cycle, per forecast, per hazard threshold/tier)
**Location (wind):** `{ROOT_DATA_DIR}/{VIEWS_DIR}/track_tile_bitmask_views/{country}_{storm}_{date}_{wind_threshold}.parquet`
- **Example:** `geodb/aos_views/track_tile_bitmask_views/PHL_BAVI_20260704000000_34.parquet`

**Location (gust):** `{ROOT_DATA_DIR}/{VIEWS_DIR}/track_tile_bitmask_views_gust/{country}_{storm}_{date}_g{gust_threshold}.parquet`
- **Example:** `geodb/aos_views/track_tile_bitmask_views_gust/PHL_BAVI_20260702000000_g17.parquet`

**Location (river flood):** `{ROOT_DATA_DIR}/{VIEWS_DIR}/track_tile_bitmask_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet`
- **Example:** `geodb/aos_views/track_tile_bitmask_views_river/PHL_20260714000000_rp100_120h.parquet`
- **Created by:** `calculate_river_tile_member_bitmask()` → `save_river_tile_bitmask_view()`

**Location (precip):** `{ROOT_DATA_DIR}/{VIEWS_DIR}/track_tile_bitmask_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet`
- **Example:** `geodb/aos_views/track_tile_bitmask_views_precip/PHL_20260814000000_p100_120h.parquet`
- **Created by:** `calculate_precip_tile_member_bitmask()` → `save_precip_tile_bitmask_view()`

- **Format:** Parquet (DataFrame)
- **Content:** Real per-z14-tile, per-ensemble-member 64-bit coverage bitmask: `tile_id`, `bits` (bit `m-1` set ⇔ member `m`'s envelope/exceedance covers that tile). One row per DISTINCT tile with ≥1 member's coverage; a tile with zero coverage from every member simply has no row (sparse, same convention as `TRACK_MAT`'s own severity columns).
- **Created by (wind):** `calculate_tile_member_bitmask()` → `save_tracks_tile_bitmask_view()`
- **Note:** Multiple files per storm/cycle (one per threshold/tier-window, same pattern as item 18). Loaded into **`TILE_WIND_BITMASK_MAT`**/**`TILE_GUST_BITMASK_MAT`**/**`TILE_RIVER_BITMASK_MAT`**/**`TILE_PRECIP_BITMASK_MAT`** in Snowflake by `REFRESH_MATERIALIZED_VIEWS()`. Persists the same envelope-vs-tile spatial join item 18's own `severity_*` columns are computed from (previously discarded immediately after collapsing to a country-wide scalar); this keeps the per-member, per-tile identity instead, enabling a real tile-level union across hazards in the dashboard's "Compare Worst Case By" feature (`pages/map_shell_concept.py`'s `_fetch_family_member_frames`, which unions this bitmask data across Wind/Gust/River/Rain into one real per-member total instead of approximating it from separate marginal totals).

### 20. JSON Impact Reports (per country, per storm, per forecast)
**Location:** `{RESULTS_DIR}/jsons/{country}_{storm}_{date}.json`
- **Example:** `results/jsons/DOM_LORENZO_20251015120000.json`
- **Format:** JSON
- **Content:** Comprehensive impact report data including:
  - Expected impacts by wind threshold: `expected_pop_{wind}`, `expected_children_{wind}`, `expected_school_{wind}`, `expected_infant_{wind}`, `expected_adolescent_{wind}`, `expected_schools_{wind}`, `expected_hcs_{wind}`, `expected_shelters_{wind}`, `expected_wash_{wind}`
  - Overall expected counts (across all thresholds): `expected_pop`, `expected_children`, `expected_school_age`, `expected_infants`, `expected_adolescent`, `expected_schools`, `expected_hcs`, `expected_shelters`, `expected_wash`
  - People/children in need (CHIN vulnerability, `None` if country not patched): `E_people_in_need`, `E_children_in_need`, `E_infant_in_need`, `E_school_age_in_need`, `E_adolescent_in_need`
  - CCI: `expected_cci_pop`, `expected_cci_school`, `expected_cci_infant`, `expected_cci_adolescent`
  - Urban/rural breakdown: `expected_pop_urban`, `expected_pop_rural`, `expected_school_urban`, `expected_school_rural`, `expected_infant_urban`, `expected_infant_rural`, `expected_adolescent_urban`, `expected_adolescent_rural`
  - RWI-based poverty breakdown: `expected_pop_poverty`, `expected_pop_severe`, `expected_school_poverty`, `expected_school_severe`, `expected_infant_poverty`, `expected_infant_severe`, `expected_adolescent_poverty`, `expected_adolescent_severe`
  - Top 5 schools, health centers, shelters, WASH facilities at risk
  - Administrative breakdowns: `rows_admins_pop_total`, `rows_admins_school`, `rows_admins_infant`, `rows_admins_adolescent`, `rows_schools_winds`, `rows_hcs_winds`, `rows_shelters_winds`, `rows_wash_winds`. Each admin row includes per-wind impact counts, a `"cci"` key (Child Cyclone Index for that admin), and a `"people_in_need"` key (`null` when country not patched with vulnerability data)
  - Change indicators vs previous forecast: `children_change`, `children_change_perc`, `children_change_direction`
  - Metadata: `storm`, `country`, `forecast_date`, `next_forecast_date`, `report_date`, `expected_landfall`, `storm_category`
- **Created by:** `do_report()` → `save_json_report()`
- **Note:** One file per country per storm per forecast. `E_*_in_need` fields are `None` (N/A) for countries not patched with vulnerability data; all other fields are unaffected.

### 21. Processed Storms Tracking File
**Location:** `{RESULTS_DIR}/{STORMS_FILE}`
- **Example:** `results/storms.json`
- **Format:** JSON
- **Content:** Dictionary tracking which storm/forecast combinations have been processed
- **Created by:** `save_json_storms()`
- **Note:** Updated after each successful storm processing

---

## Files Downloaded from External Sources

These files are downloaded automatically by the GigaSpatial library and stored in the data store. The exact location depends on the data store configuration (LOCAL vs BLOB).

> **Note on raster data storage:** The raw raster files (WorldPop, GHSL, SMOD, RWI) are downloaded via
> giga-spatial's own handlers, which write through the pipeline's configured data store (`geodb/bronze/`
> locally, or the Snowflake stage when `DATA_PIPELINE_DB=SNOWFLAKE`), and reused on subsequent runs.
> The aggregated per-tile values derived from these rasters are additionally permanently stored in the
> base mercator view parquet (`mercator_views/{country}_{zoom}.parquet`). This means the spatial
> distribution of all metrics below can be visualized directly from that parquet, each tile has a
> geometry and the corresponding aggregated value, without needing access to the original rasters.

### 22. WorldPop Population Data
- **Source:** WorldPop API (GR2, year=2025)
- **Downloaded by:** `MercatorViewGenerator` (giga-spatial internal)
- **Raw cache:** `geodb/bronze/` (subdirectory managed by giga-spatial), written to the active data store (local filesystem or Snowflake stage). On first init for a country all 62 age-band files (~175 MB) are downloaded and cached; subsequent runs reuse the cache.
- **Stored in mercator parquet as:** `population` (100m res, sum per tile), `school_age_population`, `infant_population`, `adolescent_population` (all 100m res, sum per tile)

### 23. GHSL Built Surface Data
- **Source:** Global Human Settlement Layer (GHSL), year=2020, 100m resolution
- **Downloaded by:** `MercatorViewGenerator` (giga-spatial internal)
- **Raw cache:** `geodb/bronze/` (subdirectory managed by giga-spatial), written to the active data store on first use, reused on subsequent runs
- **Stored in mercator parquet as:** `built_surface_m2` (sum per tile)

### 24. SMOD Settlement Classification Data
- **Source:** GHSL Settlement Model (SMOD), year=2020, 1km resolution
- **Downloaded by:** `MercatorViewGenerator` (giga-spatial internal)
- **Raw cache:** `geodb/bronze/` (subdirectory managed by giga-spatial), written to the active data store on first use, reused on subsequent runs
- **Stored in mercator parquet as:** `smod_class` (raw L2 median per tile, values 10–30) and `smod_class_l1` (derived 3-class: 1=rural, 2=suburban, 3=urban)

### 25. Relative Wealth Index (RWI) Data
- **Source:** Facebook/Meta RWI dataset via HDX
- **Downloaded by:** `RWIHandler` (giga-spatial internal)
- **Raw cache:** `geodb/bronze/` (subdirectory managed by giga-spatial), written to the active data store on first use, reused on subsequent runs
- **Stored in mercator parquet as:** `rwi` (mean per tile)
- **Note:** Not available for all countries. Tiles will have NaN for `rwi` where data is unavailable; no error raised.

### 26. School Locations
**Source:** GIGA School Location API
- **Fetched by:** `GigaSchoolLocationFetcher.fetch_locations()`
- **Cached to:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views/{country}_schools.parquet`
- **Note:** Cached after first fetch to avoid repeated API calls
- **Requires:** `GIGA_SCHOOL_LOCATION_API_KEY` environment variable

### 27. Health Center Locations
**Source:** HealthSites API
- **Fetched by:** `HealthSitesFetcher.fetch_facilities()`
- **Cached to:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views/{country}_health_centers.parquet`
- **Note:** Cached after first fetch to avoid repeated API calls
- **Requires:** `HEALTHSITES_API_KEY` environment variable

### 28. Administrative Boundaries
**Source:** UNICEF GeoRepo (via GigaSpatial)
- **Fetched by:** `AdminBoundaries.create()`
- **Note:** Fetched via API, not cached to disk (fetched each time)
- **Optional:** `GEOREPO_API_KEY` and `GEOREPO_USER_EMAIL` environment variables

---

## Gust Impact Views (per country, per storm, per forecast, per gust threshold)

Mirrors items 8, 9, 10, 11, 12, 14, and 18 above (core exposure views: school, HC, shelter,
WASH, mercator tile, admin, tracks), using wind gust envelope polygons
(`TC_GUST_ENVELOPES_COMBINED` in Snowflake, `GUST_THRESHOLD` column, 17/21/26/33/43/49/58/70 m/s)
instead of sustained-wind envelopes. Produced by the same underlying `create_*_view_from_envelopes()`
and `save_*_view()` functions as the wind views, called with `threshold_column='gust_threshold'`
and `dataset='gust'` respectively, identical format and columns to their wind counterparts, only
the source polygons and file location differ. Written into separate `*_views_gust` directories with
a `g`-prefixed threshold token in the filename (e.g. `g43` instead of `43`) so gust files can never
collide with, or be misclassified as, wind files by anything (this repo's own stale-file cleanup,
`geosight/admin_related_table.py`'s admin-file regex, or ORCHESTRATION's stage-path-based MAT table
loader) that scans the wind directories by path pattern.

Gust processing runs automatically alongside wind during `--type update` whenever gust envelope data
exists in Snowflake for that storm/forecast (`--skip-gust` disables it). If no gust data is available
for a given storm/forecast (e.g. upstream extraction found no gust polygon, or the storm was too weak
to register at any gust threshold), gust views are silently skipped for that run, wind views are
produced normally either way, gust availability is fully independent of wind.

### 29. Gust School Impact Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/school_views_gust/{country}_{storm}_{date}_g{gust_threshold}.parquet`
- **Example:** `geodb/aos_views/school_views_gust/PHL_BAVI_20260705000000_g43.parquet`
- **Format:** Parquet (GeoDataFrame), same columns as item 8
- **Created by:** `save_school_view(..., dataset='gust')`
- **Note:** Multiple files per storm (one per gust threshold: 17, 21, 26, 33, 43, 49, 58, 70)

### 30. Gust Health Center Impact Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/hc_views_gust/{country}_{storm}_{date}_g{gust_threshold}.parquet`
- **Example:** `geodb/aos_views/hc_views_gust/PHL_BAVI_20260705000000_g43.parquet`
- **Format:** Parquet (GeoDataFrame), same columns as item 9
- **Created by:** `save_hc_view(..., dataset='gust')`

### 31. Gust Shelter Impact Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/shelter_views_gust/{country}_{storm}_{date}_g{gust_threshold}.parquet`
- **Example:** `geodb/aos_views/shelter_views_gust/PHL_BAVI_20260705000000_g43.parquet`
- **Format:** Parquet (GeoDataFrame), same columns as item 10
- **Created by:** `save_shelter_view(..., dataset='gust')`

### 32. Gust WASH Impact Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/wash_views_gust/{country}_{storm}_{date}_g{gust_threshold}.parquet`
- **Example:** `geodb/aos_views/wash_views_gust/PHL_BAVI_20260705000000_g43.parquet`
- **Format:** Parquet (GeoDataFrame), same columns as item 11
- **Created by:** `save_wash_view(..., dataset='gust')`

### 33. Gust Mercator Tile Impact Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views_gust/{country}_{storm}_{date}_g{gust_threshold}_{zoom_level}.csv`
- **Example:** `geodb/aos_views/mercator_views_gust/PHL_BAVI_20260705000000_g43_14.csv`
- **Format:** CSV (DataFrame, no geometry), same `E_*`/`probability` columns as item 12
- **Created by:** `save_tiles_view(..., dataset='gust')`

### 34. Gust Admin Level Impact Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views_gust/{country}_{storm}_{date}_g{gust_threshold}_admin{N}.csv`
- **Example:** `geodb/aos_views/admin_views_gust/PHL_BAVI_20260705000000_g43_admin1.csv`
- **Format:** CSV (DataFrame, no geometry), same columns as item 14
- **Created by:** `save_admin_tiles_view(..., dataset='gust')`

### 35. Gust Track Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/track_views_gust/{country}_{storm}_{date}_g{gust_threshold}.parquet`
- **Example:** `geodb/aos_views/track_views_gust/PHL_BAVI_20260705000000_g43.parquet`
- **Format:** Parquet (GeoDataFrame), same `severity_*` columns as item 18
- **Created by:** `save_tracks_view(..., dataset='gust')`

---

## Precipitation/Runoff Impact Views (per country, per forecast cycle, per window, per threshold)

Storm-independent: run once per `--type update` invocation (`run_precip_analysis()` in
`main_pipeline.py`), not once per storm, and not filtered by `--storm`. Uses the *latest*
`MET_FORECASTS` tp/ro Zarr forecast by default, or the `MET_FORECASTS` row for the exact calendar
date if `--date` is passed (mirrors wind/gust's own exact-date-match backfill behavior). Source
data (`tp` = total precipitation, `ro` = runoff, both ECMWF ENS ~0.25° grids) lives only on
Snowflake's internal stage via `MET_FORECASTS.STAGE_PATH`, independent of `DATA_PIPELINE_DB`.
Disable with `--skip-precip`.

Two families of tiers, computed for 4 accumulation windows (`PRECIP_WINDOWS_H` = 6, 24, 72,
120 hours):
- **tp exceedance tiers**: probability (fraction of the 51-member ensemble) that accumulated
  rainfall exceeds a moderate/heavy/extreme mm threshold for that window
  (`PRECIP_TP_THRESHOLDS_MM`, e.g. 25/50/75mm at 6h, 50/100/150mm at 120h)
- **ro/tp ratio tiers**: probability that the runoff/precipitation ratio exceeds a dimensionless
  cut point (`RATIO_THRESHOLDS` = 0.3, 0.6; a Rational Method runoff-coefficient reference,
  0.3 = "meaningfully elevated" runoff response, 0.6 = "majority of the rain becomes runoff"),
  a flash-flood-response proxy since no real flood-forecasting system uses one fixed mm cut
  point. Members with less than `RATIO_MIN_TP_MM` (5mm) accumulated tp are treated as ratio=0, not excluded.

Tile-level and admin-level probability is sampled directly from the raster (centroid-based point
sampling via `TifProcessor.sample_by_coordinates()`, not the polygon-intersection method wind/gust
use, since a coarse continuous grid has no polygon to intersect). **Facility-level probability is
routed through each facility's containing mercator tile** (`assign_facilities_to_tiles()` +
`create_precip_facility_view()`), not sampled independently: the ~0.25° native grid is far
coarser than a zoom-14 tile, so a facility's own probability is guaranteed to
exactly equal its containing tile's probability by construction, rather than by coincidence of
where its exact coordinate happens to land. This is a deliberate difference from wind/gust
(whose own facility views have no `tile_id` concept at all, computed independently via buffered-
polygon-vs-envelope intersection, correct for them since envelope polygons carry real
fine-grained shape, unlike precip's coarse grid).

`.parquet` (not `.csv`) for facility views specifically, since these carry the facility's real
geometry (a true point, or a polygon for the minority of health-center OSM records that are
building footprints). No CCI, vulnerability, or JSON report for precip, same as gust.

### 36. Precip Tile Impact Views (tp)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.csv`
- **Example:** `geodb/aos_views/mercator_views_precip/PHL_20260705000000_p50_24h.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** `zone_id` (tile_id), `probability`, `native_cell_row`/`native_cell_col` which
  native ~0.25° precip grid cell this tile's centroid falls in: makes the tile-to-native-cell
  relationship explicit/queryable, all base mercator parquet columns except `population` (which
  is dropped, e.g. `num_schools`, etc.), `E_population` (`probability × population`)
- **Created by:** `create_precip_tile_view()` -> `save_precip_tile_view()`
- **Note:** One file per window per tp threshold (4 windows × 3 tiers = 12 files per cycle)

### 37. Precip Tile Impact Views (ro/tp ratio)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.csv`
- **Example:** `geodb/aos_views/mercator_views_precipratio/PHL_20260705000000_g30_24h.csv` (ratio 0.3)
- **Format:** CSV (DataFrame, no geometry), same columns as item 36
- **Created by:** `create_precip_tile_view()` -> `save_precip_ratio_view()`
- **Note:** One file per window per ratio tier (4 windows × 2 tiers = 8 files per cycle)

### 38. Precip Admin Impact Views (tp and ratio)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h_admin{N}.csv`,
`{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h_admin{N}.csv`
- **Example:** `geodb/aos_views/admin_views_precip/PHL_20260705000000_p50_24h_admin1.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** `tile_id` (renamed `id` on read), `E_population` (summed across tiles in the admin
  unit), `probability` (mean across tiles), `name` (admin name)
- **Created by:** `create_precip_admin_view()` -> `save_precip_admin_view()` / `save_precip_ratio_admin_view()`
- **Note:** Auto-detected from existing initialized admin levels, same as item 14

### 39. Precip School/HC/Shelter/WASH Impact Views (tp and ratio)
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/{school,hc,shelter,wash}_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet`,
`{ROOT_DATA_DIR}/{VIEWS_DIR}/{school,hc,shelter,wash}_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.parquet`
- **Example:** `geodb/aos_views/school_views_precip/PHL_20260705000000_p50_24h.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** every original attribute column from the facility's own cached location file (item
  3–6) plus `probability` (from the facility's containing tile, see above) and the facility's own
  true geometry
- **Created by:** `create_precip_facility_view()` → `save_precip_school_view()` / `save_precip_hc_view()`
  / `save_precip_shelter_view()` / `save_precip_wash_view()` (and `_ratio_` equivalents)
- **Note:** Skipped entirely (no file written) for a country/facility-type combination with zero
  cached locations (WASH/shelters are frequently sparse in OSM), rather than writing a zero-row
  file. 8 directories total (4 facility types × tp/ratio).

---

## River Flood (GloFAS x JRC) Impact Views (per country, per RP tier, per lead-time step)

Storm-independent, same calling convention as precip: run once per `--type update` invocation
(`run_river_flood_analysis()` in `main_pipeline.py`), not once per storm, and not filtered by
`--storm`. Uses the *latest* `RIVER_FORECASTS` row per RP tier by default, or the exact calendar
date if `--date` is passed. Source data (per-member flooded-pixel Parquet files, produced daily by
the separate TC-ECMWF-Forecast-Pipeline repo from GloFAS river-discharge ensembles matched against
JRC's global flood-extent maps) lives only on Snowflake's internal stage via
`RIVER_FORECASTS.STAGE_PATH`, independent of `DATA_PIPELINE_DB`. Disable with `--skip-river-flood`.

Six return-period (RP) tiers (`RIVER_RP_TIERS` = rp2, rp5, rp10, rp20, rp50, rp100, how rare a
river discharge level is, not a probability), each computed for 4 of the 7 lead-time steps the
upstream RIVER_FORECASTS Parquet actually has (`RIVER_LEADTIME_STEPS_H` = 24, 72, 120, 168 hours;
48/96/144h are dropped to keep materialized-view compute/storage proportional to the windows the
dashboard UI exposes). Unlike precip's coarse ~0.25°
continuous raster, GloFAS/JRC flood pixels are ~150m resolution, comparable to a zoom-14
mercator tile or a buffered facility footprint, not much coarser. Because of that, this hazard
type reuses **wind/gust's** modeling shape, not precip's:
- Tile/admin probability = `count(distinct flooded members in the zone) / FULL_ENSEMBLE_SIZE`
  (51-member ensemble, same convention as wind/gust), via point-in-polygon `gpd.sjoin` +
  `.nunique('member')` (counting distinct members, not rows, since one member can contribute
  multiple flooded pixels to the same zone at this resolution).
- **Facility-level probability is independently computed via direct buffer-and-intersect**
  (`create_river_facility_view()`), the *wind/gust* shape, explicitly **not** precip's
  tile-routed shape, because the resolution reasoning that justifies tile-routing for precip
  (grid far coarser than a facility) does not hold here.
- `below_min_basin` (bool): a JRC-upstream QC flag surfaced per-zone as `.any()`, **never** used
  to exclude a pixel/tile/facility, only an informational caveat (per upstream's own explicit
  design principle: "gate on RP tier for relevance, attach as a secondary attribute, not a
  pre-filter").
- `is_standin` (bool): sourced from the real `RIVER_FORECASTS.IS_STANDIN` column for that tier/date
  (not a hardcoded list),  `True` means the upstream pipeline substituted a lower-fidelity
  placeholder cycle; an upper-bound approximation, not an exact forecast, kept visible rather than
  silently absorbed.

No CCI, vulnerability, or JSON report for river flood, same as gust/precip.

### 40. River Tile Impact Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/mercator_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.csv`
- **Example:** `geodb/aos_views/mercator_views_river/PHL_20260714000000_rp10_120h.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** `zone_id` (tile_id), `probability`, `below_min_basin`, `is_standin`, and the full
  `E_*` breakdown (`E_population`, `E_school_age_population`, `E_infant_population`,
  `E_adolescent_population`, `E_built_surface_m2`, `E_smod_class`, `E_smod_class_l1`, `E_rwi`,
  `E_num_schools`, `E_num_hcs`, `E_num_shelters`, `E_num_wash`), the exact same `data_cols`
  loop item 12 (wind) and item 33 (gust) use, so this file's column set matches those exactly,
  minus the CCI/vulnerability columns (out of scope for river flood, same as gust/precip)
- **Created by:** `create_river_tile_view()` → `save_river_tile_view()`
- **Note:** One file per (RP tier × lead-time step) with at least one flooded pixel in the
  country's bounding box (a common no-file outcome for dry tiers/steps, not an error)

### 41. River Admin Impact Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/admin_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h_admin{N}.csv`
- **Example:** `geodb/aos_views/admin_views_river/PHL_20260714000000_rp10_120h_admin1.csv`
- **Format:** CSV (DataFrame, no geometry)
- **Content:** `tile_id` (admin zone id), the same full `E_*` breakdown as item 40 (summed across
  tiles in the admin unit, NaN-preserving via `_optional_sum` for optional columns), `probability`
  (mean across tiles), `below_min_basin` (OR'd across tiles), `is_standin`, `name` (admin name);
  matches item 14's (wind) column set exactly, minus CCI/vulnerability
- **Created by:** `create_river_admin_view()` → `save_river_admin_view()`
- **Note:** Auto-detected from existing initialized admin levels, same as item 14

### 42. River School/HC/Shelter/WASH Impact Views
**Location:** `{ROOT_DATA_DIR}/{VIEWS_DIR}/{school,hc,shelter,wash}_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet`
- **Example:** `geodb/aos_views/school_views_river/PHL_20260714000000_rp10_120h.parquet`
- **Format:** Parquet (GeoDataFrame)
- **Content:** every original attribute column from the facility's own cached location file (item
  3–6) plus `probability` (independently computed via buffer-and-intersect, not routed through a
  tile), `below_min_basin`, `is_standin`, and the facility's own true geometry
- **Created by:** `create_river_facility_view()` → `save_river_school_view()` /
  `save_river_hc_view()` / `save_river_shelter_view()` / `save_river_wash_view()`
- **Note:** Skipped entirely (no file written) for a country/facility-type combination with zero
  cached locations, same as precip. 4 directories total (one per facility type, no tp/ratio
  split, since river flood has only one hazard variable).

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
├── vulnerability/                      # Poverty rate source data (downloaded from Azure Blob)
│   └── {country}_vulnerability_z{zoom}.csv   # moderate/severe poverty prob per tile
└── {VIEWS_DIR}/                        # e.g., aos_views/
    ├── mercator_views/
    │   ├── {country}_{zoom}.parquet                         # Base mercator views (incl. poverty rates after patch)
    │   ├── {country}_{storm}_{date}_{wind}_{zoom}.csv       # Per-threshold impact tile views (poverty rates trailing, if patched)
    │   ├── {country}_{storm}_{date}_{zoom}_cci.csv          # CCI tile views
    │   └── {country}_{storm}_{date}_{zoom}_vulnerability.csv # People/children in need (sole E_*_in_need output)
    ├── admin_views/
    │   ├── {country}_admin{N}.parquet                          # Base admin views (one per initialized level)
    │   ├── {country}_{storm}_{date}_{wind}_admin{N}.csv        # Per-threshold impact admin views (no poverty rates)
    │   ├── {country}_{storm}_{date}_admin{N}_cci.csv           # CCI admin views (per level)
    │   └── {country}_{storm}_{date}_admin{N}_vulnerability.csv # People/children in need by admin (per level)
    ├── school_views/
    │   ├── {country}_schools.parquet                    # Cached school locations
    │   └── {country}_{storm}_{date}_{wind}.parquet      # School impact views
    ├── hc_views/
    │   ├── {country}_health_centers.parquet             # Cached health center locations
    │   └── {country}_{storm}_{date}_{wind}.parquet      # Health center impact views
    ├── shelter_views/
    │   ├── {country}_shelters.parquet                   # Cached shelter locations
    │   └── {country}_{storm}_{date}_{wind}.parquet      # Shelter impact views
    ├── wash_views/
    │   ├── {country}_wash.parquet                       # Cached WASH locations
    │   └── {country}_{storm}_{date}_{wind}.parquet      # WASH impact views
    ├── track_views/
    │   ├── {country}_{storm}_{date}_{wind}.parquet                        # Track impact views (per wind threshold)
    │   └── {country}_{storm}_{date}_{zoom}_vulnerability_tracks.parquet   # Per-member vulnerability totals
    ├── track_tile_bitmask_views/
    │   └── {country}_{storm}_{date}_{wind}.parquet             # Per-tile, per-member wind coverage bitmask
    ├── track_tile_bitmask_views_gust/
    │   └── {country}_{storm}_{date}_g{gust}.parquet            # Per-tile, per-member gust coverage bitmask
    ├── track_tile_bitmask_views_river/
    │   └── {country}_{forecast_time}_{rp_tier}_{step_h}h.parquet   # Per-tile, per-member river coverage bitmask
    ├── track_tile_bitmask_views_precip/
    │   └── {country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet   # Per-tile, per-member precip coverage bitmask
    ├── mercator_views_gust/
    │   └── {country}_{storm}_{date}_g{gust}_{zoom}.csv        # Gust tile impact views
    ├── admin_views_gust/
    │   └── {country}_{storm}_{date}_g{gust}_admin{N}.csv      # Gust admin impact views
    ├── school_views_gust/
    │   └── {country}_{storm}_{date}_g{gust}.parquet           # Gust school impact views
    ├── hc_views_gust/
    │   └── {country}_{storm}_{date}_g{gust}.parquet           # Gust health center impact views
    ├── shelter_views_gust/
    │   └── {country}_{storm}_{date}_g{gust}.parquet           # Gust shelter impact views
    ├── wash_views_gust/
    │   └── {country}_{storm}_{date}_g{gust}.parquet           # Gust WASH impact views
    ├── track_views_gust/
    │   └── {country}_{storm}_{date}_g{gust}.parquet           # Gust track impact views
    ├── mercator_views_precip/
    │   └── {country}_{forecast_time}_p{mm}_{window}h.csv          # Precip tile views (tp exceedance)
    ├── mercator_views_precipratio/
    │   └── {country}_{forecast_time}_g{ratio*100}_{window}h.csv   # Precip tile views (ro/tp ratio)
    ├── admin_views_precip/
    │   └── {country}_{forecast_time}_p{mm}_{window}h_admin{N}.csv
    ├── admin_views_precipratio/
    │   └── {country}_{forecast_time}_g{ratio*100}_{window}h_admin{N}.csv
    ├── school_views_precip/ ... wash_views_precip/
    │   └── {country}_{forecast_time}_p{mm}_{window}h.parquet      # Precip facility views (tp), 4 dirs
    ├── school_views_precipratio/ ... wash_views_precipratio/
    │   └── {country}_{forecast_time}_g{ratio*100}_{window}h.parquet  # Precip facility views (ratio), 4 dirs
    ├── mercator_views_river/
    │   └── {country}_{forecast_time}_{rp_tier}_{step_h}h.csv          # River-flood tile views
    ├── admin_views_river/
    │   └── {country}_{forecast_time}_{rp_tier}_{step_h}h_admin{N}.csv # River-flood admin views
    └── school_views_river/ ... wash_views_river/
        └── {country}_{forecast_time}_{rp_tier}_{step_h}h.parquet      # River-flood facility views, 4 dirs
```

---

## File Naming Conventions

### Date Format
- All dates in filenames use format: `YYYYMMDDHHMMSS`
- Example: `20251015120000` = October 15, 2025, 12:00:00 UTC

### Wind Threshold Values
- Common thresholds: `34`, `40`, `50`, `64`, `83`, `96`, `113`, `137`
- Represent wind speeds in knots

### Gust Threshold Values
- Common thresholds: `17`, `21`, `26`, `33`, `43`, `49`, `58`, `70`
- Represent gust speeds in m/s
- Always written with a `g` prefix in filenames (e.g. `g43`) to distinguish from wind thresholds
  of the same numeric value and to keep gust files out of any path-pattern-based classification
  that assumes wind semantics

### Precip Threshold/Window Values
- Forecast timestamp uses `forecast_time` (the MET_FORECASTS cycle), not a storm name/date:
  precip is storm-independent
- tp exceedance thresholds: `p{mm}` (e.g. `p50`), values vary per window, see `PRECIP_TP_THRESHOLDS_MM`
  in `main_pipeline.py`
- ro/tp ratio tiers: `g{ratio*100}` (e.g. `g30` for ratio 0.3, `g60` for ratio 0.6); note this
  reuses the same `g` prefix convention as gust but in a completely different directory tree
  (`*_precipratio/` vs `*_gust/`), never ambiguous by path
- Accumulation window: `{window}h` (6, 24, 72, or 120 hours)

### River Flood (GloFAS x JRC) Threshold/Window Values
- Forecast timestamp uses `forecast_time` (the `RIVER_FORECASTS` cycle for that RP tier), not a
  storm name/date: river flood is storm-independent, same as precip
- RP tier: `{rp_tier}` (`rp2`, `rp5`, `rp10`, `rp20`, `rp50`, `rp100`, how rare a river discharge
  level is, e.g. `rp100` = a 1-in-100-year discharge level for that river reach); no numeric-value
  collision risk with wind/gust thresholds, so no letter-prefix disambiguation is needed
- Lead-time step: `{step_h}h` (24, 72, 120, or 168 hours -- 4 of the 7 raw hours the upstream data has,
  see `RIVER_LEADTIME_STEPS_H` above)

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
