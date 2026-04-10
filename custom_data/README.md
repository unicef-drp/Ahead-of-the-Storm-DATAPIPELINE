# Custom Data Integration

This directory contains schema templates for providing custom data to the Ahead of the Storm
pipeline. Custom data supplements or replaces the default data sources (GIGA API, HealthSites,
WorldPop, GHSL, RWI) on a per-country basis.

## How it works

At runtime, the pipeline checks for custom files in `geodb/custom/` before fetching from external
APIs or running raster processing. If a custom file is found it is used instead, regardless of
`--rewrite` flag. **Custom files are never overwritten by the pipeline.**

To revert to API/raster data, delete or rename the custom file from the data store.

## Storage paths

All backends use the same relative path convention:

| Backend | Base path |
|---------|-----------|
| `LOCAL` | `geodb/custom/` (relative to working directory) |
| `BLOB` (ADLS) | `geodb/custom/` (relative to container root) |
| `SNOWFLAKE` | `geodb/custom/` (on `AOTS_ANALYSIS` stage, matching existing `geodb/` directory) |


## Custom file types

### Point data (facility locations)

| File | Replaces | Zoom-specific | Extra columns |
|------|---------|--------------|---------------|
| `<COUNTRY>_schools.csv` | GIGA school location API | No | **Preserved** in parquet cache |
| `<COUNTRY>_health_centers.csv` | HealthSites.io API (`amenity` column) | No | **Preserved** in parquet cache |
| `<COUNTRY>_shelters.csv` | OSM Overpass (`social_facility=shelter`) | No | **Preserved** in parquet cache |
| `<COUNTRY>_wash.csv` | OSM Overpass (WASH infrastructure types) | No | **Preserved** in parquet cache |

**Point data extra columns:** The entire CSV is written to the facility parquet cache (e.g. `school_views/PNG_schools.parquet`). Any columns beyond the required ones — such as `school_name`, `school_data_source`, `shelter_type`, `capacity` — are preserved and available for downstream use (e.g. display in the visualization app). They are not used by the pipeline's impact calculations.

### Tile-level data (pre-aggregated to mercator tiles)

| File | Replaces | Zoom-specific | Extra columns |
|------|---------|--------------|---------------|
| `<COUNTRY>_population_z<ZOOM>.csv` | WorldPop raster processing | Yes | **Ignored** |
| `<COUNTRY>_built_surface_z<ZOOM>.csv` | GHSL built surface raster | Yes | **Ignored** |
| `<COUNTRY>_smod_z<ZOOM>.csv` | GHSL SMOD raster | Yes | **Ignored** |
| `<COUNTRY>_rwi_z<ZOOM>.csv` | HDX Relative Wealth Index | Yes | **Ignored** |

**Tile-level extra columns:** Only the required value columns (e.g. `population`, `built_surface_m2`) are read from these files and merged into the base mercator parquet. Any additional columns are silently ignored and not stored anywhere.

Tile-level files are zoom-specific because different zoom levels produce different tile sets
(different quadkey IDs and tile geometries). The `<ZOOM>` suffix must match the zoom level
configured for the country in `PIPELINE_COUNTRIES` (typically `14`).

Example filenames for Papua New Guinea at zoom 14:
```
PNG_schools.csv
PNG_health_centers.csv
PNG_population_z14.csv
PNG_built_surface_z14.csv
PNG_smod_z14.csv
PNG_rwi_z14.csv
```

---

## Schema reference

### `<COUNTRY>_schools.csv`

Uses GIGA's education level classification. All education levels are kept in the cache;
filtering by level can be applied downstream.

| Column | Required | Type | Values / Notes |
|--------|----------|------|---------------|
| `id` | No | string | Unique identifier per school. Also accepted: `school_id_giga`. If omitted, sequential IDs are auto-generated. |
| `latitude` | Yes | float | WGS84 decimal degrees |
| `longitude` | Yes | float | WGS84 decimal degrees |
| `education_level` | No | string | `Pre-Primary`, `Primary`, `Secondary`, `Post-Secondary`, `Unknown` |
| `school_name` | No | string | School name |
| `school_data_source` | No | string | e.g. `government`, `ministry_of_education` |

> **Extra columns:** Any additional columns (e.g. `school_name`, `school_data_source`, `education_level`) are preserved in the parquet cache (`school_views/<COUNTRY>_schools.parquet`) and available for downstream use. They are not used in impact calculations. See `schema_schools.md` for canonical column names.

See `template_schools.csv` for a header-only template and `example_schools.csv` for sample rows.

---

### `<COUNTRY>_health_centers.csv`

The full dataset (all facility types) is stored in the cache. Filtering to relevant
facility types (`HC_FACILITY_TYPES`) happens at analysis time when generating impact views —
the same filter applies to both API-sourced and custom data.

Filtering uses the **`amenity`** column, matching the HealthSites.io API documented values.
Included by default: `hospital`, `clinic`, `doctors` (see `HC_FACILITY_TYPES` in `impact_analysis.py`).

| Column | Required | Type | Values / Notes |
|--------|----------|------|---------------|
| `id` | No | string | Unique identifier per facility. Also accepted: `osm_id`. If omitted, sequential IDs are auto-generated. |
| `latitude` | Yes | float | WGS84 decimal degrees |
| `longitude` | Yes | float | WGS84 decimal degrees |
| `amenity` | Yes | string | HealthSites.io/OSM amenity tag. Documented values: `hospital`, `clinic`, `doctors`, `dentist`, `pharmacy`. Only `hospital`, `clinic`, `doctors` are included in impact calculations by default. |
| `name` | No | string | Facility name |

> **Extra columns:** Any additional columns are preserved in the parquet cache (`hc_views/<COUNTRY>_health_centers.parquet`) and available for downstream use. They are not used in impact calculations. See `schema_health_centers.md` for canonical column names.

See `template_health_centers.csv` and `example_health_centers.csv`.

---

### `<COUNTRY>_shelters.csv`

Custom emergency shelter locations. Replaces the OSM Overpass query for `social_facility=shelter`.
OSM coverage for this tag is sparse in most countries — providing a government shelter registry
as a custom file is the recommended approach.

All facilities in this file enter impact calculations (no type filtering is applied).

| Column | Required | Type | Values / Notes |
|--------|----------|------|---------------|
| `id` | No | string | Unique identifier per shelter. Also accepted: `osm_id`. If omitted, sequential IDs are auto-generated. |
| `latitude` | Yes | float | WGS84 decimal degrees |
| `longitude` | Yes | float | WGS84 decimal degrees |
| `name` | No | string | Shelter name |
| `shelter_type` | No | string | e.g. `evacuation_centre`, `refugee_shelter`, `community_hall` |
| `capacity` | No | int | Maximum occupancy if known |

> **Extra columns:** Any additional columns are preserved in the parquet cache (`shelter_views/<COUNTRY>_shelters.parquet`) and available for downstream use. They are not used in impact calculations. See `schema_shelters.md` for canonical column names.

See `template_shelters.csv` and `example_shelters.csv`.

---

### `<COUNTRY>_wash.csv`

Custom WASH (Water, Sanitation and Hygiene) infrastructure locations. Replaces the OSM
Overpass query for humanitarian WASH types.

Reference: https://wiki.openstreetmap.org/wiki/Humanitarian_OSM_Tags/WASH

All facilities in this file enter impact calculations. The `wash_type` column is required
so facilities can be identified by type in reports (top-5 at-risk WASH facilities show
the type). Accepted values match those fetched from OSM via `WASH_LOCATION_TYPES`.

| Column | Required | Type | Values / Notes |
|--------|----------|------|---------------|
| `id` | No | string | Unique identifier per facility. Also accepted: `osm_id`. If omitted, sequential IDs are auto-generated. |
| `latitude` | Yes | float | WGS84 decimal degrees |
| `longitude` | Yes | float | WGS84 decimal degrees |
| `wash_type` | Yes | string | Facility type. Values: `drinking_water`, `water_point`, `toilets`, `shower`, `water_well`, `water_tap`, `water_works`, `pumping_station`, `wastewater_treatment_plant` |
| `name` | No | string | Facility name |

> **Extra columns:** Any additional columns are preserved in the parquet cache (`wash_views/<COUNTRY>_wash.parquet`) and available for downstream use. They are not used in impact calculations. See `schema_wash.md` for canonical column names.

See `template_wash.csv` and `example_wash.csv`.

---

### `<COUNTRY>_population_z<ZOOM>.csv`

Pre-aggregated population values per mercator tile. Tile IDs must match the quadkeys generated
at the specified zoom level for the country boundary.

| Column | Required | Type | Notes |
|--------|----------|------|-------|
| `tile_id` | Yes | string | Mercator quadkey at zoom level `<ZOOM>` (e.g. `31100123111112`) |
| `population` | Yes | float | Total population (sum within tile) |
| `school_age_population` | Yes | float | School-age population (sum within tile) |
| `infant_population` | Yes | float | Infant population 0–4 years (sum within tile) |
| `adolescent_population` | Yes | float | Adolescent population 15–19y (sum within tile) |

All four population columns are required together — they are hard requirements for the pipeline.
Use `NaN` for tiles with no data (e.g. ocean tiles).

> **Extra columns:** Ignored. Only the four required columns are read and merged into the mercator parquet.

See `template_population_z14.csv` and `example_population_z14.csv`.

---

### `<COUNTRY>_built_surface_z<ZOOM>.csv`

Pre-aggregated GHSL built surface per tile.

| Column | Required | Type | Notes |
|--------|----------|------|-------|
| `tile_id` | Yes | string | Mercator quadkey at zoom `<ZOOM>` |
| `built_surface_m2` | Yes | float | Total built surface area in m² (sum within tile). Use `NaN` for missing. |

> **Extra columns:** Ignored. Only `built_surface_m2` is read and merged into the mercator parquet.

See `template_built_surface_z14.csv` and `example_built_surface_z14.csv`.

---

### `<COUNTRY>_smod_z<ZOOM>.csv`

Pre-aggregated settlement model classification per tile. Provide the raw L2 value; the pipeline
derives `smod_class_l1` (1=rural, 2=suburban, 3=urban) automatically.

| Column | Required | Type | Notes |
|--------|----------|------|-------|
| `tile_id` | Yes | string | Mercator quadkey at zoom `<ZOOM>` |
| `smod_class` | Yes | float | GHS-SMOD L2 class (median within tile). Values: 10=water, 11=very low density rural, 12=low density rural, 13=rural cluster, 21=suburban, 22=semi-dense urban, 23=dense urban, 30=urban centre. Use `NaN` for missing. |

> **Extra columns:** Ignored. Only `smod_class` is read; `smod_class_l1` is always derived automatically.

See `template_smod_z14.csv` and `example_smod_z14.csv`.

---

### `<COUNTRY>_rwi_z<ZOOM>.csv`

Pre-aggregated Relative Wealth Index per tile.

| Column | Required | Type | Notes |
|--------|----------|------|-------|
| `tile_id` | Yes | string | Mercator quadkey at zoom `<ZOOM>` |
| `rwi` | Yes | float | Mean RWI within tile (range approximately -2.5 to +2.5). Use `NaN` for missing. |

> **Extra columns:** Ignored. Only `rwi` is read and merged into the mercator parquet.

See `template_rwi_z14.csv` and `example_rwi_z14.csv`.

---

## Getting tile IDs for a country

To generate the list of valid quadkey tile IDs for a country at a given zoom level, run:

```python
from gigaspatial.core.tiles import MercatorTiles
from gigaspatial.handlers.boundaries import AdminBoundaries

boundaries = AdminBoundaries.create(country_code='PNG', admin_level=0)
tiles = MercatorTiles.from_geometry(boundaries.to_geodataframe().geometry.union_all(), zoom_level=14)
tile_ids = [t.quadkey for t in tiles]
```

Alternatively, read the tile IDs from the existing base mercator parquet if already initialized:
```python
import geopandas as gpd
gdf = gpd.read_parquet('geodb/aos_views/mercator_views/PNG_14.parquet')
tile_ids = gdf['tile_id'].tolist()
```
