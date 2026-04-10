# WASH Facility Data Schema

Canonical column names for WASH (Water, Sanitation and Hygiene) facility data. These names
are used in the pipeline cache (`wash_views/<COUNTRY>_wash.parquet`) and in per-storm
impact views (`wash_views/<COUNTRY>_<STORM>_<DATE>_<WIND>.parquet`).

Reference: https://wiki.openstreetmap.org/wiki/Humanitarian_OSM_Tags/WASH

When adding custom WASH data, use these exact column names so downstream consumers
(Dash app, AI agent) can query them consistently across countries.

## Required columns

| Column | Type | Notes |
|--------|------|-------|
| `latitude` | float | WGS84 decimal degrees |
| `longitude` | float | WGS84 decimal degrees |
| `wash_type` | string | Facility type — see values below. Used in top-5 at-risk reporting. |

**`wash_type` values** (matching OSM tags fetched by the pipeline):

| Value | OSM tag |
|-------|---------|
| `drinking_water` | `amenity=drinking_water` |
| `water_point` | `amenity=water_point` |
| `toilets` | `amenity=toilets` |
| `shower` | `amenity=shower` |
| `water_well` | `man_made=water_well` |
| `water_tap` | `man_made=water_tap` |
| `water_works` | `man_made=water_works` |
| `pumping_station` | `man_made=pumping_station` |
| `wastewater_treatment_plant` | `man_made=wastewater_treatment_plant` |

## Standard columns (from OSM Overpass API)

These are returned by the OSM Overpass query and will be present for all OSM-sourced countries.
Include them in custom CSVs where available.

| Column | Type | Notes |
|--------|------|-------|
| `osm_id` | int/string | OSM node/way ID. Also accepted as `id` in custom CSV input. |
| `name` | string | Facility name (often empty for WASH infrastructure) |
| `name_en` | string | English name if available |
| `category` | string | OSM key matched, e.g. `amenity`, `man_made` |
| `type` | string | OSM element type: `node`, `way`, or `relation` |
| `matching_categories` | string | Raw OSM tags that matched the query |

## Well-known optional columns

Use these exact names when adding attributes from government or humanitarian datasets.
These are not returned by OSM — only relevant for custom CSVs.

| Column | Type | Notes |
|--------|------|-------|
| `capacity` | int | Number of people served per day (water points) or stalls (toilets/showers) |
| `managing_agency` | string | Operating organisation, e.g. `UNICEF`, `government`, `community` |
| `water_source` | string | e.g. `borehole`, `spring`, `river`, `rainwater`, `piped` |
| `operational_status` | string | `operational`, `non-operational`, `seasonal` |
| `has_handwashing` | string | `yes` / `no` — handwashing station present |
| `population_served` | int | Estimated population relying on this facility |
