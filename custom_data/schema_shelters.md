# Shelter Data Schema

Canonical column names for emergency shelter data. These names are used in the pipeline cache
(`shelter_views/<COUNTRY>_shelters.parquet`) and in per-storm impact views
(`shelter_views/<COUNTRY>_<STORM>_<DATE>_<WIND>.parquet`).

OSM coverage for `social_facility=shelter` is sparse in most countries. Providing a
government-issued shelter registry as a custom CSV is the recommended approach.

When adding custom shelter data, use these exact column names so downstream consumers
(Dash app, AI agent) can query them consistently across countries.

## Required columns

| Column | Type | Notes |
|--------|------|-------|
| `latitude` | float | WGS84 decimal degrees |
| `longitude` | float | WGS84 decimal degrees |

## Standard columns (from OSM Overpass API)

These are returned by the OSM Overpass query and will be present for all OSM-sourced countries.
Include them in custom CSVs where available.

| Column | Type | Notes |
|--------|------|-------|
| `osm_id` | int/string | OSM node/way ID. Also accepted as `id` in custom CSV input. |
| `name` | string | Shelter name (may be empty) |
| `name_en` | string | English name if available |
| `shelter_type` | string | OSM `social_facility` tag value, e.g. `shelter` |
| `category` | string | OSM key matched, e.g. `social_facility` |
| `type` | string | OSM element type: `node`, `way`, or `relation` |
| `matching_categories` | string | Raw OSM tags that matched the query |

## Well-known optional columns

Use these exact names when adding attributes from government shelter registries or other sources.
These are not returned by OSM — only relevant for custom CSVs.

| Column | Type | Notes |
|--------|------|-------|
| `capacity` | int | Maximum occupancy (number of people) |
| `managing_agency` | string | Agency or organisation responsible, e.g. `NDMO`, `Red Cross` |
| `has_water` | string | `yes` / `no` |
| `has_sanitation` | string | `yes` / `no` |
| `has_electricity` | string | `yes` / `no` |
| `accessible` | string | `yes` / `no` — wheelchair / disability accessible |
