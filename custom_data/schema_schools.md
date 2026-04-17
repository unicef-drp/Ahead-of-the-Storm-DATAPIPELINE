# School Data Schema

Canonical column names for school facility data. These names are used in the pipeline cache
(`school_views/<COUNTRY>_schools.parquet`) and in per-storm impact views
(`school_views/<COUNTRY>_<STORM>_<DATE>_<WIND>.parquet`).

When adding custom school data, use these exact column names so downstream consumers
(Dash app, AI agent) can query them consistently across countries.

## Required columns

| Column | Type | Notes |
|--------|------|-------|
| `latitude` | float | WGS84 decimal degrees |
| `longitude` | float | WGS84 decimal degrees |

## Standard columns (from GIGA API)

These are returned by the GIGA API and will be present for all API-sourced countries.
Include them in custom CSVs where available.

| Column | Type | Notes |
|--------|------|-------|
| `school_id_giga` | string | GIGA unique school identifier. Also accepted as `id` in custom CSV input. |
| `school_name` | string | Official school name |
| `education_level` | string | `Pre-Primary`, `Primary`, `Secondary`, `Post-Secondary`, `Unknown` |
| `school_data_source` | string | Data source, e.g. `government`, `ministry_of_education` |
| `country_iso3_code` | string | ISO3 country code |

## Well-known optional columns

Use these exact names when adding attributes not returned by the GIGA API.
Inconsistent naming across countries (e.g. `number_of_students` vs `num_students`)
will break cross-country queries downstream.

| Column | Type | Notes |
|--------|------|-------|
| `num_students` | int | Total enrolled students |
| `num_teachers` | int | Number of teachers |
| `has_electricity` | string | `yes` / `no` |
| `has_water` | string | `yes` / `no` |
| `connectivity` | string | e.g. `yes`, `no`, `unknown` |
