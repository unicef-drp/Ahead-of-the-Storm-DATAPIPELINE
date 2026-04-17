# Health Center Data Schema

Canonical column names for health facility data. These names are used in the pipeline cache
(`hc_views/<COUNTRY>_health_centers.parquet`) and in per-storm impact views
(`hc_views/<COUNTRY>_<STORM>_<DATE>_<WIND>.parquet`).

When adding custom health center data, use these exact column names so downstream consumers
(Dash app, AI agent) can query them consistently across countries.

## Required columns

| Column | Type | Notes |
|--------|------|-------|
| `latitude` | float | WGS84 decimal degrees |
| `longitude` | float | WGS84 decimal degrees |
| `amenity` | string | Facility type. Only `hospital`, `clinic`, `doctors` are included in impact calculations. Other values (e.g. `pharmacy`, `dentist`) are stored in the cache but excluded at analysis time. |

## Standard columns (from HealthSites.io API)

All columns below are returned by the HealthSites.io API and will be present for API-sourced
countries (values may be empty/null where not tagged in OSM). Include them in custom CSVs
where available so queries work consistently across countries.

| Column | Type | Notes |
|--------|------|-------|
| `osm_id` | string | OSM node/way ID. Also accepted as `id` in custom CSV input. |
| `osm_type` | string | OSM element type: `node`, `way`, or `relation` |
| `name` | string | Facility name |
| `healthcare` | string | OSM `healthcare` tag, e.g. `hospital`, `clinic` |
| `health_amenity_type` | string | Combined type descriptor |
| `operator` | string | Operating organisation name |
| `operator_type` | string | `public`, `private`, `ngo`, `religious` |
| `source` | string | Data source attribution |
| `speciality` | string | Medical speciality, e.g. `maternity`, `cardiology`, `general` |
| `beds` | int | Number of inpatient beds |
| `staff_doctors` | int | Number of doctors |
| `staff_nurses` | int | Number of nurses |
| `emergency` | string | `yes` / `no` — whether emergency services are available |
| `operational_status` | string | e.g. `operational`, `closed` |
| `opening_hours` | string | OSM opening hours format |
| `contact_number` | string | Phone number |
| `wheelchair` | string | `yes` / `no` / `limited` |
| `dispensing` | string | `yes` / `no` — pharmacy dispensing |
| `insurance` | string | Insurance accepted |
| `water_source` | string | Water supply type |
| `electricity` | string | Electricity availability |
| `is_in_health_area` | string | Administrative health area |
| `is_in_health_zone` | string | Administrative health zone |
| `completeness` | float | HealthSites.io data completeness score (0–100) |
| `url` | string | Website |
| `addr_housenumber` | string | Address |
| `addr_street` | string | Street name |
| `addr_postcode` | string | Postcode |
| `addr_city` | string | City |
| `uuid` | string | HealthSites.io internal UUID |
| `changeset_id` | float | OSM changeset ID |
| `changeset_version` | float | OSM changeset version |
| `changeset_timestamp` | string | Last edit timestamp |
| `changeset_user` | string | Last OSM editor |

## Well-known optional columns

Use these exact names for attributes not covered by the HealthSites.io API.

| Column | Type | Notes |
|--------|------|-------|
| `num_icu_beds` | int | Number of ICU beds |
| `has_generator` | string | `yes` / `no` — backup power available |
| `catchment_population` | int | Estimated population served |
