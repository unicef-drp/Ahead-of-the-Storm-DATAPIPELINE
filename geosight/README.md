# GeoSight Integration

Syncs the pipeline's admin-level impact outputs to the GeoSight related table
**"Ahead of the Storm – Admin-level Impacts"**.

## What It Uploads

The uploader reads admin impact CSV files from `geodb/aos_views/admin_views/`
(local) or the `AOTS_ANALYSIS` Snowflake stage (production), supporting all
admin levels 1–5:

- `{COUNTRY}_{STORM}_{FORECAST}_{WIND}_admin{N}.csv`

CCI files (`*_cci.csv`) are intentionally ignored — only the core impact
metrics are uploaded to GeoSight.

Each related-table row represents one admin region × forecast time × wind
threshold, with these base fields:

| Field | Description |
|---|---|
| `country_code` | ISO3 country code (e.g. `TWN`) |
| `storm` | Storm name (e.g. `FUNG-WONG`) |
| `admin_level` | Admin level (1–5) |
| `forecast_time` | Forecast time in ISO 8601 (UTC) |
| `wind_threshold` | Wind speed in knots |
| `geom_id` | GeoRepo admin ucode (e.g. `TWN_0001_V2`) |

Plus all impact metric columns found in the CSV (e.g. `E_population`,
`E_num_schools`, `E_num_hcs`). The columns `E_rwi`, `E_smod_class`, and
`E_smod_class_l1` are intentionally excluded. CCI files are ignored entirely.

## Environment

```bash
# Storage backend — must match the main pipeline setting
export DATA_PIPELINE_DB=SNOWFLAKE   # or LOCAL for development

# GeoSight credentials
export GEOSIGHT_BASE_URL=https://geosight.unicef.org
export GEOSIGHT_API_KEY="Token <api-key>"   # include the "Token " prefix
export GEOSIGHT_USER_EMAIL="your.email@example.org"

# Required when DATA_PIPELINE_DB=SNOWFLAKE
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_STAGE_NAME=AOTS_ANALYSIS
```

## Running

### Incremental (default) — safe to run as a cron job

```bash
python geosight/upload_admin_related_table.py
```

Scans GeoSight for the latest `forecast_time` already present per
`(country_code, storm, admin_level)`, then downloads and uploads only files
with a newer forecast time from the stage.

### Backfill — upload everything without dedup

```bash
python geosight/upload_admin_related_table.py --backfill
```

Combine with filters to target a specific subset:

```bash
# One country
python geosight/upload_admin_related_table.py --backfill --country TWN

# One date
python geosight/upload_admin_related_table.py --backfill --date 2025-11-10

# Date range
python geosight/upload_admin_related_table.py --backfill \
  --from-date 2025-11-01 --to-date 2025-11-30

# Country + date range
python geosight/upload_admin_related_table.py --backfill \
  --country PNG --from-date 2025-11-01
```

Filters also work in incremental mode to restrict which files are considered.

## Schema Management

The script creates the related table if it does not exist, and extends the
schema automatically when new metric columns appear in the CSV files. Existing
fields are never removed or renamed.

## Deduplication

Rows are deduplicated by the 4-tuple `storm|forecast_time|wind_threshold|geom_id`.
Since `geom_id` is a level-specific GeoRepo ucode, this key is unique across
all admin levels within the single table.

## Note on `tile_id`

The pipeline's admin impact CSVs store the GeoRepo admin ucode in a column
named `tile_id` (a naming quirk from the mercator tile pipeline). The uploader
reads that column and writes it as `geom_id` in GeoSight.
