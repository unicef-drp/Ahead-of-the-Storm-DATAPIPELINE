#!/usr/bin/env python3
"""
GloFAS River-Flood Utilities Module

Reads ambient global GloFAS x JRC flood-extent forecasts, produced daily by
TC-ECMWF-Forecast-Pipeline regardless of whether any named storm is active
(RIVER_FORECASTS' own refresh cadence, same relationship this repo already
has with MET_FORECASTS for tp/ro). Each RP tier is a sparse, true-only
Parquet file (one row per flooded (pixel, member, step) combination) on
Snowflake's internal stage, referenced via a RIVER_FORECASTS pointer row
(same pointer-table pattern as MET_FORECASTS).

Unlike precip's ~0.25 degree continuous raster, GloFAS/JRC pixels are
~150m, comparable to a zoom-14 mercator tile or a buffered facility
footprint, not much coarser. A single RP tier's global file is 70M+ rows for
one date, so this module downloads each tier's file ONCE per (tier, date)
and lets callers query it per-country via DuckDB's bbox-filtered
read_parquet(), rather than loading the whole file into pandas.

Deliberately Snowflake-only regardless of config.HAZARD_DATA_SOURCE (unlike
wind/tracks/precip, which all support LOCAL/BLOB via that setting): GloFAS's
local output layout (TC-ECMWF-Forecast-Pipeline's glofas_extent_masking.py)
nests each RP tier's Parquet under a per-date subdirectory
(f"{date_str}/river_extent_rp{{N}}_bymember_{{date_str}}.parquet"), unlike
wind/tracks/met's flat one-file-per-cycle convention that
get_hazard_data_store()/_list_local_met_files() already handle. A LOCAL/BLOB
reader for river-flood would need its own directory-walk logic, not
implemented here. This means HAZARD_DATA_SOURCE=LOCAL/BLOB still requires
live Snowflake credentials for river-flood specifically, even though wind/
tracks/precip are genuinely credential-free in that mode, an intentional,
documented exception, not an oversight. get_river_forecasts_data_store()
below logs this explicitly whenever HAZARD_DATA_SOURCE isn't SNOWFLAKE.

Usage:
    from glofas_utils import get_latest_river_forecast, \
        download_river_forecast_parquet, query_country_flooded_pixels, \
        get_river_forecasts_data_store
    row = get_latest_river_forecast('extent_rp10_bymember')
    local_path = download_river_forecast_parquet(row['stage_path'], get_river_forecasts_data_store(), tmp_dir)
    df_pixels = query_country_flooded_pixels(local_path, gdf_tiles.total_bounds)
"""

import os
import logging
from typing import Optional

import duckdb
import pandas as pd

from snowflake_utils import _execute_query
from data_store_utils import get_snowflake_auth_kwargs
from config import config as app_config

logger = logging.getLogger(__name__)

# Full ECMWF ensemble size: 50 perturbed members + 1 control. Mirrors
# impact_analysis.FULL_ENSEMBLE_SIZE / precip_utils.FULL_ENSEMBLE_SIZE
# exactly, same physical constant (GloFAS is also a real 51-member
# ensemble, same member-ID convention), same reason: hard-coded so
# probability denominators stay correct even when individual members are
# missing/corrupt for a cycle, not read from the data's own shape.
FULL_ENSEMBLE_SIZE = 51


# =============================================================================
# SNOWFLAKE: RIVER_FORECASTS POINTER TABLE
# =============================================================================

def get_river_forecasts_data_store():
    """
    A data store that always reads from Snowflake's internal stage,
    regardless of this repo's own DATA_PIPELINE_DB *or* HAZARD_DATA_SOURCE
    setting -- see this module's own docstring for why river-flood is a
    deliberate, documented exception to the LOCAL/BLOB dispatch wind/tracks/
    precip all support.

    RIVER_FORECASTS.STAGE_PATH values point at Parquet files TC-ECMWF-
    Forecast-Pipeline uploads to Snowflake's internal stage, this is
    genuinely source data (like MET_FORECASTS/TC_ENVELOPES_COMBINED, always
    queried directly regardless of DATA_PIPELINE_DB), not one of this repo's
    own output views.
    """
    from gigaspatial.core.io.snowflake_data_store import SnowflakeDataStore

    if app_config.HAZARD_DATA_SOURCE != 'SNOWFLAKE':
        logger.info(
            f"HAZARD_DATA_SOURCE={app_config.HAZARD_DATA_SOURCE}, but river-flood "
            "always reads RIVER_FORECASTS from Snowflake regardless (no LOCAL/BLOB "
            "reader implemented yet, see glofas_utils.py's own module docstring)"
        )

    return SnowflakeDataStore(
        account=app_config.SNOWFLAKE_ACCOUNT,
        warehouse=app_config.SNOWFLAKE_WAREHOUSE,
        database=app_config.SNOWFLAKE_DATABASE,
        schema=app_config.SNOWFLAKE_SCHEMA,
        stage_name=app_config.SNOWFLAKE_STAGE_NAME,
        **get_snowflake_auth_kwargs(),
    )


def get_latest_river_forecast(param: str) -> Optional[dict]:
    """
    Latest RIVER_FORECASTS row for one extent PARAM (e.g. 'extent_rp10_bymember').

    Args:
        param: one of 'extent_rp2_bymember', 'extent_rp5_bymember',
            'extent_rp10_bymember', 'extent_rp20_bymember',
            'extent_rp50_bymember', 'extent_rp100_bymember'.

    Returns:
        {'forecast_time', 'stage_path', 'is_standin'} or None if no rows
        exist for this param yet (an expected, normal outcome, not an error,
        mirroring get_latest_met_forecast_snowflake()'s own precedent).
    """
    query = (
        "SELECT FORECAST_TIME, STAGE_PATH, IS_STANDIN FROM RIVER_FORECASTS "
        "WHERE PARAM = %s ORDER BY FORECAST_TIME DESC LIMIT 1"
    )
    df = _execute_query(query, params=[param])
    if df.empty:
        logger.info(f"No RIVER_FORECASTS rows found for param='{param}'")
        return None
    row = df.iloc[0]
    return {
        'forecast_time': row['FORECAST_TIME'],
        'stage_path': row['STAGE_PATH'],
        'is_standin': bool(row['IS_STANDIN']) if pd.notna(row['IS_STANDIN']) else None,
    }


def get_river_forecast_for_date(param: str, target_date) -> Optional[dict]:
    """
    RIVER_FORECASTS row for one extent PARAM on a specific calendar date, for
    historical backfills (--type update --date ...). Mirrors
    get_met_forecast_for_date_snowflake()'s own "latest wins" tie-break if
    multiple cycles exist for that date.

    Args:
        param: see get_latest_river_forecast().
        target_date: date-like value (datetime.date, or a 'YYYY-MM-DD' string).

    Returns:
        {'forecast_time', 'stage_path', 'is_standin'} or None if no
        RIVER_FORECASTS row exists for this param on this date -- an
        expected, normal outcome for a historical date predating this
        pipeline, or one where Zarr/Parquet retention has since expired.
    """
    query = (
        "SELECT FORECAST_TIME, STAGE_PATH, IS_STANDIN FROM RIVER_FORECASTS "
        "WHERE PARAM = %s AND TO_DATE(FORECAST_TIME) = %s "
        "ORDER BY FORECAST_TIME DESC LIMIT 1"
    )
    df = _execute_query(query, params=[param, str(target_date)])
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        'forecast_time': row['FORECAST_TIME'],
        'stage_path': row['STAGE_PATH'],
        'is_standin': bool(row['IS_STANDIN']) if pd.notna(row['IS_STANDIN']) else None,
    }


# =============================================================================
# DOWNLOAD + PER-COUNTRY BBOX FILTER
# =============================================================================

def download_river_forecast_parquet(stage_path: str, data_store, local_dir: str) -> str:
    """
    Download ONE RP tier's global Parquet file to a local directory, ONCE per
    (RP tier, forecast date), never per country. Mirrors
    read_precip_window()'s data_store.read_file(stage_path) download, but
    persists bytes to a real file (not an in-memory array) because DuckDB's
    read_parquet() needs a real path to query repeatedly (once per country)
    without re-downloading.

    Args:
        stage_path: STAGE_PATH value from a RIVER_FORECASTS row.
        data_store: get_river_forecasts_data_store() instance.
        local_dir: caller-owned directory (e.g. a
            tempfile.TemporaryDirectory() kept alive for every country's
            query against the returned path, cleaned up by the caller once
            this tier's country loop completes).

    Returns:
        Local filesystem path to the downloaded Parquet file.
    """
    raw_bytes = data_store.read_file(stage_path)
    local_path = os.path.join(local_dir, os.path.basename(stage_path))
    with open(local_path, 'wb') as f:
        f.write(raw_bytes)
    return local_path


def query_country_flooded_pixels(local_parquet_path: str, bounds) -> pd.DataFrame:
    """
    One DuckDB bbox-filtered query against an already-downloaded RP-tier
    Parquet file, run ONCE per (country, tier), NOT once per
    (country, tier, step_h): step_h filtering happens afterwards, cheaply,
    in memory, against this already country-scoped result (typically
    thousands-to-low-millions of rows, vs. tens of millions untouched
    globally).

    No DuckDB extensions needed: pixel_lat/pixel_lon are plain float64
    columns on a local file (unlike gigaspatial's own OvertureAmenityFetcher,
    which needs the 'spatial'/'httpfs' extensions for a WKB-geometry-on-S3
    Parquet source).

    Args:
        local_parquet_path: from download_river_forecast_parquet(). Always a
            path this process itself created (a tempfile basename derived
            from a Snowflake-controlled stage path), never raw user input --
            safe to interpolate directly into the SQL string below.
        bounds: (xmin, ymin, xmax, ymax), e.g. from gdf_tiles.total_bounds --
            the country's own tile-covered footprint, already computed by
            the per-country setup cache. No separate GeoRepo/
            AdminBoundaries.create() call needed.

    Returns:
        DataFrame with columns: pixel_lat, pixel_lon, member, step_h,
        below_min_basin. Empty DataFrame if no flooded pixels fall in this
        country's bounding box at this tier (a common, normal outcome).
    """
    xmin, ymin, xmax, ymax = bounds
    con = duckdb.connect()
    try:
        query = f"""
            SELECT pixel_lat, pixel_lon, member, step_h, below_min_basin
            FROM read_parquet('{local_parquet_path}')
            WHERE pixel_lon BETWEEN {xmin} AND {xmax}
              AND pixel_lat BETWEEN {ymin} AND {ymax}
        """
        return con.execute(query).df()
    finally:
        con.close()
