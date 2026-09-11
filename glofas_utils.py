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

The RIVER_FORECASTS pointer-table lookup (forecast_time/stage_path per RP
tier) always queries Snowflake directly, regardless of config.HAZARD_DATA_SOURCE:
get_latest_river_forecast()/get_river_forecast_for_date() below are
unconditional Snowflake SELECTs. precip_utils.get_latest_met_forecast()/
get_met_forecast_for_date() follow this same always-Snowflake pattern; only
wind/tracks' own discovery (snowflake_utils.get_storms_local(), a deliberate
exception, see that function's own docstring for why) bypasses its
pointer table by listing files instead. This means HAZARD_DATA_SOURCE=LOCAL/
BLOB still requires live Snowflake credentials
for the pointer-table SELECT itself, an intentional, documented design choice,
not an oversight.

The actual Parquet file read, in get_river_forecasts_data_store()
below, respects HAZARD_DATA_SOURCE and returns a Blob-backed store for BLOB
(reusing get_hazard_data_store(), the same function precip/wind already use for
their own file reads). This works without any directory-walk logic because
STAGE_PATH is already the full relative path (glofas_extent_masking.py writes
the identical f"{EXTENT_STAGE_PREFIX}/{date_str}/{filename}" string to both the
Snowflake stage and Blob, the same relative layout either way, matching
MET_FORECASTS' own precedent), so the pointer-table row already tells the Blob
reader exactly where to look. LOCAL is not implemented (no HAZARD_LOCAL_RIVER_DIR-
equivalent config exists yet); only SNOWFLAKE and BLOB are supported for the file
read.

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
    A data store for reading the actual RP-tier Parquet file a RIVER_FORECASTS
    row's STAGE_PATH points at, governed by config.HAZARD_DATA_SOURCE: SNOWFLAKE
    (default) reads from Snowflake's internal stage; BLOB reads from the shared
    Blob container instead, via get_hazard_data_store() (the same function
    precip/wind's own LOCAL/BLOB file reads already use). LOCAL is not
    implemented (no HAZARD_LOCAL_RIVER_DIR-equivalent config exists yet).

    This is independent of the RIVER_FORECASTS pointer-table SELECT itself
    (get_latest_river_forecast()/get_river_forecast_for_date() below), which
    always queries Snowflake regardless of this setting; see this module's
    own docstring for why that part is a deliberate, documented exception,
    unlike wind/tracks' own LOCAL/BLOB path, which bypasses the pointer
    table entirely. Precip's own pointer lookup (get_latest_met_forecast()
    in precip_utils.py) follows this same always-Snowflake pattern; wind/tracks'
    own discovery is the only exception.
    """
    if app_config.HAZARD_DATA_SOURCE == 'BLOB':
        from data_store_utils import get_hazard_data_store
        return get_hazard_data_store()  # shared container, files under 'glofas/'

    if app_config.HAZARD_DATA_SOURCE == 'LOCAL':
        raise NotImplementedError(
            "HAZARD_DATA_SOURCE=LOCAL is not implemented for river-flood's file "
            "read (no HAZARD_LOCAL_RIVER_DIR-equivalent config exists yet); use "
            "SNOWFLAKE or BLOB."
        )

    from gigaspatial.core.io.snowflake_data_store import SnowflakeDataStore
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


def get_latest_river_forecasts_bulk(params) -> dict:
    """
    Latest RIVER_FORECASTS row for EVERY param in `params` at once, via a
    single Snowflake round-trip (one window-function query), instead of
    one get_latest_river_forecast() call per param. 6 sequential per-param
    calls cost ~2s each in Snowflake round-trip latency, ~12s total;
    collapsed into one round-trip via a single window-function query. Only
    "latest" mode (no target_date) has this bulk equivalent: a historical
    backfill (get_river_forecast_for_date(), target_date given) is a cold,
    infrequent path where 6 separate calls are not a meaningful cost driver.

    Args:
        params: iterable of PARAM values (e.g. ['extent_rp2_bymember', ...]).

    Returns:
        {param: {'forecast_time', 'stage_path', 'is_standin'} or None}, one
        entry per param in `params`, same per-entry shape
        get_latest_river_forecast() returns for that param. A param with no
        rows yet maps to None, same expected-normal-outcome semantics.
    """
    params = list(params)
    result = {p: None for p in params}
    if not params:
        return result
    placeholders = ", ".join(["%s"] * len(params))
    query = f"""
        SELECT PARAM, FORECAST_TIME, STAGE_PATH, IS_STANDIN FROM (
            SELECT PARAM, FORECAST_TIME, STAGE_PATH, IS_STANDIN,
                   ROW_NUMBER() OVER (PARTITION BY PARAM ORDER BY FORECAST_TIME DESC) AS rn
            FROM RIVER_FORECASTS
            WHERE PARAM IN ({placeholders})
        ) WHERE rn = 1
    """
    df = _execute_query(query, params=params)
    for _, row in df.iterrows():
        result[row['PARAM']] = {
            'forecast_time': row['FORECAST_TIME'],
            'stage_path': row['STAGE_PATH'],
            'is_standin': bool(row['IS_STANDIN']) if pd.notna(row['IS_STANDIN']) else None,
        }
    missing = [p for p, v in result.items() if v is None]
    if missing:
        logger.info(f"No RIVER_FORECASTS rows found for param(s)={missing}")
    return result


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
        RIVER_FORECASTS row exists for this param on this date, an
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

    The local filename is derived from os.path.basename(stage_path) alone;
    this function has no awareness of which RP tier it's downloading, so a
    caller keeping multiple tiers' downloads alive in the same shared
    directory at once must namespace each call's local_dir itself (e.g. a
    per-tier subdirectory) to guard against a basename collision; that
    guarantee is not enforced here.

    Args:
        stage_path: STAGE_PATH value from a RIVER_FORECASTS row.
        data_store: get_river_forecasts_data_store() instance.
        local_dir: caller-owned directory the file is written into, cleaned
            up by the caller once no longer needed.

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
    in memory, against this already country-scoped result.

    Args:
        local_parquet_path: from download_river_forecast_parquet(). Always a
            path this process itself created (a tempfile basename derived
            from a Snowflake-controlled stage path), never raw user input,
            safe to interpolate directly into the SQL string below.
        bounds: (xmin, ymin, xmax, ymax), e.g. from gdf_tiles.total_bounds.

    Returns:
        DataFrame with columns: pixel_lat, pixel_lon, member, step_h,
        below_min_basin. Empty DataFrame if no flooded pixels fall in this
        country's bounding box at this tier (a common, normal outcome).
    """
    xmin, ymin, xmax, ymax = bounds
    con = duckdb.connect()
    try:
        # DuckDB manages its own C++ memory pool entirely outside Python's;
        # gc.collect()/malloc_trim() (see main_pipeline.py's _release_memory())
        # have no visibility into it, and DuckDB's own default (up to 80% of
        # system RAM) assumes it owns the whole machine, not a fraction of a
        # fixed node shared with a real, memory-heavy Python process doing its
        # own geopandas/pandas work. Explicitly bounding this connection's own
        # memory forces it to spill to disk rather than compete for RAM this
        # process needs elsewhere. A fresh connection is opened and closed
        # per call already (once per country per tier), so this limit applies
        # per-query, not cumulatively across the whole run.
        con.execute("PRAGMA memory_limit='2GB'")
        query = f"""
            SELECT pixel_lat, pixel_lon, member, step_h, below_min_basin
            FROM read_parquet('{local_parquet_path}')
            WHERE pixel_lon BETWEEN {xmin} AND {xmax}
              AND pixel_lat BETWEEN {ymin} AND {ymax}
        """
        return con.execute(query).df()
    finally:
        con.close()
