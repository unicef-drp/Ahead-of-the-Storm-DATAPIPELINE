#!/usr/bin/env python3
"""
GloFAS River-Flood Utilities Module

Reads ambient global GloFAS x JRC flood-extent forecasts, produced daily by
TC-ECMWF-Forecast-Pipeline regardless of whether any named storm is active
(RIVER_FORECASTS' own refresh cadence, same relationship this repo already
has with MET_FORECASTS for tp/ro). Each RP tier is a sparse, true-only
Parquet file on Snowflake's internal stage, referenced via a RIVER_FORECASTS
pointer row (same pointer-table pattern as MET_FORECASTS). The file is either
the extent_rpN_bymember layout (one row per flooded (pixel, member, step)
combination) or the compact extent_bymask_rpN layout (one row per flooded
pixel with a uint64 member mask per step).

Unlike precip's ~0.25 degree continuous raster, GloFAS/JRC pixels are
~150m, comparable to a zoom-14 mercator tile or a buffered facility
footprint, not much coarser. A single RP tier's global file is 70M+ rows for
one date, so this module downloads each tier's file ONCE per (tier, date)
and lets callers aggregate it per-country inside DuckDB with a bbox-filtered
read_parquet() (see river_tile_source.py), rather than loading the whole file
into pandas.

The RIVER_FORECASTS pointer-table lookup (forecast_time/stage_path per RP
tier) always queries Snowflake directly, regardless of config.HAZARD_DATA_SOURCE:
get_river_tier_pointers()/get_latest_river_forecasts_bulk()/
get_river_forecast_for_date() below are unconditional Snowflake SELECTs. precip_utils.get_latest_met_forecast()/
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
    from glofas_utils import get_river_tier_pointers, \
        download_river_forecast_parquet, get_river_forecasts_data_store
    tier_rows, bymask_candidates = get_river_tier_pointers(['rp10'])
    row = tier_rows['rp10']
    local_path = download_river_forecast_parquet(row['stage_path'], get_river_forecasts_data_store(), tmp_dir)
"""

import os
import logging
from typing import Optional

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
    reads from Snowflake's internal stage; BLOB (default) reads from the shared
    Blob container instead, via get_hazard_data_store() (the same function
    precip/wind's own LOCAL/BLOB file reads already use). LOCAL is not
    implemented (no HAZARD_LOCAL_RIVER_DIR-equivalent config exists yet).

    This is independent of the RIVER_FORECASTS pointer-table SELECT itself
    (get_river_tier_pointers()/get_latest_river_forecasts_bulk()/
    get_river_forecast_for_date() below), which always queries Snowflake regardless of this setting; see this module's
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


def get_latest_river_forecasts_bulk(params) -> dict:
    """
    Latest RIVER_FORECASTS row for EVERY param in `params` at once, via a
    single Snowflake round-trip (one window-function query), instead of
    one query per param. 6 sequential per-param queries cost ~2s each in
    Snowflake round-trip latency, ~12s total; collapsed into one round-trip
    via a single window-function query. Only
    "latest" mode (no target_date) has this bulk equivalent: a historical
    backfill (get_river_forecast_for_date(), target_date given) is a cold,
    infrequent path where 6 separate calls are not a meaningful cost driver.

    Args:
        params: iterable of PARAM values (e.g. ['extent_rp2_bymember', ...]).

    Returns:
        {param: {'forecast_time', 'stage_path', 'is_standin'} or None}, one
        entry per param in `params`. A param with no rows yet maps to None,
        an expected, normal outcome rather than an error.
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
        param: a RIVER_FORECASTS PARAM value (e.g. 'extent_rp10_bymember').
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


def get_river_tier_pointers(tiers, target_date=None):
    """
    Pointer rows of every tier in `tiers` (e.g. ['rp2', ...]) from ONE
    Snowflake round-trip covering both PARAM families of a tier: the
    per-member file (extent_<rp>_bymember) and the compact mask file
    (extent_bymask_<rp>). The latest row per PARAM is read, or, when
    `target_date` is given, the latest row of that calendar date.

    Returns:
        (tier_rows, bymask_rows). tier_rows[rp] is the pointer row the tier is
        processed from, {'forecast_time', 'stage_path', 'is_standin', 'kind'},
        or None when neither family has a row. kind is 'bymember' when the
        per-member row exists and is at least as recent as the mask row, and
        'bymask' when only the mask row exists or it is the more recent cycle
        (such a tier has no per-member file to read). bymask_rows[rp] is the
        mask row of the tier (kind 'bymask') or None.
    """
    tiers = list(tiers)
    members = {rp: f"extent_{rp}_bymember" for rp in tiers}
    masks = {rp: f"extent_bymask_{rp}" for rp in tiers}
    params = list(members.values()) + list(masks.values())
    placeholders = ", ".join(["%s"] * len(params))
    date_filter = ""
    query_params = list(params)
    if target_date is not None:
        date_filter = "AND TO_DATE(FORECAST_TIME) = %s"
        query_params.append(str(target_date))
    query = f"""
        SELECT PARAM, FORECAST_TIME, STAGE_PATH, IS_STANDIN FROM (
            SELECT PARAM, FORECAST_TIME, STAGE_PATH, IS_STANDIN,
                   ROW_NUMBER() OVER (PARTITION BY PARAM ORDER BY FORECAST_TIME DESC) AS rn
            FROM RIVER_FORECASTS
            WHERE PARAM IN ({placeholders}) {date_filter}
        ) WHERE rn = 1
    """
    df = _execute_query(query, params=query_params)
    by_param = {}
    for _, row in df.iterrows():
        by_param[row['PARAM']] = {
            'forecast_time': row['FORECAST_TIME'],
            'stage_path': row['STAGE_PATH'],
            'is_standin': bool(row['IS_STANDIN']) if pd.notna(row['IS_STANDIN']) else None,
        }
    tier_rows = {}
    bymask_rows = {}
    for rp in tiers:
        member = by_param.get(members[rp])
        mask = by_param.get(masks[rp])
        if member is not None:
            member['kind'] = 'bymember'
        if mask is not None:
            mask['kind'] = 'bymask'
        bymask_rows[rp] = mask
        if mask is not None and (member is None or pd.Timestamp(mask['forecast_time']) > pd.Timestamp(member['forecast_time'])):
            tier_rows[rp] = mask
        else:
            tier_rows[rp] = member
    missing = [rp for rp, v in tier_rows.items() if v is None]
    if missing:
        logger.info(f"No RIVER_FORECASTS rows found for tier(s)={missing}")
    return tier_rows, bymask_rows


# =============================================================================
# DOWNLOAD
# =============================================================================

def download_river_forecast_parquet(stage_path: str, data_store, local_dir: str) -> str:
    """
    Download ONE RP tier's global Parquet file to a local directory, ONCE per
    (RP tier, forecast date), never per country. Downloads via
    data_store.read_file(stage_path) like the precip Zarr read, but
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
