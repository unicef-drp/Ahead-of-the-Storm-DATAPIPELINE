#!/usr/bin/env python3
"""
Precipitation/Runoff Utilities Module

Reads ambient global precipitation (tp) and runoff (ro) ensemble forecasts,
produced every pipeline cycle by TC-ECMWF-Forecast-Pipeline regardless of
whether any named storm is active. By default (config.HAZARD_DATA_SOURCE=
SNOWFLAKE), data lands in Snowflake as a pointer table (MET_FORECASTS)
referencing dense Zarr files on the stage, one file per param per forecast
cycle, each holding a (51-member, 25-step) accumulated grid. When
HAZARD_DATA_SOURCE is LOCAL/BLOB, the same Zarr files are read directly from
TC-ECMWF-Forecast-Pipeline's own met_data/ output instead (see
get_hazard_met_data_store()/get_latest_met_forecast_local()/
get_met_forecast_for_date_local() below) -- get_latest_met_forecast()/
get_met_forecast_for_date() dispatch between the two automatically. This
module downloads and reads those Zarr files, and computes exceedance-
probability grids from them.

Usage:
    from precip_utils import get_latest_met_forecast, read_precip_window, \
        exceedance_probability, ratio_exceedance_probability
    tp_row = get_latest_met_forecast('tp')
    member_numbers, lat, lon, tp_grid = read_precip_window(tp_row['stage_path'], data_store, 0, 24)
    probability_grid = exceedance_probability(tp_grid, threshold_mm=50)
"""

import os
import re
import tempfile
import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import rasterio
import zarr

from snowflake_utils import _execute_query
from config import config as app_config

logger = logging.getLogger(__name__)

# Full ECMWF ensemble size: 50 perturbed members + 1 control. Mirrors
# impact_analysis.FULL_ENSEMBLE_SIZE exactly (same physical constant, same
# reason: hard-coded so probability denominators stay correct even when
# individual members are missing/corrupt for a cycle, not read from the
# array's own possibly-shrunk shape[0]).
FULL_ENSEMBLE_SIZE = 51


# =============================================================================
# SNOWFLAKE: MET_FORECASTS POINTER TABLE
# =============================================================================

def get_met_forecasts_data_store():
    """
    A data store that always reads from Snowflake's internal stage,
    regardless of this repo's own DATA_PIPELINE_DB setting.

    MET_FORECASTS.STAGE_PATH values point at Zarr files TC-ECMWF-Forecast-
    Pipeline uploads to Snowflake's internal stage, this is genuinely source
    data (like TC_ENVELOPES_COMBINED, always queried directly regardless of
    DATA_PIPELINE_DB), not one of this repo's own output views that can
    legitimately live in LOCAL/BLOB/SNOWFLAKE depending on deployment target.
    """
    from gigaspatial.core.io.snowflake_data_store import SnowflakeDataStore

    spcs_run = os.getenv('SPCS_RUN', 'false').lower() == 'true'
    return SnowflakeDataStore(
        account=app_config.SNOWFLAKE_ACCOUNT,
        user=None if spcs_run else app_config.SNOWFLAKE_USER,
        password=None if spcs_run else app_config.SNOWFLAKE_PASSWORD,
        warehouse=app_config.SNOWFLAKE_WAREHOUSE,
        database=app_config.SNOWFLAKE_DATABASE,
        schema=app_config.SNOWFLAKE_SCHEMA,
        stage_name=app_config.SNOWFLAKE_STAGE_NAME,
    )

def get_latest_met_forecast_snowflake(param: str) -> Optional[dict]:
    """
    Latest MET_FORECASTS row for a param ('tp' or 'ro').

    Args:
        param: 'tp' (total precipitation) or 'ro' (runoff).

    Returns:
        {'forecast_time':..., 'stage_path':...} or None if no rows exist for
        this param (an expected, normal outcome, not an error, e.g. before
        TC-ECMWF-Forecast-Pipeline has produced its first cycle).
    """
    query = (
        "SELECT FORECAST_TIME, STAGE_PATH FROM MET_FORECASTS "
        "WHERE PARAM = %s ORDER BY FORECAST_TIME DESC LIMIT 1"
    )
    df = _execute_query(query, params=[param])
    if df.empty:
        logger.info(f"No MET_FORECASTS rows found for param='{param}'")
        return None
    row = df.iloc[0]
    return {
        'forecast_time': row['FORECAST_TIME'],
        'stage_path': row['STAGE_PATH'],
    }


def get_met_forecast_for_date_snowflake(param: str, target_date) -> Optional[dict]:
    """
    MET_FORECASTS row for a param ('tp' or 'ro') on a specific calendar date,
    for historical backfills (--type update --date ...). MET_FORECASTS
    retains one row per past forecast cycle, same as TC_ENVELOPES_COMBINED
    retains historical storm envelopes, it is not overwritten to only ever
    hold the latest row, so a historical cycle's Zarr file is genuinely
    available to re-read, this mirrors update_storms()'s own
    `storms_df[storms_df['DATE'] == target_date_obj]` exact-date-match
    pattern for wind/gust rather than treating precip as un-backfillable.

    If multiple cycles exist for that calendar date (e.g. 00Z and 12Z), the
    latest one on that date is used, same "latest wins" tie-break as
    get_latest_met_forecast_snowflake() uses across the whole table.

    Args:
        param: 'tp' (total precipitation) or 'ro' (runoff).
        target_date: date-like value (datetime.date, or a 'YYYY-MM-DD' string).

    Returns:
        {'forecast_time':..., 'stage_path':...} or None if no MET_FORECASTS
        row exists for this param on this date (e.g. the date predates when
        MET_FORECASTS ingestion started, or Zarr retention on the stage has
        since expired for that cycle) — an expected, normal outcome for an
        old-enough backfill date, not an error.
    """
    query = (
        "SELECT FORECAST_TIME, STAGE_PATH FROM MET_FORECASTS "
        "WHERE PARAM = %s AND TO_DATE(FORECAST_TIME) = %s "
        "ORDER BY FORECAST_TIME DESC LIMIT 1"
    )
    df = _execute_query(query, params=[param, str(target_date)])
    if df.empty:
        logger.info(f"No MET_FORECASTS rows found for param='{param}' on {target_date}")
        return None
    row = df.iloc[0]
    return {
        'forecast_time': row['FORECAST_TIME'],
        'stage_path': row['STAGE_PATH'],
    }


# =============================================================================
# HAZARD_DATA_SOURCE=LOCAL/BLOB: read the same tp/ro Zarr files directly from
# TC-ECMWF-Forecast-Pipeline's own met_data/ output instead of MET_FORECASTS.
# Path convention confirmed directly against ecmwf_met_downloader.py:
# `run_str = f'{forecast_date:%Y%m%d}_{run_time:02d}'`,
# `zip_path = output_dir / f'{param}_{run_str}.zarr.zip'` -- flat, no
# subdirectory, matching real sample files (met_data/tp_20260701_18.zarr.zip).
# =============================================================================

_MET_FILENAME_RE = re.compile(r'^([a-z]+)_(\d{8})_(\d{2})\.zarr\.zip$')


def get_hazard_met_data_store():
    """
    Data store for reading MET_FORECASTS-equivalent precip/runoff Zarr files,
    governed by config.HAZARD_DATA_SOURCE. SNOWFLAKE (default) is identical
    to get_met_forecasts_data_store(); LOCAL/BLOB read TC-ECMWF-Forecast-
    Pipeline's own met_data/ output instead.
    """
    if app_config.HAZARD_DATA_SOURCE == 'SNOWFLAKE':
        return get_met_forecasts_data_store()
    from data_store_utils import get_hazard_data_store
    if app_config.HAZARD_DATA_SOURCE == 'LOCAL':
        return get_hazard_data_store(base_path=app_config.HAZARD_LOCAL_MET_DIR)
    return get_hazard_data_store()  # BLOB -- shared container, files under 'met/'


def _list_local_met_files(param: str):
    """Returns [(forecast_time datetime, relative_path), ...] for a given
    param, found in the configured LOCAL/BLOB met directory. Empty list if
    the directory can't be listed or has no matching files -- an expected
    outcome (e.g. before the first cycle), not an error."""
    data_store = get_hazard_met_data_store()
    search_dir = '.' if app_config.HAZARD_DATA_SOURCE == 'LOCAL' else 'met'
    try:
        files = data_store.list_files(search_dir)
    except Exception as e:
        logger.info(f"Could not list local/blob met files at '{search_dir}': {e}")
        return []

    results = []
    for f in files:
        basename = f.rsplit('/', 1)[-1]
        m = _MET_FILENAME_RE.match(basename)
        if not m or m.group(1) != param:
            continue
        dt = datetime.strptime(f"{m.group(2)}{m.group(3)}", "%Y%m%d%H")
        results.append((dt, f))
    return results


def get_latest_met_forecast_local(param: str) -> Optional[dict]:
    """LOCAL/BLOB equivalent of get_latest_met_forecast_snowflake()."""
    candidates = _list_local_met_files(param)
    if not candidates:
        logger.info(f"No local/blob met files found for param='{param}'")
        return None
    dt, path = max(candidates, key=lambda c: c[0])
    return {'forecast_time': dt, 'stage_path': path}


def get_met_forecast_for_date_local(param: str, target_date) -> Optional[dict]:
    """LOCAL/BLOB equivalent of get_met_forecast_for_date_snowflake()."""
    target = pd.to_datetime(target_date).date()
    candidates = [(dt, path) for dt, path in _list_local_met_files(param) if dt.date() == target]
    if not candidates:
        logger.info(f"No local/blob met files found for param='{param}' on {target_date}")
        return None
    dt, path = max(candidates, key=lambda c: c[0])
    return {'forecast_time': dt, 'stage_path': path}


def get_latest_met_forecast(param: str) -> Optional[dict]:
    """Dispatches to get_latest_met_forecast_snowflake() or
    get_latest_met_forecast_local() based on config.HAZARD_DATA_SOURCE."""
    if app_config.HAZARD_DATA_SOURCE == 'SNOWFLAKE':
        return get_latest_met_forecast_snowflake(param)
    return get_latest_met_forecast_local(param)


def get_met_forecast_for_date(param: str, target_date) -> Optional[dict]:
    """Dispatches to get_met_forecast_for_date_snowflake() or
    get_met_forecast_for_date_local() based on config.HAZARD_DATA_SOURCE."""
    if app_config.HAZARD_DATA_SOURCE == 'SNOWFLAKE':
        return get_met_forecast_for_date_snowflake(param, target_date)
    return get_met_forecast_for_date_local(param, target_date)


# =============================================================================
# ZARR DOWNLOAD + READ
# =============================================================================

def read_precip_window(stage_path: str, data_store, step_a_hour: int, step_b_hour: int):
    """
    Read an accumulated tp/ro window from a Zarr file on the stage.

    tp/ro are stored accumulated from T+0 at 6h-step intervals (0, 6, 12, ...,
    144), already in mm , so any window is just a difference of
    two steps, no matter how wide, at no extra download or storage cost
    beyond the one Zarr read.

    Args:
        stage_path: STAGE_PATH value from a MET_FORECASTS row.
        data_store: giga-spatial DataStore instance (LOCAL/BLOB/SNOWFLAKE).
        step_a_hour: window start, hours from T+0 (e.g. 0).
        step_b_hour: window end, hours from T+0 (e.g. 24). Must be a real
            step in the Zarr's own `steps` attribute (6h multiples, 0-144).

    Returns:
        (member_numbers, lat_min, lat_max, lon_min, lon_max, period_grid)
        where period_grid has shape (51, n_lat, n_lon), the accumulated mm
        over [step_a_hour, step_b_hour) per ensemble member. Row 0 of
        period_grid corresponds to lat_max (rows run N->S), matching the
        upstream Zarr's own row order and GeoTIFF's north-at-row-0
        convention, so grid_to_geotiff() below needs no flip.
    """
    raw_bytes = data_store.read_file(stage_path)

    with tempfile.NamedTemporaryFile(suffix='.zarr.zip') as tmp:
        tmp.write(raw_bytes)
        tmp.flush()

        store = zarr.storage.ZipStore(tmp.name, mode='r')
        try:
            root = zarr.open_group(store=store, mode='r')
            z = root['data']  # shape (51, 25, n_lat, n_lon), float16, mm accumulated from T+0
            attrs = dict(root.attrs)
            steps = list(attrs['steps'])  # [0, 6, 12, ..., 144]

            if step_a_hour not in steps or step_b_hour not in steps:
                raise ValueError(
                    f"Requested window [{step_a_hour}, {step_b_hour}] not in "
                    f"available steps {steps} for {stage_path}"
                )

            ia, ib = steps.index(step_a_hour), steps.index(step_b_hour)
            data_a = np.asarray(z[:, ia, :, :]).astype('float32')
            data_b = np.asarray(z[:, ib, :, :]).astype('float32')
            period_grid_mm = data_b - data_a

            member_numbers = list(attrs.get('member_numbers', range(1, z.shape[0] + 1)))
            lat_min, lat_max = float(attrs['lat_min']), float(attrs['lat_max'])
            lon_min, lon_max = float(attrs['lon_min']), float(attrs['lon_max'])
        finally:
            store.close()

    return member_numbers, lat_min, lat_max, lon_min, lon_max, period_grid_mm


# =============================================================================
# EXCEEDANCE PROBABILITY
# =============================================================================

def exceedance_probability(period_grid: np.ndarray, threshold_mm: float) -> np.ndarray:
    """
    Fraction of ensemble members exceeding a fixed mm threshold, per grid cell.

    Mirrors wind/gust's own probability convention exactly: divides by the
    fixed FULL_ENSEMBLE_SIZE=51 constant, not period_grid.shape[0] (.mean()'s
    default), so a Zarr with a shrunk member axis (e.g. a corrupt/missing
    perturbed-member GRIB for this cycle upstream) can't silently inflate the
    probability the way it would if normalized by the array's own observed size.

    Args:
        period_grid: (51, n_lat, n_lon) accumulated mm per member.
        threshold_mm: accumulated rainfall threshold in mm.

    Returns:
        (n_lat, n_lon) fraction of members exceeding threshold_mm.
    """
    nan_count = int(np.isnan(period_grid).sum())
    if nan_count > 0:
        logger.warning(
            f"exceedance_probability: {nan_count} NaN value(s) in period_grid "
            f"(out of {period_grid.size}), treated conservatively as non-exceeding "
            "per the fixed-51-denominator convention, not excluded from it"
        )
    return (period_grid > threshold_mm).sum(axis=0) / FULL_ENSEMBLE_SIZE


def ratio_exceedance_probability(
    ro_grid: np.ndarray,
    tp_grid: np.ndarray,
    ratio_threshold: float,
    min_tp_mm: float = 5.0,
) -> np.ndarray:
    """
    Fraction of ensemble members whose own ro/tp ratio exceeds a cut point.

    Computed per member, then averaged across members, exactly like
    exceedance_probability() above and like wind/gust's own probability
    convention, not mean(ro)/mean(tp), which would mix members representing
    very different physical situations (e.g. some tracking over
    already-saturated ground, some over dry ground) into one number that
    describes neither.

    Args:
        ro_grid: (51, n_lat, n_lon) accumulated runoff, mm, same window as tp_grid.
        tp_grid: (51, n_lat, n_lon) accumulated precipitation, mm, same window as ro_grid.
        ratio_threshold: cut point on the dimensionless ro/tp ratio (e.g. 0.3, 0.6).
        min_tp_mm: below this accumulated tp, a member's ratio is treated as 0
            (negligible rain means negligible runoff-response signal from that
            member, not an excluded observation).

    Returns:
        (n_lat, n_lon) fraction of members whose own ratio exceeds ratio_threshold.
    """
    nan_count = int(np.isnan(ro_grid).sum() + np.isnan(tp_grid).sum())
    if nan_count > 0:
        logger.warning(
            f"ratio_exceedance_probability: {nan_count} NaN value(s) across ro_grid/tp_grid, "
            "treated conservatively as non-exceeding per the fixed-51-denominator "
            "convention, not excluded from it"
        )
    per_member_ratio = np.where(tp_grid >= min_tp_mm, ro_grid / np.maximum(tp_grid, 1e-6), 0.0)
    return (per_member_ratio > ratio_threshold).sum(axis=0) / FULL_ENSEMBLE_SIZE


# =============================================================================
# GRID -> GEOTIFF (for giga-spatial's TifProcessor)
# =============================================================================

def grid_to_geotiff(
    grid_2d: np.ndarray,
    lat_min: float, lat_max: float, lon_min: float, lon_max: float,
    out_path: str,
) -> None:
    """
    Materialize a 2D probability grid as a GeoTIFF, so it can be opened with
    giga-spatial's TifProcessor and sampled per-tile via centroid-based point
    sampling (see create_precip_tile_view() in impact_analysis.py).

    Args:
        grid_2d: (n_lat, n_lon) array. Row 0 must correspond to lat_max (rows
            run N->S), matching read_precip_window()'s own row order and the
            underlying Zarr's row order (both N->S from row 0), so this
            already matches GeoTIFF's north-at-row-0 convention, no flip needed.
        lat_min, lat_max, lon_min, lon_max: outermost grid point coordinates, degrees.
        out_path: local file path to write the GeoTIFF to.
    """
    n_lat, n_lon = grid_2d.shape
    dlat = (lat_max - lat_min) / (n_lat - 1) if n_lat > 1 else 0.0
    dlon = (lon_max - lon_min) / (n_lon - 1) if n_lon > 1 else 0.0
    transform = rasterio.transform.from_bounds(
        lon_min - dlon / 2, lat_min - dlat / 2,
        lon_max + dlon / 2, lat_max + dlat / 2,
        n_lon, n_lat,
    )

    with rasterio.open(
        out_path, 'w', driver='GTiff',
        height=grid_2d.shape[0], width=grid_2d.shape[1],
        count=1, dtype='float32', crs='EPSG:4326', transform=transform,
    ) as dst:
        dst.write(grid_2d.astype('float32'), 1)
