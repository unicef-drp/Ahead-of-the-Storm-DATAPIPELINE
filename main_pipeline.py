#!/usr/bin/env python3
"""
Main Impact Analysis Pipeline Orchestrator

Coordinates the complete impact analysis pipeline for tropical cyclone early warning.
Three operating modes: initialize, update, patch.

Key Features:
- initialize: builds country base layers (mercator tiles + admin views) with population,
  built surface, settlement class, wealth index, schools, health centers, shelters, WASH
- update: fetches active storm envelopes from Snowflake and runs geospatial intersection
  against all initialized countries within 100 km; generates per-facility and tile-level
  impact views at 8 wind thresholds (34–137 kt) plus CCI values
- patch: backfills specific columns in existing mercator parquets without full
  re-initialization (supported: population, school_age_population, infant_population,
  adolescent_population, built_surface_m2, smod_class, smod_class_l1, rwi,
  schools, hcs, shelters, wash, vulnerability)

- Custom data overrides: place a CSV in geodb/custom/ to replace any API or raster source
  for a specific country: custom files are never overwritten by the pipeline
- Storage-backend agnostic: LOCAL, Azure Blob (ADLS), or Snowflake internal stage

Usage Examples:
    # Initialize base data for a new country
    python main_pipeline.py --type initialize --countries TWN --zoom 14

    # Force re-initialization (regenerates all data from scratch)
    python main_pipeline.py --type initialize --countries PNG --rewrite 1

    # Process all recent storms (default: last 2 days)
    python main_pipeline.py --type update

    # Process storms for a specific date
    python main_pipeline.py --type update --date 2025-11-10

    # Process a specific storm on a specific date
    python main_pipeline.py --type update --date 2025-11-10 --storm FUNG-WONG

    # Backfill optional columns without full re-init
    python main_pipeline.py --type patch --countries PNG --columns shelters wash

    # Backfill raster columns after data becomes available
    python main_pipeline.py --type patch --countries PNG --columns built_surface_m2 rwi
"""

import os
import sys
import argparse
import logging
import gc
import ctypes
import ctypes.util
import multiprocessing
import threading
import time
import faulthandler
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import datetime, timezone
import pandas as pd
import geopandas as gpd

import pyarrow
# PyArrow's own internal thread pool can deadlock under concurrent access
# (main thread stuck inside infer_dtype, waiting on a lock a stuck
# arrow::internal::ThreadPool worker never released). Capping it to 1 is the
# standard low-risk mitigation (slightly slower large-parquet reads, no other
# downside); must be set before any parquet read, hence here, right after the
# pandas/geopandas imports that would otherwise trigger one.
pyarrow.set_cpu_count(1)
pyarrow.set_io_thread_count(1)

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dotenv import load_dotenv

# Load environment variables from the project root
# This assumes the .env file is in the project root directory
load_dotenv()


# =============================================================================
# IMPORTS
# =============================================================================
# impact_analysis.py was split into 4 sibling modules (facility_io.py,
# country_boundaries.py, precip_views.py, river_views.py); each name below is
# imported directly from whichever one actually defines it, not re-imported
# through impact_analysis.py.
#
# Full-module import of `facility_io` (alongside the specific-name imports
# below): needed so _seed_facility_failure_cache()/_new_facility_failures()
# (see their own docstrings) can read/mutate
# facility_io._FACILITY_FETCH_FAILURE_CACHE, the module-level set
# fetch_schools()/fetch_health_centers()/fetch_shelters()/fetch_wash() check
# and populate internally - always via facility_io.<name> rather than a
# `from ... import _FACILITY_FETCH_FAILURE_CACHE` binding, which would work
# but be more fragile to reason about after every fork. `pipeline_shared` is
# needed the same way for `pipeline_shared.data_store` (reset before each
# fork; see _reset_snowflake_connection_before_fork()).
import facility_io
import pipeline_shared
from pipeline_shared import admins_overlay
from impact_analysis import (
    load_envelopes_from_snowflake,
    load_gust_envelopes_from_snowflake,
    is_envelope_in_zone,
    create_views_from_envelopes_in_country,
    save_mercator_and_admin_views,
    save_json_storms,
    load_json_storms,
    patch_country_layer,
    load_mercator_view,
    load_admin_view,
    build_tiles_viewer,
    _ensure_unique_zone_ids,
    assign_facilities_to_tiles,
    project_tiles_for_facility_assignment,
    BUFFER_DISTANCE_METERS,
)
from facility_io import (
    fetch_schools,
    fetch_health_centers,
    fetch_shelters,
    fetch_wash,
    HC_FACILITY_TYPES,
)
from country_boundaries import (
    get_country_boundaries,
    get_countries_in_range_cached,
    get_initialized_admin_levels_bulk,
)
from precip_views import (
    create_precip_tile_view,
    create_precip_admin_view,
    create_precip_facility_view,
    save_precip_tile_view,
    save_precip_admin_view,
    save_precip_ratio_view,
    save_precip_ratio_admin_view,
    save_precip_school_view,
    save_precip_ratio_school_view,
    save_precip_hc_view,
    save_precip_ratio_hc_view,
    save_precip_shelter_view,
    save_precip_ratio_shelter_view,
    save_precip_wash_view,
    save_precip_ratio_wash_view,
    calculate_precip_tile_member_bitmask,
    save_precip_tile_bitmask_view,
)
from river_views import (
    create_river_tile_view,
    create_river_admin_view,
    create_river_facility_view,
    save_river_tile_view,
    river_country_tier_already_processed,
    save_river_admin_view,
    save_river_school_view,
    save_river_hc_view,
    save_river_shelter_view,
    save_river_wash_view,
    calculate_river_tile_member_bitmask,
    save_river_tile_bitmask_view,
)
from precip_utils import (
    get_latest_met_forecast,
    get_met_forecast_for_date,
    read_precip_windows,
    exceedance_probability,
    exceedance_bitmask,
    ratio_exceedance_probability,
    get_hazard_met_data_store,
)
from glofas_utils import (
    get_latest_river_forecasts_bulk,
    get_river_forecast_for_date,
    get_river_forecasts_data_store,
    download_river_forecast_parquet,
    query_country_flooded_pixels,
)

# Import gigaspatial for buffering
from gigaspatial.processing import buffer_geodataframe

import json
import tempfile
from config import config
from snowflake_utils import get_snowflake_data, get_storms_local, get_snowflake_connection, get_countries_in_range, get_countries_with_null_boundary
from country_utils import get_active_countries_from_snowflake, add_country_to_snowflake
from pipeline_runlog import (
    is_already_processed,
    log_run_start,
    log_run_complete,
    is_ambient_forecast_processed,
    log_ambient_forecast_processed,
    record_ambient_failure_attempt,
    signal_pipeline_complete,
)


# How often _release_memory() actually fires in run_precip_analysis()/
# run_river_flood_analysis()'s own per-(country, window/tier) loops (every
# Nth pass, not every single one): gc.collect() cost scales with how much is
# currently live, and calling it on every pass adds meaningful wall-clock
# overhead without meaningfully reducing peak memory, so it only fires every
# Nth pass.
RELEASE_MEMORY_EVERY_N_PASSES = 8

# Bounded worker count for _parallel_write_batch()'s per-pass Blob write
# batch, shared by precip and river-flood's own per-country save_*() calls
# (up to ~7 independent writes per pass: bitmask, per-admin-level, per-
# facility-type). A ThreadPoolExecutor throughput test against this Blob
# container plateaued around 1.6-1.7x at 8-12 workers (connection-pool/
# throttling limited, not near-linear); 8 sits at the bottom of that plateau.
BATCH_WRITE_MAX_WORKERS = 8

# Lazily resolved once, not per call: ctypes.util.find_library('c') + CDLL is
# real, non-trivial overhead to repeat on every _release_memory() call given
# how often that's invoked (once per country). None means "not yet resolved
# for this process"; False means "resolved and unavailable" (any non-Linux
# platform, or a Linux libc without malloc_trim), in which case
# _release_memory() silently skips that step rather than retrying every time.
_libc_malloc_trim = None


def _release_memory(logger):
    """Best-effort real memory reclaim: gc.collect() plus, on Linux, a real
    libc malloc_trim(0) call.

    Exists because CPython's own refcounting/cyclic GC only frees Python
    OBJECTS: it does not make glibc's own allocator (which pymalloc/numpy/
    pandas/geopandas ultimately allocate through for anything above a small
    size threshold) return that now-free memory to the OS. Under a long,
    real production run processing many countries' worth of real GeoDataFrame/
    numpy allocations one at a time (bounded peak concurrent count, not
    bounded cumulative churn), glibc's arenas can keep retaining freed memory
    for reuse rather than releasing it, so real process RSS can keep climbing
    across a run even when nothing is genuinely leaking at the Python level.
    malloc_trim(0) asks glibc to give back whatever it safely can right now.

    Safe to call frequently: gc.collect() is always available; malloc_trim is
    resolved once and skipped silently everywhere it doesn't exist (macOS
    local dev, any libc without it) rather than raising.
    """
    gc.collect()

    global _libc_malloc_trim
    if _libc_malloc_trim is None:
        try:
            libc_path = ctypes.util.find_library('c')
            libc = ctypes.CDLL(libc_path or 'libc.so.6')
            _libc_malloc_trim = libc.malloc_trim
        except (OSError, AttributeError) as e:
            logger.debug(f"malloc_trim unavailable on this platform, skipping for the rest of this process: {e}")
            _libc_malloc_trim = False

    if _libc_malloc_trim:
        try:
            _libc_malloc_trim(0)
        except Exception as e:
            logger.debug(f"malloc_trim(0) call failed, continuing without it: {e}")


# Hard ceiling on how long one country's isolated subprocess (see
# _process_one_country_isolated() below) may run before being treated as
# stuck and killed: generous relative to every real observed per-country
# wind/gust time in this codebase's own history (single-digit minutes even
# for a MEX-scale, 400k+-tile country), but still finite so a genuinely
# wedged subprocess cannot block an entire storm cycle forever the way an
# un-timed-out hang in the driver process itself would.
COUNTRY_SUBPROCESS_TIMEOUT_S = 1800

# Serializes the actual os.fork() moment (the proc.start() call itself, never
# anything else) across every isolated-country-subprocess call site in this
# module: _process_one_country_isolated() below (wind/gust) and
# _run_country_subprocess() (precip/river).
_FORK_LOCK = threading.Lock()


def _run_country_worker(country, storm, date, zoom, gdf_envelopes, gdf_envelopes_gust,
                         admin_levels_by_country, known_failed_facilities, log_level, result_queue):
    """
    Runs create_views_from_envelopes_in_country() for ONE country inside its
    own OS process (see _process_one_country_isolated()'s own docstring for
    the full "why"). Kept as a module-level function rather than a closure,
    though 'fork' itself doesn't require that (it clones the whole process
    image via os.fork(), no __main__/import machinery needed).

    admin_levels_by_country is the same bulk-precomputed {country: [levels]}
    mapping precip/river's own isolated workers take (see run_complete_
    impact_analysis()'s own admin_levels_by_country = get_initialized_admin_
    levels_bulk() call for the full "why"): passed straight through to
    create_views_from_envelopes_in_country() so that function's own
    admin_views/ directory listing happens ONCE for the whole run instead of
    once per forked country.

    known_failed_facilities: see _seed_facility_failure_
    cache()'s docstring for the full "why". Puts (result, new_failures) as
    the "ok" payload instead of just result, so the caller can fold
    new_failures into its own running set for the next country's fork.
    """
    # Under 'fork' this process already has the parent's logging handlers
    # (they were configured before the fork, and fork copies the whole
    # process state), so this call is a light, idempotent top-up rather
    # than the from-scratch setup a 'spawn' child would need; kept anyway
    # so a country's own log lines are clearly attributable to it.
    logging.basicConfig(level=getattr(logging, str(log_level).upper(), logging.INFO),
                         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    worker_logger = logging.getLogger(f"country_worker.{country}")
    _seed_facility_failure_cache(known_failed_facilities)
    try:
        result = create_views_from_envelopes_in_country(
            country, storm, date, gdf_envelopes, zoom, gdf_envelopes_gust=gdf_envelopes_gust,
            admin_levels_by_country=admin_levels_by_country)
        result_queue.put(("ok", (result, _new_facility_failures(known_failed_facilities))))
    except Exception as e:
        import traceback as _tb
        worker_logger.error(f"{country}: {e}")
        worker_logger.debug(_tb.format_exc())
        # Includes new_failures even on this error path: any facility fetch
        # that failed and got cached before the later exception that actually
        # killed this country's run is still real, useful knowledge for the
        # NEXT country's own fork - dropping it here would just mean that
        # next country pays the same fetch attempt (and, for a fast_fail
        # provider, the same timeout) again for no reason.
        result_queue.put(("error", (str(e), _new_facility_failures(known_failed_facilities))))


def _process_one_country_isolated(country, storm, date, gdf_envelopes, zoom, gdf_envelopes_gust,
                                   admin_levels_by_country, known_failed_facilities, log_level):
    """
    Returns ((wrote_base, files_written, gust_error), new_failures); raises
    on real failure. Same create_views_from_envelopes_in_country() call
    contract, just paired with new_failures (see
    _seed_facility_failure_cache()'s own docstring) since fork isolation
    would otherwise reset the cross-country facility-fetch-failure cache on
    every single country. Runs the actual computation in a fresh, throwaway
    'fork' subprocess instead of inline in the driver process.

    Uses 'fork', not 'spawn': the real call site (update_storms(), invoked
    directly from a Databricks notebook cell with no __main__ guard, which
    notebooks never have) would make 'spawn' re-execute that notebook's own
    top-level code to bootstrap the child, immediately re-triggering the
    very same process-start call recursively; Python's own multiprocessing
    module detects this and refuses with "An attempt has been made to start
    a new process before the current process has finished its bootstrapping
    phase". 'fork' has no re-execution/bootstrap-import step at all (it
    clones the running process's memory directly via os.fork()), so this
    failure mode cannot occur, and it is fully supported on the real
    deployment target (Linux/Databricks; the multiprocessing default only
    changed to 'spawn' on macOS, for Objective-C runtime fork-safety reasons
    that do not apply here). It also gives the same real isolation guarantee
    this depends on: a forked child's own memory allocations, however they
    grow, are never visible to the parent and are fully reclaimed by the OS
    the instant the child exits, regardless of the exact allocator mechanism.

    Why: sustained per-country memory growth in a long-lived process is
    driven by glibc allocator arena fragmentation across many
    differently-sized GeoDataFrame/numpy allocations, not by any single
    retained Python reference, connection object, or C-extension leak -
    gc.collect()/malloc_trim(0) can reduce but not fully reverse this.
    Isolating each country in its own process reclaims its entire address
    space unconditionally on exit, containing any out-of-memory kill to
    that one country instead of the whole run: it surfaces as an ordinary
    per-country error the existing try/except in run_complete_impact_
    analysis() already handles, and the rest of the storm cycle continues.
    """
    # A live non-main thread at fork time risks a post-fork deadlock: fork()
    # only clones the calling thread, so a lock another thread held (e.g. a
    # leaked _with_call_timeout()/_with_hard_timeout() watchdog) can stay
    # acquired forever in the child. Visibility check only, not a fix (those
    # watchdogs are daemon threads with a bounded timeout, so any leak is
    # transient) - makes a recurrence diagnosable instead of an unexplained hang.
    if threading.active_count() > 1:
        logging.getLogger(__name__).warning(
            f"{country}: {threading.active_count()} threads active immediately before forking "
            f"the isolated subprocess (expected 1, the main thread); a lock held by one of the "
            f"extra threads at the instant of fork could deadlock the child."
        )
    _reset_snowflake_connection_before_fork()
    ctx = multiprocessing.get_context('fork')
    result_queue = ctx.Queue()
    proc = ctx.Process(
        target=_run_country_worker,
        args=(country, storm, date, zoom, gdf_envelopes, gdf_envelopes_gust,
              admin_levels_by_country, known_failed_facilities, log_level, result_queue),
    )
    # See _FORK_LOCK's own module-level docstring for the full "why": only
    # the literal os.fork() instant is serialized, never the wait below.
    with _FORK_LOCK:
        proc.start()
    proc.join(COUNTRY_SUBPROCESS_TIMEOUT_S)

    if proc.is_alive():
        proc.terminate()
        proc.join(30)
        if proc.is_alive():
            proc.kill()
            proc.join()
        raise RuntimeError(
            f"{country}: subprocess exceeded {COUNTRY_SUBPROCESS_TIMEOUT_S}s and was terminated"
        )

    if result_queue.empty():
        # Killed before it could report anything at all, e.g. the child's
        # OWN out-of-memory kill (SIGKILL, exitcode -9) or a native-library
        # segfault. This is exactly the case this isolation is meant to
        # contain: the exit code tells us it died, but the process boundary
        # means it took nothing else down with it.
        raise RuntimeError(
            f"{country}: subprocess exited with code {proc.exitcode} before returning a result "
            f"(likely killed by the OS, e.g. an out-of-memory condition contained to this one country)"
        )

    status, payload = result_queue.get()
    if status == "error":
        error_msg, new_failures = payload
        # Folded in directly (mutating the caller's own set in place, the
        # same object it already passed into this call), since the normal
        # `known_failed_facilities |= new_failures` line at the call site
        # never runs once this raises - see _run_country_worker()'s own
        # comment on its matching error-path put() for the full "why".
        known_failed_facilities.update(new_failures)
        raise RuntimeError(error_msg)
    return payload


def _seed_facility_failure_cache(known_failed_facilities):
    """
    Each country runs in its own forked subprocess, so
    facility_io._FACILITY_FETCH_FAILURE_CACHE - a plain module-level set -
    starts empty in every child. This function re-seeds that empty cache
    with every (country, facility_type) failure already known from earlier
    countries this run, so a facility provider already known to be failing
    for a country isn't re-queried from a cold cache in each fork (see
    fetch_schools()/fetch_health_centers()/fetch_shelters()/fetch_wash()'s
    own module-level docstring comment for what this cache is for).

    The parent driver process (run_precip_analysis()/
    run_river_flood_analysis()/wind-gust's own country loop) accumulates a
    running `known_failed_facilities` set across every country's own result
    (see _new_facility_failures() below, the other half of this mechanism)
    and passes the CURRENT accumulated set into every subsequent country's
    fork as an explicit argument, seeded here at that child's own startup.
    This restores the same cross-country cache-hit behavior a single
    long-lived process would give for free, without requiring any shared
    memory or IPC beyond the result_queue payload each child already returns.
    """
    facility_io._FACILITY_FETCH_FAILURE_CACHE.update(known_failed_facilities)


def _new_facility_failures(known_failed_facilities):
    """
    The other half of the facility-failure-cache fix (see
    _seed_facility_failure_cache()'s own docstring for the full "why"):
    returns the (country, facility_type)
    entries THIS worker's own call added to the shared failure cache, beyond
    whatever it was seeded with at startup, so the parent can fold them into
    its own running `known_failed_facilities` set and pass the larger set
    forward into every later country's fork.
    """
    return facility_io._FACILITY_FETCH_FAILURE_CACHE - known_failed_facilities


def _reset_snowflake_connection_before_fork():
    """
    Under DATA_PIPELINE_DB=SNOWFLAKE, pipeline_shared.data_store is a
    module-level SnowflakeDataStore singleton that lazily creates and caches
    a live connection on its own `_connection` attribute the first time any
    operation needs one. Under 'fork', a child process clones the parent's
    memory at the instant of fork, including that already-open connection
    object: with no reset, every forked child inherits and reuses the SAME
    live Snowflake session/socket the parent (and every prior sibling child)
    already used. Real failure modes from sharing one session across several
    independent processes: a session-token invalidation from one child's own
    traffic affecting a later child's or the parent's subsequent use of the
    same connection, or an unexpected connection reset/retry.

    Resetting `_connection` to None here, immediately before every fork,
    means the child creates and caches its own fresh connection the first
    time it calls anything on data_store - the "each process gets its own
    connection" guarantee SnowflakeDataStore's own docstring claims (true
    under 'spawn', not under 'fork', which this repo must use instead, see
    _process_one_country_isolated()'s own docstring for why). The parent, if
    it ever needs data_store again later in the same run, transparently
    reconnects on its own next use.

    A no-op under DATA_PIPELINE_DB=BLOB/LOCAL: ADLSDataStore/LocalDataStore
    have no `_connection` attribute at all, so the getattr default below
    simply finds nothing to reset.
    """
    if getattr(pipeline_shared.data_store, '_connection', None) is not None:
        pipeline_shared.data_store._connection = None


def _run_country_subprocess(worker_target, worker_args, country, hazard_label, log_level, known_failed_facilities):
    """
    Generic fork-based per-country subprocess isolation, shared by precip's
    and river-flood's own isolation wrappers below
    (_process_one_country_precip_isolated / _process_one_country_river_isolated).
    Factored out so those two don't each duplicate the exact same
    timeout/queue/error-propagation boilerplate _process_one_country_isolated()
    implements for wind/gust; wind/gust's own functions are kept separate
    rather than refactored onto this shared helper.

    Same 'fork' (never 'spawn') requirement and reasoning as
    _process_one_country_isolated() above: these functions are reached from
    the same notebook-cell-with-no-__main__-guard call path.

    worker_target must be a module-level function accepting
    (*worker_args, log_level, result_queue) and putting ("ok", payload) or
    ("error", (str(e), new_failures)) onto result_queue exactly once.
    known_failed_facilities is the SAME set object already embedded in
    worker_args (passed again here, explicitly, so this function can fold a
    genuine error path's own new_failures into it in place before raising -
    see the "error" branch below). Returns payload on a normal completion;
    raises RuntimeError if the subprocess timed out, was killed by the OS
    before it could report anything (e.g. an out-of-memory condition
    contained to this one country), or reported a real internal error.
    """
    # See _process_one_country_isolated()'s identical check above for the
    # general "why" - visibility only, not a fix. For precip specifically,
    # read_precip_windows()'s Zarr open leaves a daemon 'zarr_io' thread plus
    # a 10-thread asyncio pool resident for the rest of the process; not
    # expected to deadlock (the isolated child never touches zarr/fsspec/
    # asyncio itself) but present at every country fork, not just the first.
    if threading.active_count() > 1:
        logging.getLogger(__name__).warning(
            f"{hazard_label} {country}: {threading.active_count()} threads active immediately before "
            f"forking the isolated subprocess (expected 1, the main thread); a lock held by one of the "
            f"extra threads at the instant of fork could deadlock the child."
        )
    _reset_snowflake_connection_before_fork()
    ctx = multiprocessing.get_context('fork')
    result_queue = ctx.Queue()
    proc = ctx.Process(
        target=worker_target,
        args=(*worker_args, log_level, result_queue),
    )
    # See _FORK_LOCK's own module-level docstring for the full "why": only
    # the literal os.fork() instant is serialized, never the wait below. This
    # is the real fix target - river-flood's own ThreadPoolExecutor(max_
    # workers=RIVER_COUNTRY_CONCURRENCY=2) is the one caller that can reach
    # this line from two threads at once; precip's own caller is a plain
    # sequential loop, so this lock is always uncontended for it.
    with _FORK_LOCK:
        proc.start()
    proc.join(COUNTRY_SUBPROCESS_TIMEOUT_S)

    if proc.is_alive():
        proc.terminate()
        proc.join(30)
        if proc.is_alive():
            proc.kill()
            proc.join()
        raise RuntimeError(
            f"{hazard_label} {country}: subprocess exceeded {COUNTRY_SUBPROCESS_TIMEOUT_S}s and was terminated"
        )

    if result_queue.empty():
        raise RuntimeError(
            f"{hazard_label} {country}: subprocess exited with code {proc.exitcode} before returning a result "
            f"(likely killed by the OS, e.g. an out-of-memory condition contained to this one country)"
        )

    status, payload = result_queue.get()
    if status == "error":
        error_msg, new_failures = payload
        # See _process_one_country_isolated()'s own matching comment on its
        # identical fold-in-before-raise pattern.
        known_failed_facilities.update(new_failures)
        raise RuntimeError(error_msg)
    return payload


def _run_country_worker_precip(country, zoom, forecast_time_str, tp_windows, ro_windows,
                                tp_probability_grids, tp_bitmask_grids, ratio_probability_grids,
                                admin_levels_by_country, known_failed_facilities, log_level, result_queue):
    """
    Runs run_precip_analysis()'s own per-country tile/admin/facility setup
    plus the full window x threshold view-creation-and-write loop for ONE
    country, inside its own OS process: runs in its own process for the same
    memory-isolation reason as wind/gust's per-country loop (see
    _process_one_country_isolated()). Every shared, country-independent
    precomputed value run_precip_analysis()'s
    own country loop needs (tp_windows, ro_windows, tp_probability_grids,
    tp_bitmask_grids, ratio_probability_grids, admin_levels_by_country) is
    passed in explicitly, mirroring wind/gust's own gdf_envelopes/
    gdf_envelopes_gust, rather than relied on implicitly via closure.
    known_failed_facilities: see _seed_facility_failure_
    cache()'s docstring for the full "why".

    Puts ("ok", ({"skipped": bool, "files_written": int}, new_failures)) onto
    result_queue on a normal completion (skipped=True only when this
    country's own mercator tile view could not be loaded at all, so this
    country is skipped entirely for this cycle), or ("error", (str(e),
    new_failures)) on a genuinely unexpected failure this function's own
    internal per-window try/except blocks did not already contain.
    """
    logging.basicConfig(level=getattr(logging, str(log_level).upper(), logging.INFO),
                         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    worker_logger = logging.getLogger(f"country_worker.precip.{country}")
    _seed_facility_failure_cache(known_failed_facilities)
    try:
        files_written = 0

        try:
            gdf_tiles = load_mercator_view(country, zoom)
        except Exception as e:
            worker_logger.error(f"Could not load mercator tiles for {country}, skipping precip for this country entirely this cycle: {e}")
            result_queue.put(("ok", ({"skipped": True, "files_written": 0}, _new_facility_failures(known_failed_facilities))))
            return

        gdf_admin_by_level = {}
        gdf_tiles_for_admin_by_level = {}
        try:
            admin_levels = admin_levels_by_country.get(country) or [1]
            for admin_level in admin_levels:
                try:
                    gdf_admin = load_admin_view(country, admin_level=admin_level)
                except Exception as e:
                    worker_logger.warning(f"Could not load admin{admin_level} view for {country}, skipping this level: {e}")
                    continue
                if admin_level == 1:
                    gdf_tiles_for_admin = gdf_tiles
                else:
                    gdf_admin_boundaries = gdf_admin[['tile_id', 'geometry']].rename(columns={'tile_id': 'id'})
                    gdf_tiles_for_admin = admins_overlay(gdf_admin_boundaries,
                                                         gdf_tiles.drop(columns=['id'], errors='ignore'))
                gdf_admin_by_level[admin_level] = gdf_admin
                gdf_tiles_for_admin_by_level[admin_level] = gdf_tiles_for_admin
        except Exception as e:
            worker_logger.warning(f"Admin-level setup failed for {country}, proceeding with tile-level views only: {e}")
            gdf_admin_by_level = {}
            gdf_tiles_for_admin_by_level = {}

        # One GeometryBasedZonalViewGenerator per distinct tile GeoDataFrame
        # used this pass (whole-country gdf_tiles, plus each admin level's own
        # gdf_tiles_for_admin when it differs), built ONCE and reused via
        # `tiles_viewer` by every create_precip_tile_view()/create_precip_
        # admin_view() call below (~20-24 per country) instead of each
        # reconstructing one for unchanging input. Keyed on id() (GeoDataFrames
        # are unhashable); safe since every value here stays alive for the
        # whole call. See build_tiles_viewer()'s docstring for the real cost.
        _tiles_viewer_by_id = {id(gdf_tiles): build_tiles_viewer(gdf_tiles)}
        for _admin_tiles in gdf_tiles_for_admin_by_level.values():
            if id(_admin_tiles) not in _tiles_viewer_by_id:
                _tiles_viewer_by_id[id(_admin_tiles)] = build_tiles_viewer(_admin_tiles)

        gdf_schools = gdf_hcs = gdf_shelters = gdf_wash = gpd.GeoDataFrame()
        school_tile_map = hc_tile_map = shelter_tile_map = wash_tile_map = pd.DataFrame()

        def _fetch_schools_and_map():
            gdf = _ensure_unique_zone_ids(fetch_schools(country, rewrite=0, fast_fail=True), 'school_id_giga', 'school')
            tile_map = assign_facilities_to_tiles(gdf, gdf_tiles, 'school_id_giga') if not gdf.empty else pd.DataFrame()
            return gdf, tile_map

        def _fetch_hcs_and_map():
            gdf_hcs_raw = fetch_health_centers(country, rewrite=0, fast_fail=True)
            if not gdf_hcs_raw.empty:
                mask = pd.Series(False, index=gdf_hcs_raw.index)
                for col, values in HC_FACILITY_TYPES.items():
                    if col in gdf_hcs_raw.columns:
                        mask |= gdf_hcs_raw[col].isin(values)
                gdf_hcs_raw = gdf_hcs_raw[mask].copy()
            gdf = _ensure_unique_zone_ids(gdf_hcs_raw, 'osm_id', 'health center')
            tile_map = assign_facilities_to_tiles(gdf, gdf_tiles, 'osm_id') if not gdf.empty else pd.DataFrame()
            return gdf, tile_map

        def _fetch_shelters_and_map():
            gdf = _ensure_unique_zone_ids(fetch_shelters(country, rewrite=0, fast_fail=True), 'osm_id', 'shelter')
            tile_map = assign_facilities_to_tiles(gdf, gdf_tiles, 'osm_id') if not gdf.empty else pd.DataFrame()
            return gdf, tile_map

        def _fetch_wash_and_map():
            gdf = _ensure_unique_zone_ids(fetch_wash(country, rewrite=0, fast_fail=True), 'osm_id', 'WASH facility')
            tile_map = assign_facilities_to_tiles(gdf, gdf_tiles, 'osm_id') if not gdf.empty else pd.DataFrame()
            return gdf, tile_map

        try:
            # Concurrent, not sequential (same fix as run_river_flood_
            # analysis()'s equivalent block): a small country hitting several
            # slow Overpass/HealthSites timeouts sequentially can cost more
            # wall-clock time than a huge country with fast responses (e.g. a
            # 145-tile country outlasting a 402,379-tile one). Concurrent fetch
            # bounds the cost by the slowest single fetch, not their sum.
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_schools = executor.submit(_fetch_schools_and_map)
                future_hcs = executor.submit(_fetch_hcs_and_map)
                future_shelters = executor.submit(_fetch_shelters_and_map)
                future_wash = executor.submit(_fetch_wash_and_map)
                gdf_schools, school_tile_map = future_schools.result()
                gdf_hcs, hc_tile_map = future_hcs.result()
                gdf_shelters, shelter_tile_map = future_shelters.result()
                gdf_wash, wash_tile_map = future_wash.result()
        except Exception as e:
            worker_logger.warning(f"Facility fetch failed for {country}, proceeding without facility-level precip views: {e}")
            gdf_schools = gdf_hcs = gdf_shelters = gdf_wash = gpd.GeoDataFrame()
            school_tile_map = hc_tile_map = shelter_tile_map = wash_tile_map = pd.DataFrame()

        for window_h in PRECIP_WINDOWS_H:
            # tp_windows/ro_windows hold bounds/member-numbers only (the raw
            # per-member grid was stripped once, in the parent, right after
            # the precompute block that's its only real consumer, see that
            # stripping's own comment for the full "why"); a tp_result/
            # ro_result being present at all is the "this window's real data
            # was available" signal, since read_precip_windows() only ever
            # inserts a window key on success.
            tp_bounds = None
            tp_result = tp_windows.get(window_h)
            if tp_result is not None:
                _, lat_min, lat_max, lon_min, lon_max = tp_result
                tp_bounds = (lat_min, lat_max, lon_min, lon_max)
            ro_result = ro_windows.get(window_h)

            if tp_result is not None:
                for threshold_mm in PRECIP_TP_THRESHOLDS_MM[window_h]:
                    probability_grid = tp_probability_grids.get((window_h, threshold_mm))
                    if probability_grid is None:
                        continue
                    try:
                        tile_view = create_precip_tile_view(gdf_tiles, grid_2d=probability_grid, bounds=tp_bounds,
                                                             tiles_viewer=_tiles_viewer_by_id[id(gdf_tiles)])
                        # Written synchronously as a gate, not queued with the
                        # rest of this pass's writes: it's the largest payload
                        # (up to ~400k rows at MEX scale, most likely to hit a
                        # Blob timeout) and the one file the dashboard's cross-
                        # hazard union at the z14-tile level depends on. A
                        # failure here raises (caught below) and skips the
                        # remaining writes, rather than letting admin/facility
                        # views for a never-written tile land and silently
                        # vanish this hazard from the combined total.
                        save_precip_tile_view(tile_view, country, forecast_time_str, threshold_mm, window_h)
                        files_written += 1
                        write_tasks = []

                        bitmask_grid = tp_bitmask_grids.get((window_h, threshold_mm))
                        if bitmask_grid is not None:
                            bitmask_view = calculate_precip_tile_member_bitmask(gdf_tiles, bitmask_grid, *tp_bounds)
                            if not bitmask_view.empty:
                                write_tasks.append(lambda bv=bitmask_view: save_precip_tile_bitmask_view(bv, country, forecast_time_str, threshold_mm, window_h))

                        for admin_level in gdf_admin_by_level:
                            admin_tiles = gdf_tiles_for_admin_by_level[admin_level]
                            admin_view = create_precip_admin_view(
                                gdf_admin_by_level[admin_level], admin_tiles,
                                precomputed_tile_view=(tile_view if admin_tiles is gdf_tiles else None),
                                grid_2d=probability_grid, bounds=tp_bounds,
                                tiles_viewer=_tiles_viewer_by_id[id(admin_tiles)]
                            )
                            write_tasks.append(lambda av=admin_view, al=admin_level: save_precip_admin_view(
                                av, country, forecast_time_str, threshold_mm, window_h, admin_level=al))

                        if not gdf_schools.empty:
                            school_view = create_precip_facility_view(gdf_schools, school_tile_map, tile_view, 'school_id_giga')
                            write_tasks.append(lambda sv=school_view: save_precip_school_view(sv, country, forecast_time_str, threshold_mm, window_h))
                        if not gdf_hcs.empty:
                            hc_view = create_precip_facility_view(gdf_hcs, hc_tile_map, tile_view, 'osm_id')
                            write_tasks.append(lambda hv=hc_view: save_precip_hc_view(hv, country, forecast_time_str, threshold_mm, window_h))
                        if not gdf_shelters.empty:
                            shelter_view = create_precip_facility_view(gdf_shelters, shelter_tile_map, tile_view, 'osm_id')
                            write_tasks.append(lambda shv=shelter_view: save_precip_shelter_view(shv, country, forecast_time_str, threshold_mm, window_h))
                        if not gdf_wash.empty:
                            wash_view = create_precip_facility_view(gdf_wash, wash_tile_map, tile_view, 'osm_id')
                            write_tasks.append(lambda wv=wash_view: save_precip_wash_view(wv, country, forecast_time_str, threshold_mm, window_h))

                        succeeded, first_exc = _parallel_write_batch(write_tasks, worker_logger)
                        files_written += succeeded
                        if first_exc is not None:
                            raise first_exc
                    except Exception as e:
                        worker_logger.warning(f"tp view failed for {country}/{window_h}h/{threshold_mm}mm: {e}")

            if tp_result is not None and ro_result is not None:
                for ratio_threshold in RATIO_THRESHOLDS:
                    probability_grid = ratio_probability_grids.get((window_h, ratio_threshold))
                    if probability_grid is None:
                        continue
                    try:
                        tile_view = create_precip_tile_view(gdf_tiles, grid_2d=probability_grid, bounds=tp_bounds,
                                                             tiles_viewer=_tiles_viewer_by_id[id(gdf_tiles)])
                        # Synchronous gate write, same reasoning as the tp
                        # loop's own tile-view write above.
                        save_precip_ratio_view(tile_view, country, forecast_time_str, ratio_threshold, window_h)
                        files_written += 1
                        write_tasks = []
                        for admin_level in gdf_admin_by_level:
                            admin_tiles = gdf_tiles_for_admin_by_level[admin_level]
                            admin_view = create_precip_admin_view(
                                gdf_admin_by_level[admin_level], admin_tiles,
                                precomputed_tile_view=(tile_view if admin_tiles is gdf_tiles else None),
                                grid_2d=probability_grid, bounds=tp_bounds,
                                tiles_viewer=_tiles_viewer_by_id[id(admin_tiles)]
                            )
                            write_tasks.append(lambda av=admin_view, al=admin_level: save_precip_ratio_admin_view(
                                av, country, forecast_time_str, ratio_threshold, window_h, admin_level=al))

                        if not gdf_schools.empty:
                            school_view = create_precip_facility_view(gdf_schools, school_tile_map, tile_view, 'school_id_giga')
                            write_tasks.append(lambda sv=school_view: save_precip_ratio_school_view(sv, country, forecast_time_str, ratio_threshold, window_h))
                        if not gdf_hcs.empty:
                            hc_view = create_precip_facility_view(gdf_hcs, hc_tile_map, tile_view, 'osm_id')
                            write_tasks.append(lambda hv=hc_view: save_precip_ratio_hc_view(hv, country, forecast_time_str, ratio_threshold, window_h))
                        if not gdf_shelters.empty:
                            shelter_view = create_precip_facility_view(gdf_shelters, shelter_tile_map, tile_view, 'osm_id')
                            write_tasks.append(lambda shv=shelter_view: save_precip_ratio_shelter_view(shv, country, forecast_time_str, ratio_threshold, window_h))
                        if not gdf_wash.empty:
                            wash_view = create_precip_facility_view(gdf_wash, wash_tile_map, tile_view, 'osm_id')
                            write_tasks.append(lambda wv=wash_view: save_precip_ratio_wash_view(wv, country, forecast_time_str, ratio_threshold, window_h))

                        succeeded, first_exc = _parallel_write_batch(write_tasks, worker_logger)
                        files_written += succeeded
                        if first_exc is not None:
                            raise first_exc
                    except Exception as e:
                        worker_logger.warning(f"ro/tp ratio view failed for {country}/{window_h}h/{ratio_threshold}: {e}")

        result_queue.put(("ok", ({"skipped": False, "files_written": files_written}, _new_facility_failures(known_failed_facilities))))
    except Exception as e:
        import traceback as _tb
        worker_logger.error(f"{country}: {e}")
        worker_logger.debug(_tb.format_exc())
        # See _run_country_worker()'s own matching comment: new_failures is
        # included even on this error path, so a facility-fetch failure
        # discovered before the later exception that actually killed this
        # country isn't silently lost for the next country's own fork.
        result_queue.put(("error", (str(e), _new_facility_failures(known_failed_facilities))))


def _process_one_country_precip_isolated(country, zoom, forecast_time_str, tp_windows, ro_windows,
                                          tp_probability_grids, tp_bitmask_grids, ratio_probability_grids,
                                          admin_levels_by_country, known_failed_facilities, log_level):
    """
    Returns ({"skipped": bool, "files_written": int}, new_failures) - see
    _run_country_worker_precip()'s own docstring for the new_failures half.
    Same call contract as running run_precip_analysis()'s own per-country
    block directly (returns {"skipped": bool, "files_written": int}), but
    executed in a fresh, throwaway 'fork' subprocess instead of inline in
    the driver process. Same isolation rationale as wind/gust's
    _process_one_country_isolated(): a country's own setup and
    view-creation loop can accumulate enough memory across a long run to
    require isolating it in its own process.
    """
    return _run_country_subprocess(
        _run_country_worker_precip,
        (country, zoom, forecast_time_str, tp_windows, ro_windows, tp_probability_grids,
         tp_bitmask_grids, ratio_probability_grids, admin_levels_by_country, known_failed_facilities),
        country, "precip", log_level, known_failed_facilities,
    )


def _run_country_worker_river(country, zoom, tiers_to_process, local_paths, tier_rows,
                               admin_levels_by_country, known_failed_facilities, log_level, result_queue):
    """
    Runs run_river_flood_analysis()'s own per-country tile/admin/facility
    setup plus the full rp_tier x step_h view-creation-and-write loop for
    ONE country, inside its own OS process: runs inside its own OS process
    because a facility-dense/large-tile-count country's memory footprint
    must not accumulate on top of previously-processed countries in the
    same long-lived process.
    tiers_to_process/local_paths/tier_rows are the same shared,
    country-independent state run_river_flood_analysis()'s own country loop
    already computed once up front (the downloaded RIVER_FORECASTS Parquet
    local paths for this cycle and their forecast_time/is_standin metadata),
    passed in explicitly rather than relied on implicitly via closure.
    admin_levels_by_country is the same bulk-precomputed {country: [levels]}
    dict run_precip_analysis() already uses (one shared admin_views/ listing
    for the whole run instead of one get_initialized_admin_levels(country)
    call per country), passed in for the same reason.

    known_failed_facilities: see _seed_facility_failure_
    cache()'s docstring for the full "why".

    Puts ("ok", ({"skipped": bool, "files_written": int, "tier_files_written":
    {rp: int}, "budget_incomplete_tiers": set}, new_failures)) onto
    result_queue on a normal completion (skipped=True only when this
    country's own mercator tile view could not be loaded at all), or
    ("error", (str(e), new_failures)) on a genuinely unexpected failure this
    function's own internal per-tier/step_h try/except blocks did not
    already contain. tier_files_written is returned per-tier (not just a
    single total) so the caller can fold each tier's own count into its own
    tier_files_written dict, which AMBIENT_HAZARD_RUN_LOG marking below
    depends on per-tier, not just in aggregate.

    This worker's own cumulative elapsed time (from the moment it starts,
    setup included) is checked against a budget at the top of every rp/step_h
    iteration below: RIVER_COUNTRY_TIME_BUDGET_S while this country hasn't
    yet confirmed any real flooded pixels this cycle, or the much larger
    RIVER_COUNTRY_TIME_BUDGET_WITH_REAL_DATA_S from the moment it has - see
    both constants' own docstrings for the full budget mechanics. Once
    whichever budget currently applies is exceeded, all remaining tiers/steps
    for this country are skipped (logged, not silent) and this puts an "ok"
    result with whatever was already written; this is a partial success, not
    a skip/error, so `skipped` stays False and `files_written`/
    `tier_files_written` simply reflect less than the full 6x4 grid.

    `budget_incomplete_tiers` is the set of tier codes THIS country did not
    finish this cycle because the budget tripped - NOT the same thing as
    `skipped`/`skip_reason` above (a country-wide, all-tiers failure before
    any tier was even touched): a budget-truncated country can have real,
    correct output for some tiers and still need a retry for others, so the
    caller (run_river_flood_analysis()) must fold this into its OWN per-tier
    AMBIENT_HAZARD_RUN_LOG marking gate via `budget_incomplete_countries_by_tier`,
    rather than the flat skipped_countries/truly_skipped_countries list,
    which would otherwise let a budget-truncated tier get marked "processed"
    on the strength of some OTHER country's output, permanently skipping
    retry for this country's own undelivered tiers/steps.
    """
    logging.basicConfig(level=getattr(logging, str(log_level).upper(), logging.INFO),
                         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    worker_logger = logging.getLogger(f"country_worker.river.{country}")
    _worker_start_time = time.time()
    tier_files_written = {rp: 0 for rp in tiers_to_process}
    _seed_facility_failure_cache(known_failed_facilities)
    # Diagnostic watchdog (see RIVER_COUNTRY_WATCHDOG_S's own docstring for
    # the threshold and full "why"): scheduled at the start of this child's
    # real work, covering setup and the rp_tier x step_h loop - every phase a
    # stuck-fork/thread-race deadlock could freeze it in. Written to stderr,
    # not this worker's logger: a long Databricks run's stdout/stderr capture
    # keeps only a head+tail, but dump_traceback_later's C-level signal
    # handler writes directly to a raw file descriptor regardless. Cancelled
    # unconditionally in the `finally` below on every exit path so a country
    # finishing just past this threshold never produces a spurious dump.
    faulthandler.dump_traceback_later(RIVER_COUNTRY_WATCHDOG_S, file=sys.stderr)
    try:
        files_written = 0

        try:
            gdf_tiles = load_mercator_view(country, zoom)
        except Exception as e:
            worker_logger.error(f"Could not load mercator tiles for {country}, skipping river-flood for this country entirely this cycle: {e}")
            # skip_reason carries the real root-cause text back to the
            # parent's result dict for persistence in
            # AMBIENT_HAZARD_FAILURE_LOG's FAILURE_REASONS column, since a
            # forked child's own stdout is not reliably captured by the
            # scheduling environment. See record_ambient_failure_attempt()'s
            # own FAILURE_REASONS column for where this ends up.
            result_queue.put(("ok", ({"skipped": True, "skip_reason": str(e), "files_written": 0, "tier_files_written": tier_files_written, "budget_incomplete_tiers": set()}, _new_facility_failures(known_failed_facilities))))
            return

        gdf_admin_by_level = {}
        gdf_tiles_for_admin_by_level = {}
        try:
            admin_levels = admin_levels_by_country.get(country) or [1]
            for admin_level in admin_levels:
                try:
                    gdf_admin = load_admin_view(country, admin_level=admin_level)
                except Exception as e:
                    worker_logger.warning(f"Could not load admin{admin_level} view for {country}, skipping this level: {e}")
                    continue
                if admin_level == 1:
                    gdf_tiles_for_admin = gdf_tiles
                else:
                    gdf_admin_boundaries = gdf_admin[['tile_id', 'geometry']].rename(columns={'tile_id': 'id'})
                    gdf_tiles_for_admin = admins_overlay(gdf_admin_boundaries,
                                                         gdf_tiles.drop(columns=['id'], errors='ignore'))
                gdf_admin_by_level[admin_level] = gdf_admin
                gdf_tiles_for_admin_by_level[admin_level] = gdf_tiles_for_admin
        except Exception as e:
            worker_logger.warning(f"Admin-level setup failed for {country}, proceeding with tile-level river views only: {e}")
            gdf_admin_by_level = {}
            gdf_tiles_for_admin_by_level = {}

        # Built ONCE for this country's gdf_tiles and reused via `tiles_viewer`
        # by every create_river_tile_view() call below (up to 6 tiers x 4
        # leadtime steps = 24), instead of each reconstructing one for
        # unchanging input. Only gdf_tiles needs one here (unlike precip's
        # equivalent cache above): create_river_admin_view() below always
        # receives an already-computed tile_view and has no `tiles_viewer`
        # param of its own. See build_tiles_viewer()'s docstring for the real cost.
        _tiles_viewer = build_tiles_viewer(gdf_tiles)

        gdf_schools = gdf_hcs = gdf_shelters = gdf_wash = gpd.GeoDataFrame()
        school_tile_map = hc_tile_map = shelter_tile_map = wash_tile_map = pd.DataFrame()

        # Fetch-only closures: the network I/O is what benefits from running
        # concurrently below (see that block for the measured latency numbers).
        # They skip assign_facilities_to_tiles() deliberately - a GIL-releasing
        # sjoin plus ESRI:54009 reprojection for unmatched facilities - since 4
        # concurrent tile-assignment calls would spike memory for a facility-
        # dense country; tile assignment below runs sequentially instead.
        def _fetch_schools():
            return _ensure_unique_zone_ids(fetch_schools(country, rewrite=0, fast_fail=True), 'school_id_giga', 'school')

        def _fetch_hcs():
            gdf_hcs_raw = fetch_health_centers(country, rewrite=0, fast_fail=True)
            if not gdf_hcs_raw.empty:
                mask = pd.Series(False, index=gdf_hcs_raw.index)
                for col, values in HC_FACILITY_TYPES.items():
                    if col in gdf_hcs_raw.columns:
                        mask |= gdf_hcs_raw[col].isin(values)
                gdf_hcs_raw = gdf_hcs_raw[mask].copy()
            return _ensure_unique_zone_ids(gdf_hcs_raw, 'osm_id', 'health center')

        def _fetch_shelters():
            return _ensure_unique_zone_ids(fetch_shelters(country, rewrite=0, fast_fail=True), 'osm_id', 'shelter')

        def _fetch_wash():
            return _ensure_unique_zone_ids(fetch_wash(country, rewrite=0, fast_fail=True), 'osm_id', 'WASH facility')

        try:
            # Concurrent, not sequential: 4 independent network fetches
            # (measured cost: health centers took 9.85s to FAIL while
            # schools/shelters/wash each finished in ~0.25s - sequentially
            # that one slow call blocks the 3 fast ones). fast_fail=True
            # already returns empty GeoDataFrames for "no data", so any
            # unexpected exception here resets all four to empty; only the
            # fetch runs here (tile assignment is split out below).
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_schools = executor.submit(_fetch_schools)
                future_hcs = executor.submit(_fetch_hcs)
                future_shelters = executor.submit(_fetch_shelters)
                future_wash = executor.submit(_fetch_wash)
                gdf_schools = future_schools.result()
                gdf_hcs = future_hcs.result()
                gdf_shelters = future_shelters.result()
                gdf_wash = future_wash.result()
        except Exception as e:
            worker_logger.warning(f"Facility fetch failed for {country}, proceeding without facility-level river views: {e}")
            gdf_schools = gdf_hcs = gdf_shelters = gdf_wash = gpd.GeoDataFrame()

        # Tile assignment runs SEQUENTIALLY, deliberately not inside the
        # ThreadPoolExecutor above - see the fetch closures' comment above for
        # the full "why". Each call below does what a fetch+map closure would,
        # just sequentially.
        # _tiles_proj: this country's tile grid reprojected to ESRI:54009
        # ONCE and reused via `tiles_proj` by every call below, instead of up
        # to 4 redundant reprojections whenever a call has an unmatched
        # border/coastal facility - same "build once, reuse many" convention
        # as _tiles_viewer above (see project_tiles_for_facility_assignment()'s
        # docstring for the real cost). Left None (never dereferenced) when
        # this country has zero facilities of any type this cycle.
        _any_facilities = not (gdf_schools.empty and gdf_hcs.empty and gdf_shelters.empty and gdf_wash.empty)
        _tiles_proj = project_tiles_for_facility_assignment(gdf_tiles) if _any_facilities else None
        school_tile_map = assign_facilities_to_tiles(gdf_schools, gdf_tiles, 'school_id_giga', tiles_proj=_tiles_proj) if not gdf_schools.empty else pd.DataFrame()
        hc_tile_map = assign_facilities_to_tiles(gdf_hcs, gdf_tiles, 'osm_id', tiles_proj=_tiles_proj) if not gdf_hcs.empty else pd.DataFrame()
        shelter_tile_map = assign_facilities_to_tiles(gdf_shelters, gdf_tiles, 'osm_id', tiles_proj=_tiles_proj) if not gdf_shelters.empty else pd.DataFrame()
        wash_tile_map = assign_facilities_to_tiles(gdf_wash, gdf_tiles, 'osm_id', tiles_proj=_tiles_proj) if not gdf_wash.empty else pd.DataFrame()

        bounds = gdf_tiles.total_bounds

        # See RIVER_COUNTRY_TIME_BUDGET_S's/RIVER_COUNTRY_TIME_BUDGET_WITH_
        # REAL_DATA_S's docstrings for the two-tier budget design.
        # `_found_real_pixels` flips True the moment ANY tier's df_pixels
        # comes back non-empty and stays True for the rest of this country's
        # loop; `_current_river_budget()` is re-evaluated at every check below
        # rather than cached, so confirming real data mid-loop immediately
        # widens the ceiling for the rest of that tier's step_h loop and every
        # later tier. By construction the inner step_h-loop check never sees
        # `_found_real_pixels=False`: reaching it already required this
        # tier's df_pixels to be non-empty, so the flag is already set - a
        # country is never cut off mid-write for a tier already confirmed to
        # hold real flood data, only pre-confirmation.
        _found_real_pixels = False

        def _current_river_budget():
            return RIVER_COUNTRY_TIME_BUDGET_WITH_REAL_DATA_S if _found_real_pixels else RIVER_COUNTRY_TIME_BUDGET_S

        # See the same docstrings for why this short-circuits both loops via
        # the explicit `if _budget_exceeded: break` after the inner loop
        # (Python has no native break-both-loops): once tripped, no further
        # tiers/steps are attempted for this country this cycle.
        _budget_exceeded = False
        # Tier codes this country did NOT finish this cycle because the
        # budget tripped - see this function's own docstring for the full
        # "why" this is tracked and returned separately from `skipped`/
        # `skip_reason`. Populated at both trip points below; never touched
        # by a tier that completed normally (empty pixels, or the pre-
        # existing river_country_tier_already_processed() skip-gate), only
        # by an actual budget cutoff.
        budget_incomplete_tiers = set()

        for rp_idx, rp in enumerate(tiers_to_process):
            _budget = _current_river_budget()
            _elapsed = time.time() - _worker_start_time
            if _elapsed > _budget:
                _remaining_tiers = tiers_to_process[rp_idx:]
                worker_logger.warning(
                    f"{country}: cumulative river-flood processing time ({_elapsed:.1f}s) "
                    f"exceeded the {_budget}s per-country budget "
                    f"({'real flood data already confirmed this cycle' if _found_real_pixels else 'no real flood data confirmed yet'}); "
                    f"skipping remaining tier(s) {_remaining_tiers} (each x {RIVER_LEADTIME_STEPS_H} "
                    f"step(s)) for this cycle and returning partial results already written, "
                    f"rather than continuing to grind through degraded infrastructure toward "
                    f"the full {COUNTRY_SUBPROCESS_TIMEOUT_S}s subprocess timeout."
                )
                _budget_exceeded = True
                budget_incomplete_tiers.update(_remaining_tiers)
                break

            if rp not in local_paths:
                continue  # this tier's download failed above, skip it for every country
            row = tier_rows[rp]
            forecast_time = row['forecast_time']
            forecast_time_str = forecast_time.strftime('%Y%m%d%H%M%S') if hasattr(forecast_time, 'strftime') else str(forecast_time)
            is_standin = row['is_standin'] if row['is_standin'] is not None else (rp in RIVER_STANDIN_TIERS_FALLBACK)

            if river_country_tier_already_processed(country, forecast_time, rp, max(RIVER_LEADTIME_STEPS_H)):
                tier_files_written[rp] += 1
                continue

            try:
                df_pixels = query_country_flooded_pixels(local_paths[rp], bounds)
                if df_pixels.empty:
                    continue  # no flooded pixels in this country's bbox at this tier: normal, common
                gdf_pixels = gpd.GeoDataFrame(
                    df_pixels,
                    geometry=gpd.points_from_xy(df_pixels['pixel_lon'], df_pixels['pixel_lat']),
                    crs='EPSG:4326',
                )
                # Real flood data confirmed - see this loop's opening comment
                # for why this widens the budget starting with this tier's own
                # step_h loop. Set only after gdf_pixels is built successfully,
                # so a construction failure below falls through to the except
                # block without incorrectly claiming real data.
                _found_real_pixels = True
            except Exception as e:
                worker_logger.warning(f"River-flood pixel data unavailable for {country}/{rp}: {e}")
                continue

            for step_idx, step_h in enumerate(RIVER_LEADTIME_STEPS_H):
                _budget = _current_river_budget()
                _elapsed = time.time() - _worker_start_time
                if _elapsed > _budget:
                    _remaining_steps = RIVER_LEADTIME_STEPS_H[step_idx:]
                    _remaining_tiers = tiers_to_process[rp_idx + 1:]
                    worker_logger.warning(
                        f"{country}: cumulative river-flood processing time ({_elapsed:.1f}s) "
                        f"exceeded the {_budget}s per-country budget (real flood data already "
                        f"confirmed this cycle) mid-tier '{rp}'; skipping remaining step(s) "
                        f"{_remaining_steps}h for this tier and all remaining tier(s) "
                        f"{_remaining_tiers} for this cycle, returning partial results already "
                        f"written, rather than continuing to grind through degraded infrastructure "
                        f"toward the full {COUNTRY_SUBPROCESS_TIMEOUT_S}s subprocess timeout."
                    )
                    _budget_exceeded = True
                    budget_incomplete_tiers.add(rp)
                    budget_incomplete_tiers.update(_remaining_tiers)
                    break
                try:
                    gdf_pixels_step = gdf_pixels[gdf_pixels['step_h'] <= step_h]
                    if gdf_pixels_step.empty:
                        continue
                    tile_view = create_river_tile_view(gdf_tiles, gdf_pixels_step, rp, is_standin,
                                                        tiles_viewer=_tiles_viewer)
                    # Synchronous gate write, same reasoning as precip's own
                    # tile-view writes above: largest payload, the file the
                    # dashboard's combined-hazard union depends on; a failure
                    # here skips this pass's remaining writes (caught below).
                    save_river_tile_view(tile_view, country, forecast_time_str, rp, step_h)
                    files_written += 1
                    tier_files_written[rp] += 1
                    write_tasks = []

                    bitmask_view = calculate_river_tile_member_bitmask(gdf_tiles, gdf_pixels_step)
                    if not bitmask_view.empty:
                        write_tasks.append(
                            lambda bitmask_view=bitmask_view: save_river_tile_bitmask_view(bitmask_view, country, forecast_time_str, rp, step_h)
                        )

                    for admin_level, gdf_admin in gdf_admin_by_level.items():
                        admin_view = create_river_admin_view(gdf_admin, gdf_tiles_for_admin_by_level[admin_level], gdf_pixels_step, rp, is_standin, tile_view=tile_view)
                        write_tasks.append(
                            lambda admin_view=admin_view, admin_level=admin_level: save_river_admin_view(admin_view, country, forecast_time_str, rp, step_h, admin_level=admin_level)
                        )

                    _need_facility_views = (not gdf_schools.empty or not gdf_hcs.empty
                                             or not gdf_shelters.empty or not gdf_wash.empty)
                    if _need_facility_views:
                        tile_probs = tile_view.set_index('zone_id')['probability'].to_dict()
                        tile_basin = tile_view.set_index('zone_id')['below_min_basin'].to_dict()

                    if not gdf_schools.empty:
                        v = create_river_facility_view(gdf_schools, school_tile_map, tile_view, 'school_id_giga', is_standin, tile_probs=tile_probs, tile_basin=tile_basin)
                        write_tasks.append(lambda v=v: save_river_school_view(v, country, forecast_time_str, rp, step_h))
                    if not gdf_hcs.empty:
                        v = create_river_facility_view(gdf_hcs, hc_tile_map, tile_view, 'osm_id', is_standin, tile_probs=tile_probs, tile_basin=tile_basin)
                        write_tasks.append(lambda v=v: save_river_hc_view(v, country, forecast_time_str, rp, step_h))
                    if not gdf_shelters.empty:
                        v = create_river_facility_view(gdf_shelters, shelter_tile_map, tile_view, 'osm_id', is_standin, tile_probs=tile_probs, tile_basin=tile_basin)
                        write_tasks.append(lambda v=v: save_river_shelter_view(v, country, forecast_time_str, rp, step_h))
                    if not gdf_wash.empty:
                        v = create_river_facility_view(gdf_wash, wash_tile_map, tile_view, 'osm_id', is_standin, tile_probs=tile_probs, tile_basin=tile_basin)
                        write_tasks.append(lambda v=v: save_river_wash_view(v, country, forecast_time_str, rp, step_h))

                    succeeded, first_exc = _parallel_write_batch(write_tasks, worker_logger)
                    files_written += succeeded
                    tier_files_written[rp] += succeeded
                    if first_exc is not None:
                        raise first_exc
                except Exception as e:
                    worker_logger.warning(f"River-flood view failed for {country}/{rp}/{step_h}h: {e}")

            if _budget_exceeded:
                break  # outer rp loop: the inner step_h loop's own break above only exits that loop

        result_queue.put(("ok", ({"skipped": False, "files_written": files_written, "tier_files_written": tier_files_written, "budget_incomplete_tiers": budget_incomplete_tiers}, _new_facility_failures(known_failed_facilities))))
    except Exception as e:
        import traceback as _tb
        worker_logger.error(f"{country}: {e}")
        worker_logger.debug(_tb.format_exc())
        # See _run_country_worker()'s own matching comment: new_failures is
        # included even on this error path, so a facility-fetch failure
        # discovered before the later exception that actually killed this
        # country isn't silently lost for the next country's own fork.
        result_queue.put(("error", (str(e), _new_facility_failures(known_failed_facilities))))
    finally:
        # Cancels the watchdog scheduled at this function's own start on
        # EVERY exit path (the early skip-return above, a normal "ok" above,
        # or the "error" above): without this, a country that finishes
        # normally just past RIVER_COUNTRY_WATCHDOG_S would still get a
        # spurious dump moments later, for a process that already reported
        # its real result and is simply on its way out.
        faulthandler.cancel_dump_traceback_later()


def _process_one_country_river_isolated(country, zoom, tiers_to_process, local_paths, tier_rows,
                                         admin_levels_by_country, known_failed_facilities, log_level):
    """
    Returns ({"skipped": bool, "files_written": int, "tier_files_written":
    {rp: int}}, new_failures) - see _run_country_worker_river()'s own
    docstring for the new_failures half. Same underlying call
    contract as running run_river_flood_analysis()'s own per-country block
    directly, but executed in a fresh 'fork' subprocess for the same
    per-country memory-isolation reason as wind/gust's
    _process_one_country_isolated().
    """
    return _run_country_subprocess(
        _run_country_worker_river,
        (country, zoom, tiers_to_process, local_paths, tier_rows, admin_levels_by_country, known_failed_facilities),
        country, "river-flood", log_level, known_failed_facilities,
    )


# =============================================================================
# CONFIGURATION
# =============================================================================
def setup_logging(log_level="INFO"):
    """
    Setup logging configuration for the pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.
    
    Returns:
        logging.Logger: Configured logger instance.
    """
    global logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('main_pipeline.log')
        ]
    )
    logger = logging.getLogger(__name__)
    return logger


# =============================================================================
# IMPACT ANALYSIS FUNCTIONS
# =============================================================================
def run_complete_impact_analysis(storm, date, countries, logger, zoom, skip_gust=False, known_failed_facilities=None):
    """
    Complete impact analysis orchestration.

    Loads hurricane envelope data from Snowflake, checks which countries are affected
    (using 100km buffer per country), and creates impact views for affected countries.
    Admin levels are determined automatically by which base admin parquets exist for each
    country (created during --type initialize).

    Also loads gust envelope data (if available) and generates core exposure gust
    views alongside the wind views, for the same affected countries. Gust data is
    optional per storm/forecast; its absence never affects the wind results.

    Args:
        storm: Storm name (e.g., 'FUNG-WONG', 'JERRY')
        date: Forecast date in YYYYMMDDHHMMSS format (e.g., '20251110000000')
        countries: List of ISO3 country codes (e.g., ['TWN', 'DOM'])
        logger: Logger instance for logging
        zoom: Zoom level for mercator tiles (default: 14)
        skip_gust: If True, skip gust envelope processing even if gust data is
            available (wind processing is unaffected either way)
        known_failed_facilities: a mutable set of (country, facility_type)
            pairs already known to fail this pipeline run (across storms and
            across the wind/gust -> precip -> river phase sequence: see
            _seed_facility_failure_cache()'s own docstring). Mutated in place
            (via `|=`, never reassigned) so the SAME object the caller passed
            keeps accumulating. None (the default) creates a fresh,
            run-scoped set here instead.

    Returns:
        dict: Summary of analysis results with keys:
            - success (bool): Whether analysis completed successfully
            - skipped (bool): Present and True only for the expired-forecast
              early-return path, where every count below is 0/empty
            - envelopes_processed (int): Number of envelope records processed
            - countries_processed (int): Number of countries processed
            - total_views_created (int): Estimated number of views created
            - affected_countries (list): List of country codes that were affected
            - country_errors (list): Per-country error strings for countries
              that failed during this run
            - country_files_written (dict): Per-country file-write counts
            - sql_prefilter_used (bool): Whether the SQL/cache-based country
              pre-filter (vs. the live per-country Python buffer check) ran.
              Purely a "which path ran" indicator, NOT a trustworthiness
              signal: see country_impact_confirmed for that.
            - country_impact_confirmed (bool): Whether every country in
              `countries` actually got a real affected/not-affected
              determination this call, however it was obtained. False only
              when a genuine evaluation could not be completed for at least
              one requested country (missing/expired envelope data, or the
              Python buffer check couldn't fetch that country's boundary - a
              country in that state must not be treated as a confirmed
              no-impact result). Callers that mark a country terminally "no
              impact" (see log_run_complete()'s own no_impact_countries) must
              gate on THIS flag, not sql_prefilter_used, since
              sql_prefilter_used only indicates which pre-filter path ran and
              says nothing about whether the result is trustworthy enough to
              finalize as no-impact.
            - unevaluated_countries (list): The subset of `countries` this
              call could not determine affected/not-affected for (why
              country_impact_confirmed is False, when non-empty). Callers
              wanting per-country precision (rather than zeroing out an
              entire otherwise-confirmed batch) should exclude THIS list from
              whatever they'd otherwise mark no-impact, e.g.
              `no_impact_candidates - set(unevaluated_countries)`.
            - error (str): Error message if success is False
    """
    logger.info(f"Running impact analysis for {storm} at {date}")
    logger.info(f"Countries: {', '.join(countries)}")

    if known_failed_facilities is None:
        known_failed_facilities = set()

    try:
        # Load envelope data directly from Snowflake
        logger.info("Loading envelope data from Snowflake...")
        gdf_envelopes = load_envelopes_from_snowflake(storm, date)
        
        if gdf_envelopes.empty:
            logger.info(f"No envelope data found for {storm} at {date}, forecast may have expired, skipping")
            # sql_prefilter_used=False here (no spatial join ever ran): callers that
            # gate a terminal "no impact" classification on a confirmed SQL result
            # must not treat this expired-forecast case as one, since it says nothing
            # about whether these countries would actually have been affected.
            return {"success": True, "skipped": True, "envelopes_processed": 0, "countries_processed": 0,
                    "total_views_created": 0, "affected_countries": [], "country_files_written": {},
                    "sql_prefilter_used": False, "country_impact_confirmed": False,
                    "unevaluated_countries": list(countries)}
        
        logger.info(f"Loaded {len(gdf_envelopes)} envelope records")
        logger.info("Envelopes already converted to GeoDataFrame")

        # Gust envelopes are optional and independent of the wind path above.
        gdf_envelopes_gust = pd.DataFrame() if skip_gust else load_gust_envelopes_from_snowflake(storm, date)
        if not gdf_envelopes_gust.empty:
            logger.info(f"Loaded {len(gdf_envelopes_gust)} gust envelope records")

        def _python_buffer_check(candidate_countries):
            """100km buffer-per-country fallback check. Returns
            (found, unevaluated): found is the subset of candidate_countries
            whose buffered boundary intersects gdf_envelopes; unevaluated is
            the subset that could not be checked at all (boundary fetch
            failed) and must NOT be treated as a confirmed "not affected"
            result by the caller (see country_impact_confirmed on this
            function's enclosing return dict)."""
            found = []
            unevaluated = []
            country_boundaries = get_country_boundaries(candidate_countries)

            for i, country in enumerate(candidate_countries):
                country_boundary = country_boundaries[i]
                if country_boundary is None:
                    # get_country_boundaries() isolates per-country failures
                    # this country's fetch failed but the others still succeeded.
                    # Skip it rather than crash the whole run for every country.
                    # Genuinely unevaluated, not confirmed "not affected": a
                    # transient boundary-fetch failure must never be trusted
                    # as a real no-impact result.
                    logger.warning(f"  {country}: Could not retrieve boundary, skipping (not counted as affected)")
                    unevaluated.append(country)
                    continue
                country_gdf = gpd.GeoDataFrame(geometry=[country_boundary], crs='EPSG:4326')

                country_buffered = buffer_geodataframe(country_gdf, buffer_distance_meters=100000)
                country_buffered_geom = country_buffered.geometry.iloc[0]

                bounds = country_buffered_geom.bounds
                if any(not (isinstance(b, (int, float)) and -1000 < b < 1000) for b in bounds):
                    logger.debug(f"Buffer geometry for {country} has invalid bounds, attempting to fix...")
                    try:
                        country_buffered_geom = country_buffered_geom.buffer(0)
                        bounds = country_buffered_geom.bounds
                    except Exception:
                        logger.debug(f"Could not fix buffer geometry for {country}, using original boundary")
                        country_buffered_geom = country_boundary

                if not country_buffered_geom.is_valid:
                    from shapely.validation import make_valid
                    try:
                        country_buffered_geom = make_valid(country_buffered_geom)
                    except Exception:
                        try:
                            country_buffered_geom = country_buffered_geom.buffer(0)
                        except Exception:
                            logger.debug(f"Could not create valid buffered geometry for {country}, using unbuffered")
                            country_buffered_geom = country_boundary

                if is_envelope_in_zone(country_buffered_geom, gdf_envelopes):  # Python fallback path
                    found.append(country)
                    bounds = country_buffered_geom.bounds
                    if bounds[2] - bounds[0] > 180:
                        logger.info(f"  {country}: Affected (buffer crosses dateline)")
                    else:
                        logger.info(f"  {country}: Affected")
                else:
                    logger.info(f"  {country}: Not affected (skipping)")
            return found, unevaluated

        # Country pre-filter: SQL against Snowflake (HAZARD_DATA_SOURCE=
        # SNOWFLAKE, the default) or the local/Blob boundary cache otherwise.
        # The cache path exists because SQL can't tell "no row for this storm"
        # from "confirmed 0 countries in range" - it would treat every storm
        # as affecting nobody; the cache never touches TC_ENVELOPES_COMBINED,
        # avoiding that ambiguity.
        affected_countries = []
        sql_prefilter_used = False
        # Countries whose affected/not-affected status could not actually be
        # determined this call (boundary fetch failed inside the Python
        # buffer-check fallback, at any call site below). Non-empty makes
        # country_impact_confirmed False regardless of which pre-filter path
        # ran, since ANY unevaluated country makes a terminal "no impact"
        # classification unsafe for this whole call.
        unevaluated_countries = set()
        if config.HAZARD_DATA_SOURCE == 'SNOWFLAKE':
            conn_prefilter = None
            try:
                conn_prefilter = get_snowflake_connection()
                cursor_prefilter = conn_prefilter.cursor()
                sql_countries = get_countries_in_range(cursor_prefilter, storm, date)
                # Trust SQL result whether empty or not, empty means confirmed out-of-range.
                # Only fall back to Python if the query itself raises (connection/auth failure).
                affected_countries = [c for c in sql_countries if c in countries]
                if affected_countries:
                    logger.info(f"SQL pre-filter: {len(affected_countries)} country/countries in range: {', '.join(affected_countries)}")
                else:
                    logger.info("SQL pre-filter: no countries within 100km, skipping storm")

                # get_countries_in_range()'s WHERE COUNTRY_BOUNDARY IS NOT NULL
                # makes "boundary never populated" indistinguishable from
                # "confirmed out of range" - a transient GeoRepo failure during
                # --type initialize would otherwise silently exclude a country
                # from every future storm forever. Check for that gap among the
                # countries requested this run and fall back to Python for those.
                null_boundary_countries = get_countries_with_null_boundary(cursor_prefilter, countries)
                cursor_prefilter.close()
                if null_boundary_countries:
                    logger.warning(
                        f"COUNTRY_BOUNDARY not populated for {', '.join(null_boundary_countries)}: "
                        f"SQL pre-filter cannot evaluate these, running Python buffer check for them"
                    )
                    fallback_found, fallback_unevaluated = _python_buffer_check(null_boundary_countries)
                    affected_countries = list(dict.fromkeys(affected_countries + fallback_found))
                    unevaluated_countries.update(fallback_unevaluated)

                # Latched here, LAST in this try block, not right after the
                # first query: get_countries_with_null_boundary() (and the
                # _python_buffer_check() it can trigger) run in this same try,
                # so if either raises, sql_prefilter_used stays False and falls
                # through to the full Python buffer check below. Setting it
                # right after the first query would let a second-query failure
                # be silently swallowed, leaving those countries unevaluated
                # while reporting the whole pre-filter as having succeeded.
                sql_prefilter_used = True
            except Exception as e:
                logger.warning(f"SQL pre-filter failed ({e}), falling back to Python buffer check")
            finally:
                if conn_prefilter is not None:
                    conn_prefilter.close()
        else:
            cached_countries = get_countries_in_range_cached(gdf_envelopes)
            # None (cache unusable) vs [] (confirmed zero in range) must be distinguished here,
            # same as sql_countries above never gates on truthiness: `if cached_countries:` would
            # be falsy for BOTH None and [], silently discarding the common "confirmed 0 countries"
            # result and defeating the whole point of the cache (falls through to a live per-country
            # GeoRepo fetch below on exactly its most common outcome).
            if cached_countries is not None:
                affected_countries = [c for c in cached_countries if c in countries]
                sql_prefilter_used = True  # cached result is trusted the same way, skip the Python fallback below
                if affected_countries:
                    logger.info(f"Cached boundary pre-filter: {len(affected_countries)} country/countries in range: {', '.join(affected_countries)}")
                else:
                    logger.info("Cached boundary pre-filter: no countries within 100km, skipping storm")
            else:
                logger.warning("Boundary cache unavailable or empty; falling back to live per-country Python buffer check")

        # Python fallback: 100km buffer per country (only when the SQL
        # pre-filter query itself failed outright; the NULL-boundary gap above
        # is handled separately, per-country, while the SQL result is still trusted)
        if not sql_prefilter_used:
            logger.info("Checking which countries are affected (100km buffer per country)...")
            affected_countries, fallback_unevaluated = _python_buffer_check(countries)
            unevaluated_countries.update(fallback_unevaluated)

        # True only if every requested country actually got a real
        # affected/not-affected determination this call (see this function's
        # own docstring for country_impact_confirmed). Used by callers to
        # gate a terminal "no impact" classification instead of
        # sql_prefilter_used, which only tracks which path ran, not whether
        # the result is trustworthy.
        country_impact_confirmed = not unevaluated_countries

        if not affected_countries:
            logger.info("Envelopes do not intersect with any of the specified countries (within 100km buffer), skipping")
            return {"success": True, "skipped": True, "envelopes_processed": 0, "countries_processed": 0,
                    "total_views_created": 0, "affected_countries": [], "country_files_written": {},
                    "sql_prefilter_used": sql_prefilter_used,
                    "country_impact_confirmed": country_impact_confirmed,
                    "unevaluated_countries": sorted(unevaluated_countries)}

        logger.info(f"Processing {len(affected_countries)} affected country/countries: {', '.join(affected_countries)}")

        # Admin levels listed ONCE across every affected country, not per
        # country: get_initialized_admin_levels() lists the WHOLE shared
        # admin_views/ directory and filters client-side by country prefix,
        # so calling it per country (each isolated in its own fork) re-lists
        # that directory redundantly. get_initialized_admin_levels_bulk()
        # does that listing once, reused via a dict lookup (same fix as
        # run_precip_analysis's/run_river_flood_analysis's own admin_levels_by_country).
        admin_levels_by_country = get_initialized_admin_levels_bulk(countries=affected_countries)

        # Create impact views only for affected countries
        logger.info("Creating impact views for affected countries...")
        total_views = 0
        country_errors = []
        succeeded_countries = []
        # Per-country file count, not just the summed total: lets callers distinguish
        # a country that genuinely wrote zero real output (candidate for a terminal
        # "no impact" classification) from one that wrote real files, without having
        # to re-derive this from MAT-table state.
        country_files_written = {}
        any_base_parquet_written = False
        _log_level_name = logging.getLevelName(logger.getEffectiveLevel())
        # known_failed_facilities is now a parameter (see this function's own
        # docstring for the full "why"), not a fresh set here: it must
        # persist and keep accumulating across storms and phases, not reset
        # per storm the way a local declaration here would.
        for country in affected_countries:
            try:
                (wrote_base, files_written_for_country, gust_error), new_failures = _process_one_country_isolated(
                    country, storm, date, gdf_envelopes, zoom, gdf_envelopes_gust,
                    admin_levels_by_country, known_failed_facilities, _log_level_name)
                known_failed_facilities |= new_failures
                if wrote_base:
                    any_base_parquet_written = True
                total_views += files_written_for_country
                country_files_written[country] = files_written_for_country
                succeeded_countries.append(country)
                # Gust failures are isolated inside create_views_from_envelopes_in_country()
                # so a gust-only error never discards the wind results above (this country
                # still counts as succeeded, files_written still reflects the real wind
                # output), but it must still surface into country_errors so it reaches
                # TC_PIPELINE_RUN_LOG's error_message even though STATUS stays SUCCESS,
                # rather than only ever existing in the container's own stdout log.
                if gust_error:
                    country_errors.append(f"{country} gust: {gust_error}")
            except Exception as country_exc:
                import traceback as _tb
                logger.error(f"Pipeline with errors for storm {storm} at {date}")
                logger.error(f"  {country}: {str(country_exc)}")
                logger.debug(_tb.format_exc())
                country_errors.append(f"{country}: {str(country_exc)}")
            finally:
                # Per-country memory isolation happens inside
                # _process_one_country_isolated() itself (its own OS process,
                # reclaimed on exit); this call is a cheap safety net for
                # whatever this driver process itself accumulates. Kept
                # unconditional (no modulo gating like precip/river) since a
                # storm cycle typically reaches only a handful of countries.
                # In `finally` so a raising country still releases first.
                _release_memory(logger)

        if country_errors and not succeeded_countries:
            # Every country failed: treat as full failure so the run stays eligible for retry
            return {"success": False, "error": "; ".join(country_errors)}

        if country_errors:
            logger.warning(f"Impact analysis completed with {len(country_errors)} country error(s): {'; '.join(country_errors)}")
        else:
            logger.info("Impact analysis completed successfully")

        # If any emergency fallback wrote a base parquet during this update run,
        # refresh the base layer MATs immediately (they are normally only refreshed
        # after --type initialize or --type patch).
        if any_base_parquet_written and os.environ.get("DATA_PIPELINE_DB", "LOCAL").upper() == "SNOWFLAKE":
            _conn = None
            try:
                _conn = get_snowflake_connection()
                _cur = _conn.cursor()
                _cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS REFRESH")
                _cur.execute("CALL AOTS.TC_ECMWF.REFRESH_BASE_LAYER_TABLES()")
                _result = _cur.fetchone()[0]
                _cur.close()
                if _result.startswith('PARTIAL') or 'errors:' in _result:
                    logger.warning(f"Base layer MAT refresh had failures after emergency fallback: {_result}")
                else:
                    logger.info(f"Base layer MATs refreshed after emergency fallback during update: {_result}")
            except Exception as e:
                logger.error(f"Could not refresh base layer tables after emergency fallback: {e}")
            finally:
                if _conn is not None:
                    _conn.close()

        return {
            "success": True,
            "envelopes_processed": len(gdf_envelopes),
            "countries_processed": len(succeeded_countries),
            "total_views_created": total_views,
            "affected_countries": succeeded_countries,
            "country_errors": country_errors,
            "country_files_written": country_files_written,
            "sql_prefilter_used": sql_prefilter_used,
            "country_impact_confirmed": country_impact_confirmed,
            "unevaluated_countries": sorted(unevaluated_countries),
        }

    except Exception as e:
        import traceback
        logger.error(f"Error during impact analysis: {str(e)}")
        logger.error(traceback.format_exc())
        return {"success": False, "error": str(e)}


# =============================================================================
# PRECIPITATION/RUNOFF ANALYSIS (storm-independent)
# =============================================================================

def _countries_cover_full_active_list(countries, logger):
    """
    True only if `countries` genuinely covers every currently-active country
    (a real superset of get_active_countries_from_snowflake()'s own result),
    False otherwise, INCLUDING when the active-country lookup itself fails.

    run_precip_analysis()/run_river_flood_analysis() mark a whole forecast
    cycle processed in AMBIENT_HAZARD_RUN_LOG based on whichever `countries`
    list the caller happens to pass, with no way to tell "the real, full
    active list" from "a deliberately narrow one" from inside the function
    itself. Every real production caller always passes the full
    active-country list, so gating marking on this check costs production
    nothing; a caller that passes only a handful of countries (a targeted
    retry or manual investigation) must not silently mark the ENTIRE cycle
    done for every OTHER country too, none of which were actually attempted.

    Fails CLOSED, not open, unlike this file's general "a bookkeeping hiccup
    must never block real processing" philosophy elsewhere: if
    get_active_countries_from_snowflake() itself fails (or genuinely returns
    zero countries), this function cannot verify the caller's list is really
    a full run, so it says False rather than guessing. A wrongful False just
    costs one redundant reprocess next cycle; a wrongful True would silently
    and permanently mark countries this run never touched as processed.
    """
    active_countries = get_active_countries_from_snowflake()
    if not active_countries:
        logger.warning(
            "Could not verify the real active-country list (empty result, likely a query "
            "failure); treating this run's own countries list as NOT a full run for marking "
            "purposes, so this cycle stays eligible for a genuine full run to mark it later."
        )
        return False
    return set(countries) >= set(active_countries)

# Four countable-impact windows, hours from T+0. 6h is the standard
# flash-flood-response window; 24h short-term outlook; 72h "next few days"
# (close to GloFAS's own first Flood Summary tier); 120h (5 days) matches
# the OCHA ROWCA reference used to anchor the tp thresholds below.
PRECIP_WINDOWS_H = [6, 24, 72, 120]

# tp exceedance thresholds (mm), per window: moderate/heavy/extreme.
# Heavy@6h (50mm) is the UK Extreme Rainfall Alert figure; the whole 120h row
# is OCHA ROWCA's own moderate/heavy/extreme scale. Both real, independently
# verifiable anchors. Every other cell is a power-law fit (E = alpha * D^beta)
# sharing one beta=0.2313 (fit from the only tier with two real anchors,
# heavy), scaled per tier from its own real 120h anchor.
PRECIP_TP_THRESHOLDS_MM = {
    6:   [25, 50, 75],
    24:  [35, 70, 103],
    72:  [45, 90, 133],
    120: [50, 100, 150],
}

# ro/tp ratio exceedance tiers: dimensionless cut points on the ro/tp ratio,
# not a fixed mm threshold (no real flood-forecasting system uses one, see
# WMO's Global Flash Flood Guidance System). Grounded in the Rational Method
# runoff coefficient (C) reference table (civil-engineering stormwater
# design standard): 0.3 sits at the entry point of "meaningfully elevated"
# runoff response, 0.6 at "majority of the rain becomes runoff".
RATIO_THRESHOLDS = [0.3, 0.6]

# Below this accumulated tp, a member's ro/tp ratio is treated as 0
# (negligible rain means negligible runoff-response signal from that
# member, not an excluded observation).
RATIO_MIN_TP_MM = 5.0

# MET_FORECASTS refreshes 4x/day. A "latest" cycle older than
# this indicates the upstream TC-ECMWF-Forecast-Pipeline has stalled. Only
# checked in "latest" mode (target_date=None); an explicit historical backfill
# is expected to be old and should not warn.
MET_FORECAST_STALE_HOURS = 24


def run_precip_analysis(countries, logger, zoom=14, target_date=None, known_failed_facilities=None):
    """
    Storm-independent precipitation/runoff analysis, run once per --type
    update invocation, not once per storm. Reads the tp/ro Zarr forecast
    from MET_FORECASTS (ambient global data, produced every pipeline cycle
    regardless of storm activity) and produces tp exceedance-probability
    tiles/admin views plus ro/tp ratio exceedance-probability tiles/admin
    views, at four windows.

    known_failed_facilities: see run_complete_impact_analysis()'s
    own docstring for the full "why" and the mutate-in-place contract. A real
    caller (main()'s CLI path, both Databricks scheduler notebooks) passes
    the SAME set already accumulated by that same run's own update_storms()
    call, so a facility-fetch failure wind/gust's phase already discovered
    for a country is a known cache hit here too, not a from-scratch retry.
    None (the default) creates a fresh, run-scoped set here instead.

    tp/ro grids for every window are read ONCE up front via
    read_precip_windows() (single download+open of each Zarr file, real
    measured size ~1.3GB/~266MB), not once per window: the Zarr file already
    holds every step any window needs, so reading it per-window would
    re-download the same ~1.5GB combined file 4x for no reason.
    Country-outer/window-inner below, for the same reason: a country's own
    setup (tiles, admin regions, facility fetch/tile-assignment) does not
    depend on which window is being processed, so computing it once per
    country instead of once per (country, window) avoids up to 4x redundant
    work (see fetch_schools()/fetch_health_centers()/fetch_shelters()/
    fetch_wash()'s own shared per-process negative-cache for the cache-miss
    case).

    A precip failure never affects storm (wind/gust) processing and vice
    versa.

    Args:
        countries: List of ISO3 country codes to process (every initialized
            country, not a storm's affected-country list: precip applies
            unconditionally everywhere).
        logger: Logger instance.
        zoom: Mercator tile zoom level (default: 14, matches wind/gust).
        target_date: Optional specific date (YYYY-MM-DD or date) to backfill
            against, mirroring update_storms()'s own target_date. MET_FORECASTS
            retains one row per past forecast cycle (same as
            TC_ENVELOPES_COMBINED retains historical envelopes), so a
            historical cycle's Zarr file is genuinely available to re-read.
            None (default) uses the latest available cycle, matching normal
            (non-backfill) --type update behavior.
    """
    met_data_store = get_hazard_met_data_store()

    if target_date is not None:
        tp_row = get_met_forecast_for_date('tp', target_date)
        ro_row = get_met_forecast_for_date('ro', target_date)
    else:
        tp_row = get_latest_met_forecast('tp')
        ro_row = get_latest_met_forecast('ro')
    if tp_row is None and ro_row is None:
        if target_date is not None:
            logger.info(f"No MET_FORECASTS data available for {target_date}, skipping precip analysis")
        else:
            logger.info("No MET_FORECASTS data available, skipping precip analysis")
        return 0

    # Staleness check: get_latest_met_forecast() only checks that a row exists,
    # never how old it is, so a stalled upstream feed would otherwise be silently
    # reprocessed as current every scheduled run with no warning. Only meaningful
    # in "latest" mode; an explicit historical backfill is expected to be old.
    # Non-blocking (logs a warning, still processes): always-process-latest is
    # the existing intended design, this only adds visibility.
    if target_date is None:
        for label, row in (('tp', tp_row), ('ro', ro_row)):
            if row is None:
                continue
            try:
                age_hours = (pd.Timestamp.now(tz=row['forecast_time'].tzinfo) - pd.Timestamp(row['forecast_time'])).total_seconds() / 3600
                if age_hours > MET_FORECAST_STALE_HOURS:
                    logger.warning(
                        f"Latest MET_FORECASTS '{label}' cycle is {age_hours:.1f}h old "
                        f"(forecast_time={row['forecast_time']}), exceeding the "
                        f"{MET_FORECAST_STALE_HOURS}h freshness threshold, upstream "
                        f"TC-ECMWF-Forecast-Pipeline may have stalled. Processing it anyway."
                    )
            except Exception as e:
                logger.debug(f"Could not evaluate staleness for '{label}' MET_FORECASTS row: {e}")

    date_label = f"for {target_date}" if target_date is not None else "(latest)"
    if tp_row is not None:
        logger.info(f"tp forecast {date_label}: {tp_row['forecast_time']}")
    if ro_row is not None:
        logger.info(f"ro forecast {date_label}: {ro_row['forecast_time']}")

    # tp and ro are downloaded independently upstream, one param's cycle can
    # fail while the other succeeds, leaving MET_FORECASTS with mismatched
    # "latest" rows per param. Never combine them into a ratio in that case,
    # tp still gets processed on its own below (it doesn't need ro at all).
    if tp_row is not None and ro_row is not None and tp_row['forecast_time'] != ro_row['forecast_time']:
        logger.warning(
            f"tp ({tp_row['forecast_time']}) and ro ({ro_row['forecast_time']}) latest forecasts "
            "are from different cycles, skipping the ro/tp ratio this run (tp itself is unaffected)"
        )
        ro_row = None

    forecast_time = (tp_row or ro_row)['forecast_time']
    if hasattr(forecast_time, 'strftime'):
        forecast_time_str = forecast_time.strftime('%Y%m%d%H%M%S')
    else:
        forecast_time_str = str(forecast_time)

    # Idempotency gate: skip entirely if this exact cycle was already fully
    # processed by a prior run (see AMBIENT_HAZARD_RUN_LOG's own comment
    # above). Only in "latest" mode - an explicit --date backfill always
    # reprocesses. Attempted unconditionally regardless of DATA_PIPELINE_DB:
    # a HAZARD_DATA_SOURCE=SNOWFLAKE deployment (the default) already has a
    # Snowflake connection open from get_latest_met_forecast() above; a
    # LOCAL/BLOB deployment has none and fails open gracefully via the
    # try/except below. Not gated on DATA_PIPELINE_DB=='SNOWFLAKE': that
    # would let a BLOB-mode scheduled run reprocess the same MET_FORECASTS
    # cycle every invocation - exactly the wasted warehouse cost this
    # mechanism exists to eliminate.
    #
    # 'tp' and 'tp_ro' are tracked as two independent params at the same
    # forecast_time: tp/ro publish independently upstream, so a cycle where
    # tp alone was processed must still reprocess once ro catches up, rather
    # than being permanently skipped as if the ratio views already existed.
    #
    # `ambient_tracking_enabled` avoids holding one Snowflake connection open
    # across the whole per-country loop below (well over an hour across 36
    # countries), risking a session/token expiry before it's needed again;
    # this check's connection closes immediately after, and a fresh one opens
    # later in the marking block.
    ambient_tracking_enabled = target_date is None
    if ambient_tracking_enabled:
        conn = None
        try:
            conn = get_snowflake_connection()
            tp_done = is_ambient_forecast_processed(conn, 'precip', 'tp', forecast_time)
            ratio_done = is_ambient_forecast_processed(conn, 'precip', 'tp_ro', forecast_time)
            if tp_done and (ratio_done or ro_row is None):
                logger.info(f"Precip cycle {forecast_time} already processed, skipping (no new upstream cycle since last run)")
                return 0
        except Exception as e:
            logger.warning(f"Could not check AMBIENT_HAZARD_RUN_LOG, processing anyway: {e}")
        finally:
            if conn is not None:
                conn.close()

    files_written = 0
    # Every country whose mercator base view failed to load gets skipped for
    # this whole run. This function returns a plain int (files_written), used
    # by 4 real call sites, so skips aren't threaded through the return value -
    # tracked here and logged loudly at the end instead, since AMBIENT_HAZARD_
    # RUN_LOG marks the whole cycle processed as soon as ANY country wrote
    # output. A country landing here on one window still gets a fresh attempt
    # on every later window (no cross-window failure cache), so it's
    # reconciled against countries_with_output before the final log line,
    # which only names a country with genuinely zero output across every
    # window, not a transient per-window failure.
    skipped_countries = []
    countries_with_output = set()
    # Gates how often _release_memory() actually fires below (every N
    # passes, not every single one): see RELEASE_MEMORY_EVERY_N_PASSES' own
    # module-level comment for why.
    _release_memory_counter = 0

    # tp/ro grids for every real window, read ONCE up front (not once per
    # window x once per country): see this function's own docstring for the
    # full "why" and the real redundant-download cost this eliminates.
    tp_windows = {}
    if tp_row is not None:
        try:
            tp_windows = read_precip_windows(tp_row['stage_path'], met_data_store, PRECIP_WINDOWS_H)
        except Exception as e:
            logger.warning(f"Failed to read tp windows: {e}")
    ro_windows = {}
    if ro_row is not None:
        try:
            # ro shares tp's own grid (same ECMWF ENS run), so only its own
            # array is kept per window below; tp's own bounds are reused for
            # both GeoTIFFs.
            ro_windows = read_precip_windows(ro_row['stage_path'], met_data_store, PRECIP_WINDOWS_H)
        except Exception as e:
            logger.warning(f"Failed to read ro windows: {e}")

    # Exceedance-probability/bitmask grids for every (window, threshold)
    # combination, computed ONCE here rather than per-country: none of
    # exceedance_probability()/exceedance_bitmask()/ratio_exceedance_probability()
    # take country-specific input, so recomputing per country would produce
    # the same array-wide result each time. create_precip_tile_view()/
    # create_precip_admin_view() below are still invoked once per (country,
    # window, threshold), just fed a precomputed grid_2d.
    tp_probability_grids = {}
    tp_bitmask_grids = {}
    for window_h, tp_result in tp_windows.items():
        tp_member_numbers, _, _, _, _, tp_grid = tp_result
        for threshold_mm in PRECIP_TP_THRESHOLDS_MM.get(window_h, []):
            try:
                tp_probability_grids[(window_h, threshold_mm)] = exceedance_probability(tp_grid, threshold_mm)
                if tp_member_numbers is not None:
                    tp_bitmask_grids[(window_h, threshold_mm)] = exceedance_bitmask(
                        tp_grid, threshold_mm, tp_member_numbers)
            except Exception as e:
                logger.warning(f"Failed to precompute tp exceedance grid for window={window_h}h "
                               f"threshold={threshold_mm}mm: {e}")

    ratio_probability_grids = {}
    for window_h in PRECIP_WINDOWS_H:
        tp_result = tp_windows.get(window_h)
        ro_result = ro_windows.get(window_h)
        if tp_result is None or ro_result is None:
            continue
        _, _, _, _, _, tp_grid_for_ratio = tp_result
        _, _, _, _, _, ro_grid_for_ratio = ro_result
        for ratio_threshold in RATIO_THRESHOLDS:
            try:
                ratio_probability_grids[(window_h, ratio_threshold)] = ratio_exceedance_probability(
                    ro_grid_for_ratio, tp_grid_for_ratio, ratio_threshold, min_tp_mm=RATIO_MIN_TP_MM)
            except Exception as e:
                logger.warning(f"Failed to precompute ratio exceedance grid for window={window_h}h "
                               f"ratio={ratio_threshold}: {e}")

    # tp_windows/ro_windows' raw per-member grids (~141MB float32 each, 8
    # windows = ~1.13GB) were only needed by the precompute block above: the
    # per-country loop below reads tp_probability_grids/tp_bitmask_grids/
    # ratio_probability_grids instead, only ever using bounds/member-numbers
    # from tp_windows/ro_windows. Stripping the grid here once keeps that
    # ~1.13GB from staying resident across the whole per-country loop (up to
    # 36 countries); bounds/member-numbers are preserved, only the grid
    # element is dropped from each tuple.
    tp_windows = {w: t[:-1] for w, t in tp_windows.items()}
    ro_windows = {w: t[:-1] for w, t in ro_windows.items()}

    if not tp_probability_grids and not ratio_probability_grids:
        # Neither tp thresholds nor the ro/tp ratio have any usable
        # precomputed grid (e.g. a newer MET_FORECASTS cycle appearing in
        # Snowflake before the shared Blob mirror has caught up). The
        # per-country loop below has zero chance of writing anything for any
        # country, since every window's tp_result/ro_result lookup would come
        # back None regardless. Running it anyway would still pay full
        # per-country setup cost (tile/admin loading, 4 facility fetches with
        # fast_fail timeouts) for guaranteed-zero output, so it's skipped
        # entirely instead. Deliberately leaves files_written at 0, so the
        # marking gates below leave this cycle eligible for retry.
        logger.warning(
            "Neither tp nor ro/tp ratio has any usable precomputed exceedance grid for this "
            "cycle; skipping the entire "
            f"{len(countries)}-country loop rather than paying full per-country setup cost for "
            "guaranteed-zero output. This cycle stays eligible for retry once real window data "
            "is available."
        )
        countries_to_process = []
    else:
        countries_to_process = countries

    # Admin levels listed ONCE across every country, not per country:
    # get_initialized_admin_levels() lists the WHOLE shared admin_views/
    # directory and filters client-side by country prefix, so calling it per
    # country re-lists that same directory redundantly. get_initialized_
    # admin_levels_bulk() does that listing once, reused via a dict lookup.
    admin_levels_by_country = get_initialized_admin_levels_bulk(countries=countries_to_process) if countries_to_process else {}

    _log_level_name = logging.getLevelName(logger.getEffectiveLevel())
    # known_failed_facilities is now a parameter (see this function's own
    # docstring for the full "why"), not a fresh set here: it must persist
    # and keep accumulating across the wind/gust -> precip -> river phase
    # sequence, not reset at the start of this one phase.
    if known_failed_facilities is None:
        known_failed_facilities = set()
    for country in countries_to_process:
        # Real wall-clock per country, logged at the end of this loop body:
        # a Databricks-hosted run's own live output buffer keeps only a fixed
        # head+tail of stdout, discarding the middle of a long run entirely,
        # so this is the only durable record of where time actually went once
        # a run is long enough to hit that cap. Not a performance measurement
        # tool on its own, just enough signal to localize a slow country
        # without needing to reconstruct it after the fact.
        country_start_time = time.time()

        # Country's own setup (tiles, admin regions, facility locations,
        # facility-to-tile maps) plus the full window x threshold
        # view-creation-and-write loop all run inside their own fork
        # subprocess (_process_one_country_precip_isolated), never inline in
        # this driver process: each country's own setup and per-window write
        # loop can accumulate enough memory in a long run to warrant
        # isolating it in its own process, same rationale as wind/gust's.
        # The isolation makes an out-of-memory kill for one country an
        # ordinary per-country error the except block below handles, rather
        # than taking down the whole precip run.
        try:
            result, new_failures = _process_one_country_precip_isolated(
                country, zoom, forecast_time_str, tp_windows, ro_windows,
                tp_probability_grids, tp_bitmask_grids, ratio_probability_grids,
                admin_levels_by_country, known_failed_facilities, _log_level_name)
            known_failed_facilities |= new_failures
        except Exception as e:
            logger.error(f"Precip processing failed catastrophically for {country}, skipping this country entirely this cycle: {e}")
            if country not in skipped_countries:
                skipped_countries.append(country)
            result = None
        finally:
            # The real per-country memory isolation now happens inside the
            # subprocess itself (reclaimed by the OS the instant it exits,
            # regardless of cause); this call is a secondary, cheap safety
            # net for whatever small amount this driver process itself
            # accumulates handling the subprocess/queue plumbing per
            # country, same real cadence as before (every 8th pass, not
            # every single one: see _release_memory_counter's own
            # declaration above for why).
            _release_memory_counter += 1
            if _release_memory_counter % RELEASE_MEMORY_EVERY_N_PASSES == 0:
                _release_memory(logger)

        if result is not None:
            if result["skipped"]:
                if country not in skipped_countries:
                    skipped_countries.append(country)
            else:
                files_written += result["files_written"]
                if result["files_written"] > 0:
                    countries_with_output.add(country)

        logger.info(f"{country}: precip processing finished in {time.time() - country_start_time:.1f}s")

    logger.info(f"Precip analysis complete for {len(countries)} countries across {len(PRECIP_WINDOWS_H)} windows ({files_written} files written)")
    # Reconciled against countries_with_output: a country that failed on an
    # early window but succeeded on a later one has real output for this
    # cycle and must not be reported here: see skipped_countries' own
    # declaration above for the full "why".
    truly_skipped_countries = [c for c in skipped_countries if c not in countries_with_output]
    if truly_skipped_countries:
        logger.error(f"Precip analysis: {len(truly_skipped_countries)} of {len(countries)} countries skipped entirely this cycle "
                     f"(no precip hazard output written for them): {', '.join(truly_skipped_countries)}")

    # Mark this cycle processed so a future run with the same forecast_time
    # skips reprocessing (see AMBIENT_HAZARD_RUN_LOG's own comment above).
    # Gated on files_written > 0 AND truly_skipped_countries being empty: a
    # cycle where every country failed (e.g. a transient stage/Snowflake
    # outage) must stay eligible for retry, and so must a cycle where SOME
    # countries wrote real output but at least one genuinely failed (e.g. an
    # isolated subprocess OOM kill on a large country), marking the whole
    # cycle done in that case would silently and permanently drop that
    # country's hazard output for this forecast_time, since a future run
    # would see the cycle already marked processed and skip it entirely,
    # never retrying the one country that actually needed it. This exact
    # failure mode is already described in truly_skipped_countries' own
    # comment above; the marking gate below checks it accordingly. 'tp' and
    # 'tp_ro' are marked
    # independently, matching the two independent checks above: 'tp'
    # whenever tp itself was available this run, 'tp_ro' only when ro was
    # also available and the ratio views were actually produced.
    _is_full_run = _countries_cover_full_active_list(countries, logger)
    ambient_conn = None
    if ambient_tracking_enabled:
        try:
            ambient_conn = get_snowflake_connection()
        except Exception as e:
            logger.warning(f"Could not open a fresh Snowflake connection to update AMBIENT_HAZARD_RUN_LOG for precip: {e}")
    if ambient_conn is not None and files_written > 0 and not truly_skipped_countries and _is_full_run:
        try:
            if tp_row is not None:
                log_ambient_forecast_processed(ambient_conn, 'precip', 'tp', forecast_time)
            if ro_row is not None:
                log_ambient_forecast_processed(ambient_conn, 'precip', 'tp_ro', forecast_time)
        except Exception as e:
            logger.warning(f"Could not update AMBIENT_HAZARD_RUN_LOG for precip: {e}")
    elif ambient_conn is not None and files_written > 0 and not truly_skipped_countries and not _is_full_run:
        logger.warning(
            f"Not marking this precip cycle processed in AMBIENT_HAZARD_RUN_LOG: this run's own "
            f"{len(countries)} countries do not cover the real, currently-active country list, so "
            f"this is not a genuine full production run. Marking it done anyway would silently and "
            f"permanently skip every country this run never touched. Only a real full run is allowed "
            f"to mark a cycle processed."
        )
    elif ambient_conn is not None and files_written > 0 and truly_skipped_countries and _is_full_run:
        # A genuine, full-run country failure: only a real full run's own
        # failures count toward the give-up threshold below, so a partial/
        # test invocation can never accidentally push a real cycle toward
        # being given up on.
        gave_up = False
        try:
            if tp_row is not None:
                gave_up = record_ambient_failure_attempt(
                    ambient_conn, 'precip', 'tp', forecast_time, truly_skipped_countries, logger) or gave_up
            if ro_row is not None:
                gave_up = record_ambient_failure_attempt(
                    ambient_conn, 'precip', 'tp_ro', forecast_time, truly_skipped_countries, logger) or gave_up
        except Exception as e:
            logger.warning(f"Could not record the precip failure attempt: {e}")
        if gave_up:
            try:
                if tp_row is not None:
                    log_ambient_forecast_processed(ambient_conn, 'precip', 'tp', forecast_time)
                if ro_row is not None:
                    log_ambient_forecast_processed(ambient_conn, 'precip', 'tp_ro', forecast_time)
            except Exception as e:
                logger.warning(f"Could not update AMBIENT_HAZARD_RUN_LOG for precip after giving up: {e}")
    elif ambient_conn is not None and files_written > 0 and truly_skipped_countries and not _is_full_run:
        logger.warning(
            f"{len(truly_skipped_countries)} countries genuinely failed this run "
            f"({', '.join(truly_skipped_countries)}), but this isn't a full production run, so this "
            f"failure does not count toward the give-up limit and this cycle is not marked processed."
        )
    if ambient_conn is not None:
        ambient_conn.close()

    return files_written


# =============================================================================
# RIVER FLOOD (GloFAS x JRC) ANALYSIS (storm-independent)
# =============================================================================

# All 6 RIVER_FORECASTS extent tiers, mirrors gust's own "process everything
# available" convention rather than an arbitrary subset.
RIVER_RP_TIERS = ['rp2', 'rp5', 'rp10', 'rp20', 'rp50', 'rp100']

# Defensive fallback only: the real IS_STANDIN column from each
# RIVER_FORECASTS row is the source of truth (see run_river_flood_analysis
# below); this is only consulted if that column ever comes back unexpectedly
# null, so the "upper-bound approximation, not exact" caveat is never
# silently lost from this repo's own output even in a degraded-metadata case.
RIVER_STANDIN_TIERS_FALLBACK = {'rp2', 'rp5'}

# Bounded concurrency for the per-country isolated-subprocess loop
RIVER_COUNTRY_CONCURRENCY = 2

# A different memory risk than RIVER_COUNTRY_CONCURRENCY above (tile-grid
# size): two facility-dense countries occupying both concurrency slots at
# once can each approach that same per-country bound, and those peaks sum
# when run concurrently. Sequential tile assignment (see the fetch-closures
# comment in _run_country_worker_river) bounds one country's own peak but
# does nothing to stop two facility-dense countries summing across the two
# worker threads below.
#
# RIVER_LARGE_COUNTRIES prevents two listed countries from running
# concurrently with each other (see the dispatch loop below, in
# run_river_flood_analysis); a large+small or two-small pairing keeps full
# 2x concurrency.
#
# Populated from a Blob survey of every active PIPELINE_COUNTRIES entry's
# school+health_center+shelter+wash combined point count, using >=8,000 as
# the threshold - chosen to sit just below BGD's 15,842 while still catching
# BEN's 8,781, since BEN and BGD are adjacent in COUNTRY_CODE alphabetical
# order (the dispatch order get_active_countries_from_snowflake()/
# river_pending below uses), making a concurrent BEN/BGD pairing a live risk.
#
# Not computed dynamically from live facility-cache counts: a country's
# cache can be temporarily missing (e.g. BGD's health-center cache above),
# which would under-classify it right as its facility fetch attempts a
# fresh, memory-heavy pull. A hardcoded list needs manual updates as
# PIPELINE_COUNTRIES grows (same tradeoff as HC_FACILITY_TYPES elsewhere)
# but never silently under-protects on a transient cache gap.
RIVER_LARGE_COUNTRIES = frozenset({
    'MEX', 'PHL', 'GTM', 'HND', 'BGD', 'SEN', 'MOZ', 'NIC', 'SLE', 'HTI', 'BEN',
})

# Cumulative wall-clock budget for _run_country_worker_river()'s rp_tier x
# step_h loop (up to 6 x 4 = 24 iterations), measured from worker start
# (setup included), nested inside COUNTRY_SUBPROCESS_TIMEOUT_S (1800s):
# each Blob op already has its own 120s _BLOB_OP_TIMEOUT_SECONDS
# (data_store_utils.py), but that alone doesn't bound how many of the ~24
# iterations (each up to ~2 sequential 120s-timeout ops) a persistently
# degraded country could grind through before hitting the full 30-minute
# ceiling with no partial output. A healthy country finishes in 10-30s;
# 360s (6 minutes) caps a stuck country at 20% of the subprocess timeout,
# leaving margin for a couple of genuinely slow iterations.
#
# Only governs the "no real flood data confirmed yet" phase: a country that
# has confirmed real flooded pixels (query_country_flooded_pixels() returned
# non-empty for some tier) is doing genuine work, not hanging - a large real
# flood event naturally takes longer, and cutting it off would silently
# under-report a real disaster. So this budget applies at the top of every
# rp-loop iteration until a tier's df_pixels comes back non-empty, at which
# point RIVER_COUNTRY_TIME_BUDGET_WITH_REAL_DATA_S (below) takes over for the
# rest of this country's processing - see _found_real_pixels in
# _run_country_worker_river(). Checked only at iteration boundaries, so an
# in-flight op is still bounded solely by its own _BLOB_OP_TIMEOUT_SECONDS.
RIVER_COUNTRY_TIME_BUDGET_S = 360

# The "real flood data already confirmed" budget (see RIVER_COUNTRY_TIME_
# BUDGET_S's comment for why this is a separate, larger constant rather than
# simply removing the cap): a country with widespread real flooding could
# need all 24 rp_tier x step_h iterations even under healthy infrastructure,
# and this tool must never drop real impact data for an arbitrary timer. But
# a real flood signal doesn't prove the remaining iterations are healthy -
# without an outer bound, even a minor real hit could grind through all 24
# iterations at degraded speed (worst case ~5760s/96min), blowing past
# COUNTRY_SUBPROCESS_TIMEOUT_S (1800s) and getting SIGKILLed with nothing
# returned. 1200s leaves a 600s margin under that 30-minute kill, so a
# degraded country with real flood data still returns a partial result.
RIVER_COUNTRY_TIME_BUDGET_WITH_REAL_DATA_S = 1200

# Diagnostic-only watchdog for _run_country_worker_river(): if the forked
# child is still alive this many seconds after starting, faulthandler dumps
# its live stack trace instead of leaving a COUNTRY_SUBPROCESS_TIMEOUT_S
# (1800s) kill with no trace of what it was doing. 600s (10 minutes) is well
# over an order of magnitude past a healthy country's 10-30s runtime (even
# at the largest known ~20M pixel count), set above RIVER_COUNTRY_TIME_
# BUDGET_S (360s) so it never fires mid-budget, and leaves the same 600s
# margin under the 30-minute kill as RIVER_COUNTRY_TIME_BUDGET_WITH_REAL_
# DATA_S. A country with confirmed real flood data can legitimately run up
# to 1200s, so this can still fire there too - harmless either way (a
# changing stack across log lines reads as progress, a parked one as a hang).
RIVER_COUNTRY_WATCHDOG_S = 600

# Cross-run relay cache for known_failed_facilities, read/written via
# get_data_store() (works under any DATA_PIPELINE_DB backend). Exists for the
# split-scheduler architecture (separate wind/gust, precip, river Databricks
# Jobs, each its own process): a facility-fetch failure one job discovers
# can't reach the others via an in-memory set, so this file is the relay
# channel instead. Not the same as hc_exist()/school_exist()/shelter_exist()/
# wash_exist()'s own permanent per-country negative cache (authoritative,
# never expires) - this is a short-lived, same-day TTL hint on top of it, for
# a country whose live refresh failed transiently (e.g. a same-hour Overpass
# outage), so a stale file left by a crashed run doesn't suppress retries forever.
KNOWN_FAILED_FACILITIES_CACHE_PATH = 'geodb/aos_views/_cache/known_failed_facilities.json'
KNOWN_FAILED_FACILITIES_CACHE_TTL_HOURS = 6


def load_known_failed_facilities_cache(logger):
    """
    Reads the cross-run known_failed_facilities relay cache written by a
    prior run's save_known_failed_facilities_cache() call (any of the 3 split
    scheduler jobs), via get_data_store(). No separate code path is needed for
    LOCAL/BLOB/SNOWFLAKE-stage, the same abstraction every other read/write
    in this repo already uses.

    Fails open (returns an empty set) on a missing file, a read/parse error,
    or an entry older than KNOWN_FAILED_FACILITIES_CACHE_TTL_HOURS: this is
    an optimization hint layered on top of the real, permanent facility
    cache (see this constant's own module-level comment above), never
    something allowed to suppress a genuinely-due retry by failing closed.
    """
    from data_store_utils import get_data_store
    try:
        store = get_data_store()
        if not store.file_exists(KNOWN_FAILED_FACILITIES_CACHE_PATH):
            return set()
        raw = store.read_file(KNOWN_FAILED_FACILITIES_CACHE_PATH)
        payload = json.loads(raw.decode('utf-8') if isinstance(raw, bytes) else raw)
        written_at = datetime.fromisoformat(payload['written_at'])
        age_hours = (datetime.now() - written_at).total_seconds() / 3600.0
        if age_hours > KNOWN_FAILED_FACILITIES_CACHE_TTL_HOURS:
            logger.info(
                f"known_failed_facilities cache is {age_hours:.1f}h old (> "
                f"{KNOWN_FAILED_FACILITIES_CACHE_TTL_HOURS}h TTL), treating as empty rather than stale"
            )
            return set()
        result = set()
        for entry in payload.get('entries', []):
            country, facility_type = entry.split('|', 1)
            result.add((country, facility_type))
        logger.info(f"Loaded {len(result)} known-failed-facility entr(y/ies) from cross-run cache ({age_hours:.1f}h old)")
        return result
    except Exception as e:
        logger.warning(f"Could not load known_failed_facilities cache, starting fresh (fail-open): {e}")
        return set()


def save_known_failed_facilities_cache(known_failed_facilities, logger):
    """
    Writes this run's known_failed_facilities set to the same cache
    load_known_failed_facilities_cache() reads. Best-effort: a write failure
    is logged and swallowed, never raised, since this cache is an
    optimization, not correctness-critical state (see this constant's own
    module-level comment above for the real, permanent cache it sits on top
    of).

    Merges with whatever is on disk right before writing, rather than a
    blind overwrite: the 3 split scheduler jobs (wind/gust, precip, river)
    can genuinely run concurrently on the same cluster, so a naive
    load-at-start/write-at-end pattern across racing processes would let the
    later write silently discard the earlier one's own discoveries.
    Re-reading here narrows that window to "both processes' own final writes
    land within the same read-then-write race" rather than "always
    clobbers": an improvement, not a full fix (this file has no
    locking/conditional-write primitive), an acceptable tradeoff given this
    is a same-day optimization hint, not correctness-critical state.
    """
    from data_store_utils import get_data_store
    try:
        store = get_data_store()
        merged = set(known_failed_facilities) | load_known_failed_facilities_cache(logger)
        payload = {
            'written_at': datetime.now().isoformat(),
            'entries': sorted(f"{country}|{facility_type}" for country, facility_type in merged),
        }
        store.write_file(KNOWN_FAILED_FACILITIES_CACHE_PATH, json.dumps(payload, indent=2))
    except Exception as e:
        logger.warning(f"Could not save known_failed_facilities cache: {e}")

# Cumulative window boundaries (hours) for the per-country impact numbers,
# matching the dashboard's ms-river-window UI options: a pixel/member counts
# as flooded within a window if it floods on any lead-time step up to that
# window, not just the exact final day (see Ahead-of-the-Storm's
# services/tile_server.py, _RIVER_EXTENT_STEP_HOURS' "ACCUMULATION SEMANTICS"
# comment). Consumed via `gdf_pixels['step_h'] <= step_h` filtering in
# _run_country_worker_river(). A subset of the full daily lead-time steps in
# the RIVER_FORECASTS extent Parquet (24-168h); dropping 48/96/144h keeps
# materialized-view compute/storage proportional to what the UI exposes.
RIVER_LEADTIME_STEPS_H = [24, 72, 120, 168]

# RIVER_FORECASTS' own refresh cadence is daily, independent of storm
# activity (like MET_FORECASTS). Same non-blocking staleness-warning
# purpose as MET_FORECAST_STALE_HOURS above.
RIVER_FORECAST_STALE_HOURS = 24


def _parallel_write_batch(write_tasks, logger, max_workers=BATCH_WRITE_MAX_WORKERS):
    """
    Runs a batch of independent save_*() calls concurrently via a bounded
    ThreadPoolExecutor. Shared by both _run_country_worker_precip() (per
    country/window/threshold, up to 7 independent Blob writes: bitmask
    view, one per admin level, up to 4 facility types) and
    _run_country_worker_river() (per country/rp_tier/step_h, the same shape
    of independent writes), hazard-agnostic despite the write_tasks
    themselves each closing over a hazard-specific save_precip_*()/
    save_river_*() call.

    Safe for the same reason a single write is safe: each write targets a
    distinct file path with already-fully-computed, independent
    DataFrame/GeoDataFrame content (no data dependency between writes), and
    each builds its own lightweight client off the shared
    BlobServiceClient's connection pool, so running them concurrently
    produces byte-identical file content to running them one at a time.

    write_tasks is a list of zero-arg callables, each one a closure over its
    own save_*() call and arguments. Every task is attempted exactly once
    regardless of whether another one raises: an independent write here can
    never be skipped by a sibling's failure, only run concurrently with (and
    possibly complete before or after) it.

    Returns (succeeded, first_exc): succeeded is the real count of writes
    that actually completed, always correct regardless of whether any other
    task failed. first_exc is the first exception raised by any task, or
    None if all succeeded.

    Callers must add `succeeded` to their own file counters THEN re-raise
    `first_exc` themselves if it is not None, inside their own existing
    per-pass `try/except`, e.g.:

        succeeded, first_exc = _parallel_write_batch(write_tasks, logger)
        files_written += succeeded
        if first_exc is not None:
            raise first_exc

    This ordering matters: returning the count separately from the
    exception, and requiring the caller to add it before re-raising, ensures
    a partial failure's own successful writes are never silently uncounted.
    """
    succeeded = 0
    first_exc = None
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(task) for task in write_tasks]
        for future in futures:
            try:
                future.result()
                succeeded += 1
            except Exception as e:
                if first_exc is None:
                    first_exc = e
                logger.warning(f"Parallel write task failed: {e}")
    return succeeded, first_exc


def run_river_flood_analysis(countries, logger, zoom=14, target_date=None, known_failed_facilities=None):
    """
    Storm-independent GloFAS x JRC river-flood analysis, run once per
    --type update invocation (like run_precip_analysis), not once per storm.
    Reads RIVER_FORECASTS' 6 extent_rpN_bymember Parquet tiers (produced by
    TC-ECMWF-Forecast-Pipeline daily, regardless of storm activity) and
    produces per-tier, per-lead-time-step tile/admin/facility flood-extent
    probability views.

    known_failed_facilities: see run_complete_impact_analysis()'s own
    docstring for the full "why" and the mutate-in-place contract. A real
    caller passes the SAME set already accumulated by update_storms() +
    run_precip_analysis().

    Unlike precip's single continuous raster, GloFAS/JRC pixels (~150m) are
    comparable to a mercator tile or buffered facility, not much coarser, so
    this reuses the wind/gust "count(members)/FULL_ENSEMBLE_SIZE" convention
    (see create_river_tile_view() in river_views.py) rather than
    precip's centroid-raster-sampling approach. Each RP tier's global
    Parquet file (70M+ rows) is downloaded ONCE per (tier, date), never per
    country, then queried per country via a DuckDB bbox filter against that
    local file. Country-outer/tier-inner below, for the same reason as
    precip's country-outer/window-inner nesting: a country's own setup does
    not depend on which tier is being processed, so computing it once per
    country instead of once per (country, tier) avoids up to 6x redundant
    work.

    A river-flood failure never affects wind/gust/precip processing and vice
    versa (own try/except isolation at the main() call site).
    """
    river_data_store = get_river_forecasts_data_store()

    tier_rows = {}
    if target_date is not None:
        for rp in RIVER_RP_TIERS:
            tier_rows[rp] = get_river_forecast_for_date(f"extent_{rp}_bymember", target_date)
    else:
        # One real Snowflake round-trip for all 6 tiers instead of 6
        # sequential ones (real, measured cost: ~11.7s of pure serialized
        # round-trip latency in a real per-country profile, ~2s/call). See
        # get_latest_river_forecasts_bulk()'s own docstring for the full "why".
        bulk = get_latest_river_forecasts_bulk([f"extent_{rp}_bymember" for rp in RIVER_RP_TIERS])
        for rp in RIVER_RP_TIERS:
            tier_rows[rp] = bulk[f"extent_{rp}_bymember"]

    if all(v is None for v in tier_rows.values()):
        logger.info("No RIVER_FORECASTS data available, skipping river-flood analysis")
        return 0

    # Staleness check, same non-blocking shape as run_precip_analysis's own
    # MET_FORECAST_STALE_HOURS check, only meaningful in "latest" mode.
    if target_date is None:
        for rp, row in tier_rows.items():
            if row is None:
                continue
            try:
                age_hours = (pd.Timestamp.now(tz=row['forecast_time'].tzinfo)
                             - pd.Timestamp(row['forecast_time'])).total_seconds() / 3600
                if age_hours > RIVER_FORECAST_STALE_HOURS:
                    logger.warning(
                        f"Latest RIVER_FORECASTS '{rp}' cycle is {age_hours:.1f}h old "
                        f"(forecast_time={row['forecast_time']}), exceeding the "
                        f"{RIVER_FORECAST_STALE_HOURS}h freshness threshold, upstream "
                        f"TC-ECMWF-Forecast-Pipeline may have stalled. Processing it anyway."
                    )
            except Exception as e:
                logger.debug(f"Could not evaluate staleness for river tier '{rp}': {e}")

    # Idempotency gate setup: see AMBIENT_HAZARD_RUN_LOG's comment above
    # run_precip_analysis() (attempted unconditionally regardless of
    # DATA_PIPELINE_DB). Checked per-tier below (each RP tier has its own
    # forecast_time), unlike precip's single shared tp/ro cycle.
    # `ambient_tracking_enabled` avoids holding one Snowflake connection open
    # across the whole per-country loop below (113+ minutes across 36
    # countries), risking a session/token expiry; this check's connection
    # closes immediately after, and a fresh one opens later in the marking block.
    ambient_tracking_enabled = target_date is None
    tiers_to_process = []
    conn = None
    if ambient_tracking_enabled:
        try:
            conn = get_snowflake_connection()
        except Exception as e:
            logger.warning(f"Could not open Snowflake connection for AMBIENT_HAZARD_RUN_LOG check: {e}")
    try:
        # Real tiers this run actually needs to process, decided up front via
        # the same real per-tier checks: a missing row, or an
        # already-processed cycle per AMBIENT_HAZARD_RUN_LOG.
        for rp in RIVER_RP_TIERS:
            row = tier_rows[rp]
            if row is None:
                logger.info(f"No RIVER_FORECASTS row for tier '{rp}', skipping this tier")
                continue
            forecast_time = row['forecast_time']
            if conn is not None:
                try:
                    if is_ambient_forecast_processed(conn, 'river', rp, forecast_time):
                        logger.info(f"River tier '{rp}' cycle {forecast_time} already processed, skipping (no new upstream cycle since last run)")
                        continue
                except Exception as e:
                    logger.warning(f"Could not check AMBIENT_HAZARD_RUN_LOG for river tier '{rp}', processing anyway: {e}")
            tiers_to_process.append(rp)
    finally:
        if conn is not None:
            conn.close()

    files_written = 0
    # Same log-only tracking as run_precip_analysis's skipped_countries (this
    # function also returns a plain int, depended on by 4 call sites). A
    # country can land here on one tier but succeed on a later one (no
    # cross-tier failure cache), so it's reconciled against
    # countries_with_output before the final log line.
    skipped_countries = []
    countries_with_output = set()
    # {country: reason_string} for skipped_countries: carries the root-cause
    # text since a forked child's stdout isn't reliably captured after the
    # fact, and a bare country code can't distinguish a hang from a fast
    # failure. Recorded in AMBIENT_HAZARD_FAILURE_LOG's FAILURE_REASONS
    # column (record_ambient_failure_attempt()) for diagnosis without live reproduction.
    skip_reasons = {}
    # {rp: {country, ...}} - countries that returned "ok" (never in
    # skipped_countries) but whose own time budget tripped before finishing
    # this tier (see `budget_incomplete_tiers` in _run_country_worker_river()).
    # Unlike a skipped country, which blocks marking for every tier uniformly,
    # a budget-truncated country can have complete output for some tiers and
    # incomplete for others, so this is tracked per-tier and folded into the
    # marking loop's gate below, alongside truly_skipped_countries.
    budget_incomplete_countries_by_tier = {}
    # Gates how often _release_memory() actually fires below (every N
    # passes, not every single one): see RELEASE_MEMORY_EVERY_N_PASSES' own
    # module-level comment for why.
    _release_memory_counter = 0
    # Per-tier file count, tracked across the WHOLE country loop (tiers are
    # the inner axis), so AMBIENT_HAZARD_RUN_LOG marking below only fires for
    # a tier that genuinely wrote something for at least one country.
    tier_files_written = {rp: 0 for rp in tiers_to_process}
    # Same approach as run_precip_analysis's own admin_levels_by_country: ONE
    # listing of the shared admin_views/ directory for every country in this
    # run, instead of one get_initialized_admin_levels(country) call per
    # country inside _run_country_worker_river.
    admin_levels_by_country = get_initialized_admin_levels_bulk(countries=countries)
    # known_failed_facilities is now a parameter (see this function's own
    # docstring for the full "why"), not a fresh set here: it must persist
    # and keep accumulating across the wind/gust -> precip -> river phase
    # sequence, not reset at the start of this one phase.
    if known_failed_facilities is None:
        known_failed_facilities = set()

    if tiers_to_process:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_paths = {}

            # Concurrent, not sequential: these are independent real network
            # downloads (real, measured cost in a per-country profile: 6
            # sequential downloads summed to ~20.4s, dominated by whichever
            # tier's own file happens to be largest, e.g. rp2 alone at
            # 11.8s). Bounded to len(tiers_to_process) (at most 6, one per
            # real RP tier) so this never over-subscribes regardless of how
            # many tiers are actually being processed.
            with ThreadPoolExecutor(max_workers=len(tiers_to_process)) as executor:
                futures = {
                    executor.submit(download_river_forecast_parquet, tier_rows[rp]['stage_path'], river_data_store, tmp_dir): rp
                    for rp in tiers_to_process
                }
                for future in futures:
                    rp = futures[future]
                    try:
                        local_paths[rp] = future.result()
                    except Exception as e:
                        logger.warning(f"Failed to download RIVER_FORECASTS tier '{rp}': {e}")
                        # Left out of local_paths: every country's own tier loop
                        # below skips this tier via the `if rp not in local_paths`
                        # check, evaluated independently per country.

            if not local_paths:
                # Every tier's download failed (e.g. HAZARD_DATA_SOURCE=BLOB
                # when the upstream Blob mirror lags Snowflake by days): the
                # per-country loop below has zero chance of writing anything,
                # since every tier's `if rp not in local_paths` check would
                # skip it anyway. Running it would still pay full per-country
                # setup cost for guaranteed-zero output, so it's skipped
                # entirely instead, matching run_precip_analysis()'s
                # `tp_row is None and ro_row is None` fast-exit. Leaves
                # tier_files_written/truly_skipped_countries untouched, so
                # this cycle stays eligible for retry.
                logger.warning(
                    f"Every one of this run's {len(tiers_to_process)} RIVER_FORECASTS tier "
                    f"downloads failed; skipping the entire {len(countries)}-country loop for "
                    f"this cycle rather than paying full per-country setup cost for guaranteed-"
                    f"zero output. This cycle stays eligible for retry once real tier data is "
                    f"available."
                )
                tiers_to_process = []

            _log_level_name = logging.getLevelName(logger.getEffectiveLevel())

            def _process_one_country_river_concurrent(country, seed_known_failed):
                """
                Runs inside a worker THREAD (not the main driver thread), one
                per country, up to RIVER_COUNTRY_CONCURRENCY at a time - see
                that constant's own docstring for the real memory math this
                bound is based on. Each call receives its own independent
                `seed_known_failed` set object (a copy taken at submission
                time, never the shared known_failed_facilities object
                itself), so two concurrently-running countries never mutate
                the same set at once. Folding the real result back into the
                shared known_failed_facilities set happens only in the main
                thread, after this call returns.

                Why threads, not processes, for the outer concurrency layer:
                _process_one_country_river_isolated()'s own real work already
                happens in a forked child process; this thread's own job is
                just to start that child and block on proc.join() (real
                waiting on a subprocess, not CPU-bound Python work), which
                releases the GIL, so N threads genuinely let N child
                processes run concurrently without needing to change
                _process_one_country_river_isolated()/_run_country_worker_
                river()'s own Process+Queue mechanism at all.

                Returns (country, result_or_None, exception_or_None,
                new_failures_discovered, elapsed_seconds).
                """
                t0 = time.time()
                exc = None
                result = None
                try:
                    result, new_failures = _process_one_country_river_isolated(
                        country, zoom, tiers_to_process, local_paths, tier_rows,
                        admin_levels_by_country, seed_known_failed, _log_level_name)
                    seed_known_failed |= new_failures
                except Exception as e:
                    exc = e
                    # seed_known_failed may already have been mutated in place
                    # by the isolated call's own error-path fold-in, before it
                    # actually failed; the delta computed below still picks
                    # that up correctly either way.
                new_discovered = seed_known_failed
                return country, result, exc, new_discovered, time.time() - t0

            # Submitted incrementally below (not all at once) so at most one
            # RIVER_LARGE_COUNTRIES member is ever in flight - see that
            # constant's docstring. river_pending preserves incoming order
            # (COUNTRY_CODE alphabetical); river_in_flight maps each live
            # future to its country, purely so river_large_running can be
            # decremented correctly when it completes.
            river_pending = list(countries) if tiers_to_process else []
            river_in_flight = {}
            river_large_running = 0

            def _river_try_dispatch(executor):
                """
                Fills any free RIVER_COUNTRY_CONCURRENCY slot(s) from the
                front of river_pending, skipping OVER (never popping) a
                RIVER_LARGE_COUNTRIES candidate while another large country
                is already in flight, so a small country further down
                river_pending can fill the slot instead - a free slot is
                never left idle while a small country is available, and the
                skipped large country simply stays at its own position for
                the next call to this function (made every time a future
                completes, below). Deliberately does NOT block/wait for
                anything itself: called only when at least one slot might be
                free, and is a no-op otherwise (the `while` condition below
                is immediately false).
                """
                nonlocal river_large_running
                i = 0
                while len(river_in_flight) < RIVER_COUNTRY_CONCURRENCY and i < len(river_pending):
                    candidate = river_pending[i]
                    if candidate in RIVER_LARGE_COUNTRIES and river_large_running > 0:
                        i += 1
                        continue
                    river_pending.pop(i)
                    fut = executor.submit(_process_one_country_river_concurrent, candidate, set(known_failed_facilities))
                    river_in_flight[fut] = candidate
                    if candidate in RIVER_LARGE_COUNTRIES:
                        river_large_running += 1

            with ThreadPoolExecutor(max_workers=RIVER_COUNTRY_CONCURRENCY) as executor:
                _river_try_dispatch(executor)
                while river_in_flight:
                    done, _ = wait(river_in_flight.keys(), return_when=FIRST_COMPLETED)
                    for future in done:
                        completed_country = river_in_flight.pop(future)
                        if completed_country in RIVER_LARGE_COUNTRIES:
                            river_large_running -= 1
                        _river_try_dispatch(executor)
                        country, result, exc, new_discovered, elapsed = future.result()
                        # Sole writer of every shared accumulator below: each
                        # completed future is unpacked in this main thread's
                        # dispatch loop, one at a time, never concurrently with
                        # a worker thread, so none of these need a lock.
                        known_failed_facilities |= new_discovered
                        if exc is not None:
                            logger.error(f"River-flood processing failed catastrophically for {country}, skipping this country entirely this cycle: {exc}")
                            if country not in skipped_countries:
                                skipped_countries.append(country)
                            skip_reasons[country] = str(exc)
                        elif result is not None:
                            if result["skipped"]:
                                if country not in skipped_countries:
                                    skipped_countries.append(country)
                                skip_reasons[country] = result.get("skip_reason", "unknown (worker reported skipped=True with no reason)")
                            else:
                                country_files_written = result["files_written"]
                                files_written += country_files_written
                                for rp, delta in result["tier_files_written"].items():
                                    tier_files_written[rp] += delta
                                if country_files_written > 0:
                                    countries_with_output.add(country)
                                # See budget_incomplete_countries_by_tier's own
                                # declaration above for the full "why": this
                                # country reported "ok" (never skipped_countries)
                                # but its own per-country time budget cut off one
                                # or more SPECIFIC tiers, so only THOSE tiers'
                                # own marking gate below must stay blocked for
                                # this country, not every tier uniformly.
                                _budget_incomplete = result.get("budget_incomplete_tiers") or set()
                                for rp in _budget_incomplete:
                                    budget_incomplete_countries_by_tier.setdefault(rp, set()).add(country)
                                if _budget_incomplete:
                                    skip_reasons[country] = (
                                        f"river-flood per-country time budget exceeded before "
                                        f"completing tier(s) {sorted(_budget_incomplete)} this cycle "
                                        f"(other tiers/steps for this country may still have written "
                                        f"real output)"
                                    )

                        logger.info(f"{country}: river-flood processing finished in {elapsed:.1f}s")

                        # See run_precip_analysis's own comment on this same call
                        # for the full "why" (a secondary, cheap safety net for
                        # this driver process's own subprocess/queue plumbing;
                        # the real per-country isolation now happens inside the
                        # subprocess itself) and _release_memory_counter's own
                        # declaration above for why this only fires every 8th
                        # pass, not every single one.
                        _release_memory_counter += 1
                        if _release_memory_counter % RELEASE_MEMORY_EVERY_N_PASSES == 0:
                            _release_memory(logger)

    # Reconciled against countries_with_output, same reasoning as
    # run_precip_analysis's own version: a country that failed on an early
    # tier but succeeded on a later one has real output for this cycle.
    # Computed here (before the marking loop below), not after it, so it can
    # gate that loop: a country truly failing here must block marking too,
    # not just the plain tier_files_written[rp] > 0 check.
    truly_skipped_countries = [c for c in skipped_countries if c not in countries_with_output]

    # Mark each tier's cycle processed so a future run with the same
    # (rp, forecast_time) skips reprocessing (see AMBIENT_HAZARD_RUN_LOG's
    # comment above run_precip_analysis()). Gated on that tier having written
    # a file across any country AND truly_skipped_countries being empty: a
    # tier where every country failed, or where even one country genuinely
    # failed (e.g. an OOM kill on a large flood-prone country like BGD), must
    # stay eligible for retry rather than silently dropping that country's
    # output forever. Also gated per-tier on budget_incomplete_countries_by_
    # tier: a country whose time budget cut off tier rp specifically returns
    # "ok" (not in skipped_countries), so without this gate rp could get
    # marked processed on another country's output alone, permanently
    # skipping retry for this country's own undelivered tier.
    _is_full_run = _countries_cover_full_active_list(countries, logger)
    if not _is_full_run:
        logger.warning(
            f"Not marking any river tier processed in AMBIENT_HAZARD_RUN_LOG: this run's own "
            f"{len(countries)} countries do not cover the real, currently-active country list, so "
            f"this is not a genuine full production run. Marking it done anyway would silently and "
            f"permanently skip every country this run never touched. Only a real full run is allowed "
            f"to mark a cycle processed."
        )
    ambient_conn = None
    if ambient_tracking_enabled:
        try:
            ambient_conn = get_snowflake_connection()
        except Exception as e:
            logger.warning(f"Could not open a fresh Snowflake connection to update AMBIENT_HAZARD_RUN_LOG for river: {e}")
    for rp in tiers_to_process:
        # Per-tier union of two "don't mark this tier done" reasons:
        # truly_skipped_countries (blocks every tier uniformly) and this
        # tier's own budget-incomplete countries (see budget_incomplete_
        # countries_by_tier's declaration above for why these can't merge
        # into one flat list). Recomputed per tier since the latter is
        # tier-specific; truly_skipped_countries is the same for every tier.
        _tier_blockers = set(truly_skipped_countries) | budget_incomplete_countries_by_tier.get(rp, set())
        if ambient_conn is not None and tier_files_written[rp] > 0 and not _tier_blockers and _is_full_run:
            try:
                log_ambient_forecast_processed(ambient_conn, 'river', rp, tier_rows[rp]['forecast_time'])
            except Exception as e:
                logger.warning(f"Could not update AMBIENT_HAZARD_RUN_LOG for river tier '{rp}': {e}")
        elif ambient_conn is not None and tier_files_written[rp] > 0 and _tier_blockers and _is_full_run:
            # Only a real full run's own failures count toward the give-up
            # threshold, so a partial/test invocation can never accidentally
            # push a real cycle toward being given up on.
            try:
                gave_up = record_ambient_failure_attempt(
                    ambient_conn, 'river', rp, tier_rows[rp]['forecast_time'], sorted(_tier_blockers), logger,
                    failure_reasons={c: skip_reasons[c] for c in _tier_blockers if c in skip_reasons})
            except Exception as e:
                logger.warning(f"Could not record the river failure attempt for tier '{rp}': {e}")
                gave_up = False
            if gave_up:
                try:
                    log_ambient_forecast_processed(ambient_conn, 'river', rp, tier_rows[rp]['forecast_time'])
                except Exception as e:
                    logger.warning(f"Could not update AMBIENT_HAZARD_RUN_LOG for river tier '{rp}' after giving up: {e}")
        elif ambient_conn is not None and tier_files_written[rp] > 0 and _tier_blockers and not _is_full_run:
            logger.warning(
                f"River tier '{rp}': {len(_tier_blockers)} countries not yet complete for this tier "
                f"({', '.join(sorted(_tier_blockers))}), but this isn't a full production run, so this "
                f"does not count toward the give-up limit and this tier is not marked processed."
            )

    if ambient_conn is not None:
        ambient_conn.close()

    tiers_processed = len(local_paths) if tiers_to_process else 0
    logger.info(f"River-flood analysis complete for {len(countries)} countries across {tiers_processed}/{len(RIVER_RP_TIERS)} tiers ({files_written} files written)")
    # truly_skipped_countries was already computed above, before the
    # AMBIENT_HAZARD_RUN_LOG marking loop, so it could gate that marking too.
    if truly_skipped_countries:
        logger.error(f"River-flood analysis: {len(truly_skipped_countries)} of {len(countries)} countries skipped entirely this cycle "
                     f"(no river-flood hazard output written for them): {', '.join(truly_skipped_countries)}")
    # Distinct from truly_skipped_countries above (those wrote NOTHING at
    # all this cycle): a budget-incomplete country has real output for some
    # tiers and is only missing others, so this is reported separately and
    # at .warning, not .error.
    _all_budget_incomplete_countries = sorted(set().union(*budget_incomplete_countries_by_tier.values())) if budget_incomplete_countries_by_tier else []
    if _all_budget_incomplete_countries:
        logger.warning(
            f"River-flood analysis: {len(_all_budget_incomplete_countries)} of {len(countries)} countries had "
            f"one or more tiers time-budget-truncated this cycle (real output for other tiers/steps may "
            f"still have been written; the affected tier(s) stay eligible for retry): "
            f"{', '.join(_all_budget_incomplete_countries)}"
        )

    return files_written


# =============================================================================
# PIPELINE STATISTICS CLASS
# =============================================================================
class ImpactPipelineStats:
    """Track pipeline execution statistics"""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.analysis_success = False
        self.countries_processed = 0
        self.views_created = 0
        self.affected_countries = []
        self.errors = []
        self.country_errors = []
        # Set to the real accumulated set by update_storms() on
        # its main path (see that function's own docstring); stays None on
        # an early-exit path (e.g. "no countries specified") that never
        # reached real per-country processing, so a caller must check for
        # None before using it, matching every other early-exit field here
        # (e.g. affected_countries stays []).
        self.known_failed_facilities = None

    def log_summary(self, logger):
        """Log pipeline execution summary"""
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else 0
        
        logger.info("=" * 70)
        logger.info("IMPACT ANALYSIS PIPELINE SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Execution time: {duration:.2f} seconds")
        logger.info(f"Impact analysis: {'SUCCESS' if self.analysis_success else 'FAILED'}")
        logger.info(f"Countries processed: {self.countries_processed}")
        logger.info(f"Views created: {self.views_created}")
        
        if self.errors:
            logger.error("Errors encountered:")
            for error in self.errors:
                logger.error(f"  - {error}")
        
        logger.info("=" * 70)


# =============================================================================
# PIPELINE EXECUTION FUNCTIONS
# =============================================================================
def run_hurricane_pipeline(storm, forecast_time, countries=None, skip_analysis=False, log_level="INFO", zoom=14, skip_gust=False, known_failed_facilities=None):
    """
    Run the complete hurricane impact analysis pipeline for a single
    storm/forecast, tracking execution statistics and handling errors
    gracefully.

    Args:
        storm: Storm name (e.g., 'FUNG-WONG', 'JERRY')
        forecast_time: Forecast time in YYYYMMDDHHMMSS format or 'YYYY-MM-DD HH:MM:SS' format
        countries: List of ISO3 country codes. If None, uses default list.
        skip_analysis: If True, skip the analysis step (useful for testing)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.
        zoom: Zoom level for mercator tiles. Default: 14.
        skip_gust: If True, skip gust envelope processing even if gust data is
            available (wind processing is unaffected either way)
        known_failed_facilities: passed straight through to
            run_complete_impact_analysis() - see that function's own
            docstring for the full "why" and the mutate-in-place contract.

    Returns:
        ImpactPipelineStats: Pipeline execution statistics object containing:
            - analysis_success (bool): Whether analysis completed successfully
            - countries_processed (int): Number of countries processed
            - views_created (int): Number of views created
            - affected_countries (list): Country codes actually affected/processed
            - errors (list): List of error messages if any
            - country_errors (list): Per-country error strings, a subset also
              folded into errors
            - known_failed_facilities: the accumulated set from
              run_complete_impact_analysis(), or None on an early-exit path
              that never reached per-country processing
            - start_time, end_time: Execution timestamps
    """
    logger = setup_logging(log_level)
    stats = ImpactPipelineStats()
    stats.start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info("HURRICANE IMPACT ANALYSIS PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Storm: {storm}")
    logger.info(f"Forecast Time: {forecast_time}")
    logger.info(f"Countries: {countries}")
    logger.info(f"Skip Analysis: {skip_analysis}")
    logger.info("=" * 70)
    
    try:
        # Step 1: Impact Analysis (reads directly from Snowflake)
        if not skip_analysis:
            logger.info("STEP 1: Impact Analysis")
            logger.info("-" * 50)
            
            # Convert forecast time to the format expected by impact analysis
            if isinstance(forecast_time, str) and len(forecast_time) == 19:  # "2025-10-10 00:00:00"
                # Convert to YYYYMMDDHHMMSS format
                dt = datetime.strptime(forecast_time, "%Y-%m-%d %H:%M:%S")
                analysis_date = dt.strftime("%Y%m%d%H%M%S")
            else:
                analysis_date = forecast_time
            
            # Run complete impact analysis orchestration
            analysis_result = run_complete_impact_analysis(storm, analysis_date, countries, logger, zoom, skip_gust=skip_gust, known_failed_facilities=known_failed_facilities)
            
            if analysis_result["success"]:
                stats.analysis_success = True
                stats.countries_processed = analysis_result["countries_processed"]
                stats.views_created = analysis_result["total_views_created"]
                stats.affected_countries = analysis_result["affected_countries"]
                # country_errors can be non-empty even when success is True (at least
                # one country succeeded, so the run stays eligible to avoid a full
                # retry), surface it into stats.errors so it's visible in the log
                # summary and doesn't get silently reported as a clean full success.
                stats.country_errors = analysis_result.get("country_errors", [])
                if stats.country_errors:
                    stats.errors.extend(stats.country_errors)
                if analysis_result.get("skipped"):
                    logger.info("Impact analysis skipped: storm not in range of any country")
                elif stats.country_errors:
                    logger.warning(
                        f"Impact analysis completed with {len(stats.country_errors)} "
                        f"country error(s), other countries succeeded"
                    )
                    logger.info(f"   Envelopes processed: {analysis_result['envelopes_processed']}")
                    logger.info(f"   Countries processed: {stats.countries_processed}")
                    logger.info(f"   Views created: {stats.views_created}")
                else:
                    logger.info(f"Impact analysis completed successfully")
                    logger.info(f"   Envelopes processed: {analysis_result['envelopes_processed']}")
                    logger.info(f"   Countries processed: {stats.countries_processed}")
                    logger.info(f"   Views created: {stats.views_created}")
            else:
                stats.analysis_success = False
                stats.errors.append(f"Analysis failed: {analysis_result['error']}")
                logger.error(f"Impact analysis failed: {analysis_result['error']}")
        else:
            logger.info("STEP 1: Impact Analysis SKIPPED")
            logger.info("-" * 50)
            stats.analysis_success = True  # Mark as success since we skipped it
            logger.info("Impact analysis step skipped")
    
        # Pipeline completion
        stats.end_time = datetime.now()
        
        if stats.analysis_success:
            logger.info("Pipeline completed successfully")
        else:
            logger.error("Pipeline completed with errors")
        
        stats.log_summary(logger)
        return stats
        
    except Exception as e:
        stats.end_time = datetime.now()
        stats.errors.append(f"Pipeline execution error: {str(e)}")
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        stats.log_summary(logger)
        return stats


# =============================================================================
# INITIALIZATION FUNCTIONS
# =============================================================================
def initialize_pipeline(countries, zoom, rewrite, admin_levels=None):
    """
    Initialize the data pipeline by creating base mercator (demographic data)
    and admin-level boundary views, cached after first creation to avoid
    redundant downloads.

    If a country is not yet in PIPELINE_COUNTRIES, it is automatically added with
    ACTIVE=TRUE before initialization proceeds, regardless of DATA_PIPELINE_DB.
    PIPELINE_COUNTRIES is a lightweight registry the dashboard reads to know which
    countries exist at all; it's independent of where a country's own view files
    actually live (Snowflake stage, Blob, or local), so a country initialized in
    BLOB/LOCAL mode still needs a real row here to be discoverable. This registration
    is best-effort: add_country_to_snowflake() already catches connection failures
    internally and returns False rather than raising, so a BLOB/LOCAL run with no
    Snowflake credentials configured at all logs a clear error here and continues
    initializing normally, it does not abort the run.

    Args:
        countries: List of ISO3 country codes (e.g., ['TWN', 'DOM'])
        zoom: Zoom level for mercator tiles (typically 14)
        rewrite: If 1, regenerate existing views; if 0, skip if they exist
        admin_levels: List of admin levels to generate base admin views for (default: [1])

    Returns:
        ImpactPipelineStats: Statistics object with analysis_success=True
    """
    if admin_levels is None:
        admin_levels = [1]
    stats = ImpactPipelineStats()

    for country in countries:
        added = add_country_to_snowflake(
            country_code=country,
            zoom_level=zoom,
        )
        if added:
            logger.info(f"{country}: auto-added to PIPELINE_COUNTRIES (map config will be set automatically from GeoRepo boundary, override via 'Update Country Config' workflow if needed)")

    save_mercator_and_admin_views(countries, zoom, rewrite, admin_levels=admin_levels)
    stats.analysis_success = True

    if os.environ.get("DATA_PIPELINE_DB", "LOCAL").upper() == "SNOWFLAKE":
        conn = None
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS REFRESH")
            cur.execute("CALL AOTS.TC_ECMWF.REFRESH_BASE_LAYER_TABLES()")
            result = cur.fetchone()[0]
            cur.close()
            if result.startswith('PARTIAL') or 'errors:' in result:
                logger.warning(f"Base layer MAT refresh had failures after initialize: {result}")
            else:
                logger.info(f"Base layer MAT tables refreshed after initialize: {result}")
        except Exception as e:
            logger.error(f"Could not refresh base layer tables after initialize: {e}")
        finally:
            if conn is not None:
                conn.close()

    return stats


# =============================================================================
# PATCH FUNCTIONS
# =============================================================================
def patch_pipeline(countries, zoom, columns, log_level="INFO"):
    """
    Backfill specific optional columns in existing mercator parquets without full re-init.

    For each country, calls patch_country_layer() which:
    - Checks for custom CSVs in geodb/custom/ first (takes priority over raster re-processing)
    - Re-runs raster processing for any columns without a custom CSV
    - Re-derives smod_class_l1 whenever smod_class is patched

    Supported columns: population, school_age_population, infant_population, adolescent_population,
    built_surface_m2, smod_class, smod_class_l1, rwi, schools, hcs, shelters, wash, vulnerability,
    admin<N> (e.g. admin2, creates a new base admin parquet for that level)

    For 'vulnerability': reads pre-computed poverty probability data from geodb/vulnerability/
    (generated by vulnerability/fetch_vulnerability_probs.py) and writes moderate_poverty_prob
    and severe_poverty_prob into the base mercator parquet.

    Args:
        countries: List of ISO3 country codes (e.g., ['PNG', 'FJI'])
        zoom: Zoom level matching the existing mercator parquet (typically 14)
        columns: List of column names to patch
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.

    Returns:
        bool: True if all countries patched successfully, False if any failed.
    """
    logger = setup_logging(log_level)
    logger.info(f"Patch mode: updating columns {columns} for countries {countries}")
    all_ok = True
    patched = []
    for country in countries:
        try:
            patch_country_layer(country, zoom, columns)
            patched.append(country)
        except (FileNotFoundError, ValueError) as e:
            logger.error(f"{country}: Patch failed: {e}")
            all_ok = False
        except Exception as e:
            logger.error(f"{country}: Unexpected error during patch: {e}", exc_info=True)
            all_ok = False

    if patched and os.environ.get("DATA_PIPELINE_DB", "LOCAL").upper() == "SNOWFLAKE":
        conn = None
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS REFRESH")
            cur.execute("CALL AOTS.TC_ECMWF.REFRESH_BASE_LAYER_TABLES()")
            result = cur.fetchone()[0]
            cur.close()
            if result.startswith('PARTIAL') or 'errors:' in result:
                logger.warning(f"Base layer MAT refresh had failures after patch: {result}")
                all_ok = False
            else:
                logger.info(f"Base layer MAT tables refreshed after patch: {result}")
        except Exception as e:
            logger.error(f"Could not refresh base layer tables after patch: {e}")
            all_ok = False
        finally:
            if conn is not None:
                conn.close()

    return all_ok


# =============================================================================
# UPDATE FUNCTIONS
# =============================================================================
def update_storms(countries, skip_analysis, log_level, zoom, rewrite, time_delta, target_date=None, target_storm=None, skip_gust=False, known_failed_facilities=None):
    """
    Update pipeline: Process hurricane data from Snowflake for matching storms.

    This function:
    1. Fetches storm data from Snowflake
    2. Filters by date and/or storm name if specified
    3. Processes each matching storm/forecast combination
    4. Skips already-processed storms unless rewrite=1
    5. Tracks processing status:
         - DATA_PIPELINE_DB=SNOWFLAKE: TC_PIPELINE_RUN_LOG table (per storm_id/forecast_time)
         - LOCAL / BLOB: JSON file (storms.json in the results directory)

    Admin levels processed are determined automatically by which base admin parquets
    exist for each country (initialized with --type initialize [--admin N ...]).

    Args:
        countries: List of ISO3 country codes to process
        skip_analysis: If True, skip the analysis step (for testing)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        zoom: Zoom level for mercator tiles
        rewrite: If 1, reprocess existing storms; if 0, skip already processed
        time_delta: Number of days in the past to consider storms (default: 2)
        target_date: Optional specific date to filter (YYYY-MM-DD format). Overrides time_delta.
        target_storm: Optional specific storm name to filter (e.g., 'FUNG-WONG')
        skip_gust: If True, skip gust envelope processing even if gust data is
            available (wind processing is unaffected either way)
        known_failed_facilities: see run_complete_impact_analysis()'s own
            docstring for the full "why" and the mutate-in-place contract.
            None (the default) creates a fresh, run-scoped set here; either
            way, the same set is exposed back on the returned
            stats.known_failed_facilities for a caller to chain into
            run_precip_analysis()/run_river_flood_analysis().

    Returns:
        ImpactPipelineStats: Statistics object with execution results
    """
    logger = setup_logging(log_level)

    if not countries:
        logger.error("No countries specified: nothing to process")
        stats = ImpactPipelineStats()
        stats.errors.append("No countries specified")
        return stats

    snowflake_mode = os.environ.get('DATA_PIPELINE_DB', 'LOCAL').upper() == 'SNOWFLAKE'

    # Tracking state: only one is used depending on mode
    d = None        # JSON tracking (LOCAL / BLOB)
    conn = None     # Snowflake connection (SNOWFLAKE mode)

    if snowflake_mode:
        try:
            conn = get_snowflake_connection()
        except Exception as e:
            logger.warning(f"Could not open Snowflake connection for run logging: {e}")
    else:
        d = load_json_storms()

    stats = ImpactPipelineStats()
    stats.analysis_success = True
    # One set for this whole update_storms() call, mutated in
    # place by every storm's own run_hurricane_pipeline() call below, then
    # exposed via stats so a real caller (main()'s CLI path, both Databricks
    # scheduler notebooks) can chain it forward into run_precip_analysis()/
    # run_river_flood_analysis() afterward. See run_complete_impact_
    # analysis()'s own docstring for the full "why" this exists at all.
    if known_failed_facilities is None:
        known_failed_facilities = set()
    stats.known_failed_facilities = known_failed_facilities
    update_start_time = datetime.now()

    # Storm discovery: SQL against Snowflake's TC_TRACKS (HAZARD_DATA_SOURCE=
    # SNOWFLAKE, the default) or a Blob-native equivalent otherwise - without
    # this dispatch a Blob-only deployment (nothing writing to TC_TRACKS)
    # would never discover a new storm. get_storms_local() (snowflake_utils.py)
    # lists tracks/ and parses filenames, the same shape TC-ECMWF-Forecast-
    # Pipeline writes and get_tracks_local() elsewhere in this file reads.
    #
    # get_snowflake_data() raises on a genuine query failure rather than
    # returning empty (see _execute_query()'s docstring). Wrapped in try/
    # except since it's the first Snowflake read here: a transient failure
    # would otherwise abort this whole invocation before the storm loop
    # starts, taking down the precip/river-flood calls that run after it.
    try:
        if config.HAZARD_DATA_SOURCE == 'SNOWFLAKE':
            storms_df = get_snowflake_data()
        else:
            storms_df = get_storms_local()
    except Exception as e:
        logger.error(f"Could not fetch storm metadata, aborting this update run: {e}")
        stats.analysis_success = False
        stats.errors.append(f"storm discovery failed: {e}")
        if conn:
            conn.close()
        return stats
    storms_df['DATE'] = pd.to_datetime(storms_df['FORECAST_TIME']).dt.date
    storms_df['TIME'] = pd.to_datetime(storms_df['FORECAST_TIME']).dt.strftime('%H:%M')

    if target_date:
        try:
            target_date_obj = pd.to_datetime(target_date).date() if isinstance(target_date, str) else target_date
        except (ValueError, TypeError) as e:
            # Give a clean, actionable error instead of a raw pandas/dateutil
            # traceback for an invalid --date value, matching
            # run_precip_analysis()'s own try/except around this same
            # --date value a few lines later in main().
            logger.error(f"Invalid --date value '{target_date}' (expected YYYY-MM-DD): {e}")
            stats.errors.append(f"Invalid --date value '{target_date}': {e}")
            stats.analysis_success = False
            if conn:
                conn.close()
            return stats
        storms_df = storms_df[storms_df['DATE'] == target_date_obj]
        logger.info(f"Filtering to storms on {target_date_obj} only")

    if target_storm:
        storms_df = storms_df[storms_df['TRACK_ID'] == target_storm]
        logger.info(f"Filtering to storm {target_storm} only")

    if storms_df.empty:
        logger.warning("No storms found matching the specified filters (date and/or storm name)")
        if conn:
            conn.close()
        return stats

    storms_processed = False
    completed_storm_ids = []
    completed_countries = set()
    total_files_written = 0

    for _, row in storms_df.iterrows():
        storm = row['TRACK_ID']
        forecast_date = row['DATE']
        forecast_time_str = row['TIME']
        forecast_time_ts = pd.to_datetime(row['FORECAST_TIME']).to_pydatetime()
        today = datetime.today().date()

        if not (target_date or (today - forecast_date).days < time_delta):
            logger.debug(f"Forecast date {forecast_date} outside time delta ({time_delta} days)")
            continue

        date_str = str(forecast_date).replace('-', '')
        time_str = forecast_time_str.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"

        # --- Deduplication check ---
        already_done = False
        if snowflake_mode and conn:
            try:
                already_done = is_already_processed(conn, storm, forecast_time_ts, countries)
            except Exception as e:
                logger.warning(f"Could not check TC_PIPELINE_RUN_LOG: {e}")
        elif d is not None:
            countries_key = ','.join(sorted(countries))
            storm_key = f"{storm}|{countries_key}"
            already_done = (
                storm_key in d['storms']
                and forecast_datetime_str in d['storms'][storm_key]
            )

        if already_done and rewrite != 1:
            logger.info(f"Storm {storm} at {forecast_datetime_str} already processed (use --rewrite 1 to reprocess)")
            continue

        # --- Log start ---
        if snowflake_mode and conn:
            try:
                log_run_start(conn, storm, forecast_time_ts, countries)
            except Exception as e:
                logger.warning(f"Could not log run start to TC_PIPELINE_RUN_LOG: {e}")

        run_started_at = datetime.now()
        storms_processed = True

        loop_stats = run_hurricane_pipeline(
            storm=storm,
            forecast_time=forecast_datetime_str,
            countries=countries,
            skip_analysis=skip_analysis,
            log_level=log_level,
            zoom=zoom,
            skip_gust=skip_gust,
            known_failed_facilities=known_failed_facilities
        )

        if loop_stats.analysis_success:
            if loop_stats.countries_processed == 0:
                logger.info(f"Storm {storm} at {forecast_datetime_str}: not in range of any country, skipped")
            elif loop_stats.country_errors:
                logger.warning(
                    f"Pipeline completed with {len(loop_stats.country_errors)} country error(s) "
                    f"for storm {storm} at {forecast_datetime_str}, other countries succeeded"
                )
            else:
                logger.info(f"Pipeline completed successfully for storm {storm} at {forecast_datetime_str}")
            stats.countries_processed += loop_stats.countries_processed
            stats.views_created += loop_stats.views_created
            stats.affected_countries.extend(loop_stats.affected_countries)

            # --- Mark success ---
            if snowflake_mode and conn:
                try:
                    # country_errors can be non-empty even here (success=True stays,
                    # matching the existing "at least one country succeeded, don't
                    # force a full retry" design), but the per-country failure detail
                    # must still be visible in the persisted audit trail, not silently
                    # dropped just because the overall run counts as a success.
                    partial_error_msg = (
                        '; '.join(loop_stats.country_errors) if loop_stats.country_errors else None
                    )
                    log_run_complete(
                        conn, storm, forecast_time_ts, success=True,
                        countries=loop_stats.affected_countries,
                        files_written=loop_stats.views_created,
                        error_message=partial_error_msg,
                        started_at=run_started_at,
                    )
                except Exception as e:
                    logger.warning(f"Could not log run success to TC_PIPELINE_RUN_LOG: {e}")
            elif d is not None:
                countries_key = ','.join(sorted(countries))
                storm_key = f"{storm}|{countries_key}"
                if storm_key not in d['storms']:
                    d['storms'][storm_key] = []
                d['storms'][storm_key].append(forecast_datetime_str)

            if loop_stats.countries_processed > 0 and storm not in completed_storm_ids:
                completed_storm_ids.append(storm)
            completed_countries.update(loop_stats.affected_countries)
            total_files_written += loop_stats.views_created

        else:
            logger.error(f"Pipeline with errors for storm {storm} at {forecast_datetime_str}")
            stats.analysis_success = False
            stats.errors.extend(loop_stats.errors)

            # --- Mark failure ---
            if snowflake_mode and conn:
                try:
                    error_msg = '; '.join(loop_stats.errors) if loop_stats.errors else 'Unknown error'
                    log_run_complete(
                        conn, storm, forecast_time_ts, success=False,
                        error_message=error_msg,
                        started_at=run_started_at,
                    )
                except Exception as e:
                    logger.warning(f"Could not log run failure to TC_PIPELINE_RUN_LOG: {e}")

    if not storms_processed:
        logger.info("All matching storms were already processed (use --rewrite 1 to reprocess)")

    # Save JSON tracking for LOCAL / BLOB modes
    if not snowflake_mode and d is not None:
        try:
            save_json_storms(d)
        except Exception as e:
            logger.warning(f"Could not save storms tracking file: {e}")

    # Signal batch completion so *_MAT tables refresh via stream trigger, only
    # meaningful in SNOWFLAKE mode (tells REFRESH_MATERIALIZED_VIEWS() there
    # are new files on @AOTS_ANALYSIS). LOCAL/BLOB output never reaches that
    # stage, so signalling would write a misleading row to TC_PIPELINE_
    # COMPLETE_LOG and needlessly fire TRIGGER_REFRESH_TASK.
    if completed_storm_ids and snowflake_mode:
        try:
            runtime_seconds = int((datetime.now() - update_start_time).total_seconds())
            if conn is None:
                conn = get_snowflake_connection()
            signal_pipeline_complete(
                conn=conn,
                storm_ids=completed_storm_ids,
                countries=list(completed_countries),
                files_written=total_files_written,
                runtime_seconds=runtime_seconds
            )
            logger.info(f"Signalled pipeline completion to Snowflake for storms: {completed_storm_ids} (runtime: {runtime_seconds}s)")
        except Exception as e:
            logger.warning(f"Could not write completion signal to Snowflake: {e}")
    elif completed_storm_ids:
        logger.info(f"DATA_PIPELINE_DB={os.environ.get('DATA_PIPELINE_DB', 'LOCAL')}, skipping Snowflake completion signal for storms: {completed_storm_ids} (no files were written to the real stage)")

    if conn:
        conn.close()

    return stats



# =============================================================================
# MAIN FUNCTION
# =============================================================================
def main():
    """
    Main entry point for the impact analysis pipeline.

    Parses command-line arguments and orchestrates pipeline execution based on
    the specified mode (initialize, update, or patch) and parameters.
    """
    parser = argparse.ArgumentParser(
        description="Hurricane Impact Analysis Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
    # Initialize base data for Taiwan (admin1 only, default)
    python main_pipeline.py --type initialize --countries TWN --zoom 14

    # Initialize with admin1 + admin2 (for countries with good sub-provincial data)
    python main_pipeline.py --type initialize --countries PNG --zoom 14 --admin 1 2

    # Force re-initialization (regenerates all data from scratch)
    python main_pipeline.py --type initialize --countries PNG --rewrite 1

    # Process all recent storms (last 2 days, per --time_delta's default)
    python main_pipeline.py --type update

    # Process storms for a specific date
    python main_pipeline.py --type update --date 2025-11-10

    # Process a specific storm on a specific date
    python main_pipeline.py --type update --date 2025-11-10 --storm FUNG-WONG

    # Backfill optional columns without full re-init
    python main_pipeline.py --type patch --countries PNG --columns built_surface_m2 rwi

    # Add admin2 to a country already initialized with admin1
    python main_pipeline.py --type patch --countries PNG --columns admin2

    # Update population data when a new WorldPop dataset is available
    python main_pipeline.py --type patch --countries PNG --columns population adolescent_population
    """
    )
    
    # ========== Pipeline Mode Arguments ==========
    parser.add_argument(
        "--type",
        type=str,
        default="update",
        choices=["initialize", "update", "patch"],
        help=(
            "Pipeline mode: "
            "'initialize' creates base data layers, "
            "'update' processes storm data, "
            "'patch' backfills specific columns in existing base mercator parquets without full re-init "
            "(use with --columns; default: update)"
        )
    )

    parser.add_argument(
        "--columns",
        nargs="+",
        metavar="COLUMN",
        default=None,
        help=(
            "Columns to patch (only used with --type patch). "
            "Supported: population, school_age_population, infant_population, adolescent_population, "
            "built_surface_m2, smod_class, smod_class_l1, rwi, schools, hcs, shelters, wash, vulnerability. "
            "Use 'vulnerability' to patch moderate_poverty_prob + severe_poverty_prob from geodb/vulnerability/ "
            "(run vulnerability/fetch_vulnerability_probs.py first). "
            "Example: --columns built_surface_m2 rwi"
        )
    )
    
    parser.add_argument(
        "--hazard",
        type=str,
        default="hurricane",
        choices=["hurricane"],
        help="Hazard type to process (currently only 'hurricane' is supported)"
    )
    
    # ========== Data Configuration Arguments ==========
    _DEFAULT_COUNTRIES = ["ATG", "JAM", "BLZ", "NIC", "DOM", "DMA", "GRD", "MSR", "KNA", "LCA", "VCT", "AIA", "VGB"]
    parser.add_argument(
        "--countries",
        nargs="+",
        default=None,
        help="ISO3 country codes to process (e.g., TWN DOM). If not specified, attempts to read from Snowflake PIPELINE_COUNTRIES table. Default: Caribbean countries list."
    )
    
    parser.add_argument(
        "--zoom",
        type=int,
        default=int(os.environ.get("ZOOM_LEVEL", 14)),
        help="Zoom level for mercator tiles (default: 14, or ZOOM_LEVEL env var if set). Higher values = finer resolution but more tiles."
    )

    parser.add_argument(
        "--rewrite",
        type=int,
        default=int(os.environ.get("REWRITE", 0)),
        choices=[0, 1],
        help="Rewrite existing data: 1=regenerate existing views, 0=skip if already exists (default: 0, or REWRITE env var if set)"
    )

    parser.add_argument(
        "--admin",
        nargs="+",
        type=int,
        default=[1],
        metavar="LEVEL",
        help="Admin levels to generate views for (default: 1). E.g. --admin 1 2 generates both admin1 and admin2 views."
    )
    
    # ========== Filtering Arguments (for update mode) ==========
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Process only storms on this specific date (format: YYYY-MM-DD, e.g., '2025-11-10'). Overrides --time_delta."
    )
    
    parser.add_argument(
        "--storm",
        type=str,
        default=None,
        metavar="STORM_NAME",
        help="Process only this specific storm (e.g., 'FUNG-WONG', 'KALMAEGI'). Can be combined with --date."
    )
    
    parser.add_argument(
        "--time_delta",
        type=int,
        default=2,
        help="Number of days in the past to consider storms for analysis (default: 2). Ignored if --date is specified."
    )
    
    # ========== Execution Control Arguments ==========
    parser.add_argument(
        "--skip-analysis",
        action="store_true",
        help="Skip the analysis step (useful for testing pipeline structure without processing data)"
    )

    parser.add_argument(
        "--skip-gust",
        action="store_true",
        help="Skip gust envelope processing even if gust data is available (wind processing is unaffected)"
    )

    parser.add_argument(
        "--skip-precip",
        action="store_true",
        help="Skip precipitation/runoff analysis even if MET_FORECASTS data is available (storm processing is unaffected)"
    )

    parser.add_argument(
        "--skip-river-flood",
        action="store_true",
        help="Skip GloFAS river-flood analysis even if RIVER_FORECASTS data is available (storm processing is unaffected)"
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level (default: INFO). DEBUG provides detailed output for troubleshooting."
    )
    
    args = parser.parse_args()
    
    # Setup logging first
    logger = setup_logging(args.log_level)

    # If --countries wasn't explicitly passed at all (args.countries is still the
    # None sentinel, not a value-equality check against _DEFAULT_COUNTRIES, which
    # would also silently override an operator who deliberately passed exactly that
    # list), try to get from Snowflake table. This allows GitHub Actions to use
    # Snowflake as source of truth.
    if args.countries is None:
        try:
            logger.info("No countries specified, attempting to read from Snowflake table...")
            countries_from_snowflake = get_active_countries_from_snowflake()
            if countries_from_snowflake:
                args.countries = countries_from_snowflake
                logger.info(f"Using {len(args.countries)} countries from Snowflake: {', '.join(args.countries)}")
            else:
                logger.warning("No active countries found in Snowflake table, using default list")
                args.countries = _DEFAULT_COUNTRIES
        except Exception as e:
            logger.warning(f"Could not read countries from Snowflake: {e}. Using default list.")
            args.countries = _DEFAULT_COUNTRIES
    
    # Run pipeline based on hazard type
    if args.hazard == "hurricane":

        if args.type == "initialize":
            stats = initialize_pipeline(args.countries, args.zoom, args.rewrite, admin_levels=args.admin)
        elif args.type == "update":
            stats = update_storms(
                countries=args.countries,
                skip_analysis=args.skip_analysis,
                log_level=args.log_level,
                zoom=args.zoom,
                rewrite=args.rewrite,
                time_delta=args.time_delta,
                target_date=args.date,
                target_storm=args.storm,
                skip_gust=args.skip_gust
            )

            # Precip/runoff is storm-independent ambient data, run once per
            # invocation alongside (not inside) update_storms(), in its own
            # try/except so a precip failure can never affect the storm-
            # processing exit code/stats above, and vice versa.
            #
            # known_failed_facilities: the same set update_storms() just
            # accumulated is passed forward into precip, then river, so a
            # facility-fetch failure wind/gust already discovered is a cache
            # hit here too. None only on an early-exit path that never
            # reached per-country processing, in which case a fresh set works.
            known_failed_facilities = stats.known_failed_facilities
            if known_failed_facilities is None:
                known_failed_facilities = set()

            ambient_files_written = 0
            if not args.skip_precip:
                try:
                    ambient_files_written += run_precip_analysis(args.countries, logger, zoom=args.zoom, target_date=args.date, known_failed_facilities=known_failed_facilities) or 0
                except Exception as e:
                    logger.error(f"Precip analysis failed, storm processing above is unaffected: {e}")
            else:
                logger.info("Skipping precip analysis (--skip-precip)")

            # River-flood (GloFAS x JRC) is also storm-independent ambient
            # data, same reasoning and isolation as precip above.
            if not args.skip_river_flood:
                try:
                    ambient_files_written += run_river_flood_analysis(args.countries, logger, zoom=args.zoom, target_date=args.date, known_failed_facilities=known_failed_facilities) or 0
                except Exception as e:
                    logger.error(f"River-flood analysis failed, storm processing above is unaffected: {e}")
            else:
                logger.info("Skipping river-flood analysis (--skip-river-flood)")

            # Signal completion for precip/river-flood's own ambient files,
            # independent of update_storms()'s signal_pipeline_complete()
            # (which only fires for a named storm). Without this, precip/
            # river output never reaches the dashboard's MAT tables during a
            # storm-quiet period, since nothing else tells
            # REFRESH_MATERIALIZED_VIEWS() these files exist.
            # ambient_files_written > 0 mirrors update_storms()'s own
            # completed_storm_ids gate, for this different source.
            snowflake_mode = os.environ.get('DATA_PIPELINE_DB', 'LOCAL').upper() == 'SNOWFLAKE'
            if ambient_files_written > 0 and snowflake_mode:
                conn = None
                try:
                    conn = get_snowflake_connection()
                    signal_pipeline_complete(
                        conn=conn,
                        storm_ids=[],
                        countries=args.countries,
                        files_written=ambient_files_written,
                    )
                    logger.info(f"Signalled pipeline completion to Snowflake for ambient precip/river-flood data ({ambient_files_written} files written)")
                except Exception as e:
                    logger.warning(f"Could not write ambient completion signal to Snowflake: {e}")
                finally:
                    if conn is not None:
                        conn.close()
            elif ambient_files_written > 0:
                logger.info(f"DATA_PIPELINE_DB={os.environ.get('DATA_PIPELINE_DB', 'LOCAL')}, skipping Snowflake completion signal for ambient precip/river-flood data ({ambient_files_written} files, not written to the real stage)")
        elif args.type == "patch":
            if not args.columns:
                logger.error("--type patch requires --columns (e.g. --columns built_surface_m2 rwi)")
                sys.exit(1)
            ok = patch_pipeline(args.countries, args.zoom, args.columns, args.log_level)
            stats = ImpactPipelineStats()
            stats.analysis_success = ok
    else:
        logger.error(f"Hazard type '{args.hazard}' not yet implemented")
        sys.exit(1)
    
    # Exit with appropriate code
    if stats.analysis_success:
        print("\nPipeline completed successfully!")
        sys.exit(0)
    else:
        print("\nPipeline completed with errors!")
        sys.exit(1)


if __name__ == "__main__":
    main()