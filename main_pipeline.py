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
  impact views at 8 wind thresholds (34-137 kt) plus CCI values
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
import contextlib
import logging
import gc
import ctypes
import ctypes.util
import multiprocessing
import threading
import time
import faulthandler
import signal
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
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
    save_json_slow_countries,
    load_json_slow_countries,
    patch_country_layer,
    load_mercator_view,
    load_admin_view,
    build_tiles_viewer,
    _ensure_unique_zone_ids,
    assign_facilities_to_tiles,
    project_tiles_for_facility_assignment,
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
from sparse_build import PrecipSparseContext, RiverSparseContext, RiverStep, admin_names, popcount_u64
from river_tile_source import (
    AggregatedBymemberSource,
    BymaskSource,
    bymask_param,
    current_rss_mb,
    format_river_mem,
    load_river_tile_bits,
    matching_bymask_row,
)
from river_views import (
    create_river_tile_view,
    create_river_tile_view_from_counts,
    create_river_admin_view,
    create_river_facility_view,
    save_river_tile_view,
    river_country_tier_already_processed,
    save_river_admin_view,
    save_river_school_view,
    save_river_hc_view,
    save_river_shelter_view,
    save_river_wash_view,
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
    get_river_tier_pointers,
    get_latest_river_forecasts_bulk,
    get_river_forecast_for_date,
    get_river_forecasts_data_store,
    download_river_forecast_parquet,
)

# Import gigaspatial for buffering
from gigaspatial.processing import buffer_geodataframe

import json
import shutil
import tempfile
from config import config
from snowflake_utils import get_snowflake_data, get_storms_local, get_snowflake_connection, get_countries_in_range, get_countries_with_null_boundary
from country_utils import get_active_countries_from_snowflake, add_country_to_snowflake
from pipeline_runlog import (
    is_already_processed,
    log_run_start,
    log_run_complete,
    # Re-exported for the Databricks notebooks, which import these two from here.
    ensure_windgust_giveup_table,
    check_and_record_windgust_giveup,
    is_ambient_forecast_processed,
    log_ambient_forecast_processed,
    record_ambient_failure_attempt,
    signal_pipeline_complete,
    trigger_mat_refresh_if_due,
)


# How often _release_memory() actually fires in run_precip_analysis()/
# run_river_flood_analysis()'s own per-country loops (every
# Nth country, not every single one): gc.collect() cost scales with how much is
# currently live, and calling it on every pass adds meaningful wall-clock
# overhead without meaningfully reducing peak memory, so it only fires every
# Nth pass.
RELEASE_MEMORY_EVERY_N_PASSES = 8

# Bounded worker count for _parallel_write_batch()'s per-pass Blob write
# batch, shared by precip and river-flood's own per-country save_*() calls
# (up to ~7 independent writes per pass: bitmask, per-admin-level, per-
# facility-type). Throughput against this Blob container
# plateaus around 1.6-1.7x at 8-12 workers (connection-pool/throttling
# limited, not near-linear); 8 sits at the bottom of that plateau.
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


# STALL-GRACE window for one country's isolated
# wind/gust subprocess (see _process_one_country_isolated() below): killed
# only once this much time has passed with NO real progress signal (see
# heartbeat_value below), not simply once this much wall-clock time has
# elapsed since it started. MEX (Mexico, ~245,000 real schools, by far the
# largest facility volume of any active country) has a baseline per-cycle
# runtime of ~1800s by design (school-view Blob writes alone take
# ~330-375s), and a run under heavy concurrent load on Databricks can take
# far longer and still complete normally. A flat ceiling is either too
# generous for a genuinely wedged subprocess or too tight for slow but
# healthy runs. A hardcoded large-country list would need manual
# maintenance on every country onboarding and goes stale as per-country
# workload shifts, whereas a progress-aware window applies uniformly to
# every country.
# The ceiling is 5400s so that a slow country (a stuck frame inside
# gigaspatial's generic map_points() spatial-join fallback is visible in
# WINDGUST_COUNTRY_WATCHDOG_S's repeat=True dumps) completes well inside the
# window. A subprocess that makes literally ZERO real progress from the
# start is still caught at this same ceiling, so a deadlock or
# deterministic hang is unaffected; only a genuinely-still-progressing
# country gets the benefit of the wider window.
COUNTRY_SUBPROCESS_TIMEOUT_S = 5400

# Diagnostic-only watchdog for _run_country_worker() (wind/gust): if the forked child is
# still alive this many seconds after starting, faulthandler dumps its live stack trace
# instead of leaving a COUNTRY_SUBPROCESS_TIMEOUT_S (5400s) kill with no trace of what it
# was doing. Same value and reasoning as river's own RIVER_COUNTRY_WATCHDOG_S: well under
# COUNTRY_SUBPROCESS_TIMEOUT_S, so it fires repeatedly (repeat=True below) well before any
# real kill, giving a real trace of whichever step turns out to be the slow one; a real
# stall's dumped stack simply won't move across repeated log lines, while genuine progress
# will look identical to a river country's own harmless watchdog firings.
WINDGUST_COUNTRY_WATCHDOG_S = 600

# Poll interval for the stall-aware supervision loop below. Implemented as
# proc.join(COUNTRY_SUBPROCESS_POLL_S) (never sleep()+is_alive()), which
# returns as soon as the child exits, so a fast-completing country is
# detected with near-zero latency; polling more often than this buys
# negligible extra responsiveness at real per-save-step durations
# (single-digit seconds at minimum, tens of seconds for MEX's own heaviest
# step).
COUNTRY_SUBPROCESS_POLL_S = 30

# Reserved off the tail of any caller-supplied deadline_ts (see
# _process_one_country_isolated()'s own docstring) before the stall-aware
# loop will actually stop extending: the real termination sequence itself
# (proc.terminate(), then up to a further 30s grace, then proc.kill()) takes
# real time, so "stop extending" must fire this much before the true
# deadline, not exactly at it, or the real process end could slip past the
# caller's own intended budget.
COUNTRY_DEADLINE_MARGIN_S = 90

# Two-pass per-country time budgets for run_complete_impact_analysis()'s own
# country loop below (see that function's own docstring for the full "why").
# Real, deliberate design: rather than one country get up to
# COUNTRY_SUBPROCESS_TIMEOUT_S (or longer, via the heartbeat-based extension
# above) while everyone else in the same batch waits behind it, PASS 1 gives
# every country a short shot; a country that's still genuinely working
# (heartbeat active, not stalled) when its own pass-1 budget runs out is
# DEFERRED, not failed, and the loop immediately moves on to the next
# country. PASS 2 (after every country has had its pass-1 turn) retries only
# the deferred ones, with more time, and a country still not done even
# then is deferred a second time, to TIER 3: a separate, independently-
# scheduled Databricks job (04_scheduler_windgust.py run with larger
# max_run_minutes, pass1_cap_s and pass2_cap_s widget values, not bound by the
# main job's own fixed 16x/day cadence) with a much larger budget, since by definition nothing else
# is competing for ITS time slot. Real memory of who has needed this is kept
# dynamically (see impact_analysis.py's slow_countries.json helpers):
# deliberately NOT a hardcoded large-country list, since such a list needs
# manual maintenance on every country onboarding and goes stale as real
# per-country workload shifts, whereas this is learned from real run
# outcomes and self-corrects on its own.
# Pass 1 is deliberately kept short for the MAIN job (a country deferred
# here is NOT lost, it gets reprocessed later this same run in pass 2, so
# other queued countries never wait on it). Pass 2 is long enough to finish the
# largest real countries from scratch. A separate tier-3 notebook job can still
# override both values via job parameters, reusing this exact same code path.
COUNTRY_PASS1_CAP_S = 600    # ~10 min: try fast, defer quickly if not done
COUNTRY_PASS2_CAP_S = 5400   # ~90 min: a retry restarts the country from scratch, so
                              # this must cover the whole run for the largest real
                              # gust footprints (MEX, 250K-340K envelope points,
                              # can exceed COUNTRY_PASS1_CAP_S);
                              # genuine COUNTRY_SUBPROCESS_TIMEOUT_S stall-grace
                              # still applies within this, so a truly-stalled
                              # (not just slow) country is correctly caught as
                              # a real failure here, not deferred a second time


class _CountryStillWorkingError(RuntimeError):
    """
    Raised by _process_one_country_isolated() instead of a plain
    RuntimeError specifically when a country's subprocess was killed while
    STILL making real progress (heartbeat active) because its caller-given
    deadline_ts was reached; never for a genuine stall (heartbeat idle for
    COUNTRY_SUBPROCESS_TIMEOUT_S), which always raises plain RuntimeError
    instead. Lets run_complete_impact_analysis()'s own two-pass loop tell
    "ran out of THIS budget, still worth retrying with more time" apart from
    "genuinely stuck, a real failure" without string-matching the exception
    message.
    """
    pass

# Serializes the actual os.fork() moment (the proc.start() call itself, never
# anything else) across every isolated-country-subprocess call site in this
# module: _process_one_country_isolated() below (wind/gust) and
# _run_country_subprocess() (precip/river).
_FORK_LOCK = threading.Lock()


def _run_country_worker(country, storm, date, zoom, gdf_envelopes, gdf_envelopes_gust,
                         admin_levels_by_country, known_failed_facilities, log_level, result_queue,
                         heartbeat_value=None, skip_wind=False):
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

    heartbeat_value: optional multiprocessing.Value('d', ...), shared with
    the parent (see _process_one_country_isolated()'s own docstring). Wrapped
    here into a plain zero-arg callable passed as
    create_views_from_envelopes_in_country()'s own `heartbeat` parameter,
    which touches it at every real save checkpoint. None (the default) is a
    safe no-op, matching that function's own contract.

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

    def _touch_heartbeat():
        if heartbeat_value is not None:
            heartbeat_value.value = time.time()

    def _report_wind_checkpoint(wrote_base, files_written):
        # Puts wind's already-complete
        # result on the SAME queue the final "ok"/"error" message uses, tagged
        # distinctly, BEFORE gust processing starts. See create_views_from_envelopes_
        # in_country()'s own on_wind_complete docstring for the full "why", and
        # _process_one_country_isolated()'s own draining logic for how the parent
        # uses this if this whole process later has to be killed during gust.
        result_queue.put(("wind_checkpoint", (wrote_base, files_written)))

    # Diagnostic watchdog, same threshold and reasoning as _run_country_worker_river()'s own
    # RIVER_COUNTRY_WATCHDOG_S (see that constant's docstring for the full "why"), but
    # dumping to stderr only: if this
    # child is still alive WINDGUST_COUNTRY_WATCHDOG_S after starting, dump its real live
    # stack trace to stderr, well before the COUNTRY_SUBPROCESS_TIMEOUT_S heartbeat-idle
    # kill. A kill at the COUNTRY_SUBPROCESS_TIMEOUT_S ceiling would otherwise leave zero trace of what the child
    # was actually stuck on (see COUNTRY_SUBPROCESS_TIMEOUT_S for why the ceiling is 5400s). Cancelled
    # unconditionally in the `finally` below on every exit path so a country finishing just
    # past this threshold never produces a spurious dump.
    # repeat=True: a one-shot dump can fire once on an early, legitimately-slow-but-completing
    # step (a shapely STRtree query), then the SAME run can genuinely stall for good at a
    # LATER step with no further dump to show it. A one-shot timer can only ever catch the
    # FIRST slow patch in a run, not
    # whichever one turns out to be fatal. Repeating dumps every WINDGUST_COUNTRY_WATCHDOG_S
    # until cancelled below guarantees a real trace of whatever is actually stuck at the end.
    faulthandler.dump_traceback_later(WINDGUST_COUNTRY_WATCHDOG_S, repeat=True, file=sys.stderr)
    try:
        result = create_views_from_envelopes_in_country(
            country, storm, date, gdf_envelopes, zoom, gdf_envelopes_gust=gdf_envelopes_gust,
            admin_levels_by_country=admin_levels_by_country, heartbeat=_touch_heartbeat,
            on_wind_complete=_report_wind_checkpoint, skip_wind=skip_wind)
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
    finally:
        faulthandler.cancel_dump_traceback_later()


def _write_gust_pending_marker(storm, date, country, logger):
    """
    Drops an empty marker file at gust_pending/{storm}_{date}_{country}.marker,
    signalling to the separate, delayed gust scheduler (databricks/07_scheduler_
    gust.py) that this country's wind result is durably saved and its gust
    envelope (if any) is safe to process independently. Written unconditionally
    whenever a wind checkpoint is drained (regardless of skip_gust), since a
    redundant marker is harmless: the gust job's own idempotency check (does
    gust output already exist?) is what actually decides whether there is real
    work, not the mere presence of a marker.

    Uses the same data_store abstraction (LOCAL/BLOB/SNOWFLAKE-mode aware) as
    every other file write in this codebase, not a separate Blob client, so
    this respects whatever DATA_PIPELINE_DB mode the caller
    is already running under. Best-effort: a failure here must never fail a
    real, already-successful wind result, so it's logged and swallowed, same
    philosophy as log_run_start()/log_run_complete() elsewhere in this codebase.
    """
    try:
        marker_path = os.path.join(pipeline_shared.ROOT_DATA_DIR, pipeline_shared.VIEWS_DIR,
                                    'gust_pending', f"{storm}_{date}_{country}.marker")
        pipeline_shared.data_store.write_file(marker_path, b"")
    except Exception as e:
        logger.warning(f"{country}: could not write gust_pending marker for {storm}/{date}: {e}")


def _process_one_country_isolated(country, storm, date, gdf_envelopes, zoom, gdf_envelopes_gust,
                                   admin_levels_by_country, known_failed_facilities, log_level,
                                   deadline_ts=None, skip_wind=False):
    """
    Returns ((wrote_base, files_written, gust_error), new_failures); raises
    on real failure. Same create_views_from_envelopes_in_country() call
    contract, just paired with new_failures (see
    _seed_facility_failure_cache()'s own docstring) since fork isolation
    would otherwise reset the cross-country facility-fetch-failure cache on
    every single country. Runs the actual computation in a fresh, throwaway
    'fork' subprocess instead of inline in the driver process.

    Supervision is progress-aware, not a flat wall-clock timeout (see
    COUNTRY_SUBPROCESS_TIMEOUT_S's own module-level comment for the full real
    "why": MEX's uniquely large facility volume making a flat ceiling
    either too tight for real work or too generous for a real stall). The
    child touches a shared heartbeat_value (a multiprocessing.Value, via
    create_views_from_envelopes_in_country()'s own `heartbeat` callback) at
    every real save checkpoint; the parent polls every
    COUNTRY_SUBPROCESS_POLL_S and keeps waiting as long as that heartbeat has
    moved within the last COUNTRY_SUBPROCESS_TIMEOUT_S seconds, however long
    the subprocess has been running in total. A subprocess that never
    advances the heartbeat at all (true deadlock/stall from the start) is
    still caught at exactly COUNTRY_SUBPROCESS_TIMEOUT_S.

    deadline_ts: optional absolute Unix epoch timestamp (not a "remaining
    minutes" figure, to avoid recomputation drift across a multi-country
    loop). When given, extension past COUNTRY_SUBPROCESS_TIMEOUT_S stops
    once real time reaches deadline_ts minus COUNTRY_DEADLINE_MARGIN_S (that
    margin reserves real time for the terminate/kill sequence below, which
    itself takes up to ~30s), even if the heartbeat is still actively
    advancing: this is what lets a caller (see run_complete_impact_
    analysis()'s own deadline_ts threading, and 04_scheduler_windgust.py's
    own real MAX_RUN_MINUTES-derived deadline) bound how much of its own
    real run budget one legitimately-slow country can consume, instead of a
    country's own extension being unbounded. None (the default) means no
    such outer bound; only the heartbeat-based stall-grace applies.

    Uses 'fork', not 'spawn': the real call site (run_complete_impact_analysis(),
    invoked directly from Databricks notebook cells with no __main__ guard,
    which notebooks never have) would make 'spawn' re-execute that notebook's own
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
    # 'd' = C double, plenty of range/precision for a Unix timestamp. Starts
    # at the fork time, not 0.0, so the very first stall check below
    # measures real elapsed time from the start, not from the Unix epoch.
    heartbeat_value = ctx.Value('d', time.time())
    proc = ctx.Process(
        target=_run_country_worker,
        args=(country, storm, date, zoom, gdf_envelopes, gdf_envelopes_gust,
              admin_levels_by_country, known_failed_facilities, log_level, result_queue),
        kwargs={"heartbeat_value": heartbeat_value, "skip_wind": skip_wind},
    )
    # See _FORK_LOCK's own module-level docstring for the full "why": only
    # the literal os.fork() instant is serialized, never the wait below.
    with _FORK_LOCK:
        proc.start()

    # final_result: the real ("ok"/"error", payload) message the child puts exactly once,
    # at the very end of _run_country_worker(). wind_checkpoint: the EARLIER, optional
    # ("wind_checkpoint", (wrote_base, files_written)) message _report_wind_checkpoint() puts as soon as wind
    # alone has fully succeeded and saved, before gust starts (see that closure's own
    # comment). Drained here, non-blocking, on every poll, not just once at the very
    # end, specifically so a later kill of the whole subprocess (gust hangs, the stall
    # watchdog below fires) doesn't destroy a wind result that already safely reached the
    # parent via this queue. Without the checkpoint, any kill would discard wind's own
    # already-good result along with it.
    final_result = None
    wind_checkpoint = None
    # Set the instant the marker below is written, so a wind_checkpoint drained
    # on a later poll (the message is already latched into wind_checkpoint by
    # then) never triggers a second, redundant write this same call.
    gust_marker_written = False

    def _drain_queue():
        nonlocal final_result, wind_checkpoint, gust_marker_written
        while not result_queue.empty():
            tag, payload = result_queue.get_nowait()
            if tag == "wind_checkpoint":
                wind_checkpoint = payload
                if not gust_marker_written:
                    _write_gust_pending_marker(storm, date, country, logging.getLogger(__name__))
                    gust_marker_written = True
            else:
                final_result = (tag, payload)

    stalled = False
    past_deadline = False
    effective_deadline = (deadline_ts - COUNTRY_DEADLINE_MARGIN_S) if deadline_ts is not None else None
    while True:
        # proc.join(timeout), not sleep()+is_alive(): returns the instant the
        # child exits, so a fast-completing country is detected with the
        # same near-zero latency as a plain proc.join(...).
        proc.join(COUNTRY_SUBPROCESS_POLL_S)
        _drain_queue()
        if not proc.is_alive():
            break
        now = time.time()
        stalled = (now - heartbeat_value.value) >= COUNTRY_SUBPROCESS_TIMEOUT_S
        past_deadline = effective_deadline is not None and now >= effective_deadline
        if stalled or past_deadline:
            break

    if proc.is_alive():
        proc.terminate()
        proc.join(30)
        _drain_queue()  # one more chance to catch a checkpoint flushed during termination
        if proc.is_alive():
            proc.kill()
            proc.join()
        if past_deadline and not stalled:
            # Still genuinely making progress (heartbeat recent), but the
            # caller's own real run budget ran out first: a different, real
            # condition from a stall. _CountryStillWorkingError (not plain
            # RuntimeError), specifically so run_complete_impact_analysis()'s
            # own two-pass loop can tell "worth retrying with more time" apart
            # from "genuinely stuck" without string-matching the message.
            # Deliberately does NOT use wind_checkpoint here even if one arrived: this
            # path means the SCHEDULER's own outer budget ran out, not a per-country
            # problem, so the existing "retry the whole country with more time" semantics
            # are left unchanged rather than special-cased.
            raise _CountryStillWorkingError(
                f"{country}: subprocess still making real progress but the scheduler's own "
                f"run budget (deadline_ts) was reached and it was terminated"
            )
        if wind_checkpoint is not None and final_result is None:
            # Wind already fully succeeded and
            # saved before gust hung and this whole subprocess had to be killed. Return
            # wind's real result instead of discarding it, surfacing gust's incompleteness
            # via gust_error, the exact same (wrote_base, files_written, gust_error)
            # contract create_views_from_envelopes_in_country()'s own in-process gust
            # try/except already uses for a caught gust exception, so every downstream
            # consumer (TC_PIPELINE_RUN_LOG's COUNTRIES_PROCESSED, _wind_ready_at in the
            # dashboard) already treats this correctly with no further changes needed.
            wrote_base, files_written = wind_checkpoint
            gust_error = (f"gust processing did not complete: subprocess exceeded "
                          f"{COUNTRY_SUBPROCESS_TIMEOUT_S}s with no progress and was terminated")
            return (wrote_base, files_written, gust_error), set()
        raise RuntimeError(
            f"{country}: subprocess exceeded {COUNTRY_SUBPROCESS_TIMEOUT_S}s with no progress "
            f"and was terminated"
        )

    if final_result is None:
        # Killed before it could report anything at all, e.g. the child's
        # OWN out-of-memory kill (SIGKILL, exitcode -9) or a native-library
        # segfault. This is exactly the case this isolation is meant to
        # contain: the exit code tells us it died, but the process boundary
        # means it took nothing else down with it.
        raise RuntimeError(
            f"{country}: subprocess exited with code {proc.exitcode} before returning a result "
            f"(likely killed by the OS, e.g. an out-of-memory condition contained to this one country)"
        )

    status, payload = final_result
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


# Hang diagnostics for one country's isolated child (river-flood arms it; any
# other hazard can opt in by calling _arm_country_hang_dump() in its worker).
# HANG_DIAG=0 turns every part of this off and restores the plain stderr watchdog.
# The child writes faulthandler stack dumps to <tmp>/aots_hang_diag/<child pid>.txt
# (repeating every HANG_DIAG_AFTER_S seconds, default RIVER_COUNTRY_WATCHDOG_S) and
# also on SIGUSR1; the parent reads that file when the child is still alive after
# HANG_DIAG_AFTER_S plus HANG_DIAG_DUMP_GRACE_S seconds, and again just before it
# kills the child, and uploads the dump plus process context to Blob (best effort).
HANG_DIAG_BLOB_PREFIX = "diagnostics/river_hang"
HANG_DIAG_DUMP_GRACE_S = 20
HANG_DIAG_MAX_DUMP_CHARS = 200000

# Keeps the dump file object referenced for the life of the child, since the
# faulthandler timer writes to its raw file descriptor.
_HANG_DIAG_CHILD_FILE = []


def _hang_diag_enabled():
    return os.environ.get("HANG_DIAG", "1").strip() != "0"


def _hang_diag_after_s():
    try:
        value = float(os.environ.get("HANG_DIAG_AFTER_S", ""))
        if value > 0:
            return value
    except ValueError:
        pass
    return float(RIVER_COUNTRY_WATCHDOG_S)


def _hang_diag_dump_path(pid):
    return os.path.join(tempfile.gettempdir(), "aots_hang_diag", f"{pid}.txt")


def _arm_country_hang_dump(legacy_after_s):
    """
    Called first thing in a forked country child. With HANG_DIAG on, arms
    faulthandler to dump every thread's stack into this child's dump file after
    HANG_DIAG_AFTER_S seconds (repeating) and whenever SIGUSR1 arrives. With
    HANG_DIAG=0, or if the file cannot be set up, arms the plain one-shot stderr
    watchdog after legacy_after_s seconds. Never raises.
    """
    if _hang_diag_enabled():
        try:
            path = _hang_diag_dump_path(os.getpid())
            os.makedirs(os.path.dirname(path), exist_ok=True)
            dump_file = open(path, "w")
            dump_file.write(f"ARMED pid={os.getpid()} unix_time={time.time():.3f}\n")
            dump_file.flush()
            _HANG_DIAG_CHILD_FILE.append(dump_file)
            faulthandler.dump_traceback_later(_hang_diag_after_s(), repeat=True, file=dump_file)
            if hasattr(faulthandler, "register") and hasattr(signal, "SIGUSR1"):
                faulthandler.register(signal.SIGUSR1, file=dump_file, all_threads=True)
            return
        except Exception:
            pass
    if _fork_safe_enabled():
        # Raw file descriptor: faulthandler never touches the (possibly stuck)
        # sys.stderr object, so arming cannot block on an inherited stream lock.
        try:
            faulthandler.dump_traceback_later(legacy_after_s, file=2)
            return
        except Exception:
            pass
    faulthandler.dump_traceback_later(legacy_after_s, file=sys.stderr)


def _disarm_country_hang_dump():
    """Cancels everything _arm_country_hang_dump() scheduled. Never raises."""
    try:
        faulthandler.cancel_dump_traceback_later()
        if _HANG_DIAG_CHILD_FILE and hasattr(faulthandler, "unregister") and hasattr(signal, "SIGUSR1"):
            faulthandler.unregister(signal.SIGUSR1)
    except Exception:
        pass


def _hang_diag_cleanup(pid):
    """Removes the child's dump file from the parent side. Never raises."""
    try:
        os.remove(_hang_diag_dump_path(pid))
    except OSError:
        pass


def _hang_diag_process_context(pid):
    """Best-effort lines describing a live child: rss, threads, fds. Never raises."""
    lines = []
    try:
        import psutil
        proc = psutil.Process(pid)
        with proc.oneshot():
            lines.append(f"status={proc.status()} rss_mb={proc.memory_info().rss / 1048576:.1f} "
                         f"num_threads={proc.num_threads()}")
            cpu = proc.cpu_times()
            lines.append(f"cpu_user_s={cpu.user:.1f} cpu_system_s={cpu.system:.1f}")
        try:
            lines.append(f"open_fds={proc.num_fds()}")
        except Exception as e:
            lines.append(f"open_fds=unavailable ({e})")
        try:
            lines.append("child_threads=" + ", ".join(
                f"{t.id}(user={t.user_time:.1f}s sys={t.system_time:.1f}s)" for t in proc.threads()))
        except Exception as e:
            lines.append(f"child_threads=unavailable ({e})")
    except Exception as e:
        lines.append(f"psutil_context=unavailable ({e})")
    try:
        task_dir = f"/proc/{pid}/task"
        for tid in sorted(os.listdir(task_dir), key=int):
            try:
                with open(f"{task_dir}/{tid}/stat") as f:
                    state = f.read().rsplit(")", 1)[1].split()[0]
                with open(f"{task_dir}/{tid}/wchan") as f:
                    wchan = f.read().strip() or "-"
                lines.append(f"task {tid}: state={state} wchan={wchan}")
            except Exception:
                continue
    except Exception:
        pass
    return lines


def _hang_diag_report(proc, country, hazard_label, diag_context, t_start_monotonic, reason, signal_child):
    """
    Uploads the child's stack dump plus process context to Blob under
    HANG_DIAG_BLOB_PREFIX and logs a WARNING with the path. Returns the Blob
    path, or None when nothing was uploaded. Does nothing if the child never
    armed its dump file. Never raises.
    """
    log = logging.getLogger(__name__)
    try:
        pid = proc.pid
        if pid is None:
            return None
        dump_path = _hang_diag_dump_path(pid)
        wall_start = time.time() - (time.monotonic() - t_start_monotonic)
        if not os.path.exists(dump_path) or os.path.getmtime(dump_path) < wall_start - 5:
            return None
        if signal_child and hasattr(signal, "SIGUSR1"):
            try:
                os.kill(pid, signal.SIGUSR1)
                time.sleep(1.0)
            except Exception:
                pass
        with open(dump_path, errors="replace") as f:
            dump_text = f.read()
        if len(dump_text) > HANG_DIAG_MAX_DUMP_CHARS:
            dump_text = dump_text[-HANG_DIAG_MAX_DUMP_CHARS:]
        now_utc = datetime.now(timezone.utc)
        forecast = str((diag_context or {}).get("forecast") or "unknown")
        header = [
            f"hazard={hazard_label} country={country} forecast={forecast}",
            f"reason={reason} child_pid={pid} child_alive={proc.is_alive()}",
            f"elapsed_s={time.monotonic() - t_start_monotonic:.1f} report_utc={now_utc.isoformat()}",
            f"parent_threads={[t.name for t in threading.enumerate()]}",
        ]
        header.extend(_hang_diag_process_context(pid))
        text = "\n".join(header) + "\n\n----- faulthandler dump -----\n" + dump_text
        blob_path = f"{HANG_DIAG_BLOB_PREFIX}/{country}_{forecast}_{now_utc:%Y%m%dT%H%M%SZ}.txt"
        # Held across the upload so no other country fork can start while the
        # upload's own helper threads exist.
        with _FORK_LOCK:
            pipeline_shared.data_store.write_file(blob_path, text)
        log.warning(f"{hazard_label} {country}: child still running after "
                    f"{time.monotonic() - t_start_monotonic:.0f}s ({reason}); stack dump and process "
                    f"context uploaded to {blob_path}")
        log.warning(f"{hazard_label} {country}: dump excerpt:\n{dump_text[:3000]}")
        return blob_path
    except Exception as e:
        log.warning(f"{hazard_label} {country}: could not upload hang diagnostics: {e}")
        return None


# Stall-aware supervision for a country child that opts in (river-flood does).
# The child touches a heartbeat file at each setup stage and each tier/step
# boundary; the parent stops waiting for it once the file has not moved for
# RIVER_STALL_TIMEOUT_S seconds (0 disables this and leaves only the flat
# COUNTRY_SUBPROCESS_TIMEOUT_S). A child killed for a stall surfaces as the same
# RuntimeError path as the flat timeout, so the country is skipped, recorded in
# AMBIENT_HAZARD_FAILURE_LOG and retried on the next poll like any other failed country.
# The default sits far above the longest healthy gap between boundaries
# (a country takes a few seconds to about two minutes in total, MEX included).
RIVER_STALL_TIMEOUT_S_DEFAULT = 900
COUNTRY_STALL_POLL_S = 15

# Path of this child's heartbeat file; None in the parent and in any child that
# did not call _start_country_heartbeat().
_STALL_HB_PATH = None


def _stall_timeout_s():
    try:
        value = float(os.environ.get("RIVER_STALL_TIMEOUT_S", RIVER_STALL_TIMEOUT_S_DEFAULT))
    except ValueError:
        value = float(RIVER_STALL_TIMEOUT_S_DEFAULT)
    return max(value, 0.0)


def _stall_heartbeat_path(pid):
    return os.path.join(tempfile.gettempdir(), "aots_stall_hb", f"{pid}.hb")


def _start_country_heartbeat():
    """Called first in a forked country child. Never raises."""
    global _STALL_HB_PATH
    try:
        path = _stall_heartbeat_path(os.getpid())
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w"):
            pass
        _STALL_HB_PATH = path
    except Exception:
        _STALL_HB_PATH = None


def _country_heartbeat():
    """Marks progress. A cheap no-op outside a child that started a heartbeat. Never raises."""
    if _STALL_HB_PATH is not None:
        try:
            os.utime(_STALL_HB_PATH, None)
        except Exception:
            pass


def _stop_country_heartbeat():
    global _STALL_HB_PATH
    _STALL_HB_PATH = None


def _stall_heartbeat_age_s(pid, wait_started_wall):
    """Seconds since the child last made progress. Counts from the start of the wait until the first beat."""
    try:
        last = os.path.getmtime(_stall_heartbeat_path(pid))
    except OSError:
        last = wait_started_wall
    return time.time() - max(last, wait_started_wall)


def _stall_cleanup(pid):
    try:
        os.remove(_stall_heartbeat_path(pid))
    except OSError:
        pass


# Fork safety for the per-country children (precip and river-flood both go through
# _run_country_subprocess). RIVER_FORK_SAFE=0 turns every part of it off.
#
# A child forked while another parent thread is writing to sys.stdout or sys.stderr
# inherits that stream's internal lock in the held state. No thread of the child can
# ever release it, so the child's first log line blocks forever. Three measures:
#   parent: hold every logging handler lock across the fork itself, so no handler is
#           in the middle of a write at that instant;
#   child:  before anything else, check whether an inherited stream is stuck, and swap
#           a stuck one for a fresh stream over a duplicate of the same file descriptor
#           (RIVER_FORK_SAFE_STREAMS: probe (default), always, off);
#   parent: take the child's result off the pipe while the child is still running, so a
#           payload larger than the pipe buffer cannot keep the child from exiting.
RIVER_FORK_SAFE_LOCK_TIMEOUT_S = 5.0
CHILD_STREAM_PROBE_TIMEOUT_S = 2.0
RESULT_DRAIN_POLL_S = 0.25
RESULT_DRAIN_AFTER_S = 1.0
RESULT_DRAIN_TIMEOUT_S = 60.0

# True once the "threads active before fork" warning has been logged by this process.
_FORK_THREADS_WARNED = False


def _fork_safe_enabled():
    return os.environ.get("RIVER_FORK_SAFE", "1").strip() != "0"


def _child_stream_mode():
    value = os.environ.get("RIVER_FORK_SAFE_STREAMS", "probe").strip().lower()
    if value == "0":
        return "off"
    return value if value in ("probe", "always", "off") else "probe"


def _all_logging_handlers():
    """Every handler attached to the root logger or any existing logger, each once."""
    loggers = [logging.getLogger()]
    loggers.extend(lg for lg in list(logging.Logger.manager.loggerDict.values())
                   if isinstance(lg, logging.Logger))
    handlers = {}
    for lg in loggers:
        for handler in list(lg.handlers):
            handlers[id(handler)] = handler
    return list(handlers.values())


@contextlib.contextmanager
def _logging_quiesced_for_fork():
    """
    Holds every logging handler lock while the body (the fork) runs, so no handler is
    mid-write when the child's memory is copied. Handler locks are taken in a fixed
    order with a bounded wait; a handler whose lock cannot be had in time is skipped
    and counted in the yielded dict, never waited on indefinitely. The parent releases
    everything on exit; the child gets fresh handler locks from logging's own at-fork
    hook and from _repair_child_streams(). Does nothing when RIVER_FORK_SAFE=0.
    """
    held = []
    info = {"held": 0, "timed_out": 0}
    if _fork_safe_enabled():
        for handler in sorted(_all_logging_handlers(), key=id):
            lock = getattr(handler, "lock", None)
            if lock is None:
                continue
            try:
                if lock.acquire(timeout=RIVER_FORK_SAFE_LOCK_TIMEOUT_S):
                    held.append(lock)
                else:
                    info["timed_out"] += 1
            except Exception:
                info["timed_out"] += 1
    info["held"] = len(held)
    try:
        yield info
    finally:
        for lock in reversed(held):
            try:
                lock.release()
            except Exception:
                pass


def _stream_is_stuck(stream, timeout_s):
    """
    True when writing to and flushing the stream does not finish within timeout_s.
    Only meaningful in a freshly forked child, where no other thread exists, so a
    lock that is held at this point can never be released.
    """
    finished = threading.Event()

    def _probe():
        try:
            stream.write("")
            stream.flush()
        except Exception:
            pass
        finished.set()

    threading.Thread(target=_probe, name="aots-stream-probe", daemon=True).start()
    return not finished.wait(timeout_s)


def _fresh_stream_over_fd(old_stream, fd):
    """A new line-buffered text stream over a duplicate of fd, same encoding as old_stream."""
    encoding = getattr(old_stream, "encoding", None) or "utf-8"
    try:
        "".encode(encoding)
    except LookupError:
        encoding = "utf-8"
    return open(os.dup(fd), "w", buffering=1, encoding=encoding, errors="backslashreplace", closefd=True)


def _repair_child_streams():
    """
    First thing in a forked country child. Gives every logging handler a fresh lock
    and, per RIVER_FORK_SAFE_STREAMS, replaces sys.stdout/sys.stderr by fresh streams
    over duplicates of file descriptors 1 and 2: in "probe" mode only a stream whose
    inherited lock is stuck, in "always" mode both, in "off" mode neither. Handlers
    that wrote to a replaced stream are repointed to the new one. Never raises.
    """
    replaced = []
    try:
        mode = _child_stream_mode()
        if mode != "off":
            for name, fd in (("stdout", 1), ("stderr", 2)):
                old = getattr(sys, name, None)
                if old is None:
                    continue
                try:
                    if mode == "probe" and not _stream_is_stuck(old, CHILD_STREAM_PROBE_TIMEOUT_S):
                        continue
                    new = _fresh_stream_over_fd(old, fd)
                except Exception:
                    continue
                setattr(sys, name, new)
                replaced.append((name, old, new))
        for handler in _all_logging_handlers():
            try:
                handler.createLock()
            except Exception:
                pass
            stream = getattr(handler, "stream", None)
            for _name, old, new in replaced:
                if stream is old:
                    handler.stream = new
        if mode == "probe" and replaced:
            sys.stderr.write(f"fork-safe: inherited {', '.join(n for n, _o, _n in replaced)} "
                             f"lock was held at fork in pid {os.getpid()}; replaced with a fresh stream\n")
    except Exception:
        pass
    return [name for name, _old, _new in replaced]


def _fork_safe_child_entry(worker_target, *args):
    """Child entry point used when RIVER_FORK_SAFE is on: repair streams, then run the real worker."""
    _repair_child_streams()
    return worker_target(*args)


class _ChildResultDrain:
    """
    Joins a child while moving its result off the multiprocessing pipe. A result larger
    than the pipe buffer keeps the child's queue feeder blocked until the parent reads,
    and a child in that state never exits, so a plain join would wait out the whole
    timeout. The read happens only once data has been waiting for RESULT_DRAIN_AFTER_S
    with the child still alive (a normal small result is picked up after the child exits),
    on a helper thread bounded by RESULT_DRAIN_TIMEOUT_S so a child
    that dies mid message cannot block the parent.
    """

    def __init__(self, proc, result_queue):
        self.proc = proc
        self.result_queue = result_queue
        self.item = None
        self.have = False
        self.abandoned = False
        self._seen_at = None

    def join(self, timeout):
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            self.proc.join(min(remaining, RESULT_DRAIN_POLL_S))
            if not self.proc.is_alive():
                return
            self._maybe_drain()

    def _maybe_drain(self):
        if self.have or self.abandoned:
            return
        try:
            if self.result_queue.empty():
                self._seen_at = None
                return
        except Exception:
            return
        now = time.monotonic()
        if self._seen_at is None:
            self._seen_at = now
            return
        if now - self._seen_at < RESULT_DRAIN_AFTER_S:
            return
        box = []

        def _get():
            try:
                box.append(("ok", self.result_queue.get()))
            except BaseException as e:
                box.append(("error", e))

        reader = threading.Thread(target=_get, name="aots-result-drain", daemon=True)
        reader.start()
        reader.join(RESULT_DRAIN_TIMEOUT_S)
        if box and box[0][0] == "ok":
            self.item = box[0][1]
            self.have = True
        else:
            self.abandoned = True


def _run_country_subprocess(worker_target, worker_args, country, hazard_label, log_level, known_failed_facilities,
                            diag_context=None, stall_aware=False):
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

    stall_aware=True enables the RIVER_STALL_TIMEOUT_S heartbeat stall kill;
    diag_context carries the forecast tag for the hang-diagnostics report.
    """
    # See _process_one_country_isolated()'s identical check above for the
    # general "why" - visibility only, not a fix. For precip specifically,
    # read_precip_windows()'s Zarr open leaves a daemon 'zarr_io' thread plus
    # a 10-thread asyncio pool resident for the rest of the process; not
    # expected to deadlock (the isolated child never touches zarr/fsspec/
    # asyncio itself) but present at every country fork, not just the first.
    global _FORK_THREADS_WARNED
    fork_safe = _fork_safe_enabled()
    if threading.active_count() > 1 and not (fork_safe and _FORK_THREADS_WARNED):
        _FORK_THREADS_WARNED = True
        logging.getLogger(__name__).warning(
            f"{hazard_label} {country}: {threading.active_count()} threads active immediately before "
            f"forking the isolated subprocess (expected 1, the main thread); a lock held by one of the "
            f"extra threads at the instant of fork could deadlock the child."
        )
    _reset_snowflake_connection_before_fork()
    ctx = multiprocessing.get_context('fork')
    result_queue = ctx.Queue()
    if fork_safe:
        proc = ctx.Process(
            target=_fork_safe_child_entry,
            args=(worker_target, *worker_args, log_level, result_queue),
        )
    else:
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
    quiesce = None
    with _FORK_LOCK:
        if fork_safe:
            with _logging_quiesced_for_fork() as quiesce:
                proc.start()
        else:
            proc.start()
    if quiesce and quiesce["timed_out"]:
        logging.getLogger(__name__).warning(
            f"{hazard_label} {country}: {quiesce['timed_out']} logging handler lock(s) could not be held "
            f"across the fork within {RIVER_FORK_SAFE_LOCK_TIMEOUT_S:.0f}s; forked anyway"
        )
    drain = _ChildResultDrain(proc, result_queue) if fork_safe else None
    _join = drain.join if drain is not None else proc.join
    _diag_on = _hang_diag_enabled()
    _t_start = time.monotonic()
    _wait_started_wall = time.time()
    _diag_wait_s = _hang_diag_after_s() + HANG_DIAG_DUMP_GRACE_S
    _stall_limit_s = _stall_timeout_s() if stall_aware else 0.0
    _stalled = False
    if 0 < _stall_limit_s < COUNTRY_SUBPROCESS_TIMEOUT_S:
        _diag_reported = False
        while True:
            _remaining = COUNTRY_SUBPROCESS_TIMEOUT_S - (time.monotonic() - _t_start)
            if _remaining <= 0:
                break
            _join(min(COUNTRY_STALL_POLL_S, _remaining))
            if not proc.is_alive():
                break
            if _diag_on and not _diag_reported and time.monotonic() - _t_start >= _diag_wait_s:
                _diag_reported = True
                _hang_diag_report(proc, country, hazard_label, diag_context, _t_start,
                                  "still_running_at_diag_threshold", signal_child=False)
            if _stall_heartbeat_age_s(proc.pid, _wait_started_wall) >= _stall_limit_s:
                _stalled = True
                break
    elif _diag_on and _diag_wait_s < COUNTRY_SUBPROCESS_TIMEOUT_S:
        _join(_diag_wait_s)
        if proc.is_alive():
            _hang_diag_report(proc, country, hazard_label, diag_context, _t_start,
                              "still_running_at_diag_threshold", signal_child=False)
        _join(max(0.0, COUNTRY_SUBPROCESS_TIMEOUT_S - (time.monotonic() - _t_start)))
    else:
        _join(COUNTRY_SUBPROCESS_TIMEOUT_S)

    if proc.is_alive():
        if _diag_on and not _stalled and time.monotonic() - _t_start < COUNTRY_SUBPROCESS_TIMEOUT_S - 60:
            logging.getLogger(__name__).warning(
                f"{hazard_label} {country}: join returned after {time.monotonic() - _t_start:.0f}s but the child "
                f"still reports alive (exitcode={proc.exitcode}); treating it as a timeout"
            )
        if _diag_on:
            _hang_diag_report(proc, country, hazard_label, diag_context, _t_start,
                              "killed_on_stall" if _stalled else "killed_on_timeout", signal_child=True)
        proc.terminate()
        proc.join(30)
        if proc.is_alive():
            proc.kill()
            proc.join()
        if _diag_on:
            _hang_diag_cleanup(proc.pid)
        if stall_aware:
            _stall_cleanup(proc.pid)
        if _stalled:
            raise RuntimeError(
                f"{hazard_label} {country}: subprocess made no progress for {_stall_limit_s:.0f}s and was terminated"
            )
        raise RuntimeError(
            f"{hazard_label} {country}: subprocess exceeded {COUNTRY_SUBPROCESS_TIMEOUT_S}s and was terminated"
        )
    if _diag_on:
        _hang_diag_cleanup(proc.pid)
    if stall_aware:
        _stall_cleanup(proc.pid)

    if drain is not None and drain.have:
        status, payload = drain.item
    else:
        if result_queue.empty() or (drain is not None and drain.abandoned):
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
    # A graceful "ok"-status skip (worker_target's own docstring: {"skipped":
    # bool, "skip_reason": str, "files_written": int, ...}) would otherwise have no
    # parent-side visibility at all: the worker logs its real skip reason via
    # its own worker_logger, but that logger lives inside this forked child's
    # own copy-on-write memory and is never seen by the parent, so a skip
    # produced zero files, zero exception, and zero surfaced explanation, for
    # example a river reprocess that silently wrote 0 files with an empty
    # log buffer. Logging the real reason here, in the
    # parent, closes that blind spot without changing any actual behavior.
    result_dict = payload[0] if isinstance(payload, tuple) and payload else None
    if isinstance(result_dict, dict) and result_dict.get("skipped"):
        logging.getLogger(__name__).warning(
            f"{hazard_label} {country}: skipped (no files written): %s",
            result_dict.get("skip_reason", "no skip_reason recorded"),
        )
    return payload


def _build_precip_sparse_context(gdf_tiles, gdf_tiles_for_admin_by_level, facilities, worker_logger):
    """
    Per country static structures for building the thin precip/ratio frames
    directly. None when the structures cannot be
    built for this country, in which case every view is built by the dense path.
    """
    try:
        return PrecipSparseContext(
            gdf_tiles,
            gdf_tiles_for_admin_by_level,
            {name: (gdf, tile_map, id_column)
             for name, (gdf, tile_map, id_column) in facilities.items() if not gdf.empty},
        )
    except Exception as e:
        worker_logger.warning(f"Sparse precip build not applied for this country, using the dense path: {e}")
        return None


def _precip_tile_view(precip_sparse, gdf_tiles, grid, bounds, tiles_viewer, worker_logger):
    """
    (tile_view, sparse_combo) for one probability grid. sparse_combo is the
    sparse builder's handle for this grid when tile_view is its thin frame,
    None when tile_view is the dense frame (the sparse build
    failed and fell back).
    """
    if precip_sparse is not None:
        try:
            sparse_combo = precip_sparse.combo(grid, bounds)
            return sparse_combo.tile_frame(), sparse_combo
        except Exception as e:
            worker_logger.warning(f"Sparse precip tile frame not applied, using the dense path: {e}")
    return create_precip_tile_view(gdf_tiles, grid_2d=grid, bounds=bounds, tiles_viewer=tiles_viewer), None


def _precip_bitmask_view(sparse_combo, gdf_tiles, bitmask_grid, bounds, worker_logger):
    if sparse_combo is not None:
        try:
            return sparse_combo.bitmask_frame(bitmask_grid)
        except Exception as e:
            worker_logger.warning(f"Sparse precip bitmask not applied, using the dense path: {e}")
    return calculate_precip_tile_member_bitmask(gdf_tiles, bitmask_grid, *bounds)


def _precip_admin_view(sparse_combo, admin_level, gdf_admin, admin_tiles, gdf_tiles, tile_view, grid, bounds,
                       tiles_viewer, worker_logger):
    if sparse_combo is not None:
        try:
            return sparse_combo.admin_view(admin_level, admin_names(gdf_admin))
        except Exception as e:
            worker_logger.warning(f"Sparse precip admin{admin_level} view not applied, using the dense path: {e}")
    # A thin tile frame is not a valid precomputed input for the dense admin
    # rollup, so it is only reused when tile_view is the dense frame.
    return create_precip_admin_view(
        gdf_admin, admin_tiles,
        precomputed_tile_view=(tile_view if (admin_tiles is gdf_tiles and sparse_combo is None) else None),
        grid_2d=grid, bounds=bounds, tiles_viewer=tiles_viewer
    )


def _precip_facility_view(sparse_combo, name, gdf, tile_map, tile_view, id_column, worker_logger):
    if sparse_combo is not None:
        try:
            return sparse_combo.facility_frame(name)
        except Exception as e:
            worker_logger.warning(f"Sparse precip {name} view not applied, using the dense path: {e}")
    return create_precip_facility_view(gdf, tile_map, tile_view, id_column)


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

        precip_sparse = _build_precip_sparse_context(
            gdf_tiles, gdf_tiles_for_admin_by_level,
            {'school': (gdf_schools, school_tile_map, 'school_id_giga'),
             'hc': (gdf_hcs, hc_tile_map, 'osm_id'),
             'shelter': (gdf_shelters, shelter_tile_map, 'osm_id'),
             'wash': (gdf_wash, wash_tile_map, 'osm_id')},
            worker_logger)

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
                        tile_view, sparse_combo = _precip_tile_view(
                            precip_sparse, gdf_tiles, probability_grid, tp_bounds,
                            _tiles_viewer_by_id[id(gdf_tiles)], worker_logger)
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
                            bitmask_view = _precip_bitmask_view(sparse_combo, gdf_tiles, bitmask_grid, tp_bounds, worker_logger)
                            if not bitmask_view.empty:
                                write_tasks.append(lambda bv=bitmask_view: save_precip_tile_bitmask_view(bv, country, forecast_time_str, threshold_mm, window_h))

                        for admin_level in gdf_admin_by_level:
                            admin_tiles = gdf_tiles_for_admin_by_level[admin_level]
                            admin_view = _precip_admin_view(
                                sparse_combo, admin_level, gdf_admin_by_level[admin_level], admin_tiles, gdf_tiles,
                                tile_view, probability_grid, tp_bounds,
                                _tiles_viewer_by_id[id(admin_tiles)], worker_logger)
                            write_tasks.append(lambda av=admin_view, al=admin_level: save_precip_admin_view(
                                av, country, forecast_time_str, threshold_mm, window_h, admin_level=al))

                        if not gdf_schools.empty:
                            school_view = _precip_facility_view(sparse_combo, 'school', gdf_schools, school_tile_map, tile_view, 'school_id_giga', worker_logger)
                            write_tasks.append(lambda sv=school_view: save_precip_school_view(sv, country, forecast_time_str, threshold_mm, window_h))
                        if not gdf_hcs.empty:
                            hc_view = _precip_facility_view(sparse_combo, 'hc', gdf_hcs, hc_tile_map, tile_view, 'osm_id', worker_logger)
                            write_tasks.append(lambda hv=hc_view: save_precip_hc_view(hv, country, forecast_time_str, threshold_mm, window_h))
                        if not gdf_shelters.empty:
                            shelter_view = _precip_facility_view(sparse_combo, 'shelter', gdf_shelters, shelter_tile_map, tile_view, 'osm_id', worker_logger)
                            write_tasks.append(lambda shv=shelter_view: save_precip_shelter_view(shv, country, forecast_time_str, threshold_mm, window_h))
                        if not gdf_wash.empty:
                            wash_view = _precip_facility_view(sparse_combo, 'wash', gdf_wash, wash_tile_map, tile_view, 'osm_id', worker_logger)
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
                        tile_view, sparse_combo = _precip_tile_view(
                            precip_sparse, gdf_tiles, probability_grid, tp_bounds,
                            _tiles_viewer_by_id[id(gdf_tiles)], worker_logger)
                        # Synchronous gate write, same reasoning as the tp
                        # loop's own tile-view write above.
                        save_precip_ratio_view(tile_view, country, forecast_time_str, ratio_threshold, window_h)
                        files_written += 1
                        write_tasks = []
                        for admin_level in gdf_admin_by_level:
                            admin_tiles = gdf_tiles_for_admin_by_level[admin_level]
                            admin_view = _precip_admin_view(
                                sparse_combo, admin_level, gdf_admin_by_level[admin_level], admin_tiles, gdf_tiles,
                                tile_view, probability_grid, tp_bounds,
                                _tiles_viewer_by_id[id(admin_tiles)], worker_logger)
                            write_tasks.append(lambda av=admin_view, al=admin_level: save_precip_ratio_admin_view(
                                av, country, forecast_time_str, ratio_threshold, window_h, admin_level=al))

                        if not gdf_schools.empty:
                            school_view = _precip_facility_view(sparse_combo, 'school', gdf_schools, school_tile_map, tile_view, 'school_id_giga', worker_logger)
                            write_tasks.append(lambda sv=school_view: save_precip_ratio_school_view(sv, country, forecast_time_str, ratio_threshold, window_h))
                        if not gdf_hcs.empty:
                            hc_view = _precip_facility_view(sparse_combo, 'hc', gdf_hcs, hc_tile_map, tile_view, 'osm_id', worker_logger)
                            write_tasks.append(lambda hv=hc_view: save_precip_ratio_hc_view(hv, country, forecast_time_str, ratio_threshold, window_h))
                        if not gdf_shelters.empty:
                            shelter_view = _precip_facility_view(sparse_combo, 'shelter', gdf_shelters, shelter_tile_map, tile_view, 'osm_id', worker_logger)
                            write_tasks.append(lambda shv=shelter_view: save_precip_ratio_shelter_view(shv, country, forecast_time_str, ratio_threshold, window_h))
                        if not gdf_wash.empty:
                            wash_view = _precip_facility_view(sparse_combo, 'wash', gdf_wash, wash_tile_map, tile_view, 'osm_id', worker_logger)
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


def _build_river_sparse_context(gdf_tiles, gdf_tiles_for_admin_by_level, facilities, worker_logger):
    """
    Per country static structures for building the thin river frames directly.
    None when the structures cannot be built for this country, in which case
    every view is built by the dense path.
    """
    try:
        return RiverSparseContext(
            gdf_tiles,
            gdf_tiles_for_admin_by_level,
            {name: (gdf, tile_map, id_column)
             for name, (gdf, tile_map, id_column) in facilities.items() if not gdf.empty},
        )
    except Exception as e:
        worker_logger.warning(f"Sparse river build not applied for this country, using the dense path: {e}")
        return None


def _write_river_marker_only_tile_views(steps, country, forecast_time_str, rp, is_standin, gdf_tiles,
                                        river_sparse, tiles_viewer, worker_logger):
    """
    Writes the marker only tile file (one zero probability row) for each
    lead-time step in `steps` that has no flooded pixel. The file records that this country, tier and step was examined, which
    is what river_country_tier_already_processed() looks for. Nothing else is
    written for such a step, and it is not counted in the files-written
    totals the run-log marking gates read. A country and tier whose steps
    are all marker only therefore adds 0 to tier_files_written on the run
    that writes them, while a rerun that finds those marker files through
    river_country_tier_already_processed() adds 1 for the same tier. A failed
    write is logged and leaves the step without a file.
    """
    if not steps:
        return
    for step_h in steps:
        try:
            frame = None
            if river_sparse is not None:
                try:
                    frame = river_sparse.empty_tile_frame(is_standin)
                except Exception as e:
                    worker_logger.warning(f"Sparse river marker frame not applied, using the dense path: {e}")
            if frame is None:
                no_pixels = gpd.GeoDataFrame({'member': [], 'below_min_basin': []},
                                             geometry=gpd.GeoSeries([], crs='EPSG:4326'))
                frame = create_river_tile_view(gdf_tiles, no_pixels, rp, is_standin, tiles_viewer=tiles_viewer)
            save_river_tile_view(frame, country, forecast_time_str, rp, step_h)
        except Exception as e:
            worker_logger.warning(f"River marker only tile file not written for {country}/{rp}/{step_h}h: {e}")


def _fetch_member_file(row, scratch_dirs):
    """
    Downloads, as bytes only, the per-member Parquet file of the tier that
    `row` (its RIVER_FORECASTS row) points at, for a tier that was set up with
    the compact mask file only. The file goes into a fresh scratch directory
    under row['download_dir'], appended to `scratch_dirs` for removal when the
    country worker ends. Returns the local path; raises on a failed download.
    """
    scratch = tempfile.mkdtemp(dir=row.get('download_dir'))
    scratch_dirs.append(scratch)
    return download_river_forecast_parquet(row['stage_path'], get_river_forecasts_data_store(), scratch)


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

    Puts ("ok", ({"skipped": bool, "skip_reason": str (when skipped),
    "files_written": int, "tier_files_written": {rp: int},
    "budget_incomplete_tiers": set, "river_mem": list, "river_fallbacks":
    list}, new_failures)) onto
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
    # RIVER_MEM telemetry lines of this child, handed back to the parent.
    _river_mem_lines = []
    # Scratch directories of per-member files fetched on demand for a mask-only tier.
    _lazy_download_dirs = []
    # Every failed link of the tile aggregation chain, handed back to the parent
    # (see _record_river_fallback()).
    _river_fallbacks = []
    _seed_facility_failure_cache(known_failed_facilities)
    # Diagnostic watchdog (see RIVER_COUNTRY_WATCHDOG_S's own docstring for
    # the threshold and full "why"): scheduled at the start of this child's
    # real work, covering setup and the rp_tier x step_h loop - every phase a
    # stuck-fork/thread-race deadlock could freeze it in. The dump goes to a
    # per-child file under the temp dir (the parent uploads it to Blob), not
    # this worker's logger: a long Databricks run's stdout/stderr capture
    # keeps only a head+tail, and faulthandler's C-level handler writes
    # directly to a raw file descriptor regardless. With HANG_DIAG=0 or a
    # file setup failure it falls back to stderr. Cancelled
    # unconditionally in the `finally` below on every exit path so a country
    # finishing just past this threshold never produces a spurious dump.
    _arm_country_hang_dump(RIVER_COUNTRY_WATCHDOG_S)
    try:
        _start_country_heartbeat()
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

        _country_heartbeat()
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
        # by every dense tile view built below (up to 6 tiers x 4 leadtime
        # steps = 24), instead of each reconstructing one for unchanging
        # input. Only gdf_tiles needs one here (unlike precip's equivalent
        # cache above): create_river_admin_view() below always receives an
        # already-computed tile_view and has no `tiles_viewer` param of its
        # own. See build_tiles_viewer()'s docstring for the real cost.
        _tiles_viewer = build_tiles_viewer(gdf_tiles)

        gdf_schools = gdf_hcs = gdf_shelters = gdf_wash = gpd.GeoDataFrame()
        school_tile_map = hc_tile_map = shelter_tile_map = wash_tile_map = pd.DataFrame()

        # Fetch-only closures: the network I/O is what benefits from running
        # concurrently below (see that block for the latency numbers).
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
            # (cost: health centers took 9.85s to FAIL while
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
        _country_heartbeat()

        river_sparse = _build_river_sparse_context(
            gdf_tiles, gdf_tiles_for_admin_by_level,
            {'school': (gdf_schools, school_tile_map, 'school_id_giga'),
             'hc': (gdf_hcs, hc_tile_map, 'osm_id'),
             'shelter': (gdf_shelters, shelter_tile_map, 'osm_id'),
             'wash': (gdf_wash, wash_tile_map, 'osm_id')},
            worker_logger)

        # See RIVER_COUNTRY_TIME_BUDGET_S's/RIVER_COUNTRY_TIME_BUDGET_WITH_
        # REAL_DATA_S's docstrings for the two-tier budget design.
        # `_found_real_pixels` flips True the moment ANY tier's tile
        # aggregation reports flooded rows and stays True for the rest of this
        # country's loop; `_current_river_budget()` is re-evaluated at every
        # check below rather than cached, so confirming real data mid-loop
        # immediately widens the ceiling for the rest of that tier's step_h
        # loop and every later tier. By construction the inner step_h-loop
        # check never sees `_found_real_pixels=False`: a window is only
        # written once its tier reported flooded rows, so the flag is already
        # set - a country is never cut off mid-write for a tier already
        # confirmed to hold real flood data, only pre-confirmation.
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

        def _write_step_outputs(tile_view, bitmask_view, rp, step_h, forecast_time_str, is_standin,
                                sparse_step=None, dense_tile_view=None):
            """
            Writes one (rp_tier, step_h) pass's tile/bitmask/admin/facility
            views, given an already fully built, whole country tile_view and
            bitmask_view. Every write below depends only on this shape.

            sparse_step: the sparse builder's handle for this pass when
            tile_view is its thin frame; admin and facility
            views are then built from it, and dense_tile_view() supplies the
            dense frame for any view whose sparse build fails. None when
            tile_view is the dense frame.

            Raises the first write failure it hits, left to the caller's own
            try/except for per (rp_tier, step_h) failure isolation.
            """
            nonlocal files_written
            _dense_cache = []

            def _dense_view():
                # The dense tile frame, built on first need; the frame handed
                # in itself when it already is the dense one.
                if sparse_step is None:
                    return tile_view
                if not _dense_cache:
                    _dense_cache.append(dense_tile_view())
                return _dense_cache[0]

            # Synchronous gate write, same reasoning as precip's own
            # tile-view writes: largest payload, the file the dashboard's
            # combined-hazard union depends on; a failure here skips this
            # pass's remaining writes (caught by the caller).
            save_river_tile_view(tile_view, country, forecast_time_str, rp, step_h)
            files_written += 1
            tier_files_written[rp] += 1
            write_tasks = []

            if not bitmask_view.empty:
                write_tasks.append(
                    lambda bitmask_view=bitmask_view: save_river_tile_bitmask_view(bitmask_view, country, forecast_time_str, rp, step_h)
                )

            for admin_level, gdf_admin in gdf_admin_by_level.items():
                admin_view = None
                if sparse_step is not None:
                    try:
                        admin_view = sparse_step.admin_view(admin_level, admin_names(gdf_admin))
                    except Exception as e:
                        worker_logger.warning(f"Sparse river admin{admin_level} view not applied, using the dense path: {e}")
                if admin_view is None:
                    admin_view = create_river_admin_view(gdf_admin, gdf_tiles_for_admin_by_level[admin_level], None, rp, is_standin, tile_view=_dense_view())
                write_tasks.append(
                    lambda admin_view=admin_view, admin_level=admin_level: save_river_admin_view(admin_view, country, forecast_time_str, rp, step_h, admin_level=admin_level)
                )

            facility_inputs = (('school', gdf_schools, school_tile_map, 'school_id_giga', save_river_school_view),
                               ('hc', gdf_hcs, hc_tile_map, 'osm_id', save_river_hc_view),
                               ('shelter', gdf_shelters, shelter_tile_map, 'osm_id', save_river_shelter_view),
                               ('wash', gdf_wash, wash_tile_map, 'osm_id', save_river_wash_view))
            _tile_dicts = []

            def _dense_tile_dicts():
                if not _tile_dicts:
                    dense = _dense_view()
                    _tile_dicts.append((dense.set_index('zone_id')['probability'].to_dict(),
                                        dense.set_index('zone_id')['below_min_basin'].to_dict()))
                return _tile_dicts[0]

            for name, gdf_facility, facility_tile_map, id_column, save_fn in facility_inputs:
                if gdf_facility.empty:
                    continue
                v = None
                if sparse_step is not None:
                    try:
                        v = sparse_step.facility_frame(name)
                    except Exception as e:
                        worker_logger.warning(f"Sparse river {name} view not applied, using the dense path: {e}")
                if v is None:
                    tile_probs, tile_basin = _dense_tile_dicts()
                    v = create_river_facility_view(gdf_facility, facility_tile_map, _dense_view(), id_column, is_standin, tile_probs=tile_probs, tile_basin=tile_basin)
                write_tasks.append(lambda v=v, save_fn=save_fn: save_fn(v, country, forecast_time_str, rp, step_h))

            succeeded, first_exc = _parallel_write_batch(write_tasks, worker_logger)
            files_written += succeeded
            tier_files_written[rp] += succeeded
            if first_exc is not None:
                raise first_exc

        def _emit_river_mem(rp, mode, rows, t_start, rss_start, hit_tiles=None):
            """Logs one RIVER_MEM line for a (country, tier) pass and queues it for the parent's log."""
            line = format_river_mem(country, rp, mode, rows, len(gdf_tiles), time.time() - t_start,
                                    rss_start_mb=rss_start, hit_tiles=hit_tiles)
            _river_mem_lines.append(line)
            worker_logger.info(line)

        def _record_river_fallback(rp, source, to, exc=None, rows=None, message=None):
            """
            Records that the tile aggregation of `source` ('bymask' or 'agg')
            did not produce this tier's result and `to` ('agg' or 'skipped')
            takes over, for the parent to log at WARNING.
            """
            text = message if message is not None else (str(exc) if exc is not None else '')
            _river_fallbacks.append({
                'country': country, 'tier': rp, 'from': source, 'to': to,
                'exception_type': type(exc).__name__ if exc is not None else None,
                'message': text[:300], 'rows': rows})

        def _tile_view_from_bits(bits_df, rp, is_standin):
            """
            (tile_view, sparse_step, dense_tile_view) for one window's tile
            bits: the thin frame with its sparse step when
            buildable, else the dense frame (sparse_step None).
            dense_tile_view() builds the dense frame on demand.
            """
            tile_ids = bits_df['tile_id'].astype(str).to_numpy()
            bits = bits_df['bits'].to_numpy()
            basin = bits_df['below_min_basin'].to_numpy()

            def _dense():
                index = pd.Index(tile_ids, name='tile_id')
                return create_river_tile_view_from_counts(
                    gdf_tiles,
                    pd.Series(popcount_u64(bits).astype('int64'), index=index),
                    pd.Series(basin, index=index),
                    is_standin, tiles_viewer=_tiles_viewer)

            if river_sparse is not None:
                try:
                    sparse_step = RiverStep.from_tile_bits(river_sparse, tile_ids, bits, basin, is_standin)
                    return sparse_step.tile_frame(), sparse_step, _dense
                except Exception as e:
                    worker_logger.warning(f"Sparse river tile frame not applied for {country}/{rp}, using the dense path: {e}")
            return _dense(), None, _dense

        def _bitmask_from_bits(bits_df):
            if bits_df.empty:
                return pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})
            return bits_df[['tile_id', 'bits']].reset_index(drop=True)

        def _open_tile_source(tile_src):
            return BymaskSource(tile_src['path']) if tile_src['kind'] == 'bymask' else AggregatedBymemberSource(tile_src['path'])

        def _run_tier_from_tile_bits(rp, rp_idx, forecast_time_str, is_standin, tile_src, next_link):
            """
            Processes one whole tier from the tile aggregation: one
            load_river_tile_bits() call, then the marker-only and
            _write_step_outputs() finishing per lead-time step.
            Returns False, writing nothing, when the aggregation cannot be run
            for this country and tier, so the caller can use the next link of the
            source chain (`next_link`, recorded as the fallback target).
            """
            nonlocal _found_real_pixels, _budget_exceeded
            t_start = time.time()
            rss_start = current_rss_mb()
            try:
                result = load_river_tile_bits(_open_tile_source(tile_src), gdf_tiles, RIVER_LEADTIME_STEPS_H, bounds=bounds)
            except Exception as e:
                worker_logger.warning(f"River-flood tile aggregation unavailable for {country}/{rp}: {e}")
                _record_river_fallback(rp, 'bymask' if tile_src['kind'] == 'bymask' else 'agg', next_link, exc=e)
                return False
            _emit_river_mem(rp, result.source, result.rows, t_start, rss_start, result.hit_tiles)
            if result.rows > 0:
                _found_real_pixels = True
            _write_river_marker_only_tile_views([w for w in RIVER_LEADTIME_STEPS_H if not result.has_pixels[w]],
                                                country, forecast_time_str, rp, is_standin,
                                                gdf_tiles, river_sparse, _tiles_viewer, worker_logger)
            for step_idx, step_h in enumerate(RIVER_LEADTIME_STEPS_H):
                if not result.has_pixels[step_h]:
                    continue  # no flooded pixels within this window: normal, common
                _country_heartbeat()
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
                    bits_df = result.by_window[step_h]
                    tile_view, sparse_step, dense_tile_view = _tile_view_from_bits(bits_df, rp, is_standin)
                    _write_step_outputs(
                        tile_view, _bitmask_from_bits(bits_df), rp, step_h, forecast_time_str, is_standin,
                        sparse_step=sparse_step, dense_tile_view=dense_tile_view)
                except Exception as e:
                    worker_logger.warning(f"River-flood view failed for {country}/{rp}/{step_h}h: {e}")
            return True

        for rp_idx, rp in enumerate(tiers_to_process):
            _country_heartbeat()
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

            if river_country_tier_already_processed(country, forecast_time, rp, RIVER_LEADTIME_STEPS_H):
                tier_files_written[rp] += 1
                continue

            # Source chain: the compact mask file, then the aggregated per-member
            # file (`member_path`, absent when only the mask file was
            # downloaded), then the tier is skipped for this cycle.
            _tile_src = row.get('tile_source')
            _member_path = row.get('member_path')
            _tier_done = False
            if _tile_src is not None:
                _tier_done = _run_tier_from_tile_bits(
                    rp, rp_idx, forecast_time_str, is_standin, _tile_src,
                    'agg' if _tile_src['kind'] == 'bymask' else 'skipped')
                if not _tier_done and _tile_src['kind'] == 'bymask':
                    if _member_path is None and row.get('kind') != 'bymask':
                        try:
                            _member_path = _fetch_member_file(row, _lazy_download_dirs)
                        except Exception as e:
                            worker_logger.warning(
                                f"RIVER_FALLBACK country={country} tier={rp} bymask -> agg unavailable, the "
                                f"per-member file could not be downloaded: {e}")
                            _record_river_fallback(rp, 'agg', 'skipped', exc=e,
                                                   message=f"per-member file download failed: {e}")
                    if _member_path is not None:
                        worker_logger.warning(f"RIVER_FALLBACK country={country} tier={rp} bymask -> agg")
                        _tier_done = _run_tier_from_tile_bits(
                            rp, rp_idx, forecast_time_str, is_standin, {'kind': 'bymember', 'path': _member_path},
                            'skipped')
                    elif row.get('kind') == 'bymask':
                        _record_river_fallback(rp, 'agg', 'skipped', message="per-member file not available")
            else:
                _record_river_fallback(rp, 'agg', 'skipped', message="no readable file for this tier")
            if not _tier_done:
                worker_logger.warning(
                    f"River-flood tile aggregation failed for {country}/{rp}, skipping this tier for this cycle")
                continue

            if _budget_exceeded:
                break  # outer rp loop: the inner step_h loop's own break above only exits that loop

            # Per-tier memory reclaim for this long-lived child process's
            # up-to-6-iteration rp_tier loop: gc.collect() plus malloc_trim(0)
            # return freed memory to the OS between tiers, the same mechanism
            # _release_memory() provides for the parent driver's per-country
            # loop (see its docstring for the glibc arena-fragmentation
            # reasoning). It costs about 0.1s per call.
            _release_memory(worker_logger)

        result_queue.put(("ok", ({"skipped": False, "files_written": files_written, "tier_files_written": tier_files_written, "budget_incomplete_tiers": budget_incomplete_tiers, "river_mem": _river_mem_lines, "river_fallbacks": _river_fallbacks}, _new_facility_failures(known_failed_facilities))))
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
        _disarm_country_hang_dump()
        _stop_country_heartbeat()
        for _lazy_dir in _lazy_download_dirs:
            shutil.rmtree(_lazy_dir, ignore_errors=True)


def _process_one_country_river_isolated(country, zoom, tiers_to_process, local_paths, tier_rows,
                                         admin_levels_by_country, known_failed_facilities, log_level):
    """
    Returns ({"skipped": bool, "files_written": int, "tier_files_written":
    {rp: int}, plus the other keys the worker reports}, new_failures) - see _run_country_worker_river()'s own
    docstring for the new_failures half. Same underlying call
    contract as running run_river_flood_analysis()'s own per-country block
    directly, but executed in a fresh 'fork' subprocess for the same
    per-country memory-isolation reason as wind/gust's
    _process_one_country_isolated().
    """
    try:
        forecast_tag = "+".join(sorted({
            (tier_rows[rp]['forecast_time'].strftime('%Y%m%d%H%M%S')
             if hasattr(tier_rows[rp]['forecast_time'], 'strftime') else str(tier_rows[rp]['forecast_time']))
            for rp in tiers_to_process}))
    except Exception:
        forecast_tag = "unknown"
    return _run_country_subprocess(
        _run_country_worker_river,
        (country, zoom, tiers_to_process, local_paths, tier_rows, admin_levels_by_country, known_failed_facilities),
        country, "river-flood", log_level, known_failed_facilities,
        diag_context={"forecast": forecast_tag},
        stall_aware=True,
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
def run_complete_impact_analysis(storm, date, countries, logger, zoom, skip_gust=False, known_failed_facilities=None,
                                  deadline_ts=None, pass1_cap_s=COUNTRY_PASS1_CAP_S, pass2_cap_s=COUNTRY_PASS2_CAP_S,
                                  skip_wind=False):
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
        skip_wind: If True, skip wind envelope processing entirely; used by
            the separate, delayed gust scheduler (databricks/07_scheduler_
            gust.py) to reuse this same country-loop/subprocess-isolation
            machinery for gust-only work on a country whose wind result is
            already saved, instead of redoing wind's own view creation. Wind
            envelope data is still loaded and still used for the country
            affected/not-affected prefilter above (gust envelopes cover
            substantially the same footprint), just never turned into wind
            views/saves.
        known_failed_facilities: a mutable set of (country, facility_type)
            pairs already known to fail this pipeline run (across storms and
            across the wind/gust -> precip -> river phase sequence: see
            _seed_facility_failure_cache()'s own docstring). Mutated in place
            (via `|=`, never reassigned) so the SAME object the caller passed
            keeps accumulating. None (the default) creates a fresh,
            run-scoped set here instead.
        deadline_ts: optional absolute Unix epoch timestamp, threaded straight
            through to _process_one_country_isolated() (see its own docstring
            for the full "why": bounds how long a legitimately-still-working
            country's progress-aware extension can run, tied to the CALLER's
            own real remaining time budget, e.g. a scheduler's MAX_RUN_MINUTES,
            rather than an arbitrary fixed number). ALSO checked directly in
            this function's own per-country loop below, before starting each
            country: the scheduler-level circuit breaker
            (databricks/04_scheduler_windgust.py's own MAX_RUN_MINUTES) is only
            checked BETWEEN whole (storm, forecast_time, countries) batch
            items, never inside this loop; without this second check here, a
            single batch item covering several countries could still consume
            far more than the caller's intended budget even though each
            individual country respects deadline_ts. None (the default, every
            call site except the wind and gust schedulers) means unbounded
            except for each country's own heartbeat-based stall-grace.
        pass1_cap_s / pass2_cap_s: per-country time budgets (seconds) for this
            function's own two-pass country loop below: see
            COUNTRY_PASS1_CAP_S/COUNTRY_PASS2_CAP_S's own module-level
            comment for the full "why" (real two-pass design, dynamic
            slow_countries.json memory, tier-3 deferral). Defaults are the
            MAIN job's own short, tight values; a tier-3 (delayed, generous-
            budget) scheduler invocation overrides both to much larger
            numbers via its own job parameters, reusing this exact same code
            path unchanged; see 04_scheduler_windgust.py's own
            dbutils.widgets (max_run_minutes, pass1_cap_s, pass2_cap_s).

    Returns:
        dict: Summary of analysis results with keys:
            - success (bool): Whether analysis completed successfully
            - skipped (bool): Present and True for the two early returns
              (expired forecast, and no requested country in range), where
              every count below is 0/empty
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
        # known_failed_facilities is a parameter (see this function's own
        # docstring for the full "why") so it persists and keeps accumulating
        # across storms and phases instead of resetting per storm the way a
        # local declaration here would.
        #
        # Real, dynamic memory of which countries have needed a deferral
        # before, loaded once per call (cheap: one small Blob read), never
        # written unless a real new deferral actually happens this call (see
        # _attempt_country()'s own use of it below). Never blocks/raises on
        # its own failure (a bookkeeping-file hiccup must not fail real
        # impact processing), same "logging is best-effort" philosophy as
        # log_run_start()/log_run_complete().
        try:
            slow_countries = load_json_slow_countries()
        except Exception as e:
            logger.warning(f"Could not load slow_countries.json, starting fresh: {e}")
            slow_countries = {'windgust': {}}
        slow_countries.setdefault('windgust', {})
        slow_countries_dirty = False

        def _attempt_country(country, cap_s, tier_label):
            """
            One real attempt at `country` with an at-most-cap_s (further
            bounded by this call's own outer deadline_ts) time budget.
            Mutates the enclosing succeeded_countries/country_errors/
            country_files_written/total_views/any_base_parquet_written/
            known_failed_facilities exactly as a single-pass loop would
            on success or a genuine failure. Returns True if `country`
            should be DEFERRED to the next tier instead (still genuinely
            working, this tier's own budget simply ran out); the caller
            decides what "next tier" means (pass 2, or tier-3-via-
            unevaluated_countries). tier_label is purely for the real,
            dynamic slow_countries.json record and log lines below.
            """
            nonlocal total_views, any_base_parquet_written, slow_countries_dirty
            country_deadline = time.time() + cap_s
            if deadline_ts is not None:
                country_deadline = min(country_deadline, deadline_ts)
            try:
                (wrote_base, files_written_for_country, gust_error), new_failures = _process_one_country_isolated(
                    country, storm, date, gdf_envelopes, zoom, gdf_envelopes_gust,
                    admin_levels_by_country, known_failed_facilities, _log_level_name,
                    deadline_ts=country_deadline, skip_wind=skip_wind)
                # .update(), not |=: known_failed_facilities isn't declared
                # nonlocal in this closure, and |= is an assignment target,
                # not a pure mutation: Python would treat it as a brand-new
                # LOCAL to _attempt_country and raise UnboundLocalError on
                # this exact line. .update() is a plain method call,
                # correctly resolving known_failed_facilities from the
                # enclosing scope with no nonlocal declaration needed, same
                # real mutate-in-place
                # semantics as the |= form used elsewhere in this file
                # (outside a nested closure, where |= is safe).
                known_failed_facilities.update(new_failures)
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
                return False
            except _CountryStillWorkingError:
                logger.warning(
                    f"{country}: still genuinely working when its {tier_label} budget "
                    f"({cap_s}s) ran out; deferring, not failing."
                )
                entry = slow_countries['windgust'].setdefault(
                    country, {"defer_count": 0, "last_deferred_at": None, "last_tier": None})
                entry["defer_count"] = entry.get("defer_count", 0) + 1
                entry["last_deferred_at"] = datetime.now(timezone.utc).isoformat()
                entry["last_tier"] = tier_label
                slow_countries_dirty = True
                return True
            except Exception as country_exc:
                import traceback as _tb
                logger.error(f"Pipeline with errors for storm {storm} at {date}")
                logger.error(f"  {country}: {str(country_exc)}")
                logger.debug(_tb.format_exc())
                country_errors.append(f"{country}: {str(country_exc)}")
                return False
            finally:
                # Per-country memory isolation happens inside
                # _process_one_country_isolated() itself (its own OS process,
                # reclaimed on exit); this call is a cheap safety net for
                # whatever this driver process itself accumulates. Kept
                # unconditional (no modulo gating like precip/river) since a
                # storm cycle typically reaches only a handful of countries.
                # In `finally` so a raising country still releases first.
                _release_memory(logger)

        # PASS 1: every affected country gets a short shot. See
        # COUNTRY_PASS1_CAP_S's own module-level comment for the full "why"
        # this stays deliberately tight for the main job.
        pass2_queue = []
        for country in affected_countries:
            # Loop-level circuit breaker: databricks/04_scheduler_windgust.py's
            # own MAX_RUN_MINUTES is only checked BETWEEN whole (storm,
            # forecast_time, countries) batch items, never inside this loop:
            # a single batch item covering several countries could otherwise
            # still consume far more than the caller's intended budget even
            # though each individual country's own subprocess respects
            # deadline_ts. Remaining countries are simply not attempted this
            # call, staying eligible for retry on the next trigger (added to
            # unevaluated_countries below so a downstream caller's own
            # no_impact computation never mistakes "not yet attempted" for
            # "confirmed no impact": see is_already_processed()'s own
            # docstring for why that distinction matters).
            if deadline_ts is not None and time.time() >= (deadline_ts - COUNTRY_DEADLINE_MARGIN_S):
                remaining = affected_countries[affected_countries.index(country):]
                logger.warning(
                    f"Run budget (deadline_ts) reached before processing {remaining}; "
                    f"stopping this cycle's country loop early, these remain eligible for retry."
                )
                unevaluated_countries.update(remaining)
                break
            if _attempt_country(country, pass1_cap_s, "pass1"):
                pass2_queue.append(country)

        # PASS 2: retry ONLY the countries pass 1 deferred, with more
        # time; everyone else already had their turn, nobody new waits on
        # this. A country still not done even here is deferred a SECOND
        # time (recorded the same way, tier_label="pass2") and simply left
        # out of succeeded_countries/country_files_written; it lands in
        # unevaluated_countries below via the same real mechanism as the
        # loop-level circuit breaker above, so a downstream caller never
        # mistakes "needs tier 3" for "confirmed no impact". No explicit
        # tier-3 dispatch call here: the real, existing discovery-query
        # mechanism (04_scheduler_windgust.py) already re-surfaces any
        # (storm, forecast_time, country) that never reached a real SUCCESS;
        # a separate, delayed, more-generous-budget scheduler invocation
        # targeting this exact same discovery query IS tier 3, no new
        # dispatch logic needed here.
        for country in pass2_queue:
            if deadline_ts is not None and time.time() >= (deadline_ts - COUNTRY_DEADLINE_MARGIN_S):
                logger.warning(f"Run budget (deadline_ts) reached before pass-2 retry of {country}.")
                unevaluated_countries.add(country)
                continue
            if _attempt_country(country, pass2_cap_s, "pass2"):
                unevaluated_countries.add(country)

        if slow_countries_dirty:
            try:
                save_json_slow_countries(slow_countries)
            except Exception as e:
                logger.warning(f"Could not save slow_countries.json: {e}")

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
    caller such as main()'s CLI path passes the SAME set already accumulated
    by that same run's own update_storms() call, so a facility-fetch failure
    wind/gust's phase already discovered for a country is a known cache hit
    here too, not a from-scratch retry. The precip and river schedulers pass
    the set loaded by load_known_failed_facilities_cache() instead.
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
    # by 3 real call sites, so skips aren't threaded through the return value -
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
    # known_failed_facilities is a parameter (see this function's own
    # docstring for the full "why") so it persists and keeps accumulating
    # across the wind/gust -> precip -> river phase sequence instead of
    # resetting at the start of this one phase.
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
            # The real per-country memory isolation happens inside the
            # subprocess itself (reclaimed by the OS the instant it exits,
            # regardless of cause); this call is a secondary, cheap safety
            # net for whatever small amount this driver process itself
            # accumulates handling the subprocess/queue plumbing per
            # country, on the same cadence (every 8th pass, not
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
        # failures count toward the give-up threshold below, so a partial
        # invocation can never accidentally push a real cycle toward
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

# Memory admission for the second country worker. A worker (forked child)
# peaks around 1.7 GB for the largest tile grid under a severe flood cycle,
# 1.1 GB for the heaviest flood volume and well below 1 GB for most
# countries, and reaches its peak within the first tier. The parent holds
# about 0.5 GB. The driver has about 4 GB free for the parent and its
# workers, so two peaking large workers are not guaranteed to fit.
# _river_worker_fits() admits a worker next to a running one only when the
# live memory of the process tree plus RIVER_WORKER_RESERVE_MB for the new
# worker, and for every running worker younger than RIVER_WORKER_RAMP_S
# (still ramping to its peak), stays within RIVER_MEMORY_BUDGET_MB. A single
# worker is always admitted. The rule depends only on measured memory, not on
# which countries are running.
RIVER_MEMORY_BUDGET_MB = 4000
RIVER_WORKER_RESERVE_MB = 1800
RIVER_WORKER_RAMP_S = 10.0
# Seconds between admission re-checks while a worker waits for memory.
RIVER_ADMISSION_POLL_S = 1.0

# Cumulative wall-clock budget for _run_country_worker_river()'s rp_tier x
# step_h loop (up to 6 x 4 = 24 iterations), measured from worker start
# (setup included), nested inside COUNTRY_SUBPROCESS_TIMEOUT_S:
# each Blob read keeps a 120s watchdog (_BLOB_OP_TIMEOUT_SECONDS in
# data_store_utils.py) and writes use max(120s, 5s per MB), but that alone
# doesn't bound how many of the ~24 iterations a persistently degraded
# country could grind through before hitting the full subprocess ceiling
# with no partial output. A healthy country finishes in a few seconds to
# about two minutes;
# 360s (6 minutes) caps a stuck country at a small fraction of the subprocess
# timeout, leaving margin for a couple of genuinely slow iterations.
#
# Only governs the "no real flood data confirmed yet" phase: a country that
# has confirmed real flooded pixels (the tile aggregation reported flooded
# rows for some tier) is doing genuine work, not hanging - a large real
# flood event naturally takes longer, and cutting it off would silently
# under-report a real disaster. So this budget applies at the top of every
# rp-loop iteration until a tier reports flooded rows, at which
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
# COUNTRY_SUBPROCESS_TIMEOUT_S and getting SIGKILLed with nothing
# returned. 1200s leaves real margin under that kill (comfortably so given
# the 5400s ceiling), so a degraded country with real flood
# data still returns a partial result well before the hard kill.
RIVER_COUNTRY_TIME_BUDGET_WITH_REAL_DATA_S = 1200

# Diagnostic-only watchdog for _run_country_worker_river(): if the forked
# child is still alive this many seconds after starting, faulthandler dumps
# its live stack trace (to a per-child file the parent uploads to Blob, or to
# stderr with HANG_DIAG=0; also the default HANG_DIAG_AFTER_S) instead of
# leaving a COUNTRY_SUBPROCESS_TIMEOUT_S kill with no trace of what it was
# doing. 600s (10 minutes) is several
# times a healthy country's runtime of a few seconds to about two
# minutes (even at the largest known ~20M pixel count), set above RIVER_COUNTRY_TIME_
# BUDGET_S (360s) so it never fires mid-budget, and stays well under the
# real subprocess kill ceiling. A country with confirmed real flood data can
# legitimately run up to 1200s, so this can still fire there too, harmlessly (a
# changing stack across log lines reads as progress, a parked one as a hang).
RIVER_COUNTRY_WATCHDOG_S = 600

# Cross-run relay cache for known_failed_facilities, read/written via
# get_data_store() (works under any DATA_PIPELINE_DB backend). Exists for the
# split-scheduler architecture (separate wind, gust, precip, river Databricks
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
    prior run's save_known_failed_facilities_cache() call (any of the Databricks
    scheduler jobs: wind, gust, precip, river), via get_data_store(). No separate code path is needed for
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
    blind overwrite: the Databricks scheduler jobs (wind, gust, precip, river)
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
# comment). Consumed as the `step_h <= window` union inside
# load_river_tile_bits(). A subset of the full daily lead-time steps in
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


def _log_river_worker_telemetry(logger, result):
    """
    Re-logs, in the parent, the RIVER_MEM lines and the RIVER_FALLBACK records a
    country child handed back in its result: the child's own stream is not
    reliably captured, so the parent log is where they must be visible.
    """
    for line in result.get("river_mem") or []:
        logger.info(line)
    for fb in result.get("river_fallbacks") or []:
        logger.warning(
            f"RIVER_FALLBACK country={fb['country']} tier={fb['tier']} from={fb['from']} "
            f"to={fb['to']} rows={fb['rows']} error={fb['exception_type'] or 'none'}: {fb['message']}")


def _resolve_bymask_rows(tiers, tier_rows, target_date, logger, candidates=None):
    """
    {rp: RIVER_FORECASTS row} of the compact mask file (PARAM extent_bymask_<rp>)
    for each tier whose pointer describes the same forecast cycle as the
    tier's per-member row. Tiers without a matching pointer are left out and
    keep reading the per-member file. A tier whose own row is a mask row
    (kind 'bymask', no per-member file exists) is always included. `candidates`
    is {rp: mask row or None} when the caller already read the mask pointers;
    otherwise they are looked up here. A failed lookup leaves every other tier
    on its per-member file.
    """
    rows = {rp: tier_rows[rp] for rp in tiers if tier_rows[rp].get('kind') == 'bymask'}
    pending = [rp for rp in tiers if rp not in rows]
    if candidates is None:
        try:
            if target_date is not None:
                candidates = {rp: get_river_forecast_for_date(bymask_param(rp), target_date) for rp in pending}
            else:
                bulk = get_latest_river_forecasts_bulk([bymask_param(rp) for rp in pending])
                candidates = {rp: bulk.get(bymask_param(rp)) for rp in pending}
        except Exception as e:
            logger.warning(f"Could not look up the compact river mask files, using the per-member files: {e}")
            return rows
    for rp in pending:
        candidate = candidates.get(rp)
        match = matching_bymask_row(tier_rows[rp], candidate)
        if match is not None:
            rows[rp] = match
        elif candidate is not None:
            logger.info(f"River tier '{rp}': compact mask file is for another cycle ({candidate['forecast_time']}), using the per-member file")
    return rows


def _river_process_tree_mb():
    """
    Live memory of this process plus all its descendants in MB: proportional
    set size where the OS reports it (shared pages of forked workers counted
    once across the tree), resident size otherwise. None when it cannot be read.
    """
    try:
        import psutil
        me = psutil.Process()
        total = 0
        for proc in [me] + me.children(recursive=True):
            try:
                info = proc.memory_full_info()
                total += getattr(info, 'pss', 0) or info.rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return total / (1024.0 * 1024.0)
    except Exception:
        return None


def _river_worker_fits(running_ages_s, tree_mb):
    """
    True when one more country worker can start next to the running ones.
    `running_ages_s` holds the age in seconds of each running worker and
    `tree_mb` the live memory of the process tree (None when unknown). A
    worker is always admitted when none runs. Otherwise the new worker and
    every running worker still ramping to its peak (age below
    RIVER_WORKER_RAMP_S) are each charged RIVER_WORKER_RESERVE_MB on top of
    the live memory, and the total must stay within RIVER_MEMORY_BUDGET_MB.
    """
    if not running_ages_s:
        return True
    if tree_mb is None:
        return False
    ramping = sum(1 for age in running_ages_s if age < RIVER_WORKER_RAMP_S)
    return tree_mb + (ramping + 1) * RIVER_WORKER_RESERVE_MB <= RIVER_MEMORY_BUDGET_MB


def _iter_bounded_completions(items, submit, max_in_flight, may_admit=None, poll_s=RIVER_ADMISSION_POLL_S):
    """
    Starts `submit(item)` (returning a Future) for each item in order, with at
    most `max_in_flight` running at once, and yields (item, future) as each
    completes. A free slot is refilled before the completed future is
    yielded. When `may_admit(running_ages_s)` is given, the next item starts
    only while it returns True for the ages (seconds since submission) of the
    running futures; it is re-checked every `poll_s` seconds while waiting. The
    first item always starts when nothing runs, so the loop cannot stall.
    """
    pending = list(items)
    in_flight = {}

    def fill():
        while pending and len(in_flight) < max_in_flight:
            if in_flight and may_admit is not None:
                now = time.monotonic()
                if not may_admit([now - started for _, started in in_flight.values()]):
                    break
            item = pending.pop(0)
            in_flight[submit(item)] = (item, time.monotonic())

    fill()
    while in_flight:
        waiting = bool(pending) and may_admit is not None
        done, _ = wait(list(in_flight), timeout=poll_s if waiting else None, return_when=FIRST_COMPLETED)
        for future in done:
            item, _ = in_flight.pop(future)
            fill()
            yield item, future
        if not done:
            fill()


def _download_river_tile_sources(tiers, tier_rows, bymask_rows, local_paths, river_data_store, tmp_dir, logger):
    """
    Downloads the compact mask file of every tier in `bymask_rows` and records
    on tier_rows[rp] which file the tile aggregation reads ('tile_source':
    {'kind': 'bymask'|'bymember', 'path'}) and the per-member file kept as the
    next link of the source chain ('member_path', None when that file was not
    downloaded). A tier without a usable mask file reads its per-member file. local_paths[rp] always names a readable file for
    a tier that has one.
    """
    mask_paths = {}
    if bymask_rows:
        with ThreadPoolExecutor(max_workers=len(bymask_rows)) as executor:
            futures = {
                executor.submit(download_river_forecast_parquet, row['stage_path'], river_data_store, tmp_dir): rp
                for rp, row in bymask_rows.items()
            }
            for future, rp in futures.items():
                try:
                    mask_paths[rp] = future.result()
                except Exception as e:
                    logger.warning(f"Failed to download the compact mask file of tier '{rp}', using its per-member file: {e}")
    for rp in tiers:
        if tier_rows[rp].get('kind') == 'bymask' and rp not in mask_paths:
            continue  # no per-member file exists for this tier, it is skipped for this cycle
        if rp not in local_paths and rp in bymask_rows and rp not in mask_paths:
            # The mask file failed and the per-member file was never requested.
            try:
                local_paths[rp] = download_river_forecast_parquet(tier_rows[rp]['stage_path'], river_data_store, tmp_dir)
            except Exception as e:
                logger.warning(f"Failed to download RIVER_FORECASTS tier '{rp}': {e}")
        member_path = local_paths.get(rp)
        if rp in mask_paths:
            tier_rows[rp]['tile_source'] = {'kind': 'bymask', 'path': mask_paths[rp]}
            local_paths.setdefault(rp, mask_paths[rp])
        elif member_path is not None:
            tier_rows[rp]['tile_source'] = {'kind': 'bymember', 'path': member_path}
        else:
            continue
        tier_rows[rp]['member_path'] = member_path
        tier_rows[rp]['download_dir'] = tmp_dir


def run_river_flood_analysis(countries, logger, zoom=14, target_date=None, known_failed_facilities=None):
    """
    Storm-independent GloFAS x JRC river-flood analysis, run once per
    --type update invocation (like run_precip_analysis), not once per storm.
    Per tier, reads the compact extent_bymask_rpN file when it is the same
    cycle as the per-member file (or the only file), else the
    extent_rpN_bymember file (both produced by TC-ECMWF-Forecast-Pipeline
    daily, regardless of storm activity); a tier with no readable file is
    skipped for the cycle. Produces per-tier, per-lead-time-step
    tile/admin/facility flood-extent probability views.

    known_failed_facilities: see run_complete_impact_analysis()'s own
    docstring for the full "why" and the mutate-in-place contract. A real
    caller such as main() passes the SAME set already accumulated by
    update_storms() + run_precip_analysis(); the river scheduler passes the
    set loaded by load_known_failed_facilities_cache().

    Unlike precip's single continuous raster, GloFAS/JRC pixels (~150m) are
    comparable to a mercator tile or buffered facility, not much coarser, so
    this reuses the wind/gust "members/FULL_ENSEMBLE_SIZE" convention (the
    popcount of the per-tile member bitmask over FULL_ENSEMBLE_SIZE, see
    RiverStep.from_tile_bits and create_river_tile_view_from_counts()) rather
    than precip's centroid-raster-sampling approach. Each RP tier's global
    Parquet file (70M+ rows) is downloaded ONCE per (tier, date), never per
    country, then aggregated per tile inside DuckDB (bbox-filtered
    read_parquet(), see river_tile_source.py). Country-outer/tier-inner below, for the same reason as
    precip's country-outer/window-inner nesting: a country's own setup does
    not depend on which tier is being processed, so computing it once per
    country instead of once per (country, tier) avoids up to 6x redundant
    work.

    A river-flood failure never affects wind/gust/precip processing and vice
    versa (own try/except isolation at the main() call site).
    """
    river_data_store = get_river_forecasts_data_store()

    # One real Snowflake round-trip for all 6 tiers and both pointer families
    # (per-member and compact mask file) instead of 6 sequential ones (~2s per
    # round trip). See get_river_tier_pointers()'s own
    # docstring for which row a tier is processed from.
    tier_rows, bymask_candidates = get_river_tier_pointers(RIVER_RP_TIERS, target_date)

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
    # across the whole per-country loop below (many minutes across all
    # active countries), risking a session/token expiry; this check's connection
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
    # known_failed_facilities is a parameter (see this function's own
    # docstring for the full "why") so it persists and keeps accumulating
    # across the wind/gust -> precip -> river phase sequence instead of
    # resetting at the start of this one phase.
    if known_failed_facilities is None:
        known_failed_facilities = set()

    if tiers_to_process:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_paths = {}

            # Concurrent, not sequential: these are independent real network
            # downloads (real cost in a per-country profile: 6
            # sequential downloads summed to ~20.4s, dominated by whichever
            # tier's own file happens to be largest, e.g. rp2 alone at
            # 11.8s). Bounded to one worker per by-member tier in this pool
            # (at most 6), and to one per mask tier in
            # _download_river_tile_sources(), so this never over-subscribes
            # regardless of how many tiers are actually being processed.
            # A tier with a compact mask file reads that file and leaves its
            # per-member file undownloaded (fetched on demand if the mask file
            # cannot be read).
            bymask_rows = _resolve_bymask_rows(tiers_to_process, tier_rows, target_date, logger, bymask_candidates)
            bymember_tiers = [rp for rp in tiers_to_process if rp not in bymask_rows]
            with ThreadPoolExecutor(max_workers=max(1, len(bymember_tiers))) as executor:
                futures = {
                    executor.submit(download_river_forecast_parquet, tier_rows[rp]['stage_path'], river_data_store, tmp_dir): rp
                    for rp in bymember_tiers
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
            logger.info(f"River tile aggregation: compact mask files for tier(s) {sorted(bymask_rows)}")
            _download_river_tile_sources(tiers_to_process, tier_rows, bymask_rows, local_paths,
                                         river_data_store, tmp_dir, logger)

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
                per country, up to RIVER_COUNTRY_CONCURRENCY at a time, as
                admitted by _river_worker_fits(). Each call receives its own
                independent `seed_known_failed` set object (a copy taken at submission
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
                    # actually failed; the returned set is unioned into
                    # known_failed_facilities, so those mutations are kept
                    # either way.
                new_discovered = seed_known_failed
                return country, result, exc, new_discovered, time.time() - t0

            # Countries are submitted incrementally (not all at once) so each
            # one starts only when _river_worker_fits() admits it next to the
            # running one; the incoming order (COUNTRY_CODE alphabetical) is
            # kept.
            river_pending = list(countries) if tiers_to_process else []

            with ThreadPoolExecutor(max_workers=RIVER_COUNTRY_CONCURRENCY) as executor:
                for _, future in _iter_bounded_completions(
                        river_pending,
                        lambda candidate: executor.submit(
                            _process_one_country_river_concurrent, candidate, set(known_failed_facilities)),
                        RIVER_COUNTRY_CONCURRENCY,
                        may_admit=lambda ages: _river_worker_fits(ages, _river_process_tree_mb())):
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
                            _log_river_worker_telemetry(logger, result)
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
                    # the real per-country isolation happens inside the
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
        elif ambient_conn is not None and tier_files_written[rp] > 0 and not _tier_blockers and not _is_full_run:
            # Mirrors run_precip_analysis()'s own equivalent branch: real output was written and no
            # country failed, but this run's own `countries` didn't cover the full active list at the
            # moment _countries_cover_full_active_list() re-checked it, so this tier stays unmarked
            # and eligible for a genuine full run to mark it. Without this branch no case
            # would match this combination, leaving tier_files_written[rp] > 0 (loud enough to
            # trigger signal_pipeline_complete()'s MAT refresh) with zero AMBIENT_HAZARD_RUN_LOG
            # trace of why the cycle was never actually marked processed.
            logger.warning(
                f"Not marking river tier '{rp}' cycle {tier_rows[rp]['forecast_time']} processed in "
                f"AMBIENT_HAZARD_RUN_LOG: this run's own {len(countries)} countries do not cover the "
                f"real, currently-active country list, so this is not a genuine full production run. "
                f"Marking it done anyway would silently and permanently skip every country this run "
                f"never touched. Only a real full run is allowed to mark a cycle processed."
            )
        elif ambient_conn is not None and tier_files_written[rp] > 0 and _tier_blockers and _is_full_run:
            # Only a real full run's own failures count toward the give-up
            # threshold, so a partial invocation can never accidentally
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
    logger.info(f"River-flood analysis complete for {len(countries)} countries across {tiers_processed} tiers with a readable file ({files_written} files written)")
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
        skip_analysis: If True, skip the analysis step
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
    (generated by a local fetch script) and writes moderate_poverty_prob
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
        skip_analysis: If True, skip the analysis step
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
    # exposed via stats so a real caller such as main()'s CLI path can chain
    # it forward into run_precip_analysis()/run_river_flood_analysis()
    # afterward. See run_complete_impact_
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
    # Pipeline writes.
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
        # row['FORECAST_TIME'] comes from a TIMESTAMP_NTZ Snowflake column
        # (TC_ENVELOPES_COMBINED), so pd.to_datetime()/.to_pydatetime() here
        # stays naive. log_run_start()/log_run_complete() below bind this
        # straight into TC_PIPELINE_RUN_LOG.FORECAST_TIME, a TIMESTAMP_TZ
        # column, so it must be explicitly UTC-aware first, as in
        # databricks/04_scheduler_windgust.py's own wind_work construction
        # (a naive bind is reinterpreted using the session's own TIMEZONE
        # parameter rather than read as UTC).
        forecast_time_ts = pd.to_datetime(row['FORECAST_TIME']).to_pydatetime().replace(tzinfo=timezone.utc)
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
        run_id = None
        if snowflake_mode and conn:
            try:
                run_id = log_run_start(conn, storm, forecast_time_ts, countries)
            except Exception as e:
                logger.warning(f"Could not log run start to TC_PIPELINE_RUN_LOG: {e}")

        # timezone.utc, not naive: see pipeline_runlog.py's own log_run_start()/
        # log_run_complete() comments on why a naive datetime bound into a
        # TIMESTAMP_TZ column gets silently reinterpreted using the client's
        # local session timezone rather than read as UTC.
        run_started_at = datetime.now(timezone.utc)
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
                        run_id=run_id,
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
                        run_id=run_id,
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
            # Direct MAT refresh trigger, same as the Databricks schedulers:
            # this manual/CLI update path (e.g. a
            # --type update backfill run) writes real new data too and
            # deserves the same fast refresh, not just the crash-recovery
            # fallback's own much longer cadence. See
            # trigger_mat_refresh_if_due()'s own docstring for the full
            # reasoning (debounce, why skipping is safe).
            trigger_mat_refresh_if_due(conn, source="update_storms", logger=logger)
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
        help="Skip the analysis step (no impact computation)"
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
                    # Direct MAT refresh trigger, same
                    # reasoning as update_storms()'s own call above.
                    trigger_mat_refresh_if_due(conn, source="cli_ambient", logger=logger)
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
