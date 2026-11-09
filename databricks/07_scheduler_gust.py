# Databricks notebook source
# MAGIC %md
# MAGIC # DATAPIPELINE on Databricks: Gust Scheduler (delayed, decoupled from wind)
# MAGIC
# MAGIC Separate, independently-scheduled job companion to `04_scheduler_windgust.py`. That job
# MAGIC is wind-only while its `skip_gust` widget keeps the default `true`, and finishes each
# MAGIC country much faster (gust is the larger share of per-country time). This job picks up gust
# MAGIC separately, later.
# MAGIC
# MAGIC **Discovery is Blob-marker-based, not a Snowflake query**, specifically so an idle poll (no
# MAGIC gust work ready yet, the overwhelmingly common case while waiting on wind) costs no Snowflake
# MAGIC warehouse resume at all: `04_scheduler_windgust.py`'s own per-country wind-checkpoint mechanism
# MAGIC drops an empty marker file at `gust_pending/{storm}_{date}_{country}.marker` the instant that
# MAGIC country's wind result is durably saved (see `main_pipeline.py`'s `_write_gust_pending_marker()`).
# MAGIC This notebook lists that prefix (pure Blob I/O) and checks each marker's own gust output
# MAGIC existence (also pure Blob I/O, `track_views_gust/{country}_{storm}_{date}_*` prefix) BEFORE
# MAGIC ever opening a Snowflake connection. Snowflake is only touched for markers with real
# MAGIC outstanding work: fetching that country's gust envelope and running the gust-only processing
# MAGIC path (`run_complete_impact_analysis(..., skip_wind=True)`).
# MAGIC
# MAGIC `TC_GUST_RUN_LOG`/`TC_GUST_GAVE_UP_LOG` (see `pipeline_runlog.py`) are attempt/retry/
# MAGIC give-up bookkeeping for this job's own real attempts (read only for the give-up count), and
# MAGIC are NOT the discovery mechanism (that's the markers above). A marker is deleted once its gust output is confirmed written, or once
# MAGIC `check_and_record_gust_giveup()` records a permanent give-up for it; otherwise it's left in
# MAGIC place for the next poll to retry.
# MAGIC
# MAGIC Deliberately does NOT call `signal_pipeline_complete()`/`trigger_mat_refresh_if_due()`: gust is
# MAGIC not on the app's own default-cycle readiness path (`_wind_ready_at()` deliberately excludes it),
# MAGIC and the wind job's own completion signal already covers the MAT refresh a new cycle needs.
# MAGIC
# MAGIC Self-contained: installs its own dependencies and applies the required giga-spatial patches
# MAGIC directly, so it does not depend on any other notebook having run first on the same cluster.
# MAGIC Set this job's own `max_concurrent_runs: 1` at the Databricks Job level (matching
# MAGIC `04_scheduler_windgust.py`'s own setting) when creating it. A single sequential instance is
# MAGIC enough here (markers are processed one at a time in this notebook's own loop), so there is no
# MAGIC need for Snowflake-side run-log locking to prevent this job racing itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install dependencies + apply required giga-spatial patches

# COMMAND ----------

# MAGIC %pip install pandas>=2.0.0 numpy>=1.24.0 geopandas>=0.13.0 shapely>=2.0.0 pyproj>=3.4.0 \
# MAGIC     "zarr>=3.0.0" "rasterio>=1.3.0" "duckdb>=1.2.0,<3" "snowflake-connector-python[pandas]>=3.0.0" \
# MAGIC     "python-dotenv>=1.0.0" "giga-spatial[all]>=0.9.4,<0.10" "psutil>=5.9.0" "pycountry>=22.3.5" \
# MAGIC     "quantulum3[classifier]>=0.1.0" "SQLAlchemy>=2.0,<2.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import importlib.util
import pathlib


def _patched_path(module_name):
    spec = importlib.util.find_spec(module_name)
    if spec is None or spec.origin is None:
        raise RuntimeError(f"Could not locate {module_name}")
    return pathlib.Path(spec.origin)


# Same 4 giga-spatial patches as 04_scheduler_windgust.py's own COMMAND 1 (see that
# notebook's own comments for each patch's full "why"); kept identical and applied
# independently since this notebook installs its own fresh environment on its own cluster.
patch_results = {}
try:
    p = _patched_path("gigaspatial.core.io.snowflake_data_store")
    text = p.read_text()

    old4 = 'list_command = f"LIST {stage_path}"'
    new4 = "list_command = f\"LIST '{stage_path}'\""
    if "list_command = f\"LIST '{stage_path}'\"" in text:
        patch_results["list_quoting"] = "already patched upstream"
    elif old4 in text:
        text = text.replace(old4, new4)
        patch_results["list_quoting"] = "PATCHED"
    else:
        patch_results["list_quoting"] = "PATTERN NOT FOUND: giga-spatial version drift, investigate before continuing"

    old5 = 'get_command = f"GET {stage_path} \'file://{temp_dir_normalized}\'"'
    new5 = "get_command = f\"GET '{stage_path}' 'file://{temp_dir_normalized}'\""
    if "get_command = f\"GET '{stage_path}' 'file://{temp_dir_normalized}'\"" in text:
        patch_results["get_quoting"] = "already patched upstream"
    elif old5 in text:
        text = text.replace(old5, new5)
        patch_results["get_quoting"] = "PATCHED"
    else:
        patch_results["get_quoting"] = "PATTERN NOT FOUND: giga-spatial version drift, investigate before continuing"

    old6 = 'temp_download_dir = os.path.join(self._temp_dir, "downloads")\n            os.makedirs(temp_download_dir, exist_ok=True)'
    new6 = 'temp_download_dir = tempfile.mkdtemp(dir=self._temp_dir)'
    if new6 in text and old6 not in text:
        patch_results["download_race"] = "already patched upstream"
    elif old6 in text:
        text = text.replace(old6, new6)
        patch_results["download_race"] = "PATCHED"
    else:
        patch_results["download_race"] = "PATTERN NOT FOUND: giga-spatial version drift, investigate before continuing"

    old7 = ('os.remove(downloaded_path)\n'
            '            # Clean up empty directories\n'
            '            try:\n'
            '                if os.path.exists(temp_download_dir) and not os.listdir(\n'
            '                    temp_download_dir\n'
            '                ):\n'
            '                    os.rmdir(temp_download_dir)\n'
            '            except OSError:\n'
            '                pass')
    new7 = ('os.remove(downloaded_path)\n'
            '            # This whole directory is unique to this call (see download_race\n'
            '            # patch above), so removing it outright is safe: no other concurrent\n'
            '            # read_file() call can still be using it.\n'
            '            import shutil\n'
            '            shutil.rmtree(temp_download_dir, ignore_errors=True)')
    if new7 in text and old7 not in text:
        patch_results["download_cleanup"] = "already patched upstream"
    elif old7 in text:
        text = text.replace(old7, new7)
        patch_results["download_cleanup"] = "PATCHED"
    else:
        patch_results["download_cleanup"] = "PATTERN NOT FOUND: giga-spatial version drift, investigate before continuing"

    p.write_text(text)
except Exception as e:
    patch_results["list_quoting"] = patch_results.get("list_quoting", f"ERROR: {e}")
    patch_results["get_quoting"] = patch_results.get("get_quoting", f"ERROR: {e}")

for name, status in patch_results.items():
    print(f"{name:15s} {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure environment

# COMMAND ----------

import os
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_PATH = "/Workspace" + "/".join(_notebook_path.split("/")[:-2])

os.environ["DATA_PIPELINE_DB"] = "BLOB"
os.environ["HAZARD_DATA_SOURCE"] = "BLOB"

os.environ["ACCOUNT_URL"] = dbutils.secrets.get("glofas-databricks-test", "aots_views_account_url")
os.environ["SAS_TOKEN"] = dbutils.secrets.get("glofas-databricks-test", "aots_views_sas_token")
os.environ["CONTAINER_NAME"] = dbutils.secrets.get("glofas-databricks-test", "aots_views_container_name")

os.environ["HAZARD_BLOB_ACCOUNT_URL"] = os.environ["ACCOUNT_URL"]
os.environ["HAZARD_BLOB_SAS_TOKEN"] = os.environ["SAS_TOKEN"]
os.environ["HAZARD_BLOB_CONTAINER"] = os.environ["CONTAINER_NAME"]

# Snowflake credentials are required even though discovery itself is Blob-only: real
# candidate work still needs the gust envelope fetch and the TC_GUST_RUN_LOG/TC_GUST_GAVE_UP_LOG
# bookkeeping.
os.environ["SNOWFLAKE_ACCOUNT"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_account")
os.environ["SNOWFLAKE_USER"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_user")
os.environ["SNOWFLAKE_PASSWORD"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_password")
os.environ["SNOWFLAKE_WAREHOUSE"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_warehouse")
os.environ["SNOWFLAKE_DATABASE"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_database")
os.environ["SNOWFLAKE_SCHEMA"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_schema")

if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)

print("Environment configured. Repo path on sys.path:", REPO_PATH in sys.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Discover pending gust work (Blob only, no Snowflake yet)

# COMMAND ----------

import logging
import re
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("databricks_scheduler_gust")

# --- Batch-cap + circuit-breaker constants, same reasoning as 04_scheduler_windgust.py's
# own equivalents: a single poll must not turn into a many-hour outlier. ---
MAX_WORK_ITEMS_PER_RUN = 20
MAX_RUN_MINUTES = 330

dbutils.widgets.text("max_run_minutes", str(MAX_RUN_MINUTES))
MAX_RUN_MINUTES = int(dbutils.widgets.get("max_run_minutes") or MAX_RUN_MINUTES)

from pipeline_shared import data_store, ROOT_DATA_DIR, VIEWS_DIR

GUST_PENDING_DIR = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'gust_pending')
# country is always a 3-letter ISO3 code; date is always the fixed-width
# YYYYMMDDHHMMSS forecast_date string; storm is whatever's left, so this
# correctly handles a storm name that itself contains an underscore.
_MARKER_RE = re.compile(r'^(?P<storm>.+)_(?P<date>\d{14})_(?P<country>[A-Z]{3})\.marker$')

try:
    marker_paths = data_store.list_files(GUST_PENDING_DIR)
except Exception as e:
    logger.warning(f"Could not list {GUST_PENDING_DIR} (may not exist yet, i.e. no wind checkpoint has ever fired): {e}")
    marker_paths = []

markers = []
for path in marker_paths:
    m = _MARKER_RE.match(os.path.basename(path))
    if not m:
        logger.warning(f"gust_pending marker does not match the expected naming pattern, skipping: {path}")
        continue
    markers.append({"path": path, "storm": m.group("storm"), "date": m.group("date"), "country": m.group("country")})

print(f"Found {len(markers)} gust_pending marker(s)")


def _gust_output_exists(country, storm, date):
    """
    Cheap Blob existence check (no Snowflake): does this country/storm/date
    already have real gust output? Mirrors create_views_from_envelopes_in_
    country()'s own stale-cleanup prefix-listing pattern
    (impact_analysis.py), checked against track_views_gust/ specifically
    since every real gust attempt writes at least one file there (see
    save_tracks_view()'s own dataset='gust' branch).
    """
    prefix = f"{country}_{storm}_{date}_"
    try:
        existing = data_store.list_files(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views_gust'))
    except Exception as e:
        logger.warning(f"Could not list track_views_gust/ while checking {prefix}: {e}")
        return False
    return any(os.path.basename(f).startswith(prefix) for f in existing)


# Markers with real outstanding work: gust output does not exist yet. A marker
# whose gust output already exists (e.g. a prior run wrote it but a later
# crash left the marker behind) is harmless leftover, consumed here
# with no Snowflake touch at all, since there is nothing left to do.
pending_work = []
for marker in markers:
    if _gust_output_exists(marker["country"], marker["storm"], marker["date"]):
        try:
            data_store.remove(marker["path"])
        except Exception as e:
            logger.warning(f"Could not remove already-satisfied marker {marker['path']}: {e}")
        continue
    pending_work.append(marker)

print(f"{len(pending_work)} marker(s) have real outstanding gust work "
      f"({len(markers) - len(pending_work)} already satisfied, cleaned up with no Snowflake touch)")

# Batch by (storm, date) so one run_complete_impact_analysis() call below can
# cover every pending country for that cycle at once (same envelope-loading
# reuse wind's own scheduler gets from its own per-cycle batching), capped at
# MAX_WORK_ITEMS_PER_RUN total country-units per poll, same reasoning as
# 04_scheduler_windgust.py's own budget.
batch_map = {}
batch_order = []
budget = MAX_WORK_ITEMS_PER_RUN
for marker in pending_work:
    if budget <= 0:
        break
    key = (marker["storm"], marker["date"])
    if key not in batch_map:
        batch_map[key] = []
        batch_order.append(key)
    batch_map[key].append(marker)
    budget -= 1

batch = [(storm, date, batch_map[(storm, date)]) for storm, date in batch_order]
batched_items = sum(len(v) for v in batch_map.values())
print(f"This run will process {batched_items} country-unit(s) across {len(batch)} cycle(s) "
      f"(cap={MAX_WORK_ITEMS_PER_RUN}); {len(pending_work) - batched_items} marker(s) with real "
      f"work carry over to later runs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Process each pending cycle (Snowflake touched only from here on)

# COMMAND ----------

from snowflake_utils import get_snowflake_connection
from main_pipeline import (
    run_complete_impact_analysis, load_known_failed_facilities_cache, save_known_failed_facilities_cache,
)
from pipeline_runlog import (
    ensure_gust_giveup_table, log_gust_run_start, log_gust_run_complete, check_and_record_gust_giveup,
)

known_failed_facilities = load_known_failed_facilities_cache(logger)

if batch:
    try:
        log_conn = get_snowflake_connection()
        ensure_gust_giveup_table(log_conn)
    except Exception as e:
        logger.warning(f"Could not open Snowflake connection for run logging: {e}")
        log_conn = None
else:
    # Nothing to process this run: no Snowflake connection opened at all,
    # matching this whole design's point (an idle poll costs ~nothing).
    log_conn = None

RUN_START = datetime.now()
RUN_DEADLINE_TS = RUN_START.timestamp() + MAX_RUN_MINUTES * 60

gust_results = []

for storm, date, cycle_markers in batch:
    if (datetime.now() - RUN_START).total_seconds() > MAX_RUN_MINUTES * 60:
        logger.warning(
            f"Wall-clock circuit breaker hit ({MAX_RUN_MINUTES} min), stopping the gust loop early. "
            f"Nothing is lost: remaining markers simply reappear in the next run's discovery."
        )
        break

    countries_subset = [m["country"] for m in cycle_markers]
    logger.info(f"Processing gust for {storm} at {date} for {len(countries_subset)} countr(y/ies): "
                f"{', '.join(countries_subset)}...")

    # `date` (the YYYYMMDDHHMMSS string parsed from the marker filename) stays
    # a plain string everywhere else in this loop (run_complete_impact_analysis's
    # own contract), but TC_GUST_RUN_LOG.FORECAST_TIME is a TIMESTAMP_TZ column,
    # so log_gust_run_start()/log_gust_run_complete() need a real, explicitly
    # UTC-aware datetime here, not the bare string, otherwise Snowflake would
    # have to implicitly parse/interpret the string itself, using the session's
    # own TIMEZONE parameter for anything not already unambiguous, exactly the
    # same class of issue handled in 04_scheduler_windgust.py's own
    # wind_work construction (see that file's own comment for the "why").
    # This string is always exactly 14 digits (the marker regex enforces it),
    # so strptime here can never raise.
    forecast_time_dt = datetime.strptime(date, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

    started_at = datetime.now(timezone.utc)
    run_id = None
    if log_conn:
        try:
            run_id = log_gust_run_start(log_conn, storm, forecast_time_dt, countries_subset)
        except Exception as e:
            logger.warning(f"Could not log run start to TC_GUST_RUN_LOG: {e}")

    try:
        result = run_complete_impact_analysis(
            storm, date, countries_subset, logger, zoom=14, skip_wind=True,
            known_failed_facilities=known_failed_facilities, deadline_ts=RUN_DEADLINE_TS,
        )
        gust_results.append((storm, date, result))
        print(f"  {storm} {date}: {result}")

        succeeded = set(result.get("affected_countries") or [])
        errored_countries = set()
        for err in (result.get("country_errors") or []):
            token = err.split(":")[0].split(" ")[0].strip()
            if token:
                errored_countries.add(token)
        run_succeeded = result.get("success", False)

        if log_conn:
            try:
                log_gust_run_complete(
                    log_conn, storm, forecast_time_dt, success=run_succeeded,
                    countries=result.get("affected_countries") or [],
                    files_written=result.get("total_views_created", 0) or 0,
                    error_message=result.get("error") or "; ".join(result.get("country_errors") or []) or None,
                    started_at=started_at, run_id=run_id,
                )
            except Exception as e:
                logger.warning(f"Could not log run completion to TC_GUST_RUN_LOG: {e}")

        # Per-marker outcome. result.get("skipped") is True only for the
        # whole-call early-return paths (expired forecast / confirmed nothing
        # in range at all). In EITHER case no per-country determination ran
        # for anyone in this cycle, so every marker here must be left alone
        # for retry, not treated as "confirmed no impact" by an absent-from-
        # every-other-list default. Checked first and explicitly, rather than
        # inferred from an else-branch, so this can never be silently
        # mishandled by a future change to the other branches below.
        if result.get("skipped"):
            logger.info(f"{storm}/{date}: whole-call skip ({result.get('unevaluated_countries')}), "
                        f"leaving all {len(cycle_markers)} marker(s) in place for retry.")
        else:
            unevaluated = set(result.get("unevaluated_countries") or [])
            for marker in cycle_markers:
                country = marker["country"]
                if country in succeeded:
                    # Real gust output written (or a confirmed, real zero-impact
                    # determination that still counts as a successful attempt -
                    # see run_complete_impact_analysis()'s own succeeded_countries
                    # semantics), either way this marker's work is done.
                    try:
                        data_store.remove(marker["path"])
                    except Exception as e:
                        logger.warning(f"Could not remove satisfied marker {marker['path']}: {e}")
                elif country in errored_countries and log_conn:
                    try:
                        gave_up = check_and_record_gust_giveup(
                            log_conn, storm, forecast_time_dt, country, logger,
                            error_message="; ".join(result.get("country_errors") or []) or None,
                        )
                        if gave_up:
                            data_store.remove(marker["path"])
                    except Exception as e:
                        logger.warning(f"Could not check/record gust give-up for {storm}/{date}/{country}: {e}")
                elif country not in unevaluated:
                    # Not succeeded, not errored, not explicitly unevaluated:
                    # the prefilter re-ran and confirmed this country genuinely
                    # out of range for this (storm, date)'s own envelope data.
                    # Consistent with wind's own determination when this marker
                    # was first created (same underlying envelope), but logged
                    # loudly since it should be rare: a real mismatch here
                    # would otherwise silently look identical to a normal
                    # no-impact confirmation.
                    logger.warning(f"{storm}/{date}/{country}: prefilter confirmed out of range on a "
                                    f"gust-only re-check; consuming marker as a real no-impact result.")
                    try:
                        data_store.remove(marker["path"])
                    except Exception as e:
                        logger.warning(f"Could not remove marker {marker['path']}: {e}")
                # else: genuinely unevaluated this call (e.g. budget-cut) -
                # marker stays, retried next poll, no give-up check (never a
                # real FAILURE attempt for this country yet).
    except Exception as e:
        import traceback as _tb
        logger.error(f"gust {storm} {date}: {str(e)}")
        logger.debug(_tb.format_exc())
        gust_results.append((storm, date, {"success": False, "error": str(e)}))
        if log_conn:
            try:
                log_gust_run_complete(log_conn, storm, forecast_time_dt, success=False, error_message=str(e),
                                       started_at=started_at, run_id=run_id)
            except Exception as log_e:
                logger.warning(f"Could not log run failure to TC_GUST_RUN_LOG: {log_e}")
            for marker in cycle_markers:
                try:
                    gave_up = check_and_record_gust_giveup(log_conn, storm, forecast_time_dt, marker["country"], logger,
                                                             error_message=str(e))
                    if gave_up:
                        data_store.remove(marker["path"])
                except Exception as give_up_e:
                    logger.warning(f"Could not check/record gust give-up state: {give_up_e}")

if log_conn:
    try:
        log_conn.close()
    except Exception:
        pass

try:
    save_known_failed_facilities_cache(known_failed_facilities, logger)
except Exception as e:
    logger.warning(f"Could not save known_failed_facilities cache: {e}")

if not batch:
    print("No pending gust work this run.")
else:
    print(f"Gust run complete: {len(gust_results)} cycle(s) attempted.")
