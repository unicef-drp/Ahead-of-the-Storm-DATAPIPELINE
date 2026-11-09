# Databricks notebook source
# MAGIC %md
# MAGIC # DATAPIPELINE on Databricks: Wind/Gust Scheduler
# MAGIC
# MAGIC One of 4 independently-scheduled Databricks Jobs (wind, precip, river, plus the separate gust
# MAGIC job `07_scheduler_gust.py`) sharing the same cluster;
# MAGIC each job's own `max_concurrent_runs=1` only prevents that job from overlapping itself, so a
# MAGIC long river run never delays wind/gust's own trigger.
# MAGIC
# MAGIC This one handles wind only by default (the `skip_gust` widget defaults to `true`; `false` runs
# MAGIC gust inline): discovery query, backlog-alert/fairness-floor batching, and
# MAGIC circuit-breaker logic as described in Section 3/4 below. Schedule is storm-track-driven,
# MAGIC matching wind/gust's real update cadence.
# MAGIC
# MAGIC Calls `signal_pipeline_complete()` for the storm branch only, mirroring `update_storms()`'s
# MAGIC own real call site (`storm_ids=completed_storm_ids`, `countries=completed_countries`,
# MAGIC `files_written=storm_files_written`), gated the same way production does
# MAGIC (`countries_processed > 0` equivalent below). **A genuinely new, alert-worthy storm processed
# MAGIC here sends a real alert/warning email**, so confirm `ALERT_SUBSCRIBERS` is scoped as intended
# MAGIC before relying on this Job to run unattended in production.
# MAGIC
# MAGIC Since the Databricks jobs run as independent processes with no shared in-memory state,
# MAGIC known_failed_facilities is seeded from, and saved back to, the cross-run Blob/stage cache
# MAGIC (`load_known_failed_facilities_cache`/`save_known_failed_facilities_cache` in
# MAGIC `main_pipeline.py`) instead of starting empty every run (see that cache's own module-level
# MAGIC docstring for why it's a short-TTL hint layered on top of the real, permanent
# MAGIC hc_exist()/school_exist()/shelter_exist()/wash_exist() caches, not a replacement for them).
# MAGIC
# MAGIC Self-contained: installs its own dependencies and applies the required giga-spatial patches
# MAGIC directly, so it does not depend on any other notebook having run first on the same cluster.
# MAGIC
# MAGIC **Section 3a below is a Blob only precheck** that can skip the whole real discovery query
# MAGIC (and therefore the Snowflake connection it needs) on a confirmed no new data poll, the
# MAGIC overwhelmingly common case across this job's 16x/day cron. See that section's own comment
# MAGIC for the full design and the safeguards that keep it fail open.

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

# Snowflake credentials are still required even in BLOB output mode: discovery queries,
# TC_PIPELINE_RUN_LOG/AMBIENT_HAZARD_RUN_LOG bookkeeping, and signal_pipeline_complete()
# all talk to Snowflake directly regardless of where this repo's own output views land.
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
# MAGIC ## 3. Discover unprocessed wind/gust work

# COMMAND ----------

import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("databricks_scheduler_windgust")

from snowflake_utils import get_snowflake_connection

# --- Wind/gust batch-cap + fairness + circuit-breaker constants -------------------
# Batch-cap + fairness + circuit-breaker constants:
# a storm's real wind-envelope footprint can vary enormously (most reach 1-2
# countries, a wide one can reach 20+), so an uncapped run can turn into a
# many-hour, memory-heavy outlier. These bound that.
MAX_WORK_ITEMS_PER_RUN = 20
MAX_RUN_MINUTES = 330
BACKLOG_ALERT_THRESHOLD = 30
BACKLOG_ALERT_RENOTIFY_HOURS = 6

# --- Tier-3 parameterization: same notebook, different job parameters -------------
# A separate, independently-scheduled Databricks job (not sharing this job's
# own fixed 16x/day cron cadence) can target this SAME notebook with
# different widget values, a much larger max_run_minutes and much larger
# pass1_cap_s/pass2_cap_s, giving still-slow/deferred countries (see
# main_pipeline.py's slow_countries.json) a genuinely generous retry budget,
# with no second, duplicated notebook file to keep in sync. Widgets default
# to blank/the existing hardcoded value above, so this main job's own real
# schedule keeps its default behaviour unless a job explicitly
# overrides them; see main_pipeline.py's COUNTRY_PASS1_CAP_S/
# COUNTRY_PASS2_CAP_S own module-level comment for the full "why" two-pass
# deferral + tier 3 exist at all.
dbutils.widgets.text("max_run_minutes", str(MAX_RUN_MINUTES))
dbutils.widgets.text("pass1_cap_s", "")  # blank = run_complete_impact_analysis()'s own default (main job)
dbutils.widgets.text("pass2_cap_s", "")  # blank = run_complete_impact_analysis()'s own default (main job)
# "true" (this job's own default, the wind/gust split): this job is wind-only, pairing it with the separate, delayed
# databricks/07_scheduler_gust.py picking up gust work later via the Blob-
# marker hand-off (see main_pipeline.py's _write_gust_pending_marker()).
# "false" runs wind and gust inline together in the same job.
dbutils.widgets.text("skip_gust", "true")
MAX_RUN_MINUTES = int(dbutils.widgets.get("max_run_minutes") or MAX_RUN_MINUTES)
_pass1_cap_s_widget = dbutils.widgets.get("pass1_cap_s").strip()
_pass2_cap_s_widget = dbutils.widgets.get("pass2_cap_s").strip()
# Only ever a real, positive integer or None; never passed through to
# run_complete_impact_analysis() as an empty string, which would break its
# own real int arithmetic (time.time() + pass1_cap_s).
PASS1_CAP_S_OVERRIDE = int(_pass1_cap_s_widget) if _pass1_cap_s_widget else None
PASS2_CAP_S_OVERRIDE = int(_pass2_cap_s_widget) if _pass2_cap_s_widget else None
SKIP_GUST = dbutils.widgets.get("skip_gust").strip().lower() == "true"

PIPELINE_OPS_RECIPIENTS = [
    r.strip() for r in dbutils.secrets.get("glofas-databricks-test", "pipeline_ops_recipients").split(",")
    if r.strip()
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3a. Blob only precheck: skip Snowflake entirely on a confirmed no new data poll

# COMMAND ----------

# TC_ENVELOPES_COMBINED/TC_TRACKS (read by the discovery query below) are themselves loaded
# FROM Blob by TC_ECMWF_Forecast_Pipeline's github_actions/blob_to_snowflake_loader.py, both
# immediately after each real cycle's own Blob upload and via a periodic catch up safety net
# (see that module's own docstring). Blob is the genuine upstream source for both tables, never
# the reverse, and both repos read the very same ACCOUNT_URL/SAS_TOKEN/CONTAINER_NAME (see
# pipeline_core.py's own comment in that repo), the same container already configured above as
# HAZARD_BLOB_ACCOUNT_URL/HAZARD_BLOB_SAS_TOKEN/HAZARD_BLOB_CONTAINER. So a cheap Blob only
# listing of the tracks/ and envelopes/ prefixes can rule out "nothing new has landed" with no
# Snowflake connection at all, as long as it is paired with the two further safeguards below.
#
# A skip is only taken when ALL of the following hold:
#   1. No blob under tracks/ or envelopes/ has a last_modified after the watermark saved the
#      last time a REAL discovery query ran.
#   2. That last real discovery query found zero outstanding candidates
#      (backlog_confirmed_clear). Real backlog can persist across polls purely from this job's
#      own MAX_WORK_ITEMS_PER_RUN budget carryover or a slow country deferral, with nothing to
#      do with new Blob arrivals, so a real query must keep running every poll until it is
#      confirmed empty again, independent of Blob state.
#   3. No more than _WIND_PRECHECK_MAX_SKIP_HOURS have passed since that last real check. The
#      discovery query's own STARTED_AT retry lock window below (search this file for
#      "STARTED_AT > DATEADD") means a candidate can become newly eligible purely because a
#      stale IN_PROGRESS row aged past that window, with no new Blob data involved at all.
#      Forcing a real check comfortably inside that window bounds how long such a candidate
#      could otherwise sit undiscovered.
#
# A missing or unreadable prior state file, a Blob listing error, or an unparseable saved
# timestamp all fall straight through to the unchanged real discovery path below, exactly as it
# already runs today: silently missing a real forecast cycle to save a few Snowflake dollars
# would be a far worse outcome than the cost problem this precheck exists to reduce.

import json as _precheck_json
from pipeline_shared import data_store as _precheck_data_store, ROOT_DATA_DIR as _PRECHECK_ROOT_DIR, VIEWS_DIR as _PRECHECK_VIEWS_DIR

_WIND_PRECHECK_STATE_PATH = os.path.join(_PRECHECK_ROOT_DIR, _PRECHECK_VIEWS_DIR, "discovery_state", "wind_blob_precheck.json")
_WIND_BLOB_PREFIXES = ("tracks", "envelopes")
_WIND_PRECHECK_MAX_SKIP_HOURS = 4


def _wind_precheck_list_last_modified(prefix):
    """
    Every real last_modified timestamp for blobs under prefix, a direct Azure SDK call
    mirroring TC_ECMWF_Forecast_Pipeline's own _list_blob_files() helper: a single
    list_blobs() call already returns last_modified for every blob it lists, so reading it
    here costs nothing extra over the network beyond the listing itself.
    """
    from azure.storage.blob import BlobServiceClient
    client = BlobServiceClient(
        account_url=os.environ["HAZARD_BLOB_ACCOUNT_URL"],
        credential=os.environ["HAZARD_BLOB_SAS_TOKEN"],
    )
    container_client = client.get_container_client(os.environ["HAZARD_BLOB_CONTAINER"])
    return [b.last_modified for b in container_client.list_blobs(name_starts_with=prefix)
            if b.last_modified is not None]


def _wind_precheck_load_state():
    try:
        if not _precheck_data_store.file_exists(_WIND_PRECHECK_STATE_PATH):
            return None
        raw = _precheck_data_store.read_file(_WIND_PRECHECK_STATE_PATH)
        return _precheck_json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except Exception as e:
        logger.warning(f"Could not read wind Blob precheck state, treating as unknown: {e}")
        return None


def _wind_precheck_current_newest_blob_timestamp():
    """
    A fresh, direct newest last_modified across tracks/ and envelopes/, independent of
    should_skip()'s own Blob listing (that one only runs once a valid prior watermark
    already exists). Seeds or refreshes the watermark every time a real discovery
    check runs, so a cold start (no state file yet, prior watermark None) can still
    establish a real watermark instead of saving None forever and staying permanently
    unable to skip on every future poll.
    """
    try:
        timestamps = []
        for prefix in _WIND_BLOB_PREFIXES:
            timestamps.extend(_wind_precheck_list_last_modified(prefix))
        return max(timestamps).isoformat() if timestamps else None
    except Exception as e:
        logger.warning(f"Could not refresh wind Blob precheck watermark: {e}")
        return None


def _wind_precheck_save_state(watermark_iso, backlog_confirmed_clear, real_check):
    """
    real_check=True only after this poll ran a genuine Snowflake discovery query, the one
    case allowed to advance last_real_check_at, the timestamp _WIND_PRECHECK_MAX_SKIP_HOURS
    bounds against. real_check=False after a skip decision, which must leave
    last_real_check_at exactly as the last real check left it; stamping it with the current time on a
    skip too would let a chain of successful skips keep pushing that bound out indefinitely,
    defeating the one safeguard it exists to provide. Best effort otherwise: a failed write
    only means the next poll cannot skip and simply re-checks Snowflake for real, never a
    correctness problem, matching this repo's existing known_failed_facilities cache save
    pattern (main_pipeline.py).
    """
    try:
        last_real_check_at_iso = datetime.now(timezone.utc).isoformat()
        if not real_check:
            prior_state = _wind_precheck_load_state()
            last_real_check_at_iso = (prior_state or {}).get("last_real_check_at", last_real_check_at_iso)
        payload = {
            "watermark": watermark_iso,
            "backlog_confirmed_clear": backlog_confirmed_clear,
            "last_real_check_at": last_real_check_at_iso,
        }
        _precheck_data_store.write_file(_WIND_PRECHECK_STATE_PATH, _precheck_json.dumps(payload))
    except Exception as e:
        logger.warning(f"Could not save wind Blob precheck state: {e}")


def _wind_precheck_should_skip():
    """
    Returns (should_skip, newest_seen_iso_or_None, reason). newest_seen_iso_or_None is the
    current newest last_modified observed across both prefixes (None only when the Blob
    listing itself failed), returned regardless of the skip decision so the real discovery
    path below can still use it as this run's fresh watermark.
    """
    prior_state = _wind_precheck_load_state()
    if prior_state is None:
        return False, None, "no prior precheck state (first run, or a prior save failed)"
    if not prior_state.get("backlog_confirmed_clear"):
        return False, None, "last real discovery still had outstanding backlog"

    last_real_check_iso = prior_state.get("last_real_check_at")
    try:
        last_real_check_at = datetime.fromisoformat(last_real_check_iso) if last_real_check_iso else None
    except Exception:
        last_real_check_at = None
    if last_real_check_at is None:
        return False, None, "prior state missing a valid last_real_check_at"
    if datetime.now(timezone.utc) - last_real_check_at > timedelta(hours=_WIND_PRECHECK_MAX_SKIP_HOURS):
        return False, None, f"more than {_WIND_PRECHECK_MAX_SKIP_HOURS}h since the last real check, forcing a recheck"

    prior_watermark_iso = prior_state.get("watermark")
    try:
        prior_watermark = datetime.fromisoformat(prior_watermark_iso) if prior_watermark_iso else None
    except Exception:
        prior_watermark = None
    if prior_watermark is None:
        return False, None, "prior state missing a valid watermark"

    try:
        timestamps = []
        for prefix in _WIND_BLOB_PREFIXES:
            timestamps.extend(_wind_precheck_list_last_modified(prefix))
    except Exception as e:
        return False, None, f"Blob listing failed, cannot confirm no new data: {e}"

    if not timestamps:
        return True, prior_watermark_iso, "no track/envelope blobs exist at all yet"

    newest_seen = max(timestamps)
    newest_seen_iso = newest_seen.isoformat()
    if newest_seen > prior_watermark:
        return False, newest_seen_iso, "new track/envelope data found since the last real check"
    return True, newest_seen_iso, "no new track/envelope data since the last real check"


_wind_should_skip, _wind_precheck_watermark_iso, _wind_precheck_reason = _wind_precheck_should_skip()
print(f"Wind/gust Blob precheck: skip={_wind_should_skip} ({_wind_precheck_reason})")

if _wind_should_skip:
    _wind_precheck_save_state(_wind_precheck_watermark_iso, backlog_confirmed_clear=True, real_check=False)
    logger.info(
        f"No new Blob track/envelope data since the last real check and last real check found "
        f"zero backlog: skipping this poll's Snowflake discovery entirely ({_wind_precheck_reason})."
    )
    dbutils.notebook.exit(f"skipped: {_wind_precheck_reason}")

# COMMAND ----------

conn = get_snowflake_connection()
cur = conn.cursor()

# Must exist before the discovery query below (it NOT EXISTS-checks this
# table on every run); ensure_windgust_giveup_table() only creates it
# lazily on an actual give-up otherwise, which would make the first
# run fail with a real "table does not exist" error.
from main_pipeline import ensure_windgust_giveup_table
ensure_windgust_giveup_table(conn)

# Candidate (track, cycle, country) triples come from the candidate country cache
# (pipeline_runlog.discover_candidate_cycles), which runs the spatial join only for
# cycles it has not seen, and falls back to the live join on any problem. The triples
# are reduced to distinct rows before the filtering and aggregation below, so no
# GEOGRAPHY column is carried into the NOT EXISTS clauses; with the filters correlated
# directly against the join, the optimizer groups on the full ENVELOPE_REGION value per
# (envelope, country) pair, which is slow and exhausts memory on an X-Small warehouse
# (intermittent Snowflake "error 300005" internal errors once the 2-day window holds
# large storms).
from pipeline_runlog import discover_candidate_cycles, CANDIDATE_PAIRS_PLACEHOLDER

_DISCOVERY_SQL = """
    WITH pairs AS (
        __CANDIDATE_PAIRS__
    )
    SELECT p.TRACK_ID, p.FORECAST_TIME,
           ARRAY_AGG(DISTINCT p.COUNTRY_CODE) AS CANDIDATE_COUNTRIES
    FROM   pairs p
    WHERE  TRUE
      -- "Already done" is read from TC_PIPELINE_RUN_LOG itself (real output
      -- written, or confirmed real zero-impact), not inferred from whether
      -- MERCATOR_TILE_IMPACT_MAT/MERCATOR_TILE_GUST_MAT happen to have rows
      -- for this country/cycle: a country that genuinely has zero real wind/
      -- gust impact writes a legitimate all-zero row, indistinguishable by
      -- row presence alone from a country whose subprocess was killed before
      -- finishing. log_run_complete()'s own docstring already documents the
      -- intended semantics this relies on: a country lands in neither
      -- COUNTRIES_PROCESSED nor NO_IMPACT_COUNTRIES when it errored, and
      -- deliberately stays eligible for retry.
      -- rl.FORECAST_TIME = p.FORECAST_TIME, not a bare comparison: te.FORECAST_TIME
      -- (TC_ENVELOPES_COMBINED) is TIMESTAMP_NTZ, rl.FORECAST_TIME (TC_PIPELINE_RUN_LOG)
      -- is TIMESTAMP_TZ. A bare NTZ = TZ comparison implicitly promotes the naive side
      -- using the SESSION's own TIMEZONE parameter (America/Los_Angeles), not treating
      -- it as already-UTC. TC_PIPELINE_RUN_LOG.FORECAST_TIME is written genuinely
      -- UTC-tagged (see pipeline_runlog.py's log_run_start()/log_run_complete()
      -- comments), so a bare comparison would match no already-SUCCESS row at all:
      -- this exclusion would never fire and already-complete work would be
      -- rediscovered and reprocessed from scratch on every poll, wasting compute
      -- and inflating the scheduler's backlog-alert count.
      -- CONVERT_TIMEZONE('UTC', rl.FORECAST_TIME) on a genuinely tz-aware value is a
      -- real, session-independent instant conversion, not an implicit reinterpretation,
      -- so this correctly matches te.FORECAST_TIME's own naive-but-really-UTC value
      -- regardless of the session's TIMEZONE parameter.
      AND  NOT EXISTS (
               SELECT 1
               FROM   TC_PIPELINE_RUN_LOG rl
               WHERE  rl.STORM_ID      = p.TRACK_ID
                 AND  CONVERT_TIMEZONE('UTC', rl.FORECAST_TIME)::TIMESTAMP_NTZ = p.FORECAST_TIME
                 AND  rl.STATUS = 'SUCCESS'
                 AND  (ARRAY_CONTAINS(p.COUNTRY_CODE::VARIANT, rl.COUNTRIES_PROCESSED)
                       OR ARRAY_CONTAINS(p.COUNTRY_CODE::VARIANT, rl.NO_IMPACT_COUNTRIES))
           )
      AND  NOT EXISTS (
               SELECT 1
               FROM   TC_PIPELINE_RUN_LOG rl
               WHERE  rl.STORM_ID      = p.TRACK_ID
                 AND  rl.FORECAST_TIME = p.FORECAST_TIME
                 AND  rl.STATUS IN ('IN_PROGRESS', 'TRIGGERED')
                 -- CURRENT_TIMESTAMP(), not SYSDATE(): rl.STARTED_AT is genuinely
                 -- TIMESTAMP_TZ (confirmed via DESC TABLE). CURRENT_TIMESTAMP() (TIMESTAMP_LTZ)
                 -- compared against a TZ column is a true absolute-instant comparison,
                 -- unaffected by the session's TIMEZONE parameter. SYSDATE() (a naive
                 -- TIMESTAMP_NTZ) would actually be WRONG here; Snowflake promotes it to
                 -- an instant using the session timezone before comparing against a TZ
                 -- column, reintroducing exactly the dependency this must avoid.
                 -- rl.STARTED_AT's own value is written as a genuine, timezone-aware UTC
                 -- instant by pipeline_runlog.py's log_run_start()/log_run_complete() (see
                 -- their own comments), so this comparison holds correctly on both sides.
                 AND  rl.STARTED_AT > DATEADD('hour', -6, CURRENT_TIMESTAMP())
                 -- TC_PIPELINE_RUN_LOG is insert-only: log_run_start() inserts this
                 -- IN_PROGRESS row, and log_run_complete() later inserts a SEPARATE
                 -- SUCCESS/FAILURE row rather than updating this one, so this marker
                 -- never goes away even after the real work it represents has long
                 -- finished. Without this check, a batch that legitimately only had
                 -- budget for SOME of a storm's real candidate countries this poll
                 -- (see MAX_WORK_ITEMS_PER_RUN below) leaves its own already-resolved
                 -- IN_PROGRESS row blocking the REST of that storm's countries from
                 -- being reconsidered for the rest of this 6h window, even though the
                 -- attempt that inserted it finished in seconds. A candidate can be
                 -- starved this way after a multi-storm poll splits the shared
                 -- budget. A
                 -- terminal row for this exact (storm, forecast_time) completed AFTER
                 -- this IN_PROGRESS row started means that attempt has already
                 -- concluded one way or another, so the marker does not represent
                 -- real in-flight work and must not keep locking out this cycle.
                 --
                 -- RUN_ID exact match, not just "any terminal row completed after
                 -- this one started": the latter can't distinguish this attempt's
                 -- own completion from a different, unrelated, faster attempt for
                 -- the same (storm, forecast_time) finishing later purely by chance
                 -- while this one is still genuinely running, which would
                 -- prematurely release a real in-flight lock (a real, if narrow,
                 -- residual risk). Falls back to the
                 -- timestamp comparison only for a row that has no RUN_ID
                 -- (RUN_ID IS NULL), so an orphaned IN_PROGRESS row without a
                 -- RUN_ID doesn't lock forever.
                 --
                 -- Two separate NOT EXISTS clauses (De Morgan's law, NOT(A OR B) =
                 -- NOT A AND NOT B), not one NOT EXISTS with an internal OR: Snowflake
                 -- rejects a correlated subquery whose WHERE clause ORs two
                 -- conditions that each reference outer-scope columns ("Unsupported
                 -- subquery type cannot be evaluated"). Splitting into two ANDed NOT EXISTS avoids the OR
                 -- entirely and is logically identical.
                 AND  NOT EXISTS (
                          SELECT 1
                          FROM   TC_PIPELINE_RUN_LOG rl2
                          WHERE  rl2.STORM_ID      = rl.STORM_ID
                            AND  rl2.FORECAST_TIME = rl.FORECAST_TIME
                            AND  rl2.STATUS IN ('SUCCESS', 'FAILURE')
                            AND  rl.RUN_ID IS NOT NULL AND rl2.RUN_ID = rl.RUN_ID
                      )
                 AND  NOT EXISTS (
                          SELECT 1
                          FROM   TC_PIPELINE_RUN_LOG rl2
                          WHERE  rl2.STORM_ID      = rl.STORM_ID
                            AND  rl2.FORECAST_TIME = rl.FORECAST_TIME
                            AND  rl2.STATUS IN ('SUCCESS', 'FAILURE')
                            AND  rl.RUN_ID IS NULL AND rl2.COMPLETED_AT > rl.STARTED_AT
                      )
           )
      -- A cycle this storm has already been LOUDLY given up on (see
      -- pipeline_runlog.py's own "GIVE-UP GATE" section) stops being a
      -- candidate here, NOT because it's done or confirmed zero-impact
      -- (it's neither, see that file's own comment for why this is
      -- deliberately never folded into COUNTRIES_PROCESSED/
      -- NO_IMPACT_COUNTRIES above), only because retrying it forever is
      -- pointless once it's reached WINDGUST_MAX_FAILURE_ATTEMPTS real
      -- failures. TC_PIPELINE_GAVE_UP_LOG is guaranteed to exist by the
      -- ensure_windgust_giveup_table(conn) call above, even on a run where
      -- nothing has ever given up yet.
      AND  NOT EXISTS (
               SELECT 1
               FROM   TC_PIPELINE_GAVE_UP_LOG gul
               WHERE  gul.STORM_ID      = p.TRACK_ID
                 AND  gul.FORECAST_TIME = p.FORECAST_TIME
           )
    GROUP BY p.TRACK_ID, p.FORECAST_TIME
    HAVING ARRAY_SIZE(ARRAY_AGG(DISTINCT p.COUNTRY_CODE)) > 0
    ORDER BY p.FORECAST_TIME ASC
"""
assert CANDIDATE_PAIRS_PLACEHOLDER in _DISCOVERY_SQL
wind_work_raw = discover_candidate_cycles(cur, _DISCOVERY_SQL, logger)
cur.close()
# conn stays open here, reused below for get_active_countries_from_snowflake() and the
# backlog-alert check/insert, both of which run moments later with only cheap, no-I/O Python
# in between. On the overwhelmingly common no-op poll (nothing new to process), this is the
# only real work this scheduler does, so avoiding 2 extra full connect/auth round trips here
# is a real, if modest, reduction in AOTS_WH round trips on every single poll, not just the
# rare ones with real backlog.

# The Snowflake Python connector returns an ARRAY_AGG column as a pretty-printed JSON
# STRING (e.g. '[\n  "MEX"\n]'), not a native Python list, confirmed directly against a
# real query result. Every downstream use of this column (country counts, per-country
# batching, run_complete_impact_analysis()'s own countries argument) needs a real list of
# country codes, so parse it here once rather than at each call site. Without this,
# list(candidates) iterates the STRING's individual characters instead of its country
# codes: a single-country cycle explodes into ~10 fake "country-units", corrupts
# TC_PIPELINE_RUN_LOG's COUNTRIES_PROCESSED/NO_IMPACT_COUNTRIES arrays, and passes garbage
# single-character strings into run_complete_impact_analysis(), which can never match a
# real ISO3 code, so the batch always reports countries_processed=0.
import json as _json


def _parse_candidate_countries(value):
    if isinstance(value, list):
        return value
    try:
        parsed = _json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    logger.warning(f"Could not parse CANDIDATE_COUNTRIES value as a JSON list, treating as no candidates: {value!r}")
    return []


wind_work = [
    # te.FORECAST_TIME is TIMESTAMP_NTZ (per DESC TABLE on
    # TC_ENVELOPES_COMBINED), so the connector returns a NAIVE Python
    # datetime here, one wall-clock value with no zone attached. By this
    # pipeline's own convention that wall-clock value already IS UTC (same
    # convention TC_ENVELOPES_COMBINED's own writer follows). This naive
    # value gets passed straight into log_run_start()/log_run_complete()
    # below, which bind it into TC_PIPELINE_RUN_LOG.FORECAST_TIME, a
    # TIMESTAMP_TZ column. It is labelled UTC explicitly: a naive bind would
    # be tagged with the session's America/Los_Angeles TIMEZONE offset
    # (-07:00) instead of Z, and is_already_processed()'s/the app's own
    # _wind_ready_at() exact-timestamp joins against TC_ENVELOPES_COMBINED
    # would then never match.
    # .replace(tzinfo=timezone.utc), not .astimezone(): the wall-clock
    # digits are already correct and must stay unchanged, this only labels
    # them as UTC instead of shifting them.
    (track_id, forecast_time.replace(tzinfo=timezone.utc), _parse_candidate_countries(candidates))
    for track_id, forecast_time, candidates in wind_work_raw
]

print(f"Wind/gust: {len(wind_work)} unprocessed storm cycle(s)")

# This run just paid for a real discovery query regardless, so record its outcome for the
# Blob precheck above to use on the NEXT poll. Prefer the newest_seen_iso should_skip()
# already computed above; that call only reaches its own Blob listing once a valid prior
# watermark already exists, so it comes back None both on the very first run ever (no state
# file yet) and whenever that listing itself failed. Try one more fresh listing directly here
# in either case, so the watermark can self heal instead of staying None forever, which would
# otherwise leave this precheck permanently unable to skip on every future poll. Only if that
# also fails does this fall back to whatever was last saved (still a real, already-observed
# past timestamp, never a fabricated one). backlog_confirmed_clear reflects this run's own
# real, just executed discovery result either way.
_wind_precheck_watermark_to_save = _wind_precheck_watermark_iso
if _wind_precheck_watermark_to_save is None:
    _wind_precheck_watermark_to_save = _wind_precheck_current_newest_blob_timestamp()
if _wind_precheck_watermark_to_save is None:
    _prior_state_for_save = _wind_precheck_load_state()
    _wind_precheck_watermark_to_save = (_prior_state_for_save or {}).get("watermark")
_wind_precheck_save_state(_wind_precheck_watermark_to_save, backlog_confirmed_clear=(len(wind_work) == 0), real_check=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Process each unprocessed storm cycle

# COMMAND ----------

from country_utils import get_active_countries_from_snowflake
from main_pipeline import (
    run_complete_impact_analysis, log_run_start, log_run_complete,
    check_and_record_windgust_giveup,
    load_known_failed_facilities_cache, save_known_failed_facilities_cache,
)

active_countries = get_active_countries_from_snowflake(conn=conn)
print(f"Active countries: {len(active_countries)}")

total_backlog_items = sum(len(candidates) for _, _, candidates in wind_work)
distinct_storms_backlogged = sorted({track_id for track_id, _, _ in wind_work})
print(f"Wind/gust real backlog: {total_backlog_items} country-unit(s) across "
      f"{len(distinct_storms_backlogged)} storm(s) ({', '.join(distinct_storms_backlogged) or 'none'}), "
      f"{len(wind_work)} cycle(s)")

# Reuses the same `conn` opened above for the discovery query: this alert check/insert runs
# unconditionally on every poll (even zero backlog), so on the overwhelmingly common no-op
# poll it and the discovery query above are the ONLY real work done; sharing one connection
# across both is a real, if modest, reduction in AOTS_WH round trips on every single poll.
try:
    alert_cur = conn.cursor()
    alert_cur.execute("""
        SELECT MAX(CHECKED_AT) FROM AOTS.TC_ECMWF.WIND_BACKLOG_ALERT_LOG WHERE ALERT_SENT = TRUE
    """)
    last_alert_at = alert_cur.fetchone()[0]
    should_alert = total_backlog_items >= BACKLOG_ALERT_THRESHOLD and (
        last_alert_at is None
        or (datetime.now() - last_alert_at) > timedelta(hours=BACKLOG_ALERT_RENOTIFY_HOURS)
    )
    if should_alert:
        subject = f"[DATAPIPELINE] Wind/gust backlog at {total_backlog_items} country-units"
        body = (
            f"<p>The Databricks DATAPIPELINE wind/gust scheduler's backlog is "
            f"<b>{total_backlog_items} country-units</b> across storm(s) "
            f"{', '.join(distinct_storms_backlogged)}, at or above the "
            f"{BACKLOG_ALERT_THRESHOLD}-item alert threshold. This run will process up to "
            f"{MAX_WORK_ITEMS_PER_RUN} of them; the rest carry over to subsequent runs. "
            f"Informational unless this keeps climbing across repeated checks.</p>"
        )
        for recipient in PIPELINE_OPS_RECIPIENTS:
            alert_cur.execute(
                "CALL SYSTEM$SEND_EMAIL('AOTS_EMAIL_INTEGRATION', %s, %s, %s, 'text/html')",
                (recipient, subject, body),
            )
        logger.warning(f"Wind/gust backlog alert sent ({total_backlog_items} country-units)")
    alert_cur.execute(
        "INSERT INTO AOTS.TC_ECMWF.WIND_BACKLOG_ALERT_LOG (BACKLOG_COUNT, ALERT_SENT) VALUES (%s, %s)",
        (total_backlog_items, should_alert),
    )
    conn.commit()
    alert_cur.close()
except Exception as e:
    logger.warning(f"Could not check/send wind backlog alert: {e}")
finally:
    conn.close()

storm_queues = {}
storm_order = []
for track_id, forecast_time, candidates in wind_work:
    if track_id not in storm_queues:
        storm_queues[track_id] = []
        storm_order.append(track_id)
    storm_queues[track_id].append([forecast_time, list(candidates)])

budget = MAX_WORK_ITEMS_PER_RUN
batch_map = {}
batch_order = []


def _take(track_id, forecast_time, countries, n):
    take_n = min(n, budget, len(countries))
    if take_n <= 0:
        return 0
    taken = countries[:take_n]
    del countries[:take_n]
    key = (track_id, forecast_time)
    if key not in batch_map:
        batch_map[key] = []
        batch_order.append(key)
    batch_map[key].extend(taken)
    return take_n


if storm_order:
    floor_per_storm = min(5, -(-MAX_WORK_ITEMS_PER_RUN // len(storm_order)))
    for track_id in storm_order:
        if budget <= 0:
            break
        for forecast_time, countries in storm_queues[track_id]:
            if countries:
                budget -= _take(track_id, forecast_time, countries, floor_per_storm)
                break

if budget > 0:
    for track_id, forecast_time, _ in wind_work:
        if budget <= 0:
            break
        for ft2, countries in storm_queues[track_id]:
            if ft2 == forecast_time and countries:
                budget -= _take(track_id, forecast_time, countries, len(countries))
                break

batch = [(t, f, batch_map[(t, f)]) for t, f in batch_order]
batched_items = sum(len(c) for _, _, c in batch)
print(f"This run will process {batched_items} country-unit(s) across {len(batch)} cycle(s) "
      f"(cap={MAX_WORK_ITEMS_PER_RUN}); {total_backlog_items - batched_items} country-unit(s) "
      f"carry over to later runs.")

wind_results = []
completed_storm_ids = []
completed_countries = set()
storm_files_written = 0
known_failed_facilities = load_known_failed_facilities_cache(logger)

if batch:
    try:
        log_conn = get_snowflake_connection()
    except Exception as e:
        logger.warning(f"Could not open Snowflake connection for run logging: {e}")
        log_conn = None
else:
    # Nothing to process this run: every use of log_conn below is inside the
    # per-batch-item loop or the final close(), so skipping the connection
    # entirely here avoids an avoidable extra Snowflake round trip on every
    # no-op poll (batch is already known above, before this point).
    log_conn = None

RUN_START = datetime.now()
# Real absolute deadline (Unix epoch, not a "remaining minutes" figure that
# would need continuous recomputation), passed into run_complete_impact_
# analysis() below so a country's own progress-aware subprocess extension
# (see main_pipeline.py's COUNTRY_SUBPROCESS_TIMEOUT_S/deadline_ts docstrings)
# is bounded by THIS run's own real remaining budget, not left unbounded.
# .timestamp() on a naive local datetime correctly reverses back to the real
# UTC epoch using the system's own local tz rules: safe here (pure Python
# stdlib on one machine), unlike a TIMESTAMP_TZ column bind, where the
# Snowflake session timezone applies (see pipeline_runlog.py's own
# STARTED_AT/COMPLETED_AT handling).
RUN_DEADLINE_TS = RUN_START.timestamp() + MAX_RUN_MINUTES * 60

for track_id, forecast_time, countries_subset in batch:
    if (datetime.now() - RUN_START).total_seconds() > MAX_RUN_MINUTES * 60:
        logger.warning(
            f"Wall-clock circuit breaker hit ({MAX_RUN_MINUTES} min), stopping the wind/gust "
            f"loop early. Nothing is lost: remaining batched items simply reappear in the next "
            f"run's discovery."
        )
        break
    date_str = forecast_time.strftime("%Y%m%d%H%M%S")
    logger.info(f"Processing {track_id} at {date_str} for {len(countries_subset)} countr(y/ies): "
                f"{', '.join(countries_subset)}...")
    # timezone.utc, not naive: this value is later passed as log_run_complete()'s
    # own `started_at`, which binds straight into a TIMESTAMP_TZ column. A naive
    # datetime there gets silently reinterpreted using the client's local session
    # timezone (for example a terminal row's own STARTED_AT would be off by a
    # full 7h under America/Los_Angeles); pipeline_runlog.py's
    # log_run_start()/log_run_complete() guard the same way for their own
    # STARTED_AT/COMPLETED_AT writes.
    started_at = datetime.now(timezone.utc)
    run_id = None
    if log_conn:
        try:
            run_id = log_run_start(log_conn, track_id, forecast_time, countries_subset)
        except Exception as e:
            logger.warning(f"Could not log run start to TC_PIPELINE_RUN_LOG: {e}")
    try:
        _pass_cap_kwargs = {}
        if PASS1_CAP_S_OVERRIDE is not None:
            _pass_cap_kwargs["pass1_cap_s"] = PASS1_CAP_S_OVERRIDE
        if PASS2_CAP_S_OVERRIDE is not None:
            _pass_cap_kwargs["pass2_cap_s"] = PASS2_CAP_S_OVERRIDE
        result = run_complete_impact_analysis(track_id, date_str, countries_subset, logger, zoom=14,
                                               known_failed_facilities=known_failed_facilities,
                                               deadline_ts=RUN_DEADLINE_TS, skip_gust=SKIP_GUST, **_pass_cap_kwargs)
        wind_results.append((track_id, forecast_time, result))
        print(f"  {track_id} {date_str}: {result}")

        requested = set(countries_subset)
        succeeded = set(result.get("affected_countries") or [])
        errored_countries = set()
        for err in (result.get("country_errors") or []):
            token = err.split(":")[0].split(" ")[0].strip()
            if token:
                errored_countries.add(token)
        country_files_written = result.get("country_files_written") or {}
        wrote_zero = {c for c in succeeded if country_files_written.get(c, 0) == 0}
        # unevaluated_countries (per-country precision) is used instead of a blanket
        # batch-level check, since sql_prefilter_used only indicates which pre-filter
        # path ran (SQL/cache vs. the live per-country Python buffer check), not whether
        # the result is trustworthy: a fully-successful Python-fallback determination
        # is just as reliable as an SQL one. unevaluated_countries lists exactly the
        # countries this call could not determine affected/not-affected for; every other
        # requested country got a real determination and is safe to mark no-impact, even
        # if another country in the same batch is unevaluated. If unevaluated_countries
        # is absent (an older main_pipeline.py without this field), treat the entire
        # batch as unevaluated (the conservative default).
        unevaluated = result.get("unevaluated_countries")
        if unevaluated is None:
            no_impact = set()
        else:
            no_impact = (wrote_zero | (requested - succeeded - errored_countries)) - set(unevaluated)

        run_succeeded = result.get("success", False)
        # A country cut off by its time budget lands in unevaluated_countries and stays retryable,
        # but the row would otherwise look like an empty, error-free SUCCESS.
        budget_cut = sorted(set(unevaluated or []) & requested - succeeded)
        base_error = result.get("error") or "; ".join(result.get("country_errors") or []) or None
        if run_succeeded and budget_cut:
            budget_note = f"unevaluated (budget cut, will retry): {','.join(budget_cut)}"
            base_error = f"{base_error}; {budget_note}" if base_error else budget_note
        if log_conn:
            try:
                log_run_complete(
                    log_conn, track_id, forecast_time,
                    success=run_succeeded,
                    countries=result.get("affected_countries") or [],
                    no_impact_countries=list(no_impact),
                    files_written=result.get("total_views_created", 0) or 0,
                    error_message=base_error,
                    started_at=started_at,
                    run_id=run_id,
                )
            except Exception as e:
                logger.warning(f"Could not log run completion to TC_PIPELINE_RUN_LOG: {e}")
        if run_succeeded and result.get("countries_processed", 0) > 0:
            if track_id not in completed_storm_ids:
                completed_storm_ids.append(track_id)
            completed_countries.update(result.get("affected_countries") or [])
            storm_files_written += result.get("total_views_created", 0) or 0
        elif not run_succeeded and log_conn:
            # See pipeline_runlog.py's own "GIVE-UP GATE" comment block for the
            # full "why": a genuine repeated-FAILURE cycle otherwise just keeps
            # being retried until it silently ages out of the scheduler's own
            # lookback window, with no alert and no queryable signal.
            still_failed = errored_countries or requested
            try:
                check_and_record_windgust_giveup(
                    log_conn, track_id, forecast_time, list(still_failed), logger,
                    error_message=result.get("error") or "; ".join(result.get("country_errors") or []) or None,
                )
            except Exception as e:
                logger.warning(f"Could not check/record windgust give-up state: {e}")
    except Exception as e:
        logger.error(f"  {track_id} {date_str}: FAILED: {e}", exc_info=True)
        wind_results.append((track_id, forecast_time, {"success": False, "error": str(e)}))
        if log_conn:
            try:
                log_run_complete(log_conn, track_id, forecast_time, success=False,
                                  error_message=str(e), started_at=started_at, run_id=run_id)
            except Exception as log_e:
                logger.warning(f"Could not log run failure to TC_PIPELINE_RUN_LOG: {log_e}")
            try:
                check_and_record_windgust_giveup(
                    log_conn, track_id, forecast_time, list(countries_subset), logger,
                    error_message=str(e),
                )
            except Exception as give_up_e:
                logger.warning(f"Could not check/record windgust give-up state: {give_up_e}")

if log_conn:
    try:
        log_conn.close()
    except Exception:
        pass

if not batch:
    print("No unprocessed storm cycles, nothing to do here this run.")

save_known_failed_facilities_cache(known_failed_facilities, logger)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Signal completion (storm branch only)

# COMMAND ----------

from main_pipeline import signal_pipeline_complete, trigger_mat_refresh_if_due

# Only open a connection when there's actually something to signal: on a poll that found no
# unprocessed cycles at all (batch empty) or none that completed, opening a connection just to
# immediately no-op and close it again is a real, avoidable AOTS_WH round trip on every poll.
if completed_storm_ids:
    conn = get_snowflake_connection()
    try:
        signal_pipeline_complete(
            conn=conn,
            storm_ids=completed_storm_ids,
            countries=list(completed_countries),
            files_written=storm_files_written,
            refresh_families=["wind", "gust"],
        )
        logger.info(f"Signalled pipeline completion for storms: {completed_storm_ids} "
                    f"({storm_files_written} files written)")
        # Direct MAT refresh trigger, replacing sole reliance on
        # TRIGGER_REFRESH_TASK's own poll; see trigger_mat_refresh_if_due()'s own
        # docstring for the full reasoning, including the debounce. Called once per
        # run, after the real completion signal above, using the same connection,
        # never inside the per-storm loop above, which would fire one refresh per storm.
        trigger_mat_refresh_if_due(conn, source="windgust", logger=logger, trailing=True)
    except Exception as e:
        logger.warning(f"Could not write storm completion signal to Snowflake: {e}")
    finally:
        conn.close()
else:
    logger.info("No completed storm cycles this run, no storm completion signal sent.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print(f"Wind/gust cycles processed: {len(wind_results)}")
for track_id, forecast_time, result in wind_results:
    print(f"  {track_id} {forecast_time}: {result}")
print(f"Storm completion signalled for: {completed_storm_ids or 'none'}")
