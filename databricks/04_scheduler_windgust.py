# Databricks notebook source
# MAGIC %md
# MAGIC # DATAPIPELINE on Databricks: Wind/Gust Scheduler
# MAGIC
# MAGIC One of 3 independently-scheduled Databricks Jobs (one per hazard) sharing the same cluster;
# MAGIC each job's own `max_concurrent_runs=1` only prevents that job from overlapping itself, so a
# MAGIC long river run never delays wind/gust's own trigger.
# MAGIC
# MAGIC This one handles wind/gust only: discovery query, backlog-alert/fairness-floor batching, and
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
# MAGIC Since the 3 hazard jobs run as independent processes with no shared in-memory state,
# MAGIC known_failed_facilities is seeded from, and saved back to, the cross-run Blob/stage cache
# MAGIC (`load_known_failed_facilities_cache`/`save_known_failed_facilities_cache` in
# MAGIC `main_pipeline.py`) instead of starting empty every run (see that cache's own module-level
# MAGIC docstring for why it's a short-TTL hint layered on top of the real, permanent
# MAGIC hc_exist()/school_exist()/shelter_exist()/wash_exist() caches, not a replacement for them).
# MAGIC
# MAGIC Self-contained: installs its own dependencies and applies the required giga-spatial patches
# MAGIC directly, so it does not depend on any other notebook having run first on the same cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install dependencies + apply required giga-spatial patches

# COMMAND ----------

# MAGIC %pip install pandas>=2.0.0 numpy>=1.24.0 geopandas>=0.13.0 shapely>=2.0.0 pyproj>=3.4.0 \
# MAGIC     "zarr>=3.0.0" "rasterio>=1.3.0" "duckdb>=1.2.0" "snowflake-connector-python[pandas]>=3.0.0" \
# MAGIC     "python-dotenv>=1.0.0" "giga-spatial[all]>=0.9.4" "psutil>=5.9.0" "pycountry>=22.3.5" \
# MAGIC     "quantulum3[classifier]>=0.1.0"

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
from datetime import datetime, timedelta

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

PIPELINE_OPS_RECIPIENTS = [
    r.strip() for r in dbutils.secrets.get("glofas-databricks-test", "pipeline_ops_recipients").split(",")
    if r.strip()
]

conn = get_snowflake_connection()
cur = conn.cursor()

cur.execute("""
    SELECT te.TRACK_ID, te.FORECAST_TIME,
           ARRAY_AGG(DISTINCT pc.COUNTRY_CODE) AS CANDIDATE_COUNTRIES
    FROM   TC_ENVELOPES_COMBINED te
    JOIN   PIPELINE_COUNTRIES pc
           ON ST_DWITHIN(pc.COUNTRY_BOUNDARY, te.ENVELOPE_REGION, 1500000)
    LEFT JOIN (SELECT DISTINCT STORM, FORECAST_DATE, COUNTRY FROM MERCATOR_TILE_IMPACT_MAT) wd
           ON wd.STORM = te.TRACK_ID AND wd.FORECAST_DATE = TO_CHAR(te.FORECAST_TIME, 'YYYYMMDDHH24MISS')
           AND wd.COUNTRY = pc.COUNTRY_CODE
    LEFT JOIN (SELECT DISTINCT STORM, FORECAST_DATE, COUNTRY FROM MERCATOR_TILE_GUST_MAT) gd
           ON gd.STORM = te.TRACK_ID AND gd.FORECAST_DATE = TO_CHAR(te.FORECAST_TIME, 'YYYYMMDDHH24MISS')
           AND gd.COUNTRY = pc.COUNTRY_CODE
    WHERE  pc.ACTIVE = TRUE
      AND  te.FORECAST_TIME >= DATEADD('day', -2, CURRENT_TIMESTAMP())
      AND  NOT (wd.STORM IS NOT NULL AND gd.STORM IS NOT NULL)
      AND  NOT EXISTS (
               SELECT 1
               FROM   TC_PIPELINE_RUN_LOG rl
               WHERE  rl.STORM_ID      = te.TRACK_ID
                 AND  rl.FORECAST_TIME = te.FORECAST_TIME
                 AND  rl.STATUS = 'SUCCESS'
                 AND  ARRAY_CONTAINS(pc.COUNTRY_CODE::VARIANT, rl.NO_IMPACT_COUNTRIES)
           )
      AND  NOT EXISTS (
               SELECT 1
               FROM   TC_PIPELINE_RUN_LOG rl
               WHERE  rl.STORM_ID      = te.TRACK_ID
                 AND  rl.FORECAST_TIME = te.FORECAST_TIME
                 AND  rl.STATUS IN ('IN_PROGRESS', 'TRIGGERED')
                 AND  rl.STARTED_AT > DATEADD('hour', -6, CURRENT_TIMESTAMP())
           )
    GROUP BY te.TRACK_ID, te.FORECAST_TIME
    HAVING ARRAY_SIZE(ARRAY_AGG(DISTINCT pc.COUNTRY_CODE)) > 0
    ORDER BY te.FORECAST_TIME ASC
""")
wind_work_raw = cur.fetchall()
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
    (track_id, forecast_time, _parse_candidate_countries(candidates))
    for track_id, forecast_time, candidates in wind_work_raw
]

print(f"Wind/gust: {len(wind_work)} unprocessed storm cycle(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Process each unprocessed storm cycle

# COMMAND ----------

from country_utils import get_active_countries_from_snowflake
from main_pipeline import (
    run_complete_impact_analysis, log_run_start, log_run_complete,
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

try:
    log_conn = get_snowflake_connection()
except Exception as e:
    logger.warning(f"Could not open Snowflake connection for run logging: {e}")
    log_conn = None

RUN_START = datetime.now()

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
    started_at = datetime.now()
    if log_conn:
        try:
            log_run_start(log_conn, track_id, forecast_time, countries_subset)
        except Exception as e:
            logger.warning(f"Could not log run start to TC_PIPELINE_RUN_LOG: {e}")
    try:
        result = run_complete_impact_analysis(track_id, date_str, countries_subset, logger, zoom=14, known_failed_facilities=known_failed_facilities)
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
        # the result is trustworthy -- a fully-successful Python-fallback determination
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

        if log_conn:
            try:
                log_run_complete(
                    log_conn, track_id, forecast_time,
                    success=result.get("success", False),
                    countries=result.get("affected_countries") or [],
                    no_impact_countries=list(no_impact),
                    files_written=result.get("total_views_created", 0) or 0,
                    error_message=result.get("error") or "; ".join(result.get("country_errors") or []) or None,
                    started_at=started_at,
                )
            except Exception as e:
                logger.warning(f"Could not log run completion to TC_PIPELINE_RUN_LOG: {e}")
        if result.get("success") and result.get("countries_processed", 0) > 0:
            if track_id not in completed_storm_ids:
                completed_storm_ids.append(track_id)
            completed_countries.update(result.get("affected_countries") or [])
            storm_files_written += result.get("total_views_created", 0) or 0
    except Exception as e:
        logger.error(f"  {track_id} {date_str}: FAILED: {e}", exc_info=True)
        wind_results.append((track_id, forecast_time, {"success": False, "error": str(e)}))
        if log_conn:
            try:
                log_run_complete(log_conn, track_id, forecast_time, success=False,
                                  error_message=str(e), started_at=started_at)
            except Exception as log_e:
                logger.warning(f"Could not log run failure to TC_PIPELINE_RUN_LOG: {log_e}")

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

from main_pipeline import signal_pipeline_complete

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
        )
        logger.info(f"Signalled pipeline completion for storms: {completed_storm_ids} "
                    f"({storm_files_written} files written)")
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
