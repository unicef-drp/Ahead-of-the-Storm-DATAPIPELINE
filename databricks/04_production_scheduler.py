# Databricks notebook source
# MAGIC %md
# MAGIC # DATAPIPELINE on Databricks: Production Scheduler
# MAGIC
# MAGIC The real, scheduled entry point for DATAPIPELINE's compute on Databricks. A Databricks Job
# MAGIC runs this notebook on a fixed schedule. Each run discovers unprocessed work, computes it, and
# MAGIC signals completion to Snowflake so downstream MAT table refresh and alerting can pick it up.
# MAGIC
# MAGIC This notebook calls `signal_pipeline_complete()` for both branches it handles:
# MAGIC - **Ambient (precip/river)**: mirrors `main_pipeline.py`'s own real call site in `main()`
# MAGIC   exactly (`storm_ids=[]`, `countries=active_countries`,
# MAGIC   `files_written=ambient_files_written`). `SEND_ALERT()`/`SEND_WARNING()` never key off
# MAGIC   precip/river data, so this signal never triggers an alert or warning email.
# MAGIC - **Storm (wind/gust)**: mirrors `update_storms()`'s own real call site
# MAGIC   (`storm_ids=completed_storm_ids`, `countries=completed_countries`,
# MAGIC   `files_written=total_files_written`), gated the same way production does
# MAGIC   (`loop_stats.countries_processed > 0` equivalent below).
# MAGIC
# MAGIC **A genuinely new, alert-worthy storm processed by a run of this notebook sends a real
# MAGIC alert/warning email.** This is the intended behavior: Databricks is a complete replacement
# MAGIC for the SPCS trigger path, not a signal-less shadow of it. Confirm `ALERT_SUBSCRIBERS` is
# MAGIC scoped as intended before relying on this Job to run unattended in production.
# MAGIC
# MAGIC **Cadence note**: one poll interval serves all four hazards (wind/gust, precip, river) even
# MAGIC though their real update cadences differ (river/GloFAS updates once daily, unlike
# MAGIC storm-driven wind/gust or more frequent precip). Each hazard's own discovery query
# MAGIC independently decides whether there is real new work, so river finding nothing new on most
# MAGIC polls is expected behavior, not a scheduling problem.
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
        patch_results["list_quoting"] = "PATTERN NOT FOUND -- giga-spatial version drift, investigate before continuing"

    old5 = 'get_command = f"GET {stage_path} \'file://{temp_dir_normalized}\'"'
    new5 = "get_command = f\"GET '{stage_path}' 'file://{temp_dir_normalized}'\""
    if "get_command = f\"GET '{stage_path}' 'file://{temp_dir_normalized}'\"" in text:
        patch_results["get_quoting"] = "already patched upstream"
    elif old5 in text:
        text = text.replace(old5, new5)
        patch_results["get_quoting"] = "PATCHED"
    else:
        patch_results["get_quoting"] = "PATTERN NOT FOUND -- giga-spatial version drift, investigate before continuing"

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

# Derived at runtime from this notebook's own workspace path, rather than a hardcoded
# per-user path, so the same code works under any workspace user's own checkout.
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_PATH = "/Workspace" + "/".join(_notebook_path.split("/")[:-2])

os.environ["DATA_PIPELINE_DB"] = "SNOWFLAKE"
os.environ["SNOWFLAKE_ACCOUNT"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_account")
os.environ["SNOWFLAKE_USER"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_user")
os.environ["SNOWFLAKE_PASSWORD"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_password")
os.environ["SNOWFLAKE_WAREHOUSE"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_warehouse")
os.environ["SNOWFLAKE_DATABASE"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_database")
os.environ["SNOWFLAKE_SCHEMA"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_schema")
os.environ["SNOWFLAKE_STAGE_NAME"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_stage_name")

if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)

print("Environment configured. Repo path on sys.path:", REPO_PATH in sys.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Discover unprocessed work

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("databricks_scheduler")

from snowflake_utils import get_snowflake_connection

LOOKBACK_DAYS = 2
RIVER_RP_TIERS = ['rp2', 'rp5', 'rp10', 'rp20', 'rp50', 'rp100']

conn = get_snowflake_connection()
cur = conn.cursor()

cur.execute("""
    SELECT DISTINCT te.TRACK_ID, te.FORECAST_TIME
    FROM   TC_ENVELOPES_COMBINED te
    JOIN   PIPELINE_COUNTRIES pc
           ON ST_DWITHIN(pc.COUNTRY_BOUNDARY, te.ENVELOPE_REGION, 1500000)
    LEFT JOIN (SELECT DISTINCT STORM, FORECAST_DATE FROM MERCATOR_TILE_IMPACT_MAT) wd
           ON wd.STORM = te.TRACK_ID AND wd.FORECAST_DATE = TO_CHAR(te.FORECAST_TIME, 'YYYYMMDDHH24MISS')
    LEFT JOIN (SELECT DISTINCT STORM, FORECAST_DATE FROM MERCATOR_TILE_GUST_MAT) gd
           ON gd.STORM = te.TRACK_ID AND gd.FORECAST_DATE = TO_CHAR(te.FORECAST_TIME, 'YYYYMMDDHH24MISS')
    WHERE  pc.ACTIVE = TRUE
      AND  te.FORECAST_TIME >= DATEADD('day', -2, CURRENT_TIMESTAMP())
      AND  NOT EXISTS (
               -- A logged SUCCESS is only trusted once real output for this exact cycle is
               -- independently confirmed present in both MAT tables (via the wd/gd LEFT JOINs
               -- above) -- mirrors the dashboard's own _wind_gust_ready_at real-output check
               -- (components/data/snowflake_utils.py in the main repo), since TC_PIPELINE_RUN_LOG's
               -- own SUCCESS flag can be logged for a run that claimed completion (countries
               -- processed, files written) but left no real rows in either table -- this discovery
               -- query would otherwise treat that cycle as permanently done and never pick it back
               -- up. Checked via a LEFT JOIN + IS NOT NULL rather than a nested EXISTS inside this
               -- subquery: Snowflake rejects a correlated EXISTS nested two levels deep ("Unsupported
               -- subquery type cannot be evaluated"), so the MAT-presence check has to happen at the
               -- outer query's own correlation level instead.
               SELECT 1
               FROM   TC_PIPELINE_RUN_LOG rl
               WHERE  rl.STORM_ID      = te.TRACK_ID
                 AND  rl.FORECAST_TIME = te.FORECAST_TIME
                 AND  rl.STATUS = 'SUCCESS'
                 AND  wd.STORM IS NOT NULL
                 AND  gd.STORM IS NOT NULL
           )
      AND  NOT EXISTS (
               SELECT 1
               FROM   TC_PIPELINE_RUN_LOG rl
               WHERE  rl.STORM_ID      = te.TRACK_ID
                 AND  rl.FORECAST_TIME = te.FORECAST_TIME
                 AND  rl.STATUS IN ('IN_PROGRESS', 'TRIGGERED')
                 AND  rl.STARTED_AT > DATEADD('hour', -6, CURRENT_TIMESTAMP())
           )
    ORDER BY te.FORECAST_TIME DESC
""")
wind_work = cur.fetchall()

cur.execute(f"""
    SELECT DISTINCT mf.FORECAST_TIME
    FROM   MET_FORECASTS mf
    WHERE  mf.PARAM = 'tp'
      AND  mf.FORECAST_TIME >= DATEADD('day', -{LOOKBACK_DAYS}, CURRENT_TIMESTAMP())
      AND  NOT EXISTS (
               SELECT 1 FROM AMBIENT_HAZARD_RUN_LOG ahr
               WHERE  ahr.SOURCE = 'precip' AND ahr.PARAM = 'tp' AND ahr.FORECAST_TIME = mf.FORECAST_TIME
           )
    ORDER BY mf.FORECAST_TIME ASC
""")
precip_backlog = [r[0] for r in cur.fetchall()]

river_backlog = {}
for rp in RIVER_RP_TIERS:
    param = f"extent_{rp}_bymember"
    cur.execute(f"""
        SELECT DISTINCT rf.FORECAST_TIME
        FROM   RIVER_FORECASTS rf
        WHERE  rf.PARAM = %s
          AND  rf.FORECAST_TIME >= DATEADD('day', -{LOOKBACK_DAYS}, CURRENT_TIMESTAMP())
          AND  NOT EXISTS (
                   SELECT 1 FROM AMBIENT_HAZARD_RUN_LOG ahr
                   WHERE  ahr.SOURCE = 'river' AND ahr.PARAM = %s AND ahr.FORECAST_TIME = rf.FORECAST_TIME
               )
        ORDER BY rf.FORECAST_TIME ASC
    """, (param, rp))
    river_backlog[rp] = [r[0] for r in cur.fetchall()]
cur.close()
conn.close()

print(f"Wind/gust: {len(wind_work)} unprocessed storm cycle(s)")
print(f"Precip backlog depth (diagnostic only): {len(precip_backlog)}")
for rp, cycles in river_backlog.items():
    if cycles:
        print(f"River {rp} backlog depth (diagnostic only): {len(cycles)}")

if len(precip_backlog) > 1:
    logger.warning(
        f"Precip backlog is {len(precip_backlog)} cycles deep -- only the latest cycle is "
        f"processed (oldest unprocessed: {precip_backlog[0]}). If this keeps growing across "
        f"polls, the scheduling cadence or a real failure needs investigating, not just faster "
        f"catch-up."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wind/gust: process each unprocessed storm cycle

# COMMAND ----------

from country_utils import get_active_countries_from_snowflake
from main_pipeline import run_complete_impact_analysis, log_run_start, log_run_complete
from datetime import datetime

active_countries = get_active_countries_from_snowflake()
print(f"Active countries: {len(active_countries)}")

# Tracked the same way update_storms() itself tracks completion for its own
# signal_pipeline_complete() call (main_pipeline.py ~line 1769-1871), adapted to
# run_complete_impact_analysis()'s own direct-call return shape (a plain dict, not the
# internal loop_stats object update_storms() uses) -- same gate
# (countries_processed > 0), same accumulation semantics.
wind_results = []
completed_storm_ids = []
completed_countries = set()
storm_files_written = 0

# TC_PIPELINE_RUN_LOG bookkeeping, mirroring update_storms()'s own log_run_start/
# log_run_complete call sites (main_pipeline.py ~1808-1888): required so the discovery
# query's own NOT EXISTS check above can actually see this run and skip it on the next
# poll, instead of reprocessing the full lookback window forever. Every call is
# individually try/except-wrapped, matching update_storms()'s own pattern exactly, so a
# transient Snowflake hiccup on the bookkeeping path degrades to "this one cycle stays
# eligible for reprocessing" rather than aborting the rest of wind_work (which would also
# skip precip/river below, and skip real alert/warning emails in section 6).
try:
    log_conn = get_snowflake_connection()
except Exception as e:
    logger.warning(f"Could not open Snowflake connection for run logging: {e}")
    log_conn = None

for track_id, forecast_time in wind_work:
    date_str = forecast_time.strftime("%Y%m%d%H%M%S")
    logger.info(f"Processing {track_id} at {date_str}...")
    started_at = datetime.now()
    if log_conn:
        try:
            log_run_start(log_conn, track_id, forecast_time, active_countries)
        except Exception as e:
            logger.warning(f"Could not log run start to TC_PIPELINE_RUN_LOG: {e}")
    try:
        result = run_complete_impact_analysis(track_id, date_str, active_countries, logger, zoom=14)
        wind_results.append((track_id, forecast_time, result))
        print(f"  {track_id} {date_str}: {result}")
        if log_conn:
            try:
                log_run_complete(
                    log_conn, track_id, forecast_time,
                    success=result.get("success", False),
                    countries=result.get("affected_countries") or [],
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

if not wind_work:
    print("No unprocessed storm cycles -- nothing to do here this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Precip + river: catch up to latest (matches production's own `main()` behavior)

# COMMAND ----------

from main_pipeline import run_precip_analysis, run_river_flood_analysis

precip_files_written = 0
river_files_written = 0
try:
    precip_files_written = run_precip_analysis(active_countries, logger, zoom=14, target_date=None) or 0
except Exception as e:
    logger.error(f"Precip analysis failed: {e}", exc_info=True)

try:
    river_files_written = run_river_flood_analysis(active_countries, logger, zoom=14, target_date=None) or 0
except Exception as e:
    logger.error(f"River-flood analysis failed: {e}", exc_info=True)

ambient_files_written = precip_files_written + river_files_written

print(f"precip_files_written: {precip_files_written}")
print(f"river_files_written: {river_files_written}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Signal completion, both branches, mirroring production's own two real call
# MAGIC sites exactly (`main_pipeline.py`'s `update_storms()` for storm data, `main()` for ambient data)

# COMMAND ----------

from main_pipeline import signal_pipeline_complete

conn = get_snowflake_connection()

if completed_storm_ids:
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
else:
    logger.info("No completed storm cycles this run -- no storm completion signal sent.")

if ambient_files_written > 0:
    try:
        signal_pipeline_complete(
            conn=conn,
            storm_ids=[],
            countries=active_countries,
            files_written=ambient_files_written,
        )
        logger.info(f"Signalled pipeline completion for ambient precip/river-flood data "
                    f"({ambient_files_written} files written)")
    except Exception as e:
        logger.warning(f"Could not write ambient completion signal to Snowflake: {e}")
else:
    logger.info("No ambient (precip/river) files written this run -- no ambient completion signal sent.")

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print(f"Wind/gust cycles processed: {len(wind_results)}")
for track_id, forecast_time, result in wind_results:
    print(f"  {track_id} {forecast_time}: {result}")
print(f"Storm completion signalled for: {completed_storm_ids or 'none'}")
print(f"Precip files written: {precip_files_written}")
print(f"River files written: {river_files_written}")
print(f"Ambient completion signalled: {'yes' if ambient_files_written > 0 else 'no'}")
