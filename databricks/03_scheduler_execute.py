# Databricks notebook source
# MAGIC %md
# MAGIC # DATAPIPELINE on Databricks: Scheduler Execute
# MAGIC
# MAGIC Runs the real compute for whatever unprocessed wind/gust storm cycles and ambient precip/river
# MAGIC backlog exist, using `02_scheduler_logic`'s discovery queries plus the real compute calls,
# MAGIC deliberately WITHOUT `signal_pipeline_complete()`. That makes this notebook a safe way to
# MAGIC reprocess or backfill data without triggering the real `SEND_ALERT()`/`SEND_WARNING()` alert
# MAGIC cascade, since `REFRESH_MATERIALIZED_VIEWS()` only picks up cycles that have been signaled
# MAGIC complete.
# MAGIC
# MAGIC **Backfill scope**: catch up to the LATEST cycle only, not a full historical replay of every
# MAGIC missed cycle. Ambient precip/river hazard maps represent current conditions and are
# MAGIC immediately superseded by the next cycle, so there is no real value in reprocessing stale
# MAGIC intermediate cycles just because a poll was missed. `run_precip_analysis()`'s own
# MAGIC `target_date` mechanism also can't select an individual earlier cycle when a later one shares
# MAGIC the same calendar date, per its own documented "latest wins" behavior. For precip/river this
# MAGIC notebook does exactly what production's own `main()` does on every invocation: call the
# MAGIC analysis functions in plain "latest" mode and let their own idempotency gate decide if there
# MAGIC is real new work. `02_scheduler_logic`'s lookback query is kept here purely as a DIAGNOSTIC
# MAGIC (how deep the backlog is, if any), not as an input to what gets computed.
# MAGIC
# MAGIC Wind/gust is different: each storm cycle is distinct and worth its own processing, matching
# MAGIC `CHECK_AND_TRIGGER_DATAPIPELINE()`'s own logic, so `02_scheduler_logic`'s wind/gust discovery
# MAGIC results directly drive which `(storm, forecast_time)` pairs get processed below.
# MAGIC
# MAGIC Real writes to the real Snowflake stage. No completion signal, no alert cascade risk. The
# MAGIC live production scheduler notebook, `04_production_scheduler.py`, runs this same compute
# MAGIC logic plus a real completion signal on a live schedule.
# MAGIC
# MAGIC Self-contained: installs its own dependencies and patches directly, so it does not depend on
# MAGIC any other notebook having run first on the same cluster.

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
        patch_results["list_quoting"] = "PATTERN NOT FOUND, giga-spatial version drift, investigate before continuing"

    old5 = 'get_command = f"GET {stage_path} \'file://{temp_dir_normalized}\'"'
    new5 = "get_command = f\"GET '{stage_path}' 'file://{temp_dir_normalized}'\""
    if "get_command = f\"GET '{stage_path}' 'file://{temp_dir_normalized}'\"" in text:
        patch_results["get_quoting"] = "already patched upstream"
    elif old5 in text:
        text = text.replace(old5, new5)
        patch_results["get_quoting"] = "PATCHED"
    else:
        patch_results["get_quoting"] = "PATTERN NOT FOUND, giga-spatial version drift, investigate before continuing"

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
# MAGIC ## 3. Discover unprocessed work (same queries as `02_scheduler_logic`)

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
    WHERE  pc.ACTIVE = TRUE
      AND  te.FORECAST_TIME >= DATEADD('day', -2, CURRENT_TIMESTAMP())
      AND  NOT EXISTS (
               SELECT 1
               FROM   TC_PIPELINE_RUN_LOG rl
               WHERE  rl.STORM_ID      = te.TRACK_ID
                 AND  rl.FORECAST_TIME = te.FORECAST_TIME
                 AND  (
                       rl.STATUS = 'SUCCESS'
                       OR (rl.STATUS IN ('IN_PROGRESS', 'TRIGGERED')
                           AND rl.STARTED_AT > DATEADD('hour', -6, CURRENT_TIMESTAMP()))
                      )
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
        f"Precip backlog is {len(precip_backlog)} cycles deep, processing only the latest "
        f"(oldest unprocessed: {precip_backlog[0]}). A backlog that keeps growing across polls "
        f"may indicate a real problem worth investigating, not just a need for faster catch-up."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wind/gust: process each real unprocessed storm cycle
# MAGIC
# MAGIC Calls `run_complete_impact_analysis()` directly: real writes, no completion signal. Uses the
# MAGIC full active-countries list, same as production's own `main()`, letting the function's own
# MAGIC internal `ST_DWITHIN` check determine which countries are actually affected, matching real
# MAGIC production behavior.

# COMMAND ----------

from country_utils import get_active_countries_from_snowflake
from main_pipeline import run_complete_impact_analysis

active_countries = get_active_countries_from_snowflake()
print(f"Active countries: {len(active_countries)}")

wind_results = []
for track_id, forecast_time in wind_work:
    date_str = forecast_time.strftime("%Y%m%d%H%M%S")
    logger.info(f"Processing {track_id} at {date_str}...")
    try:
        result = run_complete_impact_analysis(track_id, date_str, active_countries, logger, zoom=14)
        wind_results.append((track_id, forecast_time, result))
        print(f"  {track_id} {date_str}: {result}")
    except Exception as e:
        logger.error(f"  {track_id} {date_str}: FAILED: {e}", exc_info=True)
        wind_results.append((track_id, forecast_time, {"success": False, "error": str(e)}))

if not wind_work:
    print("No unprocessed storm cycles found, nothing to do this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Precip + river: catch up to latest (matches production's own `main()` behavior)
# MAGIC
# MAGIC Plain "latest" mode (`target_date=None`). The functions' own idempotency gate decides if
# MAGIC there is real new work, the same as every `--type update` invocation.

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

print(f"precip_files_written: {precip_files_written}")
print(f"river_files_written: {river_files_written}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary
# MAGIC
# MAGIC Real writes happened above if any counts are non-zero. Verify against the real Snowflake
# MAGIC stage directly (same `LIST @stage/...` pattern used throughout this series), don't just trust
# MAGIC these printed numbers. No completion signal was sent, so `REFRESH_MATERIALIZED_VIEWS()` will
# MAGIC NOT pick up any of this.

# COMMAND ----------

print(f"Wind/gust cycles processed: {len(wind_results)}")
for track_id, forecast_time, result in wind_results:
    print(f"  {track_id} {forecast_time}: {result}")
print(f"Precip files written: {precip_files_written}")
print(f"River files written: {river_files_written}")
