# Databricks notebook source
# MAGIC %md
# MAGIC # DATAPIPELINE on Databricks: Scheduler Discovery Logic
# MAGIC
# MAGIC Read-only discovery notebook for the Databricks scheduler path. It identifies unprocessed
# MAGIC wind/gust storm cycles and outstanding precip/river ambient backlog depth without writing or
# MAGIC computing anything, useful for inspecting what work is currently outstanding (for example,
# MAGIC on-call backlog inspection) without triggering any real compute or signal.
# MAGIC
# MAGIC The wind/gust query (Section 3) matches `01_trigger_logic_test`'s query. The precip and river
# MAGIC queries (Sections 4 and 5) add a missed-cycle catch-up window that the wind/gust query does
# MAGIC not need: `run_precip_analysis()`/`run_river_flood_analysis()` in "latest" mode only ever fetch
# MAGIC the single latest `MET_FORECASTS`/`RIVER_FORECASTS` row (`get_latest_met_forecast`/
# MAGIC `get_latest_river_forecast`). If a scheduled poll is missed for more than one cycle (cluster
# MAGIC down, transient error), the skipped cycle(s) are gone forever, silently, since there is no
# MAGIC equivalent of wind/gust's own `time_delta` lookback for the ambient path. The precip and river
# MAGIC discovery queries close that gap by looking back `LOOKBACK_DAYS` and finding every real cycle
# MAGIC not yet logged in `AMBIENT_HAZARD_RUN_LOG`, not just the latest one.
# MAGIC
# MAGIC This same discovery query logic (wind/gust, precip, and river) is the logic used inside the
# MAGIC live production scheduler notebook, `04_production_scheduler.py`.
# MAGIC
# MAGIC Entirely read-only: this notebook only reports what work a real scheduled poll would find. It
# MAGIC does not call any compute functions and does not write anything.
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
# MAGIC ## 3. Wind/gust: unprocessed storm cycles
# MAGIC
# MAGIC Matches `01_trigger_logic_test`'s Section 3 query: an unprocessed storm cycle is a distinct
# MAGIC `(TRACK_ID, FORECAST_TIME)` pair in `TC_ENVELOPES_COMBINED` within 1,500 km of an active
# MAGIC country and within the last 2 days, with no matching `TC_PIPELINE_RUN_LOG` row that is either
# MAGIC already `SUCCESS` or currently `IN_PROGRESS`/`TRIGGERED` within the last 6 hours.

# COMMAND ----------

from snowflake_utils import get_snowflake_connection

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
cur.close()
conn.close()

print(f"Unprocessed storm cycles: {len(wind_work)}")
for row in wind_work:
    print(" ", row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Precip: unprocessed cycles within a lookback window
# MAGIC
# MAGIC Unlike production `run_precip_analysis()`'s own "latest" mode, which only ever checks the
# MAGIC single newest `MET_FORECASTS` row, this looks back `LOOKBACK_DAYS` and flags every real `tp`
# MAGIC cycle not yet logged in `AMBIENT_HAZARD_RUN_LOG`. A missed poll cannot silently lose a cycle
# MAGIC this way. This query deliberately checks only `PARAM = 'tp'`, which is a safe superset of real
# MAGIC unprocessed work: the production gate also accounts for the `tp_ro` ratio separately and treats
# MAGIC a missing `ro` cycle as "nothing to ratio, tp alone still counts as done". That fine-grained
# MAGIC logic lives in `run_precip_analysis()` itself and does not need duplicating here, since calling
# MAGIC it again for an already-fully-processed cycle is always safe (idempotency gate).

# COMMAND ----------

LOOKBACK_DAYS = 2  # matches wind/gust's own time_delta default

conn = get_snowflake_connection()
cur = conn.cursor()
cur.execute(f"""
    SELECT DISTINCT mf.FORECAST_TIME
    FROM   MET_FORECASTS mf
    WHERE  mf.PARAM = 'tp'
      AND  mf.FORECAST_TIME >= DATEADD('day', -{LOOKBACK_DAYS}, CURRENT_TIMESTAMP())
      AND  NOT EXISTS (
               SELECT 1 FROM AMBIENT_HAZARD_RUN_LOG ahr
               WHERE  ahr.SOURCE = 'precip'
                 AND  ahr.PARAM  = 'tp'
                 AND  ahr.FORECAST_TIME = mf.FORECAST_TIME
           )
    ORDER BY mf.FORECAST_TIME ASC
""")
precip_work = [r[0] for r in cur.fetchall()]
cur.close()
conn.close()

print(f"Unprocessed precip cycles (last {LOOKBACK_DAYS}d, not just latest): {len(precip_work)}")
for ft in precip_work:
    print(" ", ft)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. River: unprocessed cycles per RP tier within the same lookback window
# MAGIC
# MAGIC Same reasoning as precip. `RIVER_RP_TIERS`, the `extent_{rp}_bymember` PARAM naming
# MAGIC convention, and the `AMBIENT_HAZARD_RUN_LOG` bare-tier-name convention (`'rp2'`, not
# MAGIC `'extent_rp2_bymember'`) all match `main_pipeline.py`'s production code exactly
# MAGIC (`RIVER_RP_TIERS`, `run_river_flood_analysis()`'s own `is_ambient_forecast_processed(
# MAGIC ambient_conn, 'river', rp, forecast_time)` call).

# COMMAND ----------

RIVER_RP_TIERS = ['rp2', 'rp5', 'rp10', 'rp20', 'rp50', 'rp100']

conn = get_snowflake_connection()
cur = conn.cursor()
river_work = {}
for rp in RIVER_RP_TIERS:
    param = f"extent_{rp}_bymember"
    cur.execute(f"""
        SELECT DISTINCT rf.FORECAST_TIME
        FROM   RIVER_FORECASTS rf
        WHERE  rf.PARAM = %s
          AND  rf.FORECAST_TIME >= DATEADD('day', -{LOOKBACK_DAYS}, CURRENT_TIMESTAMP())
          AND  NOT EXISTS (
                   SELECT 1 FROM AMBIENT_HAZARD_RUN_LOG ahr
                   WHERE  ahr.SOURCE = 'river'
                     AND  ahr.PARAM  = %s
                     AND  ahr.FORECAST_TIME = rf.FORECAST_TIME
               )
        ORDER BY rf.FORECAST_TIME ASC
    """, (param, rp))
    river_work[rp] = [r[0] for r in cur.fetchall()]
cur.close()
conn.close()

total_river = sum(len(v) for v in river_work.values())
print(f"Unprocessed river cycles (last {LOOKBACK_DAYS}d, per tier): {total_river} total")
for rp, cycles in river_work.items():
    print(f"  {rp}: {len(cycles)}")
    for ft in cycles:
        print("    ", ft)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary
# MAGIC
# MAGIC What a real scheduled poll would find right now, entirely read-only. If `RIVER_FORECASTS`
# MAGIC ingestion is currently suspended, the river section above will correctly report 0 unprocessed
# MAGIC cycles rather than error, since there are no new rows to find.

# COMMAND ----------

print(f"Wind/gust:  {len(wind_work)} unprocessed storm cycle(s)")
print(f"Precip:     {len(precip_work)} unprocessed cycle(s)")
print(f"River:      {total_river} unprocessed cycle(s)")
print()
print("Nothing was written or computed by this notebook.")
