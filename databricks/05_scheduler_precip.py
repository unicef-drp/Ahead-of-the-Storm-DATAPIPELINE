# Databricks notebook source
# MAGIC %md
# MAGIC # DATAPIPELINE on Databricks: Precip Scheduler
# MAGIC
# MAGIC One of 4 independently-scheduled Databricks Jobs (wind, precip, river, gust), sharing the same cluster
# MAGIC (see `04_scheduler_windgust.py`'s header for the current design).
# MAGIC
# MAGIC This one handles precip only: calls `run_precip_analysis` in "latest" mode, on the
# MAGIC same schedule as wind/gust, since precip's driving data (MET_FORECASTS tp/ro) comes from the
# MAGIC same ECMWF cycle.
# MAGIC
# MAGIC `run_precip_analysis()` does its own real per-(source,param,forecast_time) ambient dedup
# MAGIC check internally (AMBIENT_HAZARD_RUN_LOG): ambient data whose upstream forecast_time hasn't
# MAGIC advanced since the last successful run correctly no-ops here, cheaply, every run this cycle
# MAGIC hasn't changed. The backlog query below is diagnostic only, not a hard gate.
# MAGIC
# MAGIC Calls `signal_pipeline_complete()` for the ambient branch (mirrors `main()`'s own real call
# MAGIC site: `storm_ids=[]`, `countries=active_countries`, `files_written=precip_files_written`),
# MAGIC gated on `precip_files_written > 0`. `SEND_ALERT()`/`SEND_WARNING()` never key off precip
# MAGIC data, so this never triggers an alert/warning email. `signal_pipeline_complete()` is a plain
# MAGIC append-only INSERT into TC_PIPELINE_COMPLETE_LOG (no uniqueness constraint), so precip and
# MAGIC river can each call it independently and safely: each call simply appends a row and
# MAGIC triggers its own MAT-refresh check.
# MAGIC
# MAGIC known_failed_facilities: see `04_scheduler_windgust.py`'s own comment for the full "why" of
# MAGIC the cross-run Blob/stage cache.
# MAGIC
# MAGIC Self-contained: installs its own dependencies and applies the required giga-spatial patches
# MAGIC directly, so it does not depend on any other notebook having run first on the same cluster.

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

# Fork safety for the per-country child processes: the parent holds the logging locks across each
# fork and drains large results. The child-side stream repair stays off (set to "probe" to arm it).
os.environ["RIVER_FORK_SAFE_STREAMS"] = "off"

if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)

print("Environment configured. Repo path on sys.path:", REPO_PATH in sys.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2a. Blob only precheck: skip Snowflake entirely on a confirmed no new data poll

# COMMAND ----------

# precip's upstream data (the met/<YYYYMMDD>_<HH>/ archives) changes about 4x/day while this
# job polls 16x/day, and a poll with nothing new still resumes the warehouse several times
# below. Listing the Blob prefix needs no Snowflake connection. blob_precheck.py's module docstring
# gives the full skip conditions and the settle window that guards against the archive landing a little
# before its Snowflake pointer row. Fails open: any problem here runs the normal path below.
_precip_precheck = None
_precip_skip = False
_precip_newest_iso = None
try:
    from pipeline_shared import data_store as _pc_store, ROOT_DATA_DIR as _PC_ROOT, VIEWS_DIR as _PC_VIEWS
    from blob_precheck import BlobPrecheck
    import logging as _pc_logging

    _precip_precheck = BlobPrecheck(
        name="precip",
        prefix="met/",
        name_regex=r"^met/\d{8}_\d{2}/",
        state_path=os.path.join(_PC_ROOT, _PC_VIEWS, "discovery_state", "precip_blob_precheck.json"),
        data_store=_pc_store,
        logger=_pc_logging.getLogger("databricks_scheduler_precip"),
    )
    _precip_skip, _precip_newest_iso, _precip_reason = _precip_precheck.should_skip()
    print(f"Precip Blob precheck: skip={_precip_skip} ({_precip_reason})")
except Exception as e:
    _precip_precheck = None
    print(f"Precip Blob precheck unavailable, running the normal path: {e}")

if _precip_skip:
    _precip_precheck.save_after_skip(_precip_newest_iso)
    dbutils.notebook.exit(f"skipped: {_precip_reason}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Discover backlog depth (diagnostic only; run_precip_analysis does its own real gate)

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("databricks_scheduler_precip")

from snowflake_utils import get_snowflake_connection

LOOKBACK_DAYS = 2

conn = get_snowflake_connection()
cur = conn.cursor()
cur.execute(f"""
    SELECT DISTINCT mf.FORECAST_TIME
    FROM   MET_FORECASTS mf
    WHERE  mf.PARAM = 'tp'
      -- SYSDATE(), not CURRENT_TIMESTAMP(): keeps this lookback window anchored to real
      -- UTC "now" regardless of the session's timezone parameter, same class of issue
      -- as the wind/gust retry-lock fix (pipeline_runlog.py), lower-stakes here (a
      -- shifted window, not a lockout).
      AND  mf.FORECAST_TIME >= DATEADD('day', -{LOOKBACK_DAYS}, SYSDATE())
      AND  NOT EXISTS (
               SELECT 1 FROM AMBIENT_HAZARD_RUN_LOG ahr
               WHERE  ahr.SOURCE = 'precip' AND ahr.PARAM = 'tp' AND ahr.FORECAST_TIME = mf.FORECAST_TIME
           )
    ORDER BY mf.FORECAST_TIME ASC
""")
precip_backlog = [r[0] for r in cur.fetchall()]
cur.close()
# conn stays open here, reused immediately below for get_active_countries_from_snowflake(),
# which runs moments later with only cheap, no-I/O Python in between. On the overwhelmingly
# common no-op poll (nothing new to process), this is the only real work this scheduler does,
# so avoiding a second full connect/auth round trip here is a real, if modest, reduction in
# AOTS_WH round trips on every single poll, not just the rare ones with real backlog.

print(f"Precip backlog depth (diagnostic only): {len(precip_backlog)}")
if len(precip_backlog) > 1:
    logger.warning(
        f"Precip backlog is {len(precip_backlog)} cycles deep, only the latest cycle is "
        f"processed (oldest unprocessed: {precip_backlog[0]}). If this keeps growing across "
        f"polls, the scheduling cadence or a real failure needs investigating, not just faster "
        f"catch-up."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Precip: catch up to latest

# COMMAND ----------

from country_utils import get_active_countries_from_snowflake
from main_pipeline import (
    run_precip_analysis, _release_memory,
    load_known_failed_facilities_cache, save_known_failed_facilities_cache,
)

active_countries = get_active_countries_from_snowflake(conn=conn)
conn.close()
print(f"Active countries: {len(active_countries)}")

known_failed_facilities = load_known_failed_facilities_cache(logger)

_release_memory(logger)

precip_files_written = 0
precip_run_failed = False
try:
    precip_files_written = run_precip_analysis(active_countries, logger, zoom=14, target_date=None, known_failed_facilities=known_failed_facilities) or 0
except Exception as e:
    precip_run_failed = True
    logger.error(f"Precip analysis failed: {e}", exc_info=True)

print(f"precip_files_written: {precip_files_written}")

save_known_failed_facilities_cache(known_failed_facilities, logger)

# Record this real check for the Blob precheck above. Backlog counts as confirmed clear only
# when none was outstanding going in and the run itself did not fail; otherwise the next poll
# checks for real again.
if _precip_precheck is not None:
    _precip_precheck.save_after_real_check(
        _precip_newest_iso or _precip_precheck.current_newest_iso(),
        backlog_confirmed_clear=(len(precip_backlog) == 0 and not precip_run_failed),
        processed_something=precip_files_written > 0,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Signal completion (ambient branch)

# COMMAND ----------

from main_pipeline import signal_pipeline_complete, trigger_mat_refresh_if_due

# Only open a connection when there's actually something to signal: on the overwhelmingly
# common no-op poll (precip_files_written == 0), opening a connection just to immediately
# no-op and close it again is a real, avoidable AOTS_WH round trip on every single poll.
if precip_files_written > 0:
    conn = get_snowflake_connection()
    try:
        signal_pipeline_complete(
            conn=conn,
            storm_ids=[],
            countries=active_countries,
            files_written=precip_files_written,
            refresh_families=["precip"],
        )
        logger.info(f"Signalled pipeline completion for ambient precip data ({precip_files_written} files written)")
        # Direct MAT refresh trigger, replacing sole reliance on
        # TRIGGER_REFRESH_TASK's own poll; see trigger_mat_refresh_if_due()'s own
        # docstring, notably the debounce reasoning: precip shares wind/gust's
        # exact 16x/day cron, so their real completions often land minutes apart
        # for the same ECMWF cycle.
        trigger_mat_refresh_if_due(conn, source="precip", logger=logger, trailing=True)
    except Exception as e:
        logger.warning(f"Could not write ambient completion signal to Snowflake: {e}")
    finally:
        conn.close()
else:
    logger.info("No precip files written this run, no ambient completion signal sent.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print(f"Precip files written: {precip_files_written}")
print(f"Ambient completion signalled: {'yes' if precip_files_written > 0 else 'no'}")
