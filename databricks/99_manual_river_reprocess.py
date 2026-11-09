# Databricks notebook source
# MAGIC %md
# MAGIC # Manual reprocess: river flood cycles listed as (country, date) jobs
# MAGIC
# MAGIC Reprocesses river flood cycles whose earlier run did not produce all tier files (for example
# MAGIC after an out-of-memory kill). The notebook calls run_river_flood_analysis()
# MAGIC with an explicit target_date per job, which bypasses the AMBIENT_HAZARD_RUN_LOG/AMBIENT_HAZARD_FAILURE_LOG
# MAGIC give up gate; run_river_flood_analysis() writes no run-log or failure-log rows here, and step 4
# MAGIC appends a completion row and requests a MAT refresh. A tier whose four window files already exist is skipped.
# MAGIC
# MAGIC Parameter `jobs`: JSON list of [country, "YYYY-MM-DD"] pairs. Pairs with the same date are processed together in one call, dates in order of first appearance.
# MAGIC Run manually, attached to an existing cluster. Not on any schedule.

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

os.environ["SNOWFLAKE_ACCOUNT"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_account")
os.environ["SNOWFLAKE_USER"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_user")
os.environ["SNOWFLAKE_PASSWORD"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_password")
os.environ["SNOWFLAKE_WAREHOUSE"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_warehouse")
os.environ["SNOWFLAKE_DATABASE"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_database")
os.environ["SNOWFLAKE_SCHEMA"] = dbutils.secrets.get("glofas-databricks-test", "snowflake_schema")

# Same river setting as the scheduled river job.
os.environ["RIVER_FORK_SAFE_STREAMS"] = "off"

if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)

print("Environment configured. Repo path on sys.path:", REPO_PATH in sys.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reprocess the listed (country, cycle) jobs in order

# COMMAND ----------

import io
import json
import logging
import time
import traceback

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("manual_river_reprocess")

dbutils.widgets.text("jobs", "[]")
JOBS = [(str(cc), str(day)) for cc, day in json.loads(dbutils.widgets.get("jobs"))]


class _RiverLineFilter(logging.Filter):
    """Keeps warnings and errors, and the RIVER_* telemetry lines at any level."""

    def filter(self, record):
        return record.levelno >= logging.WARNING or "RIVER_" in record.getMessage()


# Captures this process's own log records into a string so the real text survives into
# dbutils.notebook.exit() below and is retrievable through the Jobs API (this cluster has no log delivery).
_log_buffer = io.StringIO()
_buffer_handler = logging.StreamHandler(_log_buffer)
_buffer_handler.setLevel(logging.INFO)
_buffer_handler.addFilter(_RiverLineFilter())
_buffer_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(_buffer_handler)

from main_pipeline import run_river_flood_analysis

results = {}
_run_exception = None
files_written = 0
countries_touched = set()
_by_cycle = {}
for _cc, _cycle in JOBS:
    _by_cycle.setdefault(_cycle, []).append(_cc)
for _cycle, _ccs in _by_cycle.items():
    _key = f"{','.join(_ccs) if len(_ccs) <= 3 else str(len(_ccs)) + ' countries'} {_cycle}"
    _t0 = time.time()
    try:
        _n = run_river_flood_analysis(countries=_ccs, logger=logger, zoom=14, target_date=_cycle) or 0
        results[_key] = {"files_written": _n, "seconds": round(time.time() - _t0, 1)}
        files_written += _n
        if _n:
            countries_touched.update(_ccs)
    except Exception:
        _run_exception = traceback.format_exc()
        results[_key] = {"files_written": 0, "seconds": round(time.time() - _t0, 1), "exception": _run_exception[-1500:]}

print("results:", json.dumps(results))
print("CAPTURED:\n", _log_buffer.getvalue()[-40000:])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Only if step 3 wrote real files: signal completion so the dashboard's MAT tables refresh

# COMMAND ----------

from snowflake_utils import get_snowflake_connection
from main_pipeline import signal_pipeline_complete, trigger_mat_refresh_if_due

signalled = False
if files_written > 0:
    conn = get_snowflake_connection()
    try:
        signal_pipeline_complete(conn=conn, storm_ids=[], countries=sorted(countries_touched), files_written=files_written, refresh_families=["river"])
        trigger_mat_refresh_if_due(conn, source="manual_river_reprocess", logger=logger)
        signalled = True
        print(f"Signalled completion and requested a MAT refresh for {files_written} new river files.")
    finally:
        conn.close()
else:
    print("files_written was 0, nothing to signal. See exception/warning text above for the real failure reason "
          "before retrying.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "files_written": files_written,
    "signalled": signalled,
    "results": results,
    "captured": _log_buffer.getvalue()[-40000:],
}))
