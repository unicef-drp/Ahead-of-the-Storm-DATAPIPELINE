# Databricks notebook source
# MAGIC %md
# MAGIC # DATAPIPELINE Environment Health Check
# MAGIC
# MAGIC Verifies this cluster can run the real DATAPIPELINE pipeline code: system libraries, Python
# MAGIC dependencies, the giga-spatial patches this repo still needs, and Snowflake connectivity.
# MAGIC
# MAGIC Re-run whenever the cluster/runtime, `requirements.txt`, or the installed giga-spatial version
# MAGIC changes. Each section prints a clear pass/fail. Nothing here writes to Snowflake -- the
# MAGIC connectivity check is a read-only `SELECT CURRENT_VERSION()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## System libraries (GDAL / GEOS / PROJ)

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get update -qq && apt-get install -y --no-install-recommends \
# MAGIC     libgeos-dev libproj-dev libgdal-dev gdal-bin build-essential gcc g++ \
# MAGIC     > /tmp/apt_install.log 2>&1
# MAGIC echo "exit code: $?"
# MAGIC gdal-config --version || echo "gdal-config NOT FOUND"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python packages (from `requirements.txt`)

# COMMAND ----------

# MAGIC %pip install pandas>=2.0.0 numpy>=1.24.0 geopandas>=0.13.0 shapely>=2.0.0 pyproj>=3.4.0 \
# MAGIC     "zarr>=3.0.0" "rasterio>=1.3.0" "duckdb>=1.2.0" "snowflake-connector-python[pandas]>=3.0.0" \
# MAGIC     "python-dotenv>=1.0.0" "giga-spatial[all]>=0.9.4" "psutil>=5.9.0" "pycountry>=22.3.5" \
# MAGIC     "quantulum3[classifier]>=0.1.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import importlib

results = {}
for mod in [
    "pandas", "numpy", "geopandas", "shapely", "pyproj", "zarr", "rasterio",
    "duckdb", "snowflake.connector", "dotenv", "gigaspatial", "psutil",
    "pycountry", "quantulum3",
]:
    try:
        importlib.import_module(mod)
        results[mod] = "OK"
    except Exception as e:
        results[mod] = f"FAILED: {e}"

for mod, status in results.items():
    print(f"{mod:30s} {status}")

failed = [m for m, s in results.items() if s != "OK"]
print()
print("ALL IMPORTS OK" if not failed else f"FAILED: {failed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## giga-spatial patches
# MAGIC
# MAGIC `snowflake/Dockerfile` carries 5 patches against giga-spatial 0.9.3/0.9.4. Patches 1
# MAGIC (mercator_tiles classmethod) and 3 (OSM User-Agent) are fixed upstream in giga-spatial 0.9.8 and
# MAGIC are not applied here. Only patches 2, 4, and 5 are checked and applied below, path-agnostic
# MAGIC (uses `importlib` to find the real installed location on this cluster).

# COMMAND ----------

import importlib.util
import pathlib


def _patched_path(module_name):
    spec = importlib.util.find_spec(module_name)
    if spec is None or spec.origin is None:
        raise RuntimeError(f"Could not locate {module_name}")
    return pathlib.Path(spec.origin)


patch_results = {}

# Patch 2: HealthSitesFetcher._convert_country bypass Overpass API
try:
    p = _patched_path("gigaspatial.handlers.healthsites")
    text = p.read_text()
    old = (
        '    def _convert_country(self, country: str) -> str:\n'
        '        """Resolve any country identifier to its OSM English name."""\n'
        '        try:\n'
        '            iso3 = pycountry.countries.lookup(country).alpha_3\n'
        '        except LookupError:\n'
        '            raise ValueError(f"Invalid country code: {country}")\n'
        '\n'
        '        return self._fetch_osm_country_name(iso3, country)'
    )
    new = (
        '    def _convert_country(self, country: str) -> str:\n'
        '        _OVERRIDES = {"TWN": "Taiwan", "VGB": "British Virgin Islands", "VNM": "Vietnam"}\n'
        '        try:\n'
        '            c = pycountry.countries.lookup(country)\n'
        '        except LookupError:\n'
        '            raise ValueError(f"Invalid country code: {country}")\n'
        '        return _OVERRIDES.get(c.alpha_3, c.name)'
    )
    if "_OVERRIDES = " in text:
        patch_results["healthsites_overpass_bypass"] = "already patched upstream"
    elif old in text:
        p.write_text(text.replace(old, new))
        patch_results["healthsites_overpass_bypass"] = "PATCHED"
    else:
        patch_results["healthsites_overpass_bypass"] = "PATTERN NOT FOUND -- giga-spatial version drift, investigate before relying on this cluster"
except Exception as e:
    patch_results["healthsites_overpass_bypass"] = f"ERROR: {e}"

# Patch 4 + 5: SnowflakeDataStore stage-path quoting (LIST and GET commands)
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
        patch_results["list_quoting"] = "PATTERN NOT FOUND -- giga-spatial version drift, investigate before relying on this cluster"

    old5 = 'get_command = f"GET {stage_path} \'file://{temp_dir_normalized}\'"'
    new5 = "get_command = f\"GET '{stage_path}' 'file://{temp_dir_normalized}'\""
    if "get_command = f\"GET '{stage_path}' 'file://{temp_dir_normalized}'\"" in text:
        patch_results["get_quoting"] = "already patched upstream"
    elif old5 in text:
        text = text.replace(old5, new5)
        patch_results["get_quoting"] = "PATCHED"
    else:
        patch_results["get_quoting"] = "PATTERN NOT FOUND -- giga-spatial version drift, investigate before relying on this cluster"

    p.write_text(text)
except Exception as e:
    patch_results["list_quoting"] = patch_results.get("list_quoting", f"ERROR: {e}")
    patch_results["get_quoting"] = patch_results.get("get_quoting", f"ERROR: {e}")

for name, status in patch_results.items():
    print(f"{name:25s} {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowflake connectivity (real, read-only)

# COMMAND ----------

import snowflake.connector

conn = snowflake.connector.connect(
    account=dbutils.secrets.get("glofas-databricks-test", "snowflake_account"),
    user=dbutils.secrets.get("glofas-databricks-test", "snowflake_user"),
    password=dbutils.secrets.get("glofas-databricks-test", "snowflake_password"),
    warehouse=dbutils.secrets.get("glofas-databricks-test", "snowflake_warehouse"),
    database=dbutils.secrets.get("glofas-databricks-test", "snowflake_database"),
    schema=dbutils.secrets.get("glofas-databricks-test", "snowflake_schema"),
)
cur = conn.cursor()
cur.execute("SELECT CURRENT_VERSION()")
print("Snowflake connection OK, version:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM AOTS.TC_ECMWF.PIPELINE_COUNTRIES WHERE ACTIVE = TRUE")
print("Real read against AOTS.TC_ECMWF.PIPELINE_COUNTRIES OK, active countries:", cur.fetchone()[0])

cur.close()
conn.close()
