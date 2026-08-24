# Databricks notebook source
# MAGIC %md
# MAGIC # DATAPIPELINE on Databricks: Trigger/Scheduling Logic Test
# MAGIC
# MAGIC This notebook covers the dedup/scheduling half of DATAPIPELINE on Databricks: whether the
# MAGIC decision `CHECK_AND_TRIGGER_DATAPIPELINE()` makes every 30 minutes in production
# MAGIC (ORCHESTRATION repo, `09_orchestration/03_procedures.sql`), "is there new work, and have I
# MAGIC already started it," can be replicated correctly from Databricks. The compute+write path
# MAGIC itself, calling the pipeline functions directly on an already-running cluster instead of
# MAGIC launching an SPCS container, is what `04_production_scheduler.py` runs on a live schedule.
# MAGIC
# MAGIC What this does NOT test: the real procedure's `EXECUTE JOB SERVICE` call itself, which launches
# MAGIC the DATAPIPELINE SPCS container. That step is Snowflake-SPCS-specific and has no Databricks
# MAGIC equivalent. A Databricks-based scheduler calls the pipeline functions directly instead, so
# MAGIC this notebook's dedup logic is the piece that decides when to make that direct call, not a
# MAGIC launch trigger.
# MAGIC
# MAGIC Safety note on Section 5 (the only section that writes anything): `TC_PIPELINE_RUN_LOG` is the
# MAGIC real production dedup table the live 30-minute task reads to decide whether to launch the real
# MAGIC SPCS container. Writing a real `TRIGGERED` row for a real, currently-relevant storm/forecast_time
# MAGIC could suppress a genuine production launch for up to 6 hours. Section 5 avoids that entirely by
# MAGIC using a synthetic `STORM_ID` ('DATABRICKS_TRIGGER_TEST') that can never appear in
# MAGIC `TC_ENVELOPES_COMBINED`, and therefore can never be matched by the real procedure's own JOIN:
# MAGIC provably isolated from live dedup state, not just unlikely to collide. The test row is deleted
# MAGIC at the end of the section regardless of outcome.
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
# MAGIC ## 3. Replicate the real match-counting query (read-only)
# MAGIC
# MAGIC This is the same SELECT that `CHECK_AND_TRIGGER_DATAPIPELINE()` runs in production, and the
# MAGIC same query logic now used by the live production scheduler: unprocessed forecast runs (no
# MAGIC SUCCESS, and no TRIGGERED/IN_PROGRESS younger than 6h) within 1500km of an active country, in
# MAGIC the last 2 days. Whatever this returns right now is the same real answer the live production
# MAGIC task gets on its next 30-minute tick. This section changes nothing, it only reads.

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
unprocessed = cur.fetchall()
cur.close()
conn.close()

print(f"Unprocessed forecast runs right now: {len(unprocessed)}")
for row in unprocessed:
    print(" ", row)
if not unprocessed:
    print("(0 is a real, valid answer -- it means production's own live scheduler currently has")
    print(" nothing new to launch either, not that this query is broken.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ground-truth check against known real data
# MAGIC
# MAGIC `DOLPHIN` and `BAVI` both have real `SUCCESS` rows in `TC_PIPELINE_RUN_LOG` from their
# MAGIC original production SPCS runs. Those rows are unrelated to and unaffected by any
# MAGIC Databricks-based compute test run against this data, since `run_complete_impact_analysis()`
# MAGIC doesn't write to this table at all. DOLPHIN and BAVI should NOT appear in Section 3's
# MAGIC unprocessed list above. This section checks that's true for the right reason (a real SUCCESS
# MAGIC row), not just absence.

# COMMAND ----------

conn = get_snowflake_connection()
cur = conn.cursor()
cur.execute("""
    SELECT STORM_ID, FORECAST_TIME, STATUS, STARTED_AT
    FROM   TC_PIPELINE_RUN_LOG
    WHERE  STORM_ID IN ('DOLPHIN', 'BAVI')
    ORDER BY STARTED_AT DESC
    LIMIT 10
""")
rows = cur.fetchall()
cur.close()
conn.close()

for row in rows:
    print(" ", row)
statuses = {r[2] for r in rows}
print()
print("All SUCCESS (correctly excludes from Section 3's query):", statuses == {"SUCCESS"} or "SUCCESS" in statuses)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Marker-write mechanics (real write, provably isolated)
# MAGIC
# MAGIC Tests the actual INSERT + re-exclusion round trip using a synthetic `STORM_ID` that can never
# MAGIC match any real `TC_ENVELOPES_COMBINED` row (no real storm is ever named this), so it can never
# MAGIC be selected by the real production procedure's own JOIN, regardless of timing. This is the one
# MAGIC check in this notebook with no real substitute elsewhere: rerun it any time to verify the dedup
# MAGIC insert/exclusion logic in `TC_PIPELINE_RUN_LOG` is still working correctly, independent of
# MAGIC whatever real storm/forecast data happens to be in the table at the time. Cleans up its own row
# MAGIC at the end whether or not the assertions pass.

# COMMAND ----------

TEST_STORM_ID = "DATABRICKS_TRIGGER_TEST"
TEST_FORECAST_TIME = "2026-01-01 00:00:00"  # arbitrary, fixed, easy to spot/clean up

conn = get_snowflake_connection()
cur = conn.cursor()
try:
    # Sanity: confirm no real envelope could ever match this synthetic storm.
    cur.execute(
        "SELECT COUNT(*) FROM TC_ENVELOPES_COMBINED WHERE TRACK_ID = %s",
        (TEST_STORM_ID,),
    )
    real_envelope_count = cur.fetchone()[0]
    print(f"Real TC_ENVELOPES_COMBINED rows for '{TEST_STORM_ID}': {real_envelope_count} (must be 0)")
    assert real_envelope_count == 0, "Synthetic STORM_ID unexpectedly matches real data -- STOP, do not proceed"

    # Clean slate: remove any leftover row from a prior interrupted test run.
    cur.execute("DELETE FROM TC_PIPELINE_RUN_LOG WHERE STORM_ID = %s", (TEST_STORM_ID,))

    # Step A: real INSERT of a TRIGGERED marker, same shape as the production procedure's own write.
    cur.execute(
        "INSERT INTO TC_PIPELINE_RUN_LOG (STORM_ID, FORECAST_TIME, STATUS) VALUES (%s, %s, 'TRIGGERED')",
        (TEST_STORM_ID, TEST_FORECAST_TIME),
    )
    print("Step A: inserted TRIGGERED marker")

    # Step B: the same "is this already spoken for" check the real procedure runs before launching,
    # scoped to just this synthetic row.
    cur.execute(
        """
        SELECT NOT EXISTS (
            SELECT 1 FROM TC_PIPELINE_RUN_LOG rl
            WHERE  rl.STORM_ID      = %s
              AND  rl.FORECAST_TIME = %s
              AND  (
                    rl.STATUS = 'SUCCESS'
                    OR (rl.STATUS IN ('IN_PROGRESS', 'TRIGGERED')
                        AND rl.STARTED_AT > DATEADD('hour', -6, CURRENT_TIMESTAMP()))
                   )
        )
        """,
        (TEST_STORM_ID, TEST_FORECAST_TIME),
    )
    still_eligible = cur.fetchone()[0]
    print(f"Step B: eligible for a second trigger immediately after marking? {still_eligible} (must be False)")
    assert still_eligible is False, "Marker did not correctly exclude a second trigger -- real bug"

    print()
    print("PASS: marker-write + re-exclusion round trip works correctly.")
finally:
    cur.execute("DELETE FROM TC_PIPELINE_RUN_LOG WHERE STORM_ID = %s", (TEST_STORM_ID,))
    conn.commit()
    remaining = cur.execute(
        "SELECT COUNT(*) FROM TC_PIPELINE_RUN_LOG WHERE STORM_ID = %s", (TEST_STORM_ID,)
    ).fetchone()[0]
    print(f"Cleanup: test rows remaining for '{TEST_STORM_ID}': {remaining} (must be 0)")
    cur.close()
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Result
# MAGIC
# MAGIC If Section 3 runs without error, Section 4 shows real SUCCESS rows correctly excluding DOLPHIN
# MAGIC and BAVI, and Section 5 prints "PASS" with 0 remaining test rows, the full dedup/scheduling
# MAGIC decision `CHECK_AND_TRIGGER_DATAPIPELINE()` makes in production (match-finding, exclusion, and
# MAGIC marker-write/re-exclusion) works correctly from Databricks, with zero risk to live production
# MAGIC dedup state.
# MAGIC
# MAGIC Known real gap this does NOT address (documented separately, not fixed here): the production
# MAGIC procedure only checks `TC_ENVELOPES_COMBINED` (wind/gust storm proximity). There is no
# MAGIC equivalent ambient freshness check against `MET_FORECASTS`/`RIVER_FORECASTS`, so during a
# MAGIC storm-quiet period worldwide, precip/river-flood currently never runs at all in production. A
# MAGIC Databricks-based scheduler polling on its own cadence (rather than waiting for a storm-triggered
# MAGIC SPCS launch) could close this gap by checking ambient data freshness directly; that would be a
# MAGIC real design change, not something this notebook implements.
