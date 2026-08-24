# Databricks Notebooks

This directory holds the real, live production scheduler for DATAPIPELINE's compute, plus standalone
diagnostic/backfill tools that share its code paths. See the root `README.md`'s "Production
Scheduling" section for how this compares to the other deployment options (SPCS, GitHub Actions).

## Files

| File | Runs on a schedule? | Purpose |
|---|---|---|
| `04_production_scheduler.py` | **Yes -- the only one** | The real live scheduler. Discovers unprocessed wind/gust storm cycles and precip/river backlog, computes them, and signals completion. |
| `00_environment_health_check.py` | No | Standalone diagnostic: verifies a cluster can run the real pipeline code (system libraries, Python deps, giga-spatial patches, Snowflake connectivity). Re-run whenever the cluster/runtime, `requirements.txt`, or the installed giga-spatial version changes. Read-only, writes nothing. |
| `01_trigger_logic_test.py` | No | Standalone test of the dedup/scheduling decision logic (mirrors the ORCHESTRATION repo's `CHECK_AND_TRIGGER_DATAPIPELINE()`) in isolation, without running any real compute. Only its Section 5 writes anything (a real `TRIGGERED` row to `TC_PIPELINE_RUN_LOG` for testing purposes). |
| `02_scheduler_logic.py` | No | Standalone, read-only discovery notebook: identifies unprocessed wind/gust cycles and outstanding precip/river backlog depth, for on-call backlog inspection, without triggering any compute or signal. Its wind/gust query predates `04_production_scheduler.py`'s own real-output-existence hardening (see the gotcha note below) and was never updated to match -- it still trusts a logged `SUCCESS` status unconditionally, so its backlog-depth numbers can disagree with what `04_production_scheduler.py` would actually reprocess. |
| `03_scheduler_execute.py` | No | Standalone manual backfill tool: runs `02_scheduler_logic.py`'s discovery plus the real compute calls, deliberately WITHOUT `signal_pipeline_complete()` -- lets you reprocess/backfill data without triggering a real `SEND_ALERT()`/`SEND_WARNING()` alert cascade (since `REFRESH_MATERIALIZED_VIEWS()` only picks up cycles that have been signaled complete). Catches up to the latest cycle only, not a full historical replay. |

## The live job

- **Job ID**: a fixed job ID, named "DATAPIPELINE - Production Scheduler (precip/river/wind/gust)" --
  look it up via `databricks jobs list --profile <your-profile>` rather than hardcoding it here
- **Workspace**: your organization's Databricks workspace hostname
- **Notebook path**: `/Users/<your-databricks-username>/Ahead-of-the-Storm-DATAPIPELINE/databricks/04_production_scheduler`
  (Databricks always places a user's own notebooks under `/Users/<their-username-or-email>/...`)
- **Cluster**: an existing (not job) cluster, so startup is fast -- no cold-start cluster provisioning per run
- **Schedule**: `0 0 0,1,4,5,6,7,10,11,12,13,16,17,18,19,22,23 * * ?` (UTC), 16 fixed times/day,
  cycle-aligned to real forecast cadence rather than continuous polling
- **Concurrency**: `max_concurrent_runs: 1` -- a run that's still going when the next scheduled time
  arrives is left to finish; the new trigger does not stack a second concurrent run
- **On-failure notification**: email to whoever owns/monitors the job
- **Secrets**: read from a Databricks secret scope (Snowflake account/user/password/warehouse/database/
  schema/stage name) -- set via `os.environ[...] = dbutils.secrets.get(...)` at the top of each
  notebook, not via `.env`/`sample_env.txt` the way local/CLI runs are; the real scope name is
  hardcoded in each notebook's own Section 2 ("Configure environment")

## Deploying a change to the live notebook

Editing the local `.py` file does **not** update what the scheduled job actually runs -- Databricks
Jobs run the workspace copy, which has to be re-imported explicitly. `deploy.sh` in this directory
wraps that whole process (auto-resolves your Databricks username, imports, then verifies the import
actually took by exporting it back and diffing against the local file):

```bash
./databricks/deploy.sh                    # uses $DATABRICKS_PROFILE or the CLI's default profile
./databricks/deploy.sh your-profile-name   # or pass a named profile explicitly
```

The default `databricks` CLI profile's token can go stale/invalid independently of a named profile --
if the script (or any `databricks jobs get ...`) fails with an auth error, check `databricks auth
profiles` for a working named profile and pass it explicitly, as above.

The equivalent manual commands, if you'd rather run them by hand or the script doesn't fit your setup:

```bash
databricks workspace import \
  "/Users/<your-databricks-username>/Ahead-of-the-Storm-DATAPIPELINE/databricks/04_production_scheduler" \
  --file "databricks/04_production_scheduler.py" \
  --language PYTHON --format SOURCE --overwrite \
  --profile "<your-profile>"

databricks workspace export \
  "/Users/<your-databricks-username>/Ahead-of-the-Storm-DATAPIPELINE/databricks/04_production_scheduler" \
  --format SOURCE --profile "<your-profile>" > /tmp/deployed_check.py
diff /tmp/deployed_check.py databricks/04_production_scheduler.py
```

## A real Snowflake SQL gotcha hit while editing the discovery query

Snowflake rejects a correlated `EXISTS` nested two levels deep (an `EXISTS` inside an `OR` inside
another correlated `NOT EXISTS`), with `SQL compilation error: Unsupported subquery type cannot be
evaluated`. If the discovery query in `04_production_scheduler.py` (or `02_scheduler_logic.py`, whose
own wind/gust query is a simpler ancestor of it, not an exact mirror -- see the table above) ever
needs a similar real-output-existence check added to an `OR`-combined condition,
either split the outer `NOT EXISTS(A OR B)` into two separate `NOT EXISTS(A) AND NOT EXISTS(B)` clauses
(logically equivalent), or use a `LEFT JOIN` + `IS NOT NULL` check at the outer query's own correlation
level instead of a nested `EXISTS` -- both avoid the double-nesting Snowflake rejects.
