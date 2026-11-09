# Databricks Notebooks

This directory holds the 4 production scheduler notebooks for DATAPIPELINE's compute (wind, precip,
river and a separate gust job), plus standalone diagnostic/backfill tools that share their code
paths. See the root `README.md`'s "Production Scheduling" section for how this compares to the other
deployment options (SPCS, GitHub Actions).

The 4 notebooks are independently-scheduled Databricks Jobs against the SAME existing cluster. Each
job gets its own `max_concurrent_runs: 1` (so a job never overlaps itself), while the jobs run
concurrently with each other freely: a long-running river cycle never blocks the wind job's next
scheduled trigger, since they don't share a concurrency slot. The gust job (`07_scheduler_gust.py`)
is currently paused.

## Files

| File | Runs on a schedule? | Purpose |
|---|---|---|
| `04_scheduler_windgust.py` | **Yes** | Live wind scheduler (wind-only by default: the `skip_gust` widget defaults to `true`, and `false` runs gust inline). Discovers unprocessed (storm, cycle, country) work from `TC_PIPELINE_RUN_LOG` and the candidate country cache, ignores stale `IN_PROGRESS` rows and given-up cycles, caps work per run, computes it, and signals completion. Accepts the widgets `max_run_minutes`, `pass1_cap_s`, `pass2_cap_s` and `skip_gust`; a separate job can run it with large caps to retry slow countries. |
| `05_scheduler_precip.py` | **Yes** | Live precip scheduler. Runs `run_precip_analysis()` in "latest" mode, gated by `AMBIENT_HAZARD_RUN_LOG`, then signals completion. A Blob-only listing precheck (`blob_precheck.py`) skips polls with no new `met/` archive before any Snowflake connection. |
| `06_scheduler_river.py` | **Yes** | Live river-flood scheduler, on its own sparser schedule (a handful of checks/day, not ~16x/day) since GloFAS updates roughly once daily. Runs `run_river_flood_analysis()` in "latest" mode, gated by `AMBIENT_HAZARD_RUN_LOG`. |
| `07_scheduler_gust.py` | Own job (separate from the 3 above, currently paused; accepts the `max_run_minutes` widget) | Delayed, decoupled gust scheduler. Discovers work from `gust_pending/` Blob markers written by the wind job once a country's wind result is saved, so an idle poll touches no Snowflake. Does not call `signal_pipeline_complete()`. |
| `00_environment_health_check.py` | No | Standalone diagnostic: verifies a cluster can run the real pipeline code (system libraries, Python deps, giga-spatial patches, Snowflake connectivity). Re-run whenever the cluster/runtime, `requirements.txt`, or the installed giga-spatial version changes. Read-only, writes nothing. No dependency on the scheduler notebooks. |
| `01_trigger_logic_test.py` | No | Standalone dry run of the dedup/scheduling decision logic (mirrors the ORCHESTRATION repo's `CHECK_AND_TRIGGER_DATAPIPELINE()`) in isolation, without running any real compute. Covers only the wind/gust half of the pipeline (`TC_ENVELOPES_COMBINED`-based), the same scope `04_scheduler_windgust.py` owns. Only its Section 5 writes anything (a `TRIGGERED` row to `TC_PIPELINE_RUN_LOG`, using a synthetic `STORM_ID` that can never match real data). |
| `02_scheduler_logic.py` | No | Standalone, read-only discovery notebook: identifies unprocessed wind/gust cycles and outstanding precip/river backlog depth, for on-call backlog inspection, without triggering any compute or signal. Its wind/gust query uses a coarser (storm, cycle) dedup on `TC_PIPELINE_RUN_LOG`, without `04_scheduler_windgust.py`'s per-country dedup or candidate cache, so its backlog-depth numbers can disagree with what the live wind/gust job would actually reprocess. |
| `03_scheduler_execute.py` | No | Standalone manual backfill tool: runs its own copy of `02_scheduler_logic.py`'s discovery plus the real compute calls, deliberately WITHOUT `signal_pipeline_complete()`, letting you reprocess/backfill data across all hazards in one pass without triggering a real `SEND_ALERT()`/`SEND_WARNING()` alert cascade (since `REFRESH_MATERIALIZED_VIEWS()` only picks up cycles that have been signaled complete). Catches up to the latest cycle only, not a full historical replay. Carries its own copy of the stale-lock-aware discovery query, with per-cycle (not per-country) dedup: a backfill run here can reprocess (real writes) a cycle the live `04_scheduler_windgust.py` job would skip, or vice versa. |
| `99_manual_river_reprocess.py` | No | Manual reprocess tool: runs `run_river_flood_analysis()` for an explicit list of (country, date) pairs (widget `jobs`), bypassing the give-up gate. Attach to an existing cluster and run by hand. |

## The live jobs

All jobs share the same cluster and secret scope, differ only in job name, notebook path, and
schedule:

| Job name | Notebook | Schedule (UTC, Quartz cron) |
|---|---|---|
| DATAPIPELINE - Wind Scheduler | `04_scheduler_windgust.py` | `0 0 0,1,4,5,6,7,10,11,12,13,16,17,18,19,22,23 * * ?` (16 fixed times/day) |
| DATAPIPELINE - Precip Scheduler | `05_scheduler_precip.py` | `0 0 0,1,4,5,6,7,10,11,12,13,16,17,18,19,22,23 * * ?` (same 16 fixed times/day as wind) |
| DATAPIPELINE - River-Flood Scheduler | `06_scheduler_river.py` | `0 30 16,18,21,9 * * ?` (4 checks/day: 16:30, 18:30, 21:30, 09:30 UTC) |
| DATAPIPELINE - Gust Scheduler | `07_scheduler_gust.py` | Currently paused (polls `gust_pending/` Blob markers when unpaused) |

- **Job ID**: a fixed job ID per job; look them up via `databricks jobs list --profile
  <your-profile>` rather than hardcoding them here
- **Workspace**: your organization's Databricks workspace hostname
- **Notebook path**: `/Users/<your-databricks-username>/Ahead-of-the-Storm-DATAPIPELINE/databricks/<notebook-name>`
  (Databricks always places a user's own notebooks under `/Users/<their-username-or-email>/...`)
- **Cluster**: an existing (not job) cluster, shared across all jobs, so startup is fast: no
  cold-start cluster provisioning per run
- **Concurrency**: `max_concurrent_runs: 1` per job; a run that's still going when that job's next
  scheduled time arrives is left to finish, the new trigger does not stack a second concurrent run
  for that SAME job, but the jobs are independent of each other and run concurrently freely
  (precip and river run concurrently on this cluster; the river job limits its own country workers
  by live process memory via `RIVER_MEMORY_BUDGET_MB`)
- **On-failure notification**: email to whoever owns/monitors the job
- **Secrets**: read from a Databricks secret scope (Snowflake account/user/password/warehouse/database/
  schema/stage name, plus `pipeline_ops_recipients`, a comma-separated internal recipient list for the
  wind/gust backlog-depth alert in `04_scheduler_windgust.py`'s own Section 4), set via
  `dbutils.secrets.get(...)` (Snowflake values also assigned to `os.environ[...]`) at the top of each
  notebook, not via `.env`/`sample_env.txt` the way local/CLI runs are; the real scope name is
  hardcoded in each notebook's own Section 2 ("Configure environment")
- **known_failed_facilities**: since the hazards run as separate processes (no shared
  in-memory state), each of `04`/`05`/`06`/`07` seeds this set from, and saves it back to, the cross-run
  Blob/stage cache (`load_known_failed_facilities_cache`/`save_known_failed_facilities_cache` in
  `main_pipeline.py`) instead of starting empty every run

## Deploying a change

Pushing to git never updates Databricks. The jobs run the copies in the workspace folder
`/Users/<your-databricks-username>/Ahead-of-the-Storm-DATAPIPELINE/`, which change only when a file
is imported explicitly. Each script below resolves your username, imports the file, then exports it
again and diffs it against the local file (it exits non-zero if they differ).

| What | Command |
|---|---|
| A notebook in this directory (`04` to `07`, `00` to `03`, `99`) | `./databricks/deploy.sh <notebook-name> [profile]` |
| A repo-root module (`impact_analysis`, `snowflake_utils`, `sparse_build`, ...) | `./databricks/deploy_module.sh <module-name> [profile]` |
| `main_pipeline.py` | `./databricks/deploy_main_pipeline.sh [profile]` (same as `deploy_module.sh main_pipeline`) |

- The modules import each other, so a change that touches several modules must be imported as a
  set. A job run that starts between two imports sees a mix of old and new modules.
- Before overwriting, export the current workspace copy of each file (`databricks workspace export`)
  so that a rollback is a re-import of the saved copy.
- The profile defaults to `$DATABRICKS_PROFILE`, otherwise the CLI's default profile.

## Snowflake correlated-EXISTS nesting limitation

Snowflake rejects a correlated `EXISTS` nested two levels deep (an `EXISTS` inside an `OR` inside
another correlated `NOT EXISTS`) with `SQL compilation error: Unsupported subquery type cannot be
evaluated`. When adding a real-output-existence check to an `OR`-combined condition in a discovery
query (e.g. in `04_scheduler_windgust.py` or `02_scheduler_logic.py`), split `NOT EXISTS(A OR B)`
into two separate `NOT EXISTS(A) AND NOT EXISTS(B)` clauses, or use a `LEFT JOIN` + `IS NOT NULL`
check at the outer correlation level instead.
