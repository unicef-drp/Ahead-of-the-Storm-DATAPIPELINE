# Databricks Notebooks

This directory holds the 3 real, live production scheduler notebooks for DATAPIPELINE's compute
(one per hazard), plus standalone diagnostic/test/backfill tools that share their code paths. See
the root `README.md`'s "Production Scheduling" section for how this compares to the other
deployment options (SPCS, GitHub Actions).

This directory holds 3 independently-scheduled Databricks Jobs (one per hazard) against the SAME
existing cluster. Each hazard gets its own `max_concurrent_runs: 1` (so a job never overlaps
itself), while the 3 jobs run concurrently with each other freely — a long-running river cycle
never blocks wind/gust's next scheduled trigger, since they don't share a concurrency slot.

## Files

| File | Runs on a schedule? | Purpose |
|---|---|---|
| `04_scheduler_windgust.py` | **Yes** | Live wind/gust scheduler. Discovers unprocessed storm cycles (real output-table existence check against `MERCATOR_TILE_IMPACT_MAT`/`MERCATOR_TILE_GUST_MAT`, not just a logged status), computes them, and signals completion. |
| `05_scheduler_precip.py` | **Yes** | Live precip scheduler. Uses the same "latest"-mode idempotency-gated discovery/compute/signal pattern as `04_scheduler_windgust.py`. |
| `06_scheduler_river.py` | **Yes** | Live river-flood scheduler, on its own sparser schedule (a handful of checks/day, not ~16x/day) since GloFAS updates roughly once daily. |
| `00_environment_health_check.py` | No | Standalone diagnostic: verifies a cluster can run the real pipeline code (system libraries, Python deps, giga-spatial patches, Snowflake connectivity). Re-run whenever the cluster/runtime, `requirements.txt`, or the installed giga-spatial version changes. Read-only, writes nothing. No dependency on the scheduler notebooks. |
| `01_trigger_logic_test.py` | No | Standalone test of the dedup/scheduling decision logic (mirrors the ORCHESTRATION repo's `CHECK_AND_TRIGGER_DATAPIPELINE()`) in isolation, without running any real compute. Covers only the wind/gust half of the pipeline (`TC_ENVELOPES_COMBINED`-based), the same scope `04_scheduler_windgust.py` now owns. Only its Section 5 writes anything (a real `TRIGGERED` row to `TC_PIPELINE_RUN_LOG` for testing purposes, using a synthetic `STORM_ID` that can never match real data). |
| `02_scheduler_logic.py` | No | Standalone, read-only discovery notebook: identifies unprocessed wind/gust cycles and outstanding precip/river backlog depth, for on-call backlog inspection, without triggering any compute or signal. Its wind/gust query is a simpler predecessor of, not an exact mirror of, `04_scheduler_windgust.py`'s own real-output-existence-checked query (see the gotcha note below): it still trusts a logged `SUCCESS` status unconditionally, so its backlog-depth numbers can disagree with what the live wind/gust job would actually reprocess. |
| `03_scheduler_execute.py` | No | Standalone manual backfill tool: runs `02_scheduler_logic.py`'s discovery plus the real compute calls, deliberately WITHOUT `signal_pipeline_complete()`, letting you reprocess/backfill data across all 3 hazards in one pass without triggering a real `SEND_ALERT()`/`SEND_WARNING()` alert cascade (since `REFRESH_MATERIALIZED_VIEWS()` only picks up cycles that have been signaled complete). Catches up to the latest cycle only, not a full historical replay. Inherits `02_scheduler_logic.py`'s wind/gust query staleness above: a backfill run here can reprocess (real writes) a cycle the live `04_scheduler_windgust.py` job would skip, or vice versa. |

## The 3 live jobs

All 3 share the same cluster and secret scope, differ only in job name, notebook path, and
schedule:

| Job name | Notebook | Schedule (UTC, Quartz cron) |
|---|---|---|
| DATAPIPELINE - Wind/Gust Scheduler | `04_scheduler_windgust.py` | `0 0 0,1,4,5,6,7,10,11,12,13,16,17,18,19,22,23 * * ?` (16 fixed times/day) |
| DATAPIPELINE - Precip Scheduler | `05_scheduler_precip.py` | `0 0 0,1,4,5,6,7,10,11,12,13,16,17,18,19,22,23 * * ?` (same 16 fixed times/day as wind/gust) |
| DATAPIPELINE - River-Flood Scheduler | `06_scheduler_river.py` | `0 30 16,18,21,9 * * ?` (4 checks/day: 16:30, 18:30, 21:30, 09:30 UTC) |

- **Job ID**: a fixed job ID per job; look them up via `databricks jobs list --profile
  <your-profile>` rather than hardcoding them here
- **Workspace**: your organization's Databricks workspace hostname
- **Notebook path**: `/Users/<your-databricks-username>/Ahead-of-the-Storm-DATAPIPELINE/databricks/<notebook-name>`
  (Databricks always places a user's own notebooks under `/Users/<their-username-or-email>/...`)
- **Cluster**: an existing (not job) cluster, shared across all 3 jobs, so startup is fast: no
  cold-start cluster provisioning per run
- **Concurrency**: `max_concurrent_runs: 1` per job; a run that's still going when that job's next
  scheduled time arrives is left to finish, the new trigger does not stack a second concurrent run
  for that SAME job, but the 3 jobs are independent of each other and run concurrently freely
  (precip and river run concurrently on this cluster without memory pressure or slowdown for either
  job)
- **On-failure notification**: email to whoever owns/monitors the job
- **Secrets**: read from a Databricks secret scope (Snowflake account/user/password/warehouse/database/
  schema/stage name, plus `pipeline_ops_recipients`, a comma-separated internal recipient list for the
  wind/gust backlog-depth alert in `04_scheduler_windgust.py`'s own Section 4), set via
  `dbutils.secrets.get(...)` (Snowflake values also assigned to `os.environ[...]`) at the top of each
  notebook, not via `.env`/`sample_env.txt` the way local/CLI runs are; the real scope name is
  hardcoded in each notebook's own Section 2 ("Configure environment")
- **known_failed_facilities**: since the 3 hazards now run as separate processes (no shared
  in-memory state), each of `04`/`05`/`06` seeds this set from, and saves it back to, the cross-run
  Blob/stage cache (`load_known_failed_facilities_cache`/`save_known_failed_facilities_cache` in
  `main_pipeline.py`) instead of starting empty every run

## Snowflake correlated-EXISTS nesting limitation

Snowflake rejects a correlated `EXISTS` nested two levels deep (an `EXISTS` inside an `OR` inside
another correlated `NOT EXISTS`) with `SQL compilation error: Unsupported subquery type cannot be
evaluated`. When adding a real-output-existence check to an `OR`-combined condition in a discovery
query (e.g. in `04_scheduler_windgust.py` or `02_scheduler_logic.py`), split `NOT EXISTS(A OR B)`
into two separate `NOT EXISTS(A) AND NOT EXISTS(B)` clauses, or use a `LEFT JOIN` + `IS NOT NULL`
check at the outer correlation level instead.
