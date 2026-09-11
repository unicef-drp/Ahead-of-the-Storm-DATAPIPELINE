"""
Snowflake run-log/dedup bookkeeping for the impact analysis pipeline.

Covers three independent bookkeeping tables:
- TC_PIPELINE_RUN_LOG: per (storm_id, forecast_time) run tracking/dedup for
  the storm-track branch (main_pipeline.py's update_storms()).
- AMBIENT_HAZARD_RUN_LOG / AMBIENT_HAZARD_FAILURE_LOG: per (source, param,
  forecast_time) idempotency/give-up gate for the ambient precip/river-flood
  branches (main_pipeline.py's run_precip_analysis()/run_river_flood_analysis()).
- TC_PIPELINE_COMPLETE_LOG: batch-completion signal that triggers the
  stream-based refresh of *_MAT tables in Snowflake.

Extracted out of main_pipeline.py as a self-contained concern: every function
here takes only a Snowflake connection plus plain scalar/list/dict arguments
and does not reach into any pipeline-orchestration-specific state.
"""

import json
import logging
from datetime import datetime, timezone

import pandas as pd


# =============================================================================
# SNOWFLAKE RUN LOGGING
# =============================================================================

def is_already_processed(conn, storm_id: str, forecast_time, countries: list) -> bool:
    """
    Return True if this (storm_id, forecast_time) has a SUCCESS or recent
    IN_PROGRESS record that already covers ALL of the currently-requested
    countries.

    Includes the country dimension, unlike keying purely on (storm_id,
    forecast_time): a prior narrower run (e.g. just JAM) must not silently
    satisfy a later, broader request (e.g. JAM+DOM after DOM is onboarded),
    or DOM would never actually get processed for this storm/forecast.
    Mirrors the LOCAL/BLOB JSON-mode dedup key below, which already includes
    the sorted country list in storm_key.

    Unions COUNTRIES_PROCESSED across every matching SUCCESS/IN_PROGRESS row
    rather than requiring a single row to already be a superset of the
    requested set: the scheduler caps how many countries one run processes
    per storm/forecast_time cycle, so a single cycle's coverage is routinely
    split across several SUCCESS rows, each with a small COUNTRIES_PROCESSED.
    A single-row superset check would never match a cycle fully covered
    across multiple such rows, wrongly concluding "not processed" and
    reprocessing every country from scratch.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNTRIES_PROCESSED FROM AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG
        WHERE STORM_ID = %s
          AND FORECAST_TIME = %s
          AND (
            STATUS = 'SUCCESS'
            OR (STATUS = 'IN_PROGRESS'
                AND STARTED_AT > DATEADD('hour', -6, CURRENT_TIMESTAMP()))
          )
    """, (storm_id, forecast_time))
    rows = cur.fetchall()
    cur.close()
    requested = set(countries or [])
    if not requested:
        return len(rows) > 0
    processed_union = set()
    for (countries_processed_raw,) in rows:
        if isinstance(countries_processed_raw, str):
            processed_union.update(json.loads(countries_processed_raw))
        elif isinstance(countries_processed_raw, (list, tuple)):
            processed_union.update(countries_processed_raw)
    return requested.issubset(processed_union)


def log_run_start(conn, storm_id: str, forecast_time, countries: list = None) -> None:
    """
    Insert an IN_PROGRESS marker into TC_PIPELINE_RUN_LOG.

    countries must be written here (not left NULL): is_already_processed()'s
    concurrent-run lock checks whether an IN_PROGRESS row's own
    COUNTRIES_PROCESSED is a superset of a later request's countries, a NULL
    value here would make that check always fail (falls to an empty set),
    silently disabling the lock and letting a second overlapping invocation
    for the same storm/forecast/countries run concurrently with this one.
    """
    # STARTED_AT is bound as an explicit Python UTC datetime, not
    # CURRENT_TIMESTAMP(): CURRENT_TIMESTAMP() returns TIMESTAMP_LTZ, and
    # binding/selecting it directly into a TIMESTAMP_NTZ column makes
    # Snowflake implicitly convert using the session's timezone parameter
    # rather than UTC, silently storing a wall-clock value hours off from
    # the real UTC insertion instant.
    started_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG
            (STORM_ID, FORECAST_TIME, STATUS, COUNTRIES_PROCESSED, STARTED_AT)
        SELECT %s, %s, 'IN_PROGRESS', PARSE_JSON(%s), %s
    """, (storm_id, forecast_time, json.dumps(countries or []), started_at_utc))
    conn.commit()
    cur.close()


def log_run_complete(conn, storm_id: str, forecast_time, success: bool,
                     countries: list = None, files_written: int = 0,
                     error_message: str = None, started_at=None,
                     no_impact_countries: list = None) -> None:
    """
    Insert a SUCCESS or FAILURE completion record into TC_PIPELINE_RUN_LOG.

    no_impact_countries: countries requested this run that produced no error
    and genuinely wrote zero real output (envelope never reached them, or the
    100km buffer never intersected). Distinct from `countries`
    (COUNTRIES_PROCESSED, i.e. real output written): a country can be
    requested and neither list without being lost: it stays eligible for
    retry (matches the deliberate "retry forever, never silently give up"
    policy) if it errored, or if no_impact couldn't be confirmed via a
    trusted evaluation. See run_complete_impact_analysis()'s
    country_impact_confirmed / unevaluated_countries for the caller-side
    gating on this (unevaluated_countries gives per-country precision;
    country_impact_confirmed is the same information collapsed to one
    call-level flag).
    """
    runtime_seconds = None
    if started_at:
        runtime_seconds = (datetime.now() - started_at).total_seconds()
    status = 'SUCCESS' if success else 'FAILURE'
    # COMPLETED_AT is bound as an explicit Python UTC datetime, not
    # CURRENT_TIMESTAMP(). Same TIMESTAMP_LTZ-into-TIMESTAMP_NTZ session-
    # timezone bug as STARTED_AT above (log_run_start()).
    completed_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG
            (STORM_ID, FORECAST_TIME, STATUS, COUNTRIES_PROCESSED, NO_IMPACT_COUNTRIES,
             FILES_WRITTEN, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT, RUNTIME_SECONDS)
        SELECT %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s, %s, %s, %s, %s
    """, (
        storm_id,
        forecast_time,
        status,
        json.dumps(countries or []),
        json.dumps(no_impact_countries or []),
        files_written,
        error_message,
        started_at,
        completed_at_utc,
        runtime_seconds,
    ))
    conn.commit()
    cur.close()


# =============================================================================
# AMBIENT (precip/river-flood) IDEMPOTENCY GATE
# =============================================================================
# Precip/river-flood have no equivalent of TC_PIPELINE_RUN_LOG's (storm_id,
# forecast_time) dedup: without it they'd reprocess whichever forecast cycle
# is "latest" every 30-min scheduled run, even though MET_FORECASTS only
# refreshes 4x/day and RIVER_FORECASTS once/day (up to ~48x redundant
# reprocessing for river). AMBIENT_HAZARD_RUN_LOG is a small, self-creating
# table tracking the last forecast_time processed per (source, param). Only
# consulted in "latest" mode; an explicit historical backfill always reprocesses.


def is_ambient_forecast_processed(conn, source: str, param: str, forecast_time) -> bool:
    """
    True if this (source, param, forecast_time) cycle was already fully
    processed by a prior run_precip_analysis()/run_river_flood_analysis()
    call. Fails OPEN (returns False, i.e. "not yet processed") on any query
    error rather than raising: a bookkeeping-table hiccup must never block
    real ambient-hazard processing, only cost an occasional redundant re-run,
    the same "logging is best-effort" philosophy log_run_start()/
    log_run_complete() already follow at their own call sites.

    Args:
        conn: open Snowflake connection.
        source: 'precip' or 'river'.
        param: 'tp_ro' for precip (tp/ro are reconciled to one shared
            forecast_time upstream before this is called, see
            run_precip_analysis()); the RP tier string (e.g. 'rp10') for
            river, since each tier has its own independent forecast_time.
        forecast_time: the forecast cycle's own timestamp. Converted to a
            native Python datetime before binding, since the Snowflake
            connector cannot bind a pandas Timestamp directly against a
            TIMESTAMP_NTZ column.
    """
    forecast_time = pd.Timestamp(forecast_time).to_pydatetime()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS AOTS.TC_ECMWF.AMBIENT_HAZARD_RUN_LOG (
                SOURCE VARCHAR,
                PARAM VARCHAR,
                FORECAST_TIME TIMESTAMP_NTZ,
                PROCESSED_AT TIMESTAMP_NTZ
            )
        """)
        cur.execute("""
            SELECT 1 FROM AOTS.TC_ECMWF.AMBIENT_HAZARD_RUN_LOG
            WHERE SOURCE = %s AND PARAM = %s AND FORECAST_TIME = %s
            LIMIT 1
        """, (source, param, forecast_time))
        found = cur.fetchone() is not None
        cur.close()
        return found
    except Exception as e:
        # Fails open (returns False, "not yet processed") deliberately, per this
        # function's own docstring: a bookkeeping-table hiccup must never block real
        # processing. Logged (not silently swallowed) so a real skip-check failure is
        # distinguishable from a genuine "not processed yet" without needing to
        # reproduce the exact query by hand to find out which one actually happened.
        logging.getLogger(__name__).warning(
            f"is_ambient_forecast_processed({source}, {param}, {forecast_time}) failed, "
            f"treating as not-yet-processed: {e}"
        )
        return False


def log_ambient_forecast_processed(conn, source: str, param: str, forecast_time) -> None:
    """Marks (source, param, forecast_time) as processed in
    AMBIENT_HAZARD_RUN_LOG. Best-effort: a failure here just means the next
    run redundantly reprocesses this same cycle once more, not a correctness
    problem, so it's caller-wrapped in try/except rather than raising.

    PROCESSED_AT is bound as an explicit Python UTC datetime, not
    CURRENT_TIMESTAMP(): CURRENT_TIMESTAMP() returns TIMESTAMP_LTZ, and
    binding/selecting it directly into a TIMESTAMP_NTZ column makes Snowflake
    implicitly convert using the SESSION's timezone parameter rather than
    UTC, silently storing a wall-clock value hours off from the real UTC
    insertion instant."""
    forecast_time = pd.Timestamp(forecast_time).to_pydatetime()
    processed_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.AMBIENT_HAZARD_RUN_LOG
            (SOURCE, PARAM, FORECAST_TIME, PROCESSED_AT)
        SELECT %s, %s, %s, %s
    """, (source, param, forecast_time, processed_at_utc))
    conn.commit()
    cur.close()


# A country that fails EVERY run (a reproducible OOM, a permanently-
# unloadable mercator view, etc.) would otherwise keep a (source, param,
# forecast_time) cycle unmarked forever, since the give-up gate correctly
# refuses to mark a cycle done while any country genuinely fails - but with
# no limit, the scheduler (every ~30 minutes) redoes the full country/tier
# pass every cycle for as long as that one country keeps failing.
# AMBIENT_HAZARD_FAILURE_LOG tracks consecutive genuine-failure runs per
# cycle; once it reaches AMBIENT_HAZARD_MAX_FAILURE_ATTEMPTS, the cycle is
# marked done anyway (other countries' output stays valid), but loudly:
# GAVE_UP=TRUE and the failed-countries list stay queryable in that table. A
# new forecast_time is a fresh row/attempt count, so this never blacklists a
# country permanently, only gives up on one superseded cycle.
AMBIENT_HAZARD_MAX_FAILURE_ATTEMPTS = 3


def record_ambient_failure_attempt(conn, source: str, param: str, forecast_time, failed_countries: list, logger,
                                    failure_reasons: dict = None) -> bool:
    """
    Records one more real attempt at (source, param, forecast_time) that
    still had a genuine country failure (failed_countries non-empty).
    Returns True if the real, cumulative attempt count for this exact cycle
    has now reached AMBIENT_HAZARD_MAX_FAILURE_ATTEMPTS (the caller should
    give up and mark the cycle done anyway, loudly), False otherwise (the
    caller should keep the cycle unmarked and eligible for another retry,
    matching this give-up gate's own existing behavior).

    failure_reasons: optional {country: reason_string} for some/all of
    failed_countries (e.g. a subprocess-timeout message, an internal
    exception's str(), or a worker-caught mercator-load error), persisted
    into the FAILURE_REASONS column alongside FAILED_COUNTRIES. None (the
    default) stores an empty JSON object. failure_reasons exists because
    FAILED_COUNTRIES alone records WHICH country failed but not WHY, and a
    forked child's own stdout is not reliably retrievable once the run
    completes. A country whose reason is unknown (not passed here) is still
    recorded under FAILED_COUNTRIES; this column is a strictly additive
    diagnostic aid, never a gate on the retry/give-up decision itself.

    Fails OPEN toward "keep retrying" (returns False) on any query error,
    the same "a bookkeeping-table hiccup must never wrongly finalize a real
    gap" philosophy is_ambient_forecast_processed() already follows for its
    own opposite direction (fails open toward "not yet processed").
    """
    forecast_time = pd.Timestamp(forecast_time).to_pydatetime()
    last_attempt_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    failure_reasons = failure_reasons or {}
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS AOTS.TC_ECMWF.AMBIENT_HAZARD_FAILURE_LOG (
                SOURCE VARCHAR,
                PARAM VARCHAR,
                FORECAST_TIME TIMESTAMP_NTZ,
                ATTEMPT_COUNT NUMBER,
                FAILED_COUNTRIES VARCHAR,
                LAST_ATTEMPT_AT TIMESTAMP_NTZ,
                GAVE_UP BOOLEAN DEFAULT FALSE,
                FAILURE_REASONS VARCHAR
            )
        """)
        # ALTER TABLE ... ADD COLUMN IF NOT EXISTS FAILURE_REASONS ensures the
        # column exists on any pre-existing table, since CREATE TABLE IF NOT
        # EXISTS above is a no-op once the table already exists.
        cur.execute("""
            ALTER TABLE AOTS.TC_ECMWF.AMBIENT_HAZARD_FAILURE_LOG
            ADD COLUMN IF NOT EXISTS FAILURE_REASONS VARCHAR
        """)
        cur.execute("""
            SELECT ATTEMPT_COUNT FROM AOTS.TC_ECMWF.AMBIENT_HAZARD_FAILURE_LOG
            WHERE SOURCE = %s AND PARAM = %s AND FORECAST_TIME = %s
        """, (source, param, forecast_time))
        row = cur.fetchone()
        new_count = (row[0] if row else 0) + 1
        failed_countries_json = json.dumps(sorted(failed_countries))
        failure_reasons_json = json.dumps(failure_reasons, sort_keys=True)
        gave_up = new_count >= AMBIENT_HAZARD_MAX_FAILURE_ATTEMPTS
        if row is None:
            cur.execute("""
                INSERT INTO AOTS.TC_ECMWF.AMBIENT_HAZARD_FAILURE_LOG
                    (SOURCE, PARAM, FORECAST_TIME, ATTEMPT_COUNT, FAILED_COUNTRIES, LAST_ATTEMPT_AT, GAVE_UP, FAILURE_REASONS)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s
            """, (source, param, forecast_time, new_count, failed_countries_json, last_attempt_at_utc, gave_up, failure_reasons_json))
        else:
            cur.execute("""
                UPDATE AOTS.TC_ECMWF.AMBIENT_HAZARD_FAILURE_LOG
                SET ATTEMPT_COUNT = %s, FAILED_COUNTRIES = %s, LAST_ATTEMPT_AT = %s, GAVE_UP = %s, FAILURE_REASONS = %s
                WHERE SOURCE = %s AND PARAM = %s AND FORECAST_TIME = %s
            """, (new_count, failed_countries_json, last_attempt_at_utc, gave_up, failure_reasons_json, source, param, forecast_time))
        conn.commit()
        cur.close()
        if gave_up:
            logger.error(
                f"GIVING UP on {source}/{param} at {forecast_time}: {', '.join(sorted(failed_countries))} "
                f"failed {new_count} separate real attempts in a row (limit: "
                f"{AMBIENT_HAZARD_MAX_FAILURE_ATTEMPTS}). Marking this cycle processed anyway so the "
                f"scheduler stops redoing the full pass every run; the real output every other country "
                f"already wrote stays valid. This exact gap is recorded permanently in "
                f"AMBIENT_HAZARD_FAILURE_LOG (GAVE_UP=TRUE) for {source}/{param}/{forecast_time} until a "
                f"newer forecast_time cycle supersedes it."
            )
        else:
            logger.warning(
                f"{source}/{param} at {forecast_time}: {', '.join(sorted(failed_countries))} failed "
                f"attempt {new_count} of {AMBIENT_HAZARD_MAX_FAILURE_ATTEMPTS} before this cycle would be "
                f"given up on; still eligible for retry."
            )
        return gave_up
    except Exception as e:
        logger.warning(
            f"Could not record/check the failure-attempt count for {source}/{param} at {forecast_time} "
            f"({e}); defaulting to NOT giving up (keeps this cycle eligible for retry, the safer failure "
            f"mode here)."
        )
        return False


# =============================================================================
# COMPLETION SIGNAL
# =============================================================================

def signal_pipeline_complete(conn, storm_ids: list, countries: list, files_written: int, runtime_seconds: int = None):
    """
    Insert a batch-completion record into TC_PIPELINE_COMPLETE_LOG.
    This triggers the stream-based refresh of *_MAT tables in Snowflake.
    Only called when at least one storm was processed successfully.
    """
    cur = conn.cursor()
    # Force both stage directory tables to sync before signalling completion.
    # Without this, AOTS_ANALYSIS_FILE_STREAM / AOTS_ANALYSIS_BLOB_FILE_STREAM
    # may not yet reflect newly written files when the refresh task fires,
    # causing a silent missed refresh. Both stages are refreshed unconditionally
    # regardless of which one this particular run actually wrote to, since a
    # caller may run under either DATA_PIPELINE_DB mode.
    cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS REFRESH")
    cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS_BLOB REFRESH")
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_PIPELINE_COMPLETE_LOG
            (STORM_IDS, COUNTRIES_PROCESSED, FILES_WRITTEN, STATUS, RUNTIME_SECONDS)
        SELECT PARSE_JSON(%s), PARSE_JSON(%s), %s, 'SUCCESS', %s
    """, (
        json.dumps(storm_ids),
        json.dumps(countries),
        files_written,
        runtime_seconds
    ))
    conn.commit()
    cur.close()
