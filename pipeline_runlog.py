"""
Snowflake run-log/dedup bookkeeping for the impact analysis pipeline.

Covers the run-log and bookkeeping tables for wind, gust, precip and
river-flood, the candidate country cache, and the direct MAT refresh trigger:
- TC_PIPELINE_RUN_LOG / TC_PIPELINE_GAVE_UP_LOG: per (storm_id,
  forecast_time) run tracking/dedup and the give-up record for the storm-track
  branch (main_pipeline.py's update_storms()).
- TC_GUST_RUN_LOG / TC_GUST_GAVE_UP_LOG: the same bookkeeping for the gust job.
- CANDIDATE_COUNTRY_CACHE: the cached candidate country list for discovery.
- AMBIENT_HAZARD_RUN_LOG / AMBIENT_HAZARD_FAILURE_LOG: per (source, param,
  forecast_time) idempotency/give-up gate for the ambient precip/river-flood
  branches (main_pipeline.py's run_precip_analysis()/run_river_flood_analysis()).
- TC_PIPELINE_COMPLETE_LOG: batch-completion signal that triggers the
  stream-based refresh of *_MAT tables in Snowflake.
- MAT_REFRESH_TRIGGER_LOG: the debounced direct refresh trigger.

Extracted out of main_pipeline.py as a self-contained concern: every function
here takes only a Snowflake connection plus plain scalar/list/dict arguments
and does not reach into any pipeline-orchestration-specific state.
"""

import json
import logging
import os
import time
import uuid
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
    # CURRENT_TIMESTAMP(), not SYSDATE(): STARTED_AT is genuinely TIMESTAMP_TZ
    # (per the table DDL). CURRENT_TIMESTAMP() returns
    # TIMESTAMP_LTZ; comparing two timezone-AWARE types (LTZ vs TZ) is a true
    # absolute-instant comparison, unaffected by the session's TIMEZONE
    # parameter either way. SYSDATE() (TIMESTAMP_NTZ, a naive value) would be
    # wrong here: Snowflake promotes a naive NTZ value to an instant using the
    # session timezone before comparing it against a TZ column, introducing a
    # session-timezone dependency this comparison must not have. STARTED_AT's
    # own value is written as a genuine, timezone-aware UTC instant by
    # log_run_start()/log_run_complete() below (see their own comments), so
    # this comparison holds correctly regardless of the session's TIMEZONE.
    # The IN_PROGRESS branch below excludes a stale marker the same way
    # 04_scheduler_windgust.py's own discovery query does (see that file's
    # own comment for the full "why"): TC_PIPELINE_RUN_LOG is insert-only, so
    # an attempt that already finished still leaves a permanent IN_PROGRESS
    # row for up to 6h. Without this exclusion, that row's own (often
    # narrower, interim) COUNTRIES_PROCESSED gets folded into
    # processed_union below well after a real SUCCESS/FAILURE row already
    # superseded it, which can only ever make this "already processed" check
    # too narrow (fewer countries look covered than a later completed run
    # actually reported), never wrongly mark something as done.
    #
    # RUN_ID exact match, not just "a terminal row completed after this one
    # started": a timestamp-only comparison can't tell this attempt's own
    # completion apart from a different, unrelated, faster attempt for the
    # same (storm, forecast_time) finishing later purely by chance while this
    # one is still genuinely running, which would prematurely release a real
    # in-flight lock (a real, if narrow, residual risk of a timestamp-only
    # comparison).
    # Falls back to the timestamp comparison only for a row that has no
    # RUN_ID (RUN_ID IS NULL), so an orphaned IN_PROGRESS row without a
    # RUN_ID doesn't lock forever.
    #
    # UNION ALL of two plain queries, not one query with a top-level OR, and
    # two separate NOT EXISTS clauses inside the second branch rather than one
    # NOT EXISTS with an internal OR: Snowflake rejects a correlated subquery
    # whose WHERE clause ORs two conditions that each reference outer-scope
    # columns ("Unsupported subquery type cannot be evaluated"). Both
    # restructurings are logically identical to the OR'd forms (De Morgan's
    # law for the inner one), expressed in a shape Snowflake's optimizer can
    # evaluate.
    cur.execute("""
        SELECT COUNTRIES_PROCESSED FROM AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG
        WHERE STORM_ID = %s AND FORECAST_TIME = %s AND STATUS = 'SUCCESS'
        UNION ALL
        SELECT rl.COUNTRIES_PROCESSED FROM AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG rl
        WHERE rl.STORM_ID = %s AND rl.FORECAST_TIME = %s
          AND rl.STATUS = 'IN_PROGRESS'
          AND rl.STARTED_AT > DATEADD('hour', -6, CURRENT_TIMESTAMP())
          AND NOT EXISTS (
                  SELECT 1
                  FROM   AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG rl2
                  WHERE  rl2.STORM_ID      = rl.STORM_ID
                    AND  rl2.FORECAST_TIME = rl.FORECAST_TIME
                    AND  rl2.STATUS IN ('SUCCESS', 'FAILURE')
                    AND  rl.RUN_ID IS NOT NULL AND rl2.RUN_ID = rl.RUN_ID
              )
          AND NOT EXISTS (
                  SELECT 1
                  FROM   AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG rl2
                  WHERE  rl2.STORM_ID      = rl.STORM_ID
                    AND  rl2.FORECAST_TIME = rl.FORECAST_TIME
                    AND  rl2.STATUS IN ('SUCCESS', 'FAILURE')
                    AND  rl.RUN_ID IS NULL AND rl2.COMPLETED_AT > rl.STARTED_AT
              )
    """, (storm_id, forecast_time, storm_id, forecast_time))
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


def log_run_start(conn, storm_id: str, forecast_time, countries: list = None) -> str:
    """
    Insert an IN_PROGRESS marker into TC_PIPELINE_RUN_LOG. Returns the RUN_ID
    generated for this attempt; pass it back into log_run_complete()'s own
    `run_id` parameter so the two rows for this one attempt can be correlated
    precisely, rather than by timestamp comparison alone (see
    is_already_processed()'s own comment on why an exact RUN_ID match is
    needed to fully close the stale-lock race).

    countries must be written here (not left NULL): is_already_processed()'s
    concurrent-run lock checks whether an IN_PROGRESS row's own
    COUNTRIES_PROCESSED is a superset of a later request's countries, a NULL
    value here would make that check always fail (falls to an empty set),
    silently disabling the lock and letting a second overlapping invocation
    for the same storm/forecast/countries run concurrently with this one.
    """
    run_id = str(uuid.uuid4())
    # STARTED_AT is TIMESTAMP_TZ (per the table DDL), so this
    # binds a TIMEZONE-AWARE Python datetime, not a naive one:
    # a timezone-aware UTC datetime carries its own UTC offset, which the
    # Snowflake connector uses directly to store the correct absolute
    # instant, regardless of the session's TIMEZONE parameter. Binding a
    # naive datetime into a TIMESTAMP_TZ column instead makes Snowflake
    # reinterpret it using the SESSION's timezone rather than treating it as
    # already-UTC. Under a non-UTC session TIMEZONE (e.g.
    # America/Los_Angeles, UTC-7), that silently shifts the stored instant
    # hours away from the real one, which would corrupt
    # is_already_processed()'s own 6-hour retry-lock window above (a
    # multi-hour write-side shift makes a 6-hour lock behave like a much
    # longer one).
    started_at_utc = datetime.now(timezone.utc)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG
            (STORM_ID, FORECAST_TIME, STATUS, COUNTRIES_PROCESSED, STARTED_AT, RUN_ID)
        SELECT %s, %s, 'IN_PROGRESS', PARSE_JSON(%s), %s, %s
    """, (storm_id, forecast_time, json.dumps(countries or []), started_at_utc, run_id))
    conn.commit()
    cur.close()
    return run_id


def log_run_complete(conn, storm_id: str, forecast_time, success: bool,
                     countries: list = None, files_written: int = 0,
                     error_message: str = None, started_at=None,
                     no_impact_countries: list = None, run_id: str = None) -> None:
    """
    Insert a SUCCESS or FAILURE completion record into TC_PIPELINE_RUN_LOG.

    run_id: the value log_run_start() returned for this same attempt, if
    available (call sites that do not pass this parameter still work,
    falling back to timestamp-based correlation only, see
    is_already_processed()'s own comment). Written into this row's own RUN_ID
    so the two rows for one attempt (this one and log_run_start()'s) can be
    matched exactly instead of by "which terminal row happened to complete
    after this IN_PROGRESS row's own STARTED_AT"; the latter is vulnerable
    to a different, unrelated, faster attempt for the same (storm,
    forecast_time) prematurely releasing a genuinely-still-running one's lock
    (a real, if narrow, residual risk of a timestamp-only comparison).

    no_impact_countries: countries requested this run that produced no error
    and genuinely wrote zero real output (envelope never reached them, or the
    100km buffer never intersected). Distinct from `countries`
    (COUNTRIES_PROCESSED, i.e. real output written): a country can be
    requested and appear in neither list without being lost: it stays eligible for
    retry when no_impact could not be confirmed via a trusted evaluation, and when it
    errored, until the cycle is given up after WINDGUST_MAX_FAILURE_ATTEMPTS FAILURE
    rows (recorded in TC_PIPELINE_GAVE_UP_LOG). See run_complete_impact_analysis()'s
    country_impact_confirmed / unevaluated_countries for the caller-side
    gating on this (unevaluated_countries gives per-country precision;
    country_impact_confirmed is the same information collapsed to one
    call-level flag).
    """
    runtime_seconds = None
    if started_at:
        # started_at may be naive or timezone-aware (see
        # 04_scheduler_windgust.py/main_pipeline.py's own comments on this):
        # match the current time's awareness to
        # whichever started_at actually is, since Python raises on a
        # naive-vs-aware subtraction rather than silently doing the wrong
        # thing.
        now = datetime.now(timezone.utc) if started_at.tzinfo is not None else datetime.now()
        runtime_seconds = (now - started_at).total_seconds()
    status = 'SUCCESS' if success else 'FAILURE'
    # COMPLETED_AT is TIMESTAMP_TZ, same real fix as STARTED_AT above
    # (log_run_start()): bind a timezone-AWARE datetime so the connector
    # stores the correct absolute instant, not one silently shifted by the
    # session's TIMEZONE parameter.
    completed_at_utc = datetime.now(timezone.utc)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG
            (STORM_ID, FORECAST_TIME, STATUS, COUNTRIES_PROCESSED, NO_IMPACT_COUNTRIES,
             FILES_WRITTEN, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT, RUNTIME_SECONDS, RUN_ID)
        SELECT %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s, %s, %s, %s, %s, %s
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
        run_id,
    ))
    conn.commit()
    cur.close()


# =============================================================================
# GIVE-UP GATE (wind/gust)
# =============================================================================
# Wind/gust is the counterpart of precip/river's AMBIENT_HAZARD_FAILURE_LOG
# give-up gate below. Without a gate, a storm/forecast_time that keeps
# genuinely failing every real attempt (a reproducible subprocess hang) would
# stay a candidate in the scheduler's own discovery query for as long as it
# remained inside the lookback window, then silently stop being retried once
# it aged out, with no alert and no queryable signal distinguishing
# "genuinely never attempted" from "repeatedly failed and quietly given up on."
#
# Deliberately reuses TC_PIPELINE_RUN_LOG's OWN existing per-attempt FAILURE
# rows as the attempt count (unlike AMBIENT_HAZARD_FAILURE_LOG below, which
# needs its own counter because AMBIENT_HAZARD_RUN_LOG never records a
# failure, only a success); wind/gust already logs one real FAILURE row per
# genuine attempt via log_run_complete() above, so counting those directly
# avoids a second, redundant counter that could drift out of sync with the
# real run history.
#
# Deliberately does NOT reuse TC_PIPELINE_RUN_LOG.NO_IMPACT_COUNTRIES the way
# precip/river's give-up marks AMBIENT_HAZARD_RUN_LOG "processed": that
# column has a specific, honest meaning (a real, confirmed zero-impact
# determination) that giving up must never overload; doing so would
# silently reintroduce the exact "confirmed done" vs "still genuinely
# unknown" ambiguity the COUNTRIES_PROCESSED/NO_IMPACT_COUNTRIES
# split (see is_already_processed()'s own docstring) exists to prevent.
# TC_PIPELINE_GAVE_UP_LOG is a separate, explicit signal instead: the
# scheduler's own discovery query gains one more NOT EXISTS exclusion
# against it (see 04_scheduler_windgust.py), so a given-up cycle stops being
# retried without ever being misrepresented as processed or zero-impact
# anywhere a real consumer (this dashboard's own _wind_ready_at(), a report,
# an alert) might read COUNTRIES_PROCESSED/NO_IMPACT_COUNTRIES.
WINDGUST_MAX_FAILURE_ATTEMPTS = 3


def ensure_windgust_giveup_table(conn) -> None:
    """
    CREATE TABLE IF NOT EXISTS for TC_PIPELINE_GAVE_UP_LOG, extracted as its
    own idempotent call so a scheduler's discovery query (which needs to
    NOT EXISTS-check this table on every real run, even before any give-up
    has ever happened) can guarantee the table exists first. Without this,
    the first discovery query run would fail outright with a real "table
    does not exist" SQL error, since check_and_record_windgust_giveup()
    below only creates it lazily, on an actual give-up. Call this once near the top of each scheduler script,
    before its own discovery query; see 04_scheduler_windgust.py.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS AOTS.TC_ECMWF.TC_PIPELINE_GAVE_UP_LOG (
            STORM_ID VARCHAR,
            FORECAST_TIME TIMESTAMP_TZ,
            ATTEMPT_COUNT NUMBER,
            FAILED_COUNTRIES VARCHAR,
            GAVE_UP_AT TIMESTAMP_TZ,
            LAST_ERROR VARCHAR
        )
    """)
    cur.close()


def check_and_record_windgust_giveup(conn, storm_id: str, forecast_time, failed_countries: list, logger,
                                      error_message: str = None) -> bool:
    """
    Call after logging a genuine wind/gust FAILURE (log_run_complete(...,
    success=False, ...)) for (storm_id, forecast_time). Counts this cycle's
    own real FAILURE rows already in TC_PIPELINE_RUN_LOG; once that count
    reaches WINDGUST_MAX_FAILURE_ATTEMPTS, records a loud, permanent
    TC_PIPELINE_GAVE_UP_LOG row (idempotent: a later call for the same
    cycle after it's already given up just updates ATTEMPT_COUNT,
    FAILED_COUNTRIES, GAVE_UP_AT and LAST_ERROR, never duplicates) and returns True so the caller can log
    the give-up prominently. Returns False while still under the threshold
    (keep retrying, the existing behavior). A new forecast_time is a fresh
    cycle, so this never blacklists a storm/country permanently, only gives
    up on one superseded cycle, same guarantee as
    record_ambient_failure_attempt()'s own equivalent comment.

    Fails OPEN toward "keep retrying" (returns False) on any query error,
    same philosophy as record_ambient_failure_attempt().
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG
            WHERE STORM_ID = %s AND FORECAST_TIME = %s AND STATUS = 'FAILURE'
        """, (storm_id, forecast_time))
        attempt_count = cur.fetchone()[0]
        gave_up = attempt_count >= WINDGUST_MAX_FAILURE_ATTEMPTS
        if gave_up:
            cur.close()
            ensure_windgust_giveup_table(conn)
            cur = conn.cursor()
            gave_up_at_utc = datetime.now(timezone.utc)
            cur.execute("""
                SELECT 1 FROM AOTS.TC_ECMWF.TC_PIPELINE_GAVE_UP_LOG
                WHERE STORM_ID = %s AND FORECAST_TIME = %s
            """, (storm_id, forecast_time))
            already_recorded = cur.fetchone() is not None
            if already_recorded:
                cur.execute("""
                    UPDATE AOTS.TC_ECMWF.TC_PIPELINE_GAVE_UP_LOG
                    SET ATTEMPT_COUNT = %s, FAILED_COUNTRIES = %s, GAVE_UP_AT = %s, LAST_ERROR = %s
                    WHERE STORM_ID = %s AND FORECAST_TIME = %s
                """, (attempt_count, json.dumps(sorted(failed_countries)), gave_up_at_utc,
                      error_message, storm_id, forecast_time))
            else:
                cur.execute("""
                    INSERT INTO AOTS.TC_ECMWF.TC_PIPELINE_GAVE_UP_LOG
                        (STORM_ID, FORECAST_TIME, ATTEMPT_COUNT, FAILED_COUNTRIES, GAVE_UP_AT, LAST_ERROR)
                    SELECT %s, %s, %s, %s, %s, %s
                """, (storm_id, forecast_time, attempt_count, json.dumps(sorted(failed_countries)),
                      gave_up_at_utc, error_message))
            conn.commit()
            logger.error(
                f"GIVING UP on {storm_id} @ {forecast_time} for {sorted(failed_countries)} after "
                f"{attempt_count} FAILURE attempts (limit: "
                f"{WINDGUST_MAX_FAILURE_ATTEMPTS}). The scheduler will stop retrying this cycle. "
                f"NOT marked SUCCESS or NO_IMPACT anywhere; there is genuinely no real wind/gust "
                f"data for these countries at this cycle, and that gap is recorded permanently and "
                f"queryably in TC_PIPELINE_GAVE_UP_LOG until a newer forecast_time cycle supersedes it."
            )
        cur.close()
        return gave_up
    except Exception as e:
        logger.warning(
            f"Could not check/record windgust give-up state for {storm_id} at {forecast_time} "
            f"({e}); defaulting to NOT giving up (keeps this cycle eligible for retry, the safer "
            f"failure mode here)."
        )
        return False


# =============================================================================
# GUST RUN LOG (separate, delayed gust scheduler)
# =============================================================================
# TC_GUST_RUN_LOG is the attempt/retry/give-up log for the separate,
# delayed gust scheduler (databricks/07_scheduler_gust.py), read only for the
# give-up count (check_and_record_gust_giveup()), NOT the discovery
# mechanism for that job (discovery is Blob-marker-based: the wind job drops a
# gust_pending/{storm}_{date}_{country}.marker the moment its own wind
# checkpoint fires, see main_pipeline.py's _write_gust_pending_marker(), and
# the gust job lists that prefix plus a cheap Blob existence check on its own
# output before ever touching Snowflake). Written only when the gust job
# actually attempts real work for a marker (post-idempotency-check), mirroring
# TC_PIPELINE_RUN_LOG's own shape/RUN_ID pattern above so a stale
# IN_PROGRESS lock (see is_already_processed()'s own comment) cannot occur
# here either.


def log_gust_run_start(conn, storm_id: str, forecast_time, countries: list = None) -> str:
    """
    Insert an IN_PROGRESS marker into TC_GUST_RUN_LOG. Mirrors log_run_start()
    above exactly (see its own docstring for the full "why" behind RUN_ID and
    the TIMESTAMP_TZ binding); returns the RUN_ID for this attempt, pass into
    log_gust_run_complete()'s own run_id parameter.
    """
    run_id = str(uuid.uuid4())
    started_at_utc = datetime.now(timezone.utc)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_GUST_RUN_LOG
            (STORM_ID, FORECAST_TIME, STATUS, COUNTRIES_PROCESSED, STARTED_AT, RUN_ID)
        SELECT %s, %s, 'IN_PROGRESS', PARSE_JSON(%s), %s, %s
    """, (storm_id, forecast_time, json.dumps(countries or []), started_at_utc, run_id))
    conn.commit()
    cur.close()
    return run_id


def log_gust_run_complete(conn, storm_id: str, forecast_time, success: bool,
                          countries: list = None, files_written: int = 0,
                          error_message: str = None, started_at=None,
                          no_impact_countries: list = None, run_id: str = None) -> None:
    """
    Insert a SUCCESS or FAILURE completion record into TC_GUST_RUN_LOG.
    Mirrors log_run_complete() above exactly (same started_at naive-vs-aware
    handling, same RUN_ID write, same column shape).
    """
    runtime_seconds = None
    if started_at:
        now = datetime.now(timezone.utc) if started_at.tzinfo is not None else datetime.now()
        runtime_seconds = (now - started_at).total_seconds()
    status = 'SUCCESS' if success else 'FAILURE'
    completed_at_utc = datetime.now(timezone.utc)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_GUST_RUN_LOG
            (STORM_ID, FORECAST_TIME, STATUS, COUNTRIES_PROCESSED, NO_IMPACT_COUNTRIES,
             FILES_WRITTEN, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT, RUNTIME_SECONDS, RUN_ID)
        SELECT %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s, %s, %s, %s, %s, %s
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
        run_id,
    ))
    conn.commit()
    cur.close()


GUST_MAX_FAILURE_ATTEMPTS = 3


def ensure_gust_giveup_table(conn) -> None:
    """CREATE TABLE IF NOT EXISTS for TC_GUST_GAVE_UP_LOG. Same idempotent
    lazy-create pattern as ensure_windgust_giveup_table() above; call once
    near the top of 07_scheduler_gust.py before it processes any marker."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS AOTS.TC_ECMWF.TC_GUST_GAVE_UP_LOG (
            STORM_ID VARCHAR,
            FORECAST_TIME TIMESTAMP_TZ,
            COUNTRY_CODE VARCHAR,
            ATTEMPT_COUNT NUMBER,
            GAVE_UP_AT TIMESTAMP_TZ,
            LAST_ERROR VARCHAR
        )
    """)
    cur.close()


def check_and_record_gust_giveup(conn, storm_id: str, forecast_time, country_code: str, logger,
                                  error_message: str = None) -> bool:
    """
    Call after logging a genuine gust FAILURE for (storm_id, forecast_time)
    that included country_code. Counts this cycle's own real FAILURE rows in
    TC_GUST_RUN_LOG whose COUNTRIES_PROCESSED/request included this country;
    once real attempts reach GUST_MAX_FAILURE_ATTEMPTS, records a permanent
    TC_GUST_GAVE_UP_LOG row and returns True so the caller stops retrying this
    marker (and can delete it) instead of retrying forever. Mirrors
    check_and_record_windgust_giveup()'s own fail-open-toward-retry philosophy
    on any query error.

    Scoped per-country (unlike TC_PIPELINE_GAVE_UP_LOG's whole-cycle scope):
    the gust job's own unit of work is one marker (one country), not a whole
    storm/forecast_time batch, so give-up must not block sibling countries at
    the same forecast_time that are still succeeding.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM AOTS.TC_ECMWF.TC_GUST_RUN_LOG
            WHERE STORM_ID = %s AND FORECAST_TIME = %s AND STATUS = 'FAILURE'
              AND ARRAY_CONTAINS(%s::VARIANT, COUNTRIES_PROCESSED)
        """, (storm_id, forecast_time, country_code))
        attempt_count = cur.fetchone()[0]
        gave_up = attempt_count >= GUST_MAX_FAILURE_ATTEMPTS
        if gave_up:
            cur.close()
            ensure_gust_giveup_table(conn)
            cur = conn.cursor()
            gave_up_at_utc = datetime.now(timezone.utc)
            cur.execute("""
                SELECT 1 FROM AOTS.TC_ECMWF.TC_GUST_GAVE_UP_LOG
                WHERE STORM_ID = %s AND FORECAST_TIME = %s AND COUNTRY_CODE = %s
            """, (storm_id, forecast_time, country_code))
            already_recorded = cur.fetchone() is not None
            if already_recorded:
                cur.execute("""
                    UPDATE AOTS.TC_ECMWF.TC_GUST_GAVE_UP_LOG
                    SET ATTEMPT_COUNT = %s, GAVE_UP_AT = %s, LAST_ERROR = %s
                    WHERE STORM_ID = %s AND FORECAST_TIME = %s AND COUNTRY_CODE = %s
                """, (attempt_count, gave_up_at_utc, error_message, storm_id, forecast_time, country_code))
            else:
                cur.execute("""
                    INSERT INTO AOTS.TC_ECMWF.TC_GUST_GAVE_UP_LOG
                        (STORM_ID, FORECAST_TIME, COUNTRY_CODE, ATTEMPT_COUNT, GAVE_UP_AT, LAST_ERROR)
                    SELECT %s, %s, %s, %s, %s, %s
                """, (storm_id, forecast_time, country_code, attempt_count, gave_up_at_utc, error_message))
            conn.commit()
            logger.error(
                f"GIVING UP on gust for {storm_id} @ {forecast_time} / {country_code} after "
                f"{attempt_count} FAILURE attempts (limit: "
                f"{GUST_MAX_FAILURE_ATTEMPTS}). This marker will not be retried further."
            )
        cur.close()
        return gave_up
    except Exception as e:
        logger.warning(
            f"Could not check/record gust give-up state for {storm_id} at {forecast_time}/{country_code} "
            f"({e}); defaulting to NOT giving up (keeps this marker eligible for retry)."
        )
        return False


# =============================================================================
# AMBIENT (precip/river-flood) IDEMPOTENCY GATE
# =============================================================================
# Precip/river-flood have no equivalent of TC_PIPELINE_RUN_LOG's (storm_id,
# forecast_time) dedup: without it they'd reprocess whichever forecast cycle
# is "latest" every scheduled run, even though MET_FORECASTS only
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
# no limit, the scheduler (every scheduled poll) redoes the full country/tier
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
# CANDIDATE COUNTRY CACHE
# =============================================================================
# The wind/gust discovery query needs, per (track, cycle), the active countries
# within 1,500 km of any combined envelope. That ST_DWITHIN join is expensive
# and its answer only changes when a cycle's envelopes change or the set of
# country boundaries changes. AOTS.TC_ECMWF.CANDIDATE_COUNTRY_CACHE stores the
# answer per (track, cycle, envelope row count). discover_candidate_cycles()
# runs the spatial join only for cycles without an entry for their current
# envelope row count and reads everything else from the cache.
#
# The cache never decides correctness. Any problem (table missing, statement
# failure, an uncovered cycle, a changed country signature) logs a warning and
# runs the live join, so the result is always the one the live join returns.

CANDIDATE_CACHE_TABLE = "AOTS.TC_ECMWF.CANDIDATE_COUNTRY_CACHE"
CANDIDATE_PAIRS_PLACEHOLDER = "__CANDIDATE_PAIRS__"
CANDIDATE_CACHE_RETENTION_DAYS = 7

# One reserved row records a signature of the country boundaries the cache was
# computed against. Its ENV_ROWS holds the signature and its COUNTRY_CODE is
# NULL, so no reader treats it as a country.
_CANDIDATE_SIGNATURE_TRACK = "__COUNTRY_SIGNATURE__"
_CANDIDATE_SIGNATURE_TIME = "1970-01-01 00:00:00"

_CANDIDATE_WINDOW = "te.FORECAST_TIME >= DATEADD('day', -2, SYSDATE())"

# SYSDATE(), not CURRENT_TIMESTAMP(): FORECAST_TIME is TIMESTAMP_NTZ, so the
# window needs a naive UTC current time. CURRENT_TIMESTAMP() is TIMESTAMP_LTZ and would
# be reinterpreted with the session TIMEZONE when compared to an NTZ column.
CANDIDATE_PAIRS_LIVE_SQL = f"""
        SELECT DISTINCT te.TRACK_ID, te.FORECAST_TIME, pc.COUNTRY_CODE
        FROM   TC_ENVELOPES_COMBINED te
        JOIN   PIPELINE_COUNTRIES pc
               ON ST_DWITHIN(pc.COUNTRY_BOUNDARY, te.ENVELOPE_REGION, 1500000)
        WHERE  pc.ACTIVE = TRUE
          AND  {_CANDIDATE_WINDOW}
"""

# ACTIVE is applied here at read time, so activating or deactivating a country
# takes effect immediately without recomputing any geometry.
CANDIDATE_PAIRS_CACHED_SQL = f"""
        SELECT DISTINCT c.TRACK_ID, c.FORECAST_TIME, c.COUNTRY_CODE
        FROM   {CANDIDATE_CACHE_TABLE} c
        JOIN   (SELECT te.TRACK_ID, te.FORECAST_TIME, COUNT(*) AS N
                FROM   TC_ENVELOPES_COMBINED te
                WHERE  {_CANDIDATE_WINDOW}
                GROUP BY 1, 2) t
               ON  c.TRACK_ID = t.TRACK_ID AND c.FORECAST_TIME = t.FORECAST_TIME AND c.ENV_ROWS = t.N
        JOIN   PIPELINE_COUNTRIES pc ON pc.COUNTRY_CODE = c.COUNTRY_CODE
        WHERE  pc.ACTIVE = TRUE AND c.COUNTRY_CODE IS NOT NULL
"""

_CANDIDATE_SIGNATURE_SQL = """
    SELECT HASH_AGG(COUNTRY_CODE, ST_NPOINTS(COUNTRY_BOUNDARY))
    FROM   PIPELINE_COUNTRIES
    WHERE  COUNTRY_BOUNDARY IS NOT NULL
"""

# Cycles with no entry for their current envelope row count are evaluated; a
# cycle with no country in range is stored with COUNTRY_CODE NULL.
_CANDIDATE_FILL_SQL = f"""
    INSERT INTO {CANDIDATE_CACHE_TABLE} (TRACK_ID, FORECAST_TIME, ENV_ROWS, COUNTRY_CODE, COMPUTED_AT)
    WITH todo AS (
        SELECT te.TRACK_ID, te.FORECAST_TIME, COUNT(*) AS N
        FROM   TC_ENVELOPES_COMBINED te
        WHERE  {_CANDIDATE_WINDOW}
        GROUP BY 1, 2),
    new_pairs AS (
        SELECT t.* FROM todo t WHERE NOT EXISTS (
            SELECT 1 FROM {CANDIDATE_CACHE_TABLE} c
            WHERE c.TRACK_ID = t.TRACK_ID AND c.FORECAST_TIME = t.FORECAST_TIME AND c.ENV_ROWS = t.N)),
    hits AS (
        SELECT DISTINCT te.TRACK_ID, te.FORECAST_TIME, pc.COUNTRY_CODE
        FROM   TC_ENVELOPES_COMBINED te
        JOIN   new_pairs np ON te.TRACK_ID = np.TRACK_ID AND te.FORECAST_TIME = np.FORECAST_TIME
        JOIN   PIPELINE_COUNTRIES pc ON ST_DWITHIN(pc.COUNTRY_BOUNDARY, te.ENVELOPE_REGION, 1500000)
        WHERE  pc.COUNTRY_BOUNDARY IS NOT NULL)
    SELECT np.TRACK_ID, np.FORECAST_TIME, np.N, h.COUNTRY_CODE, SYSDATE()
    FROM   new_pairs np
    LEFT JOIN hits h ON h.TRACK_ID = np.TRACK_ID AND h.FORECAST_TIME = np.FORECAST_TIME
"""

# Number of cycles in the window that have no cache entry for their current
# envelope row count. Zero means the cache covers every cycle the read sees.
_CANDIDATE_UNCOVERED_SQL = f"""
    SELECT COUNT(*)
    FROM   (SELECT te.TRACK_ID, te.FORECAST_TIME, COUNT(*) AS N
            FROM   TC_ENVELOPES_COMBINED te
            WHERE  {_CANDIDATE_WINDOW}
            GROUP BY 1, 2) t
    WHERE  NOT EXISTS (
               SELECT 1 FROM {CANDIDATE_CACHE_TABLE} c
               WHERE c.TRACK_ID = t.TRACK_ID AND c.FORECAST_TIME = t.FORECAST_TIME AND c.ENV_ROWS = t.N)
"""


def _candidate_country_signature(cur) -> int:
    cur.execute(_CANDIDATE_SIGNATURE_SQL)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _stored_candidate_signature(cur):
    cur.execute(f"SELECT MAX(ENV_ROWS) FROM {CANDIDATE_CACHE_TABLE} WHERE TRACK_ID = %s",
                (_CANDIDATE_SIGNATURE_TRACK,))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def refresh_candidate_country_cache(cur) -> int:
    """Bring the cache up to date and return the country signature it matches.

    Drops every entry when the country boundaries differ from those the cache
    was computed against, then adds entries for cycles without one. Raises on
    any failure; the caller falls back to the live join."""
    signature = _candidate_country_signature(cur)
    if _stored_candidate_signature(cur) != signature:
        cur.execute(f"DELETE FROM {CANDIDATE_CACHE_TABLE}")
        cur.execute(
            f"INSERT INTO {CANDIDATE_CACHE_TABLE} (TRACK_ID, FORECAST_TIME, ENV_ROWS, COUNTRY_CODE, COMPUTED_AT) "
            f"SELECT %s, %s::TIMESTAMP_NTZ, %s, NULL, SYSDATE()",
            (_CANDIDATE_SIGNATURE_TRACK, _CANDIDATE_SIGNATURE_TIME, signature))
    cur.execute(_CANDIDATE_FILL_SQL)
    return signature


def _expire_candidate_cache(cur, logger) -> None:
    try:
        cur.execute(
            f"DELETE FROM {CANDIDATE_CACHE_TABLE} WHERE TRACK_ID <> %s "
            f"AND FORECAST_TIME < DATEADD('day', -{CANDIDATE_CACHE_RETENTION_DAYS}, SYSDATE())",
            (_CANDIDATE_SIGNATURE_TRACK,))
    except Exception as e:
        logger.warning(f"Could not remove expired candidate country cache rows: {e}")


def discover_candidate_cycles(cur, sql_template: str, logger) -> list:
    """Run a discovery query whose candidate (track, cycle, country) rows come
    from the cache, falling back to the live spatial join on any problem.

    sql_template contains CANDIDATE_PAIRS_PLACEHOLDER where a SELECT returning
    TRACK_ID, FORECAST_TIME and COUNTRY_CODE belongs. Returns the fetched rows.
    The cache is checked again after the cached query ran, so a cycle whose
    envelopes changed in between is never silently missing from the result."""
    if CANDIDATE_PAIRS_PLACEHOLDER not in sql_template:
        raise ValueError("sql_template lacks the candidate pairs placeholder")
    try:
        signature = refresh_candidate_country_cache(cur)
        cur.execute(sql_template.replace(CANDIDATE_PAIRS_PLACEHOLDER, CANDIDATE_PAIRS_CACHED_SQL))
        rows = cur.fetchall()
        cur.execute(_CANDIDATE_UNCOVERED_SQL)
        uncovered = cur.fetchone()[0]
        if uncovered == 0 and _stored_candidate_signature(cur) == signature == _candidate_country_signature(cur):
            _expire_candidate_cache(cur, logger)
            return rows
        logger.warning(
            f"Candidate country cache disagrees with the live data ({uncovered} uncovered cycle(s) or a "
            f"changed country signature); running the live spatial join instead.")
    except Exception as e:
        logger.warning(f"Candidate country cache unavailable ({e}); running the live spatial join instead.")
    cur.execute(sql_template.replace(CANDIDATE_PAIRS_PLACEHOLDER, CANDIDATE_PAIRS_LIVE_SQL))
    return cur.fetchall()


# =============================================================================
# COMPLETION SIGNAL
# =============================================================================

# Directory stems under geodb/aos_views/ that each hazard family writes, one
# directory per (stem + suffix). The trailing slash on a refresh subpath is
# what keeps "track_views/" from also covering "track_views_gust/".
AOS_VIEW_STEMS = (
    "track_views", "track_tile_bitmask_views", "admin_views", "mercator_views",
    "school_views", "hc_views", "shelter_views", "wash_views",
)
AOS_VIEW_FAMILY_SUFFIXES = {
    "wind": ("",),
    "gust": ("_gust",),
    "precip": ("_precip", "_precipratio"),
    "river": ("_river",),
}


def blob_refresh_subpaths(families) -> list:
    """Stage subpaths holding the files written by the given hazard families.

    families is an iterable of keys of AOS_VIEW_FAMILY_SUFFIXES. An unknown key
    raises, so a typo can never silently narrow a refresh."""
    subpaths = []
    for family in families:
        if family not in AOS_VIEW_FAMILY_SUFFIXES:
            raise ValueError(f"Unknown hazard family {family!r}; expected one of {sorted(AOS_VIEW_FAMILY_SUFFIXES)}")
        for suffix in AOS_VIEW_FAMILY_SUFFIXES[family]:
            for stem in AOS_VIEW_STEMS:
                subpaths.append(f"geodb/aos_views/{stem}{suffix}/")
    return subpaths


def signal_pipeline_complete(conn, storm_ids: list, countries: list, files_written: int, runtime_seconds: int = None,
                             refresh_families=None):
    """
    Insert a batch-completion record into TC_PIPELINE_COMPLETE_LOG.
    This triggers the stream-based refresh of *_MAT tables in Snowflake.
    Called when a scheduler has written at least one file (storm, precip or river).

    refresh_families optionally scopes the BLOB-mode stage refresh to the
    hazard families this run wrote (any of "wind", "gust", "precip", "river").
    Left as None, the whole stage is refreshed.
    """
    cur = conn.cursor()
    # Force the real stage directory table this run actually wrote through to
    # sync before signalling completion. Without this, AOTS_ANALYSIS_FILE_
    # STREAM / AOTS_ANALYSIS_BLOB_FILE_STREAM may not yet reflect newly
    # written files when the refresh task fires, causing a silent missed
    # refresh. Only the one real stage the CURRENT process's own DATA_
    # PIPELINE_DB mode actually writes through gets refreshed, read directly
    # from this same process's own environment rather than trusted to a
    # caller supplied value: BLOB mode (every Databricks hazard scheduler
    # today) writes to Azure Blob behind AOTS_ANALYSIS_BLOB and never
    # touches AOTS_ANALYSIS at all; SNOWFLAKE mode (the manual update_storms
    # CLI path, main_pipeline.py's own snowflake_mode gate already only
    # calls this function at all under that mode) writes through AOTS_
    # ANALYSIS and never touches AOTS_ANALYSIS_BLOB. A genuinely unrecognized
    # or LOCAL mode, which this function is not expected to be called under
    # in practice, still refreshes both rather than guessing which one, the
    # same safe behaviour this had before either real mode was distinguished.
    data_pipeline_db = os.environ.get("DATA_PIPELINE_DB", "LOCAL").strip().upper()
    if data_pipeline_db == "BLOB":
        if refresh_families:
            # Scoped to the directories of the hazard families this run wrote,
            # so the cost does not grow with the size of the whole container.
            # Files outside these directories are picked up by the periodic
            # unscoped refresh task. Default (None) keeps the unscoped refresh.
            for subpath in blob_refresh_subpaths(refresh_families):
                cur.execute(f"ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS_BLOB REFRESH SUBPATH = '{subpath}'")
        else:
            cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS_BLOB REFRESH")
    elif data_pipeline_db == "SNOWFLAKE":
        cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS REFRESH")
    else:
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


# =============================================================================
# DIRECT MAT REFRESH TRIGGER
# =============================================================================
# trigger_mat_refresh_if_due() below calls REFRESH_MATERIALIZED_VIEWS()
# directly from each scheduler the moment it has genuinely new data, rather
# than relying solely on TRIGGER_REFRESH_TASK (ORCHESTRATION repo)'s poll of
# TC_PIPELINE_COMPLETE_STREAM (fed by signal_pipeline_complete() above) on a
# timer: DATAPIPELINE's own jobs write continuously enough during active
# season that the stream's own WHEN gate is true on ~99% of real checks, so
# polling on a timer adds latency without adding real protection over calling
# directly. TRIGGER_REFRESH_TASK remains as a
# crash-recovery-only fallback (see its own comment in 04_tasks.sql,
# ORCHESTRATION repo) for a Databricks job that crashes before reaching this
# call.

MAT_REFRESH_DEBOUNCE_MINUTES = 20

# Source suffix of the future-dated MAT_REFRESH_TRIGGER_LOG row that the table
# rebuild tools write to hold every refresh trigger. A trailing trigger never
# waits for, and never fires on top of, such a row.
MAT_REFRESH_HOLD_SOURCE_SUFFIX = "maintenance_hold"

# Extra seconds a trailing trigger waits past the end of the debounce window,
# per scheduler source. Two schedulers that were both debounced by the same
# earlier trigger would otherwise wake in the same instant; the offsets make
# the first one claim the slot and the others see it and stand down.
MAT_REFRESH_TRAILING_STAGGER_SECONDS = {"windgust": 0, "precip": 30, "river": 60}


def ensure_mat_refresh_trigger_log(conn) -> None:
    """CREATE TABLE IF NOT EXISTS for MAT_REFRESH_TRIGGER_LOG, same lazy-create
    pattern as ensure_windgust_giveup_table() above. Also declared in
    ORCHESTRATION's 09_orchestration/01_log_tables.sql for documentation
    consistency with every other table there; this call is what actually
    guarantees it exists on a fresh environment that hasn't run that script."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS AOTS.TC_ECMWF.MAT_REFRESH_TRIGGER_LOG (
            TRIGGERED_AT TIMESTAMP_NTZ,
            SOURCE VARCHAR
        )
    """)
    cur.close()


def trigger_mat_refresh_if_due(conn, source: str, logger, trailing: bool = False) -> bool:
    """
    Call REFRESH_MATERIALIZED_VIEWS() directly, right after this scheduler has
    genuinely finished writing new real data (call this AFTER
    signal_pipeline_complete() above, using the same connection, once per run;
    never inside a per-storm/per-country loop, or every individual
    completion would fire its own refresh).

    Debounced against MAT_REFRESH_TRIGGER_LOG: if another scheduler already
    triggered a refresh within MAT_REFRESH_DEBOUNCE_MINUTES (currently 20),
    this call is skipped rather than firing a second, largely-redundant
    ~196s refresh. The debounce window is sized for the completion spacing
    seen in TC_PIPELINE_COMPLETE_LOG: wind/gust and precip run on the SAME
    16x/day Databricks cron, and their completions typically stem from the
    same ECMWF cycle (both are driven off the same upstream forecast data).
    The common cluster of same-cycle pairs lands just 6-14 SECONDS apart (a
    wind/gust storm completion immediately followed by an ambient
    precip/river one), with smaller clusters at 11.9-18.2 minutes and at
    18-30 minutes; pairs 30+ minutes apart look like genuinely separate
    events. 20 minutes covers all of these clusters.

    CRITICAL ORDERING, do not change without re-reading this comment: the
    debounce slot is claimed (the INSERT below) BEFORE calling
    REFRESH_MATERIALIZED_VIEWS(), not after. Claiming the slot only once the
    ~196s CALL returns would leave a real ~196-second race window, not a
    sub-second one: since real same-cycle completions routinely land just
    6-14 SECONDS apart (see above), a second scheduler's own SELECT
    MAX(TRIGGERED_AT) could run during those 196 seconds, see nothing
    recent, and ALSO proceed to CALL REFRESH_MATERIALIZED_VIEWS()
    concurrently: two genuinely
    concurrent calls risking real transaction conflicts inside that
    procedure's own per-table retry/dead-letter bookkeeping, not just
    wasted warehouse spend.
    Claiming the slot first shrinks the real race to a genuine sub-second
    SELECT-then-INSERT gap, which two independently-scheduled Databricks
    jobs are not going to hit.

    Tradeoff of claiming first (accepted, not hidden): if the CALL itself
    then fails, the slot is still claimed, so no OTHER scheduler will retry
    the refresh for the rest of this debounce window. This is acceptable
    because the crash-recovery TRIGGER_REFRESH_TASK fallback still exists
    independently of this whole mechanism and will pick up any genuinely
    missed refresh on its own (much longer) cadence regardless.

    Skipping (debounced) is SAFE, not lossy, for the same reason: REFRESH_
    MATERIALIZED_VIEWS() does a full catch-up pass over whatever is
    currently pending (via AOTS_ANALYSIS_BLOB_FILE_STREAM /
    MAT_REFRESH_PENDING_FILES), not work scoped narrowly to this specific
    caller's own just-written files: a skipped trigger's real data simply
    gets picked up by whichever refresh call actually runs next (another
    hazard's own direct trigger moments later, or the crash-recovery
    fallback), never silently dropped.

    With trailing=True a debounced call is not dropped: it waits for the
    debounce window opened by the earlier trigger to end (at most
    MAT_REFRESH_DEBOUNCE_MINUTES) and then triggers once, provided no other
    trigger or maintenance hold appeared meanwhile and an unconsumed
    completion or file stream still has data. The refresh that was already
    running when this call arrived may have read its streams before this
    run's files landed, so the trailing trigger is what guarantees they are
    loaded within one window instead of at the next periodic task tick. A hold
    row, or any wait longer than one window, makes the call return False
    without sleeping.

    Runs on AOTS_REFRESH_WH explicitly (not whatever warehouse this
    connection's session happens to be on), matching the warehouse
    TRIGGER_REFRESH_TASK itself uses, keeping this direct-trigger path's real
    cost attributed to the same place as the task-based path it replaces.

    Never raises: a refresh-trigger failure must not fail the calling
    scheduler's own real work, which has already completed successfully by
    the time this is called. The crash-recovery fallback task still covers
    this case if the direct trigger itself fails.

    Returns True if a refresh was actually triggered (immediately or by the
    trailing path), False if skipped (debounced, held, nothing pending) or if
    the attempt itself failed.
    """
    try:
        ensure_mat_refresh_trigger_log(conn)
        cur = conn.cursor()
        last, last_source = _latest_mat_refresh_trigger(cur)
        # Naive UTC, matching MAT_REFRESH_TRIGGER_LOG.TRIGGERED_AT's own
        # TIMESTAMP_NTZ type (same "bind an already-UTC naive datetime,
        # never a TIMESTAMP_TZ-style tz-aware one, into an NTZ column"
        # pattern as log_ambient_forecast_processed()'s PROCESSED_AT above).
        now_utc_naive = _utc_now_naive()
        if last is not None and (now_utc_naive - last).total_seconds() < MAT_REFRESH_DEBOUNCE_MINUTES * 60:
            elapsed = (now_utc_naive - last).total_seconds()
            logger.info(
                f"Skipping direct MAT refresh trigger from {source}: another trigger fired "
                f"{elapsed:.0f}s ago (within the {MAT_REFRESH_DEBOUNCE_MINUTES}-min debounce "
                f"window). "
                + ("A trailing trigger will be attempted when the window ends."
                   if trailing else
                   "This source's own new data will be picked up by that refresh or the "
                   "next one, not lost.")
            )
            cur.close()
            if trailing:
                return _trailing_mat_refresh(conn, source, logger, last, last_source)
            return False
        return _claim_and_call_refresh(conn, cur, source, now_utc_naive, logger)
    except Exception as e:
        logger.warning(
            f"Direct MAT refresh trigger from {source} failed ({e}); not fatal, the "
            f"crash-recovery TRIGGER_REFRESH_TASK fallback will still pick this up. Note: if the "
            f"debounce slot was already claimed before this failure, no other scheduler will retry "
            f"until the debounce window elapses (an accepted tradeoff, see this function's own "
            f"docstring)."
        )
        return False


def _utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _latest_mat_refresh_trigger(cur):
    """Return (TRIGGERED_AT, SOURCE) of the newest MAT_REFRESH_TRIGGER_LOG row,
    or (None, None) when the table is empty. A maintenance hold row is
    future-dated, so it is the newest row for as long as it applies."""
    cur.execute(
        "SELECT TRIGGERED_AT, SOURCE FROM AOTS.TC_ECMWF.MAT_REFRESH_TRIGGER_LOG "
        "ORDER BY TRIGGERED_AT DESC LIMIT 1"
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None, None
    return row[0], row[1]


def _claim_and_call_refresh(conn, cur, source, now_utc_naive, logger) -> bool:
    """Claim the debounce slot, then call REFRESH_MATERIALIZED_VIEWS(). The claim
    comes first for the reason given under CRITICAL ORDERING in
    trigger_mat_refresh_if_due()."""
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.MAT_REFRESH_TRIGGER_LOG (TRIGGERED_AT, SOURCE)
        SELECT %s, %s
    """, (now_utc_naive, source))
    conn.commit()
    cur.execute("USE WAREHOUSE AOTS_REFRESH_WH")
    cur.execute("CALL AOTS.TC_ECMWF.REFRESH_MATERIALIZED_VIEWS()")
    result_row = cur.fetchone()
    cur.close()
    logger.info(
        f"Direct MAT refresh triggered by {source}: "
        f"{result_row[0] if result_row else '(no return value)'}"
    )
    return True


def _mat_streams_have_data(cur) -> bool:
    """True when the completion stream or the Blob file stream holds unconsumed
    rows. Fails open (True) when the check itself cannot run, so a trailing
    trigger is never suppressed by a monitoring error."""
    try:
        cur.execute(
            "SELECT SYSTEM$STREAM_HAS_DATA('AOTS.TC_ECMWF.TC_PIPELINE_COMPLETE_STREAM') "
            "OR SYSTEM$STREAM_HAS_DATA('AOTS.TC_ECMWF.AOTS_ANALYSIS_BLOB_FILE_STREAM')"
        )
        row = cur.fetchone()
        return True if row is None else bool(row[0])
    except Exception:
        return True


def _trailing_mat_refresh(conn, source, logger, last, last_source) -> bool:
    """Trailing leg of trigger_mat_refresh_if_due(): sleep until the debounce
    window that blocked this call has ended, then trigger once if the
    situation is unchanged. Never raises."""
    try:
        window = MAT_REFRESH_DEBOUNCE_MINUTES * 60
        now = _utc_now_naive()
        if last > now or (last_source or "").endswith(MAT_REFRESH_HOLD_SOURCE_SUFFIX):
            logger.info(f"Trailing MAT refresh from {source} skipped: a maintenance hold is active.")
            return False
        wait = (last - now).total_seconds() + window + MAT_REFRESH_TRAILING_STAGGER_SECONDS.get(source, 0)
        if wait > window + 120:
            logger.info(f"Trailing MAT refresh from {source} skipped: wait of {wait:.0f}s exceeds one window.")
            return False
        logger.info(f"Trailing MAT refresh from {source}: waiting {max(wait, 0):.0f}s for the debounce window to end.")
        if wait > 0:
            time.sleep(wait)
        cur = conn.cursor()
        newest, _ = _latest_mat_refresh_trigger(cur)
        now = _utc_now_naive()
        if newest is not None and (newest != last or (now - newest).total_seconds() < window):
            logger.info(f"Trailing MAT refresh from {source} skipped: another trigger or hold appeared meanwhile.")
            cur.close()
            return False
        if not _mat_streams_have_data(cur):
            logger.info(f"Trailing MAT refresh from {source} skipped: nothing pending in the refresh streams.")
            cur.close()
            return False
        return _claim_and_call_refresh(conn, cur, f"{source}_trailing", now, logger)
    except Exception as e:
        logger.warning(
            f"Trailing MAT refresh from {source} failed ({e}); not fatal, the "
            f"TRIGGER_REFRESH_TASK fallback will still pick this up."
        )
        return False
