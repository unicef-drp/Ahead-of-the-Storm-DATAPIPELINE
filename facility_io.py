#!/usr/bin/env python3
"""
Facility I/O Module

HealthSites/GIGA/OSM facility data fetching (schools, health centers, shelters,
WASH infrastructure) and the per-facility-type location-cache/impact-view
save/load/exist persistence helpers. Split out of impact_analysis.py, which
imports several of these functions back (real internal dependencies, e.g.
create_views_from_envelopes_in_country() fetches/saves facilities as part of
its own per-country orchestration).

Module structure:
    OSM/HEALTHSITES LOCATION-TYPE CONSTANTS: _HEALTHSITES_BBOX_OVERRIDES,
        HC_FACILITY_TYPES, SHELTER_LOCATION_TYPES, WASH_LOCATION_TYPES
    DATA FETCHING AND CACHING: fetch_schools, fetch_health_centers,
        fetch_shelters, fetch_wash, and their shared retry/timeout helpers
    FACILITY VIEW PERSISTENCE: save/load/exist for each facility type's
        location cache and per-storm impact views
"""

import logging
import os
import time
import threading
import queue
import geopandas as gpd
import pandas as pd

from gigaspatial.handlers import GigaSchoolLocationFetcher
from gigaspatial.handlers.healthsites import HealthSitesFetcher
from gigaspatial.core.io.readers import read_dataset
from gigaspatial.core.io.writers import write_dataset

# Shared state/helpers with no dependency of their own on this module or its
# siblings - see pipeline_shared.py's own docstring for why this exists.
from pipeline_shared import (
    data_store,
    ROOT_DATA_DIR,
    VIEWS_DIR,
    _load_custom_points_csv,
)

logger = logging.getLogger(__name__)

# HealthSites.io's own `country` filter doesn't recognize every active country's
# name in any spelling/case/ISO-code form (confirmed via direct API testing:
# 'Aruba'/'aruba'/'AW'/'ABW' and 'Curacao'/'Curaçao'/'Netherlands Antilles' all
# return 400 "not found or not a country" - a gap in HealthSites' own country
# list, not a name-resolution bug here). A bounding-box (`extent`) query reaches
# the same facility data where a country-name query can't. Bounds come from each
# country's PIPELINE_COUNTRIES.COUNTRY_BOUNDARY (ST_XMIN/YMIN/XMAX/YMAX in
# Snowflake), padded 0.05 degrees to avoid clipping coastal facilities. Keyed by
# ISO3; extend if another country hits the same 400 on every name/code variant.
# Each value is a list of bboxes so a disjoint territory like BES can use several
# tight boxes instead of one spanning empty ocean in between.
_HEALTHSITES_BBOX_OVERRIDES = {
    "ABW": [(-70.1138, 12.3618, -69.8155, 12.6734)],  # Aruba
    "CUW": [(-69.2127, 11.9283, -68.5899, 12.4427)],  # Curaçao
    "BHS": [(-80.5259, 20.8631, -72.6624, 27.3229)],  # Bahamas (archipelago; also
    # resolvable by name as "The Bahamas", but HealthSitesFetcher re-runs any
    # country string through pycountry, which does not recognize that name
    # either; the bbox path avoids that second lookup entirely).
    "BES": [  # Bonaire, Sint Eustatius and Saba: two disjoint clusters ~500km
        # apart (see _BES_GEOREPO_UCODES in country_boundaries.py); bounds taken from the real
        # per-island GeoRepo geometries, not PIPELINE_COUNTRIES.COUNTRY_BOUNDARY
        # (whose own bbox spans the huge gap between the two clusters).
        (-68.4709, 11.9747, -68.1455, 12.3622),   # Bonaire
        (-63.3085, 17.4146, -62.8959, 17.7003),   # Sint Eustatius + Saba
    ],
}

#==============================================================================
# HEALTH FACILITY TYPES
#==============================================================================
# Health facility types included in impact analysis, keyed by OSM/HealthSites column.
# Format mirrors SHELTER_LOCATION_TYPES/WASH_LOCATION_TYPES: {column: [values]} so
# other tag keys (e.g. 'healthcare') could be added alongside 'amenity' later.
#
# The HealthSites.io API (https://healthsites.io/api/v3/) returns an `amenity`
# column with documented values clinic/doctors/hospital/dentist/pharmacy.
#
# Included: hospital (inpatient/emergency care, most critical for mass-casualty
# response), clinic (outpatient primary-care backbone), doctors (individual
# practitioner offices, lower capacity but relevant for community care).
# Excluded: dentist (not cyclone-relevant), pharmacy (supply chain, not direct
# emergency care).
#
# The raw (unfiltered) API response is stored in the location cache so changing
# this dict takes effect on the next update run without re-fetching.

HC_FACILITY_TYPES = {
    'amenity': ['hospital', 'clinic', 'doctors'],
}

#==============================================================================
# OSM LOCATION TYPES
#==============================================================================
# OSM location types for emergency shelters, queried via OSMLocationFetcher
# (Overpass API). Only social_facility=shelter is included: it specifically
# denotes dedicated emergency/disaster shelters (refugee shelters, evacuation
# centres, disaster relief sites), tagged on amenity=social_facility nodes/ways.
#
# Excluded: amenity=shelter (too broad - bus stop/hiking/picnic shelters, market
# covers); emergency=assembly_point (fire evacuation points only, not shelters).

SHELTER_LOCATION_TYPES = {
    'social_facility': ['shelter'],
}

#==============================================================================
# WASH LOCATION TYPES
#==============================================================================
# OSM location types for humanitarian WASH infrastructure, queried via
# OSMLocationFetcher (Overpass API). Covers the managed water/sanitation chain
# relevant to cyclone response (HOT wiki: https://wiki.openstreetmap.org/wiki/
# Humanitarian_OSM_Tags/WASH documents amenity=water_point/toilets as primary
# tags; the man_made=* tags extend to upstream infrastructure).
#
# Included: amenity=drinking_water/water_point/toilets/shower (public access
# points, latrines, hygiene); man_made=water_well/water_tap/water_works/
# pumping_station/wastewater_treatment_plant (managed supply and sanitation
# infrastructure).
# Excluded: natural=spring/water (unmanaged, unpredictable post-cyclone
# reliability); man_made=storage_tank (too broad - fuel/agricultural/industrial
# tanks).

WASH_LOCATION_TYPES = {
    'amenity': ['drinking_water', 'water_point', 'toilets', 'shower'],
    'man_made': ['water_well', 'water_tap', 'water_works', 'pumping_station', 'wastewater_treatment_plant'],
}

#==============================================================================
# SCHOOL EDUCATION LEVELS
#==============================================================================
# Education levels from the GIGA School Location API
# (https://uni-ooi-giga-maps-service.azurewebsites.net): 'Pre-Primary',
# 'Primary', 'Secondary', 'Unknown'. Other values may exist and are retained.



# =============================================================================
# DATA FETCHING AND CACHING FUNCTIONS
# =============================================================================


# Per-process, run-scoped negative cache: remembers which (country,
# facility_type) fetches already exhausted _fetch_with_retry()'s own 3 attempts
# and failed this run, so a later call for the same country in the SAME process
# skips straight to the empty-result fallback instead of repeating the full
# retry-with-backoff cost. The redundancy this solves is cross-phase: one
# invocation processes wind/gust, then precip, then river-flood (once per
# country each), and each phase independently fetches every facility type.
# Cleared on every new process (both Databricks and SPCS get a fresh process
# per scheduled run), so a new pipeline invocation still gets a fresh retry.
# Within one run, an early transient failure can suppress a later retry that
# might have succeeded once the issue cleared; both paths converge on the same
# degraded output once retries are exhausted, so nothing but that rare
# late-run recovery is given up. Scoped to real fetch FAILURES only, not to a
# genuinely empty-but-successful result (already fast, and treated by
# _fetch_with_retry as a real "no facilities here" signal).
_FACILITY_FETCH_FAILURE_CACHE = set()

# HTTP status codes where retrying with the same request can never succeed: the
# server rejected the request itself (bad/missing credentials, no such resource),
# not a transient condition. Deliberately narrow: only codes that are genuinely
# never fixed by retrying identically. 429 (rate limit) and 5xx are NOT included
# here, those are exactly the transient case _fetch_with_retry exists for.
_NON_RETRIABLE_HTTP_STATUS_CODES = {401, 403, 404}

# OSMLocationFetcher's own defaults (timeout=600, max_retries=3, retry_delay=5) suit
# --type initialize/patch, where a missing cache is worth a patient wait
# (fetch_locations() makes 2 sequential Overpass calls, nodes/relations then ways,
# each retried up to max_retries times: worst case ~3 hours combined with
# _fetch_with_retry's outer attempts layered on top). --type update runs every
# forecast cycle, where a cache miss almost always means the country was never
# onboarded or an earlier attempt already failed, not worth hours of retrying:
# fast_fail callers get a single short Overpass attempt (~2 minutes worst case for
# both calls) so one slow/unreachable country never delays the rest of the loop.
_OSM_FAST_FAIL_KWARGS = {'timeout': 30, 'max_retries': 1, 'retry_delay': 5}

# fetch_health_centers()/fetch_schools() have no equivalent constructor kwargs to
# shorten (HealthSitesFetcher/GigaSchoolLocationFetcher expose no timeout/
# max_retries), and their worst case isn't bounded by a per-request timeout anyway:
# HealthSitesFetcher resolves a country's OSM English name internally via
# OSMLocationFetcher.get_osm_countries() before touching HealthSites.io's own API
# (why a "HealthSites.io fetch" failure can cite overpass-api.de), with its own
# nested 3-attempt retry; both fetchers also paginate with no page/time cap. A
# single wall-clock cap around the whole call bounds all of that uniformly,
# matching _with_hard_timeout() in data_store_utils.py for Blob operations.
_FACILITY_FETCH_FAST_FAIL_TIMEOUT_S = 90


def _with_call_timeout(fetch_fn, timeout_s):
    """
    Run fetch_fn() with a hard wall-clock cap, raising TimeoutError if it doesn't return
    in time. Same daemon-thread-plus-queue pattern as _with_hard_timeout() in
    data_store_utils.py, applied here per-call rather than by monkeypatching a class: a
    plain Python-level timeout parameter passed into an HTTP client can still be exceeded
    by a genuinely stuck/black-holed connection, since nothing forces that thread to give
    up. The abandoned worker thread is leaked (daemon=True), not joined, matching the same
    accepted tradeoff documented there: letting the caller move on is preferable to
    blocking the whole per-country loop indefinitely.
    """
    result_q = queue.Queue(maxsize=1)

    def _run():
        try:
            result_q.put(('ok', fetch_fn()))
        except Exception as e:
            result_q.put(('error', e))

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    try:
        status, value = result_q.get(timeout=timeout_s)
    except queue.Empty:
        raise TimeoutError(f"fetch exceeded {timeout_s}s (fast_fail)")
    if status == 'error':
        raise value
    return value


def _non_retriable_status_code(exc):
    """
    Returns the HTTP status code if exc looks like an HTTP error carrying one
    in _NON_RETRIABLE_HTTP_STATUS_CODES, else None. Duck-typed rather than an
    isinstance check against a specific library: the fetchers this feeds
    (HealthSites.io, GIGA, Overpass) raise through gigaspatial's own vendored
    code, which is free to use httpx or requests internally, and both expose
    the same real `exc.response.status_code` shape on their HTTP error types.
    Any exception without that shape (timeouts, connection errors, DNS
    failures) returns None and is treated as retriable, the existing behavior.
    """
    status = getattr(getattr(exc, 'response', None), 'status_code', None)
    return status if status in _NON_RETRIABLE_HTTP_STATUS_CODES else None


def _fetch_with_retry(fetch_fn, country, source_name, max_attempts=3, base_delay_s=10):
    """
    Call fetch_fn() with exponential-backoff retry on failure.

    fetch_health_centers/fetch_schools/fetch_shelters/fetch_wash each hit an external API
    (HealthSites.io, GIGA, Overpass) with a single-shot try/except that falls straight
    through to a NaN-fallback on any failure; retrying here, before that fallback is hit,
    absorbs an ordinary transient error (rate limit, timeout, connection reset) that would
    otherwise permanently stick the base mercator/admin layer column at NaN for that run.

    Does not retry a fetch that succeeds but returns a semantically empty (non-erroring)
    result: that's a genuine "no facilities of this type in this country" signal in some
    cases (e.g. small territories with zero shelters), not a failure to retry.

    Does not retry a non-retriable HTTP status either (e.g. a 403 from a
    missing/misconfigured API key): retrying that would just burn the full exponential
    backoff (10s/20s/...) against the same broken credentials. See
    _non_retriable_status_code's own docstring for the exact set of codes this applies to
    and why it's deliberately narrow.

    Args:
        fetch_fn: zero-arg callable that performs the actual outbound API call
        country: ISO3 code, for log messages
        source_name: human-readable API name, for log messages
        max_attempts: total attempts including the first
        base_delay_s: base delay in seconds, doubled after each failed attempt

    Raises:
        The last exception raised by fetch_fn, immediately for a non-retriable HTTP
        status, or once every attempt has failed for anything else.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fetch_fn()
        except Exception as e:
            last_exc = e
            status = _non_retriable_status_code(e)
            if status is not None:
                logger.warning(
                    f"{country}: {source_name} fetch failed with HTTP {status} ({e}), "
                    f"not retrying: this status indicates a broken request/credentials, "
                    f"not a transient failure that a retry could fix"
                )
                break
            if attempt < max_attempts:
                delay = base_delay_s * (2 ** (attempt - 1))
                logger.warning(
                    f"{country}: {source_name} fetch attempt {attempt}/{max_attempts} failed "
                    f"({e}), retrying in {delay}s"
                )
                time.sleep(delay)
            else:
                logger.warning(f"{country}: {source_name} fetch failed after {max_attempts} attempts ({e})")
    raise last_exc
def fetch_health_centers(country, rewrite=0, fast_fail=False):
    """
    Fetch health center locations for a country, using custom data or cache if available.

    Priority order:
        1. Custom file at geodb/custom/<COUNTRY>_health_centers.csv: always used if present;
           --rewrite has no effect on custom data. Custom data is never overwritten.
        2. Location cache (hc_views/<COUNTRY>_health_centers.parquet): if rewrite=0.
        3. HealthSites.io API: fetched if no cache or rewrite=1.

    The cache stores ALL facility types from HealthSites (full OSM healthcare taxonomy).
    Filtering to HC_FACILITY_TYPES happens at analysis time in
    create_health_center_view_from_envelopes(), not here. This ensures custom data is
    filtered consistently and the cache can be reused if filtering changes.

    Cache location:
        LOCAL/BLOB: geodb/aos_views/hc_views/<COUNTRY>_health_centers.parquet
        SNOWFLAKE:  AOTS_ANALYSIS stage (PUT during init, GET'd during update)

    Args:
        country: ISO3 country code
        rewrite: If 1, re-fetch from API and overwrite cache (ignored if custom file exists);
                 if 0, use cache if available
        fast_fail: If True, bounds the whole HealthSites.io call (including its own
            internal OSM-name-resolution step and its unbounded pagination, see
            _FACILITY_FETCH_FAST_FAIL_TIMEOUT_S's own comment for the full "why") to a
            single short wall-clock attempt instead of the patient default. Same
            --type initialize/patch (default) vs --type update (fast_fail=True) split as
            fetch_shelters()/fetch_wash().

    Returns:
        gpd.GeoDataFrame: All health center locations with geometry (EPSG:4326).
                         Empty GeoDataFrame if no data is available from any source.
    """
    # 1. Custom data takes priority: never overwritten
    custom_gdf = _load_custom_points_csv(country, 'health_centers')
    if custom_gdf is not None:
        save_hc_locations(custom_gdf, country)
        return custom_gdf

    # 2. Use cache if available, valid, and rewrite not requested
    if hc_exist(country) and rewrite == 0:
        cached = load_hc_locations(country)
        if not cached.empty and 'osm_id' in cached.columns and 'geometry' in cached.columns and not cached.geometry.isna().all():
            # Caches written before the 'source' column existed have none;
            # label them with the standard source now rather than forcing a
            # re-fetch just to backfill provenance.
            if 'source' not in cached.columns:
                cached['source'] = 'HealthSites.io'
            return cached
        logger.warning(f"{country}: HC cache invalid or missing required columns, re-fetching from HealthSites.io")

    # 3. Fetch from HealthSites.io API
    if fast_fail and not hc_exist(country):
        # --type update path (fast_fail=True): a country with no persistent cache
        # has never had a successful fetch - a live attempt would likely fail
        # again for the same reason, paying a connection-timeout cost every
        # future cycle for a result that stays missing either way. Missing
        # facility-level exposure for one cycle is acceptable for --type update
        # (an enrichment layer, not core hazard/impact data); --type
        # initialize/patch keep the patient retry path below to actually
        # discover/backfill this data.
        logger.info(f"{country}: no persistent HealthSites.io cache exists yet, skipping live fetch under "
                    f"fast_fail (run --type initialize/patch to populate real data for this country)")
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')
    if (country, 'health_centers') in _FACILITY_FETCH_FAILURE_CACHE:
        logger.info(f"{country}: HealthSites.io fetch already failed this run, skipping retry (returning empty)")
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')
    try:
        if country in _HEALTHSITES_BBOX_OVERRIDES:
            # Query each bbox separately and concatenate: a disjoint territory
            # (e.g. BES) needs more than one tight box, not one huge box spanning
            # the gap between clusters.
            def _do_fetch():
                parts = [
                    HealthSitesFetcher().fetch_facilities(extent=bbox, output_format='geojson')
                    for bbox in _HEALTHSITES_BBOX_OVERRIDES[country]
                ]
                parts = [p for p in parts if not p.empty]
                return pd.concat(parts, ignore_index=True) if parts else gpd.GeoDataFrame()
        else:
            def _do_fetch():
                return HealthSitesFetcher(country=country).fetch_facilities(output_format='geojson')
        outer_attempts = 1 if fast_fail else 3
        bounded_fetch = (
            (lambda: _with_call_timeout(_do_fetch, _FACILITY_FETCH_FAST_FAIL_TIMEOUT_S))
            if fast_fail else _do_fetch
        )
        gdf_hcs = _fetch_with_retry(bounded_fetch, country, 'HealthSites.io', max_attempts=outer_attempts)
        if gdf_hcs.empty or 'geometry' not in gdf_hcs.columns:
            logger.warning(f"{country}: HealthSites API returned no data")
            return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')
        gdf_hcs = gdf_hcs.set_crs(4326)
        gdf_hcs['source'] = 'HealthSites.io'
        logger.info(f"{country}: Fetched {len(gdf_hcs)} health facilities from HealthSites API "
                    f"(all types cached; filtering to {HC_FACILITY_TYPES} happens at analysis time)")
        save_hc_locations(gdf_hcs, country)
        return gdf_hcs
    except Exception as e:
        if '403' in str(e):
            logger.error(f"{country}: HealthSites API returned 403 Forbidden: likely daily rate limit "
                         f"exceeded (50 requests/day). Run scripts/test_healthsites.py to confirm. "
                         f"To skip the API, provide a custom file at "
                         f"geodb/custom/{country}_health_centers.csv (see custom_data/README.md)")
        else:
            logger.error(f"{country}: Error fetching health centers from HealthSites API: {e}")
        _FACILITY_FETCH_FAILURE_CACHE.add((country, 'health_centers'))
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')


def fetch_schools(country, rewrite=0, fast_fail=False):
    """
    Fetch school locations for a country using custom data, cache, or GIGA API.

    Priority order:
        1. Custom file at geodb/custom/<COUNTRY>_schools.csv: always used if present;
           --rewrite has no effect on custom data. Custom data is never overwritten.
        2. Location cache (school_views/<COUNTRY>_schools.parquet): if rewrite=0.
        3. GIGA school location API: fetched if no cache or rewrite=1.

    All education levels are kept in the cache. The count stored in
    mercator/admin tiles is num_schools (total across all levels). Education level
    is not preserved in tile impact files.

    Cache location:
        LOCAL/BLOB: geodb/aos_views/school_views/<COUNTRY>_schools.parquet
        SNOWFLAKE:  AOTS_ANALYSIS stage (PUT during init, GET'd during update)

    Args:
        country: ISO3 country code
        rewrite: If 1, re-fetch from GIGA API and overwrite cache (ignored if custom file exists);
                 if 0, use cache if available
        fast_fail: If True, bounds the whole GIGA API call (its own unbounded pagination
            included, see _FACILITY_FETCH_FAST_FAIL_TIMEOUT_S's own comment for the full
            "why") to a single short wall-clock attempt. Same --type initialize/patch
            (default) vs --type update (fast_fail=True) split as fetch_shelters()/fetch_wash().

    Returns:
        gpd.GeoDataFrame: School locations with geometry (EPSG:4326).
                         Empty GeoDataFrame if no data available from any source.
    """
    # 1. Custom data takes priority: never overwritten
    custom_gdf = _load_custom_points_csv(country, 'schools')
    if custom_gdf is not None:
        save_school_locations(custom_gdf, country)
        return custom_gdf

    # 2. Use cache if available and rewrite not requested
    if school_exist(country) and rewrite == 0:
        gdf_schools = load_school_locations(country)
        if not isinstance(gdf_schools, gpd.GeoDataFrame):
            if isinstance(gdf_schools, pd.DataFrame) and not gdf_schools.empty and 'geometry' in gdf_schools.columns:
                gdf_schools = gpd.GeoDataFrame(gdf_schools, geometry='geometry', crs='EPSG:4326')
            else:
                gdf_schools = None
        # Same validation fetch_health_centers/fetch_shelters/fetch_wash apply to
        # their own caches: an empty cache or one missing the required ID column
        # ('school_id_giga', needed downstream by _ensure_unique_zone_ids()) must
        # trigger a re-fetch, not be silently returned and crash/misbehave later.
        if (gdf_schools is not None and not gdf_schools.empty
                and 'school_id_giga' in gdf_schools.columns and 'geometry' in gdf_schools.columns
                and not gdf_schools.geometry.isna().all()):
            # Caches written before the 'source' column existed have none;
            # label them with the standard source now rather than forcing a
            # re-fetch just to backfill provenance.
            if 'source' not in gdf_schools.columns:
                gdf_schools['source'] = 'UNICEF Giga school-location API'
            return gdf_schools
        logger.warning(f"{country}: School cache invalid or missing required columns, re-fetching from GIGA API")

    # 3. Fetch from GIGA API
    if fast_fail and not school_exist(country):
        # See fetch_health_centers()'s own identical guard for the full "why".
        logger.info(f"{country}: no persistent GIGA API cache exists yet, skipping live fetch under "
                    f"fast_fail (run --type initialize/patch to populate real data for this country)")
        return gpd.GeoDataFrame(columns=['geometry', 'school_id_giga'], crs='EPSG:4326')
    if (country, 'schools') in _FACILITY_FETCH_FAILURE_CACHE:
        logger.info(f"{country}: GIGA API fetch already failed this run, skipping retry (returning empty)")
        return gpd.GeoDataFrame(columns=['geometry', 'school_id_giga'], crs='EPSG:4326')
    try:
        def _do_fetch():
            return GigaSchoolLocationFetcher(country).fetch_locations(process_geospatial=True)
        outer_attempts = 1 if fast_fail else 3
        bounded_fetch = (
            (lambda: _with_call_timeout(_do_fetch, _FACILITY_FETCH_FAST_FAIL_TIMEOUT_S))
            if fast_fail else _do_fetch
        )
        result = _fetch_with_retry(bounded_fetch, country, 'GIGA school-location API', max_attempts=outer_attempts)
        if not isinstance(result, gpd.GeoDataFrame):
            if isinstance(result, pd.DataFrame) and not result.empty and 'geometry' in result.columns:
                result = gpd.GeoDataFrame(result, geometry='geometry', crs='EPSG:4326')
            else:
                logger.warning(f"{country}: GIGA API returned no usable school data")
                return gpd.GeoDataFrame(columns=['geometry', 'school_id_giga'], crs='EPSG:4326')
        gdf_schools = result
        if 'giga_id_school' in gdf_schools.columns:
            gdf_schools = gdf_schools.rename(columns={'giga_id_school': 'school_id_giga'})
        if gdf_schools.crs is None:
            gdf_schools.set_crs('EPSG:4326', inplace=True)
        gdf_schools['source'] = 'UNICEF Giga school-location API'
        if gdf_schools.empty:
            # Never overwrite a good cache with an empty result (e.g. a transient
            # rate-limit/API hiccup returning a valid-but-empty GeoDataFrame rather
            # than raising): matches fetch_health_centers/fetch_shelters/fetch_wash,
            # which all return early on empty before calling save_*_locations().
            # Preserves patch_country_layer()'s fallback-to-cache safety net, since
            # that cache must not itself have just been overwritten with this
            # same empty result.
            logger.warning(f"{country}: GIGA API returned no schools; cache left untouched")
            return gdf_schools
        save_school_locations(gdf_schools, country)
        return gdf_schools
    except Exception as e:
        logger.error(f"{country}: Error fetching schools from GIGA API: {e}")
        _FACILITY_FETCH_FAILURE_CACHE.add((country, 'schools'))
        return gpd.GeoDataFrame(columns=['geometry', 'school_id_giga'], crs='EPSG:4326')


def fetch_shelters(country, rewrite=0, fast_fail=False):
    """
    Fetch emergency shelter locations for a country using custom data, cache, or OSM.

    Priority order:
        1. Custom file at geodb/custom/<COUNTRY>_shelters.csv: always used if present.
        2. Location cache (shelter_views/<COUNTRY>_shelters.parquet): if rewrite=0.
        3. OSM via Overpass API using SHELTER_LOCATION_TYPES (social_facility=shelter).

    Args:
        country: ISO3 country code
        rewrite: If 1, re-fetch from OSM and overwrite cache (ignored if custom file exists)
        fast_fail: If True, uses a single short-timeout Overpass attempt instead of the
            default patient retry behavior. See _fetch_with_retry's own docstring for the
            full "why": the --type initialize/patch base-layer paths keep the patient
            default (a fresh country's cache is genuinely missing and worth waiting for),
            while the --type update ambient/per-storm paths pass fast_fail=True (a cache
            miss there means an earlier attempt already failed or the country was never
            onboarded, so a lengthy Overpass retry here just delays the current forecast
            cycle for a result that will still be missing either way).

    Returns:
        gpd.GeoDataFrame: Shelter locations with geometry (EPSG:4326) and 'osm_id' column.
    """
    # 1. Custom data takes priority: never overwritten
    custom_gdf = _load_custom_points_csv(country, 'shelters')
    if custom_gdf is not None:
        save_shelter_locations(custom_gdf, country)
        return custom_gdf

    # 2. Use cache if available, valid, and rewrite not requested
    if shelter_exist(country) and rewrite == 0:
        cached = load_shelter_locations(country)
        if not cached.empty and 'osm_id' in cached.columns and 'geometry' in cached.columns and not cached.geometry.isna().all():
            # Caches written before the 'source' column existed have none;
            # label them with the standard source now rather than forcing a
            # re-fetch just to backfill provenance.
            if 'source' not in cached.columns:
                cached['source'] = 'OpenStreetMap (social_facility=shelter tag)'
            return cached
        logger.warning(f"{country}: Shelter cache invalid or missing required columns, re-fetching from OSM")

    # 3. Fetch from OSM via Overpass API
    if fast_fail and not shelter_exist(country):
        # See fetch_health_centers()'s own identical guard for the full "why".
        logger.info(f"{country}: no persistent OSM shelter cache exists yet, skipping live fetch under "
                    f"fast_fail (run --type initialize/patch to populate real data for this country)")
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')
    if (country, 'shelters') in _FACILITY_FETCH_FAILURE_CACHE:
        logger.info(f"{country}: OSM (Overpass) shelter fetch already failed this run, skipping retry (returning empty)")
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')
    try:
        from gigaspatial.handlers.osm import OSMLocationFetcher
        fetcher_kwargs = _OSM_FAST_FAIL_KWARGS if fast_fail else {}
        outer_attempts = 1 if fast_fail else 3
        df = _fetch_with_retry(
            lambda: OSMLocationFetcher(country=country, location_types=SHELTER_LOCATION_TYPES, **fetcher_kwargs).fetch_locations(),
            country, 'OSM (Overpass)', max_attempts=outer_attempts
        )
        if df.empty:
            logger.warning(f"{country}: No shelter data found in OSM (social_facility=shelter)")
            return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df['longitude'], df['latitude']), crs='EPSG:4326'
        )
        gdf = gdf.rename(columns={'source_id': 'osm_id', 'category_value': 'shelter_type'})
        gdf['source'] = 'OpenStreetMap (social_facility=shelter tag)'
        logger.info(f"{country}: Fetched {len(gdf)} shelters from OSM")
        save_shelter_locations(gdf, country)
        return gdf
    except Exception as e:
        logger.error(f"{country}: Error fetching shelters from OSM: {e}")
        _FACILITY_FETCH_FAILURE_CACHE.add((country, 'shelters'))
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')


def fetch_wash(country, rewrite=0, fast_fail=False):
    """
    Fetch WASH infrastructure locations for a country using custom data, cache, or OSM.

    Priority order:
        1. Custom file at geodb/custom/<COUNTRY>_wash.csv: always used if present.
        2. Location cache (wash_views/<COUNTRY>_wash.parquet): if rewrite=0.
        3. OSM via Overpass API using WASH_LOCATION_TYPES.

    Fetches: drinking_water, water_point, toilets, shower (amenity) and
    water_well, water_tap, water_works, pumping_station, wastewater_treatment_plant (man_made).
    Natural sources (springs) are excluded: only managed infrastructure.

    Args:
        country: ISO3 country code
        rewrite: If 1, re-fetch from OSM and overwrite cache (ignored if custom file exists)
        fast_fail: If True, uses a single short-timeout Overpass attempt instead of the
            default patient retry behavior. See fetch_shelters's own docstring for the
            full "why" this differs by pipeline pathway.

    Returns:
        gpd.GeoDataFrame: WASH facility locations with geometry (EPSG:4326) and 'osm_id' column.
    """
    # 1. Custom data takes priority: never overwritten
    custom_gdf = _load_custom_points_csv(country, 'wash')
    if custom_gdf is not None:
        save_wash_locations(custom_gdf, country)
        return custom_gdf

    # 2. Use cache if available, valid, and rewrite not requested
    if wash_exist(country) and rewrite == 0:
        cached = load_wash_locations(country)
        if not cached.empty and 'osm_id' in cached.columns and 'geometry' in cached.columns and not cached.geometry.isna().all():
            # Caches written before the 'source' column existed have none;
            # label them with the standard source now rather than forcing a
            # re-fetch just to backfill provenance.
            if 'source' not in cached.columns:
                cached['source'] = 'OpenStreetMap (humanitarian WASH tags)'
            return cached
        logger.warning(f"{country}: WASH cache invalid or missing required columns, re-fetching from OSM")

    # 3. Fetch from OSM via Overpass API
    if fast_fail and not wash_exist(country):
        # See fetch_health_centers()'s own identical guard for the full "why".
        logger.info(f"{country}: no persistent OSM WASH cache exists yet, skipping live fetch under "
                    f"fast_fail (run --type initialize/patch to populate real data for this country)")
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')
    if (country, 'wash') in _FACILITY_FETCH_FAILURE_CACHE:
        logger.info(f"{country}: OSM (Overpass) WASH fetch already failed this run, skipping retry (returning empty)")
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')
    try:
        from gigaspatial.handlers.osm import OSMLocationFetcher
        fetcher_kwargs = _OSM_FAST_FAIL_KWARGS if fast_fail else {}
        outer_attempts = 1 if fast_fail else 3
        df = _fetch_with_retry(
            lambda: OSMLocationFetcher(country=country, location_types=WASH_LOCATION_TYPES, **fetcher_kwargs).fetch_locations(),
            country, 'OSM (Overpass)', max_attempts=outer_attempts
        )
        if df.empty:
            logger.warning(f"{country}: No WASH data found in OSM")
            return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df['longitude'], df['latitude']), crs='EPSG:4326'
        )
        gdf = gdf.rename(columns={'source_id': 'osm_id', 'category_value': 'wash_type'})
        gdf['source'] = 'OpenStreetMap (humanitarian WASH tags)'
        logger.info(f"{country}: Fetched {len(gdf)} WASH facilities from OSM "
                    f"({df['category_value'].value_counts().to_dict()})")
        save_wash_locations(gdf, country)
        return gdf
    except Exception as e:
        logger.error(f"{country}: Error fetching WASH facilities from OSM: {e}")
        _FACILITY_FETCH_FAILURE_CACHE.add((country, 'wash'))
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')


# =============================================================================
# FACILITY VIEW PERSISTENCE
# Save / load / existence-check functions for per-facility location caches
# (written once at --type initialize) and per-storm impact views (written on
# every --type update). Grouped by facility type: schools, HCs, shelters, WASH.
# =============================================================================
def save_school_view(gdf, country, storm, date, wind_th, dataset='wind'):
    """
    Save per-facility school impact view for a specific storm, date, and wind threshold.

    File name pattern: <COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet (wind) or
    <COUNTRY>_<STORM>_<DATE>_g<GUSTTH>.parquet (gust, own school_views_gust/ dir).
    The separate directory + 'g' prefix keep gust files structurally distinct
    from wind (avoids collision with wind thresholds of the same numeric value,
    and with any path-pattern-based downstream classification of school_views/).

    Note: This is distinct from the location cache (<COUNTRY>_schools.parquet).
    The impact view has one row per school with a 'probability' column; the
    location cache has full school metadata (education_level, etc.). Join on
    school_id_giga to combine them.

    Args:
        gdf: GeoDataFrame with per-school impact data (zone_id + probability)
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        wind_th: Threshold value (knots for wind, m/s for gust)
        dataset: 'wind' (default) or 'gust'
    """
    if 'zone_id' in gdf.columns and gdf['zone_id'].dtype != object:
        gdf = gdf.copy()
        gdf['zone_id'] = gdf['zone_id'].astype(str)
    dir_name = 'school_views_gust' if dataset == 'gust' else 'school_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}.parquet"
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path

def save_school_locations(gdf, country):
    """
    Save school locations to cache.
    
    Args:
        gdf: GeoDataFrame containing school locations
        country: ISO3 country code
    """
    file_name = f"{country}_schools.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', file_name))

def load_school_locations(country):
    """
    Load cached school locations.
    
    Args:
        country: ISO3 country code
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing cached school locations
    """
    file_name = f"{country}_schools.parquet"
    return read_dataset(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', file_name), data_store)

def school_exist(country):
    """
    Check if cached school locations exist for a country.
    
    Args:
        country: ISO3 country code
    
    Returns:
        bool: True if cached school data exists, False otherwise
    """
    file_name = f"{country}_schools.parquet"
    return data_store.file_exists(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', file_name))


def save_hc_view(gdf, country, storm, date, wind_th, dataset='wind'):
    """
    Save per-facility health center impact view for a specific storm, date, and wind threshold.

    File name pattern: <COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet (wind) or
    <COUNTRY>_<STORM>_<DATE>_g<GUSTTH>.parquet (gust, own hc_views_gust/ dir).

    Note: This is distinct from the location cache (<COUNTRY>_health_centers.parquet).
    The impact view has one row per facility with a 'probability' column; the
    location cache has full facility metadata including the 'amenity' column.
    Join on osm_id to combine them, or filter the impact view directly by
    'amenity', only HC_FACILITY_TYPES values will be present since filtering
    happens before the impact views are written.

    Args:
        gdf: GeoDataFrame with per-facility impact data (zone_id + probability)
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        wind_th: Threshold value (knots for wind, m/s for gust)
        dataset: 'wind' (default) or 'gust'
    """
    if 'zone_id' in gdf.columns and gdf['zone_id'].dtype != object:
        gdf = gdf.copy()
        gdf['zone_id'] = gdf['zone_id'].astype(str)
    dir_name = 'hc_views_gust' if dataset == 'gust' else 'hc_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}.parquet"
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path

def save_hc_locations(gdf, country):
    """
    Save health center locations to cache.
    
    Args:
        gdf: GeoDataFrame containing health center locations
        country: ISO3 country code
    """
    file_name = f"{country}_health_centers.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

def load_hc_locations(country):
    """
    Load cached health center locations.
    
    Args:
        country: ISO3 country code
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing cached health center locations
    """
    file_name = f"{country}_health_centers.parquet"
    return read_dataset(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name), data_store)

def hc_exist(country):
    """
    Check if cached health center locations exist for a country.

    Args:
        country: ISO3 country code

    Returns:
        bool: True if cached health center data exists, False otherwise
    """
    file_name = f"{country}_health_centers.parquet"
    return data_store.file_exists(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))


def save_shelter_view(gdf, country, storm, date, wind_th, dataset='wind'):
    """
    Save per-facility shelter impact view for a specific storm, date, and wind threshold.

    File name pattern: <COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet (wind) or
    <COUNTRY>_<STORM>_<DATE>_g<GUSTTH>.parquet (gust, own shelter_views_gust/ dir).

    Note: This is distinct from the location cache (<COUNTRY>_shelters.parquet).
    The impact view has one row per shelter with a 'probability' column; the
    location cache has full shelter metadata (shelter_type, capacity, etc.).

    Args:
        gdf: GeoDataFrame with per-shelter impact data (zone_id + probability)
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        wind_th: Threshold value (knots for wind, m/s for gust)
        dataset: 'wind' (default) or 'gust'
    """
    if 'zone_id' in gdf.columns and gdf['zone_id'].dtype != object:
        gdf = gdf.copy()
        gdf['zone_id'] = gdf['zone_id'].astype(str)
    dir_name = 'shelter_views_gust' if dataset == 'gust' else 'shelter_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}.parquet"
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path

def save_shelter_locations(gdf, country):
    """
    Save shelter locations to cache.

    Args:
        gdf: GeoDataFrame containing shelter locations
        country: ISO3 country code
    """
    file_name = f"{country}_shelters.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views', file_name))

def load_shelter_locations(country):
    """
    Load cached shelter locations.

    Args:
        country: ISO3 country code

    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing cached shelter locations
    """
    file_name = f"{country}_shelters.parquet"
    return read_dataset(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views', file_name), data_store)

def shelter_exist(country):
    """
    Check if cached shelter locations exist for a country.

    Args:
        country: ISO3 country code

    Returns:
        bool: True if cached shelter data exists, False otherwise
    """
    file_name = f"{country}_shelters.parquet"
    return data_store.file_exists(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views', file_name))


def save_wash_view(gdf, country, storm, date, wind_th, dataset='wind'):
    """
    Save per-facility WASH impact view for a specific storm, date, and wind threshold.

    File name pattern: <COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet (wind) or
    <COUNTRY>_<STORM>_<DATE>_g<GUSTTH>.parquet (gust, own wash_views_gust/ dir).

    Note: This is distinct from the location cache (<COUNTRY>_wash.parquet).
    The impact view has one row per WASH facility with a 'probability' column; the
    location cache has full facility metadata (wash_type, name, etc.).

    Args:
        gdf: GeoDataFrame with per-facility impact data (zone_id + probability)
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        wind_th: Threshold value (knots for wind, m/s for gust)
        dataset: 'wind' (default) or 'gust'
    """
    if 'zone_id' in gdf.columns and gdf['zone_id'].dtype != object:
        gdf = gdf.copy()
        gdf['zone_id'] = gdf['zone_id'].astype(str)
    dir_name = 'wash_views_gust' if dataset == 'gust' else 'wash_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}.parquet"
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path

def save_wash_locations(gdf, country):
    """
    Save WASH facility locations to cache.

    Args:
        gdf: GeoDataFrame containing WASH facility locations
        country: ISO3 country code
    """
    file_name = f"{country}_wash.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views', file_name))

def load_wash_locations(country):
    """
    Load cached WASH facility locations.

    Args:
        country: ISO3 country code

    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing cached WASH facility locations
    """
    file_name = f"{country}_wash.parquet"
    return read_dataset(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views', file_name), data_store)

def wash_exist(country):
    """
    Check if cached WASH facility locations exist for a country.

    Args:
        country: ISO3 country code

    Returns:
        bool: True if cached WASH data exists, False otherwise
    """
    file_name = f"{country}_wash.parquet"
    return data_store.file_exists(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views', file_name))

