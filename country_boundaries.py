#!/usr/bin/env python3
"""
Country Boundaries Module

Country/admin boundary bookkeeping: fetching country and admin-level
boundaries from GeoRepo, writing PIPELINE_COUNTRIES.COUNTRY_BOUNDARY and the
local/Blob boundary pre-filter cache, and admin-level parquet discovery.
Split out of impact_analysis.py, which imports several of these functions
back (real internal dependencies, e.g. create_views_from_envelopes_in_country()
writes country boundaries as part of its own per-country orchestration).

Module structure:
    _BES_GEOREPO_UCODES
    get_country_boundaries
    add_admin_ids, _parse_admin_levels_from_filenames,
        get_initialized_admin_levels, get_initialized_admin_levels_bulk
    write_country_boundary, _split_ring_at_antimeridian, _buffered_convex_hull,
        _update_pipeline_countries_cache, get_countries_in_range_cached
"""

import logging
import os
import re
import geopandas as gpd
import pandas as pd
import pyproj

from gigaspatial.handlers import AdminBoundaries
from gigaspatial.processing import convert_to_geodataframe
from gigaspatial.handlers.unicef.georepo import GeoRepoClient
from gigaspatial.core.io.readers import read_dataset
from gigaspatial.core.io.writers import write_dataset
from shapely.geometry import shape, MultiPolygon, Polygon
from shapely.ops import unary_union
from shapely.ops import transform as shapely_transform

from snowflake_utils import get_snowflake_connection

# Shared state/helpers with no dependency of their own on this module or its
# siblings - see pipeline_shared.py's own docstring for why this exists.
from pipeline_shared import (
    data_store,
    ROOT_DATA_DIR,
    VIEWS_DIR,
    PIPELINE_COUNTRIES_CACHE_PATH,
    admins_overlay,
)

logger = logging.getLogger(__name__)

# GeoRepo's "Global Administrative Boundaries" dataset has no single level-0
# entity for BES (Bonaire, Sint Eustatius and Saba): it models the three
# islands as separate level-0 "Territory" entities, each independently
# tagged ISO3=BES but with its own ucode (BES1/BES2/BES3), not one combined
# "BES_..." entity. AdminBoundaries.create()'s ISO3 lookup expects a ucode
# starting with "BES_" and finds nothing. Confirmed via a direct GeoRepo API
# query against the "Global Administrative Boundaries (Latest)" view.
_BES_GEOREPO_UCODES = {
    "BES1_V2": "Saba",
    "BES2_V2": "Sint Eustatius",
    "BES3_V2": "Bonaire",
}


def get_country_boundaries(countries):
    """
    Retrieve country boundary geometries for a list of countries.

    A single country's fetch failure is isolated and does not abort the others
    this is the Python fallback path (used only when the SQL pre-filter itself
    fails), and one country's transient GeoRepo hiccup must not take down storm
    processing for every other unrelated, healthy country in the same request,
    matching the per-country isolation pattern used everywhere else in this
    pipeline (e.g. impact_analysis.py's own per-country
    create_views_from_envelopes_in_country loop).

    Args:
        countries: List of ISO3 country codes (e.g., ['TWN', 'DOM'])

    Returns:
        list: One entry per input country, same order, a Shapely geometry
              (admin level 0 boundary) on success, or None if that country's
              boundary could not be retrieved. Callers must handle None entries
              (skip that country, log a warning) rather than assume every
              entry is a real geometry.
    """
    country_boundaries = []
    for country in countries:
        try:
            admin_boundaries = AdminBoundaries.create(country_code=country, admin_level=0)
            country_boundaries.append(admin_boundaries.to_geodataframe().geometry.iat[0])
        except Exception as e:
            logger.error(f"Error retrieving boundaries for {country}: {e}, skipping this country, others continue")
            country_boundaries.append(None)
    return country_boundaries

def add_admin_ids(view, country, admin_level=1, strict=False):
    """
    Add admin-level IDs to a mercator tile view.

    Fetches admin boundaries at the requested level from GeoRepo and assigns each
    tile to the admin boundary with the largest intersection area (via admins_overlay).
    Unless strict=True, falls back to admin level 0 (whole country) if the requested
    level is unavailable.

    Args:
        view: GeoDataFrame containing mercator tiles (must have 'tile_id' column)
        country: ISO3 country code
        admin_level: Admin level to use for boundary assignment (default: 1)
        strict: If True, raise ValueError instead of falling back to admin 0 when
                the requested level is unavailable (default: False)

    Returns:
        tuple: (combined_view, gdf_admins) where:
            - combined_view: GeoDataFrame with tiles and 'id' column (admin boundary ID)
            - gdf_admins: GeoDataFrame with admin boundaries at the requested level

    Raises:
        ValueError: If strict=True and the requested admin level is unavailable
    """
    try:
        gdf_admins1 = AdminBoundaries.create(country_code=country, admin_level=admin_level).to_geodataframe()
        # giga-spatial 0.9.x AdminBoundaries.to_geodataframe() returns 'boundary_id';
        # older versions returned 'id'. Normalise to 'id' so downstream code is consistent.
        if "boundary_id" in gdf_admins1.columns and "id" not in gdf_admins1.columns:
            gdf_admins1 = gdf_admins1.rename(columns={"boundary_id": "id"})
        if gdf_admins1.empty or "id" not in gdf_admins1.columns:
            raise ValueError(f"Admin level {admin_level} boundaries empty or missing 'id' column")
    except Exception as e:
        if strict:
            raise ValueError(
                f"{country}: Admin level {admin_level} not available in GeoRepo ({e})"
            ) from e
        logger.warning(
            f"{country}: Admin level {admin_level} boundaries unavailable ({e}), "
            "falling back to admin level 0 (whole country as single region)"
        )
        gdf_admins1 = AdminBoundaries.create(country_code=country, admin_level=0).to_geodataframe()
        if "boundary_id" in gdf_admins1.columns and "id" not in gdf_admins1.columns:
            gdf_admins1 = gdf_admins1.rename(columns={"boundary_id": "id"})
        if "name" not in gdf_admins1.columns:
            gdf_admins1["name"] = country
        if "id" not in gdf_admins1.columns:
            gdf_admins1["id"] = country
    combined_view = admins_overlay(gdf_admins1, view)
    return combined_view, gdf_admins1


def _parse_admin_levels_from_filenames(files, country=None):
    """
    Shared parsing logic behind get_initialized_admin_levels() and
    get_initialized_admin_levels_bulk(): matches every "{country}_admin<N>.parquet"
    filename in `files` (basenames only, any directory prefix in the path is
    ignored), with no upper bound on N (see get_initialized_admin_levels()'s
    own docstring for why).

    Args:
        files: iterable of file paths/names (typically a data_store.list_files()
            result for the admin_views/ directory).
        country: if given, only that country's levels are matched and a flat
            sorted list[int] is returned (get_initialized_admin_levels()'s own
            single-country contract). If None, every country found is matched
            and a dict {country: sorted list[int]} is returned instead
            (get_initialized_admin_levels_bulk()'s own contract).
    """
    if country is not None:
        prefix, suffix = f"{country}_admin", ".parquet"
        found = []
        for f in files:
            fname = os.path.basename(f)
            if fname.startswith(prefix) and fname.endswith(suffix):
                level_str = fname[len(prefix):-len(suffix)]
                if level_str.isdigit():
                    found.append(int(level_str))
        return sorted(found)

    by_country = {}
    for f in files:
        fname = os.path.basename(f)
        m = re.match(r'^([A-Za-z0-9]+)_admin(\d+)\.parquet$', fname)
        if not m:
            continue
        by_country.setdefault(m.group(1), []).append(int(m.group(2)))
    for c in by_country:
        by_country[c] = sorted(by_country[c])
    return by_country


def get_initialized_admin_levels(country):
    """
    Return the list of admin levels that have base parquets initialized for a country.

    Lists admin_views/ and matches every "{country}_admin<N>.parquet" file found,
    with no upper bound on N; --type initialize --admin has no upper bound either
    (see main_pipeline.py's argparse), so a fixed probe range here would silently
    make any level beyond it invisible to --type update forever. This determines
    which admin-level storm views are produced during --type update.

    Args:
        country: ISO3 country code

    Returns:
        list[int]: Admin levels with existing base parquets (e.g. [1, 2])
    """
    admin_dir = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views')
    try:
        files = data_store.list_files(admin_dir)
    except Exception as e:
        logger.warning(f"Could not list {admin_dir} to detect initialized admin levels for {country}: {e}")
        return []

    return _parse_admin_levels_from_filenames(files, country=country)


def get_initialized_admin_levels_bulk(countries=None):
    """
    Bulk equivalent of get_initialized_admin_levels(): ONE listing of the
    shared admin_views/ directory, parsed into every country's own levels at
    once, instead of one listing per country.

    Exists for a caller (e.g. run_precip_analysis()) that needs every
    country's own initialized admin levels across a whole run: calling
    get_initialized_admin_levels(country) once per country in that run's own
    country list re-lists this same shared, country-independent directory
    redundantly, once per country, for a result that's already fully
    determined by this single listing.

    countries: the caller's own full country list for this run, used ONLY as
    a fallback path if the bulk listing itself fails (see below). Pass None
    to get a plain {} on failure instead of the per-country fallback.

    On a listing failure with `countries` given, this falls back to the
    original per-country listing (slower, but only on this failure path, not
    the normal one) rather than returning {} unconditionally: a caller's own
    `admin_levels_by_country.get(country) or [1]` pattern can't distinguish
    "genuinely has no admin2+ views" from "the one listing that would have
    found them failed", and a country still writes real tile/admin1/facility
    output on a degraded run (files_written > 0, no country-level failure),
    so the cycle still gets marked processed and a retry never happens
    either. The per-country fallback avoids a transient directory-listing
    hiccup silently and permanently downgrading admin coverage for the whole
    run.

    Returns:
        dict {country: list[int]}: every country found in admin_views/ mapped
        to its own sorted admin levels. A country with no initialized admin
        views at all (e.g. not yet processed by --type initialize --admin)
        simply has no key here, same as get_initialized_admin_levels()
        returning [] for that country.
    """
    admin_dir = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views')
    try:
        files = data_store.list_files(admin_dir)
    except Exception as e:
        if countries:
            logger.warning(
                f"Could not bulk-list {admin_dir} to detect initialized admin levels "
                f"({e}); falling back to one listing per country ({len(countries)} countries) "
                f"instead of silently downgrading every one of them to admin-level-1-only."
            )
            result = {}
            for country in countries:
                levels = get_initialized_admin_levels(country)
                if levels:
                    result[country] = levels
            return result
        logger.warning(f"Could not list {admin_dir} to bulk-detect initialized admin levels: {e}")
        return {}

    return _parse_admin_levels_from_filenames(files, country=None)


def write_country_boundary(country: str):
    """
    Fetch admin level 0 boundary from GeoRepo once, then write it to two
    independent, best-effort destinations: PIPELINE_COUNTRIES.COUNTRY_BOUNDARY
    in Snowflake, and the local/Blob boundary cache
    (pipeline_countries_boundaries.parquet, see _update_pipeline_countries_cache()).
    Called automatically during --type initialize for each new country.

    Deliberately independent, not sequential-with-early-return: a genuinely
    Snowflake-credential-less BLOB deployment must still be able to build/
    refresh the cache (which get_countries_in_range_cached() depends on for
    every --type update run) even though the Snowflake write will fail there
    every time. Same reasoning as PIPELINE_COUNTRIES auto-registration
    elsewhere in this file being unconditional and best-effort regardless of
    DATA_PIPELINE_DB. The GeoRepo fetch itself is the one real, unavoidable
    network dependency, done once and reused for both writes.

    BES is a special case: see _BES_GEOREPO_UCODES above. Its three real
    sub-territory geometries are fetched directly by ucode and unioned,
    bypassing AdminBoundaries.create()'s single-ISO3 lookup entirely.
    """
    if country == "BES":
        client = GeoRepoClient()
        geoms = []
        for ucode, name in _BES_GEOREPO_UCODES.items():
            feature = client.get_entity_by_ucode(ucode, geom="full_geom", format="geojson")
            if feature and feature.get("geometry"):
                geoms.append(shape(feature["geometry"]))
            else:
                logger.warning(f"{country}: GeoRepo returned no geometry for {name} ({ucode})")
        if not geoms:
            logger.warning(f"{country}: GeoRepo returned no boundary for any BES sub-territory, COUNTRY_BOUNDARY not updated")
            return
        geom = unary_union(geoms)
    else:
        boundaries = AdminBoundaries.create(country_code=country, admin_level=0)
        gdf = boundaries.to_geodataframe()
        if gdf.empty or gdf.geometry.isna().all():
            logger.warning(f"{country}: GeoRepo returned no boundary, COUNTRY_BOUNDARY not updated")
            return
        # Union all rows in case GeoRepo returns multiple polygons for admin_level=0
        geom = gdf.geometry.union_all()

    # Independent write 1: Snowflake (best-effort)
    active = True  # default when the ACTIVE SELECT below can't run (no Snowflake);
                   # matches add_country_to_snowflake()'s own default for a new row.
    conn = None
    try:
        wkt = geom.wkt
        center_lat = geom.centroid.y
        center_lon = geom.centroid.x
        span = max(
            geom.bounds[3] - geom.bounds[1],  # lat span
            geom.bounds[2] - geom.bounds[0],  # lon span
        )
        view_zoom = (11 if span < 0.5 else 10 if span < 1 else
                     9 if span < 2 else 8 if span < 4 else 7)
        conn = get_snowflake_connection()
        cur = conn.cursor()
        # Use TO_GEOGRAPHY (not TRY_TO_GEOGRAPHY) so invalid WKT raises immediately
        # rather than silently writing NULL to COUNTRY_BOUNDARY. This is a foreseen,
        # intentionally-triggered exception path for bad GeoRepo WKT, conn must still
        # be closed when it happens, hence the finally below.
        # COALESCE preserves any manually-set center/zoom values.
        cur.execute("""
            UPDATE AOTS.TC_ECMWF.PIPELINE_COUNTRIES
            SET COUNTRY_BOUNDARY = TO_GEOGRAPHY(%(wkt)s),
                CENTER_LAT = COALESCE(CENTER_LAT, %(lat)s),
                CENTER_LON = COALESCE(CENTER_LON, %(lon)s),
                VIEW_ZOOM  = COALESCE(VIEW_ZOOM,  %(zoom)s)
            WHERE COUNTRY_CODE = %(iso)s
        """, {"wkt": wkt, "iso": country, "lat": center_lat, "lon": center_lon, "zoom": view_zoom})
        conn.commit()
        # Cheap re-use of the connection already open here to also learn ACTIVE,
        # needed by the boundary cache below (this function doesn't otherwise
        # know it; ACTIVE is set separately, by add_country_to_snowflake()).
        cur.execute("SELECT ACTIVE FROM AOTS.TC_ECMWF.PIPELINE_COUNTRIES WHERE COUNTRY_CODE = %(iso)s",
                    {"iso": country})
        row = cur.fetchone()
        if row is not None:
            active = bool(row[0])
        cur.close()
        logger.info(f"{country}: COUNTRY_BOUNDARY, CENTER_LAT/LON, VIEW_ZOOM written to Snowflake")
    except Exception as e:
        logger.warning(f"{country}: Could not write COUNTRY_BOUNDARY to Snowflake: {e}")
    finally:
        if conn is not None:
            conn.close()

    # Independent write 2: local/Blob cache (best-effort, always attempted
    # regardless of whether the Snowflake write above succeeded)
    _update_pipeline_countries_cache(country, geom, active)


def _split_ring_at_antimeridian(coords):
    """
    coords: a closed ring (list of (lon, lat) tuples, first == last), with
    every longitude already in the standard -180..180 range, but where a
    single edge between two consecutive vertices may still cross the
    +/-180 seam.

    Returns a list of one or more closed (lon, lat) rings, none of which
    cross the seam. See _buffered_convex_hull()'s docstring for why this
    step exists: Shapely/GEOS has no concept that +180 and -180 are the
    same point, so an edge that (correctly) represents a short hop across
    the dateline gets drawn as a literal, wrong, ~360-degree-wide straight
    line in raw (lon, lat) space unless split explicitly, right here.
    """
    n = len(coords)
    crossings = {}
    for i in range(n - 1):
        lon1, lat1 = coords[i]
        lon2, lat2 = coords[i + 1]
        if abs(lon2 - lon1) > 180:
            delta = (lon2 - 360) - lon1 if lon1 < lon2 else (lon2 + 360) - lon1
            t = 0.5 if delta == 0 else min(max(
                ((-180 - lon1) / delta) if lon1 < lon2 else ((180 - lon1) / delta), 0.0), 1.0)
            crossings[i] = lat1 + t * (lat2 - lat1)

    if not crossings:
        return [coords]

    pieces, current = [], [coords[0]]
    for i in range(n - 1):
        lon1, _ = coords[i]
        lon2, lat2 = coords[i + 1]
        if i in crossings:
            lat_cross = crossings[i]
            edge_lon = 180.0 if lon1 > 0 else -180.0
            current.append((edge_lon, lat_cross))
            pieces.append(current)
            current = [(-edge_lon, lat_cross)]
        current.append((lon2, lat2))
    pieces.append(current)

    # The ring is closed, so the first and last pieces are really one piece
    # split across the array boundary; stitch them back together.
    if len(pieces) >= 2:
        pieces = pieces[1:-1] + [pieces[-1] + pieces[0][1:]]

    return [p if p[0] == p[-1] else p + [p[0]] for p in pieces]


def _buffered_convex_hull(geom, buffer_m: int):
    """
    Convex hull of geom, buffered by buffer_m metres, antimeridian-safe.

    A Transverse Mercator centered exactly on the antimeridian
    (+proj=tmerc +lon_0=180), driven through pyproj.Transformer directly
    (never geopandas.to_crs(), which silently re-wraps longitude
    mid-computation), maps every individual vertex back to a correct WGS84
    longitude. The remaining constraint is downstream of that: the buffered
    ring crosses the seam via ONE edge that Shapely/GEOS (which has no
    concept that +180 and -180 are the same point) draws as a literal
    straight line in raw (lon, lat) space from e.g. (179.9, y) to (-180.0,
    y), a ~360-degree-wide edge slicing across lon=0. Ray-casting
    containment then sees almost the WHOLE globe as "inside" except a thin
    sliver: a VALID but geometrically BACKWARDS result, where real points
    near the actual territory get excluded while points on the opposite side
    of the globe get included. Plain .convex_hull()/gigaspatial's
    buffer_geodataframe() have the same underlying constraint. This function
    handles it by, after transforming back to WGS84, walking the (convex,
    single-ring) buffered polygon's exterior ring itself and explicitly
    splitting it wherever it crosses the seam
    (_split_ring_at_antimeridian()), combining the pieces as a MultiPolygon.
    """
    minx, miny, maxx, maxy = geom.bounds
    if maxx - minx <= 180:
        hull = gpd.GeoSeries([geom], crs='EPSG:4326').convex_hull
        utm_crs = hull.estimate_utm_crs()
        return hull.to_crs(utm_crs).buffer(buffer_m).to_crs('EPSG:4326').iloc[0]

    proj_crs = "+proj=tmerc +lon_0=180 +datum=WGS84 +units=m +no_defs"
    fwd = pyproj.Transformer.from_crs("EPSG:4326", proj_crs, always_xy=True)
    rev = pyproj.Transformer.from_crs(proj_crs, "EPSG:4326", always_xy=True)

    projected = shapely_transform(lambda x, y, z=None: fwd.transform(x, y), geom)
    buffered = projected.convex_hull.buffer(buffer_m)
    back = shapely_transform(lambda x, y, z=None: rev.transform(x, y), buffered)

    # A buffered convex hull is itself convex (Minkowski sum of a convex
    # set with a disk), so it's a single exterior ring with no holes.
    pieces = _split_ring_at_antimeridian(list(back.exterior.coords))
    polys = [Polygon(p) for p in pieces]
    polys = [p if p.is_valid else p.buffer(0) for p in polys]
    return polys[0] if len(polys) == 1 else MultiPolygon(polys)


def _update_pipeline_countries_cache(country: str, geom, active: bool, buffer_m: int = 100_000):
    """
    Upserts one country's row into a small, self-maintained country pre-filter
    cache (geodb/pipeline_countries_boundaries.parquet), written through
    data_store so it lives wherever DATA_PIPELINE_DB points (local/Blob/
    Snowflake stage).

    Stores the country's CONVEX HULL, buffered by buffer_m, not its literal
    admin boundary, computed once here (this function only runs once per
    country, when its boundary is (re)fetched from GeoRepo) so
    get_countries_in_range_cached() below never has to reproject or buffer
    anything at request time (every storm cycle). This matters for real
    archipelago nations: measured directly, buffering SLB's (Solomon Islands)
    literal boundary once (1,764 separate island parts, 573k vertices) took
    52 seconds; its convex hull (44 vertices) buffers in under a millisecond.
    A convex hull is always equal-or-larger than the real shape, so this can
    only make a country match the pre-filter slightly more often than the
    literal boundary would, never less, consistent with this pipeline's
    existing bias toward not silently excluding a country (see
    get_countries_in_range()'s own docstring). This cache is a coarse "is
    this country roughly in range" pre-filter only, not a source of truth for
    the real country boundary used elsewhere (impact computation, mercator
    tiles, etc.), which still comes from GeoRepo/Snowflake untouched.

    Piggybacks on write_country_boundary()'s own already-fetched geometry;
    no extra GeoRepo call needed to build or maintain this. Never raises: a
    cache-write failure must not take down the real Snowflake write in the
    caller, which already succeeded by the time this runs.
    """
    try:
        buffered_hull_geom = _buffered_convex_hull(geom, buffer_m)

        gdf_new = gpd.GeoDataFrame(
            {'COUNTRY_CODE': [country], 'ACTIVE': [bool(active)]},
            geometry=[buffered_hull_geom], crs='EPSG:4326',
        )
        if data_store.file_exists(PIPELINE_COUNTRIES_CACHE_PATH):
            gdf_existing = read_dataset(PIPELINE_COUNTRIES_CACHE_PATH, data_store)
            gdf_existing = gdf_existing[gdf_existing['COUNTRY_CODE'] != country]
            gdf_combined = pd.concat([gdf_existing, gdf_new], ignore_index=True)
        else:
            gdf_combined = gdf_new
        write_dataset(convert_to_geodataframe(gdf_combined), data_store, PIPELINE_COUNTRIES_CACHE_PATH)
        logger.info(f"{country}: updated pipeline_countries_boundaries.parquet cache (buffered convex-hull pre-filter zone)")
    except Exception as e:
        logger.warning(f"{country}: could not update boundary cache (non-fatal): {e}")


def get_countries_in_range_cached(gdf_envelopes) -> list:
    """
    Country pre-filter using the local/Blob boundary cache
    (pipeline_countries_boundaries.parquet) instead of either a live Snowflake
    ST_DWITHIN query or a live per-country GeoRepo fetch (get_country_boundaries()).
    Zero network calls, and zero geometry computation here: the cache already
    stores each country's buffered convex-hull pre-filter zone (computed once,
    at write time, by _update_pipeline_countries_cache() above, see its own
    docstring for why), so this is nothing but a single spatially-indexed
    join (shapely 2.0's built-in STRtree under the hood).

    Intended for HAZARD_DATA_SOURCE != 'SNOWFLAKE': matches the philosophy
    already used for tracks/envelopes/precip/river in that mode (avoid a live
    Snowflake dependency), and unlike the raw Snowflake SQL path, correctly
    distinguishes "no data" from "confirmed 0 countries in range" by simply
    never depending on TC_ENVELOPES_COMBINED at all; the storm envelope
    passed in is whatever get_envelopes() already loaded (Snowflake or
    LOCAL/BLOB per config.HAZARD_DATA_SOURCE), not re-queried here.

    Returns None (not an empty list) if the cache file itself doesn't exist
    yet, gdf_envelopes is empty, or the cache read/join itself fails (a
    corrupt or malformed cache file must fall back the same way a SQL query
    failure does above, not abort the whole storm's analysis). Callers must
    treat that as "could not evaluate" and fall back to
    get_country_boundaries()-based Python buffer check, same as the existing
    SQL-failure fallback. A real empty list is a genuine "confirmed 0
    countries in range" result, kept distinct from this on purpose (same
    None-vs-empty distinction used elsewhere in this codebase, e.g.
    calculate_tile_member_bitmask()'s own Returns section, which documents
    its own None-vs-empty-DataFrame distinction).
    """
    if gdf_envelopes.empty or not data_store.file_exists(PIPELINE_COUNTRIES_CACHE_PATH):
        logger.warning("pipeline_countries_boundaries.parquet cache not found or no envelope data; "
                        "cannot evaluate cached country pre-filter")
        return None

    try:
        gdf_countries = read_dataset(PIPELINE_COUNTRIES_CACHE_PATH, data_store)
        gdf_countries = gdf_countries[gdf_countries['ACTIVE'] == True]  # noqa: E712 (pandas bool column)
        if gdf_countries.empty:
            return []

        joined = gpd.sjoin(gdf_countries, gdf_envelopes[['geometry']], predicate='intersects', how='inner')
        return sorted(joined['COUNTRY_CODE'].unique().tolist())
    except Exception as e:
        logger.warning(f"Could not evaluate cached country pre-filter (cache read/join failed): {e}")
        return None
