#!/usr/bin/env python3
"""
Main Impact Analysis Pipeline Orchestrator

Coordinates the complete impact analysis pipeline for tropical cyclone early warning.
Three operating modes: initialize, update, patch.

Key Features:
- initialize: builds country base layers (mercator tiles + admin views) with population,
  built surface, settlement class, wealth index, schools, health centers, shelters, WASH
- update: fetches active storm envelopes from Snowflake and runs geospatial intersection
  against all initialized countries within 500 km; generates per-facility and tile-level
  impact views at 8 wind thresholds (34–137 kt) plus JSON reports and CCI values
- patch: backfills specific columns in existing mercator parquets without full
  re-initialization (supported: population, school_age_population, infant_population,
  adolescent_population, built_surface_m2, smod_class, smod_class_l1, rwi,
  schools, hcs, shelters, wash, vulnerability)

- Custom data overrides: place a CSV in geodb/custom/ to replace any API or raster source
  for a specific country — custom files are never overwritten by the pipeline
- Storage-backend agnostic: LOCAL, Azure Blob (ADLS), or Snowflake internal stage

Usage Examples:
    # Initialize base data for a new country
    python main_pipeline.py --type initialize --countries TWN --zoom 14

    # Force re-initialization (regenerates all data from scratch)
    python main_pipeline.py --type initialize --countries PNG --rewrite 1

    # Process all recent storms (default: last 2 days)
    python main_pipeline.py --type update

    # Process storms for a specific date
    python main_pipeline.py --type update --date 2025-11-10

    # Process a specific storm on a specific date
    python main_pipeline.py --type update --date 2025-11-10 --storm FUNG-WONG

    # Backfill optional columns without full re-init
    python main_pipeline.py --type patch --countries PNG --columns shelters wash

    # Backfill raster columns after data becomes available
    python main_pipeline.py --type patch --countries PNG --columns built_surface_m2 rwi
"""

import os
import sys
import argparse
import logging
from datetime import datetime
import pandas as pd
import geopandas as gpd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dotenv import load_dotenv

# Load environment variables from the project root
# This assumes the .env file is in the project root directory
load_dotenv()


# =============================================================================
# IMPORTS
# =============================================================================
from impact_analysis import (
    load_envelopes_from_snowflake,
    load_gust_envelopes_from_snowflake,
    is_envelope_in_zone,
    get_country_boundaries,
    create_views_from_envelopes_in_country,
    save_mercator_and_admin_views,
    save_json_storms,
    load_json_storms,
    patch_country_layer,
    load_mercator_view,
    load_admin_view,
    get_initialized_admin_levels,
    admins_overlay,
    create_precip_tile_view,
    create_precip_admin_view,
    create_precip_facility_view,
    grid_to_geotiff_to_tifprocessor,
    save_precip_tile_view,
    save_precip_admin_view,
    save_precip_ratio_view,
    save_precip_ratio_admin_view,
    save_precip_school_view,
    save_precip_ratio_school_view,
    save_precip_hc_view,
    save_precip_ratio_hc_view,
    save_precip_shelter_view,
    save_precip_ratio_shelter_view,
    save_precip_wash_view,
    save_precip_ratio_wash_view,
    fetch_schools,
    fetch_health_centers,
    fetch_shelters,
    fetch_wash,
    _ensure_unique_zone_ids,
    HC_FACILITY_TYPES,
    assign_facilities_to_tiles,
)
from precip_utils import (
    get_latest_met_forecast,
    get_met_forecast_for_date,
    read_precip_window,
    exceedance_probability,
    ratio_exceedance_probability,
    get_hazard_met_data_store,
)

# Import gigaspatial for buffering
from gigaspatial.processing import buffer_geodataframe

import json
from snowflake_utils import get_snowflake_data, get_snowflake_connection, get_countries_in_range
from country_utils import get_active_countries_from_snowflake, add_country_to_snowflake


# =============================================================================
# CONFIGURATION
# =============================================================================
def setup_logging(log_level="INFO"):
    """
    Setup logging configuration for the pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.
    
    Returns:
        logging.Logger: Configured logger instance.
    """
    global logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('main_pipeline.log')
        ]
    )
    logger = logging.getLogger(__name__)
    return logger


# =============================================================================
# IMPACT ANALYSIS FUNCTIONS
# =============================================================================
def run_complete_impact_analysis(storm, date, countries, logger, zoom, skip_gust=False):
    """
    Complete impact analysis orchestration.

    Loads hurricane envelope data from Snowflake, checks which countries are affected
    (using 500km buffer per country), and creates impact views for affected countries.
    Admin levels are determined automatically by which base admin parquets exist for each
    country (created during --type initialize).

    Also loads gust envelope data (if available) and generates core exposure gust
    views alongside the wind views, for the same affected countries. Gust data is
    optional per storm/forecast; its absence never affects the wind results.

    Args:
        storm: Storm name (e.g., 'FUNG-WONG', 'JERRY')
        date: Forecast date in YYYYMMDDHHMMSS format (e.g., '20251110000000')
        countries: List of ISO3 country codes (e.g., ['TWN', 'DOM'])
        logger: Logger instance for logging
        zoom: Zoom level for mercator tiles (default: 14)
        skip_gust: If True, skip gust envelope processing even if gust data is
            available (wind processing is unaffected either way)

    Returns:
        dict: Summary of analysis results with keys:
            - success (bool): Whether analysis completed successfully
            - envelopes_processed (int): Number of envelope records processed
            - countries_processed (int): Number of countries processed
            - total_views_created (int): Estimated number of views created
            - affected_countries (list): List of country codes that were affected
            - error (str): Error message if success is False
    """
    logger.info(f"Running impact analysis for {storm} at {date}")
    logger.info(f"Countries: {', '.join(countries)}")
    
    try:
        # Load envelope data directly from Snowflake
        logger.info("Loading envelope data from Snowflake...")
        gdf_envelopes = load_envelopes_from_snowflake(storm, date)
        
        if gdf_envelopes.empty:
            logger.info(f"No envelope data found for {storm} at {date} — forecast may have expired, skipping")
            return {"success": True, "skipped": True, "envelopes_processed": 0, "countries_processed": 0, "total_views_created": 0, "affected_countries": []}
        
        logger.info(f"Loaded {len(gdf_envelopes)} envelope records")
        logger.info("Envelopes already converted to GeoDataFrame")

        # Gust envelopes are optional and independent of the wind path above.
        gdf_envelopes_gust = pd.DataFrame() if skip_gust else load_gust_envelopes_from_snowflake(storm, date)
        if not gdf_envelopes_gust.empty:
            logger.info(f"Loaded {len(gdf_envelopes_gust)} gust envelope records")

        # --- SQL pre-filter: ask Snowflake which countries are within 500km ---
        affected_countries = []
        sql_prefilter_used = False
        conn_prefilter = None
        try:
            conn_prefilter = get_snowflake_connection()
            cursor_prefilter = conn_prefilter.cursor()
            sql_countries = get_countries_in_range(cursor_prefilter, storm, date)
            cursor_prefilter.close()
            # Trust SQL result whether empty or not — empty means confirmed out-of-range.
            # Only fall back to Python if the query itself raises (connection/auth failure).
            affected_countries = [c for c in sql_countries if c in countries]
            sql_prefilter_used = True
            if affected_countries:
                logger.info(f"SQL pre-filter: {len(affected_countries)} country/countries in range: {', '.join(affected_countries)}")
            else:
                logger.info("SQL pre-filter: no countries within 500km — skipping storm")
        except Exception as e:
            logger.warning(f"SQL pre-filter failed ({e}) — falling back to Python buffer check")
        finally:
            if conn_prefilter is not None:
                conn_prefilter.close()

        # --- Python fallback: 500km buffer per country (original logic) ---
        if not sql_prefilter_used:
            logger.info("Checking which countries are affected (500km buffer per country)...")
            country_boundaries = get_country_boundaries(countries)

            for i, country in enumerate(countries):
                country_boundary = country_boundaries[i]
                country_gdf = gpd.GeoDataFrame(geometry=[country_boundary], crs='EPSG:4326')

                country_buffered = buffer_geodataframe(country_gdf, buffer_distance_meters=500000)
                country_buffered_geom = country_buffered.geometry.iloc[0]

                bounds = country_buffered_geom.bounds
                if any(not (isinstance(b, (int, float)) and -1000 < b < 1000) for b in bounds):
                    logger.debug(f"Buffer geometry for {country} has invalid bounds, attempting to fix...")
                    try:
                        country_buffered_geom = country_buffered_geom.buffer(0)
                        bounds = country_buffered_geom.bounds
                    except Exception:
                        logger.debug(f"Could not fix buffer geometry for {country}, using original boundary")
                        country_buffered_geom = country_boundary

                if not country_buffered_geom.is_valid:
                    from shapely.validation import make_valid
                    try:
                        country_buffered_geom = make_valid(country_buffered_geom)
                    except Exception:
                        try:
                            country_buffered_geom = country_buffered_geom.buffer(0)
                        except Exception:
                            logger.debug(f"Could not create valid buffered geometry for {country}, using unbuffered")
                            country_buffered_geom = country_boundary

                if is_envelope_in_zone(country_buffered_geom, gdf_envelopes):  # Python fallback path
                    affected_countries.append(country)
                    bounds = country_buffered_geom.bounds
                    if bounds[2] - bounds[0] > 180:
                        logger.info(f"  {country}: Affected (buffer crosses dateline)")
                    else:
                        logger.info(f"  {country}: Affected")
                else:
                    logger.info(f"  {country}: Not affected (skipping)")
        
        if not affected_countries:
            logger.info("Envelopes do not intersect with any of the specified countries (within 500km buffer) — skipping")
            return {"success": True, "skipped": True, "envelopes_processed": 0, "countries_processed": 0, "total_views_created": 0, "affected_countries": []}
        
        logger.info(f"Processing {len(affected_countries)} affected country/countries: {', '.join(affected_countries)}")
        
        # Create impact views only for affected countries
        logger.info("Creating impact views for affected countries...")
        total_views = 0
        country_errors = []
        succeeded_countries = []
        any_base_parquet_written = False
        for country in affected_countries:
            try:
                wrote_base, country_files_written = create_views_from_envelopes_in_country(
                    country, storm, date, gdf_envelopes, zoom, gdf_envelopes_gust=gdf_envelopes_gust)
                if wrote_base:
                    any_base_parquet_written = True
                total_views += country_files_written
                succeeded_countries.append(country)
            except Exception as country_exc:
                import traceback as _tb
                logger.error(f"Pipeline with errors for storm {storm} at {date}")
                logger.error(f"  {country}: {str(country_exc)}")
                logger.debug(_tb.format_exc())
                country_errors.append(f"{country}: {str(country_exc)}")

        if country_errors and not succeeded_countries:
            # Every country failed — treat as full failure so the run stays eligible for retry
            return {"success": False, "error": "; ".join(country_errors)}

        if country_errors:
            logger.warning(f"Impact analysis completed with {len(country_errors)} country error(s): {'; '.join(country_errors)}")
        else:
            logger.info("Impact analysis completed successfully")

        # If any emergency fallback wrote a base parquet during this update run,
        # refresh the base layer MATs immediately (they are normally only refreshed
        # after --type initialize or --type patch).
        if any_base_parquet_written and os.environ.get("DATA_PIPELINE_DB", "LOCAL").upper() == "SNOWFLAKE":
            _conn = None
            try:
                _conn = get_snowflake_connection()
                _cur = _conn.cursor()
                _cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS REFRESH")
                _cur.execute("CALL AOTS.TC_ECMWF.REFRESH_BASE_LAYER_TABLES()")
                _result = _cur.fetchone()[0]
                _cur.close()
                if _result.startswith('PARTIAL') or 'errors:' in _result:
                    logger.warning(f"Base layer MAT refresh had failures after emergency fallback: {_result}")
                else:
                    logger.info(f"Base layer MATs refreshed after emergency fallback during update: {_result}")
            except Exception as e:
                logger.error(f"Could not refresh base layer tables after emergency fallback: {e}")
            finally:
                if _conn is not None:
                    _conn.close()

        return {
            "success": True,
            "envelopes_processed": len(gdf_envelopes),
            "countries_processed": len(succeeded_countries),
            "total_views_created": total_views,
            "affected_countries": succeeded_countries,
            "country_errors": country_errors,
        }

    except Exception as e:
        import traceback
        logger.error(f"Error during impact analysis: {str(e)}")
        logger.error(traceback.format_exc())
        return {"success": False, "error": str(e)}


# =============================================================================
# PRECIPITATION/RUNOFF ANALYSIS (storm-independent)
# =============================================================================

# Four countable-impact windows, hours from T+0. 6h is the standard
# flash-flood-response window; 24h short-term outlook; 72h "next few days"
# (close to GloFAS's own first Flood Summary tier); 120h (5 days) matches
# the OCHA ROWCA reference used to anchor the tp thresholds below.
PRECIP_WINDOWS_H = [6, 24, 72, 120]

# tp exceedance thresholds (mm), per window: moderate/heavy/extreme.
# Heavy@6h (50mm) is the UK Extreme Rainfall Alert figure; the whole 120h row
# is OCHA ROWCA's own moderate/heavy/extreme scale. Both real, independently
# verifiable anchors. Every other cell is a power-law fit (E = alpha * D^beta)
# sharing one beta=0.2313 (fit from the only tier with two real anchors,
# heavy), scaled per tier from its own real 120h anchor. See the "Thresholds
# and aggregation windows" section of the precip integration plan for the
# full derivation and sourcing.
PRECIP_TP_THRESHOLDS_MM = {
    6:   [25, 50, 75],
    24:  [35, 70, 103],
    72:  [45, 90, 133],
    120: [50, 100, 150],
}

# ro/tp ratio exceedance tiers: dimensionless cut points on the ro/tp ratio,
# not a fixed mm threshold (no real flood-forecasting system uses one, see
# WMO's Global Flash Flood Guidance System). Grounded in the Rational Method
# runoff coefficient (C) reference table (civil-engineering stormwater
# design standard): 0.3 sits at the entry point of "meaningfully elevated"
# runoff response, 0.6 at "majority of the rain becomes runoff".
RATIO_THRESHOLDS = [0.3, 0.6]

# Below this accumulated tp, a member's ro/tp ratio is treated as 0
# (negligible rain means negligible runoff-response signal from that
# member, not an excluded observation).
RATIO_MIN_TP_MM = 5.0


def run_precip_analysis(countries, logger, zoom=14, target_date=None):
    """
    Storm-independent precipitation/runoff analysis, run once per --type
    update invocation, not once per storm. Reads the tp/ro Zarr forecast
    from MET_FORECASTS (ambient global data, produced every pipeline cycle
    regardless of storm activity) and produces tp exceedance-probability
    tiles/admin views plus ro/tp ratio exceedance-probability tiles/admin
    views, at four windows.

    A precip failure never affects storm (wind/gust) processing and vice
    versa.

    Args:
        countries: List of ISO3 country codes to process (every initialized
            country, not a storm's affected-country list — precip applies
            unconditionally everywhere).
        logger: Logger instance.
        zoom: Mercator tile zoom level (default: 14, matches wind/gust).
        target_date: Optional specific date (YYYY-MM-DD or date) to backfill
            against, mirroring update_storms()'s own target_date. MET_FORECASTS
            retains one row per past forecast cycle (same as
            TC_ENVELOPES_COMBINED retains historical envelopes), so a
            historical cycle's Zarr file is genuinely available to re-read.
            None (default) uses the latest available cycle, matching normal
            (non-backfill) --type update behavior.
    """
    met_data_store = get_hazard_met_data_store()

    if target_date is not None:
        tp_row = get_met_forecast_for_date('tp', target_date)
        ro_row = get_met_forecast_for_date('ro', target_date)
    else:
        tp_row = get_latest_met_forecast('tp')
        ro_row = get_latest_met_forecast('ro')
    if tp_row is None and ro_row is None:
        if target_date is not None:
            logger.info(f"No MET_FORECASTS data available for {target_date}, skipping precip analysis")
        else:
            logger.info("No MET_FORECASTS data available, skipping precip analysis")
        return

    date_label = f"for {target_date}" if target_date is not None else "(latest)"
    if tp_row is not None:
        logger.info(f"tp forecast {date_label}: {tp_row['forecast_time']}")
    if ro_row is not None:
        logger.info(f"ro forecast {date_label}: {ro_row['forecast_time']}")

    # tp and ro are downloaded independently upstream, one param's cycle can
    # fail while the other succeeds, leaving MET_FORECASTS with mismatched
    # "latest" rows per param. Never combine them into a ratio in that case,
    # tp still gets processed on its own below (it doesn't need ro at all).
    if tp_row is not None and ro_row is not None and tp_row['forecast_time'] != ro_row['forecast_time']:
        logger.warning(
            f"tp ({tp_row['forecast_time']}) and ro ({ro_row['forecast_time']}) latest forecasts "
            "are from different cycles, skipping the ro/tp ratio this run (tp itself is unaffected)"
        )
        ro_row = None

    forecast_time = (tp_row or ro_row)['forecast_time']
    if hasattr(forecast_time, 'strftime'):
        forecast_time_str = forecast_time.strftime('%Y%m%d%H%M%S')
    else:
        forecast_time_str = str(forecast_time)

    # Per-country setup (tiles, admin regions, facility locations, facility-
    # to-tile maps) is entirely window-independent, so it's cached here and
    # computed once across all 4 window_h iterations below, not once per
    # window per country (4x redundant Snowflake/blob reads and spatial
    # joins otherwise, caught by a multi-agent review of this function).
    # None means "setup failed for this country, skip it in every window".
    country_setup_cache = {}

    for window_h in PRECIP_WINDOWS_H:
        tp_bounds = tp_grid = None
        ro_grid = None

        if tp_row is not None:
            try:
                _, lat_min, lat_max, lon_min, lon_max, tp_grid = read_precip_window(
                    tp_row['stage_path'], met_data_store, 0, window_h
                )
                tp_bounds = (lat_min, lat_max, lon_min, lon_max)
            except Exception as e:
                logger.warning(f"Failed to read tp window {window_h}h: {e}")

        if ro_row is not None:
            try:
                # ro shares tp's own grid (same ECMWF ENS run), so only its
                # array is kept, tp_bounds is reused for both GeoTIFFs below.
                _, _, _, _, _, ro_grid = read_precip_window(
                    ro_row['stage_path'], met_data_store, 0, window_h
                )
            except Exception as e:
                logger.warning(f"Failed to read ro window {window_h}h: {e}")

        for country in countries:
            if country in country_setup_cache:
                cached = country_setup_cache[country]
                if cached is None:
                    continue  # setup failed for this country on an earlier window, skip here too
                (gdf_tiles, gdf_admin_by_level, gdf_tiles_for_admin_by_level,
                 gdf_schools, gdf_hcs, gdf_shelters, gdf_wash,
                 school_tile_map, hc_tile_map, shelter_tile_map, wash_tile_map) = cached
            else:
                try:
                    gdf_tiles = load_mercator_view(country, zoom)
                except Exception as e:
                    logger.warning(f"Could not load mercator tiles for {country}, skipping precip for this country: {e}")
                    country_setup_cache[country] = None
                    continue

                # Isolated per-country, same as load_mercator_view above: a
                # transient storage hiccup or malformed admin geometry for ONE
                # country must not abort processing for every country after it
                # in this list. Falls back to no admin views for this country
                # this cycle (tile-level tp/ratio processing below doesn't need
                # admin_levels at all, so it still proceeds normally).
                gdf_admin_by_level = {}
                gdf_tiles_for_admin_by_level = {}
                try:
                    admin_levels = get_initialized_admin_levels(country) or [1]
                    for admin_level in admin_levels:
                        try:
                            gdf_admin = load_admin_view(country, admin_level=admin_level)
                        except Exception as e:
                            logger.warning(f"Could not load admin{admin_level} view for {country}, skipping this level: {e}")
                            continue
                        if admin_level == 1:
                            gdf_tiles_for_admin = gdf_tiles
                        else:
                            gdf_admin_boundaries = gdf_admin[['tile_id', 'geometry']].rename(columns={'tile_id': 'id'})
                            gdf_tiles_for_admin = admins_overlay(gdf_admin_boundaries,
                                                                 gdf_tiles.drop(columns=['id'], errors='ignore'))
                        gdf_admin_by_level[admin_level] = gdf_admin
                        gdf_tiles_for_admin_by_level[admin_level] = gdf_tiles_for_admin
                except Exception as e:
                    logger.warning(f"Admin-level setup failed for {country}, proceeding with tile-level views only: {e}")
                    gdf_admin_by_level = {}
                    gdf_tiles_for_admin_by_level = {}

                # Facility locations, fetched and cleaned ONCE per country (not
                # once per window x threshold, unlike wind's own facility create
                # functions which handle dedup/filtering internally since they're
                # each called once per country too, here the threshold loop below
                # calls create_precip_facility_view fresh 20x per country, so
                # doing this setup once upfront avoids redoing it 20x for
                # unchanged data). Isolated per-country, same reasoning as the
                # admin-level setup above: a failure here must not cascade to
                # every subsequent country. Falls back to empty facility
                # GeoDataFrames (tile/admin views above are unaffected either way).
                gdf_schools = gdf_hcs = gdf_shelters = gdf_wash = gpd.GeoDataFrame()
                school_tile_map = hc_tile_map = shelter_tile_map = wash_tile_map = pd.DataFrame()
                try:
                    gdf_schools = _ensure_unique_zone_ids(fetch_schools(country, rewrite=0), 'school_id_giga', 'school')

                    gdf_hcs_raw = fetch_health_centers(country, rewrite=0)
                    if not gdf_hcs_raw.empty:
                        mask = pd.Series(False, index=gdf_hcs_raw.index)
                        for col, values in HC_FACILITY_TYPES.items():
                            if col in gdf_hcs_raw.columns:
                                mask |= gdf_hcs_raw[col].isin(values)
                        gdf_hcs_raw = gdf_hcs_raw[mask].copy()
                    gdf_hcs = _ensure_unique_zone_ids(gdf_hcs_raw, 'osm_id', 'health center')

                    gdf_shelters = _ensure_unique_zone_ids(fetch_shelters(country, rewrite=0), 'osm_id', 'shelter')
                    gdf_wash = _ensure_unique_zone_ids(fetch_wash(country, rewrite=0), 'osm_id', 'WASH facility')

                    # Facility-to-tile mapping, also computed ONCE per country:
                    # precip's own probability is routed through each facility's
                    # containing tile (its native grid cell is far coarser than a
                    # tile, so a facility must always agree with the tile it
                    # physically sits inside), rather than resampling the raster
                    # independently per facility.
                    if not gdf_schools.empty:
                        school_tile_map = assign_facilities_to_tiles(gdf_schools, gdf_tiles, 'school_id_giga')
                    if not gdf_hcs.empty:
                        hc_tile_map = assign_facilities_to_tiles(gdf_hcs, gdf_tiles, 'osm_id')
                    if not gdf_shelters.empty:
                        shelter_tile_map = assign_facilities_to_tiles(gdf_shelters, gdf_tiles, 'osm_id')
                    if not gdf_wash.empty:
                        wash_tile_map = assign_facilities_to_tiles(gdf_wash, gdf_tiles, 'osm_id')
                except Exception as e:
                    logger.warning(f"Facility fetch failed for {country}, proceeding without facility-level precip views: {e}")
                    gdf_schools = gdf_hcs = gdf_shelters = gdf_wash = gpd.GeoDataFrame()
                    school_tile_map = hc_tile_map = shelter_tile_map = wash_tile_map = pd.DataFrame()

                country_setup_cache[country] = (
                    gdf_tiles, gdf_admin_by_level, gdf_tiles_for_admin_by_level,
                    gdf_schools, gdf_hcs, gdf_shelters, gdf_wash,
                    school_tile_map, hc_tile_map, shelter_tile_map, wash_tile_map,
                )

            if tp_grid is not None:
                for threshold_mm in PRECIP_TP_THRESHOLDS_MM[window_h]:
                    try:
                        probability_grid = exceedance_probability(tp_grid, threshold_mm)
                        with grid_to_geotiff_to_tifprocessor(probability_grid, *tp_bounds) as tif:
                            tile_view = create_precip_tile_view(gdf_tiles, tif)
                            save_precip_tile_view(tile_view, country, forecast_time_str, threshold_mm, window_h)
                            for admin_level in gdf_admin_by_level:
                                admin_view = create_precip_admin_view(
                                    gdf_admin_by_level[admin_level], gdf_tiles_for_admin_by_level[admin_level], tif
                                )
                                save_precip_admin_view(admin_view, country, forecast_time_str, threshold_mm,
                                                       window_h, admin_level=admin_level)

                            # Facility-level views: each facility's exposure is its
                            # containing tile's already-computed probability (tile_view,
                            # above), not an independent raster sample, so no raster
                            # access happens here at all. Empty coverage (WASH/shelters
                            # are frequently sparse in OSM) is skipped entirely rather than
                            # writing a zero-row file.
                            if not gdf_schools.empty:
                                school_view = create_precip_facility_view(gdf_schools, school_tile_map, tile_view, 'school_id_giga')
                                save_precip_school_view(school_view, country, forecast_time_str, threshold_mm, window_h)
                            if not gdf_hcs.empty:
                                hc_view = create_precip_facility_view(gdf_hcs, hc_tile_map, tile_view, 'osm_id')
                                save_precip_hc_view(hc_view, country, forecast_time_str, threshold_mm, window_h)
                            if not gdf_shelters.empty:
                                shelter_view = create_precip_facility_view(gdf_shelters, shelter_tile_map, tile_view, 'osm_id')
                                save_precip_shelter_view(shelter_view, country, forecast_time_str, threshold_mm, window_h)
                            if not gdf_wash.empty:
                                wash_view = create_precip_facility_view(gdf_wash, wash_tile_map, tile_view, 'osm_id')
                                save_precip_wash_view(wash_view, country, forecast_time_str, threshold_mm, window_h)
                    except Exception as e:
                        logger.warning(f"tp view failed for {country}/{window_h}h/{threshold_mm}mm: {e}")

            if tp_grid is not None and ro_grid is not None:
                for ratio_threshold in RATIO_THRESHOLDS:
                    try:
                        probability_grid = ratio_exceedance_probability(
                            ro_grid, tp_grid, ratio_threshold, min_tp_mm=RATIO_MIN_TP_MM
                        )
                        with grid_to_geotiff_to_tifprocessor(probability_grid, *tp_bounds) as tif:
                            tile_view = create_precip_tile_view(gdf_tiles, tif)
                            save_precip_ratio_view(tile_view, country, forecast_time_str, ratio_threshold, window_h)
                            for admin_level in gdf_admin_by_level:
                                admin_view = create_precip_admin_view(
                                    gdf_admin_by_level[admin_level], gdf_tiles_for_admin_by_level[admin_level], tif
                                )
                                save_precip_ratio_admin_view(admin_view, country, forecast_time_str, ratio_threshold,
                                                             window_h, admin_level=admin_level)

                            # Facility-level views: same tile-routed lookup as the tp loop above.
                            if not gdf_schools.empty:
                                school_view = create_precip_facility_view(gdf_schools, school_tile_map, tile_view, 'school_id_giga')
                                save_precip_ratio_school_view(school_view, country, forecast_time_str, ratio_threshold, window_h)
                            if not gdf_hcs.empty:
                                hc_view = create_precip_facility_view(gdf_hcs, hc_tile_map, tile_view, 'osm_id')
                                save_precip_ratio_hc_view(hc_view, country, forecast_time_str, ratio_threshold, window_h)
                            if not gdf_shelters.empty:
                                shelter_view = create_precip_facility_view(gdf_shelters, shelter_tile_map, tile_view, 'osm_id')
                                save_precip_ratio_shelter_view(shelter_view, country, forecast_time_str, ratio_threshold, window_h)
                            if not gdf_wash.empty:
                                wash_view = create_precip_facility_view(gdf_wash, wash_tile_map, tile_view, 'osm_id')
                                save_precip_ratio_wash_view(wash_view, country, forecast_time_str, ratio_threshold, window_h)
                    except Exception as e:
                        logger.warning(f"ro/tp ratio view failed for {country}/{window_h}h/{ratio_threshold}: {e}")

    logger.info(f"Precip analysis complete for {len(countries)} countries across {len(PRECIP_WINDOWS_H)} windows")


# =============================================================================
# PIPELINE STATISTICS CLASS
# =============================================================================
class ImpactPipelineStats:
    """Track pipeline execution statistics"""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.analysis_success = False
        self.countries_processed = 0
        self.views_created = 0
        self.affected_countries = []
        self.errors = []
        self.country_errors = []
    
    def log_summary(self, logger):
        """Log pipeline execution summary"""
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else 0
        
        logger.info("=" * 70)
        logger.info("IMPACT ANALYSIS PIPELINE SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Execution time: {duration:.2f} seconds")
        logger.info(f"Impact analysis: {'SUCCESS' if self.analysis_success else 'FAILED'}")
        logger.info(f"Countries processed: {self.countries_processed}")
        logger.info(f"Views created: {self.views_created}")
        
        if self.errors:
            logger.error("Errors encountered:")
            for error in self.errors:
                logger.error(f"  - {error}")
        
        logger.info("=" * 70)


# =============================================================================
# PIPELINE EXECUTION FUNCTIONS
# =============================================================================
def run_hurricane_pipeline(storm, forecast_time, countries=None, skip_analysis=False, log_level="INFO", zoom=14, skip_gust=False):
    """
    Run the complete hurricane impact analysis pipeline for a single storm/forecast.

    This function orchestrates the impact analysis process, including data loading,
    geospatial processing, and view generation. It tracks execution statistics
    and handles errors gracefully.

    Args:
        storm: Storm name (e.g., 'FUNG-WONG', 'JERRY')
        forecast_time: Forecast time in YYYYMMDDHHMMSS format or 'YYYY-MM-DD HH:MM:SS' format
        countries: List of ISO3 country codes. If None, uses default list.
        skip_analysis: If True, skip the analysis step (useful for testing)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.
        zoom: Zoom level for mercator tiles. Default: 14.
        skip_gust: If True, skip gust envelope processing even if gust data is
            available (wind processing is unaffected either way)
    
    Returns:
        ImpactPipelineStats: Pipeline execution statistics object containing:
            - analysis_success (bool): Whether analysis completed successfully
            - countries_processed (int): Number of countries processed
            - views_created (int): Number of views created
            - errors (list): List of error messages if any
            - start_time, end_time: Execution timestamps
    """
    logger = setup_logging(log_level)
    stats = ImpactPipelineStats()
    stats.start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info("HURRICANE IMPACT ANALYSIS PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Storm: {storm}")
    logger.info(f"Forecast Time: {forecast_time}")
    logger.info(f"Countries: {countries}")
    logger.info(f"Skip Analysis: {skip_analysis}")
    logger.info("=" * 70)
    
    try:
        # Step 1: Impact Analysis (reads directly from Snowflake)
        if not skip_analysis:
            logger.info("STEP 1: Impact Analysis")
            logger.info("-" * 50)
            
            # Convert forecast time to the format expected by impact analysis
            if isinstance(forecast_time, str) and len(forecast_time) == 19:  # "2025-10-10 00:00:00"
                # Convert to YYYYMMDDHHMMSS format
                dt = datetime.strptime(forecast_time, "%Y-%m-%d %H:%M:%S")
                analysis_date = dt.strftime("%Y%m%d%H%M%S")
            else:
                analysis_date = forecast_time
            
            # Run complete impact analysis orchestration
            analysis_result = run_complete_impact_analysis(storm, analysis_date, countries, logger, zoom, skip_gust=skip_gust)
            
            if analysis_result["success"]:
                stats.analysis_success = True
                stats.countries_processed = analysis_result["countries_processed"]
                stats.views_created = analysis_result["total_views_created"]
                stats.affected_countries = analysis_result["affected_countries"]
                # country_errors can be non-empty even when success is True (at least
                # one country succeeded, so the run stays eligible to avoid a full
                # retry), surface it into stats.errors so it's visible in the log
                # summary and doesn't get silently reported as a clean full success.
                stats.country_errors = analysis_result.get("country_errors", [])
                if stats.country_errors:
                    stats.errors.extend(stats.country_errors)
                if analysis_result.get("skipped"):
                    logger.info("Impact analysis skipped — storm not in range of any country")
                elif stats.country_errors:
                    logger.warning(
                        f"Impact analysis completed with {len(stats.country_errors)} "
                        f"country error(s), other countries succeeded"
                    )
                    logger.info(f"   Envelopes processed: {analysis_result['envelopes_processed']}")
                    logger.info(f"   Countries processed: {stats.countries_processed}")
                    logger.info(f"   Views created: {stats.views_created}")
                else:
                    logger.info(f"Impact analysis completed successfully")
                    logger.info(f"   Envelopes processed: {analysis_result['envelopes_processed']}")
                    logger.info(f"   Countries processed: {stats.countries_processed}")
                    logger.info(f"   Views created: {stats.views_created}")
            else:
                stats.analysis_success = False
                stats.errors.append(f"Analysis failed: {analysis_result['error']}")
                logger.error(f"Impact analysis failed: {analysis_result['error']}")
        else:
            logger.info("STEP 1: Impact Analysis SKIPPED")
            logger.info("-" * 50)
            stats.analysis_success = True  # Mark as success since we skipped it
            logger.info("Impact analysis step skipped")
    
        # Pipeline completion
        stats.end_time = datetime.now()
        
        if stats.analysis_success:
            logger.info("Pipeline completed successfully")
        else:
            logger.error("Pipeline completed with errors")
        
        stats.log_summary(logger)
        return stats
        
    except Exception as e:
        stats.end_time = datetime.now()
        stats.errors.append(f"Pipeline execution error: {str(e)}")
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        stats.log_summary(logger)
        return stats


# =============================================================================
# INITIALIZATION FUNCTIONS
# =============================================================================
def initialize_pipeline(countries, zoom, rewrite, admin_levels=None):
    """
    Initialize the data pipeline by creating base mercator and admin views.

    This function creates the foundational geospatial data layers needed for impact
    analysis, including mercator tiles with demographic data and admin-level boundaries.
    The data is cached after first creation to avoid redundant downloads.

    If DATA_PIPELINE_DB=SNOWFLAKE and a country is not yet in PIPELINE_COUNTRIES,
    it is automatically added with ACTIVE=TRUE before initialization proceeds.

    Args:
        countries: List of ISO3 country codes (e.g., ['TWN', 'DOM'])
        zoom: Zoom level for mercator tiles (typically 14)
        rewrite: If 1, regenerate existing views; if 0, skip if they exist
        admin_levels: List of admin levels to generate base admin views for (default: [1])

    Returns:
        ImpactPipelineStats: Statistics object with analysis_success=True
    """
    if admin_levels is None:
        admin_levels = [1]
    stats = ImpactPipelineStats()

    if os.environ.get("DATA_PIPELINE_DB", "LOCAL").upper() == "SNOWFLAKE":
        for country in countries:
            added = add_country_to_snowflake(
                country_code=country,
                zoom_level=zoom,
            )
            if added:
                logger.info(f"{country}: auto-added to PIPELINE_COUNTRIES (map config will be set automatically from GeoRepo boundary — override via 'Update Country Config' workflow if needed)")

    save_mercator_and_admin_views(countries, zoom, rewrite, admin_levels=admin_levels)
    stats.analysis_success = True

    if os.environ.get("DATA_PIPELINE_DB", "LOCAL").upper() == "SNOWFLAKE":
        conn = None
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS REFRESH")
            cur.execute("CALL AOTS.TC_ECMWF.REFRESH_BASE_LAYER_TABLES()")
            result = cur.fetchone()[0]
            cur.close()
            if result.startswith('PARTIAL') or 'errors:' in result:
                logger.warning(f"Base layer MAT refresh had failures after initialize: {result}")
            else:
                logger.info(f"Base layer MAT tables refreshed after initialize: {result}")
        except Exception as e:
            logger.error(f"Could not refresh base layer tables after initialize: {e}")
        finally:
            if conn is not None:
                conn.close()

    return stats


# =============================================================================
# PATCH FUNCTIONS
# =============================================================================
def patch_pipeline(countries, zoom, columns, log_level="INFO"):
    """
    Backfill specific optional columns in existing mercator parquets without full re-init.

    For each country, calls patch_country_layer() which:
    - Checks for custom CSVs in geodb/custom/ first (takes priority over raster re-processing)
    - Re-runs raster processing for any columns without a custom CSV
    - Re-derives smod_class_l1 whenever smod_class is patched

    Supported columns: population, school_age_population, infant_population, adolescent_population,
    built_surface_m2, smod_class, smod_class_l1, rwi, schools, hcs, shelters, wash, vulnerability,
    admin<N> (e.g. admin2 — creates a new base admin parquet for that level)

    For 'vulnerability': reads pre-computed poverty probability data from geodb/vulnerability/
    (generated by vulnerability/fetch_vulnerability_probs.py) and writes moderate_poverty_prob
    and severe_poverty_prob into the base mercator parquet.

    Args:
        countries: List of ISO3 country codes (e.g., ['PNG', 'FJI'])
        zoom: Zoom level matching the existing mercator parquet (typically 14)
        columns: List of column names to patch
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.

    Returns:
        bool: True if all countries patched successfully, False if any failed.
    """
    logger = setup_logging(log_level)
    logger.info(f"Patch mode: updating columns {columns} for countries {countries}")
    all_ok = True
    patched = []
    for country in countries:
        try:
            patch_country_layer(country, zoom, columns)
            patched.append(country)
        except (FileNotFoundError, ValueError) as e:
            logger.error(f"{country}: Patch failed — {e}")
            all_ok = False
        except Exception as e:
            logger.error(f"{country}: Unexpected error during patch — {e}", exc_info=True)
            all_ok = False

    if patched and os.environ.get("DATA_PIPELINE_DB", "LOCAL").upper() == "SNOWFLAKE":
        conn = None
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS REFRESH")
            cur.execute("CALL AOTS.TC_ECMWF.REFRESH_BASE_LAYER_TABLES()")
            result = cur.fetchone()[0]
            cur.close()
            if result.startswith('PARTIAL') or 'errors:' in result:
                logger.warning(f"Base layer MAT refresh had failures after patch: {result}")
                all_ok = False
            else:
                logger.info(f"Base layer MAT tables refreshed after patch: {result}")
        except Exception as e:
            logger.error(f"Could not refresh base layer tables after patch: {e}")
            all_ok = False
        finally:
            if conn is not None:
                conn.close()

    return all_ok


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
    for (countries_processed_raw,) in rows:
        if isinstance(countries_processed_raw, str):
            processed = set(json.loads(countries_processed_raw))
        elif isinstance(countries_processed_raw, (list, tuple)):
            processed = set(countries_processed_raw)
        else:
            processed = set()
        if requested.issubset(processed):
            return True
    return False


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
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG
            (STORM_ID, FORECAST_TIME, STATUS, COUNTRIES_PROCESSED, STARTED_AT)
        SELECT %s, %s, 'IN_PROGRESS', PARSE_JSON(%s), CURRENT_TIMESTAMP()
    """, (storm_id, forecast_time, json.dumps(countries or [])))
    conn.commit()
    cur.close()


def log_run_complete(conn, storm_id: str, forecast_time, success: bool,
                     countries: list = None, files_written: int = 0,
                     error_message: str = None, started_at=None) -> None:
    """Insert a SUCCESS or FAILURE completion record into TC_PIPELINE_RUN_LOG."""
    runtime_seconds = None
    if started_at:
        runtime_seconds = (datetime.now() - started_at).total_seconds()
    status = 'SUCCESS' if success else 'FAILURE'
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_PIPELINE_RUN_LOG
            (STORM_ID, FORECAST_TIME, STATUS, COUNTRIES_PROCESSED, FILES_WRITTEN,
             ERROR_MESSAGE, STARTED_AT, COMPLETED_AT, RUNTIME_SECONDS)
        SELECT %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, CURRENT_TIMESTAMP(), %s
    """, (
        storm_id,
        forecast_time,
        status,
        json.dumps(countries or []),
        files_written,
        error_message,
        started_at,
        runtime_seconds,
    ))
    conn.commit()
    cur.close()


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
    # Force the stage directory table to sync before signalling completion.
    # Without this, AOTS_ANALYSIS_FILE_STREAM may not yet reflect newly PUT
    # files when the refresh task fires, causing a silent missed refresh.
    cur.execute("ALTER STAGE AOTS.TC_ECMWF.AOTS_ANALYSIS REFRESH")
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
# UPDATE FUNCTIONS
# =============================================================================
def update_storms(countries, skip_analysis, log_level, zoom, rewrite, time_delta, target_date=None, target_storm=None, skip_gust=False):
    """
    Update pipeline: Process hurricane data from Snowflake for matching storms.

    This function:
    1. Fetches storm data from Snowflake
    2. Filters by date and/or storm name if specified
    3. Processes each matching storm/forecast combination
    4. Skips already-processed storms unless rewrite=1
    5. Tracks processing status:
         - DATA_PIPELINE_DB=SNOWFLAKE: TC_PIPELINE_RUN_LOG table (per storm_id/forecast_time)
         - LOCAL / BLOB: JSON file (storms.json in the results directory)

    Admin levels processed are determined automatically by which base admin parquets
    exist for each country (initialized with --type initialize [--admin N ...]).

    Args:
        countries: List of ISO3 country codes to process
        skip_analysis: If True, skip the analysis step (for testing)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        zoom: Zoom level for mercator tiles
        rewrite: If 1, reprocess existing storms; if 0, skip already processed
        time_delta: Number of days in the past to consider storms (default: 2)
        target_date: Optional specific date to filter (YYYY-MM-DD format). Overrides time_delta.
        target_storm: Optional specific storm name to filter (e.g., 'FUNG-WONG')
        skip_gust: If True, skip gust envelope processing even if gust data is
            available (wind processing is unaffected either way)

    Returns:
        ImpactPipelineStats: Statistics object with execution results
    """
    logger = setup_logging(log_level)

    if not countries:
        logger.error("No countries specified — nothing to process")
        stats = ImpactPipelineStats()
        stats.errors.append("No countries specified")
        return stats

    snowflake_mode = os.environ.get('DATA_PIPELINE_DB', 'LOCAL').upper() == 'SNOWFLAKE'

    # Tracking state — only one is used depending on mode
    d = None        # JSON tracking (LOCAL / BLOB)
    conn = None     # Snowflake connection (SNOWFLAKE mode)

    if snowflake_mode:
        try:
            conn = get_snowflake_connection()
        except Exception as e:
            logger.warning(f"Could not open Snowflake connection for run logging: {e}")
    else:
        d = load_json_storms()

    stats = ImpactPipelineStats()
    stats.analysis_success = True
    update_start_time = datetime.now()

    storms_df = get_snowflake_data()
    storms_df['DATE'] = pd.to_datetime(storms_df['FORECAST_TIME']).dt.date
    storms_df['TIME'] = pd.to_datetime(storms_df['FORECAST_TIME']).dt.strftime('%H:%M')

    if target_date:
        try:
            target_date_obj = pd.to_datetime(target_date).date() if isinstance(target_date, str) else target_date
        except (ValueError, TypeError) as e:
            # Give a clean, actionable error instead of a raw pandas/dateutil
            # traceback: run_precip_analysis()'s own use of this same --date
            # value (a few lines later in main()) is already wrapped in
            # try/except, this path previously wasn't, an invalid --date
            # crashed the whole script here instead.
            logger.error(f"Invalid --date value '{target_date}' (expected YYYY-MM-DD): {e}")
            stats.errors.append(f"Invalid --date value '{target_date}': {e}")
            stats.analysis_success = False
            if conn:
                conn.close()
            return stats
        storms_df = storms_df[storms_df['DATE'] == target_date_obj]
        logger.info(f"Filtering to storms on {target_date_obj} only")

    if target_storm:
        storms_df = storms_df[storms_df['TRACK_ID'] == target_storm]
        logger.info(f"Filtering to storm {target_storm} only")

    if storms_df.empty:
        logger.warning("No storms found matching the specified filters (date and/or storm name)")
        if conn:
            conn.close()
        return stats

    storms_processed = False
    completed_storm_ids = []
    completed_countries = set()
    total_files_written = 0

    for _, row in storms_df.iterrows():
        storm = row['TRACK_ID']
        forecast_date = row['DATE']
        forecast_time_str = row['TIME']
        forecast_time_ts = pd.to_datetime(row['FORECAST_TIME']).to_pydatetime()
        today = datetime.today().date()

        if not (target_date or (today - forecast_date).days < time_delta):
            logger.debug(f"Forecast date {forecast_date} outside time delta ({time_delta} days)")
            continue

        date_str = str(forecast_date).replace('-', '')
        time_str = forecast_time_str.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"

        # --- Deduplication check ---
        already_done = False
        if snowflake_mode and conn:
            try:
                already_done = is_already_processed(conn, storm, forecast_time_ts, countries)
            except Exception as e:
                logger.warning(f"Could not check TC_PIPELINE_RUN_LOG: {e}")
        elif d is not None:
            countries_key = ','.join(sorted(countries))
            storm_key = f"{storm}|{countries_key}"
            already_done = (
                storm_key in d['storms']
                and forecast_datetime_str in d['storms'][storm_key]
            )

        if already_done and rewrite != 1:
            logger.info(f"Storm {storm} at {forecast_datetime_str} already processed (use --rewrite 1 to reprocess)")
            continue

        # --- Log start ---
        if snowflake_mode and conn:
            try:
                log_run_start(conn, storm, forecast_time_ts, countries)
            except Exception as e:
                logger.warning(f"Could not log run start to TC_PIPELINE_RUN_LOG: {e}")

        run_started_at = datetime.now()
        storms_processed = True

        loop_stats = run_hurricane_pipeline(
            storm=storm,
            forecast_time=forecast_datetime_str,
            countries=countries,
            skip_analysis=skip_analysis,
            log_level=log_level,
            zoom=zoom,
            skip_gust=skip_gust
        )

        if loop_stats.analysis_success:
            if loop_stats.countries_processed == 0:
                logger.info(f"Storm {storm} at {forecast_datetime_str} — not in range of any country, skipped")
            elif loop_stats.country_errors:
                logger.warning(
                    f"Pipeline completed with {len(loop_stats.country_errors)} country error(s) "
                    f"for storm {storm} at {forecast_datetime_str}, other countries succeeded"
                )
            else:
                logger.info(f"Pipeline completed successfully for storm {storm} at {forecast_datetime_str}")
            stats.countries_processed += loop_stats.countries_processed
            stats.views_created += loop_stats.views_created
            stats.affected_countries.extend(loop_stats.affected_countries)

            # --- Mark success ---
            if snowflake_mode and conn:
                try:
                    # country_errors can be non-empty even here (success=True stays,
                    # matching the existing "at least one country succeeded, don't
                    # force a full retry" design), but the per-country failure detail
                    # must still be visible in the persisted audit trail, not silently
                    # dropped just because the overall run counts as a success.
                    partial_error_msg = (
                        '; '.join(loop_stats.country_errors) if loop_stats.country_errors else None
                    )
                    log_run_complete(
                        conn, storm, forecast_time_ts, success=True,
                        countries=loop_stats.affected_countries,
                        files_written=loop_stats.views_created,
                        error_message=partial_error_msg,
                        started_at=run_started_at,
                    )
                except Exception as e:
                    logger.warning(f"Could not log run success to TC_PIPELINE_RUN_LOG: {e}")
            elif d is not None:
                countries_key = ','.join(sorted(countries))
                storm_key = f"{storm}|{countries_key}"
                if storm_key not in d['storms']:
                    d['storms'][storm_key] = []
                d['storms'][storm_key].append(forecast_datetime_str)

            if loop_stats.countries_processed > 0 and storm not in completed_storm_ids:
                completed_storm_ids.append(storm)
            completed_countries.update(loop_stats.affected_countries)
            total_files_written += loop_stats.views_created

        else:
            logger.error(f"Pipeline with errors for storm {storm} at {forecast_datetime_str}")
            stats.analysis_success = False
            stats.errors.extend(loop_stats.errors)

            # --- Mark failure ---
            if snowflake_mode and conn:
                try:
                    error_msg = '; '.join(loop_stats.errors) if loop_stats.errors else 'Unknown error'
                    log_run_complete(
                        conn, storm, forecast_time_ts, success=False,
                        error_message=error_msg,
                        started_at=run_started_at,
                    )
                except Exception as e:
                    logger.warning(f"Could not log run failure to TC_PIPELINE_RUN_LOG: {e}")

    if not storms_processed:
        logger.info("All matching storms were already processed (use --rewrite 1 to reprocess)")

    # Save JSON tracking for LOCAL / BLOB modes
    if not snowflake_mode and d is not None:
        try:
            save_json_storms(d)
        except Exception as e:
            logger.warning(f"Could not save storms tracking file: {e}")

    # Signal batch completion so *_MAT tables refresh via stream trigger
    if completed_storm_ids:
        try:
            runtime_seconds = int((datetime.now() - update_start_time).total_seconds())
            if conn is None:
                conn = get_snowflake_connection()
            signal_pipeline_complete(
                conn=conn,
                storm_ids=completed_storm_ids,
                countries=list(completed_countries),
                files_written=total_files_written,
                runtime_seconds=runtime_seconds
            )
            logger.info(f"Signalled pipeline completion to Snowflake for storms: {completed_storm_ids} (runtime: {runtime_seconds}s)")
        except Exception as e:
            logger.warning(f"Could not write completion signal to Snowflake: {e}")

    if conn:
        conn.close()

    return stats



# =============================================================================
# MAIN FUNCTION
# =============================================================================
def main():
    """
    Main entry point for the impact analysis pipeline.

    Parses command-line arguments and orchestrates pipeline execution based on
    the specified mode (initialize, update, or patch) and parameters.
    """
    parser = argparse.ArgumentParser(
        description="Hurricane Impact Analysis Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
    # Initialize base data for Taiwan (admin1 only — default)
    python main_pipeline.py --type initialize --countries TWN --zoom 14

    # Initialize with admin1 + admin2 (for countries with good sub-provincial data)
    python main_pipeline.py --type initialize --countries PNG --zoom 14 --admin 1 2

    # Force re-initialization (regenerates all data from scratch)
    python main_pipeline.py --type initialize --countries PNG --rewrite 1

    # Process all recent storms (last 2 days, per --time_delta's default)
    python main_pipeline.py --type update

    # Process storms for a specific date
    python main_pipeline.py --type update --date 2025-11-10

    # Process a specific storm on a specific date
    python main_pipeline.py --type update --date 2025-11-10 --storm FUNG-WONG

    # Backfill optional columns without full re-init
    python main_pipeline.py --type patch --countries PNG --columns built_surface_m2 rwi

    # Add admin2 to a country already initialized with admin1
    python main_pipeline.py --type patch --countries PNG --columns admin2

    # Update population data when a new WorldPop dataset is available
    python main_pipeline.py --type patch --countries PNG --columns population adolescent_population
    """
    )
    
    # ========== Pipeline Mode Arguments ==========
    parser.add_argument(
        "--type",
        type=str,
        default="update",
        choices=["initialize", "update", "patch"],
        help=(
            "Pipeline mode: "
            "'initialize' creates base data layers, "
            "'update' processes storm data, "
            "'patch' backfills specific columns in existing base mercator parquets without full re-init "
            "(use with --columns; default: update)"
        )
    )

    parser.add_argument(
        "--columns",
        nargs="+",
        metavar="COLUMN",
        default=None,
        help=(
            "Columns to patch (only used with --type patch). "
            "Supported: population, school_age_population, infant_population, adolescent_population, "
            "built_surface_m2, smod_class, smod_class_l1, rwi, schools, hcs, shelters, wash, vulnerability. "
            "Use 'vulnerability' to patch moderate_poverty_prob + severe_poverty_prob from geodb/vulnerability/ "
            "(run vulnerability/fetch_vulnerability_probs.py first). "
            "Example: --columns built_surface_m2 rwi"
        )
    )
    
    parser.add_argument(
        "--hazard",
        type=str,
        default="hurricane",
        choices=["hurricane"],
        help="Hazard type to process (currently only 'hurricane' is supported)"
    )
    
    # ========== Data Configuration Arguments ==========
    _DEFAULT_COUNTRIES = ["ATG", "JAM", "BLZ", "NIC", "DOM", "DMA", "GRD", "MSR", "KNA", "LCA", "VCT", "AIA", "VGB"]
    parser.add_argument(
        "--countries",
        nargs="+",
        default=None,
        help="ISO3 country codes to process (e.g., TWN DOM). If not specified, attempts to read from Snowflake PIPELINE_COUNTRIES table. Default: Caribbean countries list."
    )
    
    parser.add_argument(
        "--zoom",
        type=int,
        default=14,
        help="Zoom level for mercator tiles (default: 14). Higher values = finer resolution but more tiles."
    )
    
    parser.add_argument(
        "--rewrite",
        type=int,
        default=0,
        choices=[0, 1],
        help="Rewrite existing data: 1=regenerate existing views, 0=skip if already exists (default: 0)"
    )

    parser.add_argument(
        "--admin",
        nargs="+",
        type=int,
        default=[1],
        metavar="LEVEL",
        help="Admin levels to generate views for (default: 1). E.g. --admin 1 2 generates both admin1 and admin2 views."
    )
    
    # ========== Filtering Arguments (for update mode) ==========
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Process only storms on this specific date (format: YYYY-MM-DD, e.g., '2025-11-10'). Overrides --time_delta."
    )
    
    parser.add_argument(
        "--storm",
        type=str,
        default=None,
        metavar="STORM_NAME",
        help="Process only this specific storm (e.g., 'FUNG-WONG', 'KALMAEGI'). Can be combined with --date."
    )
    
    parser.add_argument(
        "--time_delta",
        type=int,
        default=2,
        help="Number of days in the past to consider storms for analysis (default: 2). Ignored if --date is specified."
    )
    
    # ========== Execution Control Arguments ==========
    parser.add_argument(
        "--skip-analysis",
        action="store_true",
        help="Skip the analysis step (useful for testing pipeline structure without processing data)"
    )

    parser.add_argument(
        "--skip-gust",
        action="store_true",
        help="Skip gust envelope processing even if gust data is available (wind processing is unaffected)"
    )

    parser.add_argument(
        "--skip-precip",
        action="store_true",
        help="Skip precipitation/runoff analysis even if MET_FORECASTS data is available (storm processing is unaffected)"
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level (default: INFO). DEBUG provides detailed output for troubleshooting."
    )
    
    args = parser.parse_args()
    
    # Setup logging first
    logger = setup_logging(args.log_level)

    # If --countries wasn't explicitly passed at all (args.countries is still the
    # None sentinel, not a value-equality check against _DEFAULT_COUNTRIES, which
    # would also silently override an operator who deliberately passed exactly that
    # list), try to get from Snowflake table. This allows GitHub Actions to use
    # Snowflake as source of truth.
    if args.countries is None:
        try:
            logger.info("No countries specified, attempting to read from Snowflake table...")
            countries_from_snowflake = get_active_countries_from_snowflake()
            if countries_from_snowflake:
                args.countries = countries_from_snowflake
                logger.info(f"Using {len(args.countries)} countries from Snowflake: {', '.join(args.countries)}")
            else:
                logger.warning("No active countries found in Snowflake table, using default list")
                args.countries = _DEFAULT_COUNTRIES
        except Exception as e:
            logger.warning(f"Could not read countries from Snowflake: {e}. Using default list.")
            args.countries = _DEFAULT_COUNTRIES
    
    # Run pipeline based on hazard type
    if args.hazard == "hurricane":

        if args.type == "initialize":
            stats = initialize_pipeline(args.countries, args.zoom, args.rewrite, admin_levels=args.admin)
        elif args.type == "update":
            stats = update_storms(
                countries=args.countries,
                skip_analysis=args.skip_analysis,
                log_level=args.log_level,
                zoom=args.zoom,
                rewrite=args.rewrite,
                time_delta=args.time_delta,
                target_date=args.date,
                target_storm=args.storm,
                skip_gust=args.skip_gust
            )

            # Precip/runoff is storm-independent ambient data, run once per
            # invocation alongside (not inside) update_storms(). Wrapped in
            # its own try/except so a precip failure can never affect the
            # storm-processing exit code/stats above, and vice versa.
            if not args.skip_precip:
                try:
                    run_precip_analysis(args.countries, logger, zoom=args.zoom, target_date=args.date)
                except Exception as e:
                    logger.error(f"Precip analysis failed, storm processing above is unaffected: {e}")
            else:
                logger.info("Skipping precip analysis (--skip-precip)")
        elif args.type == "patch":
            if not args.columns:
                logger.error("--type patch requires --columns (e.g. --columns built_surface_m2 rwi)")
                sys.exit(1)
            ok = patch_pipeline(args.countries, args.zoom, args.columns, args.log_level)
            stats = ImpactPipelineStats()
            stats.analysis_success = ok
    else:
        logger.error(f"Hazard type '{args.hazard}' not yet implemented")
        sys.exit(1)
    
    # Exit with appropriate code
    if stats.analysis_success:
        print("\nPipeline completed successfully!")
        sys.exit(0)
    else:
        print("\nPipeline completed with errors!")
        sys.exit(1)


if __name__ == "__main__":
    main()