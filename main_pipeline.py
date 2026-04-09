#!/usr/bin/env python3
"""
Main Impact Analysis Pipeline Orchestrator

Coordinates the complete impact analysis pipeline for tropical cyclone early warning.
Three operating modes: initialize, update, patch.

Key Features:
- initialize: builds country base layers (mercator tiles + admin views) with population,
  built surface, settlement class, wealth index, schools, health centers, shelters, WASH
- update: fetches active storm envelopes from Snowflake and runs geospatial intersection
  against all initialized countries within 1,500 km; generates per-facility and tile-level
  impact views at 8 wind thresholds (34–137 kt) plus JSON reports and CCI values
- patch: backfills specific columns in existing mercator parquets without full
  re-initialization (supported: population, school_age_population, infant_population,
  adolescent_population, built_surface_m2, smod_class, smod_class_l1, rwi,
  schools, hcs, shelters, wash)

- Custom data overrides: place a CSV in geodb/custom/ to replace any API or raster source
  for a specific country — custom files are never overwritten by the pipeline
- Storage-backend agnostic: LOCAL, Azure Blob (ADLS), or Snowflake internal stage

Usage Examples:
    # Initialize base data for a new country
    python main_pipeline.py --type initialize --countries TWN --zoom 14

    # Force re-initialization (regenerates all data from scratch)
    python main_pipeline.py --type initialize --countries PNG --rewrite 1

    # Process all recent storms (default: last 9 days)
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
    is_envelope_in_zone,
    get_country_boundaries,
    create_views_from_envelopes_in_country,
    save_mercator_and_admin_views,
    save_json_storms,
    load_json_storms,
    patch_country_layer,
)

# Import gigaspatial for buffering
from gigaspatial.processing import buffer_geodataframe

import json
from snowflake_utils import get_snowflake_data, get_snowflake_connection, get_countries_in_range
from country_utils import get_active_countries_from_snowflake


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
def run_complete_impact_analysis(storm, date, countries, logger, zoom):
    """
    Complete impact analysis orchestration.
    
    Loads hurricane envelope data from Snowflake, checks which countries are affected
    (using 1500km buffer per country), and creates impact views for affected countries.
    
    Args:
        storm: Storm name (e.g., 'FUNG-WONG', 'JERRY')
        date: Forecast date in YYYYMMDDHHMMSS format (e.g., '20251110000000')
        countries: List of ISO3 country codes (e.g., ['TWN', 'DOM'])
        logger: Logger instance for logging
        zoom: Zoom level for mercator tiles (default: 14)
    
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
            logger.error(f"No envelope data found for {storm} at {date}")
            return {"success": False, "error": "No envelope data found"}
        
        logger.info(f"Loaded {len(gdf_envelopes)} envelope records")
        logger.info("Envelopes already converted to GeoDataFrame")
        
        # --- SQL pre-filter: ask Snowflake which countries are within 1500km ---
        affected_countries = []
        sql_prefilter_used = False
        try:
            conn_prefilter = get_snowflake_connection()
            cursor_prefilter = conn_prefilter.cursor()
            sql_countries = get_countries_in_range(cursor_prefilter, storm, date)
            cursor_prefilter.close()
            conn_prefilter.close()
            # Trust SQL result whether empty or not — empty means confirmed out-of-range.
            # Only fall back to Python if the query itself raises (connection/auth failure).
            affected_countries = [c for c in sql_countries if c in countries]
            sql_prefilter_used = True
            if affected_countries:
                logger.info(f"SQL pre-filter: {len(affected_countries)} country/countries in range: {', '.join(affected_countries)}")
            else:
                logger.info("SQL pre-filter: no countries within 1500km — skipping storm")
        except Exception as e:
            logger.warning(f"SQL pre-filter failed ({e}) — falling back to Python buffer check")

        # --- Python fallback: 1500km buffer per country (original logic) ---
        if not sql_prefilter_used:
            logger.info("Checking which countries are affected (1500km buffer per country)...")
            country_boundaries = get_country_boundaries(countries)

            for i, country in enumerate(countries):
                country_boundary = country_boundaries[i]
                country_gdf = gpd.GeoDataFrame(geometry=[country_boundary], crs='EPSG:4326')

                country_buffered = buffer_geodataframe(country_gdf, buffer_distance_meters=1500000)
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
            logger.error("Envelopes do not intersect with any of the specified countries (within 1500km buffer)")
            return {"success": False, "error": "No intersection with countries"}
        
        logger.info(f"Processing {len(affected_countries)} affected country/countries: {', '.join(affected_countries)}")
        
        # Create impact views only for affected countries
        logger.info("Creating impact views for affected countries...")
        total_views = 0
        for country in affected_countries:
            create_views_from_envelopes_in_country(country, storm, date, gdf_envelopes, zoom)
            total_views += 4  # schools, health centers, tiles, tracks
        
        logger.info("Impact analysis completed successfully")
        return {
            "success": True,
            "envelopes_processed": len(gdf_envelopes),
            "countries_processed": len(affected_countries),
            "total_views_created": total_views,
            "affected_countries": affected_countries
        }
        
    except Exception as e:
        import traceback
        logger.error(f"Error during impact analysis: {str(e)}")
        logger.error(traceback.format_exc())
        return {"success": False, "error": str(e)}


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
def run_hurricane_pipeline(storm, forecast_time, countries=None, skip_analysis=False, log_level="INFO", zoom=14):
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
            analysis_result = run_complete_impact_analysis(storm, analysis_date, countries, logger, zoom)
            
            if analysis_result["success"]:
                stats.analysis_success = True
                stats.countries_processed = analysis_result["countries_processed"]
                stats.views_created = analysis_result["total_views_created"]
                stats.affected_countries = analysis_result["affected_countries"]
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
def initialize_pipeline(countries, zoom, rewrite):
    """
    Initialize the data pipeline by creating base mercator and admin views.
    
    This function creates the foundational geospatial data layers needed for impact
    analysis, including mercator tiles with demographic data and admin-level boundaries.
    The data is cached after first creation to avoid redundant downloads.
    
    Args:
        countries: List of ISO3 country codes (e.g., ['TWN', 'DOM'])
        zoom: Zoom level for mercator tiles (typically 14)
        rewrite: If 1, regenerate existing views; if 0, skip if they exist
    
    Returns:
        ImpactPipelineStats: Statistics object with analysis_success=True
    """
    stats = ImpactPipelineStats()
    save_mercator_and_admin_views(countries, zoom, rewrite)
    stats.analysis_success = True
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
    built_surface_m2, smod_class, smod_class_l1, rwi, schools, hcs, shelters, wash

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
    for country in countries:
        try:
            patch_country_layer(country, zoom, columns)
        except (FileNotFoundError, ValueError) as e:
            logger.error(f"{country}: Patch failed — {e}")
            all_ok = False
        except Exception as e:
            logger.error(f"{country}: Unexpected error during patch — {e}", exc_info=True)
            all_ok = False
    return all_ok


# =============================================================================
# COMPLETION SIGNAL
# =============================================================================
def signal_pipeline_complete(conn, storm_ids: list, countries: list, files_written: int):
    """
    Insert a completion record into TC_PIPELINE_COMPLETE_LOG.
    This triggers the stream-based refresh of *_MAT tables in Snowflake.
    Only called on successful runs.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AOTS.TC_ECMWF.TC_PIPELINE_COMPLETE_LOG
            (STORM_IDS, COUNTRIES_PROCESSED, FILES_WRITTEN, STATUS)
        VALUES (PARSE_JSON(%s), PARSE_JSON(%s), %s, 'SUCCESS')
    """, (
        json.dumps(storm_ids),
        json.dumps(countries),
        files_written
    ))
    conn.commit()
    cur.close()


# =============================================================================
# UPDATE FUNCTIONS
# =============================================================================
def update_storms(countries, skip_analysis, log_level, zoom, rewrite, time_delta, target_date=None, target_storm=None):
    """
    Update pipeline: Process hurricane data from Snowflake for matching storms.
    
    This function:
    1. Fetches storm data from Snowflake
    2. Filters by date and/or storm name if specified
    3. Processes each matching storm/forecast combination
    4. Skips already-processed storms unless rewrite=1
    5. Tracks processing status in JSON file
    
    Args:
        countries: List of ISO3 country codes to process
        skip_analysis: If True, skip the analysis step (for testing)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        zoom: Zoom level for mercator tiles
        rewrite: If 1, reprocess existing storms; if 0, skip already processed
        time_delta: Number of days in the past to consider storms (default: 9)
        target_date: Optional specific date to filter (YYYY-MM-DD format). Overrides time_delta.
        target_storm: Optional specific storm name to filter (e.g., 'FUNG-WONG')
    
    Returns:
        ImpactPipelineStats: Statistics object with execution results
    """
    # Initialize logger first
    logger = setup_logging(log_level)

    if not countries:
        logger.error("No countries specified — nothing to process")
        stats = ImpactPipelineStats()
        stats.errors.append("No countries specified")
        return stats

    d = load_json_storms()
    stats = ImpactPipelineStats()
    stats.analysis_success = True  # assume success; flip to False on any failure

    storms_df = get_snowflake_data()
    storms_df['DATE'] = pd.to_datetime(storms_df['FORECAST_TIME']).dt.date
    storms_df['TIME'] = pd.to_datetime(storms_df['FORECAST_TIME']).dt.strftime('%H:%M')

    # Filter by target_date if specified
    if target_date:
        target_date_obj = pd.to_datetime(target_date).date() if isinstance(target_date, str) else target_date
        storms_df = storms_df[storms_df['DATE'] == target_date_obj]
        logger.info(f"Filtering to storms on {target_date_obj} only")
    
    # Filter by target_storm if specified
    if target_storm:
        storms_df = storms_df[storms_df['TRACK_ID'] == target_storm]
        logger.info(f"Filtering to storm {target_storm} only")

    # Check if any storms match the filters
    if storms_df.empty:
        logger.warning("No storms found matching the specified filters (date and/or storm name)")
        stats.analysis_success = True  # Not an error - just no matching data
        return stats

    # Track if we processed any storms
    storms_processed = False
    completed_storm_ids = []
    completed_countries = set()
    total_files_written = 0

    for _,row in storms_df.iterrows():
        storm = row['TRACK_ID']
        forecast_date = row['DATE']
        forecast_time = row['TIME']

        # Get today's date
        today = datetime.today().date()

        # If target_date is specified, skip time_delta check (already filtered above)
        # Otherwise, use time_delta to filter
        if target_date or (today - forecast_date).days < time_delta:

            date_str = str(forecast_date).replace('-', '')
            time_str = forecast_time.replace(':', '')
            forecast_datetime_str = f"{date_str}{time_str}00"

            if (storm not in d['storms'] or forecast_datetime_str not in d['storms'][storm]) or rewrite==1:
                storms_processed = True
                loop_stats = run_hurricane_pipeline(
                    storm=storm,
                    forecast_time=forecast_datetime_str,
                    countries=countries,
                    skip_analysis=skip_analysis,
                    log_level=log_level,
                    zoom=zoom
                )
                if loop_stats.analysis_success:
                    logger.info(f"Pipeline completed successfully for storm {storm} at {forecast_datetime_str}")
                    stats.countries_processed += loop_stats.countries_processed
                    stats.views_created += loop_stats.views_created
                    stats.affected_countries.extend(loop_stats.affected_countries)
                    if storm not in d['storms']:
                        d['storms'][storm] = []
                    d['storms'][storm].append(forecast_datetime_str)
                    completed_storm_ids.append(storm)
                    completed_countries.update(loop_stats.affected_countries)
                    total_files_written += loop_stats.views_created
                else:
                    logger.error(f"Pipeline with errors for storm {storm} at {forecast_datetime_str}")
                    stats.analysis_success = False
                    stats.errors.extend(loop_stats.errors)
            else:
                # Storm already processed and rewrite=0, so skip
                logger.info(f"Storm {storm} at {forecast_datetime_str} already processed (use --rewrite 1 to reprocess)")
        else:
            logger.debug(f"Forecast date {forecast_date} outside time delta ({time_delta} days)")

    # If no storms were processed (all were already processed), stats.analysis_success remains True
    if not storms_processed:
        logger.info("All matching storms were already processed (use --rewrite 1 to reprocess)")

    # Save processed storms tracking (may fail silently if data store is not configured)
    try:
        save_json_storms(d)
    except Exception as e:
        logger.warning(f"Could not save storms tracking file: {e}")

    # Signal completion to Snowflake so *_MAT tables refresh via stream trigger
    if completed_storm_ids:
        try:
            conn = get_snowflake_connection()
            signal_pipeline_complete(
                conn=conn,
                storm_ids=completed_storm_ids,
                countries=list(completed_countries),
                files_written=total_files_written
            )
            conn.close()
            logger.info(f"Signalled pipeline completion to Snowflake for storms: {completed_storm_ids}")
        except Exception as e:
            logger.warning(f"Could not write completion signal to Snowflake: {e}")

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
    # Initialize base data for Taiwan
    python main_pipeline.py --type initialize --countries TWN --zoom 14

    # Force re-initialization (regenerates all data from scratch)
    python main_pipeline.py --type initialize --countries PNG --rewrite 1

    # Process all recent storms (last 9 days)
    python main_pipeline.py --type update

    # Process storms for a specific date
    python main_pipeline.py --type update --date 2025-11-10

    # Process a specific storm on a specific date
    python main_pipeline.py --type update --date 2025-11-10 --storm FUNG-WONG

    # Backfill optional columns without full re-init
    python main_pipeline.py --type patch --countries PNG --columns built_surface_m2 rwi

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
            "built_surface_m2, smod_class, smod_class_l1, rwi, schools, hcs, shelters, wash. "
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
        default=_DEFAULT_COUNTRIES,
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
        default=9,
        help="Number of days in the past to consider storms for analysis (default: 9). Ignored if --date is specified."
    )
    
    # ========== Execution Control Arguments ==========
    parser.add_argument(
        "--skip-analysis",
        action="store_true",
        help="Skip the analysis step (useful for testing pipeline structure without processing data)"
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

    # If countries not provided (using default), try to get from Snowflake table
    # This allows GitHub Actions to use Snowflake as source of truth
    if not args.countries or args.countries == _DEFAULT_COUNTRIES:
        try:
            logger.info("No countries specified, attempting to read from Snowflake table...")
            countries_from_snowflake = get_active_countries_from_snowflake()
            if countries_from_snowflake:
                args.countries = countries_from_snowflake
                logger.info(f"Using {len(args.countries)} countries from Snowflake: {', '.join(args.countries)}")
            else:
                logger.warning("No active countries found in Snowflake table, using default list")
        except Exception as e:
            logger.warning(f"Could not read countries from Snowflake: {e}. Using default list.")
    
    # Run pipeline based on hazard type
    if args.hazard == "hurricane":

        if args.type == "initialize":
            stats = initialize_pipeline(args.countries, args.zoom, args.rewrite)
        elif args.type == "update":
            stats = update_storms(
                countries=args.countries,
                skip_analysis=args.skip_analysis,
                log_level=args.log_level,
                zoom=args.zoom,
                rewrite=args.rewrite,
                time_delta=args.time_delta,
                target_date=args.date,
                target_storm=args.storm
            )
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