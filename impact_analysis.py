#!/usr/bin/env python3
"""
Impact Analysis Script

This script performs hurricane impact analysis by reading hurricane envelope data directly 
from Snowflake and creating impact analysis views for schools, health centers, population, 
and infrastructure. The analysis uses geospatial intersection calculations to determine 
potential impacts on critical infrastructure.

Key Features:
- Reads hurricane envelope data directly from Snowflake TC_ENVELOPES_COMBINED table
- Performs geospatial intersection analysis with infrastructure data
- Creates impact views for multiple wind speed thresholds
- Supports multiple countries and flexible storage backends (local/blob)
- Generates comprehensive impact statistics and visualizations

Usage:
    python impact_analysis.py --storm JERRY --date "20251010000000"
    python impact_analysis.py --storm JERRY --date "20251010000000" --countries DOM
"""

import os
import sys
import logging
import geopandas as gpd
import pandas as pd
import numpy as np
import math

# Add the project root to Python path so components can be imported
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import centralized configuration
from config import config

# Import GigaSpatial components
from gigaspatial.handlers import AdminBoundaries, RWIHandler
from gigaspatial.processing import convert_to_geodataframe, buffer_geodataframe
from gigaspatial.handlers import GigaSchoolLocationFetcher
from gigaspatial.generators import GeometryBasedZonalViewGenerator, MercatorViewGenerator, AdminBoundariesViewGenerator
from gigaspatial.handlers.healthsites import HealthSitesFetcher
from gigaspatial.core.io.readers import read_dataset
from gigaspatial.core.io.writers import write_dataset

# Import centralized data store utility
from data_store_utils import get_data_store
from country_utils import update_country_initialized

# Import only Snowflake data retrieval functions
from snowflake_utils import (
    get_envelopes_from_snowflake,
    convert_envelopes_to_geodataframe,
    get_snowflake_tracks
)

from reports import do_report, save_json_report

# =============================================================================
# CONSTANTS
# =============================================================================

# Data column definitions
data_cols = ['population', 'built_surface_m2', 'num_schools', 'school_age_population', 
             'infant_population', 'num_hcs', 'rwi', 'smod_class']
sum_cols = ["E_school_age_population", "E_infant_population", "E_built_surface_m2", 
            "E_population", "E_num_schools", "E_num_hcs"]
avg_cols = ["E_smod_class", "E_rwi", "probability"]
sum_cols_admin = ["school_age_population", "infant_population", "built_surface_m2", 
                  "population", "num_schools", "num_hcs"]
avg_cols_admin = ["smod_class", "rwi"]
sum_cols_cci = ['CCI_children', 'E_CCI_children', 'CCI_school_age', 'E_CCI_school_age', 
                'CCI_infants', 'E_CCI_infants', 'CCI_pop', 'E_CCI_pop']
cci_cols = ['CCI_children', 'CCI_pop', 'CCI_school_age', 'CCI_infants', 
            'E_CCI_children', 'E_CCI_pop', 'E_CCI_school_age', 'E_CCI_infants']

# Configuration constants
BUFFER_DISTANCE_METERS = 150  # Buffer distance for schools and health centers (meters)
WORLDPOP_RESOLUTION_HIGH = 1000  # High resolution for WorldPop data (meters)
WORLDPOP_RESOLUTION_LOW = 100  # Low resolution for WorldPop data (meters)
SCHOOL_AGE_MIN = 5  # Minimum age for school-age population
SCHOOL_AGE_MAX = 15  # Maximum age for school-age population
INFANT_AGE_MIN = 0  # Minimum age for infant population
INFANT_AGE_MAX = 4  # Maximum age for infant population
CCI_WEIGHT_MULTIPLIER = 1e-6  # Multiplier for CCI weight calculation (wind_speed^2 * 1e-6)

# =============================================================================
# CONFIGURATION
# =============================================================================

RESULTS_DIR = config.RESULTS_DIR
STORMS_FILE = config.STORMS_FILE
VIEWS_DIR = config.VIEWS_DIR
ROOT_DATA_DIR = config.ROOT_DATA_DIR

# =============================================================================
# DATA STORE INITIALIZATION
# =============================================================================

# Initialize data store using centralized utility
# Defaults to LOCAL if DATA_PIPELINE_DB is not set or is LOCAL
# In production (SPCS), DATA_PIPELINE_DB will be SNOWFLAKE and credentials will be available
data_store = get_data_store()

# Initialize logger
logger = logging.getLogger(__name__)

# Log which storage backend is being used
storage_backend = config.DATA_PIPELINE_DB
logger.info(f"Storage Backend: {storage_backend}")
if storage_backend == 'SNOWFLAKE':
    logger.info(f"Snowflake Stage: {config.SNOWFLAKE_STAGE_NAME}")
elif storage_backend == 'BLOB':
    logger.info(f"Azure Blob Storage: {config.ACCOUNT_URL}")
else:
    logger.info(f"Local Storage: {ROOT_DATA_DIR}/{VIEWS_DIR}/")



# =============================================================================
# DATA FETCHING AND CACHING FUNCTIONS
# =============================================================================

def fetch_health_centers(country, rewrite=0):
    """
    Fetch health center locations for a country, using cache if available.
    
    This function handles fetching health center data from HealthSites API,
    with automatic caching to avoid redundant API calls. If cached data exists
    and rewrite=0, the cached version is returned.
    
    Args:
        country: ISO3 country code
        rewrite: If 1, fetch fresh data and update cache; if 0, use cache if available
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing health center locations with geometry.
                         Returns empty GeoDataFrame if no data is available.
    
    Note:
        Empty GeoDataFrames are returned with proper CRS (EPSG:4326) to ensure
        compatibility with downstream processing.
    """
    if hc_exist(country) and rewrite == 0:
        return load_hc_locations(country)
    
    try:
        gdf_hcs = HealthSitesFetcher(country=country).fetch_facilities(output_format='geojson')
        if gdf_hcs.empty or 'geometry' not in gdf_hcs.columns:
            logger.warning(f"No health center data available for {country}, skipping...")
            return gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
        
        gdf_hcs = gdf_hcs.set_crs(4326)
        save_hc_locations(gdf_hcs, country)
        return gdf_hcs
    except Exception as e:
        logger.error(f"Error fetching health centers for {country}: {str(e)}")
        return gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')

def fetch_schools(country, rewrite=0):
    """
    Fetch school locations for a country from GIGA API, using cache if available.
    
    This function handles fetching school data from GIGA API, with automatic caching
    to avoid redundant API calls. If cached data exists and rewrite=0, the cached
    version is returned.
    
    Args:
        country: ISO3 country code
        rewrite: If 1, fetch fresh data and update cache; if 0, use cache if available
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing school locations with geometry
    
    Note:
        Handles legacy column name 'giga_id_school' by renaming to 'school_id_giga'.
        Cached files are stored in the same directory as health center caches.
    """
    if school_exist(country) and rewrite == 0:
        return load_school_locations(country)
    
    try:
        gdf_schools = GigaSchoolLocationFetcher(country).fetch_locations(process_geospatial=True)
        # Handle legacy API column name
        if 'giga_id_school' in gdf_schools.columns:
            gdf_schools = gdf_schools.rename(columns={'giga_id_school': 'school_id_giga'})
        
        # Save to cache
        save_school_locations(gdf_schools, country)
        return gdf_schools
    except Exception as e:
        logger.error(f"Error fetching schools for {country}: {str(e)}")
        # Return empty GeoDataFrame with proper structure for downstream processing
        return gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')

# =============================================================================
# GEOSPATIAL PROCESSING FUNCTIONS
# =============================================================================

def get_country_boundaries(countries):
    """
    Retrieve country boundary geometries for a list of countries.
    
    Args:
        countries: List of ISO3 country codes (e.g., ['TWN', 'DOM'])
    
    Returns:
        list: List of Shapely geometry objects representing country boundaries (admin level 0)
    
    Raises:
        Exception: If country boundaries cannot be retrieved for any country
    """
    country_boundaries = []
    for country in countries:
        try:
            admin_boundaries = AdminBoundaries.create(country_code=country, admin_level=0)
            country_boundaries.append(admin_boundaries.to_geodataframe().geometry.iat[0])
        except Exception as e:
            logger.error(f"Error retrieving boundaries for {country}: {e}")
            raise
    return country_boundaries

def is_envelope_in_zone(bbox, df_envelopes, geometry_column='geometry'):
    """
    Check if any hurricane envelope intersects with a given bounding box/zone.
    
    Args:
        bbox: Shapely polygon representing the bounding box/zone to check
        df_envelopes: DataFrame or GeoDataFrame containing hurricane envelope geometries
        geometry_column: Name of the geometry column (default: 'geometry')
    
    Returns:
        bool: True if any envelope intersects with the bbox, False otherwise
    """
    if df_envelopes.empty:
        return False

    if geometry_column != 'geometry':
        df_envelopes = df_envelopes.rename(columns={geometry_column: 'geometry'})
    
    if isinstance(df_envelopes, pd.DataFrame):
        gdf_envelopes = convert_to_geodataframe(df_envelopes.dropna(subset=['geometry']))
    else:  # GeoDataFrame
        gdf_envelopes = df_envelopes.copy()

    mask = gdf_envelopes.intersects(bbox)
    return bool(mask.any())

def create_mercator_country_layer(country, zoom_level=15, rewrite=0):
    """
    Create mercator tile layer with demographic and infrastructure data for a country.
    
    This function generates a comprehensive mercator tile view containing:
    - Population data (total, school-age, infants) from WorldPop
    - Infrastructure data (built surface, SMOD settlement classification)
    - School locations from GIGA API
    - Health center locations from HealthSites API
    - Relative Wealth Index (RWI) data
    
    Args:
        country: ISO3 country code
        zoom_level: Zoom level for mercator tiles (default: 15, typically 14 for analysis)
        rewrite: If 1, replace existing health center cache; if 0, use cached if available
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame with mercator tiles containing all demographic and
                         infrastructure data. Column 'zone_id' is renamed to 'tile_id'.
    
    Note:
        Missing data (e.g., RWI, age-specific population) is handled gracefully with NaN values.
        The function attempts multiple fallback strategies for age-specific population data.
    """
    # Fetch schools and health centers using helper functions with caching
    gdf_schools = fetch_schools(country, rewrite)
    gdf_hcs = fetch_health_centers(country, rewrite)

    tiles_viewer = MercatorViewGenerator(source=country, zoom_level=zoom_level, data_store=data_store)
    
    # Map school-age population (with fallback strategies)
    try:
        tiles_viewer.map_wp_pop(
            country=country, 
            resolution=WORLDPOP_RESOLUTION_HIGH, 
            output_column="school_age_population", 
            school_age=True, 
            project="age_structures", 
            un_adjusted=False, 
            sex='F_M'
        )
    except Exception:
        # Fallback: try age range method
        try:
            tiles_viewer.map_wp_pop(
                country=country, 
                resolution=WORLDPOP_RESOLUTION_HIGH, 
                output_column="school_age_population", 
                predicate='centroid_within', 
                school_age=False, 
                project="age_structures", 
                un_adjusted=False,
                min_age=SCHOOL_AGE_MIN, 
                max_age=SCHOOL_AGE_MAX
            )
        except Exception:
            # Final fallback: set to NaN
            logger.warning(f"No school-age population data available for {country}")
            school_age = {k: np.nan for k in tiles_viewer.view.index.unique()}
            tiles_viewer.add_variable_to_view(school_age, 'school_age_population')

    # Map infant population
    try:
        tiles_viewer.map_wp_pop(
            country=country, 
            resolution=WORLDPOP_RESOLUTION_HIGH, 
            output_column="infant_population", 
            predicate='centroid_within', 
            school_age=False, 
            project="age_structures", 
            un_adjusted=False,
            min_age=INFANT_AGE_MIN, 
            max_age=INFANT_AGE_MAX
        )
    except Exception:
        logger.warning(f"No infant population data available for {country}")
        infant_pop = {k: np.nan for k in tiles_viewer.view.index.unique()}
        tiles_viewer.add_variable_to_view(infant_pop, 'infant_population')

    tiles_viewer.map_wp_pop(country=country, resolution=100)
    tiles_viewer.map_built_s()
    tiles_viewer.map_smod()
    
    ## schools
    schools = tiles_viewer.map_points(points=gdf_schools)
    tiles_viewer.add_variable_to_view(schools, "num_schools")

    ## health centers
    hcs = tiles_viewer.map_points(points=gdf_hcs)
    tiles_viewer.add_variable_to_view(hcs, "num_hcs")

    # should we do RWI?
    try:
        handler = RWIHandler(data_store=data_store)
        rwi_df = handler.load_data(country, ensure_available=True)
        rwi_gdf = convert_to_geodataframe(rwi_df)
        rwi = tiles_viewer.map_points(rwi_gdf, value_columns='rwi', aggregation='mean')
    except:
        print("Relative Wealth Index will be empty")
        rwi = {k: np.nan for k in tiles_viewer.view.index.unique()}
    
    tiles_viewer.add_variable_to_view(rwi, 'rwi')

    gdf_tiles = tiles_viewer.to_geodataframe()
    gdf_tiles.rename(columns={'zone_id': 'tile_id'}, inplace=True)
    
    return gdf_tiles


def save_mercator_view(gdf, country, zoom_level):
    """
    Save base mercator infrastructure view for a country.
    
    Args:
        gdf: GeoDataFrame containing mercator tile data
        country: ISO3 country code
        zoom_level: Zoom level for the tiles
    """
    file_name = f"{country}_{zoom_level}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def admins_overlay(gdf_admins1, gdf_mercator):
    """
    Assign admin level 1 IDs to mercator tiles based on maximum intersection area.
    
    For each mercator tile, this function determines which admin level 1 boundary
    it belongs to by finding the admin boundary with the largest intersection area.
    This handles cases where tiles may intersect multiple admin boundaries.
    
    Args:
        gdf_admins1: GeoDataFrame containing admin level 1 boundaries (must have 'id' column)
        gdf_mercator: GeoDataFrame containing mercator tiles (must have 'tile_id' column)
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame with mercator tiles and assigned admin IDs
    """
    small_gdf = gdf_mercator.copy()
    large_gdf = gdf_admins1.copy()
    
    # Overlay to get all intersections
    intersections = gpd.overlay(small_gdf, large_gdf, how="intersection")

    # Compute intersection area for each intersection
    intersections["intersection_area"] = intersections.geometry.area

    # For each tile, keep the admin with maximum intersection area
    max_area_idx = intersections.groupby("tile_id")["intersection_area"].idxmax()
    assigned = intersections.loc[max_area_idx, ["tile_id", "id"]]

    # Merge back with original tiles to assign admin IDs
    result = small_gdf.merge(assigned, on="tile_id", how="left")

    # Ensure result is a GeoDataFrame with correct geometry and CRS
    return gpd.GeoDataFrame(result, geometry="geometry", crs=small_gdf.crs)

def add_admin_ids(view, country, zoom_level, rewrite):
    """
    Add admin level 1 IDs to a mercator tile view.
    
    Args:
        view: GeoDataFrame containing mercator tiles
        country: ISO3 country code
        zoom_level: Zoom level (unused, kept for API compatibility)
        rewrite: Rewrite flag (unused, kept for API compatibility)
    
    Returns:
        tuple: (combined_view, gdf_admins1) where:
            - combined_view: GeoDataFrame with tiles and admin IDs
            - gdf_admins1: GeoDataFrame with admin level 1 boundaries
    """
    gdf_admins1 = AdminBoundaries.create(country_code=country, admin_level=1).to_geodataframe()
    combined_view = admins_overlay(gdf_admins1, view)
    return combined_view, gdf_admins1

def save_mercator_and_admin_views(countries,zoom_level,rewrite):
    """
    Generates and saves all country mercator views and admin views.
    Automatically tracks initialization in Snowflake after successful completion.
    """
    import logging
    logger = logging.getLogger(__name__)

    for country in countries:
        file_name = f"{country}_{zoom_level}.parquet"
        file_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name)
        initialized = False
        
        if not data_store.file_exists(file_path):
            view = create_mercator_country_layer(country, zoom_level, rewrite)
            combined_view, gdf_admins1 = add_admin_ids(view,country,zoom_level,rewrite)
            d = gdf_admins1.set_index('id')['name'].to_dict()
            d_geo = gdf_admins1.set_index('id')['geometry'].to_dict()
            save_mercator_view(combined_view, country, zoom_level)
            #### Aggregate by admin ####
            
            # Define aggregation dictionary
            agg_dict = {col: "sum" for col in sum_cols_admin}
            agg_dict.update({col: "mean" for col in avg_cols_admin})

            # Group by large_id and aggregate
            agg = combined_view.groupby("id").agg(agg_dict).reset_index()
            admin_view = agg.rename(columns={'id':'tile_id'})
            ### add names ###
            admin_view['name'] = admin_view['tile_id'].map(d)
            admin_view['geometry'] = admin_view['tile_id'].map(d_geo)
            admin_view = convert_to_geodataframe(admin_view)
            save_admin_view(admin_view,country)
            ############################
            initialized = True
        elif rewrite:
            # When rewrite=1, regenerate the entire mercator view from scratch
            view = create_mercator_country_layer(country, zoom_level, rewrite)
            combined_view, gdf_admins1 = add_admin_ids(view,country,zoom_level,rewrite)
            d = gdf_admins1.set_index('id')['name'].to_dict()
            d_geo = gdf_admins1.set_index('id')['geometry'].to_dict()
            save_mercator_view(combined_view, country, zoom_level)
            #### Aggregate by admin ####
            
            # Define aggregation dictionary
            agg_dict = {col: "sum" for col in sum_cols_admin}
            agg_dict.update({col: "mean" for col in avg_cols_admin})

            # Group by large_id and aggregate
            agg = combined_view.groupby("id").agg(agg_dict).reset_index()
            admin_view = agg.rename(columns={'id':'tile_id'})
            ### add names ###
            admin_view['name'] = admin_view['tile_id'].map(d)
            admin_view['geometry'] = admin_view['tile_id'].map(d_geo)
            admin_view = convert_to_geodataframe(admin_view)
            save_admin_view(admin_view,country)
            ############################
            initialized = True
        else:
            # File already exists and rewrite=0
            # This means it was initialized before, so ensure it's tracked
            logger.info(f"File already exists for {country} at zoom {zoom_level}, ensuring tracking is up to date")
            initialized = True  # Mark as initialized since file exists
        
        # Automatically track initialization in Snowflake
        # Safe to call even if already tracked
        if initialized:
            try:
                update_country_initialized(country, zoom_level)
                logger.info(f"Tracked initialization for {country} at zoom level {zoom_level} in Snowflake")
            except Exception as e:
                logger.warning(f"Could not track initialization for {country} in Snowflake: {e}")
                logger.warning("  (Initialization completed, but tracking failed)")

def save_json_storms(d):
    """
    Save json file with processed storm,dates
    """
    filename = os.path.join(RESULTS_DIR, STORMS_FILE)
    write_dataset(d, data_store, filename)

def load_json_storms():
    """
    Read json file with saved storm,dates
    """
    filename = os.path.join(RESULTS_DIR, STORMS_FILE)
    if data_store.file_exists(filename):
        df = read_dataset(data_store, filename)
        column_name = df.columns[0]
        return {column_name: df[column_name].to_dict()}
    return {'storms':{}}


def load_mercator_view(country, zoom_level=15):
    """Load mercator view for country"""
    file_name = f"{country}_{zoom_level}.parquet"
    return read_dataset(data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def create_school_view_from_envelopes(gdf_schools, gdf_envelopes):
    """
    gdf_schools: a geodataframe with school data
    gdf_envelopes: a geodataframe with envelopes
    returns a dictionary of wind threshold - geodataframe with the corresponding school view
    """
    gdf_schools_buff = buffer_geodataframe(gdf_schools, buffer_distance_meters=150)
    wind_views = {}

    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            schools_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_schools_buff, zone_id_column='school_id_giga')
            try:
                new_col = schools_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except Exception as e:
                logger.warning(f"Error mapping polygons for school view at {wind_th}kt: {e}")
                probs = {k: 0.0 for k in schools_viewer.view.zone_id.unique()}
            schools_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = schools_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views

def create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes):
    """
    Create health center impact views from hurricane envelopes.
    
    For each wind speed threshold, calculates the probability that each health center
    will be affected by winds at or above that threshold.
    
    Args:
        gdf_hcs: GeoDataFrame containing health center locations
        gdf_envelopes: GeoDataFrame containing hurricane envelope geometries with wind_threshold column
    
    Returns:
        dict: Dictionary mapping wind threshold (int) to GeoDataFrame with health center impact data.
              Each GeoDataFrame contains 'probability' column indicating impact probability.
    """
    gdf_hcs_buff = buffer_geodataframe(gdf_hcs, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}

    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            hcs_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_hcs_buff, zone_id_column='osm_id')
            try:
                new_col = hcs_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except Exception as e:
                logger.warning(f"Error mapping polygons for health center view at {wind_th}kt: {e}")
                probs = {k: 0.0 for k in hcs_viewer.view.zone_id.unique()}
            hcs_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = hcs_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views

def create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes):
    """
    Create mercator tile impact views from hurricane envelopes.
    
    For each wind speed threshold, calculates expected impacts (E_*) for each tile,
    where expected impact = base value * probability of impact.
    
    Args:
        gdf_tiles: GeoDataFrame containing mercator tiles with demographic/infrastructure data
        gdf_envelopes: GeoDataFrame containing hurricane envelope geometries with wind_threshold column
    
    Returns:
        dict: Dictionary mapping wind threshold (int) to DataFrame with tile impact data.
              Each DataFrame contains probability and E_* columns for expected impacts.
    """
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
            try:
                new_col = tiles_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in tiles_viewer.view.zone_id.unique()}
            tiles_viewer.add_variable_to_view(probs, 'probability')

            df_view = tiles_viewer.to_dataframe()
            for col in data_cols:
                df_view[f"E_{col}"] = df_view[col]*df_view['probability']

            df_view = df_view.drop(columns=data_cols)

            wind_views[wind_th] = df_view


    return wind_views

def create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles, gdf_envelopes):
    d = gdf_admin.set_index('tile_id')['name'].to_dict()
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
            try:
                new_col = tiles_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in tiles_viewer.view.zone_id.unique()}
            tiles_viewer.add_variable_to_view(probs, 'probability')

            df_view = tiles_viewer.to_dataframe()
            for col in data_cols:
                df_view[f"E_{col}"] = df_view[col]*df_view['probability']

            df_view = df_view.drop(columns=data_cols)
            
            # Admin IDs must be present in gdf_tiles (added during initialization or on load)
            # df_view.index contains zone_id (which is tile_id)
            if 'id' not in gdf_tiles.columns:
                raise ValueError(
                    "Mercator view missing admin IDs."
                    "Admin IDs are added during initialization. "
                    "Re-initialize the country or check the mercator view file."
                )
            # Create mapping from tile_id to admin id
            id_mapping = gdf_tiles.set_index('tile_id')['id'].to_dict()
            # Map zone_id (tile_id) to admin id
            df_view['id'] = df_view.index.map(lambda x: id_mapping.get(x, x))
            
            # Define aggregation dictionary
            agg_dict = {col: "sum" for col in sum_cols}
            agg_dict.update({col: "mean" for col in avg_cols})

            # Group by large_id and aggregate
            agg = df_view.groupby("id").agg(agg_dict).reset_index()
            df_view = agg.rename(columns={'id':'zone_id'})
            ### add names ###
            df_view['name'] = df_view['zone_id'].map(d)

            wind_views[wind_th] = df_view

    return wind_views

def create_admin_view_from_envelopes(gdf_admin, gdf_envelopes):
    """Create admin tile impact views from envelopes"""
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            admin_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_admin, zone_id_column='tile_id')
            try:
                new_col = admin_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in admin_viewer.view.zone_id.unique()}
            admin_viewer.add_variable_to_view(probs, 'probability')

            df_view = admin_viewer.to_dataframe()
            for col in data_cols:
                df_view[f"E_{col}"] = df_view[col]*df_view['probability']

            df_view = df_view.drop(columns=data_cols)

            wind_views[wind_th] = df_view

    return wind_views

def create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member'):
    """Create tracks impact views from envelopes"""
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]

        tracks_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_envelopes_wth, zone_id_column=index_column)
        
        # Schools
        schools = tracks_viewer.map_points(points=gdf_schools)
        tracks_viewer.add_variable_to_view(schools, "severity_schools")

        # Health centers
        hcs = tracks_viewer.map_points(points=gdf_hcs)
        tracks_viewer.add_variable_to_view(hcs, "severity_hcs")

        # Tiles
        try:
            overlays = tracks_viewer.map_polygons(polygons=gdf_tiles, value_columns=["population","school_age_population","infant_population",'built_surface_m2'], aggregation="sum")
            tracks_viewer.add_variable_to_view(overlays['population'], "severity_population")
            tracks_viewer.add_variable_to_view(overlays['school_age_population'], "severity_school_age_population")
            tracks_viewer.add_variable_to_view(overlays['infant_population'], "severity_infant_population")
            tracks_viewer.add_variable_to_view(overlays['built_surface_m2'], "severity_built_surface_m2")
        except:
            zeros = {k: 0 for k in gdf_envelopes_wth[index_column].unique()}
            tracks_viewer.add_variable_to_view(zeros, "severity_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_school_age_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_infant_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_built_surface_m2")
        

        gdf_view = tracks_viewer.to_geodataframe()
        wind_views[wind_th] = gdf_view

    return wind_views


def save_school_view(gdf, country, storm, date, wind_th):
    """
    Save school impact view for a specific storm, date, and wind threshold.
    
    Args:
        gdf: GeoDataFrame containing school impact data
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        wind_th: Wind threshold in knots
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', file_name))

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
    return read_dataset(data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', file_name))

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


def save_hc_view(gdf, country, storm, date, wind_th):
    """
    Saves health center views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

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
    return read_dataset(data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

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



def create_admin_country_layer(country, rewrite=0):
    """
    Create admin level 1 layer with demographic and infrastructure data for a country.
    
    This function generates an admin-level view containing:
    - Population data (total, school-age, infants) from WorldPop
    - Infrastructure data (built surface, SMOD settlement classification)
    - School locations from GIGA API
    - Health center locations from HealthSites API
    - Relative Wealth Index (RWI) data
    
    Args:
        country: ISO3 country code
        rewrite: If 1, replace existing cache; if 0, use cached if available
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame with admin level 1 boundaries containing all demographic and
                         infrastructure data. Column 'zone_id' is renamed to 'tile_id'.
    
    Note:
        Missing data (e.g., RWI, age-specific population) is handled gracefully with NaN values.
        The function attempts multiple fallback strategies for age-specific population data.
    """
    # Fetch schools and health centers using helper functions with caching
    gdf_schools = fetch_schools(country, rewrite)
    gdf_hcs = fetch_health_centers(country, rewrite)

    tiles_viewer = AdminBoundariesViewGenerator(country=country, admin_level=1, data_store=data_store)
    
    # Map school-age population (with fallback strategies)
    try:
        tiles_viewer.map_wp_pop(
            country=country, 
            resolution=WORLDPOP_RESOLUTION_HIGH, 
            output_column="school_age_population", 
            school_age=True, 
            project="age_structures", 
            un_adjusted=False, 
            sex='F_M'
        )
    except Exception:
        # Fallback: try age range method
        try:
            tiles_viewer.map_wp_pop(
                country=country, 
                resolution=WORLDPOP_RESOLUTION_HIGH, 
                output_column="school_age_population", 
                predicate='centroid_within', 
                school_age=False, 
                project="age_structures", 
                un_adjusted=False,
                min_age=SCHOOL_AGE_MIN, 
                max_age=SCHOOL_AGE_MAX
            )
        except Exception:
            # Final fallback: set to NaN
            logger.warning(f"No school-age population data available for {country}")
            school_age = {k: np.nan for k in tiles_viewer.view.index.unique()}
            tiles_viewer.add_variable_to_view(school_age, 'school_age_population')

    # Map infant population
    try:
        tiles_viewer.map_wp_pop(
            country=country, 
            resolution=WORLDPOP_RESOLUTION_HIGH, 
            output_column="infant_population", 
            predicate='centroid_within', 
            school_age=False, 
            project="age_structures", 
            un_adjusted=False,
            min_age=INFANT_AGE_MIN, 
            max_age=INFANT_AGE_MAX
        )
    except Exception:
        logger.warning(f"No infant population data available for {country}")
        infant_pop = {k: np.nan for k in tiles_viewer.view.index.unique()}
        tiles_viewer.add_variable_to_view(infant_pop, 'infant_population')

    # Map total population and infrastructure data
    tiles_viewer.map_wp_pop(country=country, resolution=WORLDPOP_RESOLUTION_LOW)
    tiles_viewer.map_built_s()
    tiles_viewer.map_smod()
    
    ## schools
    schools = tiles_viewer.map_points(points=gdf_schools)
    tiles_viewer.add_variable_to_view(schools, "num_schools")

    ## health centers
    hcs = tiles_viewer.map_points(points=gdf_hcs)
    tiles_viewer.add_variable_to_view(hcs, "num_hcs")

    # should we do RWI?
    try:
        handler = RWIHandler(data_store=data_store)
        rwi_df = handler.load_data(country, ensure_available=True)
        rwi_gdf = convert_to_geodataframe(rwi_df)
        rwi = tiles_viewer.map_points(rwi_gdf, value_columns='rwi', aggregation='mean')
    except:
        print("Relative Wealth Index will be empty")
        rwi = {k: np.nan for k in tiles_viewer.view.index.unique()}
    
    tiles_viewer.add_variable_to_view(rwi, 'rwi')

    gdf_tiles = tiles_viewer.to_geodataframe()
    gdf_tiles.rename(columns={'zone_id': 'tile_id'}, inplace=True)
    
    return gdf_tiles

def save_admin_view(gdf, country):
    """Save base admin1 infrastructure view for country"""
    file_name = f"{country}_admin1.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))

def save_admin_views(countries, rewrite=0):
    """
    Generates and saves all country admin1 views
    
    Args:
        countries: List of country codes
        rewrite: If 1, replace existing files; if 0, skip if exists
    """

    for country in countries:
        view = create_admin_country_layer(country, rewrite)
        save_admin_view(view, country)

def save_tiles_view(gdf, country, storm, date, wind_th, zoom_level):
    """
    Saves tiles views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}_{zoom_level}.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def save_cci_tiles(gdf, country, storm, date, zoom_level):
    """
    Saves Child Cyclone Index (CCI) tile views to storage.
    
    Args:
        gdf: GeoDataFrame containing CCI values per tile
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        zoom_level: Zoom level for tiles
    """
    file_name = f"{country}_{storm}_{date}_{zoom_level}_cci.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def save_admin_tiles_view(gdf, country, storm, date, wind_th):
    """
    Saves admin tiles views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}_admin1.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))

def save_cci_admin(gdf, country, storm, date):
    """
    Saves Child Cyclone Index (CCI) admin-level views to storage.
    
    Args:
        gdf: GeoDataFrame containing CCI values aggregated by admin level
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
    """
    file_name = f"{country}_{storm}_{date}_admin1_cci.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))

def load_admin_view(country):
    """Load admin view for country"""
    file_name = f"{country}_admin1.parquet"
    return read_dataset(data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))

def save_tracks_view(gdf, country, storm, date, wind_th):
    """
    Saves tracks views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', file_name))


##################################################

cci_cols = ['CCI_children','CCI_pop','CCI_school_age','CCI_infants','E_CCI_children','E_CCI_pop','E_CCI_school_age','E_CCI_infants']    

def calculate_ccis(wind_tiles_views, gdf_tiles):
    """
    Calculate Child Cyclone Index (CCI) values for tiles.
    
    CCI is a weighted index that quantifies the potential impact of cyclone wind speeds
    on different population demographics (children, school-age, infants, total population).
    The index uses wind speed squared as weights to emphasize higher wind speeds.
    
    Args:
        wind_tiles_views: Dictionary of wind threshold views (key: wind speed in kt, value: DataFrame)
        gdf_tiles: GeoDataFrame containing tile data with population demographics and admin IDs
    
    Returns:
        DataFrame: CCI tile view with columns:
            - CCI_children: Child Cyclone Index for children (school-age + infants)
            - E_CCI_children: Expected CCI for children
            - CCI_school_age: CCI for school-age population
            - E_CCI_school_age: Expected CCI for school-age population
            - CCI_infants: CCI for infant population
            - E_CCI_infants: Expected CCI for infants
            - CCI_pop: CCI for total population
            - E_CCI_pop: Expected CCI for total population
    
    Raises:
        ValueError: If admin IDs are missing from gdf_tiles (required for aggregation)
    
    Note:
        Requires admin IDs in gdf_tiles for proper admin-level aggregation.
        Admin IDs should always be present (added during initialization or on load).
    """
    # Admin IDs must be present - fail fast if missing
    if 'id' not in gdf_tiles.columns:
        raise ValueError(
            "Mercator view missing admin IDs."
            "Admin IDs are added during initialization. "
            "Re-initialize the country or check the mercator view file."
        )
    d = gdf_tiles.set_index('tile_id')['id'].to_dict()
    winds = sorted(wind_tiles_views.keys())
    sorted_wind_views_indexed = []
    for wind in winds:
        df = wind_tiles_views[wind].copy().set_index('zone_id')
        sorted_wind_views_indexed.append(df)
    k = len(sorted_wind_views_indexed)
    gdf_tiles_index = gdf_tiles.rename(columns={'tile_id':'zone_id'}).set_index('zone_id')
    cci_tiles_view = pd.DataFrame(index=gdf_tiles_index.index)

    # Children cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_children'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children']]

    # Children e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_school_age_population'] + sorted_wind_views_indexed[i]['E_infant_population']) - (sorted_wind_views_indexed[i+1]['E_school_age_population'] + sorted_wind_views_indexed[i+1]['E_infant_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_school_age_population'] + sorted_wind_views_indexed[k-1]['E_infant_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_children'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children']]

    # school age cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['school_age_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_school_age'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age']]

    # school age e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_school_age_population']) - (sorted_wind_views_indexed[i+1]['E_school_age_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_school_age_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_school_age'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age']]

    # infant cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_infants'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants']]

    # infant e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_infant_population']) - (sorted_wind_views_indexed[i+1]['E_infant_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_infant_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_infants'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants']]

    # pop cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_pop'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_pop']]

    # pop e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_population']) - (sorted_wind_views_indexed[i+1]['E_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_pop'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_pop','E_CCI_pop']]
    cci_tiles_view = cci_tiles_view.reset_index()

    cci_tiles_view['id'] = cci_tiles_view['zone_id'].map(d)
    
    return cci_tiles_view

##################################################

# =============================================================================

def create_views_from_envelopes_in_country(country, storm, date, gdf_envelopes, zoom):
    """
    Create and save all impact views for a country from hurricane envelopes.
    
    This is the main orchestration function that processes a single country for a given
    storm forecast. It creates and saves:
    - School impact views (probability per wind threshold)
    - Health center impact views (probability per wind threshold)
    - Tile impact views (expected impacts per tile per wind threshold)
    - Admin level impact views (aggregated by admin level 1)
    - Child Cyclone Index (CCI) views (both tile and admin level)
    - Track views (severity metrics per ensemble member)
    - JSON impact report
    
    Args:
        country: ISO3 country code
        storm: Storm name (e.g., 'FUNG-WONG')
        date: Forecast date in YYYYMMDDHHMMSS format (e.g., '20251110000000')
        gdf_envelopes: GeoDataFrame containing hurricane envelope geometries
        zoom: Zoom level for mercator tiles
    
    Note:
        Base data (mercator tiles, admin views) are loaded if available, or created
        on-the-fly if missing. Admin IDs are automatically added if missing.
    """
    print(f"  Processing {country}...")
    
    # Schools
    print(f"    Processing schools...")
    gdf_schools = fetch_schools(country, rewrite=0)
    
    wind_school_views = create_school_view_from_envelopes(gdf_schools, gdf_envelopes)
    for wind_th in wind_school_views:
        save_school_view(wind_school_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_school_views)} school views")

    # Health centers
    print(f"    Processing health centers...")
    gdf_hcs = fetch_health_centers(country, rewrite=0)
    wind_hc_views = create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes)
    for wind_th in wind_hc_views:
        save_hc_view(wind_hc_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_hc_views)} health center views")

    # Tiles
    print(f"    Processing tiles...")
    try:
        gdf_tiles = load_mercator_view(country, zoom)
        print(f"    Loaded existing mercator tiles: {len(gdf_tiles)} tiles")
        # Ensure admin IDs are present (in case file was created without them)
        if 'id' not in gdf_tiles.columns:
            print(f"    Warning: Mercator view missing admin IDs, adding them...")
            gdf_tiles, _ = add_admin_ids(gdf_tiles, country, zoom, rewrite=0)
            save_mercator_view(gdf_tiles, country, zoom)
    except:
        print(f"    Creating base mercator tiles for {country}...")
        view = create_mercator_country_layer(country, zoom, rewrite=0)
        gdf_tiles, _ = add_admin_ids(view, country, zoom, rewrite=0)
        save_mercator_view(gdf_tiles, country, zoom)
        print(f"    Created and saved base mercator tiles: {len(gdf_tiles)} tiles")
    
    wind_tiles_views = create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes)
    for wind_th in wind_tiles_views:
        save_tiles_view(wind_tiles_views[wind_th], country, storm, date, wind_th, zoom)
    print(f"    Created {len(wind_tiles_views)} tile views")

    ### cci for tiles
    cci_tiles_view = calculate_ccis(wind_tiles_views, gdf_tiles)
    save_cci_tiles(cci_tiles_view, country, storm, date, zoom)
    #######

    # Admins
    print(f"    Processing admins...")
    try:
        gdf_admin = load_admin_view(country)
        print(f"    Loaded existing admin: {len(gdf_admin)} tiles")
    except:
        print(f"    Creating base admin for {country}...")
        gdf_admin = create_admin_country_layer(country, rewrite=0)
        save_admin_view(gdf_admin, country)
        print(f"    Created and saved base admin tiles: {len(gdf_admin)} tiles")
    
    wind_admin_views = create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles, gdf_envelopes)
    for wind_th in wind_admin_views:
        save_admin_tiles_view(wind_admin_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_admin_views)} admin views")

    ### cci for admin
    # Define aggregation dictionary
    agg_dict = {col: "sum" for col in sum_cols_cci}

    # Group by large_id and aggregate
    agg = cci_tiles_view.groupby("id").agg(agg_dict).reset_index()
    cci_admin_view = agg.rename(columns={'id':'zone_id'})
    ### add names ###
    #df_view['name'] = df_view['zone_id'].map(d)
    save_cci_admin(cci_admin_view, country, storm, date)
    #######

    # Tracks
    print(f"    Processing tracks...")
    wind_tracks_views = create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member')
    for wind_th in wind_tracks_views:
        save_tracks_view(wind_tracks_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_tracks_views)} track views")

    df_tracks = get_snowflake_tracks(date, storm)
    gdf_tracks = convert_to_geodataframe(df_tracks)

    json_report = do_report(wind_school_views, wind_hc_views, wind_tiles_views, wind_admin_views, cci_tiles_view, cci_admin_view, gdf_admin, gdf_tracks, country, storm, date)
    save_json_report(json_report, country, storm, date)

def load_envelopes_from_snowflake(storm, date):
    """Load envelope data directly from Snowflake"""
    # Convert date format if needed
    if len(date) == 14:  # YYYYMMDDHHMMSS format
        # Convert to datetime string format
        dt = pd.to_datetime(date, format="%Y%m%d%H%M%S")
        forecast_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        forecast_time = date
    
    try:
        # Get envelope data from Snowflake
        df_envelopes = get_envelopes_from_snowflake(storm, forecast_time)
        
        if df_envelopes.empty:
            print(f"Error: No envelope data found in Snowflake for {storm} at {forecast_time}")
            return pd.DataFrame()
        
        # Convert to GeoDataFrame
        gdf_envelopes = convert_envelopes_to_geodataframe(df_envelopes)
        return gdf_envelopes
        
    except Exception as e:
        print(f"Error loading envelopes from Snowflake: {str(e)}")
        return pd.DataFrame()