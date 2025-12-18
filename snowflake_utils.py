#!/usr/bin/env python3
"""
Snowflake Utilities Module

This module provides utility functions for Snowflake operations and data retrieval.
It serves as a focused toolkit for connecting to Snowflake and retrieving hurricane data.

Key Components:
- Snowflake connection management (supports SPCS OAuth and password authentication)
- Hurricane track data retrieval from TC_TRACKS table
- Hurricane envelope data retrieval from TC_ENVELOPES_COMBINED table
- Data format conversion utilities (WKT to GeoDataFrame)
- Metadata queries (available wind thresholds, latest forecast times)

Usage:
    from snowflake_utils import get_envelopes_from_snowflake, get_snowflake_tracks
    envelopes = get_envelopes_from_snowflake('JERRY', '2025-10-10 00:00:00')
    tracks = get_snowflake_tracks('20251010000000', 'JERRY')
"""

import os
from typing import Optional, List, Dict, Any
from datetime import datetime

import pandas as pd
import geopandas as gpd
import snowflake.connector
from shapely import wkt as shapely_wkt
import warnings
import logging

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from config import config

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

# Date format constants
DATE_FORMAT_COMPACT = "%Y%m%d%H%M%S"  # YYYYMMDDHHMMSS
DATE_FORMAT_STANDARD = "%Y-%m-%d %H:%M:%S"  # YYYY-MM-DD HH:MM:SS

# SPCS configuration defaults
SPCS_TOKEN_PATH_DEFAULT = '/snowflake/session/token'

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def _normalize_forecast_time(date: str) -> str:
    """
    Normalize forecast time to standard format.
    
    Args:
        date: Forecast date in YYYYMMDDHHMMSS format or datetime string
    
    Returns:
        str: Forecast time in standard format (YYYY-MM-DD HH:MM:SS)
    """
    if len(date) == 14:  # YYYYMMDDHHMMSS format
        dt = pd.to_datetime(date, format=DATE_FORMAT_COMPACT)
        return dt.strftime(DATE_FORMAT_STANDARD)
    return date

def _execute_query(query: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
    """
    Execute a SQL query against Snowflake and return results as DataFrame.
    
    Args:
        query: SQL query string
        params: Optional list of parameters for parameterized query
    
    Returns:
        pd.DataFrame: Query results, or empty DataFrame on error
    
    Note:
        Connection is automatically closed after query execution.
    """
    conn = None
    try:
        conn = get_snowflake_connection()
        if params:
            df = pd.read_sql(query, conn, params=params)
        else:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


# =============================================================================
# CONNECTION MANAGEMENT
# =============================================================================

def get_snowflake_connection():
    """
    Create Snowflake connection from centralized configuration.
    Supports SPCS OAuth and password authentication.
    
    Authentication Methods:
    1. SPCS OAuth (for Snowflake Container Services):
       - Set SPCS_RUN=true
       - Token read from SPCS_TOKEN_PATH (default: /snowflake/session/token)
       
    2. Password (default):
       - Set SNOWFLAKE_PASSWORD
    
    Returns:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection
    
    Raises:
        ValueError: If authentication fails or required config is missing
    """
    config.validate_snowflake_config()
    
    # Check for SPCS OAuth authentication
    spcs_run = os.getenv('SPCS_RUN', 'false').lower() == 'true'
    
    if spcs_run:
        return _get_spcs_connection()
    else:
        return _get_password_connection()

def _get_spcs_connection():
    """
    Create Snowflake connection using SPCS OAuth authentication.
    
    Returns:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection
    
    Raises:
        ValueError: If OAuth token cannot be loaded
    """
    logger.info("Connecting to Snowflake with SPCS OAuth authentication...")
    token_path = os.getenv('SPCS_TOKEN_PATH', SPCS_TOKEN_PATH_DEFAULT)
    
    try:
        with open(token_path, 'r') as f:
            token = f.read().strip()
        
        # SPCS requires specific connection parameters for internal network
        # Reference: https://medium.com/snowflake/connecting-to-snowflake-from-snowpark-container-services-cfc3a133480e
        conn_params = {
            'host': os.getenv('SNOWFLAKE_HOST'),
            'port': os.getenv('SNOWFLAKE_PORT'),
            'protocol': 'https',
            'account': config.SNOWFLAKE_ACCOUNT,
            'authenticator': 'oauth',
            'token': token,
            'warehouse': config.SNOWFLAKE_WAREHOUSE,
            'database': config.SNOWFLAKE_DATABASE,
            'schema': config.SNOWFLAKE_SCHEMA,
            'client_session_keep_alive': True
        }
        
        # Remove None values
        conn_params = {k: v for k, v in conn_params.items() if v is not None}
        
        logger.info(f"✓ Loaded OAuth token from {token_path}")
        if conn_params.get('host') and conn_params.get('port'):
            logger.info(f"✓ Using SPCS internal network: {conn_params['host']}:{conn_params['port']}")
        
        return snowflake.connector.connect(**conn_params)
    except FileNotFoundError:
        raise ValueError(f"OAuth token file not found at {token_path}")
    except Exception as e:
        raise ValueError(f"Failed to load OAuth token from {token_path}: {str(e)}")

def _get_password_connection():
    """
    Create Snowflake connection using password authentication.
    
    Returns:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection
    
    Raises:
        ValueError: If required credentials are missing
    """
    if not config.SNOWFLAKE_USER or not config.SNOWFLAKE_PASSWORD:
        raise ValueError("SNOWFLAKE_USER and SNOWFLAKE_PASSWORD are required for non-SPCS authentication")
    
    return snowflake.connector.connect(
        account=config.SNOWFLAKE_ACCOUNT,
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA
    )

# =============================================================================
# TRACK DATA RETRIEVAL
# =============================================================================

def get_snowflake_tracks(date: str, storm: str) -> pd.DataFrame:
    """
    Get hurricane track data from Snowflake TC_TRACKS table for report generation.
    
    Retrieves track points with coordinates, wind speeds, and pressure for all
    ensemble members at a specific forecast time.
    
    Args:
        date: Forecast date in YYYYMMDDHHMMSS format or datetime string
        storm: Storm identifier (e.g., 'JERRY', 'FUNG-WONG')
    
    Returns:
        pd.DataFrame: Hurricane track data with columns:
            - ENSEMBLE_MEMBER
            - VALID_TIME
            - LEAD_TIME
            - LATITUDE
            - LONGITUDE
            - WIND_SPEED_KNOTS
            - PRESSURE_HPA
        Returns empty DataFrame on error.
    """
    forecast_datetime = _normalize_forecast_time(date)
    logger.info(f"Loading tracks for storm={storm}, forecast_time={forecast_datetime}")
    
    query = '''
    SELECT 
        ENSEMBLE_MEMBER,
        VALID_TIME,
        LEAD_TIME,
        LATITUDE,
        LONGITUDE,
        WIND_SPEED_KNOTS,
        PRESSURE_HPA
    FROM TC_TRACKS
    WHERE TRACK_ID = %s AND FORECAST_TIME = %s
    ORDER BY ENSEMBLE_MEMBER, VALID_TIME
    '''
    
    return _execute_query(query, params=[storm, forecast_datetime])

def get_hurricane_data_from_snowflake(track_id: str, forecast_time: str) -> pd.DataFrame:
    """
    Get detailed hurricane track data from Snowflake TC_TRACKS table.
    
    Retrieves comprehensive track data including wind field polygons and radius
    information for multiple wind thresholds.
    
    Args:
        track_id: Storm identifier (e.g., 'JERRY', 'FUNG-WONG')
        forecast_time: Forecast time (e.g., '2025-10-10 00:00:00')
    
    Returns:
        pd.DataFrame: Hurricane track data with columns:
            - FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER
            - VALID_TIME, LEAD_TIME
            - LATITUDE, LONGITUDE
            - PRESSURE_HPA, WIND_SPEED_KNOTS
            - Radius data for 34kt, 50kt, 64kt winds (NE, SE, SW, NW)
            - WIND_FIELD_POLYGON_34KT, WIND_FIELD_POLYGON_50KT, WIND_FIELD_POLYGON_64KT (WKT)
        Returns empty DataFrame on error.
    """
    forecast_datetime = _normalize_forecast_time(forecast_time)
    
    query = """
    SELECT 
        FORECAST_TIME,
        TRACK_ID,
        ENSEMBLE_MEMBER,
        VALID_TIME,
        LEAD_TIME,
        LATITUDE,
        LONGITUDE,
        PRESSURE_HPA,
        WIND_SPEED_KNOTS,
        RADIUS_OF_MAXIMUM_WINDS_KM,
        RADIUS_34_KNOT_WINDS_NE_KM,
        RADIUS_34_KNOT_WINDS_SE_KM,
        RADIUS_34_KNOT_WINDS_SW_KM,
        RADIUS_34_KNOT_WINDS_NW_KM,
        RADIUS_50_KNOT_WINDS_NE_KM,
        RADIUS_50_KNOT_WINDS_SE_KM,
        RADIUS_50_KNOT_WINDS_SW_KM,
        RADIUS_50_KNOT_WINDS_NW_KM,
        RADIUS_64_KNOT_WINDS_NE_KM,
        RADIUS_64_KNOT_WINDS_SE_KM,
        RADIUS_64_KNOT_WINDS_SW_KM,
        RADIUS_64_KNOT_WINDS_NW_KM,
        ST_ASWKT(WIND_FIELD_POLYGON_34KT) AS WIND_FIELD_POLYGON_34KT,
        ST_ASWKT(WIND_FIELD_POLYGON_50KT) AS WIND_FIELD_POLYGON_50KT,
        ST_ASWKT(WIND_FIELD_POLYGON_64KT) AS WIND_FIELD_POLYGON_64KT
    FROM TC_TRACKS
    WHERE TRACK_ID = %s AND FORECAST_TIME = %s
    ORDER BY ENSEMBLE_MEMBER, LEAD_TIME
    """
    
    return _execute_query(query, params=[track_id, forecast_datetime])

# =============================================================================
# ENVELOPE DATA RETRIEVAL
# =============================================================================

def get_envelopes_from_snowflake(track_id: str, forecast_time: str) -> pd.DataFrame:
    """
    Get envelope data from Snowflake TC_ENVELOPES_COMBINED table.
    
    Retrieves hurricane envelope regions (polygons) for all ensemble members
    and wind thresholds at a specific forecast time.
    
    Args:
        track_id: Storm identifier (e.g., 'JERRY', 'FUNG-WONG')
        forecast_time: Forecast time (e.g., '2025-10-10 00:00:00' or '20251010000000')
    
    Returns:
        pd.DataFrame: Envelope data with columns:
            - FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER
            - LEAD_TIME_RANGE, WIND_THRESHOLD
            - ENVELOPE_REGION (WKT format)
        Returns empty DataFrame on error.
    """
    forecast_datetime = _normalize_forecast_time(forecast_time)
    
    query = """
    SELECT 
        FORECAST_TIME,
        TRACK_ID,
        ENSEMBLE_MEMBER,
        LEAD_TIME_RANGE,
        WIND_THRESHOLD,
        ST_ASWKT(ENVELOPE_REGION) AS ENVELOPE_REGION
    FROM TC_ENVELOPES_COMBINED
    WHERE TRACK_ID = %s AND FORECAST_TIME = %s
    ORDER BY ENSEMBLE_MEMBER, WIND_THRESHOLD
    """
    
    return _execute_query(query, params=[track_id, forecast_datetime])

def convert_envelopes_to_geodataframe(envelopes_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert envelope DataFrame to GeoDataFrame for geospatial processing.
    
    Parses WKT geometry strings from Snowflake and creates a GeoDataFrame
    with proper column naming for downstream processing.
    
    Args:
        envelopes_df: DataFrame with envelope data from Snowflake, must contain
                     'ENVELOPE_REGION' column with WKT strings
    
    Returns:
        gpd.GeoDataFrame: Envelopes as GeoDataFrame with:
            - geometry column (Shapely geometries)
            - Lowercase column names (ensemble_member, wind_threshold, etc.)
            - CRS: EPSG:4326
        Returns empty GeoDataFrame if input is empty or all geometries are invalid.
    """
    if envelopes_df.empty:
        return gpd.GeoDataFrame()
    
    # Parse WKT polygons
    geometries = []
    for wkt_str in envelopes_df['ENVELOPE_REGION']:
        if pd.notna(wkt_str) and wkt_str:
            try:
                geom = shapely_wkt.loads(wkt_str)
                geometries.append(geom)
            except Exception as e:
                logger.warning(f"Failed to parse WKT geometry: {e}")
                geometries.append(None)
        else:
            geometries.append(None)
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(envelopes_df, geometry=geometries, crs='EPSG:4326')
    
    # Rename columns to lowercase for consistency with processing functions
    column_mapping = {
        'ENSEMBLE_MEMBER': 'ensemble_member',
        'WIND_THRESHOLD': 'wind_threshold',
        'ENVELOPE_REGION': 'envelope_region'
    }
    gdf = gdf.rename(columns=column_mapping)
    
    # Remove rows with invalid geometries
    gdf = gdf[gdf.geometry.notna()]
    
    if len(gdf) < len(envelopes_df):
        logger.warning(f"Removed {len(envelopes_df) - len(gdf)} rows with invalid geometries")
    
    return gdf

# =============================================================================
# METADATA QUERIES
# =============================================================================

def get_available_wind_thresholds(storm: str, forecast_time: str) -> List[str]:
    """
    Get available wind thresholds for a specific storm and forecast time.
    
    Queries Snowflake to determine which wind thresholds have envelope data
    available for the specified storm and forecast time.
    
    Args:
        storm: Storm name (e.g., 'FENGSHEN', 'FUNG-WONG')
        forecast_time: Forecast time string (e.g., '2025-10-20 00:00:00' or '20251020000000')
    
    Returns:
        List[str]: Available wind thresholds as strings, sorted numerically.
                  Returns empty list if none found or on error.
    """
    forecast_datetime = _normalize_forecast_time(forecast_time)
    
    query = """
    SELECT DISTINCT WIND_THRESHOLD 
    FROM TC_ENVELOPES_COMBINED 
    WHERE TRACK_ID = %s 
    AND FORECAST_TIME = %s
    ORDER BY WIND_THRESHOLD
    """
    
    df = _execute_query(query, params=[storm, forecast_datetime])
    
    if not df.empty:
        # Convert to list of strings and sort numerically
        thresholds = [str(int(th)) for th in df['WIND_THRESHOLD'].tolist()]
        thresholds.sort(key=int)
        logger.info(f"Found {len(thresholds)} wind thresholds for {storm} at {forecast_datetime}: {thresholds}")
        return thresholds
    else:
        logger.warning(f"No wind thresholds found for {storm} at {forecast_datetime}")
        return []

def get_latest_forecast_time_overall() -> Optional[datetime]:
    """
    Get the latest forecast issue time from Snowflake across all storms.
    
    Queries the TC_TRACKS table to find the most recent forecast time,
    which indicates when the latest forecast was issued.
    
    Returns:
        Optional[datetime]: Latest forecast issue time, or None if no data found or on error.
    """
    query = """
    SELECT MAX(FORECAST_TIME) as MAX_FORECAST_TIME
    FROM TC_TRACKS
    """
    
    df = _execute_query(query)
    
    if not df.empty and pd.notna(df['MAX_FORECAST_TIME'].iloc[0]):
        latest_time = df['MAX_FORECAST_TIME'].iloc[0]
        logger.info(f"Latest forecast time found: {latest_time}")
        return latest_time
    else:
        logger.warning("No forecast data found in Snowflake")
        return None

def get_envelope_data_snowflake(track_id: str, forecast_time: str) -> pd.DataFrame:
    """
    Get envelope data directly from Snowflake (alternative interface).
    
    This is a simplified version of get_envelopes_from_snowflake that returns
    data with renamed columns for compatibility with some legacy code.
    
    Args:
        track_id: Storm identifier (e.g., 'JERRY', 'FUNG-WONG')
        forecast_time: Forecast time (e.g., '2025-10-10 00:00:00' or '20251010000000')
    
    Returns:
        pd.DataFrame: Envelope data with columns:
            - ENSEMBLE_MEMBER
            - wind_threshold (renamed from WIND_THRESHOLD)
            - geometry (renamed from ENVELOPE_REGION)
        Returns empty DataFrame on error.
    
    Note:
        Consider using get_envelopes_from_snowflake() for new code, as it provides
        more complete data including FORECAST_TIME and TRACK_ID.
    """
    forecast_datetime = _normalize_forecast_time(forecast_time)
    
    query = '''
    SELECT 
        ENSEMBLE_MEMBER,
        WIND_THRESHOLD,
        ENVELOPE_REGION
    FROM TC_ENVELOPES_COMBINED
    WHERE TRACK_ID = %s AND FORECAST_TIME = %s
    ORDER BY ENSEMBLE_MEMBER, WIND_THRESHOLD
    '''
    
    df = _execute_query(query, params=[track_id, forecast_datetime])
    
    if not df.empty:
        # Rename columns to match expected format
        df = df.rename(columns={'ENVELOPE_REGION': 'geometry', 'WIND_THRESHOLD': 'wind_threshold'})
    
    return df

def get_snowflake_data() -> pd.DataFrame:
    """
    Get hurricane metadata directly from Snowflake.
    
    Retrieves unique storm/forecast combinations with ensemble member counts
    from the TC_TRACKS table. Used for discovering available storms and
    forecast times in the database.
    
    Returns:
        pd.DataFrame: Metadata with columns:
            - TRACK_ID: Storm identifier
            - FORECAST_TIME: Forecast issue time
            - ENSEMBLE_COUNT: Number of ensemble members
        Returns empty DataFrame with these columns on error.
    """
    query = '''
    SELECT DISTINCT 
        TRACK_ID,
        FORECAST_TIME,
        COUNT(DISTINCT ENSEMBLE_MEMBER) as ENSEMBLE_COUNT
    FROM TC_TRACKS
    GROUP BY TRACK_ID, FORECAST_TIME
    ORDER BY FORECAST_TIME ASC, TRACK_ID
    '''
    
    df = _execute_query(query)
    
    if df.empty:
        # Return DataFrame with correct structure even if empty
        return pd.DataFrame({'TRACK_ID': [], 'FORECAST_TIME': [], 'ENSEMBLE_COUNT': []})
    
    return df