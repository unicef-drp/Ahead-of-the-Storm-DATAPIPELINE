#!/usr/bin/env python3
"""
Country Management Utilities

This module provides functions to manage countries in the pipeline,
including reading from Snowflake table and adding new countries.
"""

import pandas as pd
import logging
from snowflake_utils import get_snowflake_connection

logger = logging.getLogger(__name__)

def get_active_countries_from_snowflake():
    """
    Get list of active countries from Snowflake PIPELINE_COUNTRIES table.
    
    Returns:
        list: List of active country codes (e.g., ['TWN', 'DOM', 'VNM'])
    """
    try:
        conn = get_snowflake_connection()
        query = """
            SELECT COUNTRY_CODE 
            FROM PIPELINE_COUNTRIES 
            WHERE ACTIVE = TRUE 
            ORDER BY COUNTRY_CODE
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        countries = df['COUNTRY_CODE'].tolist()
        logger.info(f"Retrieved {len(countries)} active countries from Snowflake: {', '.join(countries)}")
        return countries
    except Exception as e:
        logger.error(f"Error retrieving countries from Snowflake: {e}")
        logger.warning("Falling back to empty list")
        return []

def get_all_countries_from_snowflake(include_inactive=False):
    """
    Get all countries from Snowflake PIPELINE_COUNTRIES table.
    
    Args:
        include_inactive: If True, include inactive countries
    
    Returns:
        pandas.DataFrame: DataFrame with country information
    """
    try:
        conn = get_snowflake_connection()
        if include_inactive:
            query = "SELECT * FROM PIPELINE_COUNTRIES ORDER BY COUNTRY_CODE"
        else:
            query = "SELECT * FROM PIPELINE_COUNTRIES WHERE ACTIVE = TRUE ORDER BY COUNTRY_CODE"
        
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Error retrieving countries from Snowflake: {e}")
        return pd.DataFrame()

def add_country_to_snowflake(country_code, country_name, zoom_level=14, center_lat=None, center_lon=None, view_zoom=None, notes=None):
    """
    Add a new country to the Snowflake PIPELINE_COUNTRIES table.
    
    Args:
        country_code: ISO3 country code (e.g., 'TWN')
        country_name: Full country name (e.g., 'Taiwan')
        zoom_level: Zoom level for mercator tiles (default: 14)
        center_lat: Latitude for map center (required for visualization)
        center_lon: Longitude for map center (required for visualization)
        view_zoom: Zoom level for visualization map (required for visualization)
        notes: Optional notes about the country
    
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Check if country already exists
        cursor.execute("SELECT COUNTRY_CODE FROM PIPELINE_COUNTRIES WHERE COUNTRY_CODE = %s", (country_code,))
        if cursor.fetchone():
            logger.warning(f"Country {country_code} already exists in table")
            return False
        
        # Insert new country
        cursor.execute("""
            INSERT INTO PIPELINE_COUNTRIES 
                (COUNTRY_CODE, COUNTRY_NAME, ZOOM_LEVEL, CENTER_LAT, CENTER_LON, VIEW_ZOOM, NOTES, ACTIVE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
        """, (country_code, country_name, zoom_level, center_lat, center_lon, view_zoom, notes))
        
        conn.commit()
        logger.info(f"Successfully added country {country_code} ({country_name}) to Snowflake table")
        return True
    except Exception as e:
        logger.error(f"Error adding country {country_code} to Snowflake: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

def update_country_initialized(country_code, zoom_level=None):
    """
    Update the initialization timestamp for a country and zoom level.
    
    Updates BOTH tables:
    1. PIPELINE_COUNTRY_ZOOM_LEVELS - Tracks specific zoom level initialization (primary tracking)
    2. PIPELINE_COUNTRIES - Updates LAST_INITIALIZED for quick queries of overall initialization status
    
    Args:
        country_code: ISO3 country code
        zoom_level: Zoom level (optional, if None uses default from PIPELINE_COUNTRIES)
    """
    conn = None
    cursor = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # If zoom_level not provided, get default from PIPELINE_COUNTRIES
        if zoom_level is None:
            cursor.execute("""
                SELECT ZOOM_LEVEL FROM PIPELINE_COUNTRIES WHERE COUNTRY_CODE = %s
            """, (country_code,))
            result = cursor.fetchone()
            if result:
                zoom_level = result[0]
            else:
                logger.warning(f"Country {country_code} not found in PIPELINE_COUNTRIES")
                return
        
        # Update or insert in PIPELINE_COUNTRY_ZOOM_LEVELS (primary tracking)
        # Check if record exists first, then UPDATE or INSERT
        cursor.execute("""
            SELECT COUNT(*) 
            FROM PIPELINE_COUNTRY_ZOOM_LEVELS 
            WHERE COUNTRY_CODE = %s AND ZOOM_LEVEL = %s
        """, (country_code, zoom_level))
        
        exists = cursor.fetchone()[0] > 0
        
        if exists:
            # Update existing record
            cursor.execute("""
                UPDATE PIPELINE_COUNTRY_ZOOM_LEVELS
                SET LAST_INITIALIZED = CURRENT_TIMESTAMP()
                WHERE COUNTRY_CODE = %s AND ZOOM_LEVEL = %s
            """, (country_code, zoom_level))
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO PIPELINE_COUNTRY_ZOOM_LEVELS (COUNTRY_CODE, ZOOM_LEVEL, LAST_INITIALIZED)
                VALUES (%s, %s, CURRENT_TIMESTAMP())
            """, (country_code, zoom_level))
        
        # Also update PIPELINE_COUNTRIES
        cursor.execute("""
            UPDATE PIPELINE_COUNTRIES
            SET LAST_INITIALIZED = CURRENT_TIMESTAMP()
            WHERE COUNTRY_CODE = %s
        """, (country_code,))
        
        conn.commit()
        logger.info(f"Updated initialization timestamp for {country_code} at zoom level {zoom_level}")
    except Exception as e:
        logger.error(f"Error updating initialization timestamp for {country_code}: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

def activate_country(country_code):
    """
    Activate a country (set ACTIVE = TRUE).
    
    Args:
        country_code: ISO3 country code
    """
    conn = None
    cursor = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE PIPELINE_COUNTRIES
            SET ACTIVE = TRUE
            WHERE COUNTRY_CODE = %s
        """, (country_code,))
        conn.commit()
        logger.info(f"Activated country {country_code}")
    except Exception as e:
        logger.error(f"Error activating country {country_code}: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

def deactivate_country(country_code):
    """
    Deactivate a country (set ACTIVE = FALSE).
    
    Args:
        country_code: ISO3 country code
    """
    conn = None
    cursor = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE PIPELINE_COUNTRIES
            SET ACTIVE = FALSE
            WHERE COUNTRY_CODE = %s
        """, (country_code,))
        conn.commit()
        logger.info(f"Deactivated country {country_code}")
    except Exception as e:
        logger.error(f"Error deactivating country {country_code}: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

def get_countries_needing_initialization(zoom_level=None):
    """
    Get list of countries that have never been initialized at a specific zoom level.
    
    Args:
        zoom_level: Zoom level to check (if None, uses default from PIPELINE_COUNTRIES)
    
    Returns:
        list: List of country codes that need initialization
    """
    try:
        conn = get_snowflake_connection()
        
        if zoom_level is None:
            # Get countries that haven't been initialized at their default zoom level
            query = """
                SELECT c.COUNTRY_CODE 
                FROM PIPELINE_COUNTRIES c
                LEFT JOIN PIPELINE_COUNTRY_ZOOM_LEVELS z 
                    ON c.COUNTRY_CODE = z.COUNTRY_CODE 
                    AND c.ZOOM_LEVEL = z.ZOOM_LEVEL
                WHERE c.ACTIVE = TRUE 
                  AND z.LAST_INITIALIZED IS NULL
                ORDER BY c.COUNTRY_CODE
            """
        else:
            # Get countries that haven't been initialized at the specified zoom level
            query = """
                SELECT c.COUNTRY_CODE 
                FROM PIPELINE_COUNTRIES c
                LEFT JOIN PIPELINE_COUNTRY_ZOOM_LEVELS z 
                    ON c.COUNTRY_CODE = z.COUNTRY_CODE 
                    AND z.ZOOM_LEVEL = %s
                WHERE c.ACTIVE = TRUE 
                  AND z.LAST_INITIALIZED IS NULL
                ORDER BY c.COUNTRY_CODE
            """
        
        if zoom_level is None:
            df = pd.read_sql(query, conn)
        else:
            df = pd.read_sql(query, conn, params=[zoom_level])
        
        conn.close()
        return df['COUNTRY_CODE'].tolist()
    except Exception as e:
        logger.error(f"Error retrieving countries needing initialization: {e}")
        return []

def get_initialized_zoom_levels(country_code):
    """
    Get list of zoom levels that have been initialized for a country.
    
    Args:
        country_code: ISO3 country code
    
    Returns:
        list: List of initialized zoom levels
    """
    try:
        conn = get_snowflake_connection()
        query = """
            SELECT ZOOM_LEVEL 
            FROM PIPELINE_COUNTRY_ZOOM_LEVELS 
            WHERE COUNTRY_CODE = %s
            ORDER BY ZOOM_LEVEL
        """
        df = pd.read_sql(query, conn, params=[country_code])
        conn.close()
        return df['ZOOM_LEVEL'].tolist()
    except Exception as e:
        logger.error(f"Error retrieving initialized zoom levels for {country_code}: {e}")
        return []

def get_countries_needing_zoom_level(country_code, zoom_level):
    """
    Check if a specific country+zoom combination needs initialization.
    
    Args:
        country_code: ISO3 country code
        zoom_level: Zoom level to check
    
    Returns:
        bool: True if needs initialization, False if already initialized
    """
    conn = None
    try:
        conn = get_snowflake_connection()
        query = """
            SELECT COUNT(*) as COUNT
            FROM PIPELINE_COUNTRY_ZOOM_LEVELS 
            WHERE COUNTRY_CODE = %s AND ZOOM_LEVEL = %s
        """
        df = pd.read_sql(query, conn, params=[country_code, zoom_level])
        return df['COUNT'].iloc[0] == 0
    except Exception as e:
        logger.error(f"Error checking zoom level initialization: {e}")
        return True  # Assume needs initialization if error
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def update_country_map_config(country_code, center_lat=None, center_lon=None, view_zoom=None):
    """
    Update the map configuration (center coordinates and/or view zoom) for a country.
    Only updates fields that are provided (not None).
    
    Args:
        country_code: ISO3 country code
        center_lat: Optional latitude for map center (None to skip update)
        center_lon: Optional longitude for map center (None to skip update)
        view_zoom: Optional zoom level for visualization map (None to skip update)
    
    Returns:
        bool: True if successful, False otherwise
    
    Raises:
        ValueError: If all parameters are None (nothing to update)
    """
    # Validate that at least one field is provided
    if center_lat is None and center_lon is None and view_zoom is None:
        raise ValueError("At least one of center_lat, center_lon, or view_zoom must be provided")
    
    conn = None
    cursor = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Check if country exists
        cursor.execute("SELECT COUNTRY_CODE FROM PIPELINE_COUNTRIES WHERE COUNTRY_CODE = %s", (country_code,))
        if not cursor.fetchone():
            logger.warning(f"Country {country_code} not found in table")
            return False
        
        # Build UPDATE query dynamically based on provided parameters
        update_fields = []
        update_values = []
        
        if center_lat is not None:
            update_fields.append("CENTER_LAT = %s")
            update_values.append(center_lat)
        
        if center_lon is not None:
            update_fields.append("CENTER_LON = %s")
            update_values.append(center_lon)
        
        if view_zoom is not None:
            update_fields.append("VIEW_ZOOM = %s")
            update_values.append(view_zoom)
        
        # Add country_code for WHERE clause
        update_values.append(country_code)
        
        # Execute update
        update_query = f"""
            UPDATE PIPELINE_COUNTRIES
            SET {', '.join(update_fields)}
            WHERE COUNTRY_CODE = %s
        """
        
        cursor.execute(update_query, tuple(update_values))
        
        conn.commit()
        
        # Build log message
        updated_fields = []
        if center_lat is not None:
            updated_fields.append(f"center_lat={center_lat}")
        if center_lon is not None:
            updated_fields.append(f"center_lon={center_lon}")
        if view_zoom is not None:
            updated_fields.append(f"view_zoom={view_zoom}")
        
        logger.info(f"Updated map configuration for {country_code}: {', '.join(updated_fields)}")
        return True
    except Exception as e:
        logger.error(f"Error updating map configuration for {country_code}: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass
