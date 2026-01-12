#!/usr/bin/env python3
"""
Impact Report Generation Module

This module generates comprehensive JSON impact reports from hurricane analysis views.
Reports include expected impacts, changes from previous forecasts, vulnerability metrics,
and administrative-level breakdowns.

Key Features:
- Generates structured JSON reports from impact analysis views
- Calculates changes from previous forecast times
- Includes vulnerability metrics (poverty, severity, urban/rural)
- Provides administrative-level impact breakdowns
- Tracks top at-risk facilities (schools and health centers)
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

# Import GigaSpatial components
from gigaspatial.handlers import AdminBoundaries
from gigaspatial.core.io.writers import write_dataset

# Import centralized configuration and data store utility
from config import config
from data_store_utils import get_data_store

# =============================================================================
# CONSTANTS
# =============================================================================

# Storm category definitions (wind speed in knots -> category name)
STORM_CATEGORIES = {
    34: 'Tropical Storm',
    40: 'Strong Tropical Storm',
    50: 'Very Strong TS',
    64: 'Cat 1 Hurricane',
    83: 'Cat 2 Hurricane',
    96: 'Cat 3 Hurricane',
    113: 'Cat 4 Hurricane',
    137: 'Cat 5 Hurricane'
}

# Configuration constants
KEY_FOR_EXPECTED = 34  # Wind threshold used for "expected" impact calculations
SEVERE_RWI_THRESHOLD = -1.0  # RWI threshold for severe poverty
POVERTY_RWI_THRESHOLD = -0.5  # RWI threshold for poverty
URBAN_SMOD_THRESHOLD = 20  # SMOD threshold for urban classification
PREVIOUS_FORECAST_HOURS = 6  # Hours to look back for previous forecast
NEXT_FORECAST_HOURS = 6  # Hours ahead for next forecast
TOP_FACILITIES_COUNT = 5  # Number of top facilities to report

# =============================================================================
# CONFIGURATION
# =============================================================================

RESULTS_DIR = config.RESULTS_DIR
REPORTS_JSON_DIR = config.REPORTS_JSON_DIR
REPORT_TEMPLATE_PATH = config.REPORT_TEMPLATE_PATH

# =============================================================================
# DATA STORE INITIALIZATION
# =============================================================================

# Lazy initialization of data store (only when needed)
_data_store = None

def _get_data_store():
    """Get or create the data store instance (lazy initialization)"""
    global _data_store
    if _data_store is None:
        _data_store = get_data_store()
    return _data_store

# Initialize logger
logger = logging.getLogger(__name__)

# =============================================================================
# REPORT TEMPLATE STRUCTURE
# =============================================================================

# Template dictionary defining all expected report keys with default values
# Used for validation to ensure all required fields are present
REPORT_TEMPLATE = {
    'storm': '', 'forecast_date': '', 'expected_landfall': '', 'storm_category': '', 'country': '',
    'expected_children': 0, 'expected_school_age': 0, 'expected_infants': 0,
    'expected_schools': 0, 'expected_hcs': 0,
    'children_change_direction': '', 'children_change': 0, 'children_change_perc': 0,
    'rows_admins_pop_total': [], 'rows_admins_school': [], 'rows_admins_infant': [],
    'rows_schools_winds': [], 'rows_hcs_winds': [],
    'expected_pop': 0, 'expected_cci_pop': 0, 'expected_cci_school': 0, 'expected_cci_infant': 0,
    'expected_pop_poverty': 0, 'expected_pop_severe': 0, 'expected_pop_urban': 0, 'expected_pop_rural': 0,
    'expected_school_poverty': 0, 'expected_school_severe': 0, 'expected_school_urban': 0, 'expected_school_rural': 0,
    'expected_infant_poverty': 0, 'expected_infant_severe': 0, 'expected_infant_urban': 0, 'expected_infant_rural': 0,
    'next_forecast_date': '', 'report_date': ''
}

# Add wind threshold-specific keys to template
for wind in STORM_CATEGORIES.keys():
    REPORT_TEMPLATE.update({
        f'expected_children_{wind}': 0, f'change_children_{wind}': '',
        f'expected_school_{wind}': 0, f'change_school_{wind}': '',
        f'expected_infant_{wind}': 0, f'change_infant_{wind}': '',
        f'expected_pop_{wind}': 0,
        f'expected_schools_{wind}': 0, f'change_schools_{wind}': '',
        f'expected_hcs_{wind}': 0, f'change_hcs_{wind}': ''
    })

# Add top facilities keys
for i in range(1, TOP_FACILITIES_COUNT + 1):
    REPORT_TEMPLATE.update({
        f'school_name_{i}': '', f'school_edulevel_{i}': '', f'school_prob_{i}': 0,
        f'hc_name_{i}': '', f'hc_type_{i}': '', f'hc_prob_{i}': 0
    })


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def save_json_report(d: Dict[str, Any], country: str, storm: str, date: str) -> None:
    """
    Save JSON report to storage.
    
    Args:
        d: Dictionary containing report data
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
    
    Note:
        If the report dictionary is empty, no file is saved.
    """
    if not d:  # If empty, do not save file
        return
    
    file = f"{country}_{storm}_{date}.json"
    filename = os.path.join(RESULTS_DIR, REPORTS_JSON_DIR, file)
    data_store = _get_data_store()
    write_dataset(d, data_store, filename)

def load_json_report(country: str, storm: str, date: str) -> Dict[str, Any]:
    """
    Load JSON report from storage.
    
    Args:
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
    
    Returns:
        dict: Report data dictionary, or empty dict if file doesn't exist
    """
    file = f"{country}_{storm}_{date}.json"
    filename = os.path.join(RESULTS_DIR, REPORTS_JSON_DIR, file)
    data_store = _get_data_store()
    
    if data_store.file_exists(filename):
        try:
            with data_store.open(filename, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading report {filename}: {e}")
            return {}
    return {}

def get_previous_date(date: str) -> str:
    """
    Get the previous forecast date (6 hours earlier).
    
    Args:
        date: Forecast date in YYYYMMDDHHMMSS format
    
    Returns:
        str: Previous forecast date in YYYYMMDDHHMMSS format
    """
    dt = datetime.strptime(date, "%Y%m%d%H%M%S")
    dt_minus_6h = dt - timedelta(hours=PREVIOUS_FORECAST_HOURS)
    return dt_minus_6h.strftime("%Y%m%d%H%M%S")

def get_future_date(date: str, delta_hours: int) -> str:
    """
    Get a future date by adding hours to the current date.
    
    Args:
        date: Forecast date in YYYYMMDDHHMMSS format
        delta_hours: Number of hours to add
    
    Returns:
        str: Future date formatted as "Month Day, Year HH:MM UTC"
    """
    dt = datetime.strptime(date, "%Y%m%d%H%M%S")
    dt_plus_delta = dt + timedelta(hours=delta_hours)
    return dt_plus_delta.strftime("%B %d, %Y %H:%M UTC")    

def get_lines_from_points(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Convert point GeoDataFrame to line segments.
    
    Creates LineString geometries connecting consecutive points, preserving
    attributes from the first point of each segment.
    
    Args:
        gdf: GeoDataFrame with point geometries
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame with LineString geometries connecting consecutive points
    """
    if len(gdf) < 2:
        return gpd.GeoDataFrame(columns=gdf.columns, crs=gdf.crs)
    
    lines = []
    for i in range(len(gdf) - 1):
        point1 = gdf.geometry.iloc[i]
        point2 = gdf.geometry.iloc[i + 1]
        
        # Create a LineString from point i to i+1
        line = LineString([point1, point2])
        
        # Copy attributes from row i
        row_data = gdf.iloc[i].drop("geometry").to_dict()
        row_data["geometry"] = line
        
        lines.append(row_data)

    return gpd.GeoDataFrame(lines, crs=gdf.crs)

def get_expected_landfall(gdf_tracks: gpd.GeoDataFrame, date: str, country: str) -> str:
    """
    Calculate expected landfall time based on deterministic track (ensemble member 51).
    
    Args:
        gdf_tracks: GeoDataFrame containing storm track points
        date: Current forecast date in YYYYMMDDHHMMSS format
        country: ISO3 country code
    
    Returns:
        str: Expected landfall time as formatted string, or "Unknown" if cannot be determined
    """
    if gdf_tracks.empty:
        return "Unknown"
    
    # Use deterministic track (ensemble member 51)
    gdf_det = gdf_tracks[gdf_tracks.ENSEMBLE_MEMBER == 51]
    
    if gdf_det.empty:
        return "Unknown"
    
    try:
        # Get country boundary
        boundary = AdminBoundaries.create(country_code=country, admin_level=0)
        polygon = boundary.to_geodataframe().geometry.iloc[0]
        
        # Check if any points are within the country
        inside_rows = gdf_det[gdf_det.within(polygon)]
        
        if inside_rows.empty:
            # Points may land outside country but track crosses boundary
            # Convert points to lines and check intersections
            gdf_det_lines = get_lines_from_points(gdf_det)
            inside_rows_lines = gdf_det_lines[gdf_det_lines.intersects(polygon)]
            
            if inside_rows_lines.empty:
                return "Unknown"
            
            lead_time = int(inside_rows_lines.iloc[0]["LEAD_TIME"])
        else:
            lead_time = int(inside_rows.iloc[0]["LEAD_TIME"])
        
        if lead_time == 0:
            return "Already landed"
        
        return get_future_date(date, lead_time)
    except Exception as e:
        logger.warning(f"Error calculating expected landfall for {country}: {e}")
        return "Unknown"


# =============================================================================
# REPORT GENERATION FUNCTIONS
# =============================================================================

def _get_max_wind_threshold(wind_admin_views: Dict[int, pd.DataFrame]) -> int:
    """
    Find the maximum wind threshold with non-zero probability.
    
    Args:
        wind_admin_views: Dictionary of wind threshold admin views
    
    Returns:
        int: Maximum wind threshold with impact, or 0 if no impact
    """
    max_wind = 0
    for wind in sorted(STORM_CATEGORIES.keys()):
        if wind not in wind_admin_views:
            continue
        if wind_admin_views[wind]['probability'].sum() > 0:
            max_wind = wind
        else:
            # Since thresholds are ordered and envelopes overlap,
            # once we hit zero probability, we've found the max
            break
    return max_wind

def _get_expected_wind_threshold(wind_tiles_views: Dict[int, pd.DataFrame]) -> Optional[int]:
    """
    Get the wind threshold to use for expected impact calculations.
    
    Args:
        wind_tiles_views: Dictionary of wind threshold tile views
    
    Returns:
        int: Wind threshold to use, or None if no views available
    """
    if not wind_tiles_views:
        return None
    
    if KEY_FOR_EXPECTED in wind_tiles_views:
        return KEY_FOR_EXPECTED
    
    return min(wind_tiles_views.keys()) if wind_tiles_views else None

def _calculate_children_change(current: int, previous: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate change in expected children from previous forecast.
    
    Args:
        current: Current expected children count
        previous: Previous report dictionary
    
    Returns:
        dict: Dictionary with change_direction, change, and change_perc
    """
    if not previous:
        return {
            'children_change_direction': 'increased',
            'children_change': f"+{current}",
            'children_change_perc': "-"
        }
    
    prev_children = previous.get('expected_children', 0)
    change = current - prev_children
    
    if change > 0:
        return {
            'children_change_direction': 'increased',
            'children_change': f"+{change}",
            'children_change_perc': (change / prev_children * 100) if prev_children > 0 else 0
        }
    else:
        return {
            'children_change_direction': 'decreased',
            'children_change': str(change),
            'children_change_perc': (abs(change) / prev_children * 100) if prev_children > 0 else 0
        }

def _calculate_vulnerability_metrics(tiles_df: pd.DataFrame) -> Dict[str, int]:
    """
    Calculate vulnerability metrics (urban/rural, poverty/severe) from tiles.
    
    Args:
        tiles_df: DataFrame with tile impact data
    
    Returns:
        dict: Dictionary with vulnerability metrics
    """
    result = {
        'expected_pop_urban': 0, 'expected_pop_rural': 0,
        'expected_school_urban': 0, 'expected_school_rural': 0,
        'expected_infant_urban': 0, 'expected_infant_rural': 0,
        'expected_pop_poverty': 0, 'expected_pop_severe': 0,
        'expected_school_poverty': 0, 'expected_school_severe': 0,
        'expected_infant_poverty': 0, 'expected_infant_severe': 0
    }
    
    # Urban/Rural classification based on SMOD
    tiles_smod = tiles_df.dropna(subset=['E_smod_class'])
    if not tiles_smod.empty:
        # Filter tiles with probability > 0
        tiles_with_prob = tiles_smod[tiles_smod['probability'] > 0]
        if not tiles_with_prob.empty:
            # Calculate actual SMOD by dividing expected by probability
            actual_smod = tiles_with_prob['E_smod_class'] / tiles_with_prob['probability']
            urban_mask = actual_smod >= URBAN_SMOD_THRESHOLD
            rural_mask = actual_smod < URBAN_SMOD_THRESHOLD
            
            urban_tiles = tiles_with_prob[urban_mask]
            rural_tiles = tiles_with_prob[rural_mask]
            
            if not urban_tiles.empty:
                result['expected_pop_urban'] = int(urban_tiles['E_population'].sum())
                result['expected_school_urban'] = int(urban_tiles['E_school_age_population'].sum())
                result['expected_infant_urban'] = int(urban_tiles['E_infant_population'].sum())
            
            if not rural_tiles.empty:
                result['expected_pop_rural'] = int(rural_tiles['E_population'].sum())
                result['expected_school_rural'] = int(rural_tiles['E_school_age_population'].sum())
                result['expected_infant_rural'] = int(rural_tiles['E_infant_population'].sum())
    
    # Poverty/Severe classification based on RWI
    tiles_rwi = tiles_df.dropna(subset=['E_rwi'])
    if not tiles_rwi.empty:
        tiles_with_prob = tiles_rwi[tiles_rwi['probability'] > 0]
        if not tiles_with_prob.empty:
            # Calculate actual RWI by dividing expected by probability
            actual_rwi = tiles_with_prob['E_rwi'] / tiles_with_prob['probability']
            poverty_mask = (actual_rwi >= SEVERE_RWI_THRESHOLD) & (actual_rwi < POVERTY_RWI_THRESHOLD)
            severe_mask = actual_rwi < SEVERE_RWI_THRESHOLD
            
            poverty_tiles = tiles_with_prob[poverty_mask]
            severe_tiles = tiles_with_prob[severe_mask]
            
            if not poverty_tiles.empty:
                result['expected_pop_poverty'] = int(poverty_tiles['E_population'].sum())
                result['expected_school_poverty'] = int(poverty_tiles['E_school_age_population'].sum())
                result['expected_infant_poverty'] = int(poverty_tiles['E_infant_population'].sum())
            
            if not severe_tiles.empty:
                result['expected_pop_severe'] = int(severe_tiles['E_population'].sum())
                result['expected_school_severe'] = int(severe_tiles['E_school_age_population'].sum())
                result['expected_infant_severe'] = int(severe_tiles['E_infant_population'].sum())
    
    return result

def _calculate_admin_rows(wind_admin_views: Dict[int, pd.DataFrame],
                          cci_admin_view: pd.DataFrame,
                          gdf_admin: gpd.GeoDataFrame,
                          d_previous: Dict[str, Any]) -> Dict[str, list]:
    """
    Calculate administrative-level impact rows for the report.
    
    Args:
        wind_admin_views: Dictionary mapping wind thresholds to admin-level views
        cci_admin_view: DataFrame with CCI values per admin
        gdf_admin: GeoDataFrame with admin boundaries
        d_previous: Previous report dictionary for change calculations
    
    Returns:
        dict: Dictionary with keys 'rows_admins_pop_total', 'rows_admins_school',
              'rows_admins_infant', 'rows_schools_winds', 'rows_hcs_winds'
    """
    rows_admins_pop_total = []
    rows_admins_school = []
    rows_admins_infant = []
    rows_schools_winds = []
    rows_hcs_winds = []
    
    for i, (_, row) in enumerate(gdf_admin.iterrows()):
        admin_id = row['tile_id']
        admin_name = row['name']
        
        # Initialize row dictionaries
        d_rows_admins_pop_total = {'name': admin_name}
        d_rows_admins_school = {'name': admin_name}
        d_rows_admins_infant = {'name': admin_name}
        d_rows_schools_winds = {'name': admin_name}
        d_rows_hcs_winds = {'name': admin_name}
        
        # Calculate values for each wind threshold
        for wind in STORM_CATEGORIES.keys():
            if wind not in wind_admin_views:
                # Set to 0 if wind threshold not present
                d_rows_admins_pop_total[f"{wind}"] = 0
                d_rows_admins_school[f"{wind}"] = 0
                d_rows_admins_infant[f"{wind}"] = 0
                d_rows_schools_winds[f"{wind}"] = 0
                d_rows_hcs_winds[f"{wind}"] = 0
            else:
                # Filter admin view for this admin ID and sum values
                admin_view = wind_admin_views[wind][wind_admin_views[wind]['tile_id'] == admin_id]
                d_rows_admins_pop_total[f"{wind}"] = int(admin_view['E_population'].sum())
                d_rows_admins_school[f"{wind}"] = int(admin_view['E_school_age_population'].sum())
                d_rows_admins_infant[f"{wind}"] = int(admin_view['E_infant_population'].sum())
                d_rows_schools_winds[f"{wind}"] = int(admin_view['E_num_schools'].sum())
                d_rows_hcs_winds[f"{wind}"] = int(admin_view['E_num_hcs'].sum())
            
            # Calculate changes from previous forecast
            if not d_previous:
                d_rows_admins_pop_total[f"change_{wind}"] = d_rows_admins_pop_total[f"{wind}"]
                d_rows_admins_school[f"change_{wind}"] = d_rows_admins_school[f"{wind}"]
                d_rows_admins_infant[f"change_{wind}"] = d_rows_admins_infant[f"{wind}"]
            else:
                prev_rows = d_previous.get('rows_admins_pop_total', [])
                prev_school_rows = d_previous.get('rows_admins_school', [])
                prev_infant_rows = d_previous.get('rows_admins_infant', [])
                
                prev_pop = prev_rows[i].get(f"{wind}", 0) if i < len(prev_rows) else 0
                prev_school = prev_school_rows[i].get(f"{wind}", 0) if i < len(prev_school_rows) else 0
                prev_infant = prev_infant_rows[i].get(f"{wind}", 0) if i < len(prev_infant_rows) else 0
                
                d_rows_admins_pop_total[f"change_{wind}"] = d_rows_admins_pop_total[f"{wind}"] - prev_pop
                d_rows_admins_school[f"change_{wind}"] = d_rows_admins_school[f"{wind}"] - prev_school
                d_rows_admins_infant[f"change_{wind}"] = d_rows_admins_infant[f"{wind}"] - prev_infant
        
        # Calculate CCI values for this admin
        admin_cci = cci_admin_view[cci_admin_view['tile_id'] == admin_id]
        d_rows_admins_pop_total["cci"] = int(admin_cci['E_CCI_pop'].sum())
        d_rows_admins_school["cci"] = int(admin_cci['E_CCI_school_age'].sum())
        d_rows_admins_infant["cci"] = int(admin_cci['E_CCI_infants'].sum())
        
        rows_admins_pop_total.append(d_rows_admins_pop_total)
        rows_admins_school.append(d_rows_admins_school)
        rows_admins_infant.append(d_rows_admins_infant)
        rows_schools_winds.append(d_rows_schools_winds)
        rows_hcs_winds.append(d_rows_hcs_winds)
    
    return {
        'rows_admins_pop_total': rows_admins_pop_total,
        'rows_admins_school': rows_admins_school,
        'rows_admins_infant': rows_admins_infant,
        'rows_schools_winds': rows_schools_winds,
        'rows_hcs_winds': rows_hcs_winds
    }

def do_report(wind_school_views: Dict[int, pd.DataFrame], 
              wind_hc_views: Dict[int, pd.DataFrame],
              wind_tiles_views: Dict[int, pd.DataFrame],
              wind_admin_views: Dict[int, pd.DataFrame],
              cci_tiles_view: pd.DataFrame,
              cci_admin_view: pd.DataFrame,
              gdf_admin: gpd.GeoDataFrame,
              gdf_tracks: gpd.GeoDataFrame,
              country: str, storm: str, date: str) -> Dict[str, Any]:
    """
    Generate comprehensive impact report from analysis views.
    
    This function aggregates impact data across multiple wind thresholds and
    administrative levels, calculates changes from previous forecasts, and
    identifies top at-risk facilities.
    
    Args:
        wind_school_views: Dictionary mapping wind thresholds to school impact views
        wind_hc_views: Dictionary mapping wind thresholds to health center impact views
        wind_tiles_views: Dictionary mapping wind thresholds to tile impact views
        wind_admin_views: Dictionary mapping wind thresholds to admin-level impact views
        cci_tiles_view: DataFrame with Child Cyclone Index values per tile
        cci_admin_view: DataFrame with Child Cyclone Index values per admin
        gdf_admin: GeoDataFrame with admin boundaries
        gdf_tracks: GeoDataFrame with storm track data
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
    
    Returns:
        dict: Comprehensive report dictionary, or empty dict if no impact detected
    """
    # Check if there's any impact
    max_wind = _get_max_wind_threshold(wind_admin_views)
    if max_wind == 0:
        return {}
    
    # Get expected wind threshold for calculations
    expected_wind = _get_expected_wind_threshold(wind_tiles_views)
    if expected_wind is None:
        return {}
    
    # Load previous report for change calculations
    previous_date = get_previous_date(date)
    d_previous = load_json_report(country, storm, previous_date)
    
    # Initialize report dictionary
    d = {
        'country': country,
        'storm': storm,
        'forecast_date': datetime.strptime(date, "%Y%m%d%H%M%S").strftime("%B %d, %Y %H:%M UTC"),
        'storm_category': STORM_CATEGORIES[max_wind],
        'expected_landfall': get_expected_landfall(gdf_tracks, date, country),
        'next_forecast_date': get_future_date(date, NEXT_FORECAST_HOURS),
        'report_date': datetime.now().strftime("%B %d, %Y %H:%M UTC")
    }

    # Calculate expected totals
    expected_tiles = wind_tiles_views[expected_wind]
    d['expected_school_age'] = int(expected_tiles['E_school_age_population'].sum())
    d['expected_infants'] = int(expected_tiles['E_infant_population'].sum())
    d['expected_children'] = int(d['expected_school_age'] + d['expected_infants'])
    d['expected_pop'] = int(expected_tiles['E_population'].sum())
    d['expected_schools'] = int(expected_tiles['E_num_schools'].sum())
    d['expected_hcs'] = int(expected_tiles['E_num_hcs'].sum())
    d['expected_cci_pop'] = int(cci_tiles_view['E_CCI_pop'].sum())
    d['expected_cci_school'] = int(cci_tiles_view['E_CCI_school_age'].sum())
    d['expected_cci_infant'] = int(cci_tiles_view['E_CCI_infants'].sum())
    
    # Calculate children change from previous forecast
    children_change = _calculate_children_change(d['expected_children'], d_previous)
    d.update(children_change)
    
    # Calculate expected impacts by wind threshold
    for wind in STORM_CATEGORIES.keys():
        if wind not in wind_tiles_views:
            continue
        
        wind_tiles = wind_tiles_views[wind]
        d[f"expected_pop_{wind}"] = int(wind_tiles['E_population'].sum())
        d[f"expected_school_{wind}"] = int(wind_tiles['E_school_age_population'].sum())
        d[f"expected_infant_{wind}"] = int(wind_tiles['E_infant_population'].sum())
        d[f"expected_children_{wind}"] = int(d[f"expected_school_{wind}"] + d[f"expected_infant_{wind}"])
        d[f"expected_schools_{wind}"] = int(wind_tiles['E_num_schools'].sum())
        d[f"expected_hcs_{wind}"] = int(wind_tiles['E_num_hcs'].sum())
        
        # Calculate changes from previous forecast
        if not d_previous:
            d[f"change_school_{wind}"] = d[f"expected_school_{wind}"]
            d[f"change_infant_{wind}"] = d[f"expected_infant_{wind}"]
            d[f"change_children_{wind}"] = d[f"expected_children_{wind}"]
            d[f"change_schools_{wind}"] = d[f"expected_schools_{wind}"]
            d[f"change_hcs_{wind}"] = d[f"expected_hcs_{wind}"]
        else:
            d[f"change_school_{wind}"] = d[f"expected_school_{wind}"] - d_previous.get(f"expected_school_{wind}", 0)
            d[f"change_infant_{wind}"] = d[f"expected_infant_{wind}"] - d_previous.get(f"expected_infant_{wind}", 0)
            d[f"change_children_{wind}"] = d[f"expected_children_{wind}"] - d_previous.get(f"expected_children_{wind}", 0)
            d[f"change_schools_{wind}"] = d[f"expected_schools_{wind}"] - d_previous.get(f"expected_schools_{wind}", 0)
            d[f"change_hcs_{wind}"] = d[f"expected_hcs_{wind}"] - d_previous.get(f"expected_hcs_{wind}", 0)
    
    # Get top at-risk facilities
    if wind_school_views:
        expected_wind_schools = KEY_FOR_EXPECTED if KEY_FOR_EXPECTED in wind_school_views else min(wind_school_views.keys())
        top_schools = wind_school_views[expected_wind_schools].nlargest(TOP_FACILITIES_COUNT, 'probability')
        for i, (_, row) in enumerate(top_schools.iterrows(), start=1):
            d[f"school_name_{i}"] = row.get('school_name', '')
            d[f"school_edulevel_{i}"] = row.get('education_level', '')
            d[f"school_prob_{i}"] = float(row.get('probability', 0))
    
    if wind_hc_views:
        expected_wind_hcs = KEY_FOR_EXPECTED if KEY_FOR_EXPECTED in wind_hc_views else min(wind_hc_views.keys())
        top_hcs = wind_hc_views[expected_wind_hcs].nlargest(TOP_FACILITIES_COUNT, 'probability')
        for i, (_, row) in enumerate(top_hcs.iterrows(), start=1):
            d[f"hc_name_{i}"] = row.get('name', '')
            d[f"hc_type_{i}"] = row.get('healthcare', '')
            d[f"hc_prob_{i}"] = float(row.get('probability', 0))
    
    # Calculate vulnerability metrics
    vulnerability_metrics = _calculate_vulnerability_metrics(expected_tiles)
    d.update(vulnerability_metrics)
    
    # Calculate administrative-level impact rows
    admin_rows = _calculate_admin_rows(wind_admin_views, cci_admin_view, gdf_admin, d_previous)
    d.update(admin_rows)

    # Validate report structure
    missing_keys = [key for key in REPORT_TEMPLATE.keys() if key not in d]
    if missing_keys:
        logger.warning(f"Report missing keys: {', '.join(missing_keys)}")
    
    extra_keys = [key for key in d.keys() if key not in REPORT_TEMPLATE]
    if extra_keys:
        logger.debug(f"Report contains extra keys: {', '.join(extra_keys)}") 
    
    return d