#!/usr/bin/env python3
"""
Impact Analysis Module

Core geospatial engine for the Ahead of the Storm pipeline. Imported and driven by
main_pipeline.py, not intended to be run directly.

Key Features:
- Initialises country base layers: mercator tile grids and admin-level views with
  population (WorldPop), built surface (GHSL), settlement class (GHSL SMOD), wealth
  index (HDX RWI), schools (GIGA API), health centers (HealthSites.io), shelters and
  WASH infrastructure (OSM Overpass)
- Intersects hurricane wind envelopes with country infrastructure at each wind speed
  threshold (34–137 kt) to produce per-facility and per-tile impact views
- Supports custom data overrides: place a CSV in geodb/custom/ to replace any API or
  raster source for a specific country (never overwritten by the pipeline)
- Patches specific columns in existing mercator parquets without full re-initialisation
  (patch_country_layer)
- Storage-backend agnostic: LOCAL filesystem, Azure Blob (ADLS), or Snowflake internal
  stage (controlled by DATA_PIPELINE_DB env var)

Entry points called from main_pipeline.py:
    create_mercator_country_layer()       -- --type initialize
    create_admin_country_layer()          -- --type initialize
    create_views_from_envelopes_in_country()  -- --type update
    patch_country_layer()                 -- --type patch

Module structure (sections in order):
    CUSTOM DATA HELPERS             -- _custom_file_path, _load_custom_points_csv,
                                       _load_custom_tiles_csv
    DATA FETCHING AND CACHING       -- fetch_schools, fetch_health_centers,
                                       fetch_shelters, fetch_wash
    GEOGRAPHIC UTILITIES            -- get_country_boundaries, is_envelope_in_zone
    BASE LAYER INITIALIZATION       -- create_mercator_country_layer, save_mercator_view,
                                       admins_overlay, add_admin_ids,
                                       write_country_boundary, patch_country_layer,
                                       save_mercator_and_admin_views
    STORM METADATA                  -- save/load_json_storms, load_mercator_view
    PER-STORM IMPACT VIEW GENERATION -- create_school/hc/shelter/wash/mercator/admin/
                                        tracks_view_from_envelopes
    FACILITY VIEW PERSISTENCE       -- save/load/exist per facility type
                                       (schools, HCs, shelters, WASH)
    ADMIN COUNTRY LAYER             -- create_admin_country_layer
                                       (logically part of base layer init)
    TILE & STORM VIEW PERSISTENCE   -- save/load for tile views, CCI views,
                                       admin views, track views
    CCI CALCULATION                 -- calculate_ccis
    MAIN IMPACT ANALYSIS            -- create_views_from_envelopes_in_country
    SNOWFLAKE DATA LOADING          -- load_envelopes_from_snowflake
"""

import io
import json
import os
import tempfile
import contextlib
import sys
import logging
import warnings
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
from gigaspatial.processing.tif_processor import TifProcessor
from gigaspatial.core.io.local_data_store import LocalDataStore

# Import centralized data store utility
from data_store_utils import get_data_store
from country_utils import update_country_initialized

# Import hazard source data retrieval functions. get_envelopes/get_gust_envelopes/
# get_tracks dispatch to Snowflake or LOCAL/BLOB based on config.HAZARD_DATA_SOURCE
# (see snowflake_utils.py's "HAZARD_DATA_SOURCE DISPATCH" section); get_snowflake_connection
# is unrelated (used only for the country pre-filter, always Snowflake).
from snowflake_utils import (
    get_envelopes,
    get_gust_envelopes,
    convert_envelopes_to_geodataframe,
    get_tracks,
    get_snowflake_connection
)

from reports import do_report, save_json_report

# =============================================================================
# CONSTANTS
# =============================================================================

# Columns stored in every mercator and admin base parquet.
# sum_cols / avg_cols define how each column is aggregated when computing E_ (expected) values during impact view generation (col * probability). 
# Counts and populations are summed; continuous indices (RWI, SMOD) are averaged to preserve their meaning.

data_cols = [
    'population',           # Total population (WorldPop GR2, year=2025, 1km)
    'school_age_population',# School-age population 5–14 years (WorldPop GR2/2025, 100m, age_structures)
    'infant_population',    # Infant population 0–4 years (WorldPop GR2/2025, 100m, age_structures)
    'adolescent_population', # Adolescent population 15–19 years (WorldPop GR2/2025, 100m, age_structures)
    'built_surface_m2',     # Built surface area in m² (GHSL GHS-BUILT-S)
    'smod_class',           # Settlement model L2 class 10–30 (GHSL GHS-SMOD)
    'smod_class_l1',        # Derived L1 class: 1=rural, 2=suburban, 3=urban
    'rwi',                  # Relative Wealth Index, ~-2.5 to +2.5 (Meta/HDX)
    'num_schools',          # Number of schools in tile (GIGA API)
    'num_hcs',              # Number of health centers in tile (HealthSites.io)
    'num_shelters',         # Number of emergency shelters in tile (OSM social_facility=shelter)
    'num_wash',             # Number of WASH facilities in tile (OSM amenity/man_made)
]

# WorldPop age-structure columns within data_cols that are a hard, always-required
# init-time dependency. A country missing one of these needs `--type patch --columns <col>`, 
# not a silent NaN/0 default.
POPULATION_COLS = ['population', 'school_age_population', 'infant_population', 'adolescent_population']

# Columns multiplied by probability to produce E_ (expected impact) values per tile.
# Used in create_mercator_view_from_envelopes and create_admin_view_from_envelopes_new.
sum_cols = [
    'E_population',
    'E_school_age_population',
    'E_infant_population',
    'E_adolescent_population',
    'E_built_surface_m2',
    'E_num_schools',
    'E_num_hcs',
    'E_num_shelters',
    'E_num_wash',
]

# Continuous index columns: averaged (not summed) when aggregating across tiles.
avg_cols = [
    'E_smod_class',     # Expected SMOD L2 class (smod_class * probability)
    'E_smod_class_l1',  # Expected SMOD L1 class
    'E_rwi',            # Expected RWI (rwi * probability)
    'probability',      # Mean ensemble probability across tiles
]

# Admin-level aggregation: same logic, but operating on already-aggregated tile values.
sum_cols_admin = [
    'population',
    'school_age_population',
    'infant_population',
    'adolescent_population',
    'built_surface_m2',
    'num_schools',
    'num_hcs',
    'num_shelters',
    'num_wash',
]

# Columns where all-NaN means "no data" and should remain NaN (not sum to 0).
_OPTIONAL_SUM_COLS = {
    'num_shelters', 'num_wash', 'E_num_shelters', 'E_num_wash',
    'num_schools',  'num_hcs',  'E_num_schools',  'E_num_hcs',
    'built_surface_m2', 'E_built_surface_m2',
}

def _optional_sum(s):
    """Sum that preserves NaN when all values are NaN (no-data semantics)."""
    return s.sum() if s.notna().any() else float('nan')

avg_cols_admin = [
    'smod_class',   # Mean SMOD L2 class across tiles in admin unit
    'smod_class_l1', # Mean SMOD L1 class
    'rwi',          # Mean RWI across tiles in admin unit
    # NOTE: moderate_poverty_prob / severe_poverty_prob are NOT here:
    # they use population-weighted mean via _poverty_weighted_mean().
]

# Poverty columns that require population-weighted mean aggregation.
_POVERTY_COLS = ('moderate_poverty_prob', 'severe_poverty_prob')


def _poverty_weighted_mean(src: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """
    Population-weighted mean of poverty rate columns, grouped by group_col.

    Returns a DataFrame indexed by group_col values with one column per poverty
    column present in src. When all tiles in a group have NaN poverty rates the 
    group gets NaN (no data), not zero.

    Using population weighting means that when tiles are aggregated back to
    admin0 (national level) the result exactly recovers the calibrated national
    target.
    """
    if 'population' not in src.columns:
        return pd.DataFrame()
    result = {}
    for col in _POVERTY_COLS:
        if col not in src.columns:
            continue
        valid = src[[group_col, col, 'population']].dropna(subset=[col, 'population'])
        if valid.empty:
            continue
        g = valid.groupby(group_col)
        weighted_sum = (valid[col] * valid['population']).groupby(valid[group_col]).sum()
        pop_sum = g['population'].sum()
        result[col] = weighted_sum / pop_sum
    if not result:
        return pd.DataFrame()
    return pd.DataFrame(result)

# CCI columns written to tile and admin CCI views.
sum_cols_cci = [
    'CCI_children',    'E_CCI_children',
    'CCI_school_age',  'E_CCI_school_age',
    'CCI_infants',     'E_CCI_infants',
    'CCI_adolescents',    'E_CCI_adolescents',
    'CCI_pop',         'E_CCI_pop',
]

# Vulnerability columns written to tile and admin vulnerability views.
# These represent expected people/children in need under wind-dependent poverty weighting.
sum_cols_vulnerability = [
    'E_infant_in_need',
    'E_school_age_in_need',
    'E_adolescent_in_need',
    'E_children_in_need',
    'E_people_in_need',
]

# Wind speed thresholds (kt) for wind-dependent vulnerability rate transitions.
# Below VULNERABILITY_WIND_SEVERE_THRESHOLD: apply severe poverty rate.
# Between thresholds: linearly interpolate moderate_poverty_prob → 1.0.
# At/above VULNERABILITY_WIND_FULL_THRESHOLD: apply 1.0 (catastrophic, all need help).
VULNERABILITY_WIND_SEVERE_THRESHOLD = 50   # kt
VULNERABILITY_WIND_FULL_THRESHOLD   = 96   # kt (Category 3)

# Configuration constants
BUFFER_DISTANCE_METERS = 150  # Buffer distance for schools and health centers (meters)
WORLDPOP_RESOLUTION_HIGH = 1000  # High resolution for WorldPop data (meters)
WORLDPOP_RESOLUTION_LOW = 100  # Low resolution for WorldPop data (meters)
SCHOOL_AGE_MIN = 5   # GR2 min_age: picks _05_ (5–9y) and _10_ (10–14y) bands
SCHOOL_AGE_MAX = 10  # GR2 max_age: picks _05_ (5–9y) and _10_ (10–14y) bands
INFANT_AGE_MIN = 0   # GR2 min_age: picks _00_ (0–12mo) and _01_ (1–4y) bands
INFANT_AGE_MAX = 1   # GR2 max_age: picks _00_ (0–12mo) and _01_ (1–4y) bands
ADOLESCENT_AGE_MIN = 15  # GR2 min_age: picks _15_ (15–19y) band
ADOLESCENT_AGE_MAX = 15  # GR2 max_age: picks _15_ (15–19y) band (1 file only)
CCI_WEIGHT_MULTIPLIER = 1e-6  # Multiplier for CCI weight calculation (wind_speed^2 * 1e-6)
# Full ECMWF ensemble size: 50 perturbed members + 1 control (member 51 = GRIB number 0).
# Hard-coded so probability denominators stay correct even when individual members fail
# to produce wind polygons or are missing from GRIB files.
FULL_ENSEMBLE_SIZE = 51


#==============================================================================
# HEALTH FACILITY TYPES
#==============================================================================
# Health facility types to include in impact analysis, keyed by OSM/HealthSites column.
#
# Format mirrors SHELTER_LOCATION_TYPES and WASH_LOCATION_TYPES: a dict of
# {column_name: [values]} so additional tag keys (e.g. 'healthcare', 'emergency')
# can be added alongside 'amenity' without changing the filter logic.
#
# The HealthSites.io API (https://healthsites.io/api/v3/) returns an `amenity` column
# whose documented values are: clinic, doctors, hospital, dentist, pharmacy.
# Filtering uses the `amenity` column (not `healthcare`).
#
# Included:
#   amenity=hospital  – Inpatient care, typically with emergency department.
#                       Most critical for mass-casualty and evacuation response.
#   amenity=clinic    – Outpatient clinics and clinics with beds.
#                       Primary care backbone in most low/middle-income countries.
#   amenity=doctors   – Individual practitioner offices (OSM amenity=doctors, plural).
#                       Lower capacity than clinics but relevant for community-level care.
#
# Excluded intentionally:
#   amenity=dentist   – Specialist care, not relevant to cyclone response.
#   amenity=pharmacy  – Supply chain node, not direct emergency care delivery.
#
# To change the included types, update this dict. The raw (unfiltered) API response
# is stored in the location cache so changes take effect on the next update run.

HC_FACILITY_TYPES = {
    'amenity': ['hospital', 'clinic', 'doctors'],
}

#==============================================================================
# OSM LOCATION TYPES
#==============================================================================
# OSM location types for emergency shelters
#
# Queried via OSMLocationFetcher (Overpass API). Only social_facility=shelter is included
# because it specifically denotes dedicated emergency/disaster shelter facilities in OSM.
#
# Included:
#   social_facility=shelter – Dedicated emergency or disaster relief shelters
#                             (refugee shelters, evacuation centres, disaster relief sites).
#                             Tagged on amenity=social_facility nodes/ways in OSM.
#
# Excluded intentionally:
#   amenity=shelter         – Too broad: includes bus stop shelters, hiking shelters,
#                             picnic shelters, market covers. Not emergency-relevant.
#   emergency=assembly_point / - Only evacuation_point, e.g. in case of a fire etc.
#

SHELTER_LOCATION_TYPES = {
    'social_facility': ['shelter'],
}

#==============================================================================
# WASH LOCATION TYPES
#==============================================================================
# OSM location types for humanitarian WASH infrastructure
#
# Queried via OSMLocationFetcher (Overpass API). Covers the full managed water and
# sanitation infrastructure chain relevant to cyclone response.
# Reference: https://wiki.openstreetmap.org/wiki/Humanitarian_OSM_Tags/WASH
# (HOT wiki documents amenity=water_point and amenity=toilets as primary tags;
# the man_made=* tags below extend coverage to upstream infrastructure.)
#
# Included:
#   amenity=drinking_water         – Drinking water taps/fountains (public access points)
#   amenity=water_point            – Water collection points (often in informal settlements)
#   amenity=toilets                – Public toilets / latrines (core HOT WASH tag)
#   amenity=shower                 – Public shower facilities (hygiene)
#   man_made=water_well            – Water wells (managed, hand-pump or motorised)
#   man_made=water_tap             – Piped water tap stands
#   man_made=water_works           – Water treatment or pumping plants
#   man_made=pumping_station       – Water supply pumping stations
#   man_made=wastewater_treatment_plant – Sanitation infrastructure
#
# Excluded intentionally:
#   natural=spring / natural=water – Natural sources, not managed infrastructure.
#                                    Reliability after a cyclone is unpredictable.
#   man_made=storage_tank          – Too broad: includes fuel, agricultural, industrial tanks.
#

WASH_LOCATION_TYPES = {
    'amenity': ['drinking_water', 'water_point', 'toilets', 'shower'],
    'man_made': ['water_well', 'water_tap', 'water_works', 'pumping_station', 'wastewater_treatment_plant'],
}

#==============================================================================
# SCHOOL EDUCATION LEVELS
#==============================================================================
# School education levels available from the GIGA School Location API
# (https://uni-ooi-giga-maps-service.azurewebsites.net)
#
#   'Pre-Primary'   – Pre-school / kindergarten
#   'Primary'       – Primary school
#   'Secondary'     – Secondary / high school
#   'Unknown'       – Education level not recorded
# Other values may exist in the API; all are retained regardless.


#==============================================================================
# GHSL SMOD L2→L1 RECLASSIFICATION MAPPING
#==============================================================================
# L2 raw values (10-30) → L1 simplified 3-class (1=rural, 2=suburban, 3=urban)
SMOD_L2_TO_L1 = {
    10: 1,  # Water → rural
    11: 1,  # Very low density rural
    12: 1,  # Low density rural
    13: 1,  # Rural cluster
    21: 2,  # Suburban/peri-urban
    22: 2,  # Semi-dense urban cluster
    23: 2,  # Dense urban cluster
    30: 3,  # Urban centre
}

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

# Path for custom data overrides (schools, health centers, pre-aggregated tile values).
# Same relative path is used across all backends (LOCAL, BLOB, SNOWFLAKE stage).
CUSTOM_DATA_DIR = os.path.join(ROOT_DATA_DIR, 'custom')

# Path for vulnerability (people/children in need) probability data.
# Files placed here by vulnerability/fetch_vulnerability_probs.py (or uploaded to stage).
VULNERABILITY_DATA_DIR = os.path.join(ROOT_DATA_DIR, 'vulnerability')

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
# CUSTOM DATA HELPERS
# =============================================================================
def _custom_file_path(country, kind, zoom=None):
    """
    Return the data store path for a custom data override file.

    Args:
        country: ISO3 country code
        kind: File type identifier: 'schools', 'health_centers', 'shelters', 'wash',
              'population', 'built_surface', 'smod', or 'rwi'
        zoom: Zoom level (required for tile-level files; None for point files)

    Returns:
        str: Path relative to the data store root (e.g. 'geodb/custom/PNG_schools.csv')
    """
    if zoom is not None:
        filename = f"{country}_{kind}_z{zoom}.csv"
    else:
        filename = f"{country}_{kind}.csv"
    return os.path.join(CUSTOM_DATA_DIR, filename)


def _load_custom_points_csv(country, kind):
    """
    Load a custom point data CSV (schools or health_centers) from the data store.

    The file is read from the data store at geodb/custom/<COUNTRY>_<kind>.csv, and
    must contain the required columns for `kind` (see the `required` mapping below;
    at minimum 'latitude'/'longitude'). If the file is absent, None is returned and
    the caller falls back to API fetching. If the file exists but is invalid (missing
    required columns or unreadable), a ValueError is raised. The pipeline does not
    silently fall back to the API when a custom file is present.

    The ID column is optional. Accepted names: 'id', 'school_id_giga' (schools),
    'osm_id' (health_centers, shelters, wash). If none are present, sequential IDs
    are auto-generated (e.g. 'schools_0', 'schools_1', ...).

    Args:
        country: ISO3 country code
        kind: 'schools', 'health_centers', 'shelters', or 'wash'

    Returns:
        gpd.GeoDataFrame or None: GeoDataFrame with Point geometry (EPSG:4326), or None
                                   if the custom file does not exist.
    """
    # Internal ID column name expected by GeometryBasedZonalViewGenerator calls
    id_col = 'school_id_giga' if kind == 'schools' else 'osm_id'

    path = _custom_file_path(country, kind)
    if not data_store.file_exists(path):
        return None
    try:
        raw = data_store.read_file(path)
        df = pd.read_csv(io.BytesIO(raw))
        # Required columns (ID is handled separately below)
        required = {
            'schools':        ['latitude', 'longitude'],
            'health_centers': ['latitude', 'longitude', 'amenity'],
            'shelters':       ['latitude', 'longitude'],
            'wash':           ['latitude', 'longitude', 'wash_type'],
        }[kind]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(
                f"{country}: Custom {kind} CSV at '{path}' is missing required columns: {missing}. "
                f"Fix the file or remove it to fall back to API fetching."
            )
        # Normalize ID column: accept 'id' as alias for the internal name,
        # or auto-generate sequential IDs if no ID column is present at all.
        if id_col not in df.columns:
            if 'id' in df.columns:
                df = df.rename(columns={'id': id_col})
            else:
                df[id_col] = [f"{kind}_{i}" for i in range(len(df))]
                logger.info(f"{country}: No ID column in custom {kind} CSV, auto-generated sequential IDs")
        # Warn early about nulls/duplicates: _ensure_unique_zone_ids will fix
        # them at view-generation time, but surfacing the issue here helps debug
        # the source CSV.
        null_count = df[id_col].isna().sum()
        dup_count = df[id_col].duplicated(keep='first').sum()
        if null_count:
            logger.warning(f"{country}: Custom {kind} CSV has {null_count} null {id_col} values, fallback IDs will be assigned at impact time")
        if dup_count:
            logger.warning(f"{country}: Custom {kind} CSV has {dup_count} duplicate {id_col} values, fallback IDs will be assigned at impact time")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df['longitude'], df['latitude']),
            crs='EPSG:4326'
        )
        # Canonical 'source' column so downstream MAT tables/the dashboard can
        # show real per-country provenance instead of always assuming the
        # standard API/OSM source. Some custom CSVs already carry their own
        # source-like column under a different name (e.g. DOM/HND's SIASAR
        # files use 'school_data_source' or 'source'), normalize whichever is
        # present into 'source'; if neither exists, fall back to a generic
        # marker rather than leaving it blank.
        if 'source' not in gdf.columns:
            alt_source_cols = [c for c in gdf.columns if c.endswith('_data_source')]
            if alt_source_cols:
                gdf['source'] = gdf[alt_source_cols[0]]
            else:
                gdf['source'] = f"Custom data ({country})"
        logger.info(f"{country}: Loaded {len(gdf)} custom {kind} from '{path}' (custom data, API skipped, --rewrite has no effect)")
        return gdf
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(
            f"{country}: Failed to read custom {kind} CSV at '{path}': {e}. "
            f"Fix the file or remove it to fall back to API fetching."
        ) from e


def _load_custom_tiles_csv(country, kind, zoom):
    """
    Load a custom tile-level CSV from the data store and return it as a DataFrame.

    The tile_id column must contain mercator quadkeys at the specified zoom level
    matching the country's base mercator parquet. Empty cells are interpreted as NaN.

    Supported kinds and their columns:
        population:    tile_id, population, school_age_population, infant_population, adolescent_population
        built_surface: tile_id, built_surface_m2
        smod:          tile_id, smod_class
        rwi:           tile_id, rwi

    Args:
        country: ISO3 country code
        kind: 'population', 'built_surface', 'smod', or 'rwi'
        zoom: Zoom level (must match country's configured zoom)

    Returns:
        pd.DataFrame or None: DataFrame with tile_id index and value columns,
                              or None if the file does not exist. Raises ValueError
                              if the file exists but is invalid.
    """
    path = _custom_file_path(country, kind, zoom)
    if not data_store.file_exists(path):
        return None
    try:
        raw = data_store.read_file(path)
        df = pd.read_csv(io.BytesIO(raw), dtype={'tile_id': str})
        if 'tile_id' not in df.columns:
            raise ValueError(
                f"{country}: Custom {kind} CSV at '{path}' is missing required 'tile_id' column. "
                f"Fix the file or remove it to fall back to raster processing."
            )
        logger.info(f"{country}: Loaded custom {kind} (zoom={zoom}) from '{path}' ({len(df)} tiles, custom data, raster processing skipped)")
        return df.set_index('tile_id')
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(
            f"{country}: Failed to read custom {kind} CSV at '{path}': {e}. "
            f"Fix the file or remove it to fall back to raster processing."
        ) from e


def _vulnerability_file_path(country, zoom):
    """Return the data store path for a vulnerability probability CSV."""
    return os.path.join(VULNERABILITY_DATA_DIR, f"{country}_vulnerability_z{zoom}.csv")


def _load_vulnerability_tiles_csv(country, zoom):
    """
    Load pre-computed vulnerability probability data from geodb/vulnerability/.

    Files are generated by vulnerability/fetch_vulnerability_probs.py and contain
    per-tile moderate and severe poverty probability estimates derived from RWI via
    PCHIP/Akima curve fitting calibrated to UNICEF child poverty rates.

    Args:
        country: ISO3 country code
        zoom: Zoom level (must match country's configured zoom)

    Returns:
        pd.DataFrame with tile_id index and columns moderate_poverty_prob /
        severe_poverty_prob, or None if the file does not exist. Raises ValueError
        if the file exists but is missing required columns.
    """
    path = _vulnerability_file_path(country, zoom)
    if not data_store.file_exists(path):
        return None
    try:
        raw = data_store.read_file(path)
        df = pd.read_csv(io.BytesIO(raw), dtype={'tile_id': str})
        required = ['tile_id', 'moderate_poverty_prob', 'severe_poverty_prob']
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(
                f"{country}: Vulnerability CSV at '{path}' is missing required columns: {missing}. "
                f"Re-run vulnerability/fetch_vulnerability_probs.py to regenerate the file."
            )
        logger.info(f"{country}: Loaded vulnerability probability data from '{path}' ({len(df)} tiles)")
        return df.set_index('tile_id')
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(
            f"{country}: Failed to read vulnerability CSV at '{path}': {e}."
        ) from e


# =============================================================================
# DATA FETCHING AND CACHING FUNCTIONS
# =============================================================================
def fetch_health_centers(country, rewrite=0):
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
    try:
        gdf_hcs = HealthSitesFetcher(country=country).fetch_facilities(output_format='geojson')
        if gdf_hcs.empty or 'geometry' not in gdf_hcs.columns:
            logger.warning(f"{country}: HealthSites API returned no data")
            return gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
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
        return gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')


def fetch_schools(country, rewrite=0):
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
    try:
        result = GigaSchoolLocationFetcher(country).fetch_locations(process_geospatial=True)
        if not isinstance(result, gpd.GeoDataFrame):
            if isinstance(result, pd.DataFrame) and not result.empty and 'geometry' in result.columns:
                result = gpd.GeoDataFrame(result, geometry='geometry', crs='EPSG:4326')
            else:
                logger.warning(f"{country}: GIGA API returned no usable school data")
                return gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
        gdf_schools = result
        if 'giga_id_school' in gdf_schools.columns:
            gdf_schools = gdf_schools.rename(columns={'giga_id_school': 'school_id_giga'})
        if gdf_schools.crs is None:
            gdf_schools.set_crs('EPSG:4326', inplace=True)
        gdf_schools['source'] = 'UNICEF Giga school-location API'
        if gdf_schools.empty:
            # Never overwrite a good cache with an empty result (e.g. a transient
            # rate-limit or API hiccup returning a valid-but-empty GeoDataFrame
            # rather than raising): matches fetch_health_centers/fetch_shelters/
            # fetch_wash, which all return early on empty BEFORE ever calling
            # their own save_*_locations(), leaving their cache untouched on
            # failure. This preserves patch_country_layer()'s fallback-to-cache
            # safety net for an empty result: the cache it falls back to must
            # not itself have just been overwritten with that same empty result.
            logger.warning(f"{country}: GIGA API returned no schools; cache left untouched")
            return gdf_schools
        save_school_locations(gdf_schools, country)
        return gdf_schools
    except Exception as e:
        logger.error(f"{country}: Error fetching schools from GIGA API: {e}")
        return gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')


def fetch_shelters(country, rewrite=0):
    """
    Fetch emergency shelter locations for a country using custom data, cache, or OSM.

    Priority order:
        1. Custom file at geodb/custom/<COUNTRY>_shelters.csv: always used if present.
        2. Location cache (shelter_views/<COUNTRY>_shelters.parquet): if rewrite=0.
        3. OSM via Overpass API using SHELTER_LOCATION_TYPES (social_facility=shelter).

    Args:
        country: ISO3 country code
        rewrite: If 1, re-fetch from OSM and overwrite cache (ignored if custom file exists)

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
    try:
        from gigaspatial.handlers.osm import OSMLocationFetcher
        df = OSMLocationFetcher(country=country, location_types=SHELTER_LOCATION_TYPES).fetch_locations()
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
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')


def fetch_wash(country, rewrite=0):
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
    try:
        from gigaspatial.handlers.osm import OSMLocationFetcher
        df = OSMLocationFetcher(country=country, location_types=WASH_LOCATION_TYPES).fetch_locations()
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
        return gpd.GeoDataFrame(columns=['geometry', 'osm_id'], crs='EPSG:4326')


# =============================================================================
# GEOGRAPHIC UTILITIES
# =============================================================================
def get_country_boundaries(countries):
    """
    Retrieve country boundary geometries for a list of countries.

    A single country's fetch failure is isolated and does not abort the others
    this is the Python fallback path (used only when the SQL pre-filter itself
    fails), and one country's transient GeoRepo hiccup must not take down storm
    processing for every other unrelated, healthy country in the same request,
    matching the per-country isolation pattern used everywhere else in this file
    (e.g. the per-country create_views_from_envelopes_in_country loop).

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


def is_envelope_in_zone(zone_geom, df_envelopes, geometry_column='geometry'):
    """
    Check if any hurricane envelope intersects with a given zone geometry.

    Used in the Python fallback path of run_complete_impact_analysis when the SQL
    pre-filter (ST_DWITHIN on COUNTRY_BOUNDARY) is unavailable. The zone geometry
    is typically a 1,500 km buffer around a country boundary.

    Args:
        zone_geom: Shapely geometry representing the zone to check against
        df_envelopes: DataFrame or GeoDataFrame containing hurricane envelope geometries
        geometry_column: Name of the geometry column (default: 'geometry')

    Returns:
        bool: True if any envelope intersects with the zone, False otherwise
    """
    if df_envelopes.empty:
        return False

    if geometry_column != 'geometry':
        df_envelopes = df_envelopes.rename(columns={geometry_column: 'geometry'})

    if isinstance(df_envelopes, pd.DataFrame):
        gdf_envelopes = convert_to_geodataframe(df_envelopes.dropna(subset=['geometry']))
    else:
        gdf_envelopes = df_envelopes.copy()

    return bool(gdf_envelopes.intersects(zone_geom).any())


# =============================================================================
# BASE LAYER INITIALIZATION
# Functions for building and persisting the base mercator and admin parquets
# for a country (--type initialize / --type patch). These are written once and
# reused across all storm update runs.
# =============================================================================
def create_mercator_country_layer(country, zoom_level=14, rewrite=0):
    """
    Create mercator tile layer with demographic and infrastructure data for a country.

    Called during `--type initialize` (and as fallback during `--type update` if the base
    mercator parquet is missing). Produces one row per tile with all data columns.

    Data requirements: **hard failures** (init aborts if unavailable):
        - Total population (WorldPop 1km)
        - School-age population (WorldPop 100m, age_structures)
        - Infant population (WorldPop 100m, age_structures)
        - Under-18 population (WorldPop 100m, age_structures)

    Data requirements: **optional** (NaN/0 if unavailable; backfill with --type patch):
        - GHSL built surface (built_surface_m2)
        - SMOD settlement class L2 (smod_class) and derived L1 (smod_class_l1)
        - Relative Wealth Index (rwi)
        - Schools (num_schools): NaN if API fails or no data
        - Health centers (num_hcs): NaN if API fails or no data
        - Emergency shelters (num_shelters): NaN if OSM returns nothing
        - WASH facilities (num_wash): NaN if OSM returns nothing

    Args:
        country: ISO3 country code
        zoom_level: Zoom level for mercator tiles (default: 14)
        rewrite: If 1, re-fetch school, HC, shelter, and WASH location caches from API/OSM;
                 if 0, use cached parquets if available

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with mercator tiles and all demographic/infrastructure
                         columns. 'zone_id' renamed to 'tile_id'.
    """
    # Fetch facility locations using helper functions with caching
    gdf_schools = fetch_schools(country, rewrite)
    gdf_hcs = fetch_health_centers(country, rewrite)
    gdf_shelters = fetch_shelters(country, rewrite)
    gdf_wash = fetch_wash(country, rewrite)

    tiles_viewer = MercatorViewGenerator(source=country, zoom_level=zoom_level, data_store=data_store)

    # ------------------------------------------------------------------
    # Population (hard requirement, raises if neither custom nor raster)
    # ------------------------------------------------------------------
    custom_pop = _load_custom_tiles_csv(country, 'population', zoom_level)
    if custom_pop is not None:
        for col in ['school_age_population', 'infant_population', 'adolescent_population', 'population']:
            if col in custom_pop.columns:
                tiles_viewer.add_variable_to_view(custom_pop[col].to_dict(), col)
            else:
                raise ValueError(f"{country}: Custom population CSV missing required column '{col}'")
    else:
        import time as _time
        _wp_attempts = 3
        for _wp_attempt in range(_wp_attempts):
            try:
                # Map school-age population (5–14y): hard requirement, raises on failure
                # GR2: uses individual 5-year age bands (_05_ = 5–9y, _10_ = 10–14y), sex='T' for combined total
                tiles_viewer.map_wp_pop(
                    country=country,
                    resolution=WORLDPOP_RESOLUTION_LOW,
                    output_column="school_age_population",
                    school_age=False,
                    project="age_structures",
                    release="GR2",
                    constrained=True,
                    un_adjusted=False,
                    min_age=SCHOOL_AGE_MIN,
                    max_age=SCHOOL_AGE_MAX,
                    sex='T',
                )
                # Map infant population (0–4y): hard requirement, raises on failure
                # GR2: uses individual 5-year age bands (_00_ = 0–12mo, _01_ = 1–4y), sex='T' for combined total
                tiles_viewer.map_wp_pop(
                    country=country,
                    resolution=WORLDPOP_RESOLUTION_LOW,
                    output_column="infant_population",
                    predicate='centroid_within',
                    school_age=False,
                    project="age_structures",
                    release="GR2",
                    constrained=True,
                    un_adjusted=False,
                    min_age=INFANT_AGE_MIN,
                    max_age=INFANT_AGE_MAX,
                    sex='T',
                )
                # Map adolescent population (15–19y): hard requirement, raises on failure
                # GR2: picks _15_ band only (15–19y), sex='T' for combined total (1 file)
                tiles_viewer.map_wp_pop(
                    country=country,
                    resolution=WORLDPOP_RESOLUTION_LOW,
                    output_column="adolescent_population",
                    school_age=False,
                    project="age_structures",
                    release="GR2",
                    constrained=True,
                    un_adjusted=False,
                    min_age=ADOLESCENT_AGE_MIN,
                    max_age=ADOLESCENT_AGE_MAX,
                    sex='T',
                )
                # Map total population: hard requirement, raises on failure
                tiles_viewer.map_wp_pop(country=country, resolution=100)
                break
            except RuntimeError as _e:
                if _wp_attempt < _wp_attempts - 1:
                    logger.warning(f"{country}: WorldPop download incomplete (attempt {_wp_attempt + 1}/{_wp_attempts}), retrying in 5s: {_e}")
                    _time.sleep(5)
                else:
                    raise

    # ------------------------------------------------------------------
    # GHSL built surface: optional, NaN fallback, custom override supported
    # ------------------------------------------------------------------
    custom_built = _load_custom_tiles_csv(country, 'built_surface', zoom_level)
    if custom_built is not None and 'built_surface_m2' in custom_built.columns:
        tiles_viewer.add_variable_to_view(custom_built['built_surface_m2'].to_dict(), 'built_surface_m2')
    else:
        try:
            tiles_viewer.map_built_s()
        except Exception as e:
            logger.warning(f"{country}: GHSL built surface unavailable, setting to NaN: {e}")
            tiles_viewer.add_variable_to_view(
                {k: np.nan for k in tiles_viewer.view.index.unique()}, 'built_surface_m2'
            )

    # ------------------------------------------------------------------
    # SMOD settlement class: optional, NaN fallback, custom override supported
    # ------------------------------------------------------------------
    custom_smod = _load_custom_tiles_csv(country, 'smod', zoom_level)
    if custom_smod is not None and 'smod_class' in custom_smod.columns:
        tiles_viewer.add_variable_to_view(custom_smod['smod_class'].to_dict(), 'smod_class')
    else:
        try:
            tiles_viewer.map_smod()
        except Exception as e:
            logger.warning(f"{country}: GHSL SMOD unavailable, setting to NaN: {e}")
            tiles_viewer.add_variable_to_view(
                {k: np.nan for k in tiles_viewer.view.index.unique()}, 'smod_class'
            )

    # Derive smod_class_l1 from smod_class (always derived, never loaded from custom)
    try:
        smod_l2 = tiles_viewer.view['smod_class']
        smod_l1 = smod_l2.map(SMOD_L2_TO_L1)
        tiles_viewer.add_variable_to_view(smod_l1.to_dict(), 'smod_class_l1')
    except Exception as e:
        logger.warning(f"{country}: Could not derive smod_class_l1: {e}")
        tiles_viewer.add_variable_to_view(
            {k: np.nan for k in tiles_viewer.view.index.unique()}, 'smod_class_l1'
        )

    # Schools, health centers, shelters, WASH
    # If the fetch returned empty (API failure, rate limit, etc.) store NaN so the
    # tile parquet records "data unavailable" rather than silently writing 0.
    # Use --type patch --columns <col> to backfill once data is available.
    _nan_tiles = {k: np.nan for k in tiles_viewer.view.index.unique()}

    if gdf_schools.empty:
        logger.warning(f"{country}: No school data: num_schools set to NaN. Backfill with --type patch --columns schools")
        tiles_viewer.add_variable_to_view(_nan_tiles, "num_schools")
    else:
        tiles_viewer.add_variable_to_view(tiles_viewer.map_points(points=gdf_schools), "num_schools")

    if gdf_hcs.empty:
        logger.warning(f"{country}: No health center data: num_hcs set to NaN. Backfill with --type patch --columns hcs")
        tiles_viewer.add_variable_to_view(_nan_tiles, "num_hcs")
    else:
        tiles_viewer.add_variable_to_view(tiles_viewer.map_points(points=gdf_hcs), "num_hcs")

    if gdf_shelters.empty:
        logger.warning(f"{country}: No shelter data: num_shelters set to NaN. Backfill with --type patch --columns shelters")
        tiles_viewer.add_variable_to_view(_nan_tiles, "num_shelters")
    else:
        tiles_viewer.add_variable_to_view(tiles_viewer.map_points(points=gdf_shelters), "num_shelters")

    if gdf_wash.empty:
        logger.warning(f"{country}: No WASH data: num_wash set to NaN. Backfill with --type patch --columns wash")
        tiles_viewer.add_variable_to_view(_nan_tiles, "num_wash")
    else:
        tiles_viewer.add_variable_to_view(tiles_viewer.map_points(points=gdf_wash), "num_wash")

    # ------------------------------------------------------------------
    # RWI: optional, NaN fallback, custom override supported
    # ------------------------------------------------------------------
    custom_rwi = _load_custom_tiles_csv(country, 'rwi', zoom_level)
    if custom_rwi is not None and 'rwi' in custom_rwi.columns:
        tiles_viewer.add_variable_to_view(custom_rwi['rwi'].to_dict(), 'rwi')
    else:
        try:
            handler = RWIHandler(data_store=data_store)
            rwi_df = handler.load_data(country, ensure_available=True)
            if rwi_df is None or (hasattr(rwi_df, 'empty') and rwi_df.empty):
                raise ValueError(f"No RWI data available for {country}")
            rwi_gdf = convert_to_geodataframe(rwi_df)
            rwi = tiles_viewer.map_points(rwi_gdf, value_columns='rwi', aggregation='mean')
        except Exception as e:
            logger.warning(f"{country}: Relative Wealth Index unavailable, setting to NaN: {e}")
            rwi = {k: np.nan for k in tiles_viewer.view.index.unique()}
        tiles_viewer.add_variable_to_view(rwi, 'rwi')

    gdf_tiles = tiles_viewer.to_geodataframe()
    gdf_tiles.rename(columns={'zone_id': 'tile_id'}, inplace=True)

    # ------------------------------------------------------------------
    # Vulnerability probability data: optional, NaN fallback
    # Generated by vulnerability/fetch_vulnerability_probs.py and patched
    # via --type patch --columns vulnerability.
    # ------------------------------------------------------------------
    vuln = _load_vulnerability_tiles_csv(country, zoom_level)
    if vuln is not None:
        gdf_tiles['moderate_poverty_prob'] = gdf_tiles['tile_id'].map(vuln['moderate_poverty_prob'])
        gdf_tiles['severe_poverty_prob']   = gdf_tiles['tile_id'].map(vuln['severe_poverty_prob'])
    else:
        logger.info(
            f"{country}: No vulnerability data found in geodb/vulnerability/. "
            f"moderate_poverty_prob and severe_poverty_prob set to NaN. "
            f"Run vulnerability/fetch_vulnerability_probs.py then "
            f"'--type patch --columns vulnerability' to populate."
        )
        gdf_tiles['moderate_poverty_prob'] = np.nan
        gdf_tiles['severe_poverty_prob']   = np.nan

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
    path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name)
    write_dataset(gdf, data_store, path)


def admins_overlay(gdf_admins1, gdf_mercator):
    """
    Assign admin boundary IDs to mercator tiles.

    Works for any admin level (1, 2, …). Three-step assignment, applied in order:

    1. Centroid-within: each tile is assigned to the admin region that contains its
       centroid (projected to equal-area CRS for accuracy). Handles most tiles.

    2. Area-overlap fallback: for tiles whose centroid falls outside every admin
       boundary (straddles a border), assign to the admin with the largest
       intersection area (equal-area CRS).

    3. Nearest-neighbour fallback: for tiles still unassigned after steps 1–2
       (ocean/far-offshore tiles), assign to the nearest admin boundary by
       centroid distance. Ensures every tile gets an admin ID.

    Note: step 3 only applies to tiles that do not intersect any admin region at all.
    Tiles that straddle admin boundaries are handled by steps 1–2.

    Callers are responsible for normalising the admin ID column to 'id' before
    passing gdf_admins1 (see add_admin_ids).

    Args:
        gdf_admins1: GeoDataFrame with admin boundaries (any level). Must have 'id' column.
        gdf_mercator: GeoDataFrame with mercator tiles. Must have 'tile_id' column.

    Returns:
        gpd.GeoDataFrame: Mercator tiles with an added 'id' column (admin boundary ID).
    """
    # Step 1: centroid-based assignment (primary)
    # Project to equal-area CRS for accurate centroid computation
    centroids = gdf_mercator[["tile_id", "geometry"]].copy()
    centroids["geometry"] = centroids.geometry.to_crs("ESRI:54009").centroid.to_crs(gdf_mercator.crs)
    centroid_join = gpd.sjoin(
        centroids,
        gdf_admins1[["id", "geometry"]],
        how="left",
        predicate="within",
    ).drop(columns=["index_right"], errors="ignore")
    # Keep first match per tile (handles rare centroid-on-boundary duplicates)
    centroid_join = centroid_join.drop_duplicates(subset="tile_id", keep="first")
    assigned = centroid_join[["tile_id", "id"]].copy()

    # Step 2: area-based fallback for tiles whose centroid is outside all admin regions
    still_unassigned = assigned[assigned["id"].isna()]["tile_id"]
    if len(still_unassigned) > 0:
        tiles_fallback = gdf_mercator[gdf_mercator["tile_id"].isin(still_unassigned)]
        intersections = gpd.overlay(tiles_fallback, gdf_admins1, how="intersection")
        if len(intersections) > 0:
            intersections["intersection_area"] = (
                intersections.geometry.to_crs("ESRI:54009").area
            )
            max_idx = intersections.groupby("tile_id")["intersection_area"].idxmax()
            fallback = intersections.loc[max_idx, ["tile_id", "id"]]
            assigned = assigned.set_index("tile_id")
            assigned.update(fallback.set_index("tile_id"))
            assigned = assigned.reset_index()

    # Step 3: nearest-neighbour fallback for tiles still unassigned (no intersection
    # with any admin, typically ocean or far-offshore tiles)
    still_unassigned = assigned[assigned["id"].isna()]["tile_id"]
    if len(still_unassigned) > 0:
        logger.debug(
            f"admins_overlay: {len(still_unassigned)} tiles unassigned after centroid "
            "and area steps, applying nearest-neighbour fallback"
        )
        # Both sides kept in ESRI:54009 (equal-area, metres) for the sjoin_nearest call
        # itself, degrees don't reflect true distance for "nearest" (same reasoning
        # assign_facilities_to_tiles() already applies for its own nearest-tile
        # fallback). Harmless to leave projected here since only tile_id/id survive
        # into `assigned` below, geometry never leaks out of this local computation.
        tiles_nn = gdf_mercator[gdf_mercator["tile_id"].isin(still_unassigned)].copy()
        tiles_nn["geometry"] = tiles_nn.geometry.to_crs("ESRI:54009").centroid
        admins_proj = gdf_admins1[["id", "geometry"]].to_crs("ESRI:54009")
        nearest = gpd.sjoin_nearest(tiles_nn[["tile_id", "geometry"]], admins_proj, how="left")
        nearest = nearest.drop_duplicates(subset="tile_id", keep="first")[["tile_id", "id"]]
        assigned = assigned.set_index("tile_id")
        assigned.update(nearest.set_index("tile_id"))
        assigned = assigned.reset_index()

    # Left-join back to preserve all original tiles
    result = gdf_mercator.merge(assigned, on="tile_id", how="left")
    return gpd.GeoDataFrame(result, geometry="geometry", crs=gdf_mercator.crs)


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

    prefix, suffix = f"{country}_admin", ".parquet"
    found = []
    for f in files:
        fname = os.path.basename(f)
        if fname.startswith(prefix) and fname.endswith(suffix):
            level_str = fname[len(prefix):-len(suffix)]
            if level_str.isdigit():
                found.append(int(level_str))
    return sorted(found)


def write_country_boundary(country: str):
    """
    Fetch admin level 0 boundary from GeoRepo and write it to
    PIPELINE_COUNTRIES.COUNTRY_BOUNDARY in Snowflake.
    Called automatically during --type initialize for each new country.
    """
    conn = None
    try:
        boundaries = AdminBoundaries.create(country_code=country, admin_level=0)
        gdf = boundaries.to_geodataframe()
        if gdf.empty or gdf.geometry.isna().all():
            logger.warning(f"{country}: GeoRepo returned no boundary, COUNTRY_BOUNDARY not updated")
            return
        # Union all rows in case GeoRepo returns multiple polygons for admin_level=0
        geom = gdf.geometry.union_all()
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
        cur.close()
        logger.info(f"{country}: COUNTRY_BOUNDARY, CENTER_LAT/LON, VIEW_ZOOM written to Snowflake")
    except Exception as e:
        logger.warning(f"{country}: Could not write COUNTRY_BOUNDARY to Snowflake: {e}")
    finally:
        if conn is not None:
            conn.close()


def patch_country_layer(country, zoom_level, columns):
    """
    Backfill specific columns in an existing base mercator parquet without full re-initialization.

    Loads the existing parquet for the country, re-fetches only the requested data sources,
    merges the new values back, and saves. After saving the mercator parquet, the admin parquet
    is also re-aggregated so baseline admin-level counts stay in sync.

    This is the preferred way to populate NaN columns (e.g. after GHSL/SMOD/RWI data becomes
    available for a country) without re-downloading population data or re-fetching schools and HCs.

    Supported columns:
        population             : re-runs WorldPop total population (or uses custom population_z<N>.csv)
        school_age_population  : re-runs WorldPop school-age population
        infant_population      : re-runs WorldPop infant population
        adolescent_population  : re-runs WorldPop adolescent population (15–19y)
        built_surface_m2       : re-runs GHSL built surface (or uses custom built_surface_z<N>.csv)
        smod_class             : re-runs GHSL SMOD (or uses custom smod_z<N>.csv); also updates smod_class_l1
        smod_class_l1          : alias for smod_class (both are always updated together)
        rwi                    : re-runs RWI (or uses custom rwi_z<N>.csv)
        schools                : re-fetches school locations and recomputes counts (updates num_schools column)
        hcs                    : re-fetches health center locations and recomputes counts (updates num_hcs column)
        shelters               : re-fetches shelter locations from OSM or custom CSV and recomputes counts (updates num_shelters column)
        wash                   : re-fetches WASH facility locations from OSM or custom CSV and recomputes counts (updates num_wash column)
        admin<N>               : creates a new base admin parquet for level N (e.g. admin2). Does not modify
                                 the mercator parquet. Fails with a clear error if GeoRepo has no level-N
                                 boundaries for the country.

    Population columns are patched individually; use this when a new WorldPop dataset is
    available without needing to re-fetch schools, HCs, or raster data for other columns.

    Custom tile CSVs (e.g. geodb/custom/<COUNTRY>_built_surface_z<ZOOM>.csv) take priority
    over raster processing for the same column, exactly as in create_mercator_country_layer().

    Args:
        country: ISO3 country code
        zoom_level: Zoom level (must match the existing parquet)
        columns: List of column names to patch (e.g. ['built_surface_m2', 'rwi'])

    Raises:
        FileNotFoundError: If no base mercator parquet exists for the country (run init first)
        ValueError: If an unsupported column is requested
    """
    import re as _re
    PATCHABLE = {
        'population', 'school_age_population', 'infant_population', 'adolescent_population',
        'built_surface_m2', 'smod_class', 'smod_class_l1', 'rwi',
        'schools', 'hcs', 'shelters', 'wash',
        'vulnerability',
    }
    # Separate admin-level columns (e.g. 'admin2', 'admin3') from regular columns
    admin_patch_levels = []
    regular_columns = []
    for col in columns:
        m = _re.fullmatch(r'admin(\d+)', col)
        if m:
            admin_patch_levels.append(int(m.group(1)))
        else:
            regular_columns.append(col)
    columns = regular_columns

    unsupported = set(columns) - PATCHABLE
    if unsupported:
        raise ValueError(
            f"{country}: Unsupported columns {unsupported}. "
            f"Patchable columns: {sorted(PATCHABLE)} or admin<N> (e.g. admin2)"
        )

    # Normalise: smod_class_l1 is always derived from smod_class
    if 'smod_class_l1' in columns and 'smod_class' not in columns:
        columns = list(columns) + ['smod_class']

    file_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', f"{country}_{zoom_level}.parquet")
    if not data_store.file_exists(file_path):
        raise FileNotFoundError(f"No base mercator parquet found for {country} at zoom {zoom_level}. "
                                f"Run --type initialize first.")

    gdf = read_dataset(file_path, data_store)
    patching_desc = columns + [f"admin{n}" for n in admin_patch_levels]
    logger.info(f"{country}: Patching {patching_desc} in existing parquet ({len(gdf)} tiles)")

    # Temporary MercatorViewGenerator seeded from existing tile geometries
    from gigaspatial.generators import MercatorViewGenerator as _MVG

    if 'built_surface_m2' in columns:
        custom = _load_custom_tiles_csv(country, 'built_surface', zoom_level)
        if custom is not None and 'built_surface_m2' in custom.columns:
            gdf['built_surface_m2'] = gdf['tile_id'].map(custom['built_surface_m2'])
            logger.info(f"{country}: Patched built_surface_m2 from custom CSV")
        else:
            viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
            try:
                viewer.map_built_s()
                v = viewer.to_geodataframe().set_index('zone_id')['built_surface_m2']
                gdf['built_surface_m2'] = gdf['tile_id'].map(v.to_dict())
                logger.info(f"{country}: Patched built_surface_m2")
            except Exception as e:
                logger.warning(f"{country}: GHSL built surface still unavailable during patch, column not updated: {e}")

    if 'smod_class' in columns:
        custom = _load_custom_tiles_csv(country, 'smod', zoom_level)
        if custom is not None and 'smod_class' in custom.columns:
            gdf['smod_class'] = gdf['tile_id'].map(custom['smod_class'])
            gdf['smod_class_l1'] = gdf['smod_class'].map(SMOD_L2_TO_L1)
            logger.info(f"{country}: Patched smod_class + smod_class_l1 from custom CSV")
        else:
            viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
            try:
                viewer.map_smod()
                v = viewer.to_geodataframe().set_index('zone_id')['smod_class']
                gdf['smod_class'] = gdf['tile_id'].map(v.to_dict())
                gdf['smod_class_l1'] = gdf['smod_class'].map(SMOD_L2_TO_L1)
                logger.info(f"{country}: Patched smod_class + smod_class_l1")
            except Exception as e:
                logger.warning(f"{country}: GHSL SMOD still unavailable during patch, column not updated: {e}")

    if 'rwi' in columns:
        custom = _load_custom_tiles_csv(country, 'rwi', zoom_level)
        if custom is not None and 'rwi' in custom.columns:
            gdf['rwi'] = gdf['tile_id'].map(custom['rwi'])
            logger.info(f"{country}: Patched rwi from custom CSV")
        else:
            try:
                handler = RWIHandler(data_store=data_store)
                rwi_df = handler.load_data(country, ensure_available=True)
                if rwi_df is None or (hasattr(rwi_df, 'empty') and rwi_df.empty):
                    raise ValueError(f"No RWI data")
                rwi_gdf = convert_to_geodataframe(rwi_df)
                viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
                rwi_vals = viewer.map_points(rwi_gdf, value_columns='rwi', aggregation='mean')
                gdf['rwi'] = gdf['tile_id'].map(rwi_vals)
                logger.info(f"{country}: Patched rwi")
            except Exception as e:
                logger.warning(f"{country}: RWI still unavailable during patch, column not updated: {e}")

    pop_cols_requested = [c for c in ['population', 'school_age_population', 'infant_population', 'adolescent_population'] if c in columns]
    if pop_cols_requested:
        custom_pop = _load_custom_tiles_csv(country, 'population', zoom_level)
        if custom_pop is not None:
            for col in pop_cols_requested:
                if col in custom_pop.columns:
                    gdf[col] = gdf['tile_id'].map(custom_pop[col])
                    logger.info(f"{country}: Patched {col} from custom CSV")
                else:
                    logger.warning(f"{country}: Custom population CSV missing column '{col}', skipping")
        else:
            viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
            if 'school_age_population' in pop_cols_requested:
                viewer.map_wp_pop(country=country, resolution=WORLDPOP_RESOLUTION_LOW,
                                  output_column='school_age_population', school_age=False,
                                  project='age_structures', release='GR2', constrained=True,
                                  un_adjusted=False, min_age=SCHOOL_AGE_MIN, max_age=SCHOOL_AGE_MAX, sex='T')
                v = viewer.to_geodataframe().set_index('zone_id')['school_age_population']
                gdf['school_age_population'] = gdf['tile_id'].map(v.to_dict())
                logger.info(f"{country}: Patched school_age_population")
            if 'infant_population' in pop_cols_requested:
                viewer.map_wp_pop(country=country, resolution=WORLDPOP_RESOLUTION_LOW,
                                  output_column='infant_population', predicate='centroid_within',
                                  school_age=False, project='age_structures', release='GR2', constrained=True,
                                  un_adjusted=False, min_age=INFANT_AGE_MIN, max_age=INFANT_AGE_MAX, sex='T')
                v = viewer.to_geodataframe().set_index('zone_id')['infant_population']
                gdf['infant_population'] = gdf['tile_id'].map(v.to_dict())
                logger.info(f"{country}: Patched infant_population")
            if 'adolescent_population' in pop_cols_requested:
                viewer.map_wp_pop(country=country, resolution=WORLDPOP_RESOLUTION_LOW,
                                  output_column='adolescent_population', school_age=False,
                                  project='age_structures', release='GR2', constrained=True,
                                  un_adjusted=False, min_age=ADOLESCENT_AGE_MIN, max_age=ADOLESCENT_AGE_MAX, sex='T')
                v = viewer.to_geodataframe().set_index('zone_id')['adolescent_population']
                gdf['adolescent_population'] = gdf['tile_id'].map(v.to_dict())
                logger.info(f"{country}: Patched adolescent_population")
            if 'population' in pop_cols_requested:
                viewer.map_wp_pop(country=country, resolution=100)
                v = viewer.to_geodataframe().set_index('zone_id')['population']
                gdf['population'] = gdf['tile_id'].map(v.to_dict())
                logger.info(f"{country}: Patched population")

    if 'schools' in columns:
        gdf_schools = fetch_schools(country, rewrite=1)
        if gdf_schools.empty and school_exist(country):
            logger.warning(f"{country}: School API re-fetch returned empty, falling back to existing cache")
            gdf_schools = load_school_locations(country)
        if gdf_schools.empty:
            logger.warning(f"{country}: No school data available: num_schools set to NaN")
            gdf['num_schools'] = float('nan')
        else:
            viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
            schools = viewer.map_points(points=gdf_schools)
            gdf['num_schools'] = gdf['tile_id'].map(schools)
            logger.info(f"{country}: Patched num_schools ({len(gdf_schools)} schools)")

    if 'hcs' in columns:
        gdf_hcs = fetch_health_centers(country, rewrite=1)
        if gdf_hcs.empty and hc_exist(country):
            logger.warning(f"{country}: HC API re-fetch returned empty, falling back to existing cache")
            gdf_hcs = load_hc_locations(country)
        if gdf_hcs.empty:
            logger.warning(f"{country}: No HC data available: num_hcs set to NaN")
            gdf['num_hcs'] = float('nan')
        else:
            viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
            hcs = viewer.map_points(points=gdf_hcs)
            gdf['num_hcs'] = gdf['tile_id'].map(hcs)
            logger.info(f"{country}: Patched num_hcs ({len(gdf_hcs)} HCs)")

    if 'shelters' in columns:
        gdf_shelters = fetch_shelters(country, rewrite=1)
        if gdf_shelters.empty and shelter_exist(country):
            logger.warning(f"{country}: Shelter data re-fetch returned empty, falling back to existing cache")
            gdf_shelters = load_shelter_locations(country)
        if gdf_shelters.empty:
            logger.warning(f"{country}: No shelter data available: num_shelters set to NaN")
            gdf['num_shelters'] = float('nan')
        else:
            viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
            shelters = viewer.map_points(points=gdf_shelters)
            gdf['num_shelters'] = gdf['tile_id'].map(shelters)
            logger.info(f"{country}: Patched num_shelters ({len(gdf_shelters)} shelters)")

    if 'wash' in columns:
        gdf_wash = fetch_wash(country, rewrite=1)
        if gdf_wash.empty and wash_exist(country):
            logger.warning(f"{country}: WASH data re-fetch returned empty, falling back to existing cache")
            gdf_wash = load_wash_locations(country)
        if gdf_wash.empty:
            logger.warning(f"{country}: No WASH data available: num_wash set to NaN")
            gdf['num_wash'] = float('nan')
        else:
            viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
            wash_pts = viewer.map_points(points=gdf_wash)
            gdf['num_wash'] = gdf['tile_id'].map(wash_pts)
            logger.info(f"{country}: Patched num_wash ({len(gdf_wash)} WASH points)")

    if 'vulnerability' in columns:
        vuln = _load_vulnerability_tiles_csv(country, zoom_level)
        if vuln is not None:
            gdf['moderate_poverty_prob'] = gdf['tile_id'].map(vuln['moderate_poverty_prob'])
            gdf['severe_poverty_prob']   = gdf['tile_id'].map(vuln['severe_poverty_prob'])
            n_mod = gdf['moderate_poverty_prob'].notna().sum()
            n_sev = gdf['severe_poverty_prob'].notna().sum()
            logger.info(
                f"{country}: Patched moderate_poverty_prob ({n_mod} tiles) + "
                f"severe_poverty_prob ({n_sev} tiles) from vulnerability CSV"
            )
        else:
            logger.warning(
                f"{country}: No vulnerability CSV found at "
                f"{_vulnerability_file_path(country, zoom_level)}; "
                f"columns not updated. Run vulnerability/fetch_vulnerability_probs.py first."
            )

    if columns:
        write_dataset(gdf, data_store, file_path)
        logger.info(f"{country}: Patch complete: saved updated mercator parquet")

        # Re-aggregate all existing admin parquets so baseline counts stay in sync.
        if 'id' not in gdf.columns:
            logger.warning(f"{country}: Mercator parquet has no 'id' column, skipping admin parquet sync "
                           f"(run --type initialize to add admin assignments)")
        else:
            for existing_level in get_initialized_admin_levels(country):
                admin_file_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views',
                                               f"{country}_admin{existing_level}.parquet")
                gdf_admin = read_dataset(admin_file_path, data_store)
                if existing_level == 1:
                    # admin1 IDs are already in the mercator parquet's 'id' column
                    src = gdf
                    group_col = 'id'
                else:
                    # For other levels, temporarily remap tile IDs via spatial join
                    src, _ = add_admin_ids(gdf.drop(columns=['id'], errors='ignore'),
                                           country, admin_level=existing_level, strict=True)
                    group_col = 'id'
                agg_dict = {col: (_optional_sum if col in _OPTIONAL_SUM_COLS else "sum")
                            for col in sum_cols_admin if col in src.columns}
                agg_dict.update({col: "mean" for col in avg_cols_admin if col in src.columns})
                agg = src.groupby(group_col).agg(agg_dict).reset_index()
                pw = _poverty_weighted_mean(src, group_col)
                for col in pw.columns:
                    agg[col] = agg[group_col].map(pw[col])
                agg = agg.rename(columns={group_col: 'tile_id'})
                # Ensure every admin region already in the persisted parquet still
                # appears, even one with zero assigned tiles this time (e.g. a tiny
                # offshore island admin unit), matching the same all_ids-merge
                # protection _build_admin_view_from_mercator() applies at init time.
                # Without this, groupby only emits ids with >=1 assigned tile, and
                # since save_admin_view() below fully overwrites the parquet (not an
                # append/merge), any zero-tile region already on file would be
                # permanently dropped here, not just reordered.
                all_tile_ids = gdf_admin[['tile_id']].copy()
                agg = all_tile_ids.merge(agg, on='tile_id', how='left')
                for col in sum_cols_admin:
                    if col in agg.columns and col not in _OPTIONAL_SUM_COLS:
                        agg[col] = agg[col].fillna(0)
                d_name = gdf_admin.set_index('tile_id')['name'].to_dict() if 'name' in gdf_admin.columns else {}
                d_geo = gdf_admin.set_index('tile_id')['geometry'].to_dict()
                agg['name'] = agg['tile_id'].map(d_name)
                agg['geometry'] = agg['tile_id'].map(d_geo)
                agg = convert_to_geodataframe(agg)
                save_admin_view(agg, country, admin_level=existing_level)
                logger.info(f"{country}: Synced admin{existing_level} parquet with patched columns")

    # Create new admin parquets for levels requested via --columns adminN
    if admin_patch_levels:
        for admin_level in admin_patch_levels:
            try:
                # gdf always already carries an 'id' column (written by the
                # admin_level=1 add_admin_ids() call at --type initialize time),
                # regardless of which admin_level is being created here.
                # _build_admin_view_from_mercator() always recomputes 'id' itself
                # via its own add_admin_ids() call, so the stale 'id' must be
                # dropped for EVERY admin_level (not just admin_level != 1), or
                # the merge inside admins_overlay() collides two 'id' columns
                # into id_x/id_y, and the subsequent groupby("id") raises an
                # uncaught KeyError that isn't caught by the except ValueError
                # below (matches the same 'id' handling in the sibling
                # save_mercator_and_admin_views() branch above).
                src = gdf.drop(columns=['id'], errors='ignore')
                admin_view = _build_admin_view_from_mercator(src, country, admin_level=admin_level)
                save_admin_view(admin_view, country, admin_level=admin_level)
                logger.info(f"{country}: Created admin{admin_level} parquet")
            except ValueError as e:
                logger.error(f"{country}: Cannot create admin{admin_level}: {e}")


def _build_admin_view_from_mercator(view, country, admin_level):
    """
    Aggregate mercator tiles to admin boundaries and return a GeoDataFrame.

    Internal helper used by save_mercator_and_admin_views to avoid duplicating
    the aggregation logic across the new/rewrite branches.

    Args:
        view: Mercator tile GeoDataFrame (must have 'tile_id' column)
        country: ISO3 country code
        admin_level: Admin level to aggregate to

    Returns:
        gpd.GeoDataFrame with one row per admin boundary and all demographic columns
    """
    combined_view, gdf_admins = add_admin_ids(view, country, admin_level=admin_level, strict=True)
    d = gdf_admins.set_index('id')['name'].to_dict()
    d_geo = gdf_admins.set_index('id')['geometry'].to_dict()

    agg_dict = {col: (_optional_sum if col in _OPTIONAL_SUM_COLS else "sum")
                for col in sum_cols_admin if col in combined_view.columns}
    agg_dict.update({col: "mean" for col in avg_cols_admin if col in combined_view.columns})
    agg = combined_view.groupby("id").agg(agg_dict).reset_index()
    # Population-weighted mean for poverty columns (must run before merge so group_col exists)
    pw = _poverty_weighted_mean(combined_view, "id")
    for col in pw.columns:
        agg[col] = agg["id"].map(pw[col])
    # Ensure all admin regions appear even if no tiles were assigned to them.
    # Only fill non-optional columns with 0; optional ones stay NaN to signal no-data.
    all_ids = gdf_admins[['id']].copy()
    agg = all_ids.merge(agg, on='id', how='left')
    for col in sum_cols_admin:
        if col in agg.columns and col not in _OPTIONAL_SUM_COLS:
            agg[col] = agg[col].fillna(0)
    admin_view = agg.rename(columns={'id': 'tile_id'})
    admin_view['name'] = admin_view['tile_id'].map(d)
    admin_view['geometry'] = admin_view['tile_id'].map(d_geo)
    return convert_to_geodataframe(admin_view)


def save_mercator_and_admin_views(countries, zoom_level, rewrite, admin_levels=None):
    """
    Generates and saves all country mercator views and admin views.
    Automatically tracks initialization in Snowflake after successful completion.

    Args:
        countries: List of ISO3 country codes
        zoom_level: Zoom level for mercator tiles
        rewrite: If 1, regenerate existing views; if 0, skip if they exist
        admin_levels: List of admin levels to generate (default: [1])
    """
    if admin_levels is None:
        admin_levels = [1]

    for country in countries:
        file_name = f"{country}_{zoom_level}.parquet"
        file_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name)
        initialized = False

        if not data_store.file_exists(file_path):
            view = create_mercator_country_layer(country, zoom_level, rewrite)
            # Admin level 1 is always used for the mercator tile 'id' assignment
            # (stored in the mercator parquet for backward-compatibility)
            combined_view, gdf_admins1 = add_admin_ids(view, country, admin_level=1)
            save_mercator_view(combined_view, country, zoom_level)

            for admin_level in admin_levels:
                try:
                    src = view
                    admin_view = _build_admin_view_from_mercator(src, country, admin_level=admin_level)
                    save_admin_view(admin_view, country, admin_level=admin_level)
                except ValueError as e:
                    logger.error(f"{country}: Skipping admin{admin_level}: {e}")

            initialized = True
        elif rewrite:
            # When rewrite=1, regenerate the entire mercator view from scratch
            view = create_mercator_country_layer(country, zoom_level, rewrite)
            combined_view, gdf_admins1 = add_admin_ids(view, country, admin_level=1)
            save_mercator_view(combined_view, country, zoom_level)

            for admin_level in admin_levels:
                try:
                    src = view
                    admin_view = _build_admin_view_from_mercator(src, country, admin_level=admin_level)
                    save_admin_view(admin_view, country, admin_level=admin_level)
                except ValueError as e:
                    logger.error(f"{country}: Skipping admin{admin_level}: {e}")

            initialized = True
        else:
            # Mercator file already exists and rewrite=0. Skip regeneration.
            # Still create any admin parquets for levels not yet initialized.
            logger.info(f"Mercator file already exists for {country} at zoom {zoom_level}, ensuring tracking is up to date")
            view = read_dataset(file_path, data_store)
            for admin_level in admin_levels:
                admin_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views',
                                          f"{country}_admin{admin_level}.parquet")
                if not data_store.file_exists(admin_path):
                    try:
                        # The persisted mercator parquet always already carries an 'id'
                        # column (written by the admin_level=1 add_admin_ids() call that
                        # ran when it was first saved), regardless of which admin_level
                        # is being built here. _build_admin_view_from_mercator() always
                        # recomputes 'id' itself via its own add_admin_ids() call, so the
                        # stale 'id' column must be dropped for EVERY admin_level (not
                        # just admin_level != 1), or the merge inside admins_overlay()
                        # collides two 'id' columns into id_x/id_y, and the subsequent
                        # groupby("id") raises an uncaught KeyError that isn't caught by
                        # the except ValueError below, aborting the whole country loop.
                        src = view.drop(columns=['id'], errors='ignore')
                        admin_view = _build_admin_view_from_mercator(src, country, admin_level=admin_level)
                        save_admin_view(admin_view, country, admin_level=admin_level)
                        logger.info(f"{country}: Created admin{admin_level} parquet")
                    except ValueError as e:
                        logger.error(f"{country}: Skipping admin{admin_level}: {e}")
                else:
                    logger.info(f"{country}: admin{admin_level} already exists, skipping")
            initialized = True
        
        # Automatically track initialization and write boundary to Snowflake
        # Both are safe to call even if already tracked / already populated
        if initialized:
            try:
                update_country_initialized(country, zoom_level)
                logger.info(f"Tracked initialization for {country} at zoom level {zoom_level} in Snowflake")
            except Exception as e:
                logger.warning(f"Could not track initialization for {country} in Snowflake: {e}")
                logger.warning("  (Initialization completed, but tracking failed)")
            write_country_boundary(country)


# =============================================================================
# STORM METADATA
# =============================================================================
def save_json_storms(d):
    """
    Save json file with processed storm,dates
    """
    filename = os.path.join(RESULTS_DIR, STORMS_FILE)
    data_store.write_file(filename, json.dumps(d).encode())


def load_json_storms():
    """
    Read json file with saved storm,dates
    """
    filename = os.path.join(RESULTS_DIR, STORMS_FILE)
    if data_store.file_exists(filename):
        raw = data_store.read_file(filename)
        return json.loads(raw)
    return {'storms': {}}


def load_mercator_view(country, zoom_level=14):
    """Load mercator view for country"""
    file_name = f"{country}_{zoom_level}.parquet"
    return read_dataset(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name), data_store)


# =============================================================================
# PER-STORM IMPACT VIEW GENERATION
# Functions that intersect storm envelopes with facility/tile data to produce
# per-storm impact probability views. Called on every --type update run.
# =============================================================================

def _ensure_unique_zone_ids(gdf, id_col, facility_label):
    """
    Ensure every row in gdf has a unique value in id_col before passing to
    GeometryBasedZonalViewGenerator. Two failure modes cause silent data errors:

    1. Null IDs: all null-ID facilities collapse into one zone. The zone gets
       one probability from the spatial join, which fans out identically to every
       null-ID row on the merge → all receive probability=0 if the null zone has
       no envelope intersection, or a shared (possibly wrong) value otherwise.

    2. Duplicate non-null IDs: N facilities sharing the same ID each contribute
       a separate buffer to the zone, so the polygon-count for that zone is
       multiplied by N → probability inflated N×, potentially exceeding 1.0.

    Both are fixed by replacing offending values with unique _custom_<i> IDs.
    The first non-duplicate occurrence of a duplicate ID is kept as-is; only
    subsequent duplicates are renamed.
    """
    gdf = gdf.copy()
    counter = 0
    fallbacks_assigned = False

    null_mask = gdf[id_col].isna()
    if null_mask.any():
        logger.warning(
            f"{null_mask.sum()} {facility_label}(s) have no {id_col}, assigning fallback IDs."
        )
        fallbacks_assigned = True

    dup_mask = gdf[id_col].duplicated(keep='first')
    if dup_mask.any():
        dup_ids = gdf.loc[dup_mask, id_col].unique().tolist()
        logger.warning(
            f"{dup_mask.sum()} {facility_label}(s) have duplicate {id_col} values {dup_ids}, "
            "assigning fallback IDs to prevent probability double-counting."
        )
        fallbacks_assigned = True

    if fallbacks_assigned:
        # Cast to str BEFORE assigning fallback strings to avoid pandas FutureWarning
        # ("Setting an item of incompatible dtype is deprecated") and the PyArrow
        # int64 conversion failure when writing mixed-type columns to Parquet.
        gdf[id_col] = gdf[id_col].astype(str)
        if null_mask.any():
            gdf.loc[null_mask, id_col] = [f"_custom_{counter + i}" for i in range(null_mask.sum())]
            counter += null_mask.sum()
        if dup_mask.any():
            gdf.loc[dup_mask, id_col] = [f"_custom_{counter + i}" for i in range(dup_mask.sum())]

    return gdf


def create_school_view_from_envelopes(gdf_schools, gdf_envelopes, threshold_column='wind_threshold'):
    """
    Create per-facility school impact views from hurricane envelopes.

    For each wind speed threshold, calculates the probability that each individual
    school will be affected by winds at or above that threshold. Probability is
    computed as the fraction of ensemble members whose wind envelope intersects
    the school (buffered by 150m).

    Output is one row per school per wind threshold, NOT aggregated to tiles.
    This preserves all school attributes from the location cache (including
    education_level, school_type, etc.) alongside the computed probability.
    To get per-school detail by type, join the impact view to the location cache
    (<COUNTRY>_schools.parquet) on school_id_giga.

    The impact views are saved as:
        school_views/<COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet
    The location cache (written once at init) is:
        school_views/<COUNTRY>_schools.parquet

    Args:
        gdf_schools: GeoDataFrame of school locations (from fetch_schools / location cache).
                     Must have 'school_id_giga' column and valid geometry.
        gdf_envelopes: GeoDataFrame of hurricane envelope geometries with
                       'wind_threshold' and 'ensemble_member' columns.

    Returns:
        dict: Maps wind threshold (int, knots) → GeoDataFrame with one row per school,
              containing 'zone_id' (= school_id_giga) and 'probability' (0.0–1.0).
              Empty dict if gdf_schools is empty or invalid.
    """
    # Validate input is a GeoDataFrame
    if not isinstance(gdf_schools, gpd.GeoDataFrame):
        logger.error(f"gdf_schools must be a GeoDataFrame, got {type(gdf_schools)}. Returning empty views.")
        return {}
    
    # Handle empty GeoDataFrame gracefully
    if gdf_schools.empty:
        logger.warning("School GeoDataFrame is empty, returning empty views")
        return {}
    
    # Ensure geometry column exists
    if 'geometry' not in gdf_schools.columns or gdf_schools.geometry.isna().all():
        logger.error("School GeoDataFrame has no valid geometry column. Returning empty views.")
        return {}
    
    gdf_schools = _ensure_unique_zone_ids(gdf_schools, 'school_id_giga', 'school')

    gdf_schools_buff = buffer_geodataframe(gdf_schools, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}

    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes[threshold_column].unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            schools_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_schools_buff, zone_id_column='school_id_giga')
            try:
                new_col = schools_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except KeyError as e:
                # gigaspatial's map_polygons(value_columns=None) internally adds a
                # '_temp_polygon_count_dummy' column and reads it back via
                # result.set_index('zone_id')[col], when zero envelope polygons
                # intersect any zone at this threshold (a normal, expected outcome,
                # e.g. no member reaches a high wind_threshold), its own internal
                # aggregate_polygons_to_zones() early-returns before ever adding
                # that column, so the read-back raises a bare KeyError. Narrowed
                # from a broad except so a genuinely different bug here doesn't
                # get silently misattributed to this benign case,  matches the
                # same real root cause already handled this way in
                # create_tracks_view_from_envelopes()'s tile aggregation below.
                logger.info(f"No envelope/school intersections at {wind_th} ({threshold_column}) -- defaulting probability to 0.0: {e}")
                probs = {k: 0.0 for k in schools_viewer.view['zone_id'].unique()}
            schools_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = schools_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views


def _filter_hcs_to_relevant_types(gdf_hcs):
    """
    Filter health center facilities to HC_FACILITY_TYPES (hospital/clinic/doctors),
    at analysis time. HC_FACILITY_TYPES is a dict of {column: [values]} so multiple
    OSM tag keys can be combined (e.g. amenity + healthcare); a facility matches if
    ANY of the specified column/value pairs apply (OR logic across keys). The
    location cache stores all types; this filter controls what enters impact files.
    Applies equally to API-sourced and custom data.

    Shared by every analysis-time consumer of gdf_hcs (create_health_center_view_
    from_envelopes, create_tracks_view_from_envelopes) so hc_views and track_views
    severity_hcs agree on which facilities count, rather than each filtering
    independently (or not at all) and silently disagreeing.
    """
    if gdf_hcs is None or gdf_hcs.empty:
        return gdf_hcs
    before = len(gdf_hcs)
    mask = pd.Series(False, index=gdf_hcs.index)
    for col, values in HC_FACILITY_TYPES.items():
        if col in gdf_hcs.columns:
            mask |= gdf_hcs[col].isin(values)
    filtered = gdf_hcs[mask].copy()
    logger.debug(f"HC type filter: {before} → {len(filtered)} facilities (kept: {HC_FACILITY_TYPES})")
    return filtered


def create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes, threshold_column='wind_threshold'):
    """
    Create per-facility health center impact views from hurricane envelopes.

    For each wind speed threshold, calculates the probability that each individual
    health center will be affected by winds at or above that threshold. Probability
    is computed as the fraction of ensemble members whose wind envelope intersects
    the facility (buffered by BUFFER_DISTANCE_METERS).

    Output is one row per facility per wind threshold, NOT aggregated to tiles.
    All attributes from the location cache are preserved (including the `amenity`
    column), so impact views can be filtered by facility type without joining back
    to the cache. Only facilities matching HC_FACILITY_TYPES are included.
    To cross-reference with full facility metadata, join on 'osm_id' to the
    location cache (<COUNTRY>_health_centers.parquet).

    The impact views are saved as:
        hc_views/<COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet
    The location cache (written once at init, filtered to HC_FACILITY_TYPES) is:
        hc_views/<COUNTRY>_health_centers.parquet

    Args:
        gdf_hcs: GeoDataFrame of health center locations (from fetch_health_centers /
                 location cache). Must have 'osm_id' column and valid geometry.
        gdf_envelopes: GeoDataFrame of hurricane envelope geometries with
                       'wind_threshold' and 'ensemble_member' columns.

    Returns:
        dict: Maps wind threshold (int, knots) → GeoDataFrame with one row per facility,
              containing 'zone_id' (= osm_id) and 'probability' (0.0–1.0).
              Empty dict if gdf_hcs is empty or invalid.
    """
    # Validate input is a GeoDataFrame with usable geometry, matching the same
    # guards create_school_view_from_envelopes/create_shelter_view_from_envelopes/
    # create_wash_view_from_envelopes all apply: without this, a malformed
    # gdf_hcs (e.g. a corrupted/legacy cache read back without an active
    # geometry column) would crash uncaught inside buffer_geodataframe() below
    # instead of returning {} gracefully like its siblings.
    if not isinstance(gdf_hcs, gpd.GeoDataFrame):
        logger.error(f"gdf_hcs must be a GeoDataFrame, got {type(gdf_hcs)}. Returning empty views.")
        return {}
    if not gdf_hcs.empty and ('geometry' not in gdf_hcs.columns or gdf_hcs.geometry.isna().all()):
        logger.error("Health center GeoDataFrame has no valid geometry column. Returning empty views.")
        return {}

    gdf_hcs = _filter_hcs_to_relevant_types(gdf_hcs)

    if gdf_hcs.empty:
        logger.warning(f"No health facilities matching {HC_FACILITY_TYPES}, returning empty impact views")
        return {}

    gdf_hcs = _ensure_unique_zone_ids(gdf_hcs, 'osm_id', 'health center')
    gdf_hcs_buff = buffer_geodataframe(gdf_hcs, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}

    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes[threshold_column].unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            hcs_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_hcs_buff, zone_id_column='osm_id')
            try:
                new_col = hcs_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except KeyError as e:
                # See create_school_view_from_envelopes() above for the full
                # explanation: a bare KeyError here means zero envelope/HC
                # intersections at this threshold, a normal expected outcome,
                # not a real failure.
                logger.info(f"No envelope/health-center intersections at {wind_th} ({threshold_column}) -- defaulting probability to 0.0: {e}")
                probs = {k: 0.0 for k in hcs_viewer.view['zone_id'].unique()}
            hcs_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = hcs_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views


def create_shelter_view_from_envelopes(gdf_shelters, gdf_envelopes, threshold_column='wind_threshold'):
    """
    Create per-facility shelter impact views from hurricane envelopes.

    For each wind speed threshold, calculates the probability that each shelter
    will be affected by winds at or above that threshold.

    All cached shelter types enter impact files (no type filtering).
    Impact views saved as: shelter_views/<COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet

    Args:
        gdf_shelters: GeoDataFrame of shelter locations. Must have 'osm_id' column.
        gdf_envelopes: GeoDataFrame of hurricane envelopes.

    Returns:
        dict: wind threshold → GeoDataFrame with 'zone_id' (=osm_id) and 'probability'.
    """
    if not isinstance(gdf_shelters, gpd.GeoDataFrame):
        logger.error(f"gdf_shelters must be a GeoDataFrame, got {type(gdf_shelters)}. Returning empty views.")
        return {}
    if gdf_shelters.empty:
        logger.warning("Shelter GeoDataFrame is empty, returning empty views")
        return {}
    if 'geometry' not in gdf_shelters.columns or gdf_shelters.geometry.isna().all():
        logger.error("Shelter GeoDataFrame has no valid geometry. Returning empty views.")
        return {}

    gdf_shelters = _ensure_unique_zone_ids(gdf_shelters, 'osm_id', 'shelter')
    gdf_shelters_buff = buffer_geodataframe(gdf_shelters, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}
    num_ensembles = FULL_ENSEMBLE_SIZE
    for wind_th in gdf_envelopes[threshold_column].unique():
        gdf_env_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_env_wth.empty:
            viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_shelters_buff, zone_id_column='osm_id')
            try:
                new_col = viewer.map_polygons(gdf_env_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except KeyError as e:
                # See create_school_view_from_envelopes() above for the full
                # explanation: a bare KeyError here means zero envelope/shelter
                # intersections at this threshold, a normal expected outcome,
                # not a real failure.
                logger.info(f"No envelope/shelter intersections at {wind_th} ({threshold_column}) -- defaulting probability to 0.0: {e}")
                probs = {k: 0.0 for k in viewer.view['zone_id'].unique()}
            viewer.add_variable_to_view(probs, 'probability')
            wind_views[wind_th] = viewer.to_geodataframe()
    return wind_views


def create_wash_view_from_envelopes(gdf_wash, gdf_envelopes, threshold_column='wind_threshold'):
    """
    Create per-facility WASH impact views from hurricane envelopes.

    For each wind speed threshold, calculates the probability that each WASH
    facility will be affected by winds at or above that threshold. All facility
    types in the cache enter impact calculations; type selection is controlled
    by WASH_LOCATION_TYPES at fetch time.

    Impact views saved as: wash_views/<COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet

    Args:
        gdf_wash: GeoDataFrame of WASH facility locations. Must have 'osm_id' column.
        gdf_envelopes: GeoDataFrame of hurricane envelopes.

    Returns:
        dict: wind threshold → GeoDataFrame with 'zone_id' (=osm_id) and 'probability'.
              Empty dict if gdf_wash is empty or invalid.
    """
    if not isinstance(gdf_wash, gpd.GeoDataFrame):
        logger.error(f"gdf_wash must be a GeoDataFrame, got {type(gdf_wash)}. Returning empty views.")
        return {}
    if gdf_wash.empty:
        logger.warning("WASH GeoDataFrame is empty, returning empty views")
        return {}
    if 'geometry' not in gdf_wash.columns or gdf_wash.geometry.isna().all():
        logger.error("WASH GeoDataFrame has no valid geometry. Returning empty views.")
        return {}

    gdf_wash = _ensure_unique_zone_ids(gdf_wash, 'osm_id', 'WASH facility')
    gdf_wash_buff = buffer_geodataframe(gdf_wash, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}
    num_ensembles = FULL_ENSEMBLE_SIZE
    for wind_th in gdf_envelopes[threshold_column].unique():
        gdf_env_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_env_wth.empty:
            viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_wash_buff, zone_id_column='osm_id')
            try:
                new_col = viewer.map_polygons(gdf_env_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except KeyError as e:
                # See create_school_view_from_envelopes() above for the full
                # explanation: a bare KeyError here means zero envelope/WASH
                # intersections at this threshold, a normal expected outcome,
                # not a real failure.
                logger.info(f"No envelope/WASH intersections at {wind_th} ({threshold_column}) -- defaulting probability to 0.0: {e}")
                probs = {k: 0.0 for k in viewer.view['zone_id'].unique()}
            viewer.add_variable_to_view(probs, 'probability')
            wind_views[wind_th] = viewer.to_geodataframe()
    return wind_views


def create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes, threshold_column='wind_threshold'):
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
    # Population columns are a hard init-time requirement, not optional data:
    # a country missing one means that country's base layer needs
    # `--type patch --columns <col>`, an operational data-completeness
    # problem, not something this function should silently route around by
    # defaulting E_<col> to NaN for every tile. Fail loudly and immediately,
    # matching create_tracks_view_from_envelopes()'s existing precedent for
    # adolescent_population: for wind this propagates to the per-country
    # try/except in run_complete_impact_analysis() (correctly failing that
    # one country while the rest of the batch continues); for gust it's
    # caught by that call's own dedicated try/except and logged at warning
    # level, isolated from wind as already designed. The other data_cols
    # entries (built_surface_m2, schools/hcs/shelters/wash, smod, rwi) stay
    # NaN-tolerant below.
    _missing_pop_cols = [c for c in POPULATION_COLS if c not in gdf_tiles.columns]
    if _missing_pop_cols:
        raise ValueError(
            f"gdf_tiles is missing {_missing_pop_cols}, run "
            f"'--type patch --columns {' '.join(_missing_pop_cols)}' for this country "
            f"before mercator tile impact views can be computed."
        )

    wind_views = {}
    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes[threshold_column].unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
            try:
                # Use tiles-left, envelopes-right sjoin (same direction as
                # create_tracks_view_from_envelopes) so boundary tiles are
                # treated identically in the probability raster and per-member
                # track view. Reversing the join direction (envelopes-left,
                # tiles-right) reprojects the CRS the opposite way, which can
                # cause borderline tiles to be included in one view but
                # excluded from the other.
                tiles_geom = gdf_tiles[['tile_id', 'geometry']].copy()
                envs_geom = gdf_envelopes_wth[['geometry']].copy()
                if tiles_geom.crs != envs_geom.crs:
                    tiles_geom = tiles_geom.to_crs(envs_geom.crs)
                joined = gpd.sjoin(tiles_geom, envs_geom, how='inner', predicate='intersects')
                tile_counts = joined.groupby('tile_id').size()
                all_tile_ids = gdf_tiles['tile_id']
                probs = {tid: int(tile_counts.get(tid, 0)) / float(num_ensembles) for tid in all_tile_ids}
            except Exception as e:
                logger.warning(f"map_polygons failed for {threshold_column}, defaulting probabilities to 0: {e}")
                probs = {k: 0.0 for k in tiles_viewer.view['zone_id'].unique()}
            tiles_viewer.add_variable_to_view(probs, 'probability')

            df_view = tiles_viewer.to_dataframe()
            for col in data_cols:
                if col in df_view.columns:
                    df_view[f"E_{col}"] = df_view[col] * df_view['probability']
                else:
                    df_view[f"E_{col}"] = np.nan
                    logger.debug(f"Column '{col}' missing from tile data, E_{col} set to NaN (re-initialize country to populate)")

            df_view = df_view.drop(columns=[c for c in data_cols if c in df_view.columns])

            # Reset index to make zone_id a column (needed for calculate_ccis)
            # Check if 'zone_id' already exists as a column
            if 'zone_id' in df_view.columns:
                # Already have zone_id column, don't reset index
                pass
            else:
                # Reset index and ensure the resulting column is named 'zone_id'
                if df_view.index.name:
                    # Index has a name, reset and rename if needed
                    df_view = df_view.reset_index()
                    # Rename the first column (the index) to 'zone_id' if it's not already
                    first_col = df_view.columns[0]
                    if first_col != 'zone_id':
                        df_view = df_view.rename(columns={first_col: 'zone_id'})
                else:
                    # Index has no name, explicitly name it 'zone_id'
                    df_view = df_view.reset_index(names=['zone_id'])

            # Keep poverty columns last so CSV positions $2–$16 are identical
            # to the 16-column format. The procedure's IFF($13 IS NULL) detection
            # continues to work; $17/$18 are ignored by the current INSERT.
            _pov_cols = [c for c in ['moderate_poverty_prob', 'severe_poverty_prob'] if c in df_view.columns]
            if _pov_cols:
                df_view = df_view[[c for c in df_view.columns if c not in _pov_cols] + _pov_cols]

            wind_views[wind_th] = df_view

    return wind_views


def create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles, gdf_envelopes, threshold_column='wind_threshold'):
    # Population columns are a hard init-time requirement, not optional data:
    # a country missing one means that country's base layer needs
    # `--type patch --columns <col>`, an operational data-completeness
    # problem, not something this function should silently route around by
    # defaulting E_<col> to NaN for every tile (and then to 0 once aggregated
    # to admin level). Fail loudly and immediately, matching
    # create_tracks_view_from_envelopes()'s existing precedent for
    # adolescent_population: for wind this propagates to the per-country
    # try/except in run_complete_impact_analysis() (correctly failing that
    # one country while the rest of the batch continues); for gust it's
    # caught by that call's own dedicated try/except and logged at warning
    # level, isolated from wind as already designed.
    _missing_pop_cols = [c for c in POPULATION_COLS if c not in gdf_tiles.columns]
    if _missing_pop_cols:
        raise ValueError(
            f"gdf_tiles is missing {_missing_pop_cols}, run "
            f"'--type patch --columns {' '.join(_missing_pop_cols)}' for this country "
            f"before admin impact views can be computed."
        )

    if 'name' in gdf_admin.columns:
        d = gdf_admin.set_index('tile_id')['name'].to_dict()
    else:
        logger.warning("Admin GeoDataFrame missing 'name' column, admin region names will be NaN")
        d = {}
    wind_views = {}
    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes[threshold_column].unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
            try:
                # Use tiles-left, envelopes-right sjoin (same direction as
                # create_tracks_view_from_envelopes) so boundary tiles are
                # treated identically in the probability raster and per-member
                # track view. Reversing the join direction (envelopes-left,
                # tiles-right) reprojects the CRS the opposite way, which can
                # cause borderline tiles to be included in one view but
                # excluded from the other.
                tiles_geom = gdf_tiles[['tile_id', 'geometry']].copy()
                envs_geom = gdf_envelopes_wth[['geometry']].copy()
                if tiles_geom.crs != envs_geom.crs:
                    tiles_geom = tiles_geom.to_crs(envs_geom.crs)
                joined = gpd.sjoin(tiles_geom, envs_geom, how='inner', predicate='intersects')
                tile_counts = joined.groupby('tile_id').size()
                all_tile_ids = gdf_tiles['tile_id']
                probs = {tid: int(tile_counts.get(tid, 0)) / float(num_ensembles) for tid in all_tile_ids}
            except Exception as e:
                logger.warning(f"map_polygons failed for {threshold_column}, defaulting probabilities to 0: {e}")
                probs = {k: 0.0 for k in tiles_viewer.view['zone_id'].unique()}
            tiles_viewer.add_variable_to_view(probs, 'probability')

            df_view = tiles_viewer.to_dataframe()
            for col in data_cols:
                if col in df_view.columns:
                    df_view[f"E_{col}"] = df_view[col] * df_view['probability']
                else:
                    df_view[f"E_{col}"] = np.nan
                    logger.debug(f"Column '{col}' missing from tile data, E_{col} set to NaN (re-initialize country to populate)")

            df_view = df_view.drop(columns=[c for c in data_cols if c in df_view.columns])
            
            # Admin IDs must be present in gdf_tiles (added during initialization or on load)
            if 'id' not in gdf_tiles.columns:
                raise ValueError(
                    "Mercator view missing admin IDs."
                    "Admin IDs are added during initialization. "
                    "Re-initialize the country or check the mercator view file."
                )
            
            # Check if 'id' column is already present in df_view (from to_dataframe())
            # If not, we need to reset index and map from zone_id to id
            if 'id' not in df_view.columns:
                # Reset index to get zone_id as a column
                if 'zone_id' in df_view.columns:
                    # Already have zone_id column, don't reset index
                    pass
                else:
                    # Reset index and ensure the resulting column is named 'zone_id'
                    if df_view.index.name:
                        df_view = df_view.reset_index()
                        first_col = df_view.columns[0]
                        if first_col != 'zone_id':
                            df_view = df_view.rename(columns={first_col: 'zone_id'})
                    else:
                        df_view = df_view.reset_index(names=['zone_id'])
                
                # Map zone_id (tile_id) to admin id
                id_mapping = gdf_tiles.set_index('tile_id')['id'].to_dict()
                df_view['id'] = df_view['zone_id'].map(lambda x: id_mapping.get(x, x))
                # Drop zone_id column since we don't need it after mapping to admin IDs
                df_view = df_view.drop(columns=['zone_id'], errors='ignore')
            else:
                # If 'id' is already present, make sure zone_id is dropped if it exists
                df_view = df_view.drop(columns=['zone_id'], errors='ignore')
            
            # Group by admin id and aggregate (this creates admin-level data, not tile-level)
            # This should result in one row per admin region, not one row per tile
            
            # Define aggregation dictionary (optional cols preserve NaN when all-NaN)
            agg_dict = {col: (_optional_sum if col in _OPTIONAL_SUM_COLS else "sum")
                        for col in sum_cols}
            agg_dict.update({col: "mean" for col in avg_cols})

            # Group by admin id and aggregate (this creates admin-level data, not tile-level)
            # This should result in one row per admin region, not one row per tile
            agg = df_view.groupby("id").agg(agg_dict).reset_index()
            
            # Rename 'id' to 'tile_id' to match base admin parquet structure
            # Note: In base admin parquet, admin IDs are stored in 'tile_id' column
            # (despite the name, it contains admin region IDs, not tile IDs)
            df_view = agg.rename(columns={'id':'tile_id'})
            
            # Ensure zone_id is not present (shouldn't be, but be safe)
            df_view = df_view.drop(columns=['zone_id'], errors='ignore')
            
            ### add names ###
            df_view['name'] = df_view['tile_id'].map(d)
            missing_names = df_view['name'].isna().sum()
            if missing_names > 0:
                logger.warning(f"  {missing_names} admin region(s) at {wind_th} ({threshold_column}) have no name mapping (tile_id not in admin GeoDataFrame)")

            wind_views[wind_th] = df_view

    return wind_views


def create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member', gdf_shelters=None, gdf_wash=None, threshold_column='wind_threshold'):
    """Create tracks impact views from envelopes"""
    wind_views = {}

    # Buffer facility points and use predicate='intersects', matching every
    # sibling facility view (create_school_view_from_envelopes,
    # create_health_center_view_from_envelopes, etc.); map_points() defaults to
    # predicate='within' on raw points, unlike map_polygons()'s 'intersects'
    # default the siblings rely on, so without this a facility near an envelope
    # boundary could be counted impacted in school_views/hc_views but not here,
    # producing internally-inconsistent severity numbers for the same run.
    gdf_schools_buff = (buffer_geodataframe(gdf_schools, buffer_distance_meters=BUFFER_DISTANCE_METERS)
                        if gdf_schools is not None and not gdf_schools.empty else gdf_schools)
    # Filtered the same way create_health_center_view_from_envelopes filters gdf_hcs,
    # so severity_hcs agrees with hc_views' own num_hcs for the same run.
    gdf_hcs_filtered = _filter_hcs_to_relevant_types(gdf_hcs)
    gdf_hcs_buff = (buffer_geodataframe(gdf_hcs_filtered, buffer_distance_meters=BUFFER_DISTANCE_METERS)
                    if gdf_hcs_filtered is not None and not gdf_hcs_filtered.empty else gdf_hcs_filtered)
    gdf_shelters_buff = (buffer_geodataframe(gdf_shelters, buffer_distance_meters=BUFFER_DISTANCE_METERS)
                        if gdf_shelters is not None and not gdf_shelters.empty else gdf_shelters)
    gdf_wash_buff = (buffer_geodataframe(gdf_wash, buffer_distance_meters=BUFFER_DISTANCE_METERS)
                     if gdf_wash is not None and not gdf_wash.empty else gdf_wash)

    wind_ths = list(gdf_envelopes[threshold_column].unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]

        tracks_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_envelopes_wth, zone_id_column=index_column)

        # Schools
        schools = tracks_viewer.map_points(points=gdf_schools_buff, predicate='intersects')
        tracks_viewer.add_variable_to_view(schools, "severity_schools")

        # Health centers
        hcs = tracks_viewer.map_points(points=gdf_hcs_buff, predicate='intersects')
        tracks_viewer.add_variable_to_view(hcs, "severity_hcs")

        # Shelters
        _nan_members = {m: float('nan') for m in gdf_envelopes_wth[index_column].unique()}
        if gdf_shelters_buff is None or gdf_shelters_buff.empty:
            tracks_viewer.add_variable_to_view(_nan_members, "severity_num_shelters")
        else:
            tracks_viewer.add_variable_to_view(tracks_viewer.map_points(points=gdf_shelters_buff, predicate='intersects'), "severity_num_shelters")

        # WASH facilities
        if gdf_wash_buff is None or gdf_wash_buff.empty:
            tracks_viewer.add_variable_to_view(_nan_members, "severity_num_wash")
        else:
            tracks_viewer.add_variable_to_view(tracks_viewer.map_points(points=gdf_wash_buff, predicate='intersects'), "severity_num_wash")

        if "adolescent_population" not in gdf_tiles.columns:
            raise ValueError(
                "gdf_tiles is missing 'adolescent_population', run "
                "'--type patch --columns adolescent_population' for this country "
                "before track severity views can be computed."
            )
        tile_value_columns = ["population", "school_age_population", "infant_population",
                               "built_surface_m2", "adolescent_population"]
        _zero_members = {m: 0.0 for m in gdf_envelopes_wth[index_column].unique()}
        try:
            # gigaspatial's aggregate_polygons_to_zones() raises a bare
            # KeyError from inside map_polygons() itself (not something a
            # try/except around the CALL's return value can catch, since it
            # crashes before returning) when the spatial join finds zero
            # tile/envelope intersections at all: no tile falls within ANY
            # ensemble member's envelope ring for this wind threshold, a
            # real, reproducible case for high thresholds (e.g. 137kt) on a
            # storm/date where few or no members reach that speed over land.
            # That's a genuine "zero severity for every member" case (not
            # missing data), so catch it here and default to 0.0 for every
            # value column/member, matching the same try/except + 0.0
            # fallback pattern create_mercator_view_from_envelopes() already
            # uses for its own analogous zero-intersection risk.
            overlays = tracks_viewer.map_polygons(polygons=gdf_tiles, value_columns=tile_value_columns, aggregation="sum")
        except KeyError as e:
            logger.warning(f"map_polygons found no tile/envelope intersections for {threshold_column}={wind_th}, defaulting severity to 0: {e}")
            overlays = {col: dict(_zero_members) for col in tile_value_columns}
        tracks_viewer.add_variable_to_view(overlays.get('population', _zero_members), "severity_population")
        tracks_viewer.add_variable_to_view(overlays.get('adolescent_population', _zero_members), "severity_adolescent_population")
        tracks_viewer.add_variable_to_view(overlays.get('school_age_population', _zero_members), "severity_school_age_population")
        tracks_viewer.add_variable_to_view(overlays.get('infant_population', _zero_members), "severity_infant_population")
        tracks_viewer.add_variable_to_view(overlays.get('built_surface_m2', _zero_members), "severity_built_surface_m2")

        gdf_view = tracks_viewer.to_geodataframe()
        wind_views[wind_th] = gdf_view

    return wind_views


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
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name))

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
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name))

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
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name))

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
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name))

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


# =============================================================================
# ADMIN COUNTRY LAYER
# Logically part of Base Layer Initialization but placed here for historical
# reasons. Mirrors create_mercator_country_layer but aggregates at admin level N.
# =============================================================================
def create_admin_country_layer(country, rewrite=0, admin_level=1):
    """
    Create an admin-level layer with demographic and infrastructure data for a country.

    Fallback used during `--type update` when the base admin parquet is missing. Produces
    one row per admin boundary with all data columns aggregated to that level.
    Applies the same data requirements as create_mercator_country_layer():

    Data requirements: **hard failures** (aborts if unavailable):
        - Total population (WorldPop 1km)
        - School-age population (WorldPop 100m, age_structures)
        - Infant population (WorldPop 100m, age_structures)
        - Under-18 population (WorldPop 100m, age_structures)

    Data requirements: **optional** (NaN/0 if unavailable; backfill with --type patch):
        - GHSL built surface (built_surface_m2)
        - SMOD settlement class L2 (smod_class) and derived L1 (smod_class_l1)
        - Relative Wealth Index (rwi)
        - Schools (num_schools): NaN if API fails or no data
        - Health centers (num_hcs): NaN if API fails or no data
        - Emergency shelters (num_shelters): NaN if OSM returns nothing
        - WASH facilities (num_wash): NaN if OSM returns nothing

    Args:
        country: ISO3 country code
        rewrite: If 1, re-fetch school, HC, shelter, and WASH location caches from API/OSM;
                 if 0, use cached parquets if available
        admin_level: Admin level to use for boundary aggregation (default: 1)

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with admin boundaries and all demographic/
                         infrastructure columns. 'zone_id' renamed to 'tile_id'.
    """
    # Fetch facility locations: custom data priority handled inside fetch_*
    gdf_schools = fetch_schools(country, rewrite)
    gdf_hcs = fetch_health_centers(country, rewrite)
    gdf_shelters = fetch_shelters(country, rewrite)
    gdf_wash = fetch_wash(country, rewrite)

    # Note: AdminBoundariesViewGenerator uses admin boundary IDs (not quadkeys), so
    # custom tile-level CSVs (population_z<N>, built_surface_z<N>, etc.) do not apply here.
    # Custom point data is handled above via fetch_schools/fetch_health_centers/fetch_shelters/fetch_wash.
    tiles_viewer = AdminBoundariesViewGenerator(country=country, admin_level=admin_level, data_store=data_store)

    # Population: hard requirements, raises on failure
    tiles_viewer.map_wp_pop(
        country=country,
        resolution=WORLDPOP_RESOLUTION_LOW,
        output_column="school_age_population",
        school_age=False,
        project="age_structures",
        release="GR2",
        constrained=True,
        un_adjusted=False,
        min_age=SCHOOL_AGE_MIN,
        max_age=SCHOOL_AGE_MAX,
        sex='T',
    )
    tiles_viewer.map_wp_pop(
        country=country,
        resolution=WORLDPOP_RESOLUTION_LOW,
        output_column="infant_population",
        predicate='centroid_within',
        school_age=False,
        project="age_structures",
        release="GR2",
        constrained=True,
        un_adjusted=False,
        min_age=INFANT_AGE_MIN,
        max_age=INFANT_AGE_MAX,
        sex='T',
    )
    tiles_viewer.map_wp_pop(
        country=country,
        resolution=WORLDPOP_RESOLUTION_LOW,
        output_column="adolescent_population",
        school_age=False,
        project="age_structures",
        release="GR2",
        constrained=True,
        un_adjusted=False,
        min_age=ADOLESCENT_AGE_MIN,
        max_age=ADOLESCENT_AGE_MAX,
        sex='T',
    )
    tiles_viewer.map_wp_pop(country=country, resolution=WORLDPOP_RESOLUTION_LOW)

    # GHSL built surface: optional, NaN fallback
    try:
        tiles_viewer.map_built_s()
    except Exception as e:
        logger.warning(f"{country}: GHSL built surface unavailable, setting to NaN: {e}")
        tiles_viewer.add_variable_to_view(
            {k: np.nan for k in tiles_viewer.view.index.unique()}, 'built_surface_m2'
        )

    # SMOD settlement class: optional, NaN fallback
    try:
        tiles_viewer.map_smod()
    except Exception as e:
        logger.warning(f"{country}: GHSL SMOD unavailable, setting to NaN: {e}")
        tiles_viewer.add_variable_to_view(
            {k: np.nan for k in tiles_viewer.view.index.unique()}, 'smod_class'
        )

    # Derive smod_class_l1
    try:
        smod_l2 = tiles_viewer.view['smod_class']
        smod_l1 = smod_l2.map(SMOD_L2_TO_L1)
        tiles_viewer.add_variable_to_view(smod_l1.to_dict(), 'smod_class_l1')
    except Exception as e:
        logger.warning(f"{country}: Could not derive smod_class_l1: {e}")
        tiles_viewer.add_variable_to_view(
            {k: np.nan for k in tiles_viewer.view.index.unique()}, 'smod_class_l1'
        )

    # Schools, health centers, shelters, WASH
    # If the fetch returned empty (API failure, rate limit, etc.) store NaN so the
    # admin parquet records "data unavailable" rather than silently writing 0,
    # matching create_mercator_country_layer()'s own convention for the same
    # underlying data (map_points()'s count aggregation fillna(0)s every zone
    # when given zero input points, which would otherwise read as "confirmed
    # zero facilities" instead of "data unavailable, needs --type patch").
    _nan_admin = {k: np.nan for k in tiles_viewer.view.index.unique()}

    if gdf_schools.empty:
        logger.warning(f"{country}: No school data: num_schools set to NaN. Backfill with --type patch --columns schools")
        tiles_viewer.add_variable_to_view(_nan_admin, "num_schools")
    else:
        tiles_viewer.add_variable_to_view(tiles_viewer.map_points(points=gdf_schools), "num_schools")

    if gdf_hcs.empty:
        logger.warning(f"{country}: No health center data: num_hcs set to NaN. Backfill with --type patch --columns hcs")
        tiles_viewer.add_variable_to_view(_nan_admin, "num_hcs")
    else:
        tiles_viewer.add_variable_to_view(tiles_viewer.map_points(points=gdf_hcs), "num_hcs")

    if gdf_shelters.empty:
        logger.warning(f"{country}: No shelter data: num_shelters set to NaN. Backfill with --type patch --columns shelters")
        tiles_viewer.add_variable_to_view(_nan_admin, "num_shelters")
    else:
        tiles_viewer.add_variable_to_view(tiles_viewer.map_points(points=gdf_shelters), "num_shelters")

    if gdf_wash.empty:
        logger.warning(f"{country}: No WASH data: num_wash set to NaN. Backfill with --type patch --columns wash")
        tiles_viewer.add_variable_to_view(_nan_admin, "num_wash")
    else:
        tiles_viewer.add_variable_to_view(tiles_viewer.map_points(points=gdf_wash), "num_wash")

    # RWI: optional, NaN fallback
    try:
        handler = RWIHandler(data_store=data_store)
        rwi_df = handler.load_data(country, ensure_available=True)
        if rwi_df is None or (hasattr(rwi_df, 'empty') and rwi_df.empty):
            raise ValueError(f"No RWI data available for {country}")
        rwi_gdf = convert_to_geodataframe(rwi_df)
        rwi = tiles_viewer.map_points(rwi_gdf, value_columns='rwi', aggregation='mean')
    except Exception as e:
        logger.warning(f"{country}: Relative Wealth Index unavailable, setting to NaN: {e}")
        rwi = {k: np.nan for k in tiles_viewer.view.index.unique()}
    tiles_viewer.add_variable_to_view(rwi, 'rwi')

    gdf_tiles = tiles_viewer.to_geodataframe()
    gdf_tiles.rename(columns={'zone_id': 'tile_id'}, inplace=True)

    return gdf_tiles


# =============================================================================
# TILE & STORM VIEW PERSISTENCE
# Save / load functions for per-storm tile impact views, CCI views, admin
# aggregated views, and track views.
# =============================================================================
def save_admin_view(gdf, country, admin_level=1):
    """Save base admin infrastructure view for country"""
    file_name = f"{country}_admin{admin_level}.parquet"
    path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name)
    write_dataset(gdf, data_store, path)

def save_admin_views(countries, rewrite=0, admin_level=1):
    """
    Generates and saves all country admin views for a given admin level.

    Args:
        countries: List of country codes
        rewrite: If 1, replace existing files; if 0, skip if exists
        admin_level: Admin level to generate views for (default: 1)
    """
    for country in countries:
        view = create_admin_country_layer(country, rewrite, admin_level=admin_level)
        save_admin_view(view, country, admin_level=admin_level)

# =============================================================================
# PRECIPITATION/RUNOFF VIEWS
# Storm-independent: tp exceedance-probability tiers and the ro/tp ratio's own
# exceedance-probability tiers, both raster-sampled per tile via centroid-
# based point sampling (TifProcessor.sample_by_coordinates()) rather than the
# polygon sjoin wind/gust use or map_rasters()'s polygon-based zonal stats
# Called from run_precip_analysis() once per --type update run, not once per storm.
# =============================================================================

@contextlib.contextmanager
def grid_to_geotiff_to_tifprocessor(grid_2d, lat_min, lat_max, lon_min, lon_max):
    """
    Materialize a probability grid as a temp GeoTIFF and wrap it in a
    TifProcessor, ready for centroid-based sampling via
    create_precip_tile_view(). Written to a local temp file regardless of
    DATA_PIPELINE_DB (ephemeral intermediate, never needs to persist). A
    context manager, not a plain function: TifProcessor reads the underlying
    file lazily on each call (rasterio.open() inside
    sample_by_coordinates()), not eagerly at construction, so the temp file
    must stay alive for the `with` block's duration and is only deleted on
    exit, not immediately after construction.

    Usage:
        with grid_to_geotiff_to_tifprocessor(grid, *bounds) as tif:
            view = create_precip_tile_view(gdf_tiles, tif)
    """
    from precip_utils import grid_to_geotiff  # local import: avoids a module-load-order
                                               # dependency between precip_utils and impact_analysis
    tmp_path = tempfile.NamedTemporaryFile(suffix='.tif', delete=False).name
    try:
        grid_to_geotiff(grid_2d, lat_min, lat_max, lon_min, lon_max, tmp_path)
        yield TifProcessor(dataset_path=tmp_path, data_store=LocalDataStore())
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def create_precip_tile_view(gdf_tiles, tif_processor):
    """
    Sample a precip/ratio probability raster onto mercator tiles.

    Uses each tile's centroid, not map_rasters()'s polygon-based zonal mean:
    the precip grid's own cells (~0.25 degrees) are far coarser than a
    zoom-14 mercator tile, so a tile's tiny polygon usually doesn't contain
    the enclosing raster cell's pixel-center point, which is what
    TifProcessor.sample_by_polygons() (rasterio.mask.mask with the default
    all_touched=False, and no all_touched override exposed) requires to
    count any pixel as "inside" the polygon.

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (from load_mercator_view()).
        tif_processor: TifProcessor wrapping a GeoTIFF probability grid
            (from precip_utils.grid_to_geotiff()).

    Returns:
        DataFrame with one row per tile: 'zone_id' (tile_id), 'probability',
        the full 'E_<col>' breakdown for every data_cols entry (E_population,
        E_school_age_population, E_infant_population, E_adolescent_population,
        E_built_surface_m2, E_smod_class, E_smod_class_l1, E_rwi, E_num_schools,
        E_num_hcs, E_num_shelters, E_num_wash: probability * <col>, the exact
        same loop-then-drop pattern wind/gust/river-flood's own tile views
        use, see create_mercator_view_from_envelopes()), 'native_cell_row'/
        'native_cell_col' (which precip grid cell this tile's centroid falls
        in; makes the tile-to-native-cell relationship explicit and
        queryable, e.g. "which other tiles share my native cell", rather than
        only true by coincidence of deterministic point-sampling, which
        already guarantees two tiles in the same cell get an identical
        probability today, this just makes that fact visible without needing
        to recompute the point-in-raster math from scratch). No raw
        moderate_poverty_prob/severe_poverty_prob: always hazard-independent,
        sourced from the base mercator parquet only, never carried per-hazard
        here (matches create_precip_admin_view()'s own already-clean shape).
    """
    tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
    try:
        # Centroid-on-geographic-CRS distortion is negligible at zoom-14 tile
        # scale (well under the ~0.25 degree precip grid resolution), so the
        # geopandas warning here is expected and safe to suppress, not a sign
        # of an actual precision problem worth reprojecting for.
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message='Geometry is in a geographic CRS')
            centroids = gdf_tiles.geometry.centroid
        coords = list(zip(centroids.x, centroids.y))
        values = tif_processor.sample_by_coordinates(coords)
        probs = dict(zip(gdf_tiles['tile_id'], values))

        # Native grid cell each tile's centroid falls in
        # against the GeoTIFF's own already-correct (half-cell-expanded)
        # bounds, not a second raster read.
        bounds = tif_processor.bounds
        dlon = (bounds.right - bounds.left) / tif_processor.width
        dlat = (bounds.top - bounds.bottom) / tif_processor.height
        # Clamped to the valid pixel range: a tile centroid sitting exactly on
        # (or within floating-point epsilon of) the raster's outer edge would
        # otherwise produce an out-of-range index (e.g. col == width at the
        # right edge, valid columns are 0..width-1).
        native_rows = {tid: min(int((bounds.top - y) / dlat), tif_processor.height - 1)
                       for tid, y in zip(gdf_tiles['tile_id'], centroids.y)}
        native_cols = {tid: min(int((x - bounds.left) / dlon), tif_processor.width - 1)
                       for tid, x in zip(gdf_tiles['tile_id'], centroids.x)}
    except Exception as e:
        logger.warning(f"sample_by_coordinates failed for precip tile view, defaulting probabilities to 0: {e}")
        probs = {k: 0.0 for k in tiles_viewer.view['zone_id'].unique()}
        native_rows = native_cols = {k: None for k in tiles_viewer.view['zone_id'].unique()}
    tiles_viewer.add_variable_to_view(probs, 'probability')
    tiles_viewer.add_variable_to_view(native_rows, 'native_cell_row')
    tiles_viewer.add_variable_to_view(native_cols, 'native_cell_col')

    df_view = tiles_viewer.to_dataframe()
    # Weight every data_cols column (school_age_population, infant_population,
    # adolescent_population, built_surface_m2, smod_class, smod_class_l1, rwi,
    # num_schools, num_hcs, num_shelters, num_wash, population, ...) by
    # 'probability' to produce its E_* equivalent, using the same loop-then-drop
    # pattern as wind's own create_mercator_view_from_envelopes() (this file,
    # ~line 2325), so precip's E_* breakdown is complete and consistent with
    # every other hazard rather than leaving raw/unweighted columns that merely
    # duplicate the base mercator parquet.
    for col in data_cols:
        if col in df_view.columns:
            df_view[f"E_{col}"] = df_view[col] * df_view['probability']
        else:
            df_view[f"E_{col}"] = np.nan
            logger.debug(f"Column '{col}' missing from tile data, E_{col} set to NaN")
    df_view = df_view.drop(columns=[c for c in data_cols if c in df_view.columns])

    # moderate_poverty_prob/severe_poverty_prob are dropped entirely rather than
    # E_-weighted or passed through: these are always hazard-independent, sourced
    # raw from the base table only (matches _EXPOSURE_E_PROP_MAP's own exclusion
    # principle in the dashboard). They are not part of data_cols, so the loop
    # above never touches them, and create_precip_admin_view() never carries
    # them either, so dropping them here keeps precip consistent with the other
    # hazard views.
    df_view = df_view.drop(columns=[c for c in ('moderate_poverty_prob', 'severe_poverty_prob')
                                     if c in df_view.columns])

    if 'zone_id' not in df_view.columns:
        if df_view.index.name:
            df_view = df_view.reset_index()
            first_col = df_view.columns[0]
            if first_col != 'zone_id':
                df_view = df_view.rename(columns={first_col: 'zone_id'})
        else:
            df_view = df_view.reset_index(names=['zone_id'])

    return df_view


def calculate_precip_tile_member_bitmask(gdf_tiles, bitmask_grid, lat_min, lat_max, lon_min, lon_max):
    """
    Real per-z14-tile, per-ensemble-member 64-bit coverage bitmask for one
    (threshold_mm, window_h) combination of rainfall exceedance (bit `m-1`
    set <=> member `m`'s accumulated rainfall exceeds threshold_mm at that
    tile's own native precip-grid cell). Same shape/purpose as Wind's own
    calculate_tile_member_bitmask() and River's own
    calculate_river_tile_member_bitmask() (see those functions' own
    docstrings) -- exists to move a live, per-request dashboard decode into
    a real pipeline output, not because that live decode was wrong.

    Unlike River (a genuine point-in-polygon sjoin against real flooded
    pixels) rain's own precip grid (~0.25 degrees) is far coarser than a
    z14 tile, so this reuses the exact same centroid-to-native-cell mapping
    create_precip_tile_view() already computes for its own probability
    sampling (bounds/dlon/dlat/native_row/native_col, see that function's
    own comment for the "why" of centroid sampling over polygon zonal
    stats) -- NOT a new sjoin. Every tile whose centroid falls in the same
    native cell shares the exact same bitmask value, by construction, same
    as they already share the same probability value today.

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (from load_mercator_view()).
        bitmask_grid: (n_lat, n_lon) uint64 array from precip_utils.
            exceedance_bitmask(), same shape/orientation as the raw
            period_grid it was derived from (row 0 = lat_max, matching
            read_precip_window()'s own north-at-row-0 convention).
        lat_min, lat_max, lon_min, lon_max: bitmask_grid's own real geo
            bounds, the same 4 values read_precip_window() returns
            alongside period_grid itself.

    Returns:
        DataFrame with columns ['tile_id' (string), 'bits' (uint64)], sparse
        (only tiles whose native cell has >=1 member exceeding threshold_mm
        get a row) -- same convention Wind/River's own bitmask functions
        use, and the same "real, common outcome" contract for an
        all-zero/empty result (no flooding-equivalent case here, this
        mirrors an all-below-threshold cycle, not an error).
    """
    n_lat, n_lon = bitmask_grid.shape
    # EXACT same dlat/dlon + half-cell bounds expansion grid_to_geotiff()
    # (this file's own module, precip_utils.py) uses to build the GeoTIFF
    # create_precip_tile_view() actually samples from. lat_min/lat_max/
    # lon_min/lon_max are OUTERMOST GRID POINT (pixel-center) coordinates,
    # NOT already the raster's own edge bounds: grid_to_geotiff() divides
    # by (n-1) intervals between points (not n), then expands the sampled
    # bounds by half a cell on every side before calling rasterio.transform.
    # from_bounds(), so tif_processor.bounds (what create_precip_tile_view()
    # actually reads) is systematically half a cell wider than the raw
    # min/max on each side. An earlier version of this function divided by
    # n (not n-1) and skipped the half-cell expansion entirely -- a real,
    # caught-in-review bug that silently mis-assigned a real fraction of
    # tiles to the wrong native cell, since it re-derived a parallel
    # formula instead of replicating grid_to_geotiff()'s own real math.
    # This must stay byte-for-byte consistent with that function, not an
    # independently-plausible-looking approximation.
    dlat = (lat_max - lat_min) / (n_lat - 1) if n_lat > 1 else 0.0
    dlon = (lon_max - lon_min) / (n_lon - 1) if n_lon > 1 else 0.0
    top = lat_max + dlat / 2
    left = lon_min - dlon / 2
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', message='Geometry is in a geographic CRS')
        centroids = gdf_tiles.geometry.centroid
    cy, cx = centroids.y.to_numpy(), centroids.x.to_numpy()
    # Same clamped-to-valid-range row/col math create_precip_tile_view()
    # already uses (a tile centroid sitting exactly on the grid's outer
    # edge would otherwise produce an out-of-range index). dlat/dlon == 0
    # only in the degenerate n_lat==1/n_lon==1 case (grid_to_geotiff()'s
    # own same guard), every tile centroid maps to the single row/col.
    rows = (np.clip(((top - cy) / dlat).astype(np.int64), 0, n_lat - 1)
            if dlat else np.zeros(len(cy), dtype=np.int64))
    cols = (np.clip(((cx - left) / dlon).astype(np.int64), 0, n_lon - 1)
            if dlon else np.zeros(len(cx), dtype=np.int64))
    tile_bits = bitmask_grid[rows, cols]
    tile_ids = gdf_tiles['tile_id'].to_numpy()

    nonzero = tile_bits != 0
    if not nonzero.any():
        return pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})
    return pd.DataFrame({
        'tile_id': pd.array(tile_ids[nonzero].astype(str), dtype='string'),
        'bits': tile_bits[nonzero],
    })


def create_precip_admin_view(gdf_admin, gdf_tiles, tif_processor):
    """
    Admin-level equivalent of create_precip_tile_view(): tile-level probability
    and the full E_* breakdown, then aggregated via groupby('id'), the exact
    same sum(E_*)/mean(avg_cols) pattern wind/gust's own admin views use (see
    create_admin_view_from_envelopes_new()), since this is a genuine per-member
    exceedance probability, the same "expected impact" shape, not a
    physical-property average needing different treatment.

    Uses the same sum_cols/avg_cols-driven agg_dict wind's own admin view
    builds, so every E_* column create_precip_tile_view() produces rolls up
    here too, not just population.

    Args:
        gdf_admin: GeoDataFrame of admin regions with 'tile_id'/'name' columns.
        gdf_tiles: GeoDataFrame of mercator tiles, must carry an admin 'id' column.
        tif_processor: TifProcessor wrapping a GeoTIFF probability grid.

    Returns:
        DataFrame with one row per admin region: 'tile_id' (admin id), 'name',
        the full E_* breakdown (summed, NaN-preserving for _OPTIONAL_SUM_COLS
        plus E_population, see below), 'probability' (mean, via avg_cols).
    """
    if 'name' in gdf_admin.columns:
        name_by_id = gdf_admin.set_index('tile_id')['name'].to_dict()
    else:
        logger.warning("Admin GeoDataFrame missing 'name' column, admin region names will be NaN")
        name_by_id = {}

    df_view = create_precip_tile_view(gdf_tiles, tif_processor)

    if 'id' not in gdf_tiles.columns:
        raise ValueError(
            "Mercator view missing admin IDs. Admin IDs are added during "
            "initialization. Re-initialize the country or check the mercator view file."
        )
    id_mapping = gdf_tiles.set_index('tile_id')['id'].to_dict()
    df_view['id'] = df_view['zone_id'].map(lambda x: id_mapping.get(x, x))
    df_view = df_view.drop(columns=['zone_id'], errors='ignore')

    # E_population is additionally treated as NaN-preserving here (via
    # _optional_sum, even though it's not in the global _OPTIONAL_SUM_COLS
    # set wind's own admin view uses) because unlike wind, which hard-
    # requires population at generation time (POPULATION_COLS, see
    # create_mercator_view_from_envelopes()'s own check), precip's
    # create_precip_tile_view() can genuinely produce a NaN E_population
    # (missing 'population' column case), so an admin region rolling up
    # all-NaN tiles must stay NaN, not silently become a confirmed zero
    # ("no precip-affected population here" vs. "data unavailable").
    agg_dict = {col: (_optional_sum if (col in _OPTIONAL_SUM_COLS or col == 'E_population') else "sum")
                for col in sum_cols}
    agg_dict.update({col: "mean" for col in avg_cols})
    agg = df_view.groupby('id').agg(agg_dict).reset_index()
    agg = agg.rename(columns={'id': 'tile_id'})
    agg['name'] = agg['tile_id'].map(name_by_id)
    missing_names = agg['name'].isna().sum()
    if missing_names > 0:
        logger.warning(f"  {missing_names} admin region(s) have no name mapping in precip admin view")

    return agg


def assign_facilities_to_tiles(gdf_facilities, gdf_tiles, id_column):
    """
    Map each facility to the single mercator tile containing it.

    Mirrors admins_overlay()'s centroid-based assignment pattern (this file),
    for the facility-to-tile direction instead of tile-to-admin. Same
    underlying gpd.sjoin(..., predicate='within') idiom this codebase already
    relies on for facility-to-zone assignment via
    ZonalViewGenerator.map_points() -> aggregate_points_to_zones() (used for
    num_schools/num_hcs/num_shelters/num_wash), but that path immediately
    collapses the per-facility result into a per-tile count and never exposes
    the per-facility mapping, this function keeps it instead of discarding it.
    Both sides are already EPSG:4326 (fetch_schools/fetch_health_centers/
    fetch_shelters/fetch_wash and load_mercator_view all use WGS84 natively),
    no reprojection needed for the primary within-predicate join.

    Args:
        gdf_facilities: GeoDataFrame of facility locations (already deduped
            via _ensure_unique_zone_ids and any type-filtered).
        gdf_tiles: GeoDataFrame of mercator tiles (tile_id + geometry).
        id_column: facility unique ID column ('school_id_giga' or 'osm_id').

    Returns:
        DataFrame with id_column and 'tile_id'. A facility can genuinely
        match zero tiles (coastal/border facilities geocoded just outside
        the tile-covered country polygon, gdf_tiles only includes tiles that
        intersect the true country boundary, not a padded bounding box),
        handled with a sjoin_nearest fallback, same three-tier pattern
        admins_overlay uses for tiles matching zero admin regions.
    """
    dup_count = gdf_facilities[id_column].duplicated().sum()
    if dup_count > 0:
        logger.warning(
            f"assign_facilities_to_tiles got {dup_count} duplicate {id_column} value(s); "
            "the precondition is that the caller already deduped via _ensure_unique_zone_ids. "
            "Proceeding, but only one tile per duplicated id will be kept (drop_duplicates keep='first')."
        )

    facility_points = gdf_facilities[[id_column, 'geometry']].copy()
    joined = gpd.sjoin(facility_points, gdf_tiles[['tile_id', 'geometry']],
                       how='left', predicate='within').drop(columns=['index_right'], errors='ignore')
    joined = joined.drop_duplicates(subset=id_column, keep='first')

    unmatched = joined[joined['tile_id'].isna()]
    if len(unmatched) > 0:
        logger.debug(f"{len(unmatched)} facility(s) matched no tile directly, falling back to nearest tile")
        # Equal-area reprojection before sjoin_nearest, same courtesy
        # admins_overlay's own nearest-neighbour fallback already applies,
        # geographic-CRS degrees don't reflect true distance for "nearest".
        unmatched_facilities = gdf_facilities[gdf_facilities[id_column].isin(unmatched[id_column])][[id_column, 'geometry']].copy()
        unmatched_facilities['geometry'] = unmatched_facilities.geometry.to_crs("ESRI:54009")
        tiles_proj = gdf_tiles[['tile_id', 'geometry']].to_crs("ESRI:54009")
        nearest = gpd.sjoin_nearest(unmatched_facilities, tiles_proj, how='left').drop_duplicates(subset=id_column, keep='first')
        # Only tile_id, not geometry: nearest's own geometry is the equal-area
        # reprojection above (metres, ESRI:54009), carrying it into `joined`
        # would silently mislabel that facility's coordinates while `joined`
        # still declares itself EPSG:4326.
        joined = joined.set_index(id_column)
        joined.update(nearest.set_index(id_column)[['tile_id']])
        joined = joined.reset_index()

    return joined[[id_column, 'tile_id']]


def create_precip_facility_view(gdf_facilities, facility_tile_map, tile_view_df, id_column):
    """
    A facility's precip/ratio exposure is its containing tile's already-
    computed probability, not an independent raster sample. Precip's native
    grid (~0.25 degrees, ~27km/cell) is far coarser than a mercator tile
    (~100-150m at zoom 14), so a tile's own probability is already just
    "whichever native cell my centroid lands in"; routing facility exposure
    through the tile guarantees a facility always agrees with the tile it
    physically sits inside (no possible discrepancy near a native-grid-cell
    boundary), and needs no raster access here at all, just a merge.

    Unlike wind/gust's per-facility-type create functions (which compute
    each facility's probability independently via their own buffered-
    polygon-vs-envelope intersection test, with zero reference to mercator
    tiles, confirmed directly: real wind facility files have no tile_id
    column at all), this deliberately ties precip's facility exposure to the
    tile grid, since precip's native grid is intrinsically coarser than a
    tile, wind's per-facility independence remains correct for wind because
    its envelope polygons carry real fine-grained shape.

    Args:
        gdf_facilities: GeoDataFrame of facility locations, already deduped
            via _ensure_unique_zone_ids() and any type-filtered.
        facility_tile_map: DataFrame from assign_facilities_to_tiles(),
            id_column + 'tile_id'.
        tile_view_df: create_precip_tile_view()'s own output for this
            threshold/window (zone_id==tile_id, probability).
        id_column: unique ID column ('school_id_giga' for schools, 'osm_id'
            for health centers/shelters/WASH, same as wind/gust).

    Returns:
        GeoDataFrame carrying every original attribute column from
        gdf_facilities (unlike wind/gust's own facility views, which only
        keep zone_id+geometry, confirmed directly against real output, not
        assumed) plus 'probability' (from the containing tile) and the
        facility's own true geometry (point, or polygon for HC building
        footprints, never a buffered approximation).
    """
    tile_probs = tile_view_df.set_index('zone_id')['probability'].to_dict()
    merged = gdf_facilities.merge(facility_tile_map, on=id_column, how='left')
    merged['probability'] = merged['tile_id'].map(tile_probs).fillna(0.0)
    return merged.drop(columns=['tile_id'])


def save_precip_tile_view(df, country, forecast_time, threshold_mm, window_h):
    """Save a tp exceedance-probability tile view: mercator_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.csv"""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h.csv"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views_precip', file_name))


def save_precip_tile_bitmask_view(df, country, forecast_time, threshold_mm, window_h):
    """track_tile_bitmask_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet

    Same 'precip' naming this repo already uses throughout (mercator_views_precip,
    save_precip_tile_view, etc, this repo's own consistent term regardless of
    the dashboard repo's own 'rain' shorthand for the same hazard) and the same
    p{threshold_mm}_{window_h}h token convention save_precip_tile_view already
    uses, separate top-level directory mirroring Wind's own
    save_tracks_tile_bitmask_view (track_tile_bitmask_views/) convention. See
    calculate_precip_tile_member_bitmask's own docstring for what this data is."""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_tile_bitmask_views_precip', file_name))


def save_precip_admin_view(df, country, forecast_time, threshold_mm, window_h, admin_level=1):
    """Save a tp exceedance-probability admin view: admin_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h_admin{admin_level}.csv"""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h_admin{admin_level}.csv"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views_precip', file_name))


def save_precip_ratio_view(df, country, forecast_time, ratio_threshold, window_h):
    """
    Save a ro/tp ratio exceedance-probability tile view. 'g' token (ratio
    cut point x100) distinguishes this from tp's 'p'-prefixed mm thresholds
    and avoids a decimal point in the filename:
    mercator_views_precipratio/{country}_{forecast_time}_g{ratio_threshold*100}_{window_h}h.csv
    """
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h.csv"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views_precipratio', file_name))


def save_precip_ratio_admin_view(df, country, forecast_time, ratio_threshold, window_h, admin_level=1):
    """Admin-level equivalent of save_precip_ratio_view(), see its docstring for the g-token convention."""
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h_admin{admin_level}.csv"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views_precipratio', file_name))


# =============================================================================
# PRECIP FACILITY-LEVEL VIEWS (schools/health centers/shelters/WASH)
# Parquet, not CSV: these carry a real geometry column (the facility's true
# point location), matching wind/gust's own facility-view file format
# exactly, unlike precip's tile/admin views above (CSV, no geometry).
# =============================================================================

def save_precip_school_view(gdf, country, forecast_time, threshold_mm, window_h):
    """school_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views_precip', file_name))


def save_precip_ratio_school_view(gdf, country, forecast_time, ratio_threshold, window_h):
    """school_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.parquet"""
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views_precipratio', file_name))


def save_precip_hc_view(gdf, country, forecast_time, threshold_mm, window_h):
    """hc_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views_precip', file_name))


def save_precip_ratio_hc_view(gdf, country, forecast_time, ratio_threshold, window_h):
    """hc_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.parquet"""
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views_precipratio', file_name))


def save_precip_shelter_view(gdf, country, forecast_time, threshold_mm, window_h):
    """shelter_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views_precip', file_name))


def save_precip_ratio_shelter_view(gdf, country, forecast_time, ratio_threshold, window_h):
    """shelter_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.parquet"""
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views_precipratio', file_name))


def save_precip_wash_view(gdf, country, forecast_time, threshold_mm, window_h):
    """wash_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views_precip', file_name))


def save_precip_ratio_wash_view(gdf, country, forecast_time, ratio_threshold, window_h):
    """wash_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.parquet"""
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views_precipratio', file_name))


# =============================================================================
# RIVER FLOOD (GloFAS x JRC) VIEWS
# =============================================================================
# GloFAS/JRC pixels are ~150m resolution, comparable to a zoom-14 mercator
# tile or a buffered facility footprint, NOT much coarser the way precip's
# ~0.25 degree grid is. So unlike create_precip_tile_view() (which must
# centroid-sample a continuous raster because a tile's tiny polygon usually
# doesn't contain the enclosing coarse cell's pixel-center), river-flood
# views reuse the wind/gust "count(members)/FULL_ENSEMBLE_SIZE" convention:
# a member "hits" a zone if at least one of its flooded pixels falls within
# that zone, same idiom create_mercator_view_from_envelopes() already uses
# (manual gpd.sjoin + groupby, not a generic library helper), just points-
# in-polygon (predicate='within') instead of polygon-in-polygon
# (predicate='intersects'), and .nunique('member') instead of .size(): a
# single member can contribute several flooded pixels to the same zone at
# this resolution, so counting distinct members (not rows) avoids inflating
# exposure.
#
# below_min_basin is a per-pixel QC flag (this cell's own upstream drainage
# area is below GloFAS's own 500km^2 minimum-catchment cutoff for
# JRC-extent-map generation), a matching-availability flag, NOT a
# confidence statement about the discharge forecast. Per the upstream
# TC-ECMWF-Forecast-Pipeline repo's own explicit design principle ("gate on
# RP tier for relevance, attach channel scale as a secondary attribute, not
# a pre-filter"), it is NEVER used to exclude a pixel/zone here, only
# surfaced as a per-zone .any() boolean alongside the real probability.
def create_river_tile_view(gdf_tiles, gdf_pixels_step, rp_tier, is_standin,
                            num_ensembles=FULL_ENSEMBLE_SIZE):
    """
    Per-tile flood-extent probability for one RP tier + lead-time WINDOW:
    fraction of ensemble members with >=1 flooded JRC pixel inside the tile
    at ANY real day within that window (real union across days, not a
    single-day snapshot -- see RIVER_LEADTIME_STEPS_H's own "ACCUMULATION"
    comment in main_pipeline.py for the full rationale).

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (from load_mercator_view()).
        gdf_pixels_step: GeoDataFrame of this country's flooded pixels for
            every real day up through the caller's selected window already
            (caller filters by `step_h <= window` before calling -- this
            function itself does no day-level filtering, it just counts
            distinct members across whatever rows it's given, which is what
            makes multi-day rows here a real union rather than requiring
            any extra union logic in this function), columns: member,
            below_min_basin, geometry (Point, EPSG:4326). May be empty (a
            normal, common outcome -- no flooding within this window).
        rp_tier: label for logging only (e.g. 'rp10').
        is_standin: bool, from the real RIVER_FORECASTS.IS_STANDIN column --
            True for rp2/rp5 (JRC has no native map, they reuse rp10's own
            extent as a labelled upper-bound approximation), False for
            rp10/rp20/rp50/rp100 (native JRC tier match). Passed through
            unchanged as a per-tier constant column, never re-derived here.

    Returns:
        DataFrame with one row per tile: 'zone_id' (tile_id), 'probability',
        the full E_* breakdown (E_population, E_school_age_population,
        E_infant_population, E_adolescent_population, E_built_surface_m2,
        E_smod_class, E_smod_class_l1, E_rwi, E_num_schools, E_num_hcs,
        E_num_shelters, E_num_wash) -- the same `data_cols` loop
        create_mercator_view_from_envelopes() (wind) and gust use, so this
        file's shape matches a wind/gust mercator_views CSV exactly except
        for the missing CCI/vulnerability columns (out of scope for river
        flood, same as gust/precip), plus 'below_min_basin' (any
        contributing pixel flagged) and 'is_standin'.

    Raises:
        ValueError: if gdf_tiles is missing any POPULATION_COLS column, same
            hard-fail guard create_mercator_view_from_envelopes() (wind) and
            create_admin_view_from_envelopes_new() use -- these are a hard,
            always-required init-time dependency, not optional data. Without
            this guard, a country missing e.g. school_age_population would
            silently get NaN at tile level, and create_river_admin_view()'s
            plain-'sum' aggregation for these columns (deliberately excluded
            from _OPTIONAL_SUM_COLS, since they're never expected to be
            all-NaN) would then turn that into a fabricated 0.0 at admin
            level -- indistinguishable from a genuine zero-exposure result.
    """
    _missing_pop_cols = [c for c in POPULATION_COLS if c not in gdf_tiles.columns]
    if _missing_pop_cols:
        raise ValueError(
            f"gdf_tiles is missing {_missing_pop_cols}, run "
            f"'--type patch --columns {' '.join(_missing_pop_cols)}' for this country "
            f"before river-flood tile impact views can be computed."
        )

    tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
    all_tile_ids = gdf_tiles['tile_id']
    try:
        if gdf_pixels_step.empty:
            raise ValueError("no flooded pixels for this country/step")
        tiles_geom = gdf_tiles[['tile_id', 'geometry']].copy()
        pix_geom = gdf_pixels_step[['member', 'below_min_basin', 'geometry']].copy()
        if pix_geom.crs != tiles_geom.crs:
            pix_geom = pix_geom.to_crs(tiles_geom.crs)
        # Pixels (points) left, tiles (polygons) right: predicate='within'
        # requires left.within(right), so the point must be the left side
        # unlike create_mercator_view_from_envelopes()'s symmetric
        # 'intersects' (a free direction choice there, made for CRS-
        # reprojection-precision reasons), this direction is required by
        # 'within' semantics, not a style choice.
        joined = gpd.sjoin(pix_geom, tiles_geom, how='inner', predicate='within')
        member_counts = joined.groupby('tile_id')['member'].nunique()
        basin_flags = joined.groupby('tile_id')['below_min_basin'].any()
        probs = {tid: int(member_counts.get(tid, 0)) / float(num_ensembles) for tid in all_tile_ids}
        basin = {tid: bool(basin_flags.get(tid, False)) for tid in all_tile_ids}
    except ValueError:
        # Expected, common outcome: no flooded pixels this country/step.
        probs = {tid: 0.0 for tid in all_tile_ids}
        basin = {tid: False for tid in all_tile_ids}
    except Exception as e:
        # Anything else (e.g. a CRS mismatch, an upstream schema change
        # dropping/renaming 'below_min_basin') is a real bug, not a normal
        # "no flooding" outcome -- must be visible at warning level, same as
        # create_mercator_view_from_envelopes()'s own catch-all does for the
        # structurally identical case, not silently absorbed at debug level.
        logger.warning(f"River-flood tile intersection failed for {rp_tier}, defaulting probabilities to 0: {e}")
        probs = {tid: 0.0 for tid in all_tile_ids}
        basin = {tid: False for tid in all_tile_ids}

    tiles_viewer.add_variable_to_view(probs, 'probability')
    tiles_viewer.add_variable_to_view(basin, 'below_min_basin')
    df_view = tiles_viewer.to_dataframe()

    # Same generic data_cols -> E_{col} loop as create_mercator_view_from_envelopes()
    # (wind) / gust, not precip's E_population-only shape: river-flood's
    # architecture is modeled on wind/gust throughout (see module docstring),
    # so its tile view should carry the same E_* breakdown, not a reduced one.
    for col in data_cols:
        if col in df_view.columns:
            df_view[f"E_{col}"] = df_view[col] * df_view['probability']
        else:
            df_view[f"E_{col}"] = np.nan
    df_view = df_view.drop(columns=[c for c in data_cols if c in df_view.columns])
    df_view['is_standin'] = bool(is_standin)

    if 'zone_id' not in df_view.columns:
        if df_view.index.name:
            df_view = df_view.reset_index()
            first_col = df_view.columns[0]
            if first_col != 'zone_id':
                df_view = df_view.rename(columns={first_col: 'zone_id'})
        else:
            df_view = df_view.reset_index(names=['zone_id'])

    # Keep poverty columns last, same convention as create_mercator_view_from_envelopes().
    _pov_cols = [c for c in ['moderate_poverty_prob', 'severe_poverty_prob'] if c in df_view.columns]
    if _pov_cols:
        df_view = df_view[[c for c in df_view.columns if c not in _pov_cols] + _pov_cols]

    return df_view


def calculate_river_tile_member_bitmask(gdf_tiles, gdf_pixels_step):
    """
    Real per-z14-tile, per-ensemble-member 64-bit coverage bitmask for one
    (rp_tier, step_h window) of river flood extent (bit `m-1` set <=> member
    `m` has >=1 flooded JRC pixel inside that tile), same convention/shape
    as Wind/Gust's own calculate_tile_member_bitmask() (mirrors it directly,
    see that function's own docstring for the full rationale), applied to
    River instead: this repo's dashboard sibling already does an identical
    live decode of this same raw per-member GloFAS parquet
    (`_RiverExtentCache` in services/tile_server.py, `bits = uint64(1) <<
    (member - 1)`), this function exists to move that decode from a live,
    multi-minute, cold-cache dashboard operation to a real pipeline output,
    not because the live decode was wrong.

    Genuinely near-zero new I/O: reuses the same per-member pixel data
    create_river_tile_view() already reads (gdf_pixels_step, already
    country-scoped and step_h-filtered by the caller), running an
    independent gpd.sjoin() with the identical inputs/predicate/direction
    that function's own `joined` already uses (pixels left, tiles right,
    predicate='within' -- required by 'within' semantics, not a style
    choice, see create_river_tile_view's own comment). The sjoin itself is
    cheap; the real cost (downloading/DuckDB-filtering the raw global
    parquet) already happened upstream, once, before either function runs.

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (from load_mercator_view()),
            needs only 'tile_id' and 'geometry'.
        gdf_pixels_step: same shape create_river_tile_view() takes -- this
            country's flooded pixels for every real day up through the
            caller's selected window already (member, below_min_basin,
            geometry columns), may be empty (a normal, common outcome).

    Returns:
        DataFrame with columns ['tile_id' (string), 'bits' (uint64)], one
        row per distinct tile with >=1 member's flooded pixel falling
        inside it -- sparse, same convention Wind's own bitmask uses (a
        tile with zero coverage from every member simply doesn't appear).
        An empty gdf_pixels_step or a real zero-intersection result both
        return an empty DataFrame (not an error -- a common, expected
        outcome, same as create_river_tile_view's own no-flooding case).
        A genuine sjoin failure (e.g. a CRS mismatch) logs a warning and
        also returns an empty DataFrame -- unlike Wind's per-threshold
        None-vs-empty distinction, there is only ever ONE call per (rp_tier,
        step_h) here (not one per multiple real thresholds in one call), so
        the caller's own `files_written` bookkeeping simply skips writing a
        file when this returns empty, no separate None sentinel needed.
    """
    if gdf_pixels_step.empty:
        return pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})

    tiles_geom = gdf_tiles[['tile_id', 'geometry']].copy()
    pix_geom = gdf_pixels_step[['member', 'geometry']].copy()
    try:
        if pix_geom.crs != tiles_geom.crs:
            pix_geom = pix_geom.to_crs(tiles_geom.crs)
        joined = gpd.sjoin(pix_geom, tiles_geom, how='inner', predicate='within')
    except Exception as e:
        logger.warning(f"River tile_member_bitmask sjoin FAILED, no bitmask file will be written: {e}")
        return pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})

    if joined.empty:
        return pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})

    # Same vectorized OR-reduction as Wind's own calculate_tile_member_bitmask
    # (np.unique(..., return_inverse=True) + np.bitwise_or.at), and the same
    # 1-indexed member convention the dashboard's own _RiverExtentCache
    # already assumes for this exact raw data (`bits = 1 << (member - 1)`).
    member_arr = joined['member'].to_numpy().astype(np.uint64)
    bits = np.uint64(1) << (member_arr - np.uint64(1))
    tile_ids = joined['tile_id'].to_numpy()
    uniq_tiles, inverse = np.unique(tile_ids, return_inverse=True)
    tile_bits = np.zeros(len(uniq_tiles), dtype=np.uint64)
    np.bitwise_or.at(tile_bits, inverse, bits)

    return pd.DataFrame({
        'tile_id': pd.array(uniq_tiles.astype(str), dtype='string'),
        'bits': tile_bits,
    })


def create_river_admin_view(gdf_admin, gdf_tiles, gdf_pixels_step, rp_tier, is_standin):
    """
    Admin-level rollup of create_river_tile_view(), same sum_cols/avg_cols
    groupby-aggregation pattern create_admin_view_from_envelopes_new() (wind)
    and gust use -- not precip's E_population-only shape. below_min_basin
    OR'd (.any() -- a QC flag rolls up as "does ANY contributing tile carry
    it", never averaged/summed), is_standin passed through as the same
    per-tier constant.
    """
    if 'name' in gdf_admin.columns:
        name_by_id = gdf_admin.set_index('tile_id')['name'].to_dict()
    else:
        logger.warning("Admin GeoDataFrame missing 'name' column, admin region names will be NaN")
        name_by_id = {}

    df_view = create_river_tile_view(gdf_tiles, gdf_pixels_step, rp_tier, is_standin)

    if 'id' not in gdf_tiles.columns:
        raise ValueError(
            "Mercator view missing admin IDs. Admin IDs are added during "
            "initialization. Re-initialize the country or check the mercator view file."
        )
    id_mapping = gdf_tiles.set_index('tile_id')['id'].to_dict()
    df_view['id'] = df_view['zone_id'].map(lambda x: id_mapping.get(x, x))
    df_view = df_view.drop(columns=['zone_id'], errors='ignore')

    agg_dict = {col: (_optional_sum if col in _OPTIONAL_SUM_COLS else "sum")
                for col in sum_cols if col in df_view.columns}
    agg_dict.update({col: "mean" for col in avg_cols if col in df_view.columns})
    agg_dict['below_min_basin'] = 'any'

    agg = df_view.groupby('id').agg(agg_dict).reset_index()
    agg = agg.rename(columns={'id': 'tile_id'})
    agg['is_standin'] = bool(is_standin)
    agg['name'] = agg['tile_id'].map(name_by_id)
    missing_names = agg['name'].isna().sum()
    if missing_names > 0:
        logger.warning(f"  {missing_names} admin region(s) have no name mapping in river admin view")

    return agg


def create_river_facility_view(gdf_facilities, facility_tile_map, tile_view_df, id_column, is_standin):
    """
    A facility's river-flood exposure is its containing mercator tile's
    already-computed probability (and below_min_basin QC flag), not an
    independent per-facility buffer-and-pixel-intersect test.

    GloFAS/JRC flood-extent pixels are patchy/localized (river channels,
    low-lying pockets), unlike wind/gust's smooth, contiguous envelope
    polygons. A facility's own point-buffer can sit in a part of a tile the
    flood-extent pixels never touch, even though another part of that SAME
    tile genuinely floods, so an independent per-facility buffer-and-intersect
    test against the raw flooded pixels can report 0.0 for a facility inside
    a tile whose own probability is nonzero -- a confusing, inconsistent
    result. Deriving facility exposure from the containing tile's own
    probability instead keeps the two views (tile and facility) consistent
    by construction. Wind/gust's envelope polygons don't have this failure
    mode (their independent per-facility buffer-intersect is left untouched),
    but precip's grid faces the identical patchy-vs-point problem and is
    solved the same way (create_precip_facility_view, this function's own
    direct template).

    Args:
        gdf_facilities: GeoDataFrame of facility locations (already deduped
            via _ensure_unique_zone_ids, type-filtered where relevant) --
            UNBUFFERED. No buffer_geodataframe() call is needed by the caller:
            the tile lookup below needs each facility's true point/footprint
            for assign_facilities_to_tiles' own within-predicate join, not an
            inflated one; the output below also carries this true geometry
            (point, or polygon for HC building footprints), matching precip's
            own established convention.
        facility_tile_map: DataFrame from assign_facilities_to_tiles(),
            id_column + 'tile_id' -- computed ONCE per country (tile
            assignment doesn't depend on rp_tier/step_h), not once per call.
        tile_view_df: create_river_tile_view()'s own real output for this
            exact (rp_tier, step_h) -- 'zone_id' (==tile_id), 'probability',
            'below_min_basin'.
        id_column: 'school_id_giga' or 'osm_id'.
        is_standin: bool, same per-tier constant create_river_tile_view()
            already receives -- passed straight through, not re-derived.

    Returns:
        DataFrame carrying every original attribute column from
        gdf_facilities (name/type/lat/lon/geometry/etc, matching
        create_precip_facility_view's own real-output shape, matching
        SCHOOL_RIVER_MAT's ORCHESTRATION ingestion SQL, which parses fields
        by name via VARIANT, not positionally, so exact column ORDER here
        doesn't matter, only names) plus 'probability'/'below_min_basin'
        (both from the containing tile) and 'is_standin'. `id_column` is
        renamed to 'zone_id' in the output (unlike precip's own function,
        which leaves this rename to its SQL ingestion layer instead) to
        match SCHOOL_RIVER_MAT's ingestion SQL verbatim (`parquet_variant:zone_id`,
        not aliased from the facility's own id column the way precip's is).
        A facility whose tile assignment is missing (should not happen given
        assign_facilities_to_tiles' own nearest-tile fallback) defaults to
        0.0/False, matching create_precip_facility_view's own convention.
    """
    tile_probs = tile_view_df.set_index('zone_id')['probability'].to_dict()
    tile_basin = tile_view_df.set_index('zone_id')['below_min_basin'].to_dict()
    merged = gdf_facilities.merge(facility_tile_map, on=id_column, how='left')
    merged['probability'] = merged['tile_id'].map(tile_probs).fillna(0.0)
    merged['below_min_basin'] = merged['tile_id'].map(tile_basin).fillna(False)
    merged['is_standin'] = bool(is_standin)
    return merged.drop(columns=['tile_id']).rename(columns={id_column: 'zone_id'})


def save_river_tile_view(df, country, forecast_time, rp_tier, step_h):
    """mercator_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.csv"""
    file_name = f"{country}_{forecast_time}_{rp_tier}_{step_h}h.csv"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views_river', file_name))


def save_river_tile_bitmask_view(df, country, forecast_time, rp_tier, step_h):
    """track_tile_bitmask_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet

    Same filename token order as save_river_tile_view's own mercator_views_river/
    convention (real key: country, forecast_time, rp_tier, step_h -- river is
    date/cycle-keyed, not storm-keyed, see calculate_river_tile_member_bitmask's
    own docstring), separate top-level directory mirroring Wind's own
    save_tracks_tile_bitmask_view (track_tile_bitmask_views/) convention."""
    file_name = f"{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_tile_bitmask_views_river', file_name))


def save_river_admin_view(df, country, forecast_time, rp_tier, step_h, admin_level=1):
    """admin_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h_admin{level}.csv"""
    file_name = f"{country}_{forecast_time}_{rp_tier}_{step_h}h_admin{admin_level}.csv"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views_river', file_name))


def save_river_school_view(gdf, country, forecast_time, rp_tier, step_h):
    """school_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views_river', file_name))


def save_river_hc_view(gdf, country, forecast_time, rp_tier, step_h):
    """hc_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views_river', file_name))


def save_river_shelter_view(gdf, country, forecast_time, rp_tier, step_h):
    """shelter_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views_river', file_name))


def save_river_wash_view(gdf, country, forecast_time, rp_tier, step_h):
    """wash_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views_river', file_name))


def save_tiles_view(gdf, country, storm, date, wind_th, zoom_level, dataset='wind'):
    """
    Saves tiles views. dataset='gust' writes to mercator_views_gust/ with a
    'g'-prefixed threshold token, keeping gust files structurally distinct
    from wind (own directory, no numeric-threshold collision).
    """
    dir_name = 'mercator_views_gust' if dataset == 'gust' else 'mercator_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}_{zoom_level}.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name))


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

def save_admin_tiles_view(gdf, country, storm, date, wind_th, admin_level=1, dataset='wind'):
    """
    Saves admin tiles views. dataset='gust' writes to admin_views_gust/ with a
    'g'-prefixed threshold token, keeping gust files structurally distinct
    from wind (own directory, so ADMIN_IMPACT_FILE_RE-style downstream parsers
    that assume wind semantics never see them).
    """
    dir_name = 'admin_views_gust' if dataset == 'gust' else 'admin_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}_admin{admin_level}.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name))


def save_cci_admin(gdf, country, storm, date, admin_level=1):
    """
    Saves Child Cyclone Index (CCI) admin-level views to storage.

    Args:
        gdf: GeoDataFrame containing CCI values aggregated by admin level
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        admin_level: Admin level these views correspond to (default: 1)
    """
    file_name = f"{country}_{storm}_{date}_admin{admin_level}_cci.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))


def save_vulnerability_tiles(gdf, country, storm, date, zoom_level):
    """
    Save vulnerability (people/children in need) tile-level view.

    One file per storm/date, not per wind threshold. Contains wind-dependent
    expected in-need counts aggregated across the full ensemble.

    Args:
        gdf: DataFrame with vulnerability columns (E_*_in_need) per tile
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        zoom_level: Zoom level for tiles
    """
    file_name = f"{country}_{storm}_{date}_{zoom_level}_vulnerability.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))


def save_vulnerability_admin(gdf, country, storm, date, admin_level=1):
    """
    Save vulnerability (people/children in need) admin-level view.

    One file per admin level per storm/date. Contains wind-dependent expected
    in-need counts aggregated to admin boundaries.

    Args:
        gdf: DataFrame with vulnerability columns (E_*_in_need) per admin unit
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        admin_level: Admin level (default: 1)
    """
    file_name = f"{country}_{storm}_{date}_admin{admin_level}_vulnerability.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))


def load_admin_view(country, admin_level=1):
    """Load admin view for country"""
    file_name = f"{country}_admin{admin_level}.parquet"
    return read_dataset(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name), data_store)


def save_tracks_view(gdf, country, storm, date, wind_th, dataset='wind'):
    """
    Saves tracks views. dataset='gust' writes to track_views_gust/ with a
    'g'-prefixed threshold token, keeping gust files structurally distinct
    from wind.
    """
    dir_name = 'track_views_gust' if dataset == 'gust' else 'track_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name))


def save_tracks_tile_bitmask_view(df, country, storm, date, wind_th, dataset='wind'):
    """
    Saves per-tile, per-member coverage bitmask views (see
    calculate_tile_member_bitmask's own docstring for what these are and
    why). Mirrors save_tracks_view's own dataset='gust' convention exactly
    (g-prefixed threshold token, separate _gust directory); real per-tile
    per-member data that was previously computed transiently for wind/gust
    severity and discarded; this is the first time it's persisted.
    """
    dir_name = 'track_tile_bitmask_views_gust' if dataset == 'gust' else 'track_tile_bitmask_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}.parquet"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name))


def save_vulnerability_tracks(df, country, storm, date, zoom_level):
    """
    Save per-ensemble-member vulnerability (people/children in need) view.

    One file per storm/date. Contains per-member summed vulnerability values across
    all tiles covered by that member's wind envelopes.

    Filename pattern: {CC}_{STORM}_{DATETIME}_{ZOOM}_vulnerability_tracks.parquet
    Saved alongside the per-threshold track parquets in track_views/.

    Args:
        df: DataFrame with one row per ensemble member (zone_id = member number)
            and severity_* vulnerability columns.
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        zoom_level: Zoom level for tiles
    """
    file_name = f"{country}_{storm}_{date}_{zoom_level}_vulnerability_tracks.parquet"
    write_dataset(df, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', file_name))


# =============================================================================
# CCI CALCULATION
# =============================================================================
cci_cols = ['CCI_children', 'CCI_pop', 'CCI_school_age', 'CCI_infants', 'CCI_adolescents',
            'E_CCI_children', 'E_CCI_pop', 'E_CCI_school_age', 'E_CCI_infants', 'E_CCI_adolescents']


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
            - CCI_children: Child Cyclone Index for children (0–19: infants + school-age + adolescents)
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
    # Ensure all expected population columns are present (old tile files may be missing new columns)
    for pop_col in ['school_age_population', 'infant_population', 'adolescent_population', 'population']:
        if pop_col not in gdf_tiles_index.columns:
            logger.warning(f"Column '{pop_col}' missing from tile data, CCI_{pop_col} will be NaN (re-initialize country)")
            gdf_tiles_index[pop_col] = np.nan
    cci_tiles_view = pd.DataFrame(index=gdf_tiles_index.index)

    # Children cci (0–19: school_age 5–14 + infants 0–4 + adolescents 15–19)
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'] + gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'] + gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'] + gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_children'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children']]

    # Children e cci (0–19: school_age 5–14 + infants 0–4 + adolescents 15–19)
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((sorted_wind_views_indexed[i]['E_school_age_population'] + sorted_wind_views_indexed[i]['E_infant_population'] + sorted_wind_views_indexed[i]['E_adolescent_population']) - (sorted_wind_views_indexed[i+1]['E_school_age_population'] + sorted_wind_views_indexed[i+1]['E_infant_population'] + sorted_wind_views_indexed[i+1]['E_adolescent_population'])).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_school_age_population'] + sorted_wind_views_indexed[k-1]['E_infant_population'] + sorted_wind_views_indexed[k-1]['E_adolescent_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_children'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children']]

    # school age cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((gdf_tiles_index['school_age_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['school_age_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_school_age'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age']]

    # school age e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((sorted_wind_views_indexed[i]['E_school_age_population']) - (sorted_wind_views_indexed[i+1]['E_school_age_population'])).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_school_age_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_school_age'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age']]

    # infant cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_infants'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants']]

    # infant e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((sorted_wind_views_indexed[i]['E_infant_population']) - (sorted_wind_views_indexed[i+1]['E_infant_population'])).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_infant_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_infants'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants']]

    # under-18 cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_adolescents'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_adolescents']]

    # under-18 e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((sorted_wind_views_indexed[i]['E_adolescent_population']) - (sorted_wind_views_indexed[i+1]['E_adolescent_population'])).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_adolescent_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_adolescents'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_adolescents','E_CCI_adolescents']]

    # pop cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((gdf_tiles_index['population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['population'])*(sorted_wind_views_indexed[i+1]['probability']>0)).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_pop'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_adolescents','E_CCI_adolescents','CCI_pop']]

    # pop e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((sorted_wind_views_indexed[i]['E_population']) - (sorted_wind_views_indexed[i+1]['E_population'])).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_pop'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_adolescents','E_CCI_adolescents','CCI_pop','E_CCI_pop']]
    cci_tiles_view = cci_tiles_view.reset_index()

    # Ensure the index column is named 'zone_id'
    if cci_tiles_view.columns[0] != 'zone_id':
        cci_tiles_view = cci_tiles_view.rename(columns={cci_tiles_view.columns[0]: 'zone_id'})

    cci_tiles_view['id'] = cci_tiles_view['zone_id'].map(d)

    return cci_tiles_view


def calculate_vulnerability_view(wind_tiles_views, gdf_tiles):
    """
    Calculate wind-dependent vulnerability (people/children in need) per tile.

    Combines per-tile poverty probability curves (stored in the base mercator parquet)
    with wind forecast probabilities to estimate expected humanitarian need under the
    full ensemble forecast.

    Unlike per-threshold views this aggregates ALL wind thresholds using mutually
    exclusive band probabilities, the output is a single view per storm/date, not
    per threshold. Follow the same pattern as calculate_ccis / CCI views.

    Wind-dependent rate per band:
      band < VULNERABILITY_WIND_SEVERE_THRESHOLD (50 kt):
          rate = severe_poverty_prob  (only the severely poor are considered immediately vulnerable)
      VULNERABILITY_WIND_SEVERE_THRESHOLD ≤ band < VULNERABILITY_WIND_FULL_THRESHOLD (96 kt):
          rate linearly interpolates: moderate_poverty_prob (at 50 kt) → 1.0 (at 96 kt)
          rate = moderate_poverty_prob × (1 − t) + t,  where t = (band − 50) / (96 − 50)
      band ≥ VULNERABILITY_WIND_FULL_THRESHOLD (96 kt):
          rate = 1.0 (catastrophic, all people need assistance)

    Note: child poverty rates are used as a proxy for total population vulnerability
    (E_people_in_need), a reasonable approximation given within-country wealth
    correlations are similar for children and adults.

    Args:
        wind_tiles_views: Dict[int, DataFrame] keyed by wind threshold in knots, as
                          returned by create_mercator_view_from_envelopes.
        gdf_tiles: GeoDataFrame: base mercator tiles including moderate_poverty_prob,
                   severe_poverty_prob, population demographics, and admin IDs.

    Returns:
        DataFrame with columns: zone_id, id, E_infant_in_need, E_school_age_in_need,
        E_adolescent_in_need, E_children_in_need, E_people_in_need.
        All E_* columns are NaN when no vulnerability data has been patched for the country.

    Raises:
        ValueError: If admin IDs are missing from gdf_tiles.
    """
    if 'id' not in gdf_tiles.columns:
        raise ValueError(
            "Mercator view missing admin IDs. "
            "Re-initialize the country or check the mercator view file."
        )

    tile_id_to_admin = gdf_tiles.set_index('tile_id')['id'].to_dict()
    winds = sorted(wind_tiles_views.keys())

    # Index all wind views by zone_id
    prob_by_wind = {
        w: wind_tiles_views[w].set_index('zone_id')['probability']
        for w in winds
    }

    base = gdf_tiles.rename(columns={'tile_id': 'zone_id'}).set_index('zone_id')

    for col in ['infant_population', 'school_age_population', 'adolescent_population', 'population']:
        if col not in base.columns:
            logger.warning(f"Column '{col}' missing from tile data, vulnerability will be NaN (re-initialize country)")
            base[col] = np.nan

    has_vuln_data = (
        'moderate_poverty_prob' in base.columns
        and 'severe_poverty_prob' in base.columns
        and base['moderate_poverty_prob'].notna().any()
    )

    result = pd.DataFrame(index=base.index)

    if not has_vuln_data:
        logger.warning(
            "No vulnerability probability data found (moderate_poverty_prob all NaN). "
            "Run vulnerability/fetch_vulnerability_probs.py then "
            "'--type patch --columns vulnerability' to populate."
        )
        for col in sum_cols_vulnerability:
            result[col] = np.nan
        result = result.reset_index()
        if result.columns[0] != 'zone_id':
            result = result.rename(columns={result.columns[0]: 'zone_id'})
        result['id'] = result['zone_id'].map(tile_id_to_admin)
        return result[['zone_id', 'id'] + sum_cols_vulnerability]

    # No fillna(0.0) here: a tile with genuinely missing RWI/poverty data (a real,
    # expected occurrence, RWI coverage is incomplete for some remote/border tiles
    # even in an otherwise-covered country, per vulnerability/fetch_vulnerability_probs.py's
    # own per-country NaN counting/logging) must not be silently treated as "0% poor".
    # Leaving it NaN correctly propagates through rate/vuln_weight below, so only
    # THAT tile's E_*_in_need columns come out NaN, not the whole country's (the
    # has_vuln_data check above already handles the all-NaN country-wide case).
    n_partial_nan = int((base['moderate_poverty_prob'].isna() | base['severe_poverty_prob'].isna()).sum())
    if n_partial_nan > 0:
        logger.warning(
            f"{n_partial_nan} tile(s) have partial NaN poverty-probability coverage "
            "(RWI unavailable for those specific tiles); their E_*_in_need columns "
            "will be NaN rather than silently treated as 0% poor"
        )
    mod_prob = base['moderate_poverty_prob']
    sev_prob = base['severe_poverty_prob']

    # Accumulate vulnerability weight: Σ p_band[k] × rate(k) over all wind bands
    vuln_weight = pd.Series(0.0, index=base.index)

    for i, wind in enumerate(winds):
        p_cum  = prob_by_wind[wind].reindex(base.index, fill_value=0.0)
        p_next = (
            prob_by_wind[winds[i + 1]].reindex(base.index, fill_value=0.0)
            if i < len(winds) - 1
            else pd.Series(0.0, index=base.index)
        )
        p_band = (p_cum - p_next).clip(lower=0.0)

        if wind < VULNERABILITY_WIND_SEVERE_THRESHOLD:
            rate = sev_prob
        elif wind < VULNERABILITY_WIND_FULL_THRESHOLD:
            t = (wind - VULNERABILITY_WIND_SEVERE_THRESHOLD) / (
                VULNERABILITY_WIND_FULL_THRESHOLD - VULNERABILITY_WIND_SEVERE_THRESHOLD
            )
            rate = mod_prob * (1.0 - t) + t
        else:
            rate = pd.Series(1.0, index=base.index)

        vuln_weight = vuln_weight + p_band * rate

    result['E_infant_in_need']     = base['infant_population']     * vuln_weight
    result['E_school_age_in_need'] = base['school_age_population'] * vuln_weight
    result['E_adolescent_in_need'] = base['adolescent_population'] * vuln_weight
    result['E_children_in_need']   = (
        base['infant_population'] + base['school_age_population'] + base['adolescent_population']
    ) * vuln_weight
    result['E_people_in_need']     = base['population'] * vuln_weight

    result = result.reset_index()
    if result.columns[0] != 'zone_id':
        result = result.rename(columns={result.columns[0]: 'zone_id'})
    result['id'] = result['zone_id'].map(tile_id_to_admin)
    return result[['zone_id', 'id'] + sum_cols_vulnerability]


def calculate_vulnerability_tracks(gdf_envelopes, gdf_tiles):
    """
    Calculate per-ensemble-member vulnerability (people/children in need).

    Mirrors calculate_vulnerability_view but produces one row per ensemble member
    instead of one row per tile. For each member, applies the same wind-band
    vulnerability rate logic, but using the member's own wind coverage (binary
    yes/no) rather than ensemble-probability weights.

    For each tile the member covers, the effective vulnerability rate is determined
    by the *highest* wind band that member's envelope reaches over that tile:
      - Highest band < 50 kt  → rate = severe_poverty_prob
      - Highest band 50–96 kt → rate linearly interpolates moderate_poverty_prob → 1.0
      - Highest band ≥ 96 kt  → rate = 1.0

    The per-member sum across all covered tiles gives the severity_* columns,
    analogous to severity_population in the existing per-threshold track parquets.

    Args:
        gdf_envelopes: GeoDataFrame with columns ensemble_member, wind_threshold,
                       geometry, as loaded from Snowflake TC_ENVELOPES_COMBINED.
        gdf_tiles: GeoDataFrame: base mercator tiles including moderate_poverty_prob,
                   severe_poverty_prob, and population demographics.

    Returns:
        DataFrame with one row per ensemble member and columns:
            zone_id                    : ensemble member number
            severity_people_in_need    : total PIN for tiles this member affects
            severity_children_in_need  : CHIN (infants + school-age + adolescents)
            severity_infant_in_need    : infants (0–4)
            severity_school_age_in_need: school age (5–14)
            severity_adolescent_in_need: adolescents (15–19)
        All severity_* columns are NaN when no vulnerability data has been
        patched for the country (same guard as calculate_vulnerability_view).
    """
    has_vuln_data = (
        'moderate_poverty_prob' in gdf_tiles.columns
        and 'severe_poverty_prob' in gdf_tiles.columns
        and gdf_tiles['moderate_poverty_prob'].notna().any()
    )

    members = sorted(gdf_envelopes['ensemble_member'].unique())
    winds = sorted(gdf_envelopes['wind_threshold'].unique())

    out_cols = [
        'severity_people_in_need',
        'severity_children_in_need',
        'severity_infant_in_need',
        'severity_school_age_in_need',
        'severity_adolescent_in_need',
    ]

    if not has_vuln_data:
        logger.warning(
            "No vulnerability probability data found; vulnerability_tracks will be all NaN. "
            "Run vulnerability/fetch_vulnerability_probs.py then "
            "'--type patch --columns vulnerability' to populate."
        )
        result = pd.DataFrame({'zone_id': members})
        for col in out_cols:
            result[col] = np.nan
        return result

    # Build a lookup: tile_id → {moderate_poverty_prob, severe_poverty_prob, pop cols}
    tile_index = gdf_tiles.set_index('tile_id')
    tiles_geom = gdf_tiles[['tile_id', 'geometry']].copy()

    # For each wind threshold, build a per-member tile mask via sjoin.
    # member_tiles_at[wind] = dict(member → set of tile_ids covered)
    member_tiles_at = {}
    for wind in winds:
        envs_w = gdf_envelopes[gdf_envelopes['wind_threshold'] == int(wind)][
            ['ensemble_member', 'geometry']
        ].copy()
        if envs_w.empty:
            member_tiles_at[wind] = {}
            continue
        if tiles_geom.crs != envs_w.crs:
            tiles_geom_proj = tiles_geom.to_crs(envs_w.crs)
        else:
            tiles_geom_proj = tiles_geom
        try:
            joined = gpd.sjoin(tiles_geom_proj, envs_w, how='inner', predicate='intersects')
            # joined has tile_id (index or column) and ensemble_member
            if 'tile_id' not in joined.columns:
                joined = joined.reset_index()
            grp = joined.groupby('ensemble_member')['tile_id'].apply(set).to_dict()
        except Exception as e:
            logger.warning(f"vulnerability_tracks sjoin failed at {wind}kt: {e}")
            grp = {}
        member_tiles_at[wind] = grp

    rows = []
    for member in members:
        # For each tile, find the highest wind band this member reaches over it.
        # vuln_weight[tile_id] = vulnerability rate for that tile under this member.
        tile_vuln = {}  # tile_id → rate

        for i, wind in enumerate(winds):
            covered = member_tiles_at[wind].get(member, set())
            if not covered:
                continue

            # Determine the rate for this wind band
            if wind < VULNERABILITY_WIND_SEVERE_THRESHOLD:
                # rate is per-tile (severe_poverty_prob)
                use_rate = 'severe'
            elif wind < VULNERABILITY_WIND_FULL_THRESHOLD:
                t = (wind - VULNERABILITY_WIND_SEVERE_THRESHOLD) / (
                    VULNERABILITY_WIND_FULL_THRESHOLD - VULNERABILITY_WIND_SEVERE_THRESHOLD
                )
                use_rate = ('interp', t)
            else:
                use_rate = 'full'

            # Always overwrite: winds iterated ascending → last assignment = highest band = correct rate.
            covered_list = list(covered)
            # Tiles this member's envelope covers but that aren't in tile_index at all
            # (absent from the base layer entirely) are a genuinely different, narrower
            # case than a tile with merely-missing RWI data, only these get filled to
            # 0.0 below; a tile present in tile_index with a real NaN poverty prob (RWI
            # unavailable for that specific tile, a normal, expected, partial-coverage
            # occurrence) must stay NaN, not silently become "0% poor".
            missing_from_base = set(covered_list) - set(tile_index.index)
            if use_rate == 'full':
                rates = pd.Series(1.0, index=covered_list)
            elif use_rate == 'severe':
                rates = tile_index.loc[
                    tile_index.index.intersection(covered_list), 'severe_poverty_prob'
                ].reindex(covered_list)
                if missing_from_base:
                    rates.loc[list(missing_from_base)] = 0.0
            else:
                _, t_val = use_rate
                mod = tile_index.loc[
                    tile_index.index.intersection(covered_list), 'moderate_poverty_prob'
                ].reindex(covered_list)
                if missing_from_base:
                    mod.loc[list(missing_from_base)] = 0.0
                rates = mod * (1.0 - t_val) + t_val
            tile_vuln.update(rates.to_dict())

        if not tile_vuln:
            rows.append({
                'zone_id': member,
                'severity_people_in_need': 0.0,
                'severity_children_in_need': 0.0,
                'severity_infant_in_need': 0.0,
                'severity_school_age_in_need': 0.0,
                'severity_adolescent_in_need': 0.0,
            })
            continue

        # Sum vulnerability across all covered tiles for this member (vectorized)
        rate_series = pd.Series(tile_vuln, name='rate')
        pop_cols = ['population', 'infant_population', 'school_age_population', 'adolescent_population']
        df_member = (
            tile_index.loc[tile_index.index.intersection(rate_series.index), pop_cols]
            .reindex(rate_series.index)
            .fillna(0.0)
            .multiply(rate_series, axis=0)
        )
        # min_count=1 (not plain .sum()'s default skipna=True/min_count=0):
        # a NaN rate (genuinely missing RWI for that tile, kept as NaN above,
        # not silently zero-filled) must propagate to a NaN total rather than
        # being silently dropped from the sum -- the same "don't let a missing
        # rate look like a confirmed 0" reasoning the admin-level aggregation's
        # own sum(min_count=1) elsewhere in this file applies.
        pin        = df_member['population'].sum(min_count=1)
        infant     = df_member['infant_population'].sum(min_count=1)
        school_age = df_member['school_age_population'].sum(min_count=1)
        adolescent = df_member['adolescent_population'].sum(min_count=1)
        chin       = infant + school_age + adolescent

        rows.append({
            'zone_id': member,
            'severity_people_in_need': pin,
            'severity_children_in_need': chin,
            'severity_infant_in_need': infant,
            'severity_school_age_in_need': school_age,
            'severity_adolescent_in_need': adolescent,
        })

    result = pd.DataFrame(rows)
    return result[['zone_id'] + out_cols]


def calculate_tile_member_bitmask(gdf_envelopes, gdf_tiles, threshold_column='wind_threshold',
                                  label='tile'):
    """
    Real per-z14-tile, per-ensemble-member 64-bit coverage bitmask, one
    DataFrame per real threshold in gdf_envelopes (bit `m-1` set <=> member
    `m`'s envelope covers that tile).

    The envelope-vs-tile spatial join this needs is also computed transiently,
    twice, inside create_tracks_view_from_envelopes()/
    create_mercator_view_from_envelopes() (once buried inside gigaspatial's
    own internal sjoin, once as a first-party one), but both discard the
    per-member identity as soon as it collapses to a country-wide
    scalar/tile-count. This function does the same kind of sjoin (mirrors
    calculate_vulnerability_tracks's own per-threshold block above almost
    exactly) but keeps the per-member identity instead of discarding it,
    OR-accumulating each member's own bit into a compact uint64 per tile,
    the same technique the dashboard repo's own _RiverExtentCache uses for
    River's raw flood-extent layer, applied here to Wind/Gust.

    Args:
        gdf_envelopes: GeoDataFrame with columns ensemble_member,
                       `threshold_column`, geometry.
        gdf_tiles: GeoDataFrame: base mercator tiles, needs only 'tile_id'
                   and 'geometry'.
        threshold_column: 'wind_threshold' or 'gust_threshold'.
        label: call-site tag ('wind tile' / 'gust tile') echoed in every
               warning this function emits, so an operator can tell which
               output is affected; different call sites can otherwise emit
               the identical warning string for the same threshold in the
               same run.

    Returns:
        dict {threshold: DataFrame}, one entry per real threshold present in
        gdf_envelopes. Each DataFrame has columns ['tile_id', 'bits']
        (bits: uint64), one row per distinct tile with >=1 member's envelope
        covering it, a tile with zero coverage from every member simply
        doesn't appear, the same sparse convention River's own bitmask uses
        (real, not a fabricated 0 row). A threshold with a real
        zero-intersection result (e.g. no member reaches 137kt over land for
        this storm) returns an empty DataFrame for that threshold, not an
        error; mirrors create_tracks_view_from_envelopes's own
        zero-intersection fallback.

        A threshold whose sjoin actually fails maps to None, not to an empty
        DataFrame: the two are distinguishable downstream, where an empty
        DataFrame means a legitimate 'no member reaches this threshold' and
        None means the sjoin itself failed and no file should be written for
        it; otherwise a self-intersecting geometry could silently persist a
        zero-row parquet indistinguishable from a real zero-hazard result.
        Callers MUST skip a None (see the two call sites in
        create_views_from_envelopes_in_country, which skip writing a file for
        it). 'tile_id' is also emitted with the SAME pandas 'string' dtype in
        the empty and non-empty cases, so both file variants share one
        parquet schema.
    """
    tiles_geom = gdf_tiles[['tile_id', 'geometry']].copy()
    thresholds = sorted(gdf_envelopes[threshold_column].unique())
    bitmask_views = {}

    for th in thresholds:
        envs_th = gdf_envelopes[gdf_envelopes[threshold_column] == int(th)][
            ['ensemble_member', 'geometry']
        ].copy()
        if envs_th.empty:
            bitmask_views[th] = pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})
            continue
        try:
            # The CRS check/reprojection sits inside this try block so a
            # naive-geometry CRS mismatch (gdf_tiles.crs is None or
            # gdf_envelopes.crs is None) is caught here rather than
            # propagating out of the unwrapped call site in
            # create_views_from_envelopes_in_country(), which would otherwise
            # lose that country's JSON report (and, for the wind call, the
            # entire gust section that runs after it). In practice CRS is
            # always EPSG:4326 for both inputs (hardcoded in
            # convert_envelopes_to_geodataframe / geoparquet tiles), so this
            # is defensive hardening rather than a live failure mode; it
            # shares the same zero-intersection fallback the sjoin itself
            # already has below.
            if tiles_geom.crs != envs_th.crs:
                tiles_geom_proj = tiles_geom.to_crs(envs_th.crs)
            else:
                tiles_geom_proj = tiles_geom
            joined = gpd.sjoin(tiles_geom_proj, envs_th, how='inner', predicate='intersects')
            if 'tile_id' not in joined.columns:
                joined = joined.reset_index()
        except Exception as e:
            # None (not an empty frame) so the caller can tell a real
            # failure from a legitimate zero-intersection; see this
            # function's own Returns section.
            logger.error(f"tile_member_bitmask [{label}] sjoin FAILED at {threshold_column}={th}, "
                         f"no bitmask file will be written for it: {e}")
            bitmask_views[th] = None
            continue

        if joined.empty:
            bitmask_views[th] = pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})
            continue

        # Per-row bit for that row's own member, OR-reduced per tile,
        # vectorized (no per-row Python loop), same np.unique(...,
        # return_inverse=True) + np.bitwise_or.at pattern the dashboard
        # repo's own _RiverExtentCache already uses for its per-batch
        # bitmask reduction.
        member_arr = joined['ensemble_member'].to_numpy().astype(np.uint64)
        bits = np.uint64(1) << (member_arr - np.uint64(1))
        tile_ids = joined['tile_id'].to_numpy()
        uniq_tiles, inverse = np.unique(tile_ids, return_inverse=True)
        tile_bits = np.zeros(len(uniq_tiles), dtype=np.uint64)
        np.bitwise_or.at(tile_bits, inverse, bits)

        bitmask_views[th] = pd.DataFrame({
            # Same pandas 'string' dtype the empty-threshold frames above
            # use, so every parquet this function feeds shares one schema.
            'tile_id': pd.array(uniq_tiles.astype(str), dtype='string'),
            'bits': tile_bits,
        })

    return bitmask_views


# NOTE: there is deliberately NO per-ADMIN-REGION member bitmask here.
#
# Running calculate_tile_member_bitmask against admin polygons instead of z14
# tiles (bit `m` set <=> member `m`'s envelope intersects the region ANYWHERE)
# is a real quantity, but it is not the one the admin layer renders.
# create_admin_view_from_envelopes_new
# defines an admin region's `probability` as the AREA-MEAN of its z14 tiles'
# probabilities ('probability' sits in avg_cols and is aggregated with
# "mean"), and its E_* columns as sum_over_tiles(raw_tile x prob_tile). So
# popcount(bits)/51 does NOT reproduce the admin layer's own probability the
# way the z14 bitmask exactly reproduces the tile layer's; for a large
# admin-1 that a cyclone only clips it overstates by 10-100x, and the
# dashboard's combined layer could read HIGHER than every single-hazard layer
# it was built from.
#
# There is no per-region summary that fixes this: an exact cross-hazard union
# needs per-member per-TILE sets, which is exactly what the z14 bitmask
# (calculate_tile_member_bitmask above, TILE_WIND_BITMASK_MAT /
# TILE_GUST_BITMASK_MAT) already carries. The dashboard therefore unions at
# z14 and aggregates DOWN to admin regions with the same two aggregations
# used here (mean for probability, raw-weighted sum for E_*), which also
# covers river/precip for free; see services/tile_server.py's own
# _combine_bitmask_aware_admin in the Ahead-of-the-Storm repo.


# =============================================================================
# MAIN IMPACT ANALYSIS ORCHESTRATION
# Top-level function called per country per storm on every --type update run.
# Coordinates all view generation, CCI calculation, and report writing.
# =============================================================================
def create_views_from_envelopes_in_country(country, storm, date, gdf_envelopes, zoom, gdf_envelopes_gust=None):
    """
    Create and save all impact views for a country from hurricane envelopes.

    This is the main orchestration function that processes a single country for a given
    storm forecast. It creates and saves:
    - School impact views (probability per wind threshold)
    - Health center impact views (probability per wind threshold)
    - Shelter impact views (probability per wind threshold)
    - WASH facility impact views (probability per wind threshold)
    - Tile impact views (expected impacts per tile per wind threshold)
    - Admin level impact views (for every admin level that has a base parquet)
    - Child Cyclone Index (CCI) views (both tile and admin level)
    - Track views (severity metrics per ensemble member)
    - JSON impact report
    - Gust envelope core exposure views (school/HC/shelter/WASH/tiles/admin/tracks
      only, if gdf_envelopes_gust is provided; no CCI/vulnerability/report for gust)

    Args:
        country: ISO3 country code
        storm: Storm name (e.g., 'FUNG-WONG')
        date: Forecast date in YYYYMMDDHHMMSS format (e.g., '20251110000000')
        gdf_envelopes: GeoDataFrame containing hurricane envelope geometries
        zoom: Zoom level for mercator tiles
        gdf_envelopes_gust: Optional GeoDataFrame of gust envelope geometries
            (same shape as gdf_envelopes, 'gust_threshold' column instead of
            'wind_threshold'). None or empty means no gust data for this
            storm/forecast, gust views are skipped, wind views are unaffected.

    Note:
        Base data (mercator tiles, admin views) are loaded if available, or created
        on-the-fly if missing. Admin levels are detected from existing base parquets
        created during --type initialize. Add new levels with --type patch --columns adminN.

    Returns:
        tuple[bool, int]: (wrote_base_parquet, files_written). files_written is a
            real running count of impact files actually saved this call (varies
            with active wind thresholds, admin levels, and gust presence), not a
            fixed placeholder, feeds TC_PIPELINE_RUN_LOG/TC_PIPELINE_COMPLETE_LOG's
            FILES_WRITTEN column.
    """
    admin_levels = get_initialized_admin_levels(country)
    if not admin_levels:
        # Fallback: ensure admin1 is always processed (creates on-the-fly if missing)
        admin_levels = [1]

    # Track whether any base parquets were written during this run (emergency fallbacks).
    # Returned to the caller so it can call REFRESH_BASE_LAYER_TABLES() if needed.
    wrote_base_parquet = False

    # Running count of impact files actually written this call. Feeds
    # TC_PIPELINE_RUN_LOG/TC_PIPELINE_COMPLETE_LOG's FILES_WRITTEN column,
    # which varies with actual output (number of active wind thresholds, admin
    # levels, gust presence).
    files_written = 0

    # Remove all existing output files for this country/storm/forecast run before writing
    # new ones. This prevents stale threshold files (e.g. from a run where 137kt had a
    # few envelope members that have since been cleaned up) from persisting on the stage.
    prefix = f"{country}_{storm}_{date}_"
    for view_dir in ('school_views', 'hc_views', 'shelter_views', 'wash_views',
                     'mercator_views', 'admin_views', 'track_views', 'track_tile_bitmask_views',
                     'school_views_gust', 'hc_views_gust', 'shelter_views_gust', 'wash_views_gust',
                     'mercator_views_gust', 'admin_views_gust', 'track_views_gust', 'track_tile_bitmask_views_gust'):
        dir_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, view_dir)
        try:
            existing = data_store.list_files(dir_path)
            for f in existing:
                fname = os.path.basename(f)
                if fname.startswith(prefix):
                    data_store.remove(f)
                    logger.debug(f"Removed stale file: {f}")
        except Exception as e:
            logger.warning(f"Could not clean up {view_dir} for {country}/{storm}/{date}: {e}")

    logger.info(f"  Processing {country}...")

    # Schools
    logger.info(f"    Processing schools...")
    gdf_schools = fetch_schools(country, rewrite=0)

    wind_school_views = create_school_view_from_envelopes(gdf_schools, gdf_envelopes)
    for wind_th in wind_school_views:
        save_school_view(wind_school_views[wind_th], country, storm, date, wind_th)
    files_written += len(wind_school_views)
    logger.info(f"    Created {len(wind_school_views)} school views")

    # Health centers
    logger.info(f"    Processing health centers...")
    gdf_hcs = fetch_health_centers(country, rewrite=0)
    wind_hc_views = create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes)
    for wind_th in wind_hc_views:
        save_hc_view(wind_hc_views[wind_th], country, storm, date, wind_th)
    files_written += len(wind_hc_views)
    logger.info(f"    Created {len(wind_hc_views)} health center views")

    # Shelters
    logger.info(f"    Processing shelters...")
    gdf_shelters = fetch_shelters(country, rewrite=0)
    wind_shelter_views = create_shelter_view_from_envelopes(gdf_shelters, gdf_envelopes)
    for wind_th in wind_shelter_views:
        save_shelter_view(wind_shelter_views[wind_th], country, storm, date, wind_th)
    files_written += len(wind_shelter_views)
    logger.info(f"    Created {len(wind_shelter_views)} shelter views")

    # WASH
    logger.info(f"    Processing WASH...")
    gdf_wash = fetch_wash(country, rewrite=0)
    wind_wash_views = create_wash_view_from_envelopes(gdf_wash, gdf_envelopes)
    for wind_th in wind_wash_views:
        save_wash_view(wind_wash_views[wind_th], country, storm, date, wind_th)
    files_written += len(wind_wash_views)
    logger.info(f"    Created {len(wind_wash_views)} WASH views")

    # Tiles
    logger.info(f"    Processing tiles...")
    try:
        gdf_tiles = load_mercator_view(country, zoom)
        logger.info(f"    Loaded existing mercator tiles: {len(gdf_tiles)} tiles")
        # Ensure admin IDs are present (in case file was created without them)
        if 'id' not in gdf_tiles.columns:
            logger.error(f"    {country}: Mercator view missing admin IDs, adding on-the-fly. Run --type initialize to fix permanently.")
            gdf_tiles, _ = add_admin_ids(gdf_tiles, country)
            save_mercator_view(gdf_tiles, country, zoom)
            wrote_base_parquet = True
    except Exception as e:
        logger.error(f"    {country}: Mercator view missing, creating on-the-fly ({e}). Run --type initialize first.")
        view = create_mercator_country_layer(country, zoom, rewrite=0)
        gdf_tiles, _ = add_admin_ids(view, country)
        save_mercator_view(gdf_tiles, country, zoom)
        wrote_base_parquet = True
        logger.info(f"    Created and saved base mercator tiles: {len(gdf_tiles)} tiles")

    wind_tiles_views = create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes)
    for wind_th in wind_tiles_views:
        save_tiles_view(wind_tiles_views[wind_th], country, storm, date, wind_th, zoom)
    files_written += len(wind_tiles_views)
    logger.info(f"    Created {len(wind_tiles_views)} tile views")

    # CCI for tiles
    cci_tiles_view = calculate_ccis(wind_tiles_views, gdf_tiles)
    save_cci_tiles(cci_tiles_view, country, storm, date, zoom)
    files_written += 1

    # Vulnerability (people/children in need): wind-dependent poverty weighting
    vuln_tiles_view = calculate_vulnerability_view(wind_tiles_views, gdf_tiles)
    save_vulnerability_tiles(vuln_tiles_view, country, storm, date, zoom)
    files_written += 1

    # Per-member vulnerability: analogous to per-threshold track parquets
    vuln_tracks_view = calculate_vulnerability_tracks(gdf_envelopes, gdf_tiles)
    save_vulnerability_tracks(vuln_tracks_view, country, storm, date, zoom)
    files_written += 1
    logger.info(f"    Created vulnerability tracks view ({len(vuln_tracks_view)} members)")

    # Admins: one pass per requested admin level
    logger.info(f"    Processing admins (levels: {admin_levels})...")
    # Captured per level so the gust admin pass below can reuse them without
    # recomputing admins_overlay() a second time.
    gdf_admin_by_level = {}
    gdf_tiles_for_admin_by_level = {}
    for admin_level in admin_levels:
        try:
            gdf_admin = load_admin_view(country, admin_level=admin_level)
            logger.info(f"    Loaded existing admin{admin_level}: {len(gdf_admin)} regions")
        except Exception as e:
            logger.error(f"    {country}: Admin{admin_level} view missing, creating on-the-fly ({e}). Run --type initialize first.")
            gdf_admin = create_admin_country_layer(country, rewrite=0, admin_level=admin_level)
            save_admin_view(gdf_admin, country, admin_level=admin_level)
            wrote_base_parquet = True
            logger.info(f"    Created and saved base admin{admin_level}: {len(gdf_admin)} regions")

        # For admin level 1, gdf_tiles already has 'id' = admin1 IDs (from mercator parquet).
        # For other levels, derive the tile mapping from the already-loaded admin parquet
        # (which stores boundary geometries), avoids a redundant GeoRepo API call.
        if admin_level == 1:
            gdf_tiles_for_admin = gdf_tiles
        else:
            gdf_admin_boundaries = gdf_admin[['tile_id', 'geometry']].rename(columns={'tile_id': 'id'})
            gdf_tiles_for_admin = admins_overlay(gdf_admin_boundaries,
                                                 gdf_tiles.drop(columns=['id'], errors='ignore'))

        gdf_admin_by_level[admin_level] = gdf_admin
        gdf_tiles_for_admin_by_level[admin_level] = gdf_tiles_for_admin

        wind_admin_views = create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles_for_admin, gdf_envelopes)
        for wind_th in wind_admin_views:
            save_admin_tiles_view(wind_admin_views[wind_th], country, storm, date, wind_th,
                                  admin_level=admin_level)
        files_written += len(wind_admin_views)
        logger.info(f"    Created {len(wind_admin_views)} admin{admin_level} views")

        # CCI for this admin level
        # min_count=1 (not plain "sum"): matches vagg_dict below, a country whose
        # tile-level CCI columns are entirely NaN (e.g. an old mercator parquet
        # pre-dating the WorldPop age-structure columns) must roll up to NaN at
        # admin level too, not silently to 0 ("zero children impacted" when the
        # real state is "data unavailable, needs --type patch").
        agg_dict = {col: (lambda x: x.sum(min_count=1)) for col in sum_cols_cci}
        agg = cci_tiles_view.copy()
        if admin_level != 1:
            # Map quadkey tile IDs (zone_id) to this admin level's ucodes.
            # agg['id'] holds admin1 ucodes at this point, not quadkeys, so we
            # must re-derive from zone_id which is the original quadkey.
            id_map = gdf_tiles_for_admin.set_index('tile_id')['id'].to_dict()
            agg['id'] = agg['zone_id'].map(id_map)
        agg = agg.groupby("id").agg(agg_dict).reset_index()
        cci_admin_view = agg.rename(columns={'id': 'tile_id'})
        save_cci_admin(cci_admin_view, country, storm, date, admin_level=admin_level)
        files_written += 1

        # Vulnerability for this admin level
        vagg_dict = {col: lambda x: x.sum(min_count=1) for col in sum_cols_vulnerability}
        vagg = vuln_tiles_view.copy()
        if admin_level != 1:
            id_map = gdf_tiles_for_admin.set_index('tile_id')['id'].to_dict()
            vagg['id'] = vagg['zone_id'].map(id_map)
        vagg = vagg.groupby("id").agg(vagg_dict).reset_index()
        vuln_admin_view = vagg.rename(columns={'id': 'tile_id'})
        save_vulnerability_admin(vuln_admin_view, country, storm, date, admin_level=admin_level)
        files_written += 1

    # Keep a reference to admin1 for the JSON report (always in admin_levels or generated above)
    try:
        gdf_admin = load_admin_view(country, admin_level=1)
    except Exception:
        gdf_admin = create_admin_country_layer(country, rewrite=0, admin_level=1)

    agg_dict = {col: (lambda x: x.sum(min_count=1)) for col in sum_cols_cci}
    agg = cci_tiles_view.groupby("id").agg(agg_dict).reset_index()
    cci_admin_view = agg.rename(columns={'id': 'tile_id'})

    # Re-load admin1 wind views for the report (already saved above)
    wind_admin_views = create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles, gdf_envelopes)

    # Tracks
    logger.info(f"    Processing tracks...")
    wind_tracks_views = create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member', gdf_shelters=gdf_shelters, gdf_wash=gdf_wash)
    for wind_th in wind_tracks_views:
        save_tracks_view(wind_tracks_views[wind_th], country, storm, date, wind_th)
    files_written += len(wind_tracks_views)
    logger.info(f"    Created {len(wind_tracks_views)} track views")

    # Per-tile, per-member coverage bitmask: see calculate_tile_member_bitmask's
    # own docstring for the full "why".
    wind_bitmask_views = calculate_tile_member_bitmask(gdf_envelopes, gdf_tiles,
                                                       threshold_column='wind_threshold',
                                                       label='wind tile')
    # A None entry means that threshold's sjoin genuinely FAILED (see
    # calculate_tile_member_bitmask's own Returns section), skip it rather
    # than persisting a zero-row parquet that downstream cannot tell apart
    # from a real 'no member reaches this threshold'.
    wind_bitmask_written = 0
    for wind_th, bm in wind_bitmask_views.items():
        if bm is None:
            continue
        save_tracks_tile_bitmask_view(bm, country, storm, date, wind_th)
        wind_bitmask_written += 1
    files_written += wind_bitmask_written
    logger.info(f"    Created {wind_bitmask_written} tile-member bitmask views")

    df_tracks = get_tracks(date, storm)
    gdf_tracks = convert_to_geodataframe(df_tracks)

    json_report = do_report(wind_school_views, wind_hc_views, wind_tiles_views, wind_admin_views, cci_tiles_view, cci_admin_view, gdf_admin, gdf_tracks, country, storm, date, wind_shelter_views=wind_shelter_views, wind_wash_views=wind_wash_views, vulnerability_tiles_view=vuln_tiles_view)
    save_json_report(json_report, country, storm, date)
    files_written += 1

    # --- Gust envelopes (optional, core exposure views only) ---
    # No CCI, vulnerability, or JSON report for gust
    # Wrapped in its own try/except: wind processing above
    # has already fully succeeded and saved, an uncaught exception here must not
    # propagate to the per-country try/except in run_complete_impact_analysis(),
    # which would otherwise discard the wind results from this same call.
    if gdf_envelopes_gust is not None and not gdf_envelopes_gust.empty:
        try:
            logger.info(f"    Processing gust envelopes ({len(gdf_envelopes_gust)} records)...")

            gust_school_views = create_school_view_from_envelopes(gdf_schools, gdf_envelopes_gust, threshold_column='gust_threshold')
            for gth in gust_school_views:
                save_school_view(gust_school_views[gth], country, storm, date, gth, dataset='gust')
            files_written += len(gust_school_views)

            gust_hc_views = create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes_gust, threshold_column='gust_threshold')
            for gth in gust_hc_views:
                save_hc_view(gust_hc_views[gth], country, storm, date, gth, dataset='gust')
            files_written += len(gust_hc_views)

            gust_shelter_views = create_shelter_view_from_envelopes(gdf_shelters, gdf_envelopes_gust, threshold_column='gust_threshold')
            for gth in gust_shelter_views:
                save_shelter_view(gust_shelter_views[gth], country, storm, date, gth, dataset='gust')
            files_written += len(gust_shelter_views)

            gust_wash_views = create_wash_view_from_envelopes(gdf_wash, gdf_envelopes_gust, threshold_column='gust_threshold')
            for gth in gust_wash_views:
                save_wash_view(gust_wash_views[gth], country, storm, date, gth, dataset='gust')
            files_written += len(gust_wash_views)

            gust_tiles_views = create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes_gust, threshold_column='gust_threshold')
            for gth in gust_tiles_views:
                save_tiles_view(gust_tiles_views[gth], country, storm, date, gth, zoom, dataset='gust')
            files_written += len(gust_tiles_views)

            for admin_level in admin_levels:
                gust_admin_views = create_admin_view_from_envelopes_new(
                    gdf_admin_by_level[admin_level], gdf_tiles_for_admin_by_level[admin_level],
                    gdf_envelopes_gust, threshold_column='gust_threshold')
                for gth in gust_admin_views:
                    save_admin_tiles_view(gust_admin_views[gth], country, storm, date, gth,
                                          admin_level=admin_level, dataset='gust')
                files_written += len(gust_admin_views)

            gust_tracks_views = create_tracks_view_from_envelopes(
                gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes_gust, index_column='ensemble_member',
                gdf_shelters=gdf_shelters, gdf_wash=gdf_wash, threshold_column='gust_threshold')
            for gth in gust_tracks_views:
                save_tracks_view(gust_tracks_views[gth], country, storm, date, gth, dataset='gust')
            files_written += len(gust_tracks_views)

            # Same as wind's own bitmask above: see
            # calculate_tile_member_bitmask's own docstring.
            gust_bitmask_views = calculate_tile_member_bitmask(gdf_envelopes_gust, gdf_tiles,
                                                               threshold_column='gust_threshold',
                                                               label='gust tile')
            # Same skip-on-None contract as the wind call site above.
            for gth, bm in gust_bitmask_views.items():
                if bm is None:
                    continue
                save_tracks_tile_bitmask_view(bm, country, storm, date, gth, dataset='gust')
                files_written += 1

            logger.info(f"    Created gust views ({len(gust_school_views)} thresholds)")
        except Exception as e:
            logger.warning(f"    Gust envelope processing failed for {country}/{storm}/{date}, gust views skipped this run, wind views unaffected: {e}")
    else:
        logger.info(f"    No gust envelope data for {country}/{storm}/{date}, skipping gust views")

    return wrote_base_parquet, files_written


# =============================================================================
# SNOWFLAKE DATA LOADING
# =============================================================================
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
        # Get envelope data (Snowflake or LOCAL/BLOB per config.HAZARD_DATA_SOURCE)
        df_envelopes = get_envelopes(storm, forecast_time)

        if df_envelopes.empty:
            logger.error(f"No envelope data found for {storm} at {forecast_time}")
            return pd.DataFrame()

        # Convert to GeoDataFrame
        gdf_envelopes = convert_envelopes_to_geodataframe(df_envelopes)
        return gdf_envelopes

    except Exception as e:
        logger.error(f"Error loading envelopes: {str(e)}")
        return pd.DataFrame()

def load_gust_envelopes_from_snowflake(storm, date):
    """
    Load gust envelope data directly from Snowflake (mirrors load_envelopes_from_snowflake).

    Gust data is optional per storm/forecast (upstream extraction may not have
    produced a gust polygon, or the storm may be too weak for any gust threshold
    to register). Uses info/warning logging rather than error, and always returns
    an empty DataFrame rather than raising, so callers can treat "no gust data"
    as a normal, expected outcome rather than a failure.
    """
    if len(date) == 14:  # YYYYMMDDHHMMSS format
        dt = pd.to_datetime(date, format="%Y%m%d%H%M%S")
        forecast_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        forecast_time = date

    try:
        df_gust_envelopes = get_gust_envelopes(storm, forecast_time)

        if df_gust_envelopes.empty:
            logger.info(f"No gust envelope data found for {storm} at {forecast_time}")
            return pd.DataFrame()

        gdf_gust_envelopes = convert_envelopes_to_geodataframe(df_gust_envelopes)
        return gdf_gust_envelopes

    except Exception as e:
        logger.warning(f"Error loading gust envelopes: {str(e)}")
        return pd.DataFrame()