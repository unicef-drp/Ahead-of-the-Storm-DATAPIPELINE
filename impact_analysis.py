#!/usr/bin/env python3
"""
Impact Analysis Module

Core geospatial engine for the Ahead of the Storm pipeline. Imported and driven by
main_pipeline.py — not intended to be run directly.

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
  stage — controlled by DATA_PIPELINE_DB env var

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
    get_snowflake_tracks,
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

# Continuous index columns — averaged (not summed) when aggregating across tiles.
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
avg_cols_admin = [
    'smod_class',       # Mean SMOD L2 class across tiles in admin unit
    'smod_class_l1',    # Mean SMOD L1 class
    'rwi',              # Mean RWI across tiles in admin unit
]

# CCI columns written to tile and admin CCI views.
sum_cols_cci = [
    'CCI_children',    'E_CCI_children',
    'CCI_school_age',  'E_CCI_school_age',
    'CCI_infants',     'E_CCI_infants',
    'CCI_adolescents',    'E_CCI_adolescents',
    'CCI_pop',         'E_CCI_pop',
]
# Configuration constants
BUFFER_DISTANCE_METERS = 150  # Buffer distance for schools and health centers (meters)
WORLDPOP_RESOLUTION_HIGH = 1000  # High resolution for WorldPop data (meters)
WORLDPOP_RESOLUTION_LOW = 100  # Low resolution for WorldPop data (meters)
SCHOOL_AGE_MIN = 5   # GR2 min_age: picks _05_ (5–9y) and _10_ (10–14y) bands
SCHOOL_AGE_MAX = 10  # GR2 max_age: picks _05_ (5–9y) and _10_ (10–14y) bands
INFANT_AGE_MIN = 0   # GR2 min_age: picks _00_ (0–12mo) and _01_ (1–4y) bands
INFANT_AGE_MAX = 1   # GR2 max_age: picks _00_ (0–12mo) and _01_ (1–4y) bands
ADOLESCENT_AGE_MIN = 15  # GR2 min_age: picks _15_ (15–19y) band
ADOLESCENT_AGE_MAX = 15  # GR2 max_age: picks _15_ (15–19y) band — 1 file only
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
# Other values may exist in the API — all are retained regardless.


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
# See custom_data/README.md for file formats and upload instructions.
CUSTOM_DATA_DIR = os.path.join(ROOT_DATA_DIR, 'custom')

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
        kind: File type identifier — 'schools', 'health_centers', 'shelters', 'wash',
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

    Custom point files must follow the schema in custom_data/README.md. The file is
    read from the data store at geodb/custom/<COUNTRY>_<kind>.csv. If the file is
    absent, None is returned and the caller falls back to API fetching. If the file
    exists but is invalid (missing required columns or unreadable), a ValueError is
    raised — the pipeline does not silently fall back to the API when a custom file
    is present.

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
                logger.info(f"{country}: No ID column in custom {kind} CSV — auto-generated sequential IDs")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df['longitude'], df['latitude']),
            crs='EPSG:4326'
        )
        logger.info(f"{country}: Loaded {len(gdf)} custom {kind} from '{path}' (custom data — API skipped, --rewrite has no effect)")
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

    Custom tile files must follow the schema in custom_data/README.md. The tile_id
    column must contain mercator quadkeys at the specified zoom level matching the
    country's base mercator parquet. Empty cells are interpreted as NaN.

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
        logger.info(f"{country}: Loaded custom {kind} (zoom={zoom}) from '{path}' ({len(df)} tiles, custom data — raster processing skipped)")
        return df.set_index('tile_id')
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(
            f"{country}: Failed to read custom {kind} CSV at '{path}': {e}. "
            f"Fix the file or remove it to fall back to raster processing."
        ) from e


# =============================================================================
# DATA FETCHING AND CACHING FUNCTIONS
# =============================================================================
def fetch_health_centers(country, rewrite=0):
    """
    Fetch health center locations for a country, using custom data or cache if available.

    Priority order:
        1. Custom file at geodb/custom/<COUNTRY>_health_centers.csv — always used if present;
           --rewrite has no effect on custom data. Custom data is never overwritten.
        2. Location cache (hc_views/<COUNTRY>_health_centers.parquet) — if rewrite=0.
        3. HealthSites.io API — fetched if no cache or rewrite=1.

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
    # 1. Custom data takes priority — never overwritten
    custom_gdf = _load_custom_points_csv(country, 'health_centers')
    if custom_gdf is not None:
        save_hc_locations(custom_gdf, country)
        return custom_gdf

    # 2. Use cache if available and rewrite not requested
    if hc_exist(country) and rewrite == 0:
        return load_hc_locations(country)

    # 3. Fetch from HealthSites.io API
    try:
        gdf_hcs = HealthSitesFetcher(country=country).fetch_facilities(output_format='geojson')
        if gdf_hcs.empty or 'geometry' not in gdf_hcs.columns:
            logger.warning(f"{country}: HealthSites API returned no data")
            return gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
        gdf_hcs = gdf_hcs.set_crs(4326)
        logger.info(f"{country}: Fetched {len(gdf_hcs)} health facilities from HealthSites API "
                    f"(all types cached; filtering to {HC_FACILITY_TYPES} happens at analysis time)")
        save_hc_locations(gdf_hcs, country)
        return gdf_hcs
    except Exception as e:
        if '403' in str(e):
            logger.error(f"{country}: HealthSites API returned 403 Forbidden — likely daily rate limit "
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
        1. Custom file at geodb/custom/<COUNTRY>_schools.csv — always used if present;
           --rewrite has no effect on custom data. Custom data is never overwritten.
        2. Location cache (school_views/<COUNTRY>_schools.parquet) — if rewrite=0.
        3. GIGA school location API — fetched if no cache or rewrite=1.

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
    # 1. Custom data takes priority — never overwritten
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
                logger.warning(f"{country}: Cached school data invalid — re-fetching from API")
                # Fall through to API fetch below
                gdf_schools = None
        if gdf_schools is not None:
            return gdf_schools

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
        if rewrite == 1 or not gdf_schools.empty:
            save_school_locations(gdf_schools, country)
        return gdf_schools
    except Exception as e:
        logger.error(f"{country}: Error fetching schools from GIGA API: {e}")
        return gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')


def fetch_shelters(country, rewrite=0):
    """
    Fetch emergency shelter locations for a country using custom data, cache, or OSM.

    Priority order:
        1. Custom file at geodb/custom/<COUNTRY>_shelters.csv — always used if present.
        2. Location cache (shelter_views/<COUNTRY>_shelters.parquet) — if rewrite=0.
        3. OSM via Overpass API using SHELTER_LOCATION_TYPES (social_facility=shelter).

    Args:
        country: ISO3 country code
        rewrite: If 1, re-fetch from OSM and overwrite cache (ignored if custom file exists)

    Returns:
        gpd.GeoDataFrame: Shelter locations with geometry (EPSG:4326) and 'osm_id' column.
    """
    # 1. Custom data takes priority — never overwritten
    custom_gdf = _load_custom_points_csv(country, 'shelters')
    if custom_gdf is not None:
        save_shelter_locations(custom_gdf, country)
        return custom_gdf

    # 2. Use cache if available and rewrite not requested
    if shelter_exist(country) and rewrite == 0:
        return load_shelter_locations(country)

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
        1. Custom file at geodb/custom/<COUNTRY>_wash.csv — always used if present.
        2. Location cache (wash_views/<COUNTRY>_wash.parquet) — if rewrite=0.
        3. OSM via Overpass API using WASH_LOCATION_TYPES.

    Fetches: drinking_water, water_point, toilets, shower (amenity) and
    water_well, water_tap, water_works, pumping_station, wastewater_treatment_plant (man_made).
    Natural sources (springs) are excluded — only managed infrastructure.

    Args:
        country: ISO3 country code
        rewrite: If 1, re-fetch from OSM and overwrite cache (ignored if custom file exists)

    Returns:
        gpd.GeoDataFrame: WASH facility locations with geometry (EPSG:4326) and 'osm_id' column.
    """
    # 1. Custom data takes priority — never overwritten
    custom_gdf = _load_custom_points_csv(country, 'wash')
    if custom_gdf is not None:
        save_wash_locations(custom_gdf, country)
        return custom_gdf

    # 2. Use cache if available and rewrite not requested
    if wash_exist(country) and rewrite == 0:
        return load_wash_locations(country)

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

    Data requirements — **hard failures** (init aborts if unavailable):
        - Total population (WorldPop 1km)
        - School-age population (WorldPop 100m, age_structures)
        - Infant population (WorldPop 100m, age_structures)
        - Under-18 population (WorldPop 100m, age_structures)

    Data requirements — **optional** (NaN/0 if unavailable; backfill with --type patch):
        - GHSL built surface (built_surface_m2)
        - SMOD settlement class L2 (smod_class) and derived L1 (smod_class_l1)
        - Relative Wealth Index (rwi)
        - Schools (num_schools) — 0 if API fails or no data
        - Health centers (num_hcs) — 0 if API fails or no data
        - Emergency shelters (num_shelters) — 0 if OSM returns nothing
        - WASH facilities (num_wash) — 0 if OSM returns nothing

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
    # Population (hard requirement — raises if neither custom nor raster)
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
                # Map school-age population (5–14y) — hard requirement, raises on failure
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
                # Map infant population (0–4y) — hard requirement, raises on failure
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
                # Map adolescent population (15–19y) — hard requirement, raises on failure
                # GR2: picks _15_ band only (15–19y), sex='T' for combined total — 1 file
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
                # Map total population — hard requirement, raises on failure
                tiles_viewer.map_wp_pop(country=country, resolution=100)
                break
            except RuntimeError as _e:
                if _wp_attempt < _wp_attempts - 1:
                    logger.warning(f"{country}: WorldPop download incomplete (attempt {_wp_attempt + 1}/{_wp_attempts}), retrying in 5s: {_e}")
                    _time.sleep(5)
                else:
                    raise

    # ------------------------------------------------------------------
    # GHSL built surface — optional, NaN fallback, custom override supported
    # ------------------------------------------------------------------
    custom_built = _load_custom_tiles_csv(country, 'built_surface', zoom_level)
    if custom_built is not None and 'built_surface_m2' in custom_built.columns:
        tiles_viewer.add_variable_to_view(custom_built['built_surface_m2'].to_dict(), 'built_surface_m2')
    else:
        try:
            tiles_viewer.map_built_s()
        except Exception as e:
            logger.warning(f"{country}: GHSL built surface unavailable — setting to NaN: {e}")
            tiles_viewer.add_variable_to_view(
                {k: np.nan for k in tiles_viewer.view.index.unique()}, 'built_surface_m2'
            )

    # ------------------------------------------------------------------
    # SMOD settlement class — optional, NaN fallback, custom override supported
    # ------------------------------------------------------------------
    custom_smod = _load_custom_tiles_csv(country, 'smod', zoom_level)
    if custom_smod is not None and 'smod_class' in custom_smod.columns:
        tiles_viewer.add_variable_to_view(custom_smod['smod_class'].to_dict(), 'smod_class')
    else:
        try:
            tiles_viewer.map_smod()
        except Exception as e:
            logger.warning(f"{country}: GHSL SMOD unavailable — setting to NaN: {e}")
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
    schools = tiles_viewer.map_points(points=gdf_schools)
    tiles_viewer.add_variable_to_view(schools, "num_schools")
    
    hcs = tiles_viewer.map_points(points=gdf_hcs)
    tiles_viewer.add_variable_to_view(hcs, "num_hcs")
    
    shelters = tiles_viewer.map_points(points=gdf_shelters)
    tiles_viewer.add_variable_to_view(shelters, "num_shelters")
    
    wash_pts = tiles_viewer.map_points(points=gdf_wash)
    tiles_viewer.add_variable_to_view(wash_pts, "num_wash")

    # ------------------------------------------------------------------
    # RWI — optional, NaN fallback, custom override supported
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
            logger.warning(f"{country}: Relative Wealth Index unavailable — setting to NaN: {e}")
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
    # with any admin — typically ocean or far-offshore tiles)
    still_unassigned = assigned[assigned["id"].isna()]["tile_id"]
    if len(still_unassigned) > 0:
        logger.debug(
            f"admins_overlay: {len(still_unassigned)} tiles unassigned after centroid "
            "and area steps — applying nearest-neighbour fallback"
        )
        tiles_nn = gdf_mercator[gdf_mercator["tile_id"].isin(still_unassigned)].copy()
        tiles_nn["geometry"] = tiles_nn.geometry.to_crs("ESRI:54009").centroid.to_crs(gdf_mercator.crs)
        admins_proj = gdf_admins1[["id", "geometry"]].copy()
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
            f"{country}: Admin level {admin_level} boundaries unavailable ({e}) — "
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

    Probes for admin1 through admin5 parquets. This determines which admin-level
    storm views are produced during --type update.

    Args:
        country: ISO3 country code

    Returns:
        list[int]: Admin levels with existing base parquets (e.g. [1, 2])
    """
    found = []
    for level in range(1, 6):
        file_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views',
                                 f"{country}_admin{level}.parquet")
        if data_store.file_exists(file_path):
            found.append(level)
    return found


def write_country_boundary(country: str):
    """
    Fetch admin level 0 boundary from GeoRepo and write it to
    PIPELINE_COUNTRIES.COUNTRY_BOUNDARY in Snowflake.
    Called automatically during --type initialize for each new country.
    """
    try:
        boundaries = AdminBoundaries.create(country_code=country, admin_level=0)
        gdf = boundaries.to_geodataframe()
        if gdf.empty or gdf.geometry.isna().all():
            logger.warning(f"{country}: GeoRepo returned no boundary — COUNTRY_BOUNDARY not updated")
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
        # rather than silently writing NULL to COUNTRY_BOUNDARY.
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
        conn.close()
        logger.info(f"{country}: COUNTRY_BOUNDARY, CENTER_LAT/LON, VIEW_ZOOM written to Snowflake")
    except Exception as e:
        logger.warning(f"{country}: Could not write COUNTRY_BOUNDARY to Snowflake: {e}")


def patch_country_layer(country, zoom_level, columns):
    """
    Backfill specific columns in an existing base mercator parquet without full re-initialization.

    Loads the existing parquet for the country, re-fetches only the requested data sources,
    merges the new values back, and saves. After saving the mercator parquet, the admin parquet
    is also re-aggregated so baseline admin-level counts stay in sync.

    This is the preferred way to populate NaN columns (e.g. after GHSL/SMOD/RWI data becomes
    available for a country) without re-downloading population data or re-fetching schools and HCs.

    Supported columns:
        population             — re-runs WorldPop total population (or uses custom population_z<N>.csv)
        school_age_population  — re-runs WorldPop school-age population
        infant_population      — re-runs WorldPop infant population
        adolescent_population  — re-runs WorldPop adolescent population (15–19y)
        built_surface_m2       — re-runs GHSL built surface (or uses custom built_surface_z<N>.csv)
        smod_class             — re-runs GHSL SMOD (or uses custom smod_z<N>.csv); also updates smod_class_l1
        smod_class_l1          — alias for smod_class (both are always updated together)
        rwi                    — re-runs RWI (or uses custom rwi_z<N>.csv)
        schools                — re-fetches school locations and recomputes counts (updates num_schools column)
        hcs                    — re-fetches health center locations and recomputes counts (updates num_hcs column)
        shelters               — re-fetches shelter locations from OSM or custom CSV and recomputes counts (updates num_shelters column)
        wash                   — re-fetches WASH facility locations from OSM or custom CSV and recomputes counts (updates num_wash column)
        admin<N>               — creates a new base admin parquet for level N (e.g. admin2). Does not modify
                                 the mercator parquet. Fails with a clear error if GeoRepo has no level-N
                                 boundaries for the country.

    Population columns are patched individually — use this when a new WorldPop dataset is
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
        else:
            viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
            try:
                viewer.map_built_s()
                v = viewer.to_geodataframe().set_index('zone_id')['built_surface_m2']
                gdf['built_surface_m2'] = gdf['tile_id'].map(v.to_dict())
            except Exception as e:
                logger.warning(f"{country}: GHSL built surface still unavailable during patch: {e}")
        logger.info(f"{country}: Patched built_surface_m2")

    if 'smod_class' in columns:
        custom = _load_custom_tiles_csv(country, 'smod', zoom_level)
        if custom is not None and 'smod_class' in custom.columns:
            gdf['smod_class'] = gdf['tile_id'].map(custom['smod_class'])
        else:
            viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
            try:
                viewer.map_smod()
                v = viewer.to_geodataframe().set_index('zone_id')['smod_class']
                gdf['smod_class'] = gdf['tile_id'].map(v.to_dict())
            except Exception as e:
                logger.warning(f"{country}: GHSL SMOD still unavailable during patch: {e}")
        # Always re-derive L1 when L2 is patched
        gdf['smod_class_l1'] = gdf['smod_class'].map(SMOD_L2_TO_L1)
        logger.info(f"{country}: Patched smod_class + smod_class_l1")

    if 'rwi' in columns:
        custom = _load_custom_tiles_csv(country, 'rwi', zoom_level)
        if custom is not None and 'rwi' in custom.columns:
            gdf['rwi'] = gdf['tile_id'].map(custom['rwi'])
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
            except Exception as e:
                logger.warning(f"{country}: RWI still unavailable during patch: {e}")
        logger.info(f"{country}: Patched rwi")

    pop_cols_requested = [c for c in ['population', 'school_age_population', 'infant_population', 'adolescent_population'] if c in columns]
    if pop_cols_requested:
        custom_pop = _load_custom_tiles_csv(country, 'population', zoom_level)
        if custom_pop is not None:
            for col in pop_cols_requested:
                if col in custom_pop.columns:
                    gdf[col] = gdf['tile_id'].map(custom_pop[col])
                    logger.info(f"{country}: Patched {col} from custom CSV")
                else:
                    logger.warning(f"{country}: Custom population CSV missing column '{col}' — skipping")
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
        viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
        schools = viewer.map_points(points=gdf_schools)
        gdf['num_schools'] = gdf['tile_id'].map(schools)
        logger.info(f"{country}: Patched num_schools")

    if 'hcs' in columns:
        gdf_hcs = fetch_health_centers(country, rewrite=1)
        viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
        hcs = viewer.map_points(points=gdf_hcs)
        gdf['num_hcs'] = gdf['tile_id'].map(hcs)
        logger.info(f"{country}: Patched num_hcs")

    if 'shelters' in columns:
        gdf_shelters = fetch_shelters(country, rewrite=1)
        viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
        shelters = viewer.map_points(points=gdf_shelters)
        gdf['num_shelters'] = gdf['tile_id'].map(shelters)
        logger.info(f"{country}: Patched num_shelters")

    if 'wash' in columns:
        gdf_wash = fetch_wash(country, rewrite=1)
        viewer = _MVG(source=country, zoom_level=zoom_level, data_store=data_store)
        wash_pts = viewer.map_points(points=gdf_wash)
        gdf['num_wash'] = gdf['tile_id'].map(wash_pts)
        logger.info(f"{country}: Patched num_wash")

    if columns:
        write_dataset(gdf, data_store, file_path)
        logger.info(f"{country}: Patch complete — saved updated mercator parquet")

        # Re-aggregate all existing admin parquets so baseline counts stay in sync.
        if 'id' not in gdf.columns:
            logger.warning(f"{country}: Mercator parquet has no 'id' column — skipping admin parquet sync "
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
                agg_dict = {col: "sum" for col in sum_cols_admin if col in src.columns}
                agg_dict.update({col: "mean" for col in avg_cols_admin if col in src.columns})
                agg = src.groupby(group_col).agg(agg_dict).reset_index()
                agg = agg.rename(columns={group_col: 'tile_id'})
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
                # Use raw mercator tiles (without admin1 'id') as input for arbitrary levels
                src = gdf.drop(columns=['id'], errors='ignore') if admin_level != 1 else gdf
                admin_view = _build_admin_view_from_mercator(src, country, admin_level=admin_level)
                save_admin_view(admin_view, country, admin_level=admin_level)
                logger.info(f"{country}: Created admin{admin_level} parquet")
            except ValueError as e:
                logger.error(f"{country}: Cannot create admin{admin_level} — {e}")


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

    agg_dict = {col: "sum" for col in sum_cols_admin if col in combined_view.columns}
    agg_dict.update({col: "mean" for col in avg_cols_admin if col in combined_view.columns})
    agg = combined_view.groupby("id").agg(agg_dict).reset_index()
    # Ensure all admin regions appear even if no tiles were assigned to them
    all_ids = gdf_admins[['id']].copy()
    agg = all_ids.merge(agg, on='id', how='left')
    for col in list(sum_cols_admin) + list(avg_cols_admin):
        if col in agg.columns:
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
                    logger.error(f"{country}: Skipping admin{admin_level} — {e}")

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
                    logger.error(f"{country}: Skipping admin{admin_level} — {e}")

            initialized = True
        else:
            # Mercator file already exists and rewrite=0 — skip regeneration.
            # Still create any admin parquets for levels not yet initialized.
            logger.info(f"Mercator file already exists for {country} at zoom {zoom_level}, ensuring tracking is up to date")
            view = read_dataset(file_path, data_store)
            for admin_level in admin_levels:
                admin_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views',
                                          f"{country}_admin{admin_level}.parquet")
                if not data_store.file_exists(admin_path):
                    try:
                        src = view if admin_level == 1 else view.drop(columns=['id'], errors='ignore')
                        admin_view = _build_admin_view_from_mercator(src, country, admin_level=admin_level)
                        save_admin_view(admin_view, country, admin_level=admin_level)
                        logger.info(f"{country}: Created admin{admin_level} parquet")
                    except ValueError as e:
                        logger.error(f"{country}: Skipping admin{admin_level} — {e}")
                else:
                    logger.info(f"{country}: admin{admin_level} already exists — skipping")
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
def create_school_view_from_envelopes(gdf_schools, gdf_envelopes):
    """
    Create per-facility school impact views from hurricane envelopes.

    For each wind speed threshold, calculates the probability that each individual
    school will be affected by winds at or above that threshold. Probability is
    computed as the fraction of ensemble members whose wind envelope intersects
    the school (buffered by 150m).

    Output is one row per school per wind threshold — NOT aggregated to tiles.
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
    
    gdf_schools_buff = buffer_geodataframe(gdf_schools, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}

    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            schools_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_schools_buff, zone_id_column='school_id_giga')
            try:
                new_col = schools_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except Exception as e:
                logger.warning(f"Error mapping polygons for school view at {wind_th}kt: {e}")
                probs = {k: 0.0 for k in schools_viewer.view['zone_id'].unique()}
            schools_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = schools_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views


def create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes):
    """
    Create per-facility health center impact views from hurricane envelopes.

    For each wind speed threshold, calculates the probability that each individual
    health center will be affected by winds at or above that threshold. Probability
    is computed as the fraction of ensemble members whose wind envelope intersects
    the facility (buffered by BUFFER_DISTANCE_METERS).

    Output is one row per facility per wind threshold — NOT aggregated to tiles.
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
    # Filter to relevant facility types at analysis time using HC_FACILITY_TYPES.
    # HC_FACILITY_TYPES is a dict of {column: [values]} so multiple OSM tag keys
    # can be combined (e.g. amenity + healthcare). A facility matches if ANY
    # of the specified column/value pairs apply (OR logic across keys).
    # The cache stores all types; this filter controls what enters impact files.
    # Applies equally to API-sourced and custom data.
    if not gdf_hcs.empty:
        before = len(gdf_hcs)
        mask = pd.Series(False, index=gdf_hcs.index)
        for col, values in HC_FACILITY_TYPES.items():
            if col in gdf_hcs.columns:
                mask |= gdf_hcs[col].isin(values)
        gdf_hcs = gdf_hcs[mask].copy()
        logger.debug(f"HC type filter: {before} → {len(gdf_hcs)} facilities (kept: {HC_FACILITY_TYPES})")

    if gdf_hcs.empty:
        logger.warning(f"No health facilities matching {HC_FACILITY_TYPES} — returning empty impact views")
        return {}

    gdf_hcs_buff = buffer_geodataframe(gdf_hcs, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}

    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            hcs_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_hcs_buff, zone_id_column='osm_id')
            try:
                new_col = hcs_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except Exception as e:
                logger.warning(f"Error mapping polygons for health center view at {wind_th}kt: {e}")
                probs = {k: 0.0 for k in hcs_viewer.view['zone_id'].unique()}
            hcs_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = hcs_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views


def create_shelter_view_from_envelopes(gdf_shelters, gdf_envelopes):
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

    gdf_shelters_buff = buffer_geodataframe(gdf_shelters, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}
    num_ensembles = FULL_ENSEMBLE_SIZE
    for wind_th in gdf_envelopes.wind_threshold.unique():
        gdf_env_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_env_wth.empty:
            viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_shelters_buff, zone_id_column='osm_id')
            try:
                new_col = viewer.map_polygons(gdf_env_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except Exception as e:
                logger.warning(f"Error mapping polygons for shelter view at {wind_th}kt: {e}")
                probs = {k: 0.0 for k in viewer.view['zone_id'].unique()}
            viewer.add_variable_to_view(probs, 'probability')
            wind_views[wind_th] = viewer.to_geodataframe()
    return wind_views


def create_wash_view_from_envelopes(gdf_wash, gdf_envelopes):
    """
    Create per-facility WASH impact views from hurricane envelopes.

    For each wind speed threshold, calculates the probability that each WASH
    facility will be affected by winds at or above that threshold. All facility
    types in the cache enter impact calculations — type selection is controlled
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

    gdf_wash_buff = buffer_geodataframe(gdf_wash, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}
    num_ensembles = FULL_ENSEMBLE_SIZE
    for wind_th in gdf_envelopes.wind_threshold.unique():
        gdf_env_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_env_wth.empty:
            viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_wash_buff, zone_id_column='osm_id')
            try:
                new_col = viewer.map_polygons(gdf_env_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except Exception as e:
                logger.warning(f"Error mapping polygons for WASH view at {wind_th}kt: {e}")
                probs = {k: 0.0 for k in viewer.view['zone_id'].unique()}
            viewer.add_variable_to_view(probs, 'probability')
            wind_views[wind_th] = viewer.to_geodataframe()
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
    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
            try:
                new_col = tiles_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except Exception as e:
                logger.warning(f"map_polygons failed for wind threshold, defaulting probabilities to 0: {e}")
                probs = {k: 0.0 for k in tiles_viewer.view['zone_id'].unique()}
            tiles_viewer.add_variable_to_view(probs, 'probability')

            df_view = tiles_viewer.to_dataframe()
            for col in data_cols:
                if col in df_view.columns:
                    df_view[f"E_{col}"] = df_view[col] * df_view['probability']
                else:
                    df_view[f"E_{col}"] = np.nan
                    logger.debug(f"Column '{col}' missing from tile data — E_{col} set to NaN (re-initialize country to populate)")

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

            wind_views[wind_th] = df_view

    return wind_views


def create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles, gdf_envelopes):
    if 'name' in gdf_admin.columns:
        d = gdf_admin.set_index('tile_id')['name'].to_dict()
    else:
        logger.warning("Admin GeoDataFrame missing 'name' column — admin region names will be NaN")
        d = {}
    wind_views = {}
    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
            try:
                new_col = tiles_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except Exception as e:
                logger.warning(f"map_polygons failed for wind threshold, defaulting probabilities to 0: {e}")
                probs = {k: 0.0 for k in tiles_viewer.view['zone_id'].unique()}
            tiles_viewer.add_variable_to_view(probs, 'probability')

            df_view = tiles_viewer.to_dataframe()
            for col in data_cols:
                if col in df_view.columns:
                    df_view[f"E_{col}"] = df_view[col] * df_view['probability']
                else:
                    df_view[f"E_{col}"] = np.nan
                    logger.debug(f"Column '{col}' missing from tile data — E_{col} set to NaN (re-initialize country to populate)")

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
            
            # Define aggregation dictionary
            agg_dict = {col: "sum" for col in sum_cols}
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
                logger.warning(f"  {missing_names} admin region(s) at {wind_th}kt have no name mapping (tile_id not in admin GeoDataFrame)")

            wind_views[wind_th] = df_view

    return wind_views


def create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member', gdf_shelters=None, gdf_wash=None):
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

        # Shelters
        shelters = tracks_viewer.map_points(points=gdf_shelters)
        tracks_viewer.add_variable_to_view(shelters, "severity_num_shelters")

        # WASH facilities
        wash_pts = tracks_viewer.map_points(points=gdf_wash)
        tracks_viewer.add_variable_to_view(wash_pts, "severity_num_wash")

        # Tiles
        tile_value_columns = ["population", "school_age_population", "infant_population", "built_surface_m2"]
        if "adolescent_population" in gdf_tiles.columns:
            tile_value_columns.append("adolescent_population")
        try:
            overlays = tracks_viewer.map_polygons(polygons=gdf_tiles, value_columns=tile_value_columns, aggregation="sum")
            tracks_viewer.add_variable_to_view(overlays['population'], "severity_population")
            tracks_viewer.add_variable_to_view(overlays['adolescent_population'], "severity_adolescent_population")
            tracks_viewer.add_variable_to_view(overlays['school_age_population'], "severity_school_age_population")
            tracks_viewer.add_variable_to_view(overlays['infant_population'], "severity_infant_population")
            tracks_viewer.add_variable_to_view(overlays['built_surface_m2'], "severity_built_surface_m2")
        except Exception as e:
            logger.warning(f"Track severity overlay failed, defaulting to zeros: {e}")
            zeros = {k: 0 for k in gdf_envelopes_wth[index_column].unique()}
            tracks_viewer.add_variable_to_view(zeros, "severity_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_adolescent_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_school_age_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_infant_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_built_surface_m2")

        gdf_view = tracks_viewer.to_geodataframe()
        wind_views[wind_th] = gdf_view

    return wind_views


# =============================================================================
# FACILITY VIEW PERSISTENCE
# Save / load / existence-check functions for per-facility location caches
# (written once at --type initialize) and per-storm impact views (written on
# every --type update). Grouped by facility type: schools, HCs, shelters, WASH.
# =============================================================================
def save_school_view(gdf, country, storm, date, wind_th):
    """
    Save per-facility school impact view for a specific storm, date, and wind threshold.

    File name pattern: <COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet
    Stored under: school_views/ in the configured data store.

    Note: This is distinct from the location cache (<COUNTRY>_schools.parquet).
    The impact view has one row per school with a 'probability' column; the
    location cache has full school metadata (education_level, etc.). Join on
    school_id_giga to combine them.

    Args:
        gdf: GeoDataFrame with per-school impact data (zone_id + probability)
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


def save_hc_view(gdf, country, storm, date, wind_th):
    """
    Save per-facility health center impact view for a specific storm, date, and wind threshold.

    File name pattern: <COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet
    Stored under: hc_views/ in the configured data store.

    Note: This is distinct from the location cache (<COUNTRY>_health_centers.parquet).
    The impact view has one row per facility with a 'probability' column; the
    location cache has full facility metadata including the 'amenity' column.
    Join on osm_id to combine them, or filter the impact view directly by
    'amenity' — only HC_FACILITY_TYPES values will be present since filtering
    happens before the impact views are written.

    Args:
        gdf: GeoDataFrame with per-facility impact data (zone_id + probability)
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        wind_th: Wind threshold in knots
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


def save_shelter_view(gdf, country, storm, date, wind_th):
    """
    Save per-facility shelter impact view for a specific storm, date, and wind threshold.

    File name pattern: <COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet
    Stored under: shelter_views/ in the configured data store.

    Note: This is distinct from the location cache (<COUNTRY>_shelters.parquet).
    The impact view has one row per shelter with a 'probability' column; the
    location cache has full shelter metadata (shelter_type, capacity, etc.).

    Args:
        gdf: GeoDataFrame with per-shelter impact data (zone_id + probability)
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        wind_th: Wind threshold in knots
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views', file_name))

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


def save_wash_view(gdf, country, storm, date, wind_th):
    """
    Save per-facility WASH impact view for a specific storm, date, and wind threshold.

    File name pattern: <COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet
    Stored under: wash_views/ in the configured data store.

    Note: This is distinct from the location cache (<COUNTRY>_wash.parquet).
    The impact view has one row per WASH facility with a 'probability' column; the
    location cache has full facility metadata (wash_type, name, etc.).

    Args:
        gdf: GeoDataFrame with per-facility impact data (zone_id + probability)
        country: ISO3 country code
        storm: Storm name
        date: Forecast date in YYYYMMDDHHMMSS format
        wind_th: Wind threshold in knots
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views', file_name))

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

    Data requirements — **hard failures** (aborts if unavailable):
        - Total population (WorldPop 1km)
        - School-age population (WorldPop 100m, age_structures)
        - Infant population (WorldPop 100m, age_structures)
        - Under-18 population (WorldPop 100m, age_structures)

    Data requirements — **optional** (NaN/0 if unavailable; backfill with --type patch):
        - GHSL built surface (built_surface_m2)
        - SMOD settlement class L2 (smod_class) and derived L1 (smod_class_l1)
        - Relative Wealth Index (rwi)
        - Schools (num_schools) — 0 if API fails or no data
        - Health centers (num_hcs) — 0 if API fails or no data
        - Emergency shelters (num_shelters) — 0 if OSM returns nothing
        - WASH facilities (num_wash) — 0 if OSM returns nothing

    Args:
        country: ISO3 country code
        rewrite: If 1, re-fetch school, HC, shelter, and WASH location caches from API/OSM;
                 if 0, use cached parquets if available
        admin_level: Admin level to use for boundary aggregation (default: 1)

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with admin boundaries and all demographic/
                         infrastructure columns. 'zone_id' renamed to 'tile_id'.
    """
    # Fetch facility locations — custom data priority handled inside fetch_*
    gdf_schools = fetch_schools(country, rewrite)
    gdf_hcs = fetch_health_centers(country, rewrite)
    gdf_shelters = fetch_shelters(country, rewrite)
    gdf_wash = fetch_wash(country, rewrite)

    # Note: AdminBoundariesViewGenerator uses admin boundary IDs (not quadkeys), so
    # custom tile-level CSVs (population_z<N>, built_surface_z<N>, etc.) do not apply here.
    # Custom point data is handled above via fetch_schools/fetch_health_centers/fetch_shelters/fetch_wash.
    tiles_viewer = AdminBoundariesViewGenerator(country=country, admin_level=admin_level, data_store=data_store)

    # Population — hard requirements, raises on failure
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

    # GHSL built surface — optional, NaN fallback
    try:
        tiles_viewer.map_built_s()
    except Exception as e:
        logger.warning(f"{country}: GHSL built surface unavailable — setting to NaN: {e}")
        tiles_viewer.add_variable_to_view(
            {k: np.nan for k in tiles_viewer.view.index.unique()}, 'built_surface_m2'
        )

    # SMOD settlement class — optional, NaN fallback
    try:
        tiles_viewer.map_smod()
    except Exception as e:
        logger.warning(f"{country}: GHSL SMOD unavailable — setting to NaN: {e}")
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
    schools = tiles_viewer.map_points(points=gdf_schools)
    tiles_viewer.add_variable_to_view(schools, "num_schools")
    hcs = tiles_viewer.map_points(points=gdf_hcs)
    tiles_viewer.add_variable_to_view(hcs, "num_hcs")
    shelters = tiles_viewer.map_points(points=gdf_shelters)
    tiles_viewer.add_variable_to_view(shelters, "num_shelters")
    wash_pts = tiles_viewer.map_points(points=gdf_wash)
    tiles_viewer.add_variable_to_view(wash_pts, "num_wash")

    # RWI — optional, NaN fallback
    try:
        handler = RWIHandler(data_store=data_store)
        rwi_df = handler.load_data(country, ensure_available=True)
        if rwi_df is None or (hasattr(rwi_df, 'empty') and rwi_df.empty):
            raise ValueError(f"No RWI data available for {country}")
        rwi_gdf = convert_to_geodataframe(rwi_df)
        rwi = tiles_viewer.map_points(rwi_gdf, value_columns='rwi', aggregation='mean')
    except Exception as e:
        logger.warning(f"{country}: Relative Wealth Index unavailable — setting to NaN: {e}")
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
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))

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

def save_admin_tiles_view(gdf, country, storm, date, wind_th, admin_level=1):
    """
    Saves admin tiles views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}_admin{admin_level}.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))


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

def load_admin_view(country, admin_level=1):
    """Load admin view for country"""
    file_name = f"{country}_admin{admin_level}.parquet"
    return read_dataset(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name), data_store)


def save_tracks_view(gdf, country, storm, date, wind_th):
    """
    Saves tracks views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', file_name))



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
            logger.warning(f"Column '{pop_col}' missing from tile data — CCI_{pop_col} will be NaN (re-initialize country)")
            gdf_tiles_index[pop_col] = np.nan
    cci_tiles_view = pd.DataFrame(index=gdf_tiles_index.index)

    # Children cci (0–19: school_age 5–14 + infants 0–4 + adolescents 15–19)
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'] + gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'] + gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'] + gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_children'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children']]

    # Children e cci (0–19: school_age 5–14 + infants 0–4 + adolescents 15–19)
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_school_age_population'] + sorted_wind_views_indexed[i]['E_infant_population'] + sorted_wind_views_indexed[i]['E_adolescent_population']) - (sorted_wind_views_indexed[i+1]['E_school_age_population'] + sorted_wind_views_indexed[i+1]['E_infant_population'] + sorted_wind_views_indexed[i+1]['E_adolescent_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_school_age_population'] + sorted_wind_views_indexed[k-1]['E_infant_population'] + sorted_wind_views_indexed[k-1]['E_adolescent_population'])
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

    # under-18 cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_adolescents'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_adolescents']]

    # under-18 e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_adolescent_population']) - (sorted_wind_views_indexed[i+1]['E_adolescent_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_adolescent_population'])
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_adolescents'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_adolescents','E_CCI_adolescents']]

    # pop cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_pop'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_adolescents','E_CCI_adolescents','CCI_pop']]

    # pop e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_population']) - (sorted_wind_views_indexed[i+1]['E_population'])
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



# =============================================================================
# MAIN IMPACT ANALYSIS ORCHESTRATION
# Top-level function called per country per storm on every --type update run.
# Coordinates all view generation, CCI calculation, and report writing.
# =============================================================================
def create_views_from_envelopes_in_country(country, storm, date, gdf_envelopes, zoom):
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

    Args:
        country: ISO3 country code
        storm: Storm name (e.g., 'FUNG-WONG')
        date: Forecast date in YYYYMMDDHHMMSS format (e.g., '20251110000000')
        gdf_envelopes: GeoDataFrame containing hurricane envelope geometries
        zoom: Zoom level for mercator tiles

    Note:
        Base data (mercator tiles, admin views) are loaded if available, or created
        on-the-fly if missing. Admin levels are detected from existing base parquets
        created during --type initialize. Add new levels with --type patch --columns adminN.
    """
    admin_levels = get_initialized_admin_levels(country)
    if not admin_levels:
        # Fallback: ensure admin1 is always processed (creates on-the-fly if missing)
        admin_levels = [1]

    # Remove all existing output files for this country/storm/forecast run before writing
    # new ones. This prevents stale threshold files (e.g. from a run where 137kt had a
    # few envelope members that have since been cleaned up) from persisting on the stage.
    prefix = f"{country}_{storm}_{date}_"
    for view_dir in ('school_views', 'hc_views', 'shelter_views', 'wash_views',
                     'mercator_views', 'admin_views', 'track_views'):
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
    logger.info(f"    Created {len(wind_school_views)} school views")

    # Health centers
    logger.info(f"    Processing health centers...")
    gdf_hcs = fetch_health_centers(country, rewrite=0)
    wind_hc_views = create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes)
    for wind_th in wind_hc_views:
        save_hc_view(wind_hc_views[wind_th], country, storm, date, wind_th)
    logger.info(f"    Created {len(wind_hc_views)} health center views")

    # Shelters
    logger.info(f"    Processing shelters...")
    gdf_shelters = fetch_shelters(country, rewrite=0)
    wind_shelter_views = create_shelter_view_from_envelopes(gdf_shelters, gdf_envelopes)
    for wind_th in wind_shelter_views:
        save_shelter_view(wind_shelter_views[wind_th], country, storm, date, wind_th)
    logger.info(f"    Created {len(wind_shelter_views)} shelter views")

    # WASH
    logger.info(f"    Processing WASH...")
    gdf_wash = fetch_wash(country, rewrite=0)
    wind_wash_views = create_wash_view_from_envelopes(gdf_wash, gdf_envelopes)
    for wind_th in wind_wash_views:
        save_wash_view(wind_wash_views[wind_th], country, storm, date, wind_th)
    logger.info(f"    Created {len(wind_wash_views)} WASH views")

    # Tiles
    logger.info(f"    Processing tiles...")
    try:
        gdf_tiles = load_mercator_view(country, zoom)
        logger.info(f"    Loaded existing mercator tiles: {len(gdf_tiles)} tiles")
        # Ensure admin IDs are present (in case file was created without them)
        if 'id' not in gdf_tiles.columns:
            logger.warning(f"    Mercator view missing admin IDs, adding them...")
            gdf_tiles, _ = add_admin_ids(gdf_tiles, country)
            save_mercator_view(gdf_tiles, country, zoom)
    except Exception as e:
        logger.info(f"    Creating base mercator tiles for {country}... ({e})")
        view = create_mercator_country_layer(country, zoom, rewrite=0)
        gdf_tiles, _ = add_admin_ids(view, country)
        save_mercator_view(gdf_tiles, country, zoom)
        logger.info(f"    Created and saved base mercator tiles: {len(gdf_tiles)} tiles")

    wind_tiles_views = create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes)
    for wind_th in wind_tiles_views:
        save_tiles_view(wind_tiles_views[wind_th], country, storm, date, wind_th, zoom)
    logger.info(f"    Created {len(wind_tiles_views)} tile views")

    # CCI for tiles
    cci_tiles_view = calculate_ccis(wind_tiles_views, gdf_tiles)
    save_cci_tiles(cci_tiles_view, country, storm, date, zoom)

    # Admins — one pass per requested admin level
    logger.info(f"    Processing admins (levels: {admin_levels})...")
    for admin_level in admin_levels:
        try:
            gdf_admin = load_admin_view(country, admin_level=admin_level)
            logger.info(f"    Loaded existing admin{admin_level}: {len(gdf_admin)} regions")
        except Exception as e:
            logger.info(f"    Creating base admin{admin_level} for {country}... ({e})")
            gdf_admin = create_admin_country_layer(country, rewrite=0, admin_level=admin_level)
            save_admin_view(gdf_admin, country, admin_level=admin_level)
            logger.info(f"    Created and saved base admin{admin_level}: {len(gdf_admin)} regions")

        # For admin level 1, gdf_tiles already has 'id' = admin1 IDs (from mercator parquet).
        # For other levels, derive the tile mapping from the already-loaded admin parquet
        # (which stores boundary geometries) — avoids a redundant GeoRepo API call.
        if admin_level == 1:
            gdf_tiles_for_admin = gdf_tiles
        else:
            gdf_admin_boundaries = gdf_admin[['tile_id', 'geometry']].rename(columns={'tile_id': 'id'})
            gdf_tiles_for_admin = admins_overlay(gdf_admin_boundaries,
                                                 gdf_tiles.drop(columns=['id'], errors='ignore'))

        wind_admin_views = create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles_for_admin, gdf_envelopes)
        for wind_th in wind_admin_views:
            save_admin_tiles_view(wind_admin_views[wind_th], country, storm, date, wind_th,
                                  admin_level=admin_level)
        logger.info(f"    Created {len(wind_admin_views)} admin{admin_level} views")

        # CCI for this admin level
        agg_dict = {col: "sum" for col in sum_cols_cci}
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

    # Keep a reference to admin1 for the JSON report (always in admin_levels or generated above)
    try:
        gdf_admin = load_admin_view(country, admin_level=1)
    except Exception:
        gdf_admin = create_admin_country_layer(country, rewrite=0, admin_level=1)

    agg_dict = {col: "sum" for col in sum_cols_cci}
    agg = cci_tiles_view.groupby("id").agg(agg_dict).reset_index()
    cci_admin_view = agg.rename(columns={'id': 'tile_id'})

    # Re-load admin1 wind views for the report (already saved above)
    wind_admin_views = create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles, gdf_envelopes)

    # Tracks
    logger.info(f"    Processing tracks...")
    wind_tracks_views = create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member', gdf_shelters=gdf_shelters, gdf_wash=gdf_wash)
    for wind_th in wind_tracks_views:
        save_tracks_view(wind_tracks_views[wind_th], country, storm, date, wind_th)
    logger.info(f"    Created {len(wind_tracks_views)} track views")

    df_tracks = get_snowflake_tracks(date, storm)
    gdf_tracks = convert_to_geodataframe(df_tracks)

    json_report = do_report(wind_school_views, wind_hc_views, wind_tiles_views, wind_admin_views, cci_tiles_view, cci_admin_view, gdf_admin, gdf_tracks, country, storm, date, wind_shelter_views=wind_shelter_views, wind_wash_views=wind_wash_views)
    save_json_report(json_report, country, storm, date)


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
        # Get envelope data from Snowflake
        df_envelopes = get_envelopes_from_snowflake(storm, forecast_time)
        
        if df_envelopes.empty:
            logger.error(f"No envelope data found in Snowflake for {storm} at {forecast_time}")
            return pd.DataFrame()

        # Convert to GeoDataFrame
        gdf_envelopes = convert_envelopes_to_geodataframe(df_envelopes)
        return gdf_envelopes

    except Exception as e:
        logger.error(f"Error loading envelopes from Snowflake: {str(e)}")
        return pd.DataFrame()