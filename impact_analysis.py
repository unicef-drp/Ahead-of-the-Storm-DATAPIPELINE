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
  threshold (34-137 kt) to produce per-facility and per-tile impact views
- Supports custom data overrides: place a CSV in geodb/custom/ to replace any API or
  raster source for a specific country (never overwritten by the pipeline)
- Patches specific columns in existing mercator parquets without full re-initialisation
  (patch_country_layer)
- Storage-backend agnostic: LOCAL filesystem, Azure Blob (ADLS), or Snowflake internal
  stage (controlled by DATA_PIPELINE_DB env var)

Entry points called from main_pipeline.py:
    create_mercator_country_layer():          --type initialize
    create_admin_country_layer():             --type initialize
    create_views_from_envelopes_in_country(): --type update
    patch_country_layer():                    --type patch

This module is the "core" of the impact analysis code, which is split across
five sibling modules, each covering one concern; the core holds wind/gust view
creation and the main per-country orchestrator:
    pipeline_shared.py      - state/helpers every module below needs, with no
                              dependency of its own on any of them (data_store,
                              path/column constants, admins_overlay(), the
                              custom-data-override loaders) - see that
                              module's own docstring for why it exists
    facility_io.py          - HealthSites/GIGA/OSM facility fetch + the
                              per-facility-type location-cache/impact-view
                              save/load/exist persistence helpers
    country_boundaries.py   - country/admin boundary bookkeeping (GeoRepo
                              fetch, PIPELINE_COUNTRIES.COUNTRY_BOUNDARY and
                              boundary pre-filter cache writes, admin-level
                              parquet discovery)
    precip_views.py         - all precip (tp/ro-tp-ratio exceedance
                              probability) tile/admin/facility view creation
                              and persistence
    river_views.py          - all river-flood (GloFAS x JRC) tile/admin/
                              facility view creation and persistence
This module imports several names back from facility_io.py/country_boundaries.py
(real internal dependencies - e.g. create_views_from_envelopes_in_country()
fetches/saves facilities and writes country boundaries as part of its own
per-country orchestration), plus sparse_output.py/sparse_build.py for the
sparse writers and frame builders, all as plain top-of-file imports; since
facility_io.py and country_boundaries.py get their own shared state from
pipeline_shared.py rather than from this module, there is no circular import
to work around.

Module structure (sections in order, this file only):
    CUSTOM DATA HELPERS: _load_custom_tiles_csv, _vulnerability_file_path,
                                       _load_vulnerability_tiles_csv
                                       (_custom_file_path/_load_custom_points_csv
                                       moved to pipeline_shared.py)
    GEOGRAPHIC UTILITIES: is_envelope_in_zone (get_country_boundaries moved
                                       to country_boundaries.py)
    BASE LAYER INITIALIZATION: create_mercator_country_layer, save_mercator_view,
                                       patch_country_layer,
                                       save_mercator_and_admin_views
                                       (admins_overlay moved to pipeline_shared.py;
                                       add_admin_ids/write_country_boundary
                                       moved to country_boundaries.py)
    STORM METADATA: save/load_json_storms, load_mercator_view
    PER-STORM IMPACT VIEW GENERATION: create_school/hc/shelter/wash/mercator/admin/
                                        tracks_view_from_envelopes
    ADMIN COUNTRY LAYER: create_admin_country_layer
                                       (logically part of base layer init)
    SHARED FACILITY-TO-TILE ASSIGNMENT: project_tiles_for_facility_assignment,
                                       assign_facilities_to_tiles (used
                                       directly by main_pipeline.py's own
                                       precip/river loops, and by precip_views.py/
                                       river_views.py's own facility views)
    TILE & STORM VIEW PERSISTENCE: save/load for tile views, CCI views,
                                       admin views, track views
    CCI CALCULATION: calculate_ccis
    VULNERABILITY CALCULATION: calculate_vulnerability_view,
                                       calculate_vulnerability_tracks
    RASTERIZED ENVELOPE COVERAGE / BITMASK HELPERS: calculate_tile_member_bitmask
                                       and its private helpers
    MAIN IMPACT ANALYSIS: create_views_from_envelopes_in_country
    SNOWFLAKE DATA LOADING: load_envelopes_from_snowflake,
                                       load_gust_envelopes_from_snowflake
"""

import io
import json
import os
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
from gigaspatial.handlers import RWIHandler
from gigaspatial.processing import convert_to_geodataframe, buffer_geodataframe
from gigaspatial.processing.geo import aggregate_points_to_zones
from gigaspatial.generators import GeometryBasedZonalViewGenerator, MercatorViewGenerator, AdminBoundariesViewGenerator
from gigaspatial.core.io.readers import read_dataset
from gigaspatial.core.io.writers import write_dataset

from country_utils import update_country_initialized

# Import hazard source data retrieval functions. get_envelopes/get_gust_envelopes
# dispatch to Snowflake or LOCAL/BLOB based on config.HAZARD_DATA_SOURCE (see
# snowflake_utils.py's "HAZARD_DATA_SOURCE DISPATCH" section).
from snowflake_utils import (
    get_envelopes,
    get_gust_envelopes,
    convert_envelopes_to_geodataframe,
)

# Shared state/generic helpers with no dependency of their own on this module
# or its 4 siblings (facility_io/country_boundaries/precip_views/river_views) -
# see pipeline_shared.py's own docstring for why this exists as its own module.
from pipeline_shared import (
    data_store,
    ROOT_DATA_DIR,
    VIEWS_DIR,
    data_cols,
    POPULATION_COLS,
    sum_cols,
    avg_cols,
    _OPTIONAL_SUM_COLS,
    _optional_sum,
    FULL_ENSEMBLE_SIZE,
    admins_overlay,
    _custom_file_path,
)

# Sibling modules impact_analysis.py imports from. facility_io and
# country_boundaries import their own shared state from pipeline_shared.py (not
# from here), so there's no cycle - these are plain, ordinary top-of-file imports.
from facility_io import (
    HC_FACILITY_TYPES,
    fetch_health_centers,
    fetch_schools,
    fetch_shelters,
    fetch_wash,
    save_school_view,
    load_school_locations,
    school_exist,
    save_hc_view,
    load_hc_locations,
    hc_exist,
    save_shelter_view,
    load_shelter_locations,
    shelter_exist,
    save_wash_view,
    load_wash_locations,
    wash_exist,
)
from country_boundaries import (
    add_admin_ids,
    get_initialized_admin_levels,
    write_country_boundary,
)
from sparse_output import (
    describe_reduction,
    sparsify_facility_frame,
    sparsify_tile_frame,
)
from sparse_build import EnvelopeSparse

# =============================================================================
# CONSTANTS
# =============================================================================
# Admin-level aggregation: same logic as sum_cols, but operating on already-aggregated tile values.
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

# _OPTIONAL_SUM_COLS/_optional_sum live in pipeline_shared.py (imported above);
# not redefined here.

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
WORLDPOP_RESOLUTION_LOW = 100  # Low resolution for WorldPop data (meters)
SCHOOL_AGE_MIN = 5   # GR2 min_age: picks _05_ (5-9y) and _10_ (10-14y) bands
SCHOOL_AGE_MAX = 10  # GR2 max_age: picks _05_ (5-9y) and _10_ (10-14y) bands
INFANT_AGE_MIN = 0   # GR2 min_age: picks _00_ (0-12mo) and _01_ (1-4y) bands
INFANT_AGE_MAX = 1   # GR2 max_age: picks _00_ (0-12mo) and _01_ (1-4y) bands
ADOLESCENT_AGE_MIN = 15  # GR2 min_age: picks _15_ (15-19y) band
ADOLESCENT_AGE_MAX = 15  # GR2 max_age: picks _15_ (15-19y) band (1 file only)
CCI_WEIGHT_MULTIPLIER = 1e-6  # Multiplier for CCI weight calculation (wind_speed^2 * 1e-6)
# FULL_ENSEMBLE_SIZE lives in pipeline_shared.py (imported above); not redefined here.


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
# VIEWS_DIR/ROOT_DATA_DIR/data_store come from pipeline_shared.py (imported
# above) - RESULTS_DIR/STORMS_FILE are only ever used in this file, so they stay.
RESULTS_DIR = config.RESULTS_DIR
STORMS_FILE = config.STORMS_FILE

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
# _custom_file_path/_load_custom_points_csv live in pipeline_shared.py
# (facility_io.py needs them too); imported above alongside data_store etc.
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


# =============================================================================
# GEOGRAPHIC UTILITIES
# =============================================================================


def is_envelope_in_zone(zone_geom, df_envelopes, geometry_column='geometry'):
    """
    Check if any hurricane envelope intersects with a given zone geometry.

    Used in the Python fallback path of run_complete_impact_analysis when the SQL
    pre-filter (ST_DWITHIN on COUNTRY_BOUNDARY) is unavailable. The zone geometry
    is typically a 100 km buffer around a country boundary.

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
def create_mercator_country_layer(country, zoom_level=14, rewrite=0, fast_fail=False):
    """
    Create mercator tile layer with demographic and infrastructure data for a country.

    Called during `--type initialize` (and as fallback during `--type update` if the base
    mercator parquet is missing). Produces one row per tile with all data columns.

    fast_fail: forwarded to fetch_shelters()/fetch_wash() (see their own docstrings). The
        `--type initialize` caller leaves this at the patient default; the `--type update`
        fallback caller passes True, since it runs on every forecast cycle and a slow OSM
        retry there would stall the whole cycle exactly like the main per-storm fetch this
        fallback sits alongside.

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
                         columns, indexed by 'tile_id'.
    """
    # Fetch facility locations using helper functions with caching
    gdf_schools = fetch_schools(country, rewrite, fast_fail=fast_fail)
    gdf_hcs = fetch_health_centers(country, rewrite, fast_fail=fast_fail)
    gdf_shelters = fetch_shelters(country, rewrite, fast_fail=fast_fail)
    gdf_wash = fetch_wash(country, rewrite, fast_fail=fast_fail)

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
                # Map school-age population (5-14y): hard requirement, raises on failure
                # GR2: uses individual 5-year age bands (_05_ = 5-9y, _10_ = 10-14y), sex='T' for combined total
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
                # Map infant population (0-4y): hard requirement, raises on failure
                # GR2: uses individual 5-year age bands (_00_ = 0-12mo, _01_ = 1-4y), sex='T' for combined total
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
                # Map adolescent population (15-19y): hard requirement, raises on failure
                # GR2: picks _15_ band only (15-19y), sex='T' for combined total (1 file)
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
    # tiles_viewer.view's own DataFrame index is a positional RangeIndex, not zone_id
    # (zone_id is a regular column); must re-key by zone_id before .map()/.to_dict(),
    # since add_variable_to_view() joins the resulting dict by zone_id.
    try:
        smod_l2 = tiles_viewer.view.set_index('zone_id')['smod_class']
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


# admins_overlay lives in pipeline_shared.py (imported above); not redefined here.


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
        adolescent_population  : re-runs WorldPop adolescent population (15-19y)
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
                # offshore island unit), matching the all_ids-merge protection
                # _build_admin_view_from_mercator() applies at init time: groupby
                # alone only emits ids with >=1 assigned tile, and save_admin_view()
                # fully overwrites (not merges) the parquet, so a zero-tile region
                # would otherwise be permanently dropped.
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
                # admin_level=1 add_admin_ids() call at --type initialize time).
                # _build_admin_view_from_mercator() recomputes 'id' itself via its
                # own add_admin_ids() call, so the stale 'id' must be dropped for
                # every admin_level, or the merge inside admins_overlay() collides
                # two 'id' columns into id_x/id_y and the subsequent groupby("id")
                # raises an uncaught KeyError (matches the 'id' handling in the
                # sibling save_mercator_and_admin_views() branch).
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
                        # The persisted mercator parquet already carries an 'id' column
                        # (written by the admin_level=1 add_admin_ids() call at save
                        # time); drop it before rebuilding, same stale-'id' issue as
                        # the admin_patch_levels branch above.
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


# Real, DYNAMIC memory of which countries have needed a second-pass deferral
# (see the COUNTRY_PASS1_CAP_S comment in main_pipeline.py for the full
# "why"). Learned from real run outcomes and self-correcting (fewer real
# deferrals over time -> naturally stops being treated as a likely
# straggler). Same real Blob-backed JSON pattern as
# save_json_storms()/load_json_storms() above, deliberately a separate file
# (not folded into storms.json) since its own schema/purpose is unrelated.
SLOW_COUNTRIES_FILE = 'slow_countries.json'


def save_json_slow_countries(d):
    """Save the real, dynamic per-country deferral-history JSON."""
    filename = os.path.join(RESULTS_DIR, SLOW_COUNTRIES_FILE)
    data_store.write_file(filename, json.dumps(d).encode())


def load_json_slow_countries():
    """
    Read the real, dynamic per-country deferral-history JSON. Schema:
    {"windgust": {"<COUNTRY>": {"defer_count": int, "last_deferred_at":
    ISO-8601 str, "last_tier": "pass1" | "pass2"}}}. Namespaced by hazard
    ("windgust" today) so a future precip/river-specific use of this same
    file, if ever added, can't collide with wind/gust's own entries.
    """
    filename = os.path.join(RESULTS_DIR, SLOW_COUNTRIES_FILE)
    if data_store.file_exists(filename):
        raw = data_store.read_file(filename)
        return json.loads(raw)
    return {'windgust': {}}


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


def _precompute_facility_buffer(gdf, id_col, facility_label, filter_fn=None,
                                 buffer_distance_meters=BUFFER_DISTANCE_METERS):
    """
    Runs the same validate -> filter -> deduplicate -> buffer sequence each of
    create_school_view_from_envelopes()/create_health_center_view_from_
    envelopes()/create_shelter_view_from_envelopes()/create_wash_view_from_
    envelopes() and create_tracks_view_from_envelopes() already run
    internally for one facility type, ONCE, so create_views_from_envelopes_
    in_country() can compute a single buffered GeoDataFrame per facility type
    and reuse it across its own wind-view, wind-tracks, gust-view, and
    gust-tracks calls for that same facility type, instead of each of those
    four independently re-running buffer_geodataframe() (a real UTM-
    reprojection-and-buffer geometry operation, not a cheap one) on the exact
    same input. Deduplication never changes row count, order, or geometry
    (see _ensure_unique_zone_ids()'s own docstring: only id_col values on
    null/duplicate rows are rewritten), so the buffered GEOMETRY this
    produces is identical whether or not deduplication happened to run first,
    which is why create_tracks_view_from_envelopes()'s own point-in-polygon
    counting (unaffected by facility ID identity) can safely reuse a buffer
    built via this same dedup-then-buffer path as the per-facility-probability
    views that DO depend on unique IDs.

    Returns None on any invalid/missing/empty input (mirroring each
    downstream function's own early-return guard) so the caller can pass None
    straight through as that function's own precomputed_buff argument: a None
    there makes the function fall back to computing everything internally,
    exactly as if this precompute had never run, including that function's
    own facility-specific validation logging for the failure. Only a
    genuinely valid, real precomputed buffer is ever returned here.
    """
    if not isinstance(gdf, gpd.GeoDataFrame) or gdf.empty:
        return None
    if 'geometry' not in gdf.columns or gdf.geometry.isna().all():
        return None
    if filter_fn is not None:
        gdf = filter_fn(gdf)
        if gdf is None or gdf.empty:
            return None
    gdf = _ensure_unique_zone_ids(gdf, id_col, facility_label)
    return buffer_geodataframe(gdf, buffer_distance_meters=buffer_distance_meters)


def create_school_view_from_envelopes(gdf_schools, gdf_envelopes, threshold_column='wind_threshold',
                                       precomputed_buff=None):
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
        threshold_column: 'wind_threshold' or 'gust_threshold'.
        precomputed_buff: Optional, an already-deduplicated-and-buffered
            version of gdf_schools (see _precompute_facility_buffer()'s own
            docstring), reused instead of deduplicating/buffering gdf_schools
            again here. Passed by create_views_from_envelopes_in_country()
            when it has already computed this same buffer for its own
            create_tracks_view_from_envelopes() call (and, for gust, this
            same call again); None (the default) computes it here exactly as
            before, for any caller that doesn't have one handy.

    Returns:
        dict: Maps wind threshold (int, knots) → GeoDataFrame with one row per school,
              containing 'zone_id' (= school_id_giga) and 'probability' (0.0-1.0).
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
    
    if precomputed_buff is not None:
        gdf_schools_buff = precomputed_buff
    else:
        gdf_schools = _ensure_unique_zone_ids(gdf_schools, 'school_id_giga', 'school')
        gdf_schools_buff = buffer_geodataframe(gdf_schools, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}

    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes[threshold_column].unique())

    # Built ONCE and reused across every wind_th below, not reconstructed per
    # threshold (typically 6x for wind, more for gust): __init__ renames+copies
    # every column of gdf_schools_buff and rebuilds its zone_id->geometry dict
    # from scratch, overhead avoided since gdf_schools_buff never changes across
    # this loop. `_view` is reset to None before each threshold's own
    # add_variable_to_view() call, matching a freshly constructed instance's
    # lazy `if self._view is None` init path, so only setup cost is removed, not
    # per-threshold behavior. Same pattern applied to the sibling
    # create_health_center/shelter/wash/mercator/admin_view_from_envelopes*
    # functions below, but NOT create_tracks_view_from_envelopes(), whose
    # zone_data (gdf_envelopes_wth) genuinely differs every iteration.
    schools_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_schools_buff, zone_id_column='school_id_giga')
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            schools_viewer._view = None
            try:
                new_col = schools_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except KeyError as e:
                # gigaspatial's map_polygons(value_columns=None) adds a
                # '_temp_polygon_count_dummy' column and reads it back via
                # result.set_index('zone_id')[col]; when zero envelope polygons
                # intersect any zone at this threshold (normal, e.g. no member
                # reaches a high wind_threshold), aggregate_polygons_to_zones()
                # early-returns before adding that column, so the read-back raises
                # a bare KeyError. Only KeyError is caught so an unrelated error
                # isn't misattributed here, matching
                # create_tracks_view_from_envelopes()'s tile aggregation below.
                logger.info(f"No envelope/school intersections at {wind_th} ({threshold_column}), defaulting probability to 0.0: {e}")
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


def create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes, threshold_column='wind_threshold',
                                              precomputed_buff=None):
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
        threshold_column: 'wind_threshold' or 'gust_threshold'.
        precomputed_buff: Optional, an already-filtered-deduplicated-and-
            buffered version of gdf_hcs (see _precompute_facility_buffer()'s
            own docstring), reused instead of filtering/deduplicating/
            buffering gdf_hcs again here. None (the default) computes it
            here.

    Returns:
        dict: Maps wind threshold (int, knots) → GeoDataFrame with one row per facility,
              containing 'zone_id' (= osm_id) and 'probability' (0.0-1.0).
              Empty dict if gdf_hcs is empty or invalid.
    """
    # Validate input is a GeoDataFrame with usable geometry, matching the same
    # guards create_school_view_from_envelopes/create_shelter_view_from_envelopes/
    # create_wash_view_from_envelopes all apply: without this, a malformed
    # gdf_hcs (e.g. a corrupted or older cache read back without an active
    # geometry column) would crash uncaught inside buffer_geodataframe() below
    # instead of returning {} gracefully like its siblings.
    if not isinstance(gdf_hcs, gpd.GeoDataFrame):
        logger.error(f"gdf_hcs must be a GeoDataFrame, got {type(gdf_hcs)}. Returning empty views.")
        return {}
    if not gdf_hcs.empty and ('geometry' not in gdf_hcs.columns or gdf_hcs.geometry.isna().all()):
        logger.error("Health center GeoDataFrame has no valid geometry column. Returning empty views.")
        return {}

    if precomputed_buff is not None:
        gdf_hcs_buff = precomputed_buff
    else:
        gdf_hcs = _filter_hcs_to_relevant_types(gdf_hcs)

        if gdf_hcs.empty:
            logger.warning(f"No health facilities matching {HC_FACILITY_TYPES}, returning empty impact views")
            return {}

        gdf_hcs = _ensure_unique_zone_ids(gdf_hcs, 'osm_id', 'health center')
        gdf_hcs_buff = buffer_geodataframe(gdf_hcs, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}

    num_ensembles = FULL_ENSEMBLE_SIZE
    wind_ths = list(gdf_envelopes[threshold_column].unique())

    # Built ONCE and reused across every wind_th, `_view` reset before each
    # reuse: see create_school_view_from_envelopes()'s own comment above for
    # the full "why" (identical redundancy, same optimization).
    hcs_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_hcs_buff, zone_id_column='osm_id')
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            hcs_viewer._view = None
            try:
                new_col = hcs_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except KeyError as e:
                # See create_school_view_from_envelopes() above for the full
                # explanation: a bare KeyError here means zero envelope/HC
                # intersections at this threshold, a normal expected outcome,
                # not a real failure.
                logger.info(f"No envelope/health-center intersections at {wind_th} ({threshold_column}), defaulting probability to 0.0: {e}")
                probs = {k: 0.0 for k in hcs_viewer.view['zone_id'].unique()}
            hcs_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = hcs_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views


def create_shelter_view_from_envelopes(gdf_shelters, gdf_envelopes, threshold_column='wind_threshold',
                                        precomputed_buff=None):
    """
    Create per-facility shelter impact views from hurricane envelopes.

    For each wind speed threshold, calculates the probability that each shelter
    will be affected by winds at or above that threshold.

    All cached shelter types enter impact files (no type filtering).
    Impact views saved as: shelter_views/<COUNTRY>_<STORM>_<DATE>_<WINDTH>.parquet

    Args:
        gdf_shelters: GeoDataFrame of shelter locations. Must have 'osm_id' column.
        gdf_envelopes: GeoDataFrame of hurricane envelopes.
        threshold_column: 'wind_threshold' or 'gust_threshold'.
        precomputed_buff: Optional, an already-deduplicated-and-buffered
            version of gdf_shelters (see _precompute_facility_buffer()'s own
            docstring), reused instead of deduplicating/buffering gdf_shelters
            again here. None (the default) computes it here.

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

    if precomputed_buff is not None:
        gdf_shelters_buff = precomputed_buff
    else:
        gdf_shelters = _ensure_unique_zone_ids(gdf_shelters, 'osm_id', 'shelter')
        gdf_shelters_buff = buffer_geodataframe(gdf_shelters, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}
    num_ensembles = FULL_ENSEMBLE_SIZE
    # Built ONCE and reused across every wind_th, `_view` reset before each
    # reuse: see create_school_view_from_envelopes()'s own comment above for
    # the full "why" (identical redundancy, same optimization).
    viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_shelters_buff, zone_id_column='osm_id')
    for wind_th in gdf_envelopes[threshold_column].unique():
        gdf_env_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_env_wth.empty:
            viewer._view = None
            try:
                new_col = viewer.map_polygons(gdf_env_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except KeyError as e:
                # See create_school_view_from_envelopes() above for the full
                # explanation: a bare KeyError here means zero envelope/shelter
                # intersections at this threshold, a normal expected outcome,
                # not a real failure.
                logger.info(f"No envelope/shelter intersections at {wind_th} ({threshold_column}), defaulting probability to 0.0: {e}")
                probs = {k: 0.0 for k in viewer.view['zone_id'].unique()}
            viewer.add_variable_to_view(probs, 'probability')
            wind_views[wind_th] = viewer.to_geodataframe()
    return wind_views


def create_wash_view_from_envelopes(gdf_wash, gdf_envelopes, threshold_column='wind_threshold',
                                     precomputed_buff=None):
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
        threshold_column: 'wind_threshold' or 'gust_threshold'.
        precomputed_buff: Optional, an already-deduplicated-and-buffered
            version of gdf_wash (see _precompute_facility_buffer()'s own
            docstring), reused instead of deduplicating/buffering gdf_wash
            again here. None (the default) computes it here.

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

    if precomputed_buff is not None:
        gdf_wash_buff = precomputed_buff
    else:
        gdf_wash = _ensure_unique_zone_ids(gdf_wash, 'osm_id', 'WASH facility')
        gdf_wash_buff = buffer_geodataframe(gdf_wash, buffer_distance_meters=BUFFER_DISTANCE_METERS)
    wind_views = {}
    num_ensembles = FULL_ENSEMBLE_SIZE
    # Built ONCE and reused across every wind_th, `_view` reset before each
    # reuse: see create_school_view_from_envelopes()'s own comment above for
    # the full "why" (identical redundancy, same optimization).
    viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_wash_buff, zone_id_column='osm_id')
    for wind_th in gdf_envelopes[threshold_column].unique():
        gdf_env_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_env_wth.empty:
            viewer._view = None
            try:
                new_col = viewer.map_polygons(gdf_env_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except KeyError as e:
                # See create_school_view_from_envelopes() above for the full
                # explanation: a bare KeyError here means zero envelope/WASH
                # intersections at this threshold, a normal expected outcome,
                # not a real failure.
                logger.info(f"No envelope/WASH intersections at {wind_th} ({threshold_column}), defaulting probability to 0.0: {e}")
                probs = {k: 0.0 for k in viewer.view['zone_id'].unique()}
            viewer.add_variable_to_view(probs, 'probability')
            wind_views[wind_th] = viewer.to_geodataframe()
    return wind_views


def build_tiles_viewer(gdf_tiles, zone_id_column='tile_id'):
    """
    Construct a GeometryBasedZonalViewGenerator for gdf_tiles, exposed so
    callers OUTSIDE this module (main_pipeline.py's own per-country precip/
    river loops) can build one ONCE per country and pass it into every
    create_precip_tile_view()/create_precip_admin_view()/
    create_river_tile_view() call for that country via their own
    `tiles_viewer` param, instead of each of those ~20-24 calls (one per
    window x threshold for precip, one per rp_tier x leadtime step for
    river) independently reconstructing the same GeometryBasedZonalView
    Generator for the same unchanging gdf_tiles. See create_precip_tile_
    view()'s own `tiles_viewer` docstring for the full "why" of what this
    construction costs and why repeating it is real, measurable overhead.

    Safe to reuse across calls for different admin levels too, as long as
    each admin level's own gdf_tiles_for_admin object gets its own viewer
    (built from that object, not the whole-country one): the cost being
    avoided is per-object, not per-country.

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (or an admin-level tile
            GeoDataFrame from admins_overlay()), used verbatim as `zone_data`.
        zone_id_column: column in gdf_tiles identifying each zone. Always
            'tile_id' for every real caller today (mercator/admin tile grids
            all use this column name), kept as a parameter rather than
            hardcoded only so this function's contract doesn't silently
            assume it.

    Returns:
        GeometryBasedZonalViewGenerator: freshly constructed, `_view` unset
            (i.e. in the exact same state passing it straight into
            create_precip_tile_view()/create_river_tile_view() as their own
            internal `tiles_viewer = GeometryBasedZonalViewGenerator(...)`
            line would produce).
    """
    return GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column=zone_id_column)


def create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes, threshold_column='wind_threshold', bitmask_views=None):
    """
    Create mercator tile impact views from hurricane envelopes.

    For each wind speed threshold, calculates expected impacts (E_*) for each tile,
    where expected impact = base value * probability of impact.

    Args:
        gdf_tiles: GeoDataFrame containing mercator tiles with demographic/infrastructure data
        gdf_envelopes: GeoDataFrame containing hurricane envelope geometries with wind_threshold column
        threshold_column: 'wind_threshold' or 'gust_threshold'.
        bitmask_views: Optional, an already-computed {threshold: DataFrame}
            result of calculate_tile_member_bitmask(), reused instead of
            recomputing it here. None (the default) computes it here.

    Returns:
        dict: Dictionary mapping wind threshold (int) to DataFrame with tile impact data.
              Each DataFrame contains probability and E_* columns for expected impacts.
    """
    # Population columns are a hard init-time requirement, not optional data: a
    # country missing one needs `--type patch --columns <col>`, an operational
    # data-completeness problem, not something to silently route around by
    # defaulting E_<col> to NaN. Fail loudly and immediately, matching
    # create_tracks_view_from_envelopes()'s precedent for adolescent_population:
    # for wind this propagates to the per-country try/except in
    # run_complete_impact_analysis() (failing only that country); for gust it's
    # caught by that call's own dedicated try/except and logged as a warning.
    # The other data_cols entries (built_surface_m2, schools/hcs/shelters/wash,
    # smod, rwi) stay NaN-tolerant below.
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

    # Canonical per-tile-per-member coverage bitmask, computed ONCE for every
    # threshold in gdf_envelopes (shared tile reprojection + centroid extraction
    # cost, see calculate_tile_member_bitmask()'s own docstring), instead of each
    # threshold running its own gpd.sjoin(). Reused, not recomputed, when the
    # caller already has one: this function, create_admin_view_from_envelopes_new(),
    # and create_tracks_view_from_envelopes() all need the identical bitmask for
    # the same (gdf_envelopes, gdf_tiles, threshold_column);
    # create_views_from_envelopes_in_country() computes it once per hazard and
    # passes it to all three instead of each recomputing this expensive
    # per-member rasterize pass independently (a real OOM contributor at
    # MEX/PHL scale, found live in production).
    if bitmask_views is None:
        bitmask_views = calculate_tile_member_bitmask(gdf_envelopes, gdf_tiles,
                                                       threshold_column=threshold_column,
                                                       label='mercator tile')

    # Built ONCE and reused across every wind_th below, `_view` reset before
    # each reuse: see create_school_view_from_envelopes()'s comment for the
    # full "why" (identical optimization). gdf_tiles never changes across this
    # loop, only bm_df/probs does, so the expensive rename+copy+zone_dict setup
    # runs once here instead of once per threshold (and again, independently,
    # in create_admin_view_from_envelopes_new() below for admin_level 1, on the
    # same gdf_tiles object).
    tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            tiles_viewer._view = None
            bm_df = bitmask_views.get(int(wind_th))
            if bm_df is None:
                logger.warning(f"tile_member_bitmask FAILED for {threshold_column}={wind_th}, defaulting probabilities to 0")
                probs = {k: 0.0 for k in tiles_viewer.view['zone_id'].unique()}
            else:
                probs = _tile_probability_from_bitmask(bm_df, gdf_tiles['tile_id'], float(num_ensembles))
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

            # Keep poverty columns last so CSV positions $2-$16 are identical
            # to the 16-column format. The procedure's IFF($13 IS NULL) detection
            # continues to work; $17/$18 are ignored by the current INSERT.
            _pov_cols = [c for c in ['moderate_poverty_prob', 'severe_poverty_prob'] if c in df_view.columns]
            if _pov_cols:
                df_view = df_view[[c for c in df_view.columns if c not in _pov_cols] + _pov_cols]

            wind_views[wind_th] = df_view

    return wind_views


def create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles, gdf_envelopes, threshold_column='wind_threshold', bitmask_views=None,
                                          precomputed_tile_views=None):
    """
    precomputed_tile_views: Optional {threshold: DataFrame} dict, the exact
        return shape of create_mercator_view_from_envelopes(), to reuse for
        admin level 1 instead of rebuilding the same per-tile probability/E_*
        DataFrame from scratch. Admin level 1's own gdf_tiles is always the
        exact same object as the caller's whole-country mercator gdf_tiles
        (see create_views_from_envelopes_in_country()'s own
        gdf_tiles_for_admin_by_level construction: `gdf_tiles_for_admin =
        gdf_tiles` for admin_level == 1), and this function is always called
        with the same bitmask_views the sibling mercator call already used,
        so for that level every per-tile quantity below is byte-identical to
        what create_mercator_view_from_envelopes() already computed moments
        earlier in the same per-country pass. Unlike precip's own analogous
        create_precip_admin_view() reuse (which still needs its own
        gdf_tiles-derived id_mapping even when reusing, because precip's tile
        view is a raw grid sample keyed only by zone_id), this one needs no
        remapping step: the mercator view's own tiles_viewer.view already
        carries every original gdf_tiles column straight through, 'id' (this
        admin level's own admin id per tile) included. A copy is taken per
        threshold, never the caller's own object, since the caller
        (create_views_from_envelopes_in_country()) still saves that same
        DataFrame as the mercator tile view afterward. None (the default) or
        a threshold missing from this dict falls back to computing that
        threshold's tile view directly here.
    """
    # Population columns are a hard init-time requirement, not optional data:
    # a country missing one needs `--type patch --columns <col>`, not a silent
    # E_<col> NaN default (0 once aggregated to admin level). Fail loudly, same
    # handling as create_mercator_view_from_envelopes()'s identical check above.
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

    # Canonical per-tile-per-member coverage bitmask, computed ONCE per threshold
    # in gdf_envelopes; see create_mercator_view_from_envelopes()'s identical
    # comment for the full "why" (shared reprojection cost, OOM risk if
    # recomputed independently by each caller). Tile-level probability only;
    # aggregation up to admin regions (area-mean) happens further down,
    # unchanged (see the NOTE after calculate_tile_member_bitmask()'s own
    # module-level comment for why an admin-region-level bitmask is wrong here).
    if bitmask_views is None:
        bitmask_views = calculate_tile_member_bitmask(gdf_envelopes, gdf_tiles,
                                                       threshold_column=threshold_column,
                                                       label='admin tile')

    # Admin IDs must be present in gdf_tiles (added during initialization or on
    # load). Checked once, and the tile_id -> admin id mapping built once here,
    # since gdf_tiles is never reassigned inside the loop below: rebuilding the
    # same dict once per wind_th would repeat identical work, the same
    # redundancy the bitmask_views hoist above already avoids.
    if 'id' not in gdf_tiles.columns:
        raise ValueError(
            "Mercator view missing admin IDs."
            "Admin IDs are added during initialization. "
            "Re-initialize the country or check the mercator view file."
        )
    id_mapping = gdf_tiles.set_index('tile_id')['id'].to_dict()

    # Built ONCE and reused across every wind_th below, `_view` reset before
    # each reuse: same rationale as the id_mapping hoist above (gdf_tiles never
    # reassigned inside the loop); see create_school_view_from_envelopes()'s
    # comment for the GeometryBasedZonalViewGenerator construction cost itself.
    tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            # Admin level 1 reuse: precomputed_tile_views holds the sibling
            # mercator call's already-computed per-tile probability/E_* DataFrame
            # for this exact threshold. When present, skip the bitmask-to-
            # probability derivation and tiles_viewer rebuild below (see this
            # function's own docstring). Copied, not used directly, since the
            # caller still saves this same DataFrame as the mercator tile view.
            tile_view_th = precomputed_tile_views.get(int(wind_th)) if precomputed_tile_views is not None else None
            if tile_view_th is not None:
                df_view = tile_view_th.copy()
            else:
                tiles_viewer._view = None
                bm_df = bitmask_views.get(int(wind_th))
                if bm_df is None:
                    logger.warning(f"tile_member_bitmask FAILED for {threshold_column}={wind_th}, defaulting probabilities to 0")
                    probs = {k: 0.0 for k in tiles_viewer.view['zone_id'].unique()}
                else:
                    probs = _tile_probability_from_bitmask(bm_df, gdf_tiles['tile_id'], float(num_ensembles))
                tiles_viewer.add_variable_to_view(probs, 'probability')

                df_view = tiles_viewer.to_dataframe()
                for col in data_cols:
                    if col in df_view.columns:
                        df_view[f"E_{col}"] = df_view[col] * df_view['probability']
                    else:
                        df_view[f"E_{col}"] = np.nan
                        logger.debug(f"Column '{col}' missing from tile data, E_{col} set to NaN (re-initialize country to populate)")

                df_view = df_view.drop(columns=[c for c in data_cols if c in df_view.columns])

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
                
                # Map zone_id (tile_id) to admin id (id_mapping built once above the loop)
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


def _map_points_exploded(gdf_zones, zone_id_column, points, predicate='intersects'):
    """Counts how many points intersect each zone, after splitting every
    zone geometry into its individual disjoint polygon parts first.

    A zone geometry can be a MultiPolygon scattered across a large area as
    many disjoint parts, for example a low threshold gust envelope member.
    The spatial index used to find candidate points for a zone prunes using
    that zone's own bounding box. A zone scattered across a wide area has an
    enormous bounding box, so the index cannot prune anything for it, and
    every point in the country ends up paying a full geometric check
    against it.

    Splitting each zone into one row per disjoint part first gives every
    row its own small, real bounding box, so the index can prune correctly.
    Because a zone's own parts never overlap each other, summing the counts
    from its parts back together after aggregation gives the exact same
    total for that zone. No geometry is changed, only reorganized for the
    index.

    Returns a dictionary mapping each real zone id to its point count.
    """
    rows = []
    for _, row in gdf_zones.iterrows():
        geom = row.geometry
        parts = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]
        for part in parts:
            r = row.copy()
            r['geometry'] = part
            rows.append(r)
    exploded = gpd.GeoDataFrame(rows, crs=gdf_zones.crs).reset_index(drop=True)
    exploded['_explode_zone_id'] = range(len(exploded))

    result = aggregate_points_to_zones(
        points=points, zones=exploded, aggregation='count',
        point_zone_predicate=predicate, zone_id_column='_explode_zone_id',
    )
    # A zone's own exploded parts never overlap each other, since they are
    # the same MultiPolygon's own disjoint pieces, so summing point_count
    # across them is an exact recombination and does not count anything
    # twice.
    return result.groupby(zone_id_column)['point_count'].sum().to_dict()


def create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member',
                                       gdf_shelters=None, gdf_wash=None, threshold_column='wind_threshold', bitmask_views=None,
                                       schools_buff=None, hcs_buff=None, shelters_buff=None, wash_buff=None):
    """
    Create tracks impact views from envelopes.

    schools_buff/hcs_buff/shelters_buff/wash_buff are each an optional
    already-buffered version of the correspondingly-named gdf_* argument (see
    _precompute_facility_buffer()'s own docstring): create_views_from_
    envelopes_in_country() computes each of these once per facility type and
    passes the same buffer into both this function and its own sibling
    create_school_view_from_envelopes()/create_health_center_view_from_
    envelopes()/create_shelter_view_from_envelopes()/create_wash_view_from_
    envelopes() calls (for both wind and, when present, gust), instead of
    every one of those calls independently re-running buffer_geodataframe()
    on the exact same input. None (the default for each) computes it here,
    for any caller that doesn't have one handy.
    """
    wind_views = {}

    # Buffer facility points and use predicate='intersects', matching every
    # sibling facility view (create_school_view_from_envelopes,
    # create_health_center_view_from_envelopes, etc.); map_points() defaults to
    # predicate='within' on raw points, unlike map_polygons()'s 'intersects'
    # default the siblings rely on, so without this a facility near an envelope
    # boundary could be counted impacted in school_views/hc_views but not here,
    # producing internally-inconsistent severity numbers for the same run.
    if schools_buff is not None:
        gdf_schools_buff = schools_buff
    else:
        gdf_schools_buff = (buffer_geodataframe(gdf_schools, buffer_distance_meters=BUFFER_DISTANCE_METERS)
                            if gdf_schools is not None and not gdf_schools.empty else gdf_schools)
    if hcs_buff is not None:
        gdf_hcs_buff = hcs_buff
    else:
        # Filtered the same way create_health_center_view_from_envelopes filters gdf_hcs,
        # so severity_hcs agrees with hc_views' own num_hcs for the same run.
        gdf_hcs_filtered = _filter_hcs_to_relevant_types(gdf_hcs)
        gdf_hcs_buff = (buffer_geodataframe(gdf_hcs_filtered, buffer_distance_meters=BUFFER_DISTANCE_METERS)
                        if gdf_hcs_filtered is not None and not gdf_hcs_filtered.empty else gdf_hcs_filtered)
    if shelters_buff is not None:
        gdf_shelters_buff = shelters_buff
    else:
        gdf_shelters_buff = (buffer_geodataframe(gdf_shelters, buffer_distance_meters=BUFFER_DISTANCE_METERS)
                            if gdf_shelters is not None and not gdf_shelters.empty else gdf_shelters)
    if wash_buff is not None:
        gdf_wash_buff = wash_buff
    else:
        gdf_wash_buff = (buffer_geodataframe(gdf_wash, buffer_distance_meters=BUFFER_DISTANCE_METERS)
                         if gdf_wash is not None and not gdf_wash.empty else gdf_wash)

    if "adolescent_population" not in gdf_tiles.columns:
        raise ValueError(
            "gdf_tiles is missing 'adolescent_population', run "
            "'--type patch --columns adolescent_population' for this country "
            "before track severity views can be computed."
        )
    tile_value_columns = ["population", "school_age_population", "infant_population",
                           "built_surface_m2", "adolescent_population"]
    tile_values = gdf_tiles[['tile_id'] + tile_value_columns]

    wind_ths = list(gdf_envelopes[threshold_column].unique())

    # Canonical per-tile-per-member coverage bitmask, computed ONCE per threshold;
    # see create_mercator_view_from_envelopes()'s identical comment on this
    # bitmask_views parameter for the full "why". Each member's own rasterize
    # call is naturally bounded to its own envelope extent, so no separate
    # tile-narrowing step is needed here.
    if bitmask_views is None:
        bitmask_views = calculate_tile_member_bitmask(gdf_envelopes, gdf_tiles,
                                                       threshold_column=threshold_column,
                                                       label='tracks tile')

    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes[threshold_column] == int(wind_th)]

        tracks_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_envelopes_wth, zone_id_column=index_column)

        # Schools
        schools = _map_points_exploded(gdf_envelopes_wth, index_column, gdf_schools_buff, predicate='intersects')
        tracks_viewer.add_variable_to_view(schools, "severity_schools")

        # Health centers
        hcs = _map_points_exploded(gdf_envelopes_wth, index_column, gdf_hcs_buff, predicate='intersects')
        tracks_viewer.add_variable_to_view(hcs, "severity_hcs")

        # Shelters
        _nan_members = {m: float('nan') for m in gdf_envelopes_wth[index_column].unique()}
        if gdf_shelters_buff is None or gdf_shelters_buff.empty:
            tracks_viewer.add_variable_to_view(_nan_members, "severity_num_shelters")
        else:
            tracks_viewer.add_variable_to_view(_map_points_exploded(gdf_envelopes_wth, index_column, gdf_shelters_buff, predicate='intersects'), "severity_num_shelters")

        # WASH facilities
        if gdf_wash_buff is None or gdf_wash_buff.empty:
            tracks_viewer.add_variable_to_view(_nan_members, "severity_num_wash")
        else:
            tracks_viewer.add_variable_to_view(_map_points_exploded(gdf_envelopes_wth, index_column, gdf_wash_buff, predicate='intersects'), "severity_num_wash")

        members_present = gdf_envelopes_wth[index_column].unique()
        _zero_members = {m: 0.0 for m in members_present}

        # Per-member SUM over tiles covered by that member's own envelope,
        # derived from the same canonical bitmask create_mercator_view_
        # from_envelopes()/create_admin_view_from_envelopes_new() also use. A
        # member with zero tile coverage gets NaN, not 0.0 (matching
        # aggregate_polygons_to_zones()'s "no matching rows" -> NaN fill for
        # "sum"); only a threshold with ZERO tiles covered by every
        # member falls back to 0.0 for all (bm_df.empty or a rasterize failure).
        bm_df = bitmask_views.get(int(wind_th))
        if bm_df is None:
            logger.warning(f"tile_member_bitmask FAILED for {threshold_column}={wind_th}, defaulting severity to 0")
            overlays = {col: dict(_zero_members) for col in tile_value_columns}
        elif bm_df.empty:
            overlays = {col: dict(_zero_members) for col in tile_value_columns}
        else:
            merged = tile_values.merge(bm_df, on='tile_id', how='inner')
            bits = merged['bits'].to_numpy()
            overlays = {}
            for col in tile_value_columns:
                vals = merged[col].to_numpy(dtype=float)
                col_sums = {}
                for m in members_present:
                    bitpos = np.uint64(1) << np.uint64(int(m) - 1)
                    covered = (bits & bitpos) != 0
                    col_sums[m] = float(np.nansum(vals[covered])) if covered.any() else float('nan')
                overlays[col] = col_sums
        tracks_viewer.add_variable_to_view(overlays.get('population', _zero_members), "severity_population")
        tracks_viewer.add_variable_to_view(overlays.get('adolescent_population', _zero_members), "severity_adolescent_population")
        tracks_viewer.add_variable_to_view(overlays.get('school_age_population', _zero_members), "severity_school_age_population")
        tracks_viewer.add_variable_to_view(overlays.get('infant_population', _zero_members), "severity_infant_population")
        tracks_viewer.add_variable_to_view(overlays.get('built_surface_m2', _zero_members), "severity_built_surface_m2")

        gdf_view = tracks_viewer.to_geodataframe()
        wind_views[wind_th] = gdf_view

    return wind_views



# =============================================================================
# ADMIN COUNTRY LAYER
# Logically part of Base Layer Initialization. Mirrors create_mercator_country_layer
# but aggregates at admin level N.
# =============================================================================
def create_admin_country_layer(country, rewrite=0, admin_level=1, fast_fail=False):
    """
    Create an admin-level layer with demographic and infrastructure data for a country.

    Fallback used during `--type update` when the base admin parquet is missing. Produces
    one row per admin boundary with all data columns aggregated to that level.
    Applies the same data requirements as create_mercator_country_layer():

    fast_fail: forwarded to fetch_shelters()/fetch_wash() (see their own docstrings and
        create_mercator_country_layer()'s own fast_fail note). Defaults to the patient
        behavior; the `--type update` fallback caller passes True.

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
                         infrastructure columns, indexed by 'tile_id'.
    """
    # Fetch facility locations: custom data priority handled inside fetch_*
    gdf_schools = fetch_schools(country, rewrite, fast_fail=fast_fail)
    gdf_hcs = fetch_health_centers(country, rewrite, fast_fail=fast_fail)
    gdf_shelters = fetch_shelters(country, rewrite, fast_fail=fast_fail)
    gdf_wash = fetch_wash(country, rewrite, fast_fail=fast_fail)

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
    # tiles_viewer.view's own DataFrame index is a positional RangeIndex, not zone_id
    # (zone_id is a regular column); must re-key by zone_id before .map()/.to_dict(),
    # since add_variable_to_view() joins the resulting dict by zone_id.
    try:
        smod_l2 = tiles_viewer.view.set_index('zone_id')['smod_class']
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

def project_tiles_for_facility_assignment(gdf_tiles):
    """
    Reproject gdf_tiles (tile_id + geometry only) to ESRI:54009 once, exposed
    so callers OUTSIDE this module (main_pipeline.py's own per-country river
    loop) can build it ONCE per country and pass it into every
    assign_facilities_to_tiles() call for that country via that function's
    own `tiles_proj` param, instead of each call (up to 4 per country - one
    per facility type: schools/health centers/shelters/WASH) independently
    reprojecting the identical, unchanging whole-country tile grid whenever
    that call happens to have any unmatched (border/coastal) facility. Same
    "build once, reuse many" convention as build_tiles_viewer() above (see
    that function's own docstring for the full "why" of avoiding this kind
    of repeated per-call reconstruction).

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (tile_id + geometry).

    Returns:
        GeoDataFrame: gdf_tiles[['tile_id', 'geometry']] reprojected to
            ESRI:54009 - byte-identical to what assign_facilities_to_tiles()
            would build internally for the same gdf_tiles when no
            `tiles_proj` is passed in.
    """
    return gdf_tiles[['tile_id', 'geometry']].to_crs("ESRI:54009")


def assign_facilities_to_tiles(gdf_facilities, gdf_tiles, id_column, tiles_proj=None):
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
        tiles_proj: Optional, an already-computed
            project_tiles_for_facility_assignment(gdf_tiles) result, reused
            instead of reprojecting gdf_tiles to ESRI:54009 here. None (the
            default, and what every caller other than main_pipeline.py's
            river worker passes) reprojects internally -
            required only when `unmatched` below is non-empty, so a caller
            that never passes this still pays that cost at most once, same
            as pre-existing behaviour. When passed, it must have been built
            from this same gdf_tiles (not a different country/admin-level
            tile grid) - see project_tiles_for_facility_assignment()'s own
            docstring.

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
        tiles_proj_local = tiles_proj if tiles_proj is not None else project_tiles_for_facility_assignment(gdf_tiles)
        nearest = gpd.sjoin_nearest(unmatched_facilities, tiles_proj_local, how='left').drop_duplicates(subset=id_column, keep='first')
        # Only tile_id, not geometry: nearest's own geometry is the equal-area
        # reprojection above (metres, ESRI:54009), carrying it into `joined`
        # would silently mislabel that facility's coordinates while `joined`
        # still declares itself EPSG:4326.
        joined = joined.set_index(id_column)
        joined.update(nearest.set_index(id_column)[['tile_id']])
        joined = joined.reset_index()

    return joined[[id_column, 'tile_id']]



def save_tiles_view(gdf, country, storm, date, wind_th, zoom_level, dataset='wind'):
    """
    Saves tiles views. dataset='gust' writes to mercator_views_gust/ with a
    'g'-prefixed threshold token, keeping gust files structurally distinct
    from wind (own directory, no numeric-threshold collision).
    """
    dir_name = 'mercator_views_gust' if dataset == 'gust' else 'mercator_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}_{zoom_level}.csv"
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name)
    # Thin copy for the file only; the caller's dense frame is untouched.
    # A failure here falls back to the dense file rather than losing the view.
    try:
        frame = sparsify_tile_frame(gdf).reset_index(drop=True)
        logger.info(f"    Sparse {dataset} tile view {file_name}: {describe_reduction(gdf, frame)}")
    except Exception as e:
        frame = gdf
        logger.warning(f"    Sparse {dataset} tile view {file_name} not applied, writing the dense file: {e}")
    write_dataset(frame, data_store, out_path)
    return out_path


def _facility_view_for_write(gdf, dataset, label):
    """
    Frame handed to a facility view writer: zero-probability rows are dropped
    from a copy. The result may be empty; the file is still written so
    existence checks and stale-file cleanup see it.
    """
    try:
        # Contiguous index, so the parquet file carries no extra index column.
        frame = sparsify_facility_frame(gdf).reset_index(drop=True)
    except Exception as e:
        logger.warning(f"    Sparse {dataset} {label} not applied, writing the dense file: {e}")
        return gdf
    logger.debug(f"    Sparse {dataset} {label}: {describe_reduction(gdf, frame)}")
    return frame


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
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path

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
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path


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
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path


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
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path


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
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path


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
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name)
    write_dataset(gdf, data_store, out_path)
    return out_path


def save_tracks_tile_bitmask_view(df, country, storm, date, wind_th, dataset='wind'):
    """
    Saves per-tile, per-member coverage bitmask views (see
    calculate_tile_member_bitmask's own docstring for what these are and
    why). Mirrors save_tracks_view's own dataset='gust' convention exactly
    (g-prefixed threshold token, separate _gust directory); persists this
    per-tile, per-member data as a real pipeline output.
    """
    dir_name = 'track_tile_bitmask_views_gust' if dataset == 'gust' else 'track_tile_bitmask_views'
    th_token = f"g{wind_th}" if dataset == 'gust' else f"{wind_th}"
    file_name = f"{country}_{storm}_{date}_{th_token}.parquet"
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, dir_name, file_name)
    write_dataset(df, data_store, out_path)
    return out_path


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
    out_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', file_name)
    write_dataset(df, data_store, out_path)
    return out_path


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
            - CCI_children: Child Cyclone Index for children (0-19: infants + school-age + adolescents)
            - E_CCI_children: Expected CCI for children
            - CCI_school_age: CCI for school-age population
            - E_CCI_school_age: Expected CCI for school-age population
            - CCI_infants: CCI for infant population
            - E_CCI_infants: Expected CCI for infants
            - CCI_adolescents: CCI for adolescent population
            - E_CCI_adolescents: Expected CCI for adolescents
            - CCI_pop: CCI for total population
            - E_CCI_pop: Expected CCI for total population
        Also includes 'zone_id' (tile_id) and 'id' (admin boundary ID).

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

    # Children cci (0-19: school_age 5-14 + infants 0-4 + adolescents 15-19)
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = ((gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'] + gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'] + gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)).clip(lower=0.0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'] + gdf_tiles_index['adolescent_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col] * math.pow(int(col), 2) * CCI_WEIGHT_MULTIPLIER 
             for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_children'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children']]

    # Children e cci (0-19: school_age 5-14 + infants 0-4 + adolescents 15-19)
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
          rate = moderate_poverty_prob × (1 - t) + t,  where t = (band - 50) / (96 - 50)
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
      - Highest band 50-96 kt → rate linearly interpolates moderate_poverty_prob → 1.0
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
            severity_infant_in_need    : infants (0-4)
            severity_school_age_in_need: school age (5-14)
            severity_adolescent_in_need: adolescents (15-19)
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
            # sorted(), not list(): tile_id set iteration order is hash-seed-dependent
            # (PYTHONHASHSEED unset), and float64 summation isn't associative, so an
            # unsorted order made the final severity sums drift by ~1e-11 across processes.
            covered_list = sorted(covered)
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
        # being silently dropped from the sum: the same "don't let a missing
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


# =============================================================================
# RASTERIZED ENVELOPE COVERAGE. Rasterizes each ensemble member's threshold-
# contour polygon (still produced by TC-ECMWF-Forecast-Pipeline's
# matplotlib.contour() interpolation, unchanged) onto a grid aligned to the
# Web Mercator zoom-14 XYZ tile lattice, so the canonical per-member,
# per-tile coverage computation (calculate_tile_member_bitmask, below) can be
# computed once and reused by create_tracks_view_from_envelopes/
# create_mercator_view_from_envelopes/create_admin_view_from_envelopes_new
# instead of each running its own GEOS gpd.sjoin() for the same "does tile T
# fall under member M's envelope" question. With all_touched=True and the
# grid snapped to the lattice (not the polygon's bbox), this reproduces the
# same gpd.sjoin ground truth exactly, while running 5-10x faster per member.
# =============================================================================

# Fixed Web Mercator constants (EPSG:3857): world half-circumference (anchors
# the tile lattice's origin so every rasterize grid aligns to the same tile
# boundaries regardless of member/threshold/storm) and the zoom-14 tile size
# in meters, matching the same XYZ tile pyramid the tile server/mercator
# views are built on.
_WEB_MERCATOR_WORLD_ORIGIN_X = -20037508.342789244
_WEB_MERCATOR_WORLD_ORIGIN_Y = 20037508.342789244
_WEB_MERCATOR_CIRCUMFERENCE_M = 40075016.6855785


def _z14_tile_size_m(zoom_level: int = 14) -> float:
    """Real Web Mercator tile size in meters at the given zoom level (a tile
    is a fixed-size square everywhere in EPSG:3857, unlike in EPSG:4326 where
    tile height in degrees varies by latitude, a real difference,
    not a theoretical concern)."""
    return _WEB_MERCATOR_CIRCUMFERENCE_M / (2 ** zoom_level)


def _rasterize_member_envelope_mask(geom_3857, tile_size_m: float):
    """
    Rasterize one real member's envelope polygon (already in EPSG:3857) onto
    a grid exactly aligned to the real Web Mercator XYZ tile lattice,
    bounded to the polygon's OWN extent (not the whole country). This is
    what makes each call cheap regardless of country size: a "don't process
    area far from the storm" benefit a gpd.sjoin() needs an explicit
    tile-narrowing step to get, but the grid here shrinks on its own as a
    higher threshold's real envelope shrinks.

    Two correctness requirements this function depends on:
    1. A naive plain-lon/lat-degree grid is WRONG: real inspection of zoom-14
       tile geometries shows tile width in degrees is constant, but tile
       HEIGHT in degrees varies by latitude band (an inherent Web Mercator
       property), so a single global dy misaligns rows far from where it was
       measured. The grid works entirely in EPSG:3857, where tiles are
       exact fixed-size squares everywhere.
    2. A rasterize grid anchored to the polygon's own bounding box (not the
       real, fixed-origin XYZ tile lattice) silently offsets every result by
       a tile: pixel centers don't land on real tile centers. Fixed by
       snapping the grid's origin onto the real lattice via floor-division
       against the true Web Mercator world origin.

    Args:
        geom_3857: one real member's envelope geometry, already reprojected
            to EPSG:3857 (Polygon or MultiPolygon).
        tile_size_m: real zoom-14 tile size in meters (_z14_tile_size_m()).

    Returns:
        (mask_arr, transform): mask_arr is a uint8 array, 1 where the real
        polygon covers that cell. all_touched=True is required here: the
        rasterize default, all_touched=False, undercounts at boundaries
        (misses real tiles the true contour genuinely reaches). transform
        maps array row/col indices to real EPSG:3857 coordinates, for
        sampling tile centroids against this mask afterward.
    """
    from rasterio.features import rasterize as _rio_rasterize
    from rasterio.transform import from_origin

    dx = dy = tile_size_m
    minx, miny, maxx, maxy = geom_3857.bounds
    minx = _WEB_MERCATOR_WORLD_ORIGIN_X + np.floor((minx - _WEB_MERCATOR_WORLD_ORIGIN_X) / dx) * dx
    maxy = _WEB_MERCATOR_WORLD_ORIGIN_Y - np.floor((_WEB_MERCATOR_WORLD_ORIGIN_Y - maxy) / dy) * dy
    width = max(1, int(np.ceil((maxx - minx) / dx)) + 2)
    height = max(1, int(np.ceil((maxy - miny) / dy)) + 2)
    transform = from_origin(minx - dx, maxy + dy, dx, dy)

    mask_arr = _rio_rasterize(
        [(geom_3857, 1)],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        all_touched=True,
        dtype='uint8',
    )
    return mask_arr, transform


def _sample_tile_centroids_against_mask(cx: np.ndarray, cy: np.ndarray, mask_arr, transform) -> np.ndarray:
    """
    Sample already-extracted real tile centroid coordinate arrays (EPSG:3857
    x/y, same CRS as the mask) against one member's rasterize mask.

    A tile centroid whose row/col falls outside this specific mask's own
    (deliberately small, polygon-bounded) extent is correctly treated as
    "not covered" via the `valid` bounds check below. This is what lets
    the SAME full cx/cy arrays (extracted once, outside any per-member loop)
    be safely reused across every member's own differently-sized small mask,
    without needing to pre-filter tiles by bbox first.

    Returns:
        Boolean array, same length/order as cx/cy, True where that tile
        centroid falls inside the real rasterized envelope.
    """
    inv_transform = ~transform
    cols, rows = inv_transform * (cx, cy)
    cols = np.floor(cols).astype(int)
    rows = np.floor(rows).astype(int)
    height, width = mask_arr.shape
    valid = (rows >= 0) & (rows < height) & (cols >= 0) & (cols < width)
    inside = np.zeros(len(cx), dtype=bool)
    inside[valid] = mask_arr[rows[valid], cols[valid]].astype(bool)
    return inside


def _popcount_u64(bits_arr: np.ndarray) -> np.ndarray:
    """Vectorized population count (number of set bits) for a uint64 array,
    the standard SWAR bit-trick. Used to turn a calculate_tile_member_bitmask()
    bitmask column into a per-tile member COUNT without a per-row Python
    loop, since the sparse bitmask can itself have hundreds of thousands of
    rows for a wide low-threshold envelope over a large country."""
    x = bits_arr.astype(np.uint64)
    x = x - ((x >> np.uint64(1)) & np.uint64(0x5555555555555555))
    x = (x & np.uint64(0x3333333333333333)) + ((x >> np.uint64(2)) & np.uint64(0x3333333333333333))
    x = (x + (x >> np.uint64(4))) & np.uint64(0x0f0f0f0f0f0f0f0f)
    return ((x * np.uint64(0x0101010101010101)) >> np.uint64(56)).astype(np.uint64)


def _tile_probability_from_bitmask(bm_df, all_tile_ids, num_ensembles: float) -> dict:
    """Per-z14-tile probability (fraction of ensemble members whose envelope
    covers that tile) derived from one threshold's calculate_tile_member_bitmask()
    DataFrame. Shared by create_mercator_view_from_envelopes() and
    create_admin_view_from_envelopes_new(), which both define tile-level
    probability the same way (create_admin_view_from_envelopes_new() then
    aggregates it UP to admin regions by area-mean, unchanged by this helper;
    see the NOTE above calculate_tile_member_bitmask()'s own module-level
    comment block for why an admin-region-level bitmask is NOT the right
    quantity to compute directly).

    bm_df is None (rasterize failure) is the caller's responsibility to check
    before calling this; bm_df.empty (genuine zero-coverage) and the
    non-empty case are both handled here, uniformly returning 0.0 for every
    tile_id in all_tile_ids not present in bm_df.

    Aligned via a vectorized pandas reindex rather than a per-tile Python
    dict comprehension with a `.get()` call per id: called once per real
    threshold (up to 8 for wind, 8 for gust) with all_tile_ids as large as
    ~400k rows for a MEX-sized country, so a per-item Python loop here is
    real, measurable, avoidable overhead of the same kind already fixed in
    precip's own per-point raster sampling.
    """
    if bm_df.empty:
        return {tid: 0.0 for tid in all_tile_ids}
    counts = pd.Series(_popcount_u64(bm_df['bits'].to_numpy()), index=bm_df['tile_id'].to_numpy())
    aligned = counts.reindex(all_tile_ids, fill_value=0).to_numpy(dtype=float) / num_ensembles
    return dict(zip(all_tile_ids, aligned))


def calculate_tile_member_bitmask(gdf_envelopes, gdf_tiles, threshold_column='wind_threshold',
                                  label='tile'):
    """
    Real per-z14-tile, per-ensemble-member 64-bit coverage bitmask, one
    DataFrame per real threshold in gdf_envelopes (bit `m-1` set <=> member
    `m`'s envelope covers that tile).

    Rasterizes each member's own real envelope polygon onto a grid exactly
    aligned to the real Web Mercator zoom-14 tile lattice (see
    _rasterize_member_envelope_mask()'s own docstring for the two
    correctness requirements this depends on for an exact result),
    reproducing the exact same ground truth a gpd.sjoin() would (bit-for-bit
    identical population sums) while running 5-10x faster per member than
    that sjoin.

    This is the one canonical per-tile coverage computation: create_tracks_
    view_from_envelopes()/create_mercator_view_from_envelopes()/
    create_admin_view_from_envelopes_new() all derive their own values from
    it (see their own call sites) instead of each running a separate
    gpd.sjoin(). create_admin_view_from_envelopes_new() consumes it at the
    z14-tile level, same as the mercator view, then aggregates UP to admin
    regions unchanged (area-mean for probability, raw-weighted sum for E_*);
    see the NOTE directly following this function's own module-level comment
    block for why an admin-region-level bitmask would be the wrong quantity
    to compute directly.

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
        doesn't appear, the same sparse convention River's own bitmask uses.
        A threshold with a zero-intersection result (e.g. no member reaches
        137kt over land for this storm) returns an empty DataFrame for that
        threshold, not an error; mirrors create_tracks_view_from_envelopes's
        own zero-intersection fallback.

        A threshold whose sjoin actually fails maps to None, not to an empty
        DataFrame: the two are distinguishable downstream (empty means a
        legitimate "no member reaches this threshold", None means the sjoin
        itself failed and no file should be written for it). Callers MUST
        skip a None (see the two call sites in
        create_views_from_envelopes_in_country, which skip writing a file
        for it). 'tile_id' is emitted with the SAME pandas 'string' dtype in
        the empty and non-empty cases, so both file variants share one
        parquet schema.
    """
    thresholds = sorted(gdf_envelopes[threshold_column].unique())
    bitmask_views = {}

    if gdf_envelopes.empty or gdf_tiles.empty:
        return {th: pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})
                for th in thresholds}

    try:
        # Reprojected + tile centroids extracted ONCE, outside the per-threshold/
        # per-member loop below: this is the one real per-call cost (402k rows
        # for a country the size of MEX), reused for every member at every
        # threshold via _sample_tile_centroids_against_mask()'s own bounds-check
        # design (a member's small mask safely no-ops for tiles outside its own
        # extent, no bbox pre-filter needed).
        tiles_3857 = gdf_tiles[['tile_id', 'geometry']].to_crs(3857)
        tile_ids_arr = tiles_3857['tile_id'].to_numpy()
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message='Geometry is in a geographic CRS')
            _tile_centroids = tiles_3857.geometry.centroid
        cx = _tile_centroids.x.to_numpy()
        cy = _tile_centroids.y.to_numpy()
    except Exception as e:
        # None (not an empty frame) so the caller can tell a real failure from
        # a legitimate zero-intersection; see this function's own Returns
        # section. A reprojection failure here is unrecoverable for every
        # threshold, not just one, unlike the per-threshold rasterize failure
        # handled below.
        logger.error(f"tile_member_bitmask [{label}] tile reprojection FAILED, "
                     f"no bitmask files will be written for this country: {e}")
        return {th: None for th in thresholds}

    tile_size_m = _z14_tile_size_m()

    for th in thresholds:
        envs_th = gdf_envelopes[gdf_envelopes[threshold_column] == int(th)][
            ['ensemble_member', 'geometry']
        ].copy()
        if envs_th.empty:
            bitmask_views[th] = pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})
            continue

        try:
            envs_th_3857 = envs_th.to_crs(3857) if envs_th.crs != 3857 else envs_th
            tile_bits_accum = np.zeros(len(tile_ids_arr), dtype=np.uint64)
            for _, row in envs_th_3857.iterrows():
                geom = row.geometry
                if geom is None or geom.is_empty:
                    continue
                member = int(row['ensemble_member'])
                mask_arr, transform = _rasterize_member_envelope_mask(geom, tile_size_m)
                inside = _sample_tile_centroids_against_mask(cx, cy, mask_arr, transform)
                if inside.any():
                    tile_bits_accum[inside] |= np.uint64(1) << np.uint64(member - 1)
        except Exception as e:
            # None (not an empty frame) so the caller can tell a real
            # failure from a legitimate zero-intersection; see this
            # function's own Returns section.
            logger.error(f"tile_member_bitmask [{label}] rasterize FAILED at {threshold_column}={th}, "
                         f"no bitmask file will be written for it: {e}")
            bitmask_views[th] = None
            continue

        if not tile_bits_accum.any():
            bitmask_views[th] = pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})
            continue

        # Sparse output (only tiles with at least
        # one member's real coverage appear), via a vectorized boolean-mask
        # select.
        nonzero = tile_bits_accum != 0
        bitmask_views[th] = pd.DataFrame({
            # Same pandas 'string' dtype the empty-threshold frames above
            # use, so every parquet this function feeds shares one schema.
            'tile_id': pd.array(tile_ids_arr[nonzero].astype(str), dtype='string'),
            'bits': tile_bits_accum[nonzero],
        })

    return bitmask_views


# NOTE: there is deliberately NO per-ADMIN-REGION member bitmask here.
#
# Running calculate_tile_member_bitmask against admin polygons instead of z14
# tiles (bit `m` set <=> member `m`'s envelope intersects the region anywhere)
# is a real quantity, but not the one the admin layer renders.
# create_admin_view_from_envelopes_new defines an admin region's `probability`
# as the AREA-MEAN of its z14 tiles' probabilities (mean, via avg_cols), and
# its E_* columns as sum_over_tiles(raw_tile x prob_tile). popcount(bits)/51
# does not reproduce that; for a large admin-1 a cyclone only clips, it
# overstates by 10-100x, and the dashboard's combined layer could read higher
# than every single-hazard layer it was built from.
#
# No per-region summary fixes this: an exact cross-hazard union needs
# per-member per-TILE sets, which the z14 bitmask (calculate_tile_member_
# bitmask above, TILE_WIND_BITMASK_MAT/TILE_GUST_BITMASK_MAT) already carries.
# The dashboard unions at z14 and aggregates down to admin regions with the
# same two aggregations used here (mean for probability, raw-weighted sum for
# E_*), covering river/precip for free; see services/tile_server.py's
# _combine_bitmask_aware_admin in the Ahead-of-the-Storm repo.


# =============================================================================
# MAIN IMPACT ANALYSIS ORCHESTRATION
# Top-level function called per country per storm on every --type update run.
# Coordinates all view generation, CCI calculation, and report writing.
# =============================================================================
def create_views_from_envelopes_in_country(country, storm, date, gdf_envelopes, zoom, gdf_envelopes_gust=None,
                                            admin_levels_by_country=None, heartbeat=None, on_wind_complete=None,
                                            skip_wind=False):
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
        admin_levels_by_country: Optional bulk-precomputed {country: [levels]}
            mapping from get_initialized_admin_levels_bulk(), the same shared
            admin_views/ directory listing precip/river's own per-country
            processing already reuses this way. When given, this function
            looks this country's own levels up in it instead of calling
            get_initialized_admin_levels(country) itself, which would re-list
            that same shared, country-independent directory redundantly for
            every country a caller processes in a loop. None (the default,
            for any caller that doesn't have a bulk mapping handy, e.g. a
            direct call outside the pipeline) falls back to the original
            per-country listing so this function stays correct on its own.
        heartbeat: Optional zero-arg callable, invoked after every real save
            checkpoint below (schools, HCs, shelters, WASH, tiles, each admin
            level, tracks, bitmask: both wind and gust). Lets the caller's
            own subprocess supervisor (see _process_one_country_isolated() in
            main_pipeline.py) distinguish "still genuinely working" from "truly
            stalled" instead of using a single flat wall-clock timeout for the
            whole call: see that function's own docstring for the full "why".
            None (the default) is a safe no-op, for any caller (e.g. a
            direct call outside the pipeline) that doesn't need this.
        on_wind_complete: Optional callable(wrote_base_parquet, files_written),
            invoked exactly once, right after wind processing has fully
            succeeded and saved but before gust processing begins (see the
            real call site's own comment for why this exact point). Lets the
            caller's own subprocess supervisor persist wind's already-good
            result BEFORE gust starts, so a later external kill of this whole
            OS process (for example gust hangs, the parent's stall-watchdog
            SIGKILLs the entire subprocess, and wind's already-saved result,
            held only in this process's own memory and not yet returned via
            the result queue, is destroyed along with it) does not have to
            discard a genuinely successful wind outcome. None (the default) is a safe
            no-op, for any caller that doesn't need this.
        skip_wind: If True, skip wind envelope processing entirely (gust
            processing is unaffected either way, and requires gdf_envelopes_gust
            to actually have data or this call does nothing). The shared raw
            inputs gust also needs (facility fetches, tile/admin loading) are
            still loaded, just never turned into wind views/saves. Lets a
            gust-only scheduler invocation reuse this same function without
            redoing wind's own view creation for a country wind has already
            finished. on_wind_complete is never invoked when this is True
            (there is no real wind completion to report).

    Note:
        Base data (mercator tiles, admin views) are loaded if available, or created
        on-the-fly if missing. Admin levels are detected from existing base parquets
        created during --type initialize. Add new levels with --type patch --columns adminN.

    Returns:
        tuple[bool, int, str | None]: (wrote_base_parquet, files_written, gust_error).
            files_written is a real running count of impact files actually saved
            this call (varies with active wind thresholds, admin levels, and gust
            presence), not a fixed placeholder, feeds TC_PIPELINE_RUN_LOG/
            TC_PIPELINE_COMPLETE_LOG's FILES_WRITTEN column. gust_error is None on
            success (including the normal "no gust data for this storm" case) or
            the exception message if gust processing raised; wind views are
            still saved and files_written still reflects them (see the gust
            try/except below), but the caller can surface gust_error into
            TC_PIPELINE_RUN_LOG's error_message so a gust-specific failure is
            visible even on an otherwise STATUS=SUCCESS run, rather than only
            ever reaching a container's own stdout log.
    """
    heartbeat = heartbeat or (lambda: None)
    on_wind_complete = on_wind_complete or (lambda wrote_base, files_written: None)

    # Computed once, reused both for the stale-cleanup dir list below and the
    # gust branch further down, so the two can never drift out of sync.
    process_gust = gdf_envelopes_gust is not None and not gdf_envelopes_gust.empty

    if admin_levels_by_country is not None:
        admin_levels = admin_levels_by_country.get(country) or [1]
    else:
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

    # Stale-threshold cleanup (e.g. a run where 137kt had a few envelope
    # members that have since been cleaned up) happens AFTER all new views are
    # written, not before: deleting first and writing incrementally afterward
    # leaves a window where, if this call is interrupted partway through,
    # already-deleted-but-not-yet-rewritten view types are simply MISSING
    # rather than stale, and this file's convention (data_store.file_exists()
    # checks throughout) treats a missing file as a silent skip, not a loud
    # error - an understated impact number, not a visible failure. Only
    # capture the pre-run file list here; written_paths (populated at every
    # save_*() call below) is diffed against it at the end, and only genuinely
    # stale leftovers (existed before, never rewritten this call) are deleted.
    #
    # Only the dirs for a hazard actually being processed THIS call are
    # included: skip_wind/skip_gust (or gust simply having no data this cycle)
    # mean the corresponding views are never in written_paths below, so
    # including their dirs here would make every one of that hazard's real,
    # still-current files look like a stale leftover and delete them.
    prefix = f"{country}_{storm}_{date}_"
    view_dirs_to_clean = []
    if not skip_wind:
        view_dirs_to_clean += ['school_views', 'hc_views', 'shelter_views', 'wash_views',
                               'mercator_views', 'admin_views', 'track_views', 'track_tile_bitmask_views']
    if process_gust:
        view_dirs_to_clean += ['school_views_gust', 'hc_views_gust', 'shelter_views_gust', 'wash_views_gust',
                               'mercator_views_gust', 'admin_views_gust', 'track_views_gust', 'track_tile_bitmask_views_gust']
    existing_before = {}
    for view_dir in view_dirs_to_clean:
        dir_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, view_dir)
        try:
            existing_before[view_dir] = [f for f in data_store.list_files(dir_path)
                                         if os.path.basename(f).startswith(prefix)]
        except Exception as e:
            logger.warning(f"Could not list {view_dir} for {country}/{storm}/{date}: {e}")
            existing_before[view_dir] = []

    # Populated at every save_*() call site below with the real path each one
    # wrote, so the end-of-function cleanup step can tell a genuinely stale
    # leftover (in existing_before, never added here) from a file this call
    # rewrote (in existing_before AND here, left alone).
    written_paths = set()

    logger.info(f"  Processing {country}...")

    # Schools
    logger.info(f"    Processing schools...")
    gdf_schools = fetch_schools(country, rewrite=0, fast_fail=True)
    # Buffered ONCE here and reused for every one of this call's own
    # school-facing calls below (wind view, wind tracks, and, when gust data
    # is present, gust view and gust tracks): see _precompute_facility_
    # buffer()'s own docstring for why a single buffer is correct to reuse
    # across all of them, instead of each independently re-buffering the
    # exact same gdf_schools.
    gdf_schools_buff = _precompute_facility_buffer(gdf_schools, 'school_id_giga', 'school')

    if not skip_wind:
        wind_school_views = create_school_view_from_envelopes(gdf_schools, gdf_envelopes, precomputed_buff=gdf_schools_buff)
        for wind_th in wind_school_views:
            written_paths.add(save_school_view(_facility_view_for_write(wind_school_views[wind_th], 'wind', f'school {country} {wind_th}kt'), country, storm, date, wind_th))
        files_written += len(wind_school_views)
        logger.info(f"    Created {len(wind_school_views)} school views")
        heartbeat()

    # Health centers
    logger.info(f"    Processing health centers...")
    gdf_hcs = fetch_health_centers(country, rewrite=0, fast_fail=True)
    # Same single-buffer reuse as gdf_schools_buff above, filtered to
    # HC_FACILITY_TYPES first (_filter_hcs_to_relevant_types) so the reused
    # buffer matches what every HC-facing call below would have filtered to
    # on its own.
    gdf_hcs_buff = _precompute_facility_buffer(gdf_hcs, 'osm_id', 'health center', filter_fn=_filter_hcs_to_relevant_types)
    if not skip_wind:
        wind_hc_views = create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes, precomputed_buff=gdf_hcs_buff)
        for wind_th in wind_hc_views:
            written_paths.add(save_hc_view(_facility_view_for_write(wind_hc_views[wind_th], 'wind', f'hc {country} {wind_th}kt'), country, storm, date, wind_th))
        files_written += len(wind_hc_views)
        logger.info(f"    Created {len(wind_hc_views)} health center views")
        heartbeat()

    # Shelters
    logger.info(f"    Processing shelters...")
    gdf_shelters = fetch_shelters(country, rewrite=0, fast_fail=True)
    # Same single-buffer reuse as gdf_schools_buff above.
    gdf_shelters_buff = _precompute_facility_buffer(gdf_shelters, 'osm_id', 'shelter')
    if not skip_wind:
        wind_shelter_views = create_shelter_view_from_envelopes(gdf_shelters, gdf_envelopes, precomputed_buff=gdf_shelters_buff)
        for wind_th in wind_shelter_views:
            written_paths.add(save_shelter_view(_facility_view_for_write(wind_shelter_views[wind_th], 'wind', f'shelter {country} {wind_th}kt'), country, storm, date, wind_th))
        files_written += len(wind_shelter_views)
        logger.info(f"    Created {len(wind_shelter_views)} shelter views")
        heartbeat()

    # WASH
    logger.info(f"    Processing WASH...")
    gdf_wash = fetch_wash(country, rewrite=0, fast_fail=True)
    # Same single-buffer reuse as gdf_schools_buff above.
    gdf_wash_buff = _precompute_facility_buffer(gdf_wash, 'osm_id', 'WASH facility')
    if not skip_wind:
        wind_wash_views = create_wash_view_from_envelopes(gdf_wash, gdf_envelopes, precomputed_buff=gdf_wash_buff)
        for wind_th in wind_wash_views:
            written_paths.add(save_wash_view(_facility_view_for_write(wind_wash_views[wind_th], 'wind', f'wash {country} {wind_th}kt'), country, storm, date, wind_th))
        files_written += len(wind_wash_views)
        logger.info(f"    Created {len(wind_wash_views)} WASH views")
        heartbeat()

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
        view = create_mercator_country_layer(country, zoom, rewrite=0, fast_fail=True)
        gdf_tiles, _ = add_admin_ids(view, country)
        save_mercator_view(gdf_tiles, country, zoom)
        wrote_base_parquet = True
        logger.info(f"    Created and saved base mercator tiles: {len(gdf_tiles)} tiles")

    # Population columns are a hard init-time requirement (same check
    # create_mercator_view_from_envelopes()/create_admin_view_from_envelopes_new()/
    # create_tracks_view_from_envelopes() each also run internally as a
    # defense-in-depth default for other callers). Checked HERE too, before
    # the bitmask precompute below, so a country with incomplete base data
    # fails immediately rather than paying for a full rasterize pass first.
    # gdf_tiles is not reassigned below this point (wind or gust), so one
    # check here covers both hazards.
    _missing_pop_cols = [c for c in POPULATION_COLS if c not in gdf_tiles.columns]
    if _missing_pop_cols:
        raise ValueError(
            f"gdf_tiles is missing {_missing_pop_cols}, run "
            f"'--type patch --columns {' '.join(_missing_pop_cols)}' for this country "
            f"before mercator tile impact views can be computed."
        )

    # Per-tile, per-member coverage bitmask: computed ONCE per hazard here and
    # reused by every view below (mercator tiles, every admin level, tracks,
    # and this function's own bitmask MAT save) instead of each independently
    # recomputing this expensive per-member reprojection+rasterize pass.
    # Repeating it 4x per hazard (8 total) with no memory released between is
    # an avoidable OOM risk at large-country scale. See
    # calculate_tile_member_bitmask's own docstring for the computation itself.
    if not skip_wind:
        wind_bitmask_views = calculate_tile_member_bitmask(gdf_envelopes, gdf_tiles,
                                                           threshold_column='wind_threshold',
                                                           label='wind tile')

        wind_tiles_views = create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes, bitmask_views=wind_bitmask_views)
        for wind_th in wind_tiles_views:
            written_paths.add(save_tiles_view(wind_tiles_views[wind_th], country, storm, date, wind_th, zoom))
        files_written += len(wind_tiles_views)
        logger.info(f"    Created {len(wind_tiles_views)} tile views")
        heartbeat()

        # CCI for tiles
        cci_tiles_view = calculate_ccis(wind_tiles_views, gdf_tiles)
        written_paths.add(save_cci_tiles(cci_tiles_view, country, storm, date, zoom))
        files_written += 1

        # Vulnerability (people/children in need): wind-dependent poverty weighting
        vuln_tiles_view = calculate_vulnerability_view(wind_tiles_views, gdf_tiles)
        written_paths.add(save_vulnerability_tiles(vuln_tiles_view, country, storm, date, zoom))
        files_written += 1

        # Per-member vulnerability: analogous to per-threshold track parquets
        vuln_tracks_view = calculate_vulnerability_tracks(gdf_envelopes, gdf_tiles)
        written_paths.add(save_vulnerability_tracks(vuln_tracks_view, country, storm, date, zoom))
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
            gdf_admin = create_admin_country_layer(country, rewrite=0, admin_level=admin_level, fast_fail=True)
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

        if not skip_wind:
            # precomputed_tile_views reuses wind_tiles_views (this country's
            # already-computed mercator tile view) for admin level 1 only:
            # `gdf_tiles_for_admin is gdf_tiles` is the same identity check its
            # construction above already keys off (admin_level == 1 sets
            # gdf_tiles_for_admin = gdf_tiles directly), so this stays correct
            # even if admin_levels is reordered. See
            # create_admin_view_from_envelopes_new()'s docstring for why this
            # reuse is safe (byte-identical per-tile inputs).
            wind_admin_views = create_admin_view_from_envelopes_new(
                gdf_admin, gdf_tiles_for_admin, gdf_envelopes,
                bitmask_views=wind_bitmask_views,
                precomputed_tile_views=(wind_tiles_views if gdf_tiles_for_admin is gdf_tiles else None))
            for wind_th in wind_admin_views:
                written_paths.add(save_admin_tiles_view(wind_admin_views[wind_th], country, storm, date, wind_th,
                                                         admin_level=admin_level))
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
            written_paths.add(save_cci_admin(cci_admin_view, country, storm, date, admin_level=admin_level))
            files_written += 1

            # Vulnerability for this admin level
            vagg_dict = {col: lambda x: x.sum(min_count=1) for col in sum_cols_vulnerability}
            vagg = vuln_tiles_view.copy()
            if admin_level != 1:
                id_map = gdf_tiles_for_admin.set_index('tile_id')['id'].to_dict()
                vagg['id'] = vagg['zone_id'].map(id_map)
            vagg = vagg.groupby("id").agg(vagg_dict).reset_index()
            vuln_admin_view = vagg.rename(columns={'id': 'tile_id'})
            written_paths.add(save_vulnerability_admin(vuln_admin_view, country, storm, date, admin_level=admin_level))
            files_written += 1
            heartbeat()

    if not skip_wind:
        # Tracks
        logger.info(f"    Processing tracks...")
        wind_tracks_views = create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member', gdf_shelters=gdf_shelters, gdf_wash=gdf_wash,
                                                               bitmask_views=wind_bitmask_views,
                                                               schools_buff=gdf_schools_buff, hcs_buff=gdf_hcs_buff,
                                                               shelters_buff=gdf_shelters_buff, wash_buff=gdf_wash_buff)
        for wind_th in wind_tracks_views:
            written_paths.add(save_tracks_view(wind_tracks_views[wind_th], country, storm, date, wind_th))
        files_written += len(wind_tracks_views)
        logger.info(f"    Created {len(wind_tracks_views)} track views")
        heartbeat()

        # Per-tile, per-member coverage bitmask: already computed once, above,
        # and reused here rather than recomputed (see the comment at its first
        # computation for the full "why").
        # A None entry means that threshold's sjoin genuinely FAILED (see
        # calculate_tile_member_bitmask's own Returns section), skip it rather
        # than persisting a zero-row parquet that downstream cannot tell apart
        # from a real 'no member reaches this threshold'.
        wind_bitmask_written = 0
        for wind_th, bm in wind_bitmask_views.items():
            if bm is None:
                continue
            written_paths.add(save_tracks_tile_bitmask_view(bm, country, storm, date, wind_th))
            wind_bitmask_written += 1
        files_written += wind_bitmask_written
        logger.info(f"    Created {wind_bitmask_written} tile-member bitmask views")
        heartbeat()

        # Wind is fully done and saved at this exact point (nothing below this line is
        # wind-related). Report it here, before gust starts, so a caller-side subprocess
        # supervisor has wind's real result durably recorded even if gust later hangs and
        # this whole OS process gets killed from outside. See on_wind_complete's own
        # docstring for the full "why".
        on_wind_complete(wrote_base_parquet, files_written)

    # --- Gust envelopes (optional, core exposure views only) ---
    # No CCI, vulnerability, or JSON report for gust
    # Wrapped in its own try/except: wind processing above
    # has already fully succeeded and saved, an uncaught exception here must not
    # propagate to the per-country try/except in run_complete_impact_analysis(),
    # which would otherwise discard the wind results from this same call.
    gust_error = None
    if process_gust:
        try:
            logger.info(f"    Processing gust envelopes ({len(gdf_envelopes_gust)} records)...")

            # Same precomputed buffers as wind's own view calls above are reused
            # here too: gdf_schools/gdf_hcs/gdf_shelters/gdf_wash are unchanged
            # between wind and gust processing (only the envelopes differ), so
            # the buffered geometry computed once at the top of this function
            # remains correct for gust's own facility calls as well.
            gust_school_views = create_school_view_from_envelopes(gdf_schools, gdf_envelopes_gust, threshold_column='gust_threshold', precomputed_buff=gdf_schools_buff)
            for gth in gust_school_views:
                written_paths.add(save_school_view(_facility_view_for_write(gust_school_views[gth], 'gust', f'school {country} g{gth}'), country, storm, date, gth, dataset='gust'))
            files_written += len(gust_school_views)
            heartbeat()

            gust_hc_views = create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes_gust, threshold_column='gust_threshold', precomputed_buff=gdf_hcs_buff)
            for gth in gust_hc_views:
                written_paths.add(save_hc_view(_facility_view_for_write(gust_hc_views[gth], 'gust', f'hc {country} g{gth}'), country, storm, date, gth, dataset='gust'))
            files_written += len(gust_hc_views)
            heartbeat()

            gust_shelter_views = create_shelter_view_from_envelopes(gdf_shelters, gdf_envelopes_gust, threshold_column='gust_threshold', precomputed_buff=gdf_shelters_buff)
            for gth in gust_shelter_views:
                written_paths.add(save_shelter_view(_facility_view_for_write(gust_shelter_views[gth], 'gust', f'shelter {country} g{gth}'), country, storm, date, gth, dataset='gust'))
            files_written += len(gust_shelter_views)
            heartbeat()

            gust_wash_views = create_wash_view_from_envelopes(gdf_wash, gdf_envelopes_gust, threshold_column='gust_threshold', precomputed_buff=gdf_wash_buff)
            for gth in gust_wash_views:
                written_paths.add(save_wash_view(_facility_view_for_write(gust_wash_views[gth], 'gust', f'wash {country} g{gth}'), country, storm, date, gth, dataset='gust'))
            files_written += len(gust_wash_views)
            heartbeat()

            # Per-tile, per-member coverage bitmask: computed ONCE here, same
            # hoisting rationale as wind's own bitmask above (see that
            # comment for the full "why").
            gust_bitmask_views = calculate_tile_member_bitmask(gdf_envelopes_gust, gdf_tiles,
                                                               threshold_column='gust_threshold',
                                                               label='gust tile')

            # The tile and admin frames are built directly in their thin form
            # from the bitmasks; any failure falls back to the dense builders below. Wind keeps the
            # dense frames: CCI and vulnerability rank over every tile.
            gust_sparse = None
            gust_tiles_views = None
            try:
                gust_sparse = EnvelopeSparse(gdf_tiles, gdf_tiles_for_admin_by_level)
                gust_tiles_views = gust_sparse.tile_frames(
                    list(gdf_envelopes_gust['gust_threshold'].unique()), gust_bitmask_views)
            except Exception as e:
                logger.warning(f"    Sparse gust tile frames not applied for {country}, using the dense path: {e}")
                gust_sparse = None
                gust_tiles_views = None
            if gust_tiles_views is None:
                gust_tiles_views = create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes_gust, threshold_column='gust_threshold',
                                                                        bitmask_views=gust_bitmask_views)
            for gth in gust_tiles_views:
                written_paths.add(save_tiles_view(gust_tiles_views[gth], country, storm, date, gth, zoom, dataset='gust'))
            files_written += len(gust_tiles_views)
            heartbeat()

            for admin_level in admin_levels:
                # Same admin-level-1 reuse as wind's own call above, this
                # time against gust_tiles_views: see wind's own call site
                # comment and create_admin_view_from_envelopes_new()'s
                # docstring for the full "why".
                gust_admin_views = None
                if gust_sparse is not None:
                    try:
                        gust_admin_views = gust_sparse.admin_views(
                            admin_level, gdf_admin_by_level[admin_level],
                            list(gdf_envelopes_gust['gust_threshold'].unique()), gust_bitmask_views)
                    except Exception as e:
                        logger.warning(f"    Sparse gust admin{admin_level} views not applied for {country}, using the dense path: {e}")
                if gust_admin_views is None:
                    gust_admin_views = create_admin_view_from_envelopes_new(
                        gdf_admin_by_level[admin_level], gdf_tiles_for_admin_by_level[admin_level],
                        gdf_envelopes_gust, threshold_column='gust_threshold', bitmask_views=gust_bitmask_views,
                        precomputed_tile_views=(gust_tiles_views if (gdf_tiles_for_admin_by_level[admin_level] is gdf_tiles and gust_sparse is None) else None))
                for gth in gust_admin_views:
                    written_paths.add(save_admin_tiles_view(gust_admin_views[gth], country, storm, date, gth,
                                                             admin_level=admin_level, dataset='gust'))
                files_written += len(gust_admin_views)
                heartbeat()

            gust_tracks_views = create_tracks_view_from_envelopes(
                gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes_gust, index_column='ensemble_member',
                gdf_shelters=gdf_shelters, gdf_wash=gdf_wash, threshold_column='gust_threshold',
                bitmask_views=gust_bitmask_views,
                schools_buff=gdf_schools_buff, hcs_buff=gdf_hcs_buff,
                shelters_buff=gdf_shelters_buff, wash_buff=gdf_wash_buff)
            for gth in gust_tracks_views:
                written_paths.add(save_tracks_view(gust_tracks_views[gth], country, storm, date, gth, dataset='gust'))
            files_written += len(gust_tracks_views)
            heartbeat()

            # Same skip-on-None contract as the wind call site above.
            for gth, bm in gust_bitmask_views.items():
                if bm is None:
                    continue
                written_paths.add(save_tracks_tile_bitmask_view(bm, country, storm, date, gth, dataset='gust'))
                files_written += 1
            heartbeat()

            logger.info(f"    Created gust views ({len(gust_school_views)} thresholds)")
        except Exception as e:
            gust_error = str(e)
            logger.warning(
                f"    Gust envelope processing failed for {country}/{storm}/{date}, "
                f"gust views skipped this run, wind views unaffected: {e}",
                exc_info=True,
            )
    else:
        logger.info(f"    No gust envelope data for {country}/{storm}/{date}, skipping gust views")

    # Stale-threshold cleanup, deferred from the top of this function (see the
    # comment there): delete only pre-existing files this call never rewrote.
    # Matched by basename, not exact path string, since data_store.list_files()'s
    # returned path format isn't guaranteed identical to what write_dataset() was
    # given; the removal itself still uses list_files()'s own returned string,
    # the same pattern the upfront-delete loop relies on.
    written_basenames = {os.path.basename(p) for p in written_paths if p}
    for view_dir, existing in existing_before.items():
        for f in existing:
            if os.path.basename(f) not in written_basenames:
                try:
                    data_store.remove(f)
                    logger.debug(f"Removed stale file: {f}")
                except Exception as e:
                    logger.warning(f"Could not remove stale file {f} in {view_dir}: {e}")

    return wrote_base_parquet, files_written, gust_error


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
        logger.warning(f"Error loading gust envelopes: {str(e)}", exc_info=True)
        return pd.DataFrame()
