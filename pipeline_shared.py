"""
Shared state and generic helpers used by impact_analysis.py, facility_io.py,
country_boundaries.py, precip_views.py, and river_views.py, with no dependency
of its own on any of them - this is what actually breaks the circular import
those five files would otherwise have between each other (each needs the
other's functions; before this module existed, that meant importing one of
them at the very bottom of the other, after all its own definitions, just so
Python's partially-initialized module object had what the other side needed
by the time it was imported). Every name here is either pure configuration
(paths, column lists), a real singleton that must be the exact same object
everywhere (`data_store`, so a Snowflake connection reset before a fork
affects the one object every module actually uses), or a fully self-contained
generic helper with no dependency on anything wind/gust/precip/river-specific.
"""

from __future__ import annotations

import io
import logging
import os

import geopandas as gpd
import pandas as pd

from config import config
from data_store_utils import get_data_store

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION / PATHS
# =============================================================================
ROOT_DATA_DIR = config.ROOT_DATA_DIR
VIEWS_DIR = config.VIEWS_DIR
PIPELINE_COUNTRIES_CACHE_PATH = os.path.join(ROOT_DATA_DIR, 'pipeline_countries_boundaries.parquet')
CUSTOM_DATA_DIR = os.path.join(ROOT_DATA_DIR, 'custom')

# =============================================================================
# DATA STORE
# =============================================================================
# Defaults to LOCAL if DATA_PIPELINE_DB is not set or is LOCAL. In production
# (SPCS), DATA_PIPELINE_DB is SNOWFLAKE and credentials are available. One
# singleton, imported by name everywhere it's used (never re-constructed per
# module) so a connection reset before a subprocess fork
# (main_pipeline.py's own _reset_snowflake_connection_before_fork()) affects
# the actual object every module's read/write calls go through.
data_store = get_data_store()

# =============================================================================
# AGGREGATION COLUMNS
# Counts and populations are summed; continuous indices (RWI, SMOD) are averaged
# to preserve their meaning. Shared by wind/gust, precip, and river view creation.
# =============================================================================
data_cols = [
    'population',           # Total population (WorldPop GR2, year=2025, 1km)
    'school_age_population',# School-age population 5-14 years (WorldPop GR2/2025, 100m, age_structures)
    'infant_population',    # Infant population 0-4 years (WorldPop GR2/2025, 100m, age_structures)
    'adolescent_population', # Adolescent population 15-19 years (WorldPop GR2/2025, 100m, age_structures)
    'built_surface_m2',     # Built surface area in m2 (GHSL GHS-BUILT-S)
    'smod_class',           # Settlement model L2 class 10-30 (GHSL GHS-SMOD)
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

# Columns where all-NaN means "no data" and should remain NaN (not sum to 0).
_OPTIONAL_SUM_COLS = {
    'num_shelters', 'num_wash', 'E_num_shelters', 'E_num_wash',
    'num_schools',  'num_hcs',  'E_num_schools',  'E_num_hcs',
    'built_surface_m2', 'E_built_surface_m2',
}


def _optional_sum(s):
    """Sum that preserves NaN when all values are NaN (no-data semantics)."""
    return s.sum() if s.notna().any() else float('nan')


# Full ECMWF ensemble size: 50 perturbed members + 1 deterministic (member 51).
# Hard-coded so probability denominators stay correct even when individual members
# fail to produce wind polygons or are missing from GRIB files.
FULL_ENSEMBLE_SIZE = 51


# =============================================================================
# CUSTOM DATA OVERRIDES
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


# =============================================================================
# ADMIN/TILE SPATIAL ASSIGNMENT
# =============================================================================
def admins_overlay(gdf_admins1, gdf_mercator):
    """
    Assign admin boundary IDs to mercator tiles.

    Works for any admin level (1, 2, ...). Three-step assignment, applied in order:

    1. Centroid-within: each tile is assigned to the admin region that contains its
       centroid (projected to equal-area CRS for accuracy). Handles most tiles.

    2. Area-overlap fallback: for tiles whose centroid falls outside every admin
       boundary (straddles a border), assign to the admin with the largest
       intersection area (equal-area CRS).

    3. Nearest-neighbour fallback: for tiles still unassigned after steps 1-2
       (ocean/far-offshore tiles), assign to the nearest admin boundary by
       centroid distance. Ensures every tile gets an admin ID.

    Note: step 3 only applies to tiles that do not intersect any admin region at all.
    Tiles that straddle admin boundaries are handled by steps 1-2.

    Callers are responsible for normalising the admin ID column to 'id' before
    passing gdf_admins1 (see country_boundaries.add_admin_ids).

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
        # facility_io.assign_facilities_to_tiles() already applies for its own
        # nearest-tile fallback). Harmless to leave projected here since only
        # tile_id/id survive into `assigned` below, geometry never leaks out of this
        # local computation.
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
