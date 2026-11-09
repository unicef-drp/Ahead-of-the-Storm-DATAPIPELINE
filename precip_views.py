#!/usr/bin/env python3
"""
Precipitation/Runoff Views Module

All precip-specific (tp exceedance-probability and ro/tp ratio exceedance-
probability) view creation and persistence: tile/admin/facility impact views,
per-tile-per-member coverage bitmasks, and every save_precip_*/
save_precip_ratio_* function.

Storm-independent: called from run_precip_analysis() once per --type update
run, not once per storm. See this module's own "PRECIPITATION/RUNOFF VIEWS"
section below for why precip uses centroid-based grid sampling rather than
the polygon sjoin wind/gust use or map_rasters()'s polygon-based zonal stats.

Module structure:
    _precip_grid_row_col
    create_precip_tile_view, calculate_precip_tile_member_bitmask,
        create_precip_admin_view
    create_precip_facility_view
    save_precip_tile_view, save_precip_tile_bitmask_view, save_precip_admin_view,
        save_precip_ratio_view, save_precip_ratio_admin_view
    PRECIP FACILITY-LEVEL VIEWS: save_precip_*/save_precip_ratio_* for
        schools/health centers/shelters/WASH
"""

import logging
import os
import warnings
import numpy as np
import pandas as pd

from gigaspatial.generators import GeometryBasedZonalViewGenerator
from gigaspatial.core.io.writers import write_dataset

# Shared state/helpers with no dependency of their own on this module or its
# siblings - see pipeline_shared.py's own docstring for why this exists.
from pipeline_shared import (
    data_store,
    ROOT_DATA_DIR,
    VIEWS_DIR,
    data_cols,
    sum_cols,
    avg_cols,
    _OPTIONAL_SUM_COLS,
    _optional_sum,
)
from sparse_output import (
    describe_reduction,
    sparsify_facility_frame,
    sparsify_tile_frame,
)

logger = logging.getLogger(__name__)


def _tile_view_for_write(df, family, file_name):
    """
    Frame handed to a tile view writer: a thin copy (nonzero rows plus one
    marker row). Any failure falls back to the dense frame so the file is
    still written.
    """
    try:
        # Contiguous index, so the CSV index column matches the thin frame.
        frame = sparsify_tile_frame(df).reset_index(drop=True)
    except Exception as e:
        logger.warning(f"    Sparse {family} tile view {file_name} not applied, writing the dense file: {e}")
        return df
    logger.info(f"    Sparse {family} tile view {file_name}: {describe_reduction(df, frame)}")
    return frame


def _facility_view_for_write(gdf, family, file_name):
    """
    Frame handed to a facility view writer: zero-probability rows are dropped
    from a copy. The result may be empty; the file is still written so
    existence checks see it.
    """
    try:
        # Contiguous index, so the parquet file carries no extra index column.
        frame = sparsify_facility_frame(gdf).reset_index(drop=True)
    except Exception as e:
        logger.warning(f"    Sparse {family} facility view {file_name} not applied, writing the dense file: {e}")
        return gdf
    logger.debug(f"    Sparse {family} facility view {file_name}: {describe_reduction(gdf, frame)}")
    return frame


# =============================================================================
# PRECIPITATION/RUNOFF VIEWS
# Storm-independent: tp exceedance-probability tiers and the ro/tp ratio's own
# exceedance-probability tiers, both grid-sampled per tile via centroid-
# based point sampling rather than the polygon sjoin wind/gust use or
# map_rasters()'s polygon-based zonal stats
# Called from run_precip_analysis() once per --type update run, not once per storm.
# =============================================================================

def _precip_grid_row_col(cx, cy, lat_min, lat_max, lon_min, lon_max, n_lat, n_lon):
    """
    Vectorized centroid -> native precip-grid row/col lookup. The single
    shared implementation of this math: both create_precip_tile_view() and
    calculate_precip_tile_member_bitmask() call this, rather than each
    deriving their own formula, so both stay consistent
    with dividing by n-1 (not n) and the half-cell expansion described below.

    lat_min/lat_max/lon_min/lon_max are the grid's OUTERMOST GRID POINT
    (pixel-center) coordinates, NOT already the grid's own edge bounds. The
    cell size divides by (n-1) intervals between points (not n), and the
    sampled bounds expand by half a cell on every side, so the grid's actual
    pixel edges are half a cell wider than the raw min/max on each side.
    Row 0 = lat_max (north-at-row-0, matching read_precip_windows()'s
    convention).

    Args:
        cx, cy: 1D numpy arrays of centroid longitude/latitude, same length.
        lat_min, lat_max, lon_min, lon_max: grid's own outermost grid-point
            bounds (the same 4 values read_precip_windows() returns
            alongside the raw grid).
        n_lat, n_lon: grid shape (rows, cols).

    Returns:
        (rows, cols): int64 numpy arrays, same length as cx/cy, each clamped
        to the valid [0, n-1] pixel range (a centroid sitting exactly on, or
        within floating-point epsilon of, the grid's outer edge would
        otherwise produce an out-of-range index).
    """
    dlat = (lat_max - lat_min) / (n_lat - 1) if n_lat > 1 else 0.0
    dlon = (lon_max - lon_min) / (n_lon - 1) if n_lon > 1 else 0.0
    top = lat_max + dlat / 2
    left = lon_min - dlon / 2
    # dlat/dlon == 0 only in the degenerate n_lat==1/n_lon==1 case; every
    # centroid maps to the single row/col in that case.
    rows = (np.clip(((top - cy) / dlat).astype(np.int64), 0, n_lat - 1)
            if dlat else np.zeros(len(cy), dtype=np.int64))
    cols = (np.clip(((cx - left) / dlon).astype(np.int64), 0, n_lon - 1)
            if dlon else np.zeros(len(cx), dtype=np.int64))
    return rows, cols


def create_precip_tile_view(gdf_tiles, grid_2d, bounds, tiles_viewer=None):
    """
    Sample a precip/ratio probability raster onto mercator tiles.

    Uses each tile's centroid, not map_rasters()'s polygon-based zonal mean:
    the precip grid's own cells (~0.25 degrees) are far coarser than a
    zoom-14 mercator tile, so a tile's tiny polygon usually doesn't contain
    the enclosing raster cell's pixel-center point, which is what
    TifProcessor.sample_by_polygons() (rasterio.mask.mask with the default
    all_touched=False, and no all_touched override exposed) requires to
    count any pixel as "inside" the polygon.

    The raw (n_lat, n_lon) probability array plus its own real (lat_min,
    lat_max, lon_min, lon_max) bounds are sampled via the same vectorized
    centroid->native-cell math calculate_precip_tile_member_bitmask() uses
    (see _precip_grid_row_col()): a single vectorized fancy-index lookup for
    every tile at once, no per-point Python-level sampling loop.

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (from load_mercator_view()).
        grid_2d: (n_lat, n_lon) raw probability array.
        bounds: (lat_min, lat_max, lon_min, lon_max) tuple, grid_2d's own
            real geo bounds. A missing or invalid grid or bounds does not
            raise: probabilities default to 0 and a WARNING is logged.
        tiles_viewer: Optional, an already-constructed
            GeometryBasedZonalViewGenerator(zone_data=gdf_tiles,
            zone_id_column='tile_id') for this same gdf_tiles, reused
            (resetting `_view` to None first) instead of building a fresh
            one: the dense fallback of main_pipeline.py's per-country precip
            loop calls this function once per (window_h, threshold_mm) and
            once per (window_h, ratio_threshold) combination (up to ~20
            calls for a country with every window/threshold populated)
            against the exact same gdf_tiles every time, and GeometryBasedZonalViewGenerator.
            __init__ renames+copies every column of gdf_tiles and rebuilds
            its zone_id->geometry dict from scratch on each construction.
            When None (default, e.g. this function's own internal call from
            create_precip_admin_view() below for an admin_level > 1 whose
            own admin_tiles differs from the whole-country gdf_tiles), builds
            one fresh.

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
    if tiles_viewer is not None:
        tiles_viewer._view = None
    else:
        tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
    try:
        # Centroid-on-geographic-CRS distortion is negligible at zoom-14 tile
        # scale (well under the ~0.25 degree precip grid resolution), so the
        # geopandas warning here is expected and safe to suppress, not a sign
        # of an actual precision problem worth reprojecting for.
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message='Geometry is in a geographic CRS')
            centroids = gdf_tiles.geometry.centroid
        tile_ids = gdf_tiles['tile_id'].to_numpy()

        if bounds is None:
            raise ValueError("create_precip_tile_view: bounds is required")
        lat_min, lat_max, lon_min, lon_max = bounds
        n_lat, n_lon = grid_2d.shape
        cy, cx = centroids.y.to_numpy(), centroids.x.to_numpy()
        rows, cols = _precip_grid_row_col(cx, cy, lat_min, lat_max, lon_min, lon_max, n_lat, n_lon)
        # Cast to float32: probabilities are stored at float32 precision.
        values = grid_2d[rows, cols].astype(np.float32)
        probs = dict(zip(tile_ids, values))
        native_rows = dict(zip(tile_ids, rows))
        native_cols = dict(zip(tile_ids, cols))
    except Exception as e:
        logger.warning(f"precip tile view sampling failed, defaulting probabilities to 0: {e}")
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
    # pattern as wind's own create_mercator_view_from_envelopes() (impact_analysis.py),
    # so precip's E_* breakdown is complete and consistent with
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
    Per-z14-tile, per-ensemble-member 64-bit coverage bitmask for one
    (threshold_mm, window_h) combination of rainfall exceedance (bit `m-1`
    set <=> member `m`'s accumulated rainfall exceeds threshold_mm at that
    tile's own native precip-grid cell). Same shape/purpose as Wind's own
    calculate_tile_member_bitmask() (see its docstring), persisted as a real
    pipeline output.

    Unlike River (a genuine point-in-polygon sjoin against real flooded
    pixels) rain's own precip grid (~0.25 degrees) is far coarser than a
    z14 tile, so this reuses the exact same centroid-to-native-cell mapping
    create_precip_tile_view() already computes for its own probability
    sampling (bounds/dlon/dlat/native_row/native_col, see that function's
    own comment for the "why" of centroid sampling over polygon zonal
    stats), NOT a new sjoin. Every tile whose centroid falls in the same
    native cell shares the exact same bitmask value, by construction, same
    as they already share the same probability value.

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (from load_mercator_view()).
        bitmask_grid: (n_lat, n_lon) uint64 array from precip_utils.
            exceedance_bitmask(), same shape/orientation as the raw
            period_grid it was derived from (row 0 = lat_max, matching
            read_precip_windows()'s own north-at-row-0 convention).
        lat_min, lat_max, lon_min, lon_max: bitmask_grid's own real geo
            bounds, the same 4 values read_precip_windows() returns
            alongside period_grid itself.

    Returns:
        DataFrame with columns ['tile_id' (string), 'bits' (uint64)], sparse
        (only tiles whose native cell has >=1 member exceeding threshold_mm
        get a row), the same convention Wind/River's own bitmask functions
        use, and the same "real, common outcome" contract for an
        all-zero/empty result (no flooding-equivalent case here, this
        mirrors an all-below-threshold cycle, not an error).
    """
    n_lat, n_lon = bitmask_grid.shape
    # lat_min/lat_max/lon_min/lon_max are OUTERMOST GRID POINT (pixel-center)
    # coordinates, not the grid's edge bounds. _precip_grid_row_col() is the
    # single shared implementation of the centroid->cell math (also used by
    # create_precip_tile_view()).
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', message='Geometry is in a geographic CRS')
        centroids = gdf_tiles.geometry.centroid
    cy, cx = centroids.y.to_numpy(), centroids.x.to_numpy()
    rows, cols = _precip_grid_row_col(cx, cy, lat_min, lat_max, lon_min, lon_max, n_lat, n_lon)
    tile_bits = bitmask_grid[rows, cols]
    tile_ids = gdf_tiles['tile_id'].to_numpy()

    nonzero = tile_bits != 0
    if not nonzero.any():
        return pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})
    return pd.DataFrame({
        'tile_id': pd.array(tile_ids[nonzero].astype(str), dtype='string'),
        'bits': tile_bits[nonzero],
    })


def create_precip_admin_view(gdf_admin, gdf_tiles, precomputed_tile_view=None,
                              grid_2d=None, bounds=None, tiles_viewer=None):
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
            Ignored (beyond the 'id' mapping below) when precomputed_tile_view is given.
        precomputed_tile_view: Optional tile-level view (create_precip_tile_view()'s own
            return shape) to reuse instead of resampling the raster again. Admin level 1's
            own gdf_tiles is always the exact same object as the caller's whole-country
            tile GeoDataFrame (see main_pipeline.py's own gdf_tiles_for_admin_by_level
            construction: `gdf_tiles_for_admin = gdf_tiles` for admin_level == 1), so this
            lets that level reuse a result already computed moments earlier by the caller
            instead of resampling the raster again. A copy is taken, never the caller's
            own object, since this function adds an 'id' column below and callers (e.g.
            facility views) still read the original afterward. Takes priority over
            grid_2d when given.
        grid_2d: Optional (n_lat, n_lon) raw probability array, forwarded to
            create_precip_tile_view() when precomputed_tile_view
            is not given (e.g. an admin_level > 1 call, whose own admin_tiles differs
            from the whole-country gdf_tiles, so a fresh raster sample is required).
        bounds: (lat_min, lat_max, lon_min, lon_max) tuple, grid_2d's own real geo
            bounds. Used with grid_2d; a missing grid or bounds defaults
            probabilities to 0 with a WARNING (see create_precip_tile_view()).
        tiles_viewer: Optional, forwarded as-is to create_precip_tile_view()'s own
            `tiles_viewer` param when precomputed_tile_view is not given: must be a
            GeometryBasedZonalViewGenerator already built for THIS gdf_tiles (i.e. this
            admin level's own admin_tiles), not the whole-country one. Ignored entirely
            when precomputed_tile_view is given.

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

    df_view = (
        precomputed_tile_view.copy() if precomputed_tile_view is not None
        else create_precip_tile_view(gdf_tiles, grid_2d=grid_2d, bounds=bounds,
                                      tiles_viewer=tiles_viewer)
    )

    if 'id' not in gdf_tiles.columns:
        raise ValueError(
            "Mercator view missing admin IDs. Admin IDs are added during "
            "initialization. Re-initialize the country or check the mercator view file."
        )
    id_mapping = gdf_tiles.set_index('tile_id')['id'].to_dict()
    df_view['id'] = df_view['zone_id'].map(lambda x: id_mapping.get(x, x))
    df_view = df_view.drop(columns=['zone_id'], errors='ignore')

    # E_population is treated as NaN-preserving here (via _optional_sum, even
    # though it's not in the global _OPTIONAL_SUM_COLS wind's admin view uses):
    # unlike wind, which hard-requires population at generation time
    # (POPULATION_COLS), precip's create_precip_tile_view() can genuinely
    # produce a NaN E_population, so an admin region rolling up all-NaN tiles
    # must stay NaN, not become a false "confirmed zero".
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
    tiles: real wind facility files have no tile_id
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
        keep zone_id+geometry) plus 'probability' (from the containing tile) and the
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
    write_dataset(_tile_view_for_write(df, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views_precip', file_name))


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
    write_dataset(_tile_view_for_write(df, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views_precipratio', file_name))


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
    write_dataset(_facility_view_for_write(gdf, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views_precip', file_name))


def save_precip_ratio_school_view(gdf, country, forecast_time, ratio_threshold, window_h):
    """school_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.parquet"""
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h.parquet"
    write_dataset(_facility_view_for_write(gdf, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views_precipratio', file_name))


def save_precip_hc_view(gdf, country, forecast_time, threshold_mm, window_h):
    """hc_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"
    write_dataset(_facility_view_for_write(gdf, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views_precip', file_name))


def save_precip_ratio_hc_view(gdf, country, forecast_time, ratio_threshold, window_h):
    """hc_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.parquet"""
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h.parquet"
    write_dataset(_facility_view_for_write(gdf, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views_precipratio', file_name))


def save_precip_shelter_view(gdf, country, forecast_time, threshold_mm, window_h):
    """shelter_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"
    write_dataset(_facility_view_for_write(gdf, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views_precip', file_name))


def save_precip_ratio_shelter_view(gdf, country, forecast_time, ratio_threshold, window_h):
    """shelter_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.parquet"""
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h.parquet"
    write_dataset(_facility_view_for_write(gdf, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views_precipratio', file_name))


def save_precip_wash_view(gdf, country, forecast_time, threshold_mm, window_h):
    """wash_views_precip/{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"""
    file_name = f"{country}_{forecast_time}_p{threshold_mm}_{window_h}h.parquet"
    write_dataset(_facility_view_for_write(gdf, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views_precip', file_name))


def save_precip_ratio_wash_view(gdf, country, forecast_time, ratio_threshold, window_h):
    """wash_views_precipratio/{country}_{forecast_time}_g{ratio*100}_{window_h}h.parquet"""
    g_token = int(round(ratio_threshold * 100))
    file_name = f"{country}_{forecast_time}_g{g_token}_{window_h}h.parquet"
    write_dataset(_facility_view_for_write(gdf, 'precip', file_name), data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views_precipratio', file_name))
