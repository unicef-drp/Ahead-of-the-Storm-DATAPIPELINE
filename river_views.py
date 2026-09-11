#!/usr/bin/env python3
"""
River Flood Views Module

All river-flood-specific (GloFAS x JRC) view creation and persistence:
tile/admin/facility impact views, per-tile-per-member coverage bitmasks, the
per-(country, rp_tier) idempotency check, and every save_river_* function.

GloFAS/JRC pixels are ~150m resolution, comparable to a zoom-14 mercator tile
or a buffered facility footprint, not coarse the way precip's ~0.25 degree
grid is. So unlike precip_views.create_precip_tile_view() (which must
centroid-sample a continuous raster), river-flood views reuse the wind/gust
"count(members)/FULL_ENSEMBLE_SIZE" convention: a member "hits" a zone if at
least one flooded pixel falls within it, same idiom
impact_analysis.create_mercator_view_from_envelopes() uses (manual gpd.sjoin +
groupby), just points-in-polygon ('within') instead of polygon-in-polygon
('intersects'), and .nunique('member') instead of .size() since a single
member can contribute several flooded pixels to the same zone at this
resolution.

below_min_basin is a per-pixel QC flag (this cell's upstream drainage area is
below GloFAS's 500km^2 minimum-catchment cutoff for JRC-extent-map
generation), a matching-availability flag, not a confidence statement about
the discharge forecast; per the upstream TC-ECMWF-Forecast-Pipeline repo's
design principle ("gate on RP tier for relevance, attach channel scale as a
secondary attribute, not a pre-filter"), it is never used to exclude a
pixel/zone here, only surfaced as a per-zone .any() boolean.

Module structure:
    create_river_tile_view, calculate_river_tile_member_bitmask,
        create_river_admin_view
    create_river_facility_view
    save_river_tile_view, river_country_tier_already_processed,
        save_river_tile_bitmask_view, save_river_admin_view
    save_river_school_view, save_river_hc_view, save_river_shelter_view,
        save_river_wash_view
"""

import logging
import os
import numpy as np
import pandas as pd
import geopandas as gpd

from gigaspatial.generators import GeometryBasedZonalViewGenerator
from gigaspatial.core.io.writers import write_dataset

# Shared state/helpers with no dependency of their own on this module or its
# siblings - see pipeline_shared.py's own docstring for why this exists.
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
)

logger = logging.getLogger(__name__)

# =============================================================================
# RIVER FLOOD (GloFAS x JRC) VIEWS
# =============================================================================
# GloFAS/JRC pixels are ~150m resolution, comparable to a zoom-14 mercator tile
# or a buffered facility footprint, not coarse the way precip's ~0.25 degree
# grid is. So unlike create_precip_tile_view() (which must centroid-sample a
# continuous raster since a tile's tiny polygon usually misses the enclosing
# coarse cell's pixel-center), river-flood views reuse the wind/gust
# "count(members)/FULL_ENSEMBLE_SIZE" convention: a member "hits" a zone if at
# least one flooded pixel falls within it, same idiom
# create_mercator_view_from_envelopes() uses (manual gpd.sjoin + groupby), just
# points-in-polygon ('within') instead of polygon-in-polygon ('intersects'),
# and .nunique('member') instead of .size() since a single member can
# contribute several flooded pixels to the same zone at this resolution.
#
# below_min_basin is a per-pixel QC flag (this cell's upstream drainage area is
# below GloFAS's 500km^2 minimum-catchment cutoff for JRC-extent-map
# generation) - a matching-availability flag, not a confidence statement about
# the discharge forecast. Per the upstream TC-ECMWF-Forecast-Pipeline repo's
# design principle ("gate on RP tier for relevance, attach channel scale as a
# secondary attribute, not a pre-filter"), it is never used to exclude a
# pixel/zone here, only surfaced as a per-zone .any() boolean.
def create_river_tile_view(gdf_tiles, gdf_pixels_step, rp_tier, is_standin,
                            num_ensembles=FULL_ENSEMBLE_SIZE, tiles_viewer=None):
    """
    Per-tile flood-extent probability for one RP tier + lead-time WINDOW:
    fraction of ensemble members with >=1 flooded JRC pixel inside the tile
    at ANY real day within that window (real union across days, not a
    single-day snapshot: see RIVER_LEADTIME_STEPS_H's own "ACCUMULATION"
    comment in main_pipeline.py for the full rationale).

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (from load_mercator_view()).
        gdf_pixels_step: GeoDataFrame of this country's flooded pixels for
            every real day up through the caller's selected window already
            (caller filters by `step_h <= window` before calling; this
            function itself does no day-level filtering, it just counts
            distinct members across whatever rows it's given, which is what
            makes multi-day rows here a real union rather than requiring
            any extra union logic in this function), columns: member,
            below_min_basin, geometry (Point, EPSG:4326). May be empty (a
            normal, common outcome: no flooding within this window).
        rp_tier: label for logging only (e.g. 'rp10').
        is_standin: bool, from the real RIVER_FORECASTS.IS_STANDIN column --
            True for rp2/rp5 (JRC has no native map, they reuse rp10's own
            extent as a labelled upper-bound approximation), False for
            rp10/rp20/rp50/rp100 (native JRC tier match). Passed through
            unchanged as a per-tier constant column, never re-derived here.
        num_ensembles: denominator for probability = distinct flooded
            members / num_ensembles. Default FULL_ENSEMBLE_SIZE.
        tiles_viewer: Optional, an already-constructed
            GeometryBasedZonalViewGenerator(zone_data=gdf_tiles,
            zone_id_column='tile_id') for this same gdf_tiles, reused with
            `_view` reset to None first (only the expensive rename+copy+
            zone_dict setup is skipped, result unaffected) instead of
            building a fresh one: main_pipeline.py's own per-country river
            loop calls this function once per (rp_tier, step_h) combination
            (up to 6 tiers x 4 windows = 24 calls for a country) against the
            exact same gdf_tiles every time, the same redundancy
            create_precip_tile_view()'s own `tiles_viewer` param eliminates.
            When None (default), built fresh.

    Returns:
        DataFrame with one row per tile: 'zone_id' (tile_id), 'probability',
        the full E_* breakdown (E_population, E_school_age_population,
        E_infant_population, E_adolescent_population, E_built_surface_m2,
        E_smod_class, E_smod_class_l1, E_rwi, E_num_schools, E_num_hcs,
        E_num_shelters, E_num_wash), the same `data_cols` loop
        create_mercator_view_from_envelopes() (wind) and gust use, so this
        file's shape matches a wind/gust mercator_views CSV exactly except
        for the missing CCI/vulnerability columns (out of scope for river
        flood, same as gust/precip), plus 'below_min_basin' (any
        contributing pixel flagged) and 'is_standin'.

    Raises:
        ValueError: if gdf_tiles is missing any POPULATION_COLS column, same
            hard-fail guard create_mercator_view_from_envelopes() (wind) and
            create_admin_view_from_envelopes_new() use: these are a hard,
            always-required init-time dependency, not optional data. Without
            this guard, a country missing e.g. school_age_population would
            silently get NaN at tile level, and create_river_admin_view()'s
            plain-'sum' aggregation for these columns (deliberately excluded
            from _OPTIONAL_SUM_COLS, since they're never expected to be
            all-NaN) would then turn that into a fabricated 0.0 at admin
            level, indistinguishable from a genuine zero-exposure result.
    """
    _missing_pop_cols = [c for c in POPULATION_COLS if c not in gdf_tiles.columns]
    if _missing_pop_cols:
        raise ValueError(
            f"gdf_tiles is missing {_missing_pop_cols}, run "
            f"'--type patch --columns {' '.join(_missing_pop_cols)}' for this country "
            f"before river-flood tile impact views can be computed."
        )

    if tiles_viewer is not None:
        tiles_viewer._view = None
    else:
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
        # Vectorized reindex against the full tile grid, not a per-tile Python
        # dict comprehension: the real cost was never the sjoin/groupby above
        # (cheap, scales with pixel count), it was that step doing hundreds of
        # thousands of individual Series.get() calls regardless of how few
        # tiles matched (measured flat ~2.3-3.2s per call whether 9 or 1.77M
        # pixels fed it). add_variable_to_view() accepts a plain Series exactly
        # like a dict (wraps it in pd.Series(data_dict, name=...) either way).
        probs = (member_counts.reindex(all_tile_ids, fill_value=0).astype(float)
                 / float(num_ensembles))
        basin = basin_flags.reindex(all_tile_ids, fill_value=False).astype(bool)
    except ValueError:
        # Expected, common outcome: no flooded pixels this country/step.
        probs = pd.Series(0.0, index=all_tile_ids)
        basin = pd.Series(False, index=all_tile_ids)
    except Exception as e:
        # Anything else (e.g. a CRS mismatch, an upstream schema change
        # dropping/renaming 'below_min_basin') indicates a genuine problem,
        # not a normal "no flooding" outcome: must be visible at warning level, same as
        # create_mercator_view_from_envelopes()'s own catch-all does for the
        # structurally identical case, not silently absorbed at debug level.
        logger.warning(f"River-flood tile intersection failed for {rp_tier}, defaulting probabilities to 0: {e}")
        probs = pd.Series(0.0, index=all_tile_ids)
        basin = pd.Series(False, index=all_tile_ids)

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
    Per-z14-tile, per-ensemble-member 64-bit coverage bitmask for one
    (rp_tier, step_h window) of river flood extent (bit `m-1` set <=> member
    `m` has >=1 flooded JRC pixel inside that tile), same convention/shape
    as Wind/Gust's own calculate_tile_member_bitmask() (mirrors it directly,
    see that function's own docstring for the full rationale), applied to
    River instead, persisted as a real pipeline output (mirrors this repo's
    dashboard sibling's own live decode of the same raw per-member GloFAS
    parquet, `_RiverExtentCache` in services/tile_server.py, `bits =
    uint64(1) << (member - 1)`).

    Genuinely near-zero new I/O: reuses the same per-member pixel data
    create_river_tile_view() already reads (gdf_pixels_step, already
    country-scoped and step_h-filtered by the caller), running an
    independent gpd.sjoin() with the identical inputs/predicate/direction
    that function's own `joined` already uses (pixels left, tiles right,
    predicate='within', required by 'within' semantics, not a style
    choice, see create_river_tile_view's own comment). The sjoin itself is
    cheap; the cost of downloading/DuckDB-filtering the raw global
    parquet already happened upstream, once, before either function runs.

    Args:
        gdf_tiles: GeoDataFrame of mercator tiles (from load_mercator_view()),
            needs only 'tile_id' and 'geometry'.
        gdf_pixels_step: same shape create_river_tile_view() takes: this
            country's flooded pixels for every real day up through the
            caller's selected window already (member, below_min_basin,
            geometry columns), may be empty (a normal, common outcome).

    Returns:
        DataFrame with columns ['tile_id' (string), 'bits' (uint64)], one
        row per distinct tile with >=1 member's flooded pixel falling
        inside it, sparse, same convention Wind's own bitmask uses. An empty
        gdf_pixels_step, a zero-intersection result, or a sjoin failure
        (e.g. a CRS mismatch, logged as a warning) all return an empty
        DataFrame; there is only ever ONE call per (rp_tier, step_h) here,
        so the caller's own `files_written` bookkeeping simply skips writing
        a file when this returns empty, no separate None sentinel needed.
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


def create_river_admin_view(gdf_admin, gdf_tiles, gdf_pixels_step, rp_tier, is_standin, tile_view=None):
    """
    Admin-level rollup of create_river_tile_view(), same sum_cols/avg_cols
    groupby-aggregation pattern create_admin_view_from_envelopes_new() (wind)
    and gust use, not precip's E_population-only shape. below_min_basin
    OR'd (.any(): a QC flag rolls up as "does ANY contributing tile carry
    it", never averaged/summed), is_standin passed through as the same
    per-tier constant.

    tile_view: optional, an already-computed create_river_tile_view() result
        for this same (gdf_pixels_step, rp_tier, is_standin). Safe to reuse
        across every admin level for the same country/tier/step:
        create_river_tile_view()'s own output (probability/below_min_basin/
        E_*) depends only on tile_id/geometry/pixel data, never on the admin
        'id' column admins_overlay() assigns per level, so recomputing it
        fresh per level (the default, tile_view=None) was pure duplicated
        work. Always copied before use here (this function mutates its own
        df_view in place), so passing the same tile_view into two calls for
        two different admin levels is safe.
    """
    if 'name' in gdf_admin.columns:
        name_by_id = gdf_admin.set_index('tile_id')['name'].to_dict()
    else:
        logger.warning("Admin GeoDataFrame missing 'name' column, admin region names will be NaN")
        name_by_id = {}

    df_view = (tile_view.copy() if tile_view is not None
               else create_river_tile_view(gdf_tiles, gdf_pixels_step, rp_tier, is_standin))

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


def create_river_facility_view(gdf_facilities, facility_tile_map, tile_view_df, id_column, is_standin,
                                tile_probs=None, tile_basin=None):
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
    a tile whose own probability is nonzero, a confusing, inconsistent
    result. Deriving facility exposure from the containing tile's own
    probability instead keeps the two views (tile and facility) consistent
    by construction. Wind/gust's envelope polygons don't have this failure
    mode (their independent per-facility buffer-intersect is left untouched),
    but precip's grid faces the identical patchy-vs-point problem and is
    solved the same way (create_precip_facility_view, this function's own
    direct template).

    Args:
        gdf_facilities: GeoDataFrame of facility locations (already deduped
            via _ensure_unique_zone_ids, type-filtered where relevant),
            UNBUFFERED. No buffer_geodataframe() call is needed by the caller:
            the tile lookup below needs each facility's true point/footprint
            for assign_facilities_to_tiles' own within-predicate join, not an
            inflated one; the output below also carries this true geometry
            (point, or polygon for HC building footprints), matching precip's
            own established convention.
        facility_tile_map: DataFrame from assign_facilities_to_tiles(),
            id_column + 'tile_id', computed ONCE per country (tile
            assignment doesn't depend on rp_tier/step_h), not once per call.
        tile_view_df: create_river_tile_view()'s own real output for this
            exact (rp_tier, step_h): 'zone_id' (==tile_id), 'probability',
            'below_min_basin'.
        id_column: 'school_id_giga' or 'osm_id'.
        is_standin: bool, same per-tier constant create_river_tile_view()
            already receives, passed straight through, not re-derived.
        tile_probs, tile_basin: optional, {tile_id: probability}/
            {tile_id: below_min_basin} dicts already derived from
            tile_view_df (via its own set_index('zone_id')[...].to_dict()).
            Safe to reuse across every facility type for the same
            (country, rp_tier, step_h): both dicts depend only on
            tile_view_df, never on which facility type is being looked up.
            Pass the caller's own precomputed dicts to skip rebuilding them
            when this is called once per facility type (schools/HCs/
            shelters/WASH) for the same tile_view_df; leave both None to
            build them here fresh (e.g. for a standalone call with no other
            facility types sharing this tile_view_df).

    Returns:
        DataFrame carrying every original attribute column from
        gdf_facilities (name/type/lat/lon/geometry/etc, matching
        create_precip_facility_view's own output shape) plus
        'probability'/'below_min_basin' (both from the containing tile) and
        'is_standin'. `id_column` is renamed to 'zone_id' in the output
        (unlike precip's own function, which leaves this rename to its SQL
        ingestion layer instead) to match SCHOOL_RIVER_MAT's ingestion SQL
        verbatim (`parquet_variant:zone_id`, not aliased from the facility's
        own id column the way precip's is). A facility whose tile assignment
        is missing (should not happen given assign_facilities_to_tiles' own
        nearest-tile fallback) defaults to 0.0/False, matching
        create_precip_facility_view's own convention.
    """
    if tile_probs is None:
        tile_probs = tile_view_df.set_index('zone_id')['probability'].to_dict()
    if tile_basin is None:
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


def river_country_tier_already_processed(country, forecast_time, rp_tier, max_step_h):
    """
    Checks whether a (country, rp_tier, forecast_time) combination already has
    real river-flood output on disk for the largest lead-time step, used by
    run_river_flood_analysis() as a per-(country, tier) idempotency check.

    AMBIENT_HAZARD_RUN_LOG only marks a whole (source, param, forecast_time)
    processed once every country in a run has been attempted, so a crash
    partway through the country loop leaves that marking never reached even
    though most countries genuinely already have real, saved output for this
    exact cycle. Without this check, a retry re-downloads every tier's file
    and reprocesses every country from scratch, discarding real completed
    work. RIVER_LEADTIME_STEPS_H's own values are cumulative window
    boundaries (see that constant's own comment), so the largest step_h's
    tile view existing on disk is a reliable proxy for "this (country, tier,
    forecast_time) already ran to completion"; a country that only got partway
    through the step_h loop before a crash correctly reports False here and
    is safely, idempotently reprocessed (every save_river_*() call overwrites
    in place).
    """
    forecast_time_str = forecast_time.strftime('%Y%m%d%H%M%S') if hasattr(forecast_time, 'strftime') else str(forecast_time)
    file_name = f"{country}_{forecast_time_str}_{rp_tier}_{max_step_h}h.csv"
    path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views_river', file_name)
    try:
        return data_store.file_exists(path)
    except Exception:
        return False


def save_river_tile_bitmask_view(df, country, forecast_time, rp_tier, step_h):
    """track_tile_bitmask_views_river/{country}_{forecast_time}_{rp_tier}_{step_h}h.parquet

    Same filename token order as save_river_tile_view's own mercator_views_river/
    convention (real key: country, forecast_time, rp_tier, step_h; river is
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

