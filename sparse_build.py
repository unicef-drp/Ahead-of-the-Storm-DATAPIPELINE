"""Sparse by construction view frames.

The dense view builders (precip_views, river_views, impact_analysis) create one row
per tile, or per facility, of a whole country for every hazard combination, and the
writers then thin that frame down to the nonzero rows plus one zero marker row
(sparse_output). For most combinations nearly every row is zero, so the dense frame is
pure overhead. The builders here produce the thin frame directly from the nonzero
hazard cells, without ever materialising the dense one.

The result of every frame builder is the frame that sparsify_tile_frame() or
sparsify_facility_frame() returns for the corresponding dense frame (same rows, order,
columns, dtypes and values, with a contiguous index), so the files written from it are
byte identical. Admin frames are the exception: sums and means are accumulated in a
different order than pandas groupby does, so values agree to floating point rounding
(about 1e-14 relative for the E_* columns; the float32 probability mean differs by up to
about 1.2e-7 relative because pandas averages the float32 column in float32).

Static structures (tile to native cell index, facility to tile index, admin codes) are
built once per country and process and reused for every hazard combination. Anything a
builder cannot reproduce exactly raises SparseUnsupported (or any other exception); the
call sites catch it, log a warning and use the dense path, so the output is always the
dense path's output.
"""
import logging
import math
import warnings

import numpy as np
import pandas as pd

from pipeline_shared import (
    FULL_ENSEMBLE_SIZE,
    POPULATION_COLS,
    _OPTIONAL_SUM_COLS,
    avg_cols,
    data_cols,
    sum_cols,
)

logger = logging.getLogger(__name__)

_POVERTY_COLS = ('moderate_poverty_prob', 'severe_poverty_prob')


class SparseUnsupported(Exception):
    """The inputs have a shape the sparse builders do not reproduce exactly."""


def popcount_u64(values):
    """Number of set bits per element of a uint64 array (SWAR), as uint64."""
    x = np.asarray(values).astype(np.uint64)
    x = x - ((x >> np.uint64(1)) & np.uint64(0x5555555555555555))
    x = (x & np.uint64(0x3333333333333333)) + ((x >> np.uint64(2)) & np.uint64(0x3333333333333333))
    x = (x + (x >> np.uint64(4))) & np.uint64(0x0f0f0f0f0f0f0f0f)
    return ((x * np.uint64(0x0101010101010101)) >> np.uint64(56)).astype(np.uint64)


def _ranges(starts, lengths):
    """Concatenation of arange(start, start + length) over all pairs, vectorised."""
    total = int(lengths.sum())
    if total == 0:
        return np.empty(0, dtype=np.int64)
    ends = np.cumsum(lengths)
    offsets = np.repeat(starts - (ends - lengths), lengths)
    return offsets + np.arange(total, dtype=np.int64)


def _grouped_sum(codes, values, n_groups):
    """Per group sums of float values, each correctly rounded (math.fsum), so the result does not depend on summation order."""
    out = np.zeros(n_groups, dtype=np.float64)
    if len(values) == 0:
        return out
    order = np.argsort(codes, kind='stable')
    sorted_codes = codes[order]
    sorted_values = values[order].astype(np.float64)
    cuts = np.flatnonzero(np.diff(sorted_codes)) + 1
    starts = np.concatenate(([0], cuts))
    ends = np.concatenate((cuts, [len(sorted_codes)]))
    for start, end in zip(starts.tolist(), ends.tolist()):
        out[sorted_codes[start]] = math.fsum(sorted_values[start:end])
    return out


class _CellIndex:
    """Tile to native grid cell mapping for one (grid bounds, grid shape) pair.

    cell = row * n_lon + col. Tile positions are grouped by cell (frame order inside a
    cell), so the tiles of any set of cells are found without touching other tiles.
    """

    def __init__(self, rows, cols, n_lon):
        self.n_lon = int(n_lon)
        self.row = rows.astype(np.int64)
        self.col = cols.astype(np.int64)
        self.cell = self.row * self.n_lon + self.col
        order = np.argsort(self.cell, kind='stable')
        grouped = self.cell[order]
        self.ucell, self.cstart, self.ccount = np.unique(grouped, return_index=True, return_counts=True)
        self.order = order

    def tiles_of_cells(self, cells):
        """Sorted (frame order) positions of the tiles whose native cell is in `cells`."""
        cells = np.asarray(cells, dtype=np.int64)
        if len(cells) == 0 or len(self.ucell) == 0:
            return np.empty(0, dtype=np.int64)
        j = np.minimum(np.searchsorted(self.ucell, cells), len(self.ucell) - 1)
        hit = self.ucell[j] == cells
        jh = j[hit]
        pos = self.order[_ranges(self.cstart[jh], self.ccount[jh])]
        pos.sort()
        return pos


class TileStatic:
    """Static per country structure of one tile GeoDataFrame (whole country or one admin level).

    Holds the BASE attribute arrays in their native dtypes, so `base * probability`
    promotes exactly as the dense frame's column multiplication does, plus the admin
    codes and per admin denominators. The native cell index depends on the hazard grid
    and is built on first use per (bounds, shape).
    """

    def __init__(self, gdf_tiles, require_population=False):
        if len(gdf_tiles) == 0:
            raise SparseUnsupported("no tiles")
        geometry_name = getattr(gdf_tiles, '_geometry_column_name', 'geometry')
        known = {'tile_id', 'id', geometry_name, *data_cols, *_POVERTY_COLS}
        extra = [c for c in gdf_tiles.columns if c not in known]
        if extra:
            raise SparseUnsupported(f"unexpected tile columns {extra}")
        if 'tile_id' not in gdf_tiles.columns:
            raise SparseUnsupported("no tile_id column")
        if require_population:
            missing = [c for c in POPULATION_COLS if c not in gdf_tiles.columns]
            if missing:
                raise SparseUnsupported(f"missing population columns {missing}")
        self._gdf = gdf_tiles
        self.n = len(gdf_tiles)
        self.tile_ids = gdf_tiles['tile_id'].to_numpy()
        if not pd.Index(self.tile_ids).is_unique:
            raise SparseUnsupported("tile_id values are not unique")
        self.admin = gdf_tiles['id'].to_numpy() if 'id' in gdf_tiles.columns else None
        self.base = {c: gdf_tiles[c].to_numpy() for c in data_cols if c in gdf_tiles.columns}
        self.poverty = {c: gdf_tiles[c].to_numpy() for c in _POVERTY_COLS if c in gdf_tiles.columns}
        self.has_admin = self.admin is not None
        if self.has_admin:
            codes, uniques = pd.factorize(self.admin, sort=True)
            if (codes < 0).any():
                raise SparseUnsupported("tiles without an admin id")
            self.admin_codes = codes.astype(np.int64)
            self.admin_unique = uniques
            n_admin = len(uniques)
            self.admin_tiles = np.bincount(self.admin_codes, minlength=n_admin)
            self.admin_nonnull = {
                c: np.bincount(self.admin_codes, weights=pd.notna(v).astype(float), minlength=n_admin).astype(np.int64)
                for c, v in self.base.items()
            }
        self._centroids = None
        self._cell_indexes = {}
        self._pos_of = None

    def position_of(self, tile_ids):
        """Frame positions of tile ids (-1 where a tile id is not in this country)."""
        if self._pos_of is None:
            self._pos_of = pd.Index(self.tile_ids)
        return self._pos_of.get_indexer(tile_ids)

    def cell_index(self, bounds, shape):
        key = (tuple(float(b) for b in bounds), tuple(int(s) for s in shape))
        index = self._cell_indexes.get(key)
        if index is None:
            from precip_views import _precip_grid_row_col  # local import: precip_views does not import this module
            if self._centroids is None:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', message='Geometry is in a geographic CRS')
                    centroids = self._gdf.geometry.centroid
                self._centroids = (centroids.x.to_numpy(), centroids.y.to_numpy())
            cx, cy = self._centroids
            lat_min, lat_max, lon_min, lon_max = bounds
            n_lat, n_lon = shape
            rows, cols = _precip_grid_row_col(cx, cy, lat_min, lat_max, lon_min, lon_max, n_lat, n_lon)
            index = _CellIndex(rows, cols, n_lon)
            self._cell_indexes[key] = index
        return index

    def hits_from_grid(self, grid, bounds):
        """(positions, float32 probabilities) of the tiles with a nonzero value in `grid`.

        The probability is the grid value at the tile's native cell cast to float32, as
        create_precip_tile_view() samples it. A value that is zero after the cast counts
        as zero, as it does in the dense frame.
        """
        index = self.cell_index(bounds, grid.shape)
        rows, cols = np.nonzero(grid)
        cells = rows.astype(np.int64) * index.n_lon + cols.astype(np.int64)
        pos = index.tiles_of_cells(cells)
        prob = grid[index.row[pos], index.col[pos]].astype(np.float32)
        keep = prob != 0
        return pos[keep], prob[keep]

    def bitmask_frame(self, bitmask_grid, bounds):
        """Frame calculate_precip_tile_member_bitmask() returns, from the nonzero cells only."""
        index = self.cell_index(bounds, bitmask_grid.shape)
        rows, cols = np.nonzero(bitmask_grid)
        cells = rows.astype(np.int64) * index.n_lon + cols.astype(np.int64)
        pos = index.tiles_of_cells(cells)
        bits = bitmask_grid[index.row[pos], index.col[pos]]
        keep = bits != 0
        pos, bits = pos[keep], bits[keep]
        if len(pos) == 0:
            return pd.DataFrame({'tile_id': pd.Series(dtype='string'), 'bits': pd.Series(dtype='uint64')})
        return pd.DataFrame({
            'tile_id': pd.array(self.tile_ids[pos].astype(str), dtype='string'),
            'bits': bits,
        })

    def hits_from_bitmask(self, bm_df, num_ensembles=FULL_ENSEMBLE_SIZE):
        """(positions, float64 probabilities) from a tile/bits frame (wind, gust)."""
        if bm_df is None:
            raise SparseUnsupported("no bitmask for this threshold")
        if bm_df.empty:
            return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)
        pos = self.position_of(bm_df['tile_id'].to_numpy())
        if (pos < 0).any():
            raise SparseUnsupported("bitmask tile ids outside the tile frame")
        counts = popcount_u64(bm_df['bits'].to_numpy())
        order = np.argsort(pos, kind='stable')
        pos, counts = pos[order], counts[order]
        if len(pos) > 1 and (np.diff(pos) == 0).any():
            raise SparseUnsupported("duplicate tile ids in the bitmask")
        prob = counts.astype(float) / float(num_ensembles)
        keep = prob != 0
        return pos[keep], prob[keep]

    def hits_from_member_counts(self, member_counts, basin_flags, num_ensembles=FULL_ENSEMBLE_SIZE):
        """(positions, float64 probabilities, below_min_basin flags) from per tile river counts."""
        if len(member_counts) == 0:
            return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64), np.empty(0, dtype=bool)
        pos = self.position_of(member_counts.index.to_numpy())
        if (pos < 0).any():
            raise SparseUnsupported("river tile ids outside the tile frame")
        order = np.argsort(pos, kind='stable')
        pos = pos[order]
        prob = member_counts.to_numpy()[order].astype(float) / float(num_ensembles)
        basin = basin_flags.reindex(member_counts.index).to_numpy()[order].astype(bool)
        keep = prob != 0
        return pos[keep], prob[keep], basin[keep]


def _first_missing_position(pos, n):
    """First position in 0..n-1 that is not in the sorted array `pos` (None if all are present)."""
    if len(pos) == n:
        return None
    if len(pos) == 0 or pos[0] != 0:
        return 0
    gap = np.flatnonzero(pos != np.arange(len(pos)))
    return int(gap[0]) if len(gap) else len(pos)


def tile_frame(static, pos, prob, native_cells_of=None, poverty_last=False, basin=None, is_standin=None):
    """Thin tile frame: every nonzero tile plus one zero marker row.

    pos: sorted tile positions with nonzero probability; prob: their probabilities (the
    dtype of the dense probability column: float32 for precip, float64 otherwise).
    native_cells_of: a _CellIndex to emit native_cell_row / native_cell_col (precip),
    None otherwise. basin / is_standin: river columns. poverty_last: wind, gust and river
    keep the raw base poverty columns last.

    The marker is the first zero probability tile in frame order; where one of its E_*
    values is missing it takes the maximum of that column over all zero rows, which is the
    rule sparsify_tile_frame() applies.
    """
    n = static.n
    marker = _first_missing_position(pos, n)
    insert_at = None
    if marker is None:
        rows, p, b = pos, prob, basin
    else:
        insert_at = int(np.searchsorted(pos, marker))
        rows = np.insert(pos, insert_at, marker)
        p = np.insert(prob, insert_at, 0)
        b = np.insert(basin, insert_at, False) if basin is not None else None

    out = {'zone_id': static.tile_ids[rows]}
    if static.has_admin:
        out['id'] = static.admin[rows]
    out['probability'] = p
    if b is not None:
        out['below_min_basin'] = b
    if native_cells_of is not None:
        out['native_cell_row'] = native_cells_of.row[rows]
        out['native_cell_col'] = native_cells_of.col[rows]
    for c in data_cols:
        if c in static.base:
            out['E_' + c] = static.base[c][rows] * p
        else:
            out['E_' + c] = np.full(len(rows), np.nan)
    if is_standin is not None:
        out['is_standin'] = np.full(len(rows), bool(is_standin))
    if poverty_last:
        for c, values in static.poverty.items():
            out[c] = values[rows]
    df = pd.DataFrame(out)

    if marker is not None:
        zero_rows = np.ones(n, dtype=bool)
        zero_rows[pos] = False
        zero_p = np.zeros(1, dtype=prob.dtype)
        for c in data_cols:
            col = 'E_' + c
            if c not in static.base or not pd.isna(df[col].iloc[insert_at]):
                continue
            fill = pd.Series(static.base[c][zero_rows] * zero_p[0]).max()
            if not pd.isna(fill):
                df.iloc[insert_at, df.columns.get_loc(col)] = fill
    return df


class FacilityStatic:
    """Facility to tile position index for one facility GeoDataFrame and its tile map."""

    def __init__(self, gdf_facilities, tile_map, id_column, static):
        for reserved in ('tile_id', 'probability'):
            if reserved in gdf_facilities.columns:
                raise SparseUnsupported(f"facility frame already has a {reserved} column")
        ids = gdf_facilities[id_column]
        if ids.isna().any() or not ids.is_unique:
            raise SparseUnsupported("facility ids are missing or not unique")
        if not tile_map[id_column].is_unique:
            raise SparseUnsupported("facility tile map has duplicate ids")
        self.id_column = id_column
        tile_of_facility = ids.map(tile_map.set_index(id_column)['tile_id'])
        positions = static.position_of(tile_of_facility.to_numpy())
        positions = np.where(tile_of_facility.isna().to_numpy(), -1, positions)
        self.tile_pos = positions.astype(np.int64)
        has_tile = np.flatnonzero(self.tile_pos >= 0)
        order = has_tile[np.argsort(self.tile_pos[has_tile], kind='stable')]
        self.order = order
        sorted_tiles = self.tile_pos[order]
        grid = np.arange(static.n)
        self.start = np.searchsorted(sorted_tiles, grid, side='left')
        self.count = np.searchsorted(sorted_tiles, grid, side='right') - self.start

    def facilities_of_tiles(self, tile_pos):
        return np.sort(self.order[_ranges(self.start[tile_pos], self.count[tile_pos])])


def facility_frame(gdf_facilities, fstatic, pos, prob, basin=None, is_standin=None):
    """Facilities that sit in a nonzero tile, original order and columns.

    probability is the tile's value as float64 (the dense frame maps python floats from a
    dict). With `basin` (per nonzero tile flags) the river columns below_min_basin and
    is_standin are added and the id column is renamed to zone_id, as
    create_river_facility_view() does.
    """
    selected = fstatic.facilities_of_tiles(pos)
    out = gdf_facilities.iloc[selected].copy()
    k = np.searchsorted(pos, fstatic.tile_pos[selected])
    out['probability'] = prob[k].astype(np.float64)
    if basin is not None:
        out['below_min_basin'] = basin[k].astype(bool)
        out['is_standin'] = bool(is_standin)
        out = out.rename(columns={fstatic.id_column: 'zone_id'})
    return out.reset_index(drop=True)


def admin_view(static, pos, prob, name_by_id, population_optional, basin=None, is_standin=None):
    """Admin rollup of a tile frame computed from the nonzero tiles and static denominators.

    Sums of E_* run over the nonzero tiles (zero tiles add nothing); means divide by the
    static per admin tile count (probability) or non-missing base count (E_ means).
    population_optional: E_population stays NaN when every tile of a region is missing
    (precip and ratio), instead of summing to 0 (river, wind, gust).
    """
    if not static.has_admin:
        raise SparseUnsupported("tile frame has no admin ids")
    n_admin = len(static.admin_unique)
    a = static.admin_codes[pos]
    values = {}
    for c in data_cols:
        values['E_' + c] = (static.base[c][pos] * prob) if c in static.base else np.full(len(pos), np.nan)

    def accumulate(x):
        m = ~np.isnan(x)
        return _grouped_sum(a[m], x[m], n_admin)

    optional = set(_OPTIONAL_SUM_COLS)
    if population_optional:
        optional.add('E_population')
    res = {}
    for col in sum_cols:
        total = accumulate(values[col])
        if col in optional:
            base = col[2:]
            has_value = (static.admin_nonnull[base] > 0) if base in static.admin_nonnull else np.zeros(n_admin, dtype=bool)
            total = np.where(has_value, total, np.nan)
        res[col] = total
    for col in avg_cols:
        if col == 'probability':
            mean = _grouped_sum(a, prob, n_admin) / static.admin_tiles
            res[col] = mean.astype(prob.dtype)
        else:
            base = col[2:]
            den = static.admin_nonnull[base].astype(float) if base in static.admin_nonnull else np.zeros(n_admin)
            with np.errstate(invalid='ignore', divide='ignore'):
                res[col] = np.where(den > 0, accumulate(values[col]) / den, np.nan)
    if basin is not None:
        res['below_min_basin'] = np.bincount(a, weights=basin.astype(float), minlength=n_admin) > 0
    df = pd.DataFrame({'tile_id': static.admin_unique, **res})
    if is_standin is not None:
        df['is_standin'] = bool(is_standin)
    df['name'] = df['tile_id'].map(name_by_id)
    missing = int(df['name'].isna().sum())
    if missing > 0:
        logger.warning(f"  {missing} admin region(s) have no name mapping in the sparse admin view")
    return df


class PrecipSparseContext:
    """Per country holder of the static structures for precip and ratio views.

    gdf_tiles: whole country tiles. admin_tiles_by_level: {level: tile GeoDataFrame}
    (level 1 is the same object as gdf_tiles). facilities: {name: (gdf, tile_map, id_column)}.
    Statics are built on first use and kept for the life of the context.
    """

    def __init__(self, gdf_tiles, admin_tiles_by_level, facilities):
        self.gdf_tiles = gdf_tiles
        self.static = TileStatic(gdf_tiles)
        self._admin_tiles = admin_tiles_by_level
        self._admin_static = {}
        self._facilities = facilities
        self._facility_static = {}

    def admin_static(self, level):
        admin_tiles = self._admin_tiles[level]
        if admin_tiles is self.gdf_tiles:
            return self.static
        static = self._admin_static.get(level)
        if static is None:
            static = TileStatic(admin_tiles)
            self._admin_static[level] = static
        return static

    def facility_static(self, name):
        fstatic = self._facility_static.get(name)
        if fstatic is None:
            gdf, tile_map, id_column = self._facilities[name]
            fstatic = FacilityStatic(gdf, tile_map, id_column, self.static)
            self._facility_static[name] = fstatic
        return fstatic

    def combo(self, grid, bounds):
        return PrecipCombo(self, grid, bounds)


class PrecipCombo:
    """Nonzero tiles of one precip or ratio probability grid and the frames built from them."""

    def __init__(self, context, grid, bounds):
        self.context = context
        self.grid = grid
        self.bounds = bounds
        static = context.static
        self.pos, self.prob = static.hits_from_grid(grid, bounds)

    def tile_frame(self):
        static = self.context.static
        return tile_frame(static, self.pos, self.prob, native_cells_of=static.cell_index(self.bounds, self.grid.shape))

    def bitmask_frame(self, bitmask_grid):
        return self.context.static.bitmask_frame(bitmask_grid, self.bounds)

    def facility_frame(self, name):
        gdf = self.context._facilities[name][0]
        return facility_frame(gdf, self.context.facility_static(name), self.pos, self.prob)

    def admin_view(self, level, name_by_id):
        static = self.context.admin_static(level)
        if static is self.context.static:
            pos, prob = self.pos, self.prob
        else:
            pos, prob = static.hits_from_grid(self.grid, self.bounds)
        return admin_view(static, pos, prob, name_by_id, population_optional=True)


def admin_names(gdf_admin):
    """{admin id: name} as the dense admin views build it (empty when there is no name column)."""
    if 'name' in gdf_admin.columns:
        return gdf_admin.set_index('tile_id')['name'].to_dict()
    logger.warning("Admin GeoDataFrame missing 'name' column, admin region names will be NaN")
    return {}


class RiverSparseContext:
    """Per country holder of the static structures for river views (see PrecipSparseContext)."""

    def __init__(self, gdf_tiles, admin_tiles_by_level, facilities):
        self.gdf_tiles = gdf_tiles
        self.static = TileStatic(gdf_tiles)
        self._admin_tiles = admin_tiles_by_level
        self._admin_static = {}
        self._facilities = facilities
        self._facility_static = {}

    def admin_static(self, level):
        """Static of an admin level's tile frame, which must cover exactly this country's tiles."""
        admin_tiles = self._admin_tiles[level]
        if admin_tiles is self.gdf_tiles:
            return self.static
        static = self._admin_static.get(level)
        if static is None:
            static = TileStatic(admin_tiles)
            if static.n != self.static.n or (static.position_of(self.static.tile_ids) < 0).any():
                raise SparseUnsupported("admin tile frame differs from the country tile frame")
            self._admin_static[level] = static
        return static

    def facility_static(self, name):
        fstatic = self._facility_static.get(name)
        if fstatic is None:
            gdf, tile_map, id_column = self._facilities[name]
            fstatic = FacilityStatic(gdf, tile_map, id_column, self.static)
            self._facility_static[name] = fstatic
        return fstatic

    def empty_tile_frame(self, is_standin):
        """Marker only tile frame for a tier and step without any flooded pixel."""
        return tile_frame(self.static, np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64),
                          basin=np.empty(0, dtype=bool), is_standin=is_standin, poverty_last=True)


class RiverStep:
    """Flooded tiles of one (tier, step) from the per tile member counts and the frames built from them."""

    def __init__(self, context, member_counts, basin_flags, is_standin):
        self.context = context
        self.is_standin = is_standin
        if member_counts is None or len(member_counts) == 0:
            self.pos = np.empty(0, dtype=np.int64)
            self.prob = np.empty(0, dtype=np.float64)
            self.basin = np.empty(0, dtype=bool)
        else:
            self.pos, self.prob, self.basin = context.static.hits_from_member_counts(member_counts, basin_flags)

    @classmethod
    def from_tile_bits(cls, context, tile_ids, bits, basin, is_standin):
        """
        RiverStep from per tile results: the uint64 member bitmask and
        below_min_basin flag of each flooded tile. The distinct member count
        of a tile is the popcount of its bitmask.
        """
        tile_ids = np.asarray(tile_ids)
        index = pd.Index(tile_ids, name='tile_id')
        member_counts = pd.Series(popcount_u64(np.asarray(bits)).astype(np.int64), index=index)
        basin_flags = pd.Series(np.asarray(basin, dtype=bool), index=index)
        return cls(context, member_counts, basin_flags, is_standin)

    def tile_frame(self):
        return tile_frame(self.context.static, self.pos, self.prob, poverty_last=True,
                          basin=self.basin, is_standin=self.is_standin)

    def facility_frame(self, name):
        gdf = self.context._facilities[name][0]
        return facility_frame(gdf, self.context.facility_static(name), self.pos, self.prob,
                              basin=self.basin, is_standin=self.is_standin)

    def admin_view(self, level, name_by_id):
        static = self.context.admin_static(level)
        pos = self.pos
        if static is not self.context.static:
            pos = static.position_of(self.context.static.tile_ids[self.pos])
        return admin_view(static, pos, self.prob, name_by_id, population_optional=False,
                          basin=self.basin, is_standin=self.is_standin)


class EnvelopeSparse:
    """Thin tile and admin frames for wind or gust envelopes, built from the tile member bitmasks.

    gdf_tiles: whole country tiles (population columns required, as the dense builders
    require them). admin_tiles_by_level: {level: tile GeoDataFrame}. Probability is the
    number of member bits over the full ensemble size, as float64.
    """

    def __init__(self, gdf_tiles, admin_tiles_by_level):
        self.gdf_tiles = gdf_tiles
        self.static = TileStatic(gdf_tiles, require_population=True)
        self._admin_tiles = admin_tiles_by_level
        self._admin_static = {}

    def admin_static(self, level):
        admin_tiles = self._admin_tiles[level]
        if admin_tiles is self.gdf_tiles:
            return self.static
        static = self._admin_static.get(level)
        if static is None:
            static = TileStatic(admin_tiles, require_population=True)
            self._admin_static[level] = static
        return static

    def tile_frames(self, thresholds, bitmask_views):
        """{threshold: thin tile frame}, in the order of `thresholds`."""
        out = {}
        for th in thresholds:
            pos, prob = self.static.hits_from_bitmask(bitmask_views.get(int(th)))
            out[th] = tile_frame(self.static, pos, prob, poverty_last=True)
        return out

    def admin_views(self, level, gdf_admin, thresholds, bitmask_views):
        """{threshold: admin frame}, in the order of `thresholds`."""
        static = self.admin_static(level)
        names = admin_names(gdf_admin)
        out = {}
        for th in thresholds:
            pos, prob = static.hits_from_bitmask(bitmask_views.get(int(th)))
            out[th] = admin_view(static, pos, prob, names, population_optional=False)
        return out
