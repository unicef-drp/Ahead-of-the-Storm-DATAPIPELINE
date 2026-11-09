#!/usr/bin/env python3
"""
River Tile Source Module

Per-tile river-flood member coverage straight from the RP-tier Parquet files,
aggregated inside DuckDB, so a country's memory use does not scale with the
number of flooded (pixel, member, step) rows in its bounding box.

The consumers of a river tile view need, per z14 tile and per lead-time
window, three things: the uint64 member bitmask (bit = member - 1, members
1..51), the distinct member count (the popcount of that bitmask) and whether
any contributing pixel carries below_min_basin. load_river_tile_bits()
computes exactly those, already unioned across step_h <= window and
restricted to the country's own tile grid, from either file layout:

    AggregatedBymemberSource   the per (pixel, member, step) rows of
                               extent_rpN_bymember, reduced to one row per
                               pixel inside DuckDB first, then to one row
                               per tile
    BymaskSource               the compact extent_bymask_rpN file, one row
                               per pixel with a uint64 member mask per step

Pixel to tile assignment is integer arithmetic on the web-mercator tile
index plus an exact strict-inequality test against the stored tile bounds,
which reproduces the 'within' predicate of a geopandas point-in-polygon join
(a pixel exactly on a tile edge belongs to no tile). A tile grid that is not
made of axis-aligned z-tiles raises RiverTileSourceUnsupported so the caller
can move to the next source (compact mask, then aggregated by-member file,
then skip the tier).

The module also holds the compact mask file pointer selection and the
RIVER_MEM telemetry line.

Usage:
    from river_tile_source import BymaskSource, load_river_tile_bits
    result = load_river_tile_bits(BymaskSource(path), gdf_tiles, [24, 72, 120, 168])
    result.by_window[72]   # DataFrame[tile_id, bits, below_min_basin]
"""

import logging
import os
import re
import resource
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# DuckDB buffer pool for one aggregation. The pixel stage holds about one row
# per distinct flooded pixel, far below this; beyond it DuckDB spills.
_DUCKDB_MEMORY_LIMIT = '2GB'

# Tolerance, in tile units, inside which a pixel is tested against the
# neighbouring tile as well, so float rounding in the tile index formula can
# never decide which tile an edge pixel belongs to (the strict bounds test
# against the stored geometry does).
_EDGE_EPS = 1e-6

_MASK_COLUMN = re.compile(r'^m(\d+)$')


class RiverTileSourceUnsupported(Exception):
    """The inputs have a shape the tile aggregation does not reproduce exactly."""


# =============================================================================
# TELEMETRY
# =============================================================================

def peak_rss_mb() -> float:
    """Peak resident set size of this process in MB (ru_maxrss is bytes on macOS, KiB elsewhere)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024.0 * 1024.0) if sys.platform == 'darwin' else peak / 1024.0


def current_rss_mb() -> Optional[float]:
    """Current resident set size of this process in MB, None where /proc is unavailable."""
    try:
        with open('/proc/self/statm') as f:
            pages = int(f.read().split()[1])
        return pages * resource.getpagesize() / (1024.0 * 1024.0)
    except Exception:
        return None


def format_river_mem(country, rp, mode, rows, tiles, wall_s, rss_start_mb=None, hit_tiles=None) -> str:
    """
    One greppable telemetry line for a (country, tier) pass:
    RIVER_MEM country=<CC> tier=<rpN> mode=<agg|bymask> rows=<n> tiles=<n> peak_rss_mb=<x> wall_s=<y>
    Optional trailing fields: rss_start_mb (resident size when the pass began,
    the baseline the peak is measured against, since the peak is cumulative
    for the whole process) and hit_tiles (tiles with any flooded member in
    the widest window).
    """
    line = (f"RIVER_MEM country={country} tier={rp} mode={mode} rows={int(rows)} tiles={int(tiles)} "
            f"peak_rss_mb={peak_rss_mb():.0f} wall_s={wall_s:.1f}")
    if rss_start_mb is not None:
        line += f" rss_start_mb={rss_start_mb:.0f}"
    if hit_tiles is not None:
        line += f" hit_tiles={int(hit_tiles)}"
    return line


# =============================================================================
# TILE GRID
# =============================================================================

def _quadkey_to_xy(tile_ids):
    """(x, y, zoom) integer tile coordinates of equal-length quadkeys, vectorised."""
    ids = np.asarray(tile_ids)
    n = len(ids)
    if n == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.int64), 0
    zoom = len(str(ids[0]))
    raw = ''.join(str(q) for q in ids)
    if len(raw) != n * zoom or zoom == 0:
        raise RiverTileSourceUnsupported("tile ids do not share one quadkey length")
    digits = np.frombuffer(raw.encode('ascii'), dtype=np.uint8).reshape(n, zoom).astype(np.int64) - 48
    if digits.min() < 0 or digits.max() > 3:
        raise RiverTileSourceUnsupported("tile ids are not quadkeys")
    x = np.zeros(n, dtype=np.int64)
    y = np.zeros(n, dtype=np.int64)
    for k in range(zoom):
        x = (x << 1) | (digits[:, k] & 1)
        y = (y << 1) | (digits[:, k] >> 1)
    return x, y, zoom


class TileGrid:
    """
    A country's tile grid as the lookup table the aggregation joins against:
    one row per tile with its integer tile key and the bounds of its stored
    geometry. Raises RiverTileSourceUnsupported unless every tile is an
    axis-aligned rectangle on the z-tile of its quadkey, the shape under which
    the strict bounds test equals a point-in-polygon 'within' join.
    """

    def __init__(self, gdf_tiles):
        self.tile_ids = gdf_tiles['tile_id'].to_numpy()
        n = len(self.tile_ids)
        self.n = n
        if n == 0:
            self.zoom = 0
            self.table = pd.DataFrame({'tid': np.empty(0, dtype=np.int64), 'key': np.empty(0, dtype=np.int64),
                                       'minx': np.empty(0), 'maxx': np.empty(0),
                                       'miny': np.empty(0), 'maxy': np.empty(0)})
            return
        tx, ty, zoom = _quadkey_to_xy(self.tile_ids)
        self.zoom = zoom
        side = 2 ** zoom
        keys = tx * side + ty
        if len(np.unique(keys)) != n:
            raise RiverTileSourceUnsupported("duplicate tile ids in the tile grid")
        bounds = gdf_tiles.geometry.bounds
        minx, maxx = bounds['minx'].to_numpy(), bounds['maxx'].to_numpy()
        miny, maxy = bounds['miny'].to_numpy(), bounds['maxy'].to_numpy()
        # The stored bounds must be those of the quadkey's tile, otherwise the
        # tile index of a pixel does not identify the stored tile.
        west = tx / side * 360.0 - 180.0
        east = (tx + 1) / side * 360.0 - 180.0
        north = np.degrees(np.arctan(np.sinh(np.pi * (1.0 - 2.0 * ty / side))))
        south = np.degrees(np.arctan(np.sinh(np.pi * (1.0 - 2.0 * (ty + 1) / side))))
        if not (np.allclose(minx, west, atol=1e-9, rtol=0) and np.allclose(maxx, east, atol=1e-9, rtol=0)
                and np.allclose(miny, south, atol=1e-9, rtol=0) and np.allclose(maxy, north, atol=1e-9, rtol=0)):
            raise RiverTileSourceUnsupported("tile geometry bounds differ from the quadkey tile bounds")
        import shapely
        area = shapely.area(np.asarray(gdf_tiles.geometry.values))
        box_area = (maxx - minx) * (maxy - miny)
        if not np.allclose(area, box_area, rtol=1e-9, atol=0):
            raise RiverTileSourceUnsupported("tile geometry is not an axis-aligned rectangle")
        self.table = pd.DataFrame({'tid': np.arange(n, dtype=np.int64), 'key': keys,
                                   'minx': minx, 'maxx': maxx, 'miny': miny, 'maxy': maxy})


# =============================================================================
# SOURCES
# =============================================================================

def _bbox_clause(bounds) -> str:
    xmin, ymin, xmax, ymax = (float(v) for v in bounds)
    return (f"pixel_lon BETWEEN {xmin!r} AND {xmax!r} AND pixel_lat BETWEEN {ymin!r} AND {ymax!r}")


class AggregatedBymemberSource:
    """
    extent_rpN_bymember: one row per flooded (pixel, member, step) with
    columns pixel_lat, pixel_lon, member, step_h, below_min_basin. Reduced to
    one row per pixel (a member mask per window) inside DuckDB.
    """
    name = 'agg'

    def __init__(self, path):
        self.path = path

    def pixel_sql(self, windows, bounds) -> str:
        cols = []
        for w in windows:
            cols.append(
                f"coalesce(bit_or(CASE WHEN step_h <= {w} THEN (1::UBIGINT << ((member - 1)::UBIGINT)) "
                f"ELSE 0::UBIGINT END), 0::UBIGINT) AS m{w}")
            cols.append(f"coalesce(bool_or(below_min_basin) FILTER (WHERE step_h <= {w}), false) AS b{w}")
            cols.append(f"count(*) FILTER (WHERE step_h <= {w}) AS n{w}")
        cols.append("count(*) AS n_all")
        return (f"SELECT pixel_lat, pixel_lon, {', '.join(cols)} "
                f"FROM read_parquet('{self.path}') WHERE {_bbox_clause(bounds)} "
                f"GROUP BY pixel_lat, pixel_lon")


class BymaskSource:
    """
    extent_bymask_rpN: one row per pixel with columns pixel_lat, pixel_lon,
    one uint64 member mask per step (m024, m048, ..., discovered by column
    name; bit = member - 1) and below_min_basin. A window mask is the OR of
    the per-step masks up to the window.
    """
    name = 'bymask'

    def __init__(self, path):
        self.path = path

    def step_columns(self) -> Dict[int, str]:
        con = duckdb.connect()
        try:
            names = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{self.path}')").fetchall()]
        finally:
            con.close()
        steps = {}
        for name in names:
            m = _MASK_COLUMN.match(name)
            if m:
                steps[int(m.group(1))] = name
        if not steps:
            raise RiverTileSourceUnsupported(f"no per-step mask columns in {os.path.basename(self.path)}")
        return steps

    def pixel_sql(self, windows, bounds) -> str:
        steps = self.step_columns()
        cols = []
        for w in windows:
            members = [f'"{steps[s]}"' for s in sorted(steps) if s <= w]
            cols.append(f"({' | '.join(members) if members else '0::UBIGINT'}) AS m{w}")
            cols.append(f"coalesce(below_min_basin, false) AS b{w}")
            # Rows of the equivalent per (pixel, member, step) file: one per set bit.
            counts = [f"bit_count({c})::BIGINT" for c in members]
            cols.append(f"({' + '.join(counts) if counts else '0::BIGINT'}) AS n{w}")
        all_bits = ' + '.join(f"bit_count(\"{c}\")::BIGINT" for _, c in sorted(steps.items()))
        cols.append(f"({all_bits}) AS n_all")
        return (f"SELECT pixel_lat, pixel_lon, {', '.join(cols)} "
                f"FROM read_parquet('{self.path}') WHERE {_bbox_clause(bounds)}")


# =============================================================================
# LOAD
# =============================================================================

@dataclass
class RiverTileBits:
    """
    Result of load_river_tile_bits(). `by_window[w]` is the sparse frame
    [tile_id (str), bits (uint64), below_min_basin (bool)] of tiles with at
    least one flooded member in the window, sorted by tile_id.
    `rows_by_window[w]` is the number of flooded (pixel, member, step) rows in
    the country's bounding box with step_h <= w, whether or not they fall
    inside a tile, and `has_pixels[w]` whether that is non-zero (the condition
    under which a window is written as a real view rather than a marker).
    `rows` is the row count over every step in the bounding box.
    """
    source: str
    windows: List[int]
    by_window: Dict[int, pd.DataFrame] = field(default_factory=dict)
    has_pixels: Dict[int, bool] = field(default_factory=dict)
    rows_by_window: Dict[int, int] = field(default_factory=dict)
    rows: int = 0

    @property
    def hit_tiles(self) -> int:
        widest = max(self.windows) if self.windows else None
        return len(self.by_window[widest]) if widest in self.by_window else 0


def _empty_bits_frame() -> pd.DataFrame:
    return pd.DataFrame({'tile_id': pd.array([], dtype='string'),
                         'bits': np.empty(0, dtype=np.uint64),
                         'below_min_basin': np.empty(0, dtype=bool)})


def load_river_tile_bits(source, gdf_tiles, windows, bounds=None, memory_limit=_DUCKDB_MEMORY_LIMIT,
                         threads=None) -> RiverTileBits:
    """
    Per window, the tiles of `gdf_tiles` with flooded members and their
    member bitmask and below_min_basin flag, from one DuckDB aggregation of
    `source`'s Parquet file.

    Args:
        source: AggregatedBymemberSource or BymaskSource.
        gdf_tiles: the country's tile GeoDataFrame ('tile_id' quadkeys plus
            rectangular z-tile geometry). Only these tiles are returned.
        windows: lead-time windows in hours (e.g. [24, 72, 120, 168]); a
            window unions every step with step_h <= window.
        bounds: (xmin, ymin, xmax, ymax) bounding-box filter pushed into the
            Parquet scan; defaults to gdf_tiles.total_bounds.
        memory_limit, threads: DuckDB settings for this one aggregation.

    Raises:
        RiverTileSourceUnsupported: the tile grid is not made of rectangular
            z-tiles; the caller moves to the next source, or skips the tier
            when none is left.
    """
    windows = [int(w) for w in windows]
    grid = TileGrid(gdf_tiles)
    if bounds is None:
        bounds = gdf_tiles.total_bounds
    result = RiverTileBits(source=source.name, windows=windows)
    pixel_sql = source.pixel_sql(windows, bounds)

    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
        con.execute("SET preserve_insertion_order = false")
        if threads:
            con.execute(f"PRAGMA threads={int(threads)}")
        con.execute(f"CREATE TEMP TABLE px AS {pixel_sql}")

        # Results are fetched completely: a partially fetched result keeps the connection busy in newer
        # DuckDB versions and the next statement on it (register) would fail.
        row_total = con.execute("SELECT coalesce(sum(n_all), 0)::BIGINT FROM px").fetchall()[0][0]
        result.rows = int(row_total)
        window_rows = con.execute(
            f"SELECT {', '.join(f'coalesce(sum(n{w}), 0)::BIGINT' for w in windows)} FROM px").fetchall()[0]
        for w, n in zip(windows, window_rows):
            result.rows_by_window[w] = int(n)
            result.has_pixels[w] = int(n) > 0

        if grid.n == 0 or not any(result.has_pixels.values()):
            for w in windows:
                result.by_window[w] = _empty_bits_frame()
            return result

        side = 2 ** grid.zoom
        con.register('tiles_df', grid.table)
        con.execute("CREATE TEMP TABLE tiles AS SELECT * FROM tiles_df")
        agg_cols = []
        for w in windows:
            agg_cols.append(f"bit_or(m{w}) AS bits{w}")
            agg_cols.append(f"coalesce(bool_or(b{w}) FILTER (WHERE m{w} <> 0), false) AS basin{w}")
        tile_sql = f"""
            WITH f AS (
                SELECT *,
                       (pixel_lon + 180.0) / 360.0 * {side} AS fx,
                       (1.0 - ln(tan(radians(pixel_lat)) + 1.0 / cos(radians(pixel_lat))) / pi()) / 2.0 * {side} AS fy
                FROM px),
            cx AS (
                SELECT *, unnest(list_distinct([floor(fx - {_EDGE_EPS})::BIGINT, floor(fx + {_EDGE_EPS})::BIGINT])) AS xc
                FROM f),
            cy AS (
                SELECT *, unnest(list_distinct([floor(fy - {_EDGE_EPS})::BIGINT, floor(fy + {_EDGE_EPS})::BIGINT])) AS yc
                FROM cx)
            SELECT t.tid, {', '.join(agg_cols)}
            FROM cy JOIN tiles t ON t.key = cy.xc * {side} + cy.yc
            WHERE cy.pixel_lon > t.minx AND cy.pixel_lon < t.maxx
              AND cy.pixel_lat > t.miny AND cy.pixel_lat < t.maxy
            GROUP BY t.tid"""
        agg = con.execute(tile_sql).df()
    finally:
        con.close()

    tile_ids = grid.tile_ids[agg['tid'].to_numpy()].astype(str) if len(agg) else np.empty(0, dtype=str)
    for w in windows:
        bits = agg[f'bits{w}'].to_numpy().astype(np.uint64) if len(agg) else np.empty(0, dtype=np.uint64)
        keep = bits != 0
        if not keep.any():
            result.by_window[w] = _empty_bits_frame()
            continue
        order = np.argsort(tile_ids[keep], kind='stable')
        frame = pd.DataFrame({
            'tile_id': pd.array(tile_ids[keep][order], dtype='string'),
            'bits': bits[keep][order],
            'below_min_basin': agg[f'basin{w}'].to_numpy().astype(bool)[keep][order],
        })
        result.by_window[w] = frame
    return result


# =============================================================================
# FILE SELECTION
# =============================================================================

def bymask_param(rp: str) -> str:
    """RIVER_FORECASTS PARAM of a tier's compact per-pixel mask file."""
    return f"extent_bymask_{rp}"


def matching_bymask_row(bymember_row, bymask_row):
    """
    The bymask RIVER_FORECASTS row when it describes the same forecast cycle
    as the bymember row, else None (no pointer, or a stale/different cycle).
    """
    if bymember_row is None or bymask_row is None:
        return None
    try:
        if pd.Timestamp(bymask_row['forecast_time']) != pd.Timestamp(bymember_row['forecast_time']):
            return None
    except Exception:
        return None
    if not bymask_row.get('stage_path'):
        return None
    return bymask_row
