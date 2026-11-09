"""Sparse output for the view files the pipeline writes.

A dense view file has one row per tile or facility of a whole country, and for most
(storm, cycle, threshold[, window]) combinations nearly all of them carry probability 0.
The helpers here return a thinner copy of such a frame for writing; the dense frame the
caller holds is never modified, so admin, CCI and facility merges can keep using it.

The row rules match the Snowflake loader's sparse load, so a thin file and the loader's
own filter agree when both are applied to the same file:

  Tile frames (E_* exposure columns): keep every row whose probability is not exactly 0
  (NaN or missing probability counts as "not 0" and is kept), plus exactly one
  zero-probability marker row when the file has at least one zero row. The marker is the
  first zero row in frame order. Its missing E_* values are filled with the maximum of that
  column over all zero rows, so SUM(E_*) over the file stays 0.0 rather than NULL.

  Facility frames: keep only rows whose probability is not exactly 0. No marker row; the
  result may be empty, with the original columns and dtypes preserved.

Nothing is decided from E_* values. Column order, dtypes, index labels and attrs are kept.
"""
import copy

import numpy as np
import pandas as pd


def _zero_mask(df, prob_col):
    """Boolean ndarray, True where probability is exactly 0. NaN and missing are False."""
    if prob_col not in df.columns:
        raise KeyError(f"probability column {prob_col!r} not found")
    prob = df[prob_col]
    if not pd.api.types.is_numeric_dtype(prob):
        prob = pd.to_numeric(prob, errors='coerce')
    return (prob == 0).fillna(False).to_numpy(dtype=bool)


def _extra_keep(df, keep_mask):
    """keep_mask as a positional boolean ndarray (missing values count as False)."""
    if keep_mask is None:
        return np.zeros(len(df), dtype=bool)
    if isinstance(keep_mask, pd.Series):
        if len(keep_mask) != len(df):
            raise ValueError("keep_mask length does not match the frame")
        if not keep_mask.index.equals(df.index):
            if not df.index.is_unique:
                raise ValueError("keep_mask index differs from a non-unique frame index")
            keep_mask = keep_mask.reindex(df.index)
        arr = keep_mask.to_numpy()
    else:
        arr = np.asarray(keep_mask)
        if len(arr) != len(df):
            raise ValueError("keep_mask length does not match the frame")
    return pd.Series(arr, dtype=object).map(lambda v: False if pd.isna(v) else bool(v)).to_numpy(dtype=bool)


def _finish(df, keep):
    """Copy of the kept rows (original order and index labels) with attrs carried over."""
    out = df.iloc[np.flatnonzero(keep)].copy()
    out.attrs = copy.deepcopy(df.attrs)
    return out


def sparsify_facility_frame(df, prob_col='probability', keep_mask=None):
    """Facility frame without its zero-probability rows; may be empty.

    keep_mask (boolean Series aligned to df, or array) only ADDS rows to the kept set.
    """
    zero = _zero_mask(df, prob_col)
    keep = ~zero | _extra_keep(df, keep_mask)
    return _finish(df, keep)


def sparsify_tile_frame(df, prob_col='probability', keep_mask=None):
    """Tile frame reduced to nonzero rows plus one zero-probability marker row.

    keep_mask (boolean Series aligned to df, or array) only ADDS rows to the kept set;
    it never removes the nonzero rows or the marker. A frame with no zero rows gets no
    marker. The marker's missing E_* values come from the zero rows' column maximum.
    """
    zero = _zero_mask(df, prob_col)
    keep = ~zero | _extra_keep(df, keep_mask)
    zero_pos = np.flatnonzero(zero)
    if len(zero_pos) == 0:
        return _finish(df, keep)

    marker = zero_pos[0]
    keep[marker] = True
    out = _finish(df, keep)
    marker_row = int(keep[:marker].sum())  # position of the marker inside out

    for i, name in enumerate(df.columns):
        if not str(name).upper().startswith('E_'):
            continue
        col = df.iloc[:, i]
        if not pd.api.types.is_numeric_dtype(col) or pd.api.types.is_bool_dtype(col):
            continue
        if not pd.isna(col.iloc[marker]):
            continue
        fill = col.iloc[zero_pos].max()  # NaN-skipping; NaN when every zero row is missing
        if pd.isna(fill):
            continue
        new_col = out.iloc[:, i].copy()
        new_col.iloc[marker_row] = fill
        out.isetitem(i, new_col)
    return out


def describe_reduction(before_df, after_df) -> str:
    """One-line row count summary for log output."""
    before, after = len(before_df), len(after_df)
    pct = 100.0 * (before - after) / before if before else 0.0
    return f"{before:,} -> {after:,} rows ({pct:.1f}% fewer)"
