#!/usr/bin/env python3
"""
Helpers for converting pipeline admin outputs into GeoSight related-table rows.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


ADMIN_IMPACT_FILE_RE = re.compile(
    r"^(?P<country>[A-Z0-9]{3})_(?P<storm>.+)_(?P<forecast>\d{14})_(?P<wind>\d+)_admin(?P<admin_level>[1-5])\.csv$"
)

TABLE_NAME = "Ahead of the Storm – Admin-level Impacts"
TABLE_DESCRIPTION = (
    "Administrative-area impact outputs for Ahead of the Storm. "
    "Each row represents forecast-based impacts for one area, forecast time, and wind threshold."
)

BASE_FIELDS = [
    {"name": "country_code",   "label": "Country",                  "type": "string"},
    {"name": "storm",          "label": "Storm Name",               "type": "string"},
    {"name": "admin_level",    "label": "Admin Level",              "type": "number"},
    {"name": "forecast_time",  "label": "Forecast Time",            "type": "date"},
    {"name": "wind_threshold", "label": "Wind Threshold (knots)",   "type": "number"},
    {"name": "geom_id",        "label": "Admin Region ID",          "type": "string"},
]

METRIC_LABELS: dict[str, str] = {
    "E_population":              "Expected Affected Population",
    "E_school_age_population":   "Expected Affected School-Age Population (5–14)",
    "E_infant_population":       "Expected Affected Infant Population (0–4)",
    "E_adolescent_population":   "Expected Affected Adolescent Population (15–19)",
    "E_built_surface_m2":        "Expected Affected Built Surface (m²)",
    "E_num_schools":             "Expected Affected Schools",
    "E_num_hcs":                 "Expected Affected Health Centers",
    "E_num_shelters":            "Expected Affected Shelters",
    "E_num_wash":                "Expected Affected WASH Facilities",
    "probability":               "Probability of Wind Exposure",
}

ALLOWED_METRIC_COLUMNS = set(METRIC_LABELS.keys())


def _resolve_geom_column(df: pd.DataFrame, requested_column: str) -> str:
    if requested_column in df.columns:
        return requested_column
    for fallback in ("tile_id", "zone_id"):
        if fallback in df.columns:
            return fallback
    raise ValueError(
        f"Input data is missing geometry identifier column '{requested_column}' "
        f"and fallback columns ['tile_id', 'zone_id']."
    )


def build_row_signature(properties: dict[str, Any]) -> str:
    return "|".join([
        str(properties["storm"]),
        str(properties["forecast_time"]),
        str(properties["wind_threshold"]),
        str(properties["geom_id"]),
    ])


def format_forecast_time(compact_value: str) -> str:
    ts = pd.to_datetime(compact_value, format="%Y%m%d%H%M%S", utc=True)
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def _json_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize("UTC")
        else:
            value = value.tz_convert("UTC")
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else float(value)
    return str(value)


def _field_type_for_series(series: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(series):
        return "date"
    if pd.api.types.is_numeric_dtype(series):
        return "number"
    return "string"


def _parse_admin_impact_name(file_name: str) -> dict[str, str] | None:
    m = ADMIN_IMPACT_FILE_RE.match(file_name)
    return m.groupdict() if m else None



def discover_admin_impact_files(
    input_dir: Path, admin_level: int | None = None
) -> list[Path]:
    pattern = f"*_admin{admin_level}.csv" if admin_level else "*_admin[1-5].csv"
    return [p for p in sorted(input_dir.glob(pattern)) if _parse_admin_impact_name(p.name)]



def build_related_table_rows(
    input_dir: Path,
    geom_column: str = "tile_id",
    admin_level: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    impact_files = discover_admin_impact_files(input_dir, admin_level=admin_level)
    if not impact_files:
        raise FileNotFoundError(
            f"No admin impact CSV files found in {input_dir}"
            + (f" for admin level {admin_level}" if admin_level else "")
        )

    rows: list[dict[str, Any]] = []
    metric_field_types: dict[str, str] = {}

    for path in impact_files:
        parts = _parse_admin_impact_name(path.name)
        if not parts:
            continue

        country = parts["country"]
        storm = parts["storm"]
        forecast = parts["forecast"]
        wind_threshold = int(parts["wind"])
        level = int(parts["admin_level"])

        df = pd.read_csv(path)
        unnamed = [c for c in df.columns if str(c).startswith("Unnamed:")]
        if unnamed:
            df = df.drop(columns=unnamed)

        effective_geom_col = _resolve_geom_column(df, geom_column)

        metric_columns = [c for c in df.columns if c in ALLOWED_METRIC_COLUMNS]
        for col in metric_columns:
            metric_field_types.setdefault(col, _field_type_for_series(df[col]))

        forecast_time = format_forecast_time(forecast)

        for _, row in df.iterrows():
            properties: dict[str, Any] = {
                "country_code":   country,
                "storm":          storm,
                "admin_level":    level,
                "forecast_time":  forecast_time,
                "wind_threshold": wind_threshold,
                "geom_id":        str(row[effective_geom_col]),
            }
            for col in metric_columns:
                v = _json_scalar(row[col])
                if v is not None:
                    properties[col] = v
            rows.append(properties)

    dynamic_fields = [
        {"name": n, "label": METRIC_LABELS.get(n, n.replace("_", " ").title()), "type": t}
        for n, t in sorted(metric_field_types.items())
    ]
    return rows, BASE_FIELDS + dynamic_fields


def merge_missing_fields(
    existing_fields: list[dict[str, str]],
    desired_fields: list[dict[str, str]],
) -> list[dict[str, str]]:
    merged = {f["name"]: f for f in existing_fields}
    for f in desired_fields:
        merged.setdefault(f["name"], f)
    return list(merged.values())
