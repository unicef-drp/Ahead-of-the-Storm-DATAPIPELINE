#!/usr/bin/env python3
"""
Sync admin-level impact outputs to the GeoSight related table
"Ahead of the Storm – Admin-level Impacts".

MODES
-----
incremental (default)
    Scans GeoSight for the latest forecast_time already uploaded per
    (country, storm, admin_level).  Only files with a newer forecast time
    are downloaded and uploaded.  Safe to run as a cron job.

--backfill
    Uploads files without any dedup checks.  Use to populate GeoSight from
    scratch or to re-push data after a fix.  Combine with --country /
    --from-date / --to-date / --date to target a specific subset.

--replace
    Deletes existing GeoSight rows that match the active filters, then
    uploads the fresh data from the stage.  Use after a pipeline re-run to
    overwrite stale rows.  Requires --country and/or date filters so you
    don't accidentally wipe unrelated data.

FILTERS (apply to all modes)
-----------------------------
--country ISO3          process only this country (e.g. TWN, PNG)
--storm   NAME          process only this storm (e.g. MELISSA)
--date    YYYY-MM-DD    process only files whose forecast falls on this date
--from-date YYYY-MM-DD  process files with forecast >= this date
--to-date   YYYY-MM-DD  process files with forecast <= this date
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dotenv import load_dotenv

try:
    from geosight.admin_related_table import (
        ADMIN_IMPACT_FILE_RE,
        TABLE_DESCRIPTION,
        TABLE_NAME,
        build_related_table_rows,
        build_row_signature,
        format_forecast_time,
        merge_missing_fields,
    )
    from geosight.client import GeoSightClient
except ImportError:
    from admin_related_table import (
        ADMIN_IMPACT_FILE_RE,
        TABLE_DESCRIPTION,
        TABLE_NAME,
        build_related_table_rows,
        build_row_signature,
        format_forecast_time,
        merge_missing_fields,
    )
    from client import GeoSightClient

ADMIN_VIEWS_PATH = "geodb/aos_views/admin_views"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--backfill", action="store_true", help="Upload without dedup (see module docstring).")
    parser.add_argument("--replace", action="store_true", help="Delete matching GeoSight rows first, then upload fresh data.")
    parser.add_argument("--country", metavar="ISO3", help="Restrict to one country (e.g. TWN).")
    parser.add_argument("--storm", metavar="NAME", help="Restrict to one storm (e.g. MELISSA).")
    parser.add_argument("--date", metavar="YYYY-MM-DD", help="Restrict to files whose forecast date matches exactly.")
    parser.add_argument("--from-date", dest="from_date", metavar="YYYY-MM-DD", help="Restrict to forecast date >= this value.")
    parser.add_argument("--to-date",   dest="to_date",   metavar="YYYY-MM-DD", help="Restrict to forecast date <= this value.")
    return parser.parse_args()


def _get_data_store():
    db = os.getenv("DATA_PIPELINE_DB", "LOCAL")
    if db == "LOCAL":
        return None
    from data_store_utils import get_data_store
    return get_data_store()


def list_admin_impact_filenames(data_store) -> list[str]:
    if data_store is None:
        local_dir = Path(ADMIN_VIEWS_PATH)
        return [p.name for p in sorted(local_dir.glob("*.csv"))] if local_dir.exists() else []
    return [os.path.basename(p) for p in data_store.list_files(ADMIN_VIEWS_PATH) if p.endswith(".csv")]


def _matches_date_filters(forecast_compact: str, date: str | None, from_date: str | None, to_date: str | None) -> bool:
    """forecast_compact is YYYYMMDDHHMMSS; date filters are YYYY-MM-DD."""
    forecast_date = forecast_compact[:8]  # YYYYMMDD
    if date and forecast_date != date.replace("-", ""):
        return False
    if from_date and forecast_date < from_date.replace("-", ""):
        return False
    if to_date and forecast_date > to_date.replace("-", ""):
        return False
    return True


def download_csv(data_store, fname: str, dest_dir: Path) -> None:
    dest = dest_dir / fname
    if data_store is None:
        import shutil
        shutil.copy2(Path(ADMIN_VIEWS_PATH) / fname, dest)
    else:
        raw = data_store.read_file(f"{ADMIN_VIEWS_PATH}/{fname}")
        dest.write_bytes(raw if isinstance(raw, bytes) else raw.encode())


def delete_matching_rows(
    client: GeoSightClient,
    table_id,
    country: str | None,
    storm: str | None,
    date: str | None,
    from_date: str | None,
    to_date: str | None,
) -> int:
    """Fetch all rows from GeoSight, delete those matching the active filters."""
    if not country and not storm and not date and not from_date and not to_date:
        raise ValueError(
            "--replace requires at least one filter (--country, --storm, --date, "
            "--from-date, --to-date) to avoid accidentally wiping all data."
        )

    print("Scanning GeoSight rows to delete (this may take a while for large tables)...")
    to_delete: list[int] = []
    for row in client.iter_related_table_rows(table_id=table_id, page_size=500):
        props = row.get("properties", {})
        row_id = row.get("id")
        if row_id is None:
            continue
        if country and props.get("country_code", "").upper() != country.upper():
            continue
        if storm and props.get("storm", "").upper() != storm.upper():
            continue
        ft = props.get("forecast_time", "")
        # forecast_time in GeoSight is ISO 8601 e.g. "2025-10-22T00:00:00"
        ft_date = ft[:10].replace("-", "") if ft else ""
        if date and ft_date != date.replace("-", ""):
            continue
        if from_date and ft_date < from_date.replace("-", ""):
            continue
        if to_date and ft_date > to_date.replace("-", ""):
            continue
        to_delete.append(row_id)

    print(f"Found {len(to_delete)} row(s) to delete.")
    for i, row_id in enumerate(to_delete, 1):
        client.delete_related_table_row(table_id, row_id)
        if i % 100 == 0:
            print(f"  Deleted {i}/{len(to_delete)}...")
    if to_delete:
        print(f"Deleted {len(to_delete)} row(s).")
    return len(to_delete)


def fetch_latest_forecast_time(client: GeoSightClient, table_id) -> str:
    """
    Scan GeoSight RT once; return the single latest forecast_time across all rows.
    Returns '' if the table is empty. ISO 8601 strings sort lexicographically.
    """
    latest = ""
    for row in client.iter_related_table_rows(table_id=table_id, page_size=500):
        ft = row.get("properties", {}).get("forecast_time", "")
        if ft > latest:
            latest = ft
    return latest


def ensure_related_table(
    client: GeoSightClient,
    desired_fields: list[dict],
    existing_table: dict | None,
) -> dict:
    if existing_table:
        merged = merge_missing_fields(existing_table.get("fields_definition", []), desired_fields)
        if len(merged) != len(existing_table.get("fields_definition", [])):
            existing_table = client.update_related_table(
                table_id=existing_table["id"],
                name=TABLE_NAME,
                description=TABLE_DESCRIPTION,
                fields_definition=merged,
            )
            print(f"Updated schema for '{TABLE_NAME}'.")
        return existing_table
    table = client.create_related_table(
        name=TABLE_NAME,
        description=TABLE_DESCRIPTION,
        fields_definition=desired_fields,
    )
    print(f"Created related table '{TABLE_NAME}' (id={table['id']}).")
    return table


def upload_rows(
    client: GeoSightClient,
    table_id,
    rows: list[dict],
    backfill: bool,
) -> tuple[int, int]:
    existing_ids: set[str] = set()
    if not backfill:
        for row in client.iter_related_table_rows(table_id=table_id, page_size=500):
            props = row.get("properties", {})
            if {"storm", "forecast_time", "wind_threshold", "geom_id"} <= set(props):
                existing_ids.add(build_row_signature(props))

    uploaded = skipped = 0
    for i, row in enumerate(rows, 1):
        row_id = build_row_signature(row)
        if not backfill and row_id in existing_ids:
            skipped += 1
            continue
        client.create_related_table_row(table_id=table_id, properties=row)
        existing_ids.add(row_id)
        uploaded += 1
        if uploaded % 50 == 0:
            print(f"  Uploaded {uploaded} rows...")
        if i % 500 == 0:
            print(f"  Processed {i}/{len(rows)} candidates...")
    return uploaded, skipped


def main() -> None:
    load_dotenv()
    args = parse_args()

    api_key    = os.getenv("GEOSIGHT_API_KEY")
    user_email = os.getenv("GEOSIGHT_USER_EMAIL")
    base_url   = os.getenv("GEOSIGHT_BASE_URL") or "https://geosight.unicef.org"

    if not api_key:
        raise ValueError("GEOSIGHT_API_KEY must be set.")
    if not user_email:
        raise ValueError("GEOSIGHT_USER_EMAIL must be set.")

    data_store = _get_data_store()
    client = GeoSightClient(base_url=base_url, authorization=api_key, user_email=user_email)

    # 1. List all filenames (no content downloaded yet)
    all_filenames = list_admin_impact_filenames(data_store)
    print(f"Found {len(all_filenames)} CSV file(s) in admin_views.")

    # 2. Parse impact files and group by admin level
    by_level: dict[int, list[tuple[str, dict]]] = {}
    for fname in all_filenames:
        m = ADMIN_IMPACT_FILE_RE.match(fname)
        if m:
            level = int(m.group("admin_level"))
            by_level.setdefault(level, []).append((fname, m.groupdict()))

    if not by_level:
        print("No admin impact files found. Nothing to do.")
        return

    # 3. Apply country + storm + date filters
    def _passes_filters(parts: dict) -> bool:
        if args.country and parts["country"] != args.country.upper():
            return False
        if args.storm and parts["storm"].upper() != args.storm.upper():
            return False
        return _matches_date_filters(parts["forecast"], args.date, args.from_date, args.to_date)

    by_level = {
        lvl: [(f, p) for f, p in files if _passes_filters(p)]
        for lvl, files in by_level.items()
    }
    by_level = {lvl: files for lvl, files in by_level.items() if files}

    if not by_level:
        print("No files match the specified filters. Nothing to do.")
        return

    total_files = sum(len(v) for v in by_level.values())
    print(f"{total_files} file(s) match filters across admin level(s): {sorted(by_level)}.")

    # 4. Fetch existing RT (used for both incremental scan and schema update)
    existing_table = client.get_related_table_by_name(TABLE_NAME)

    # 4b. --replace: delete matching rows first, then treat the rest as backfill
    if args.replace:
        if existing_table:
            delete_matching_rows(
                client,
                table_id=existing_table["id"],
                country=args.country,
                storm=args.storm,
                date=args.date,
                from_date=args.from_date,
                to_date=args.to_date,
            )
        else:
            print("Related table does not exist yet — skipping delete step.")
        # Upload all matching files unconditionally (no dedup needed after delete)
        args.backfill = True

    # 5. For incremental mode: scan RT once for the global latest forecast_time
    latest_forecast_time = ""
    if not args.backfill and existing_table:
        print("Scanning GeoSight RT for latest forecast time...")
        latest_forecast_time = fetch_latest_forecast_time(client, existing_table["id"])
        if latest_forecast_time:
            print(f"  Latest forecast time already in GeoSight: {latest_forecast_time}")
        else:
            print("  GeoSight RT is empty — will upload all matching files.")

    # 6. Process each admin level into the single RT
    all_rows: list[dict] = []
    all_desired_fields: list[dict] = []

    for admin_level in sorted(by_level.keys()):
        files = by_level[admin_level]

        if args.backfill:
            selected = files
        else:
            selected = [
                (fname, parts) for fname, parts in files
                if format_forecast_time(parts["forecast"]) > latest_forecast_time
            ]
            skipped_count = len(files) - len(selected)
            print(f"Admin level {admin_level}: {len(selected)} new file(s), {skipped_count} already up to date.")

        if not selected:
            continue

        print(f"Admin level {admin_level}: downloading {len(selected)} file(s)...")

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            for fname, _ in selected:
                download_csv(data_store, fname, tmp_path)

            rows, desired_fields = build_related_table_rows(
                input_dir=tmp_path,
                geom_column="tile_id",
                admin_level=admin_level,
            )

        print(f"Admin level {admin_level}: prepared {len(rows)} row(s).")
        all_rows.extend(rows)
        all_desired_fields = merge_missing_fields(all_desired_fields, desired_fields)

    if not all_rows:
        print("Nothing to upload.")
        return

    # 7. Ensure RT exists with up-to-date schema
    table = ensure_related_table(client, all_desired_fields, existing_table)

    # 8. Upload
    print(f"Uploading {len(all_rows)} row(s) to '{TABLE_NAME}'...")
    uploaded, skipped = upload_rows(client, table["id"], all_rows, backfill=args.backfill)
    print(f"Done: {uploaded} uploaded, {skipped} skipped.")


if __name__ == "__main__":
    main()
