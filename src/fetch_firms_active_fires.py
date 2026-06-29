from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time as time_module
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import geopandas as gpd
import psycopg2
import requests
from psycopg2.extras import Json
from shapely.geometry import Point

try:
    from src.map_copernicus_to_municipalities import (
        DEFAULT_GEOJSON,
        ROOT,
        load_municipalities,
        load_normalized_name_lookup,
        resolve_database_url,
    )
except ModuleNotFoundError:
    from map_copernicus_to_municipalities import (
        DEFAULT_GEOJSON,
        ROOT,
        load_municipalities,
        load_normalized_name_lookup,
        resolve_database_url,
    )


FIRMS_AREA_CSV_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
DEFAULT_SOURCE_PRODUCTS = (
    "VIIRS_NOAA21_NRT",
    "VIIRS_NOAA20_NRT",
    "VIIRS_SNPP_NRT",
    "MODIS_NRT",
)
DEFAULT_AREA = "19,34,30,42"
DEFAULT_DAY_RANGE = 1
DEFAULT_OUTPUT_CSV = ROOT / "data" / "fires" / "firms_active_fire_latest.csv"
GREECE_BBOX = (19.0, 34.0, 30.0, 42.0)
GREECE_TIMEZONE = ZoneInfo("Europe/Athens")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch NASA FIRMS active fire detections, map them to Greek municipalities, and upsert into Postgres.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="FIRMS UTC end date / detection date (YYYY-MM-DD). Defaults to today's UTC date.",
    )
    parser.add_argument(
        "--source-product",
        action="append",
        default=None,
        help=(
            "FIRMS source product. Can be repeated. "
            "Defaults to VIIRS_NOAA21_NRT, VIIRS_NOAA20_NRT, VIIRS_SNPP_NRT, MODIS_NRT."
        ),
    )
    parser.add_argument(
        "--area",
        default=DEFAULT_AREA,
        help="FIRMS area path segment, default: Greece bbox 19,34,30,42.",
    )
    parser.add_argument("--day-range", type=int, default=DEFAULT_DAY_RANGE, help="FIRMS day range, default: 1")
    parser.add_argument("--map-key", default=None, help="FIRMS map key. Defaults to NASA_FIRMS_MAP_KEY from env/.env")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON, help="Municipalities GeoJSON")
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV, help="Local snapshot CSV output")
    parser.add_argument("--db-path", type=str, default=None, help="Optional DATABASE_URL override")
    parser.add_argument(
        "--fetch-retries",
        type=int,
        default=3,
        help="Number of FIRMS HTTP attempts per source product, default: 3",
    )
    parser.add_argument(
        "--fetch-retry-delay",
        type=float,
        default=10.0,
        help="Seconds to wait between FIRMS HTTP retry attempts, default: 10",
    )
    parser.add_argument(
        "--skip-on-fetch-error",
        action="store_true",
        help="Exit successfully without touching snapshot/DB if FIRMS cannot be fetched.",
    )
    parser.add_argument(
        "--exclude-low-confidence",
        action="store_true",
        help="Drop low-confidence detections: confidence=l for VIIRS or numeric confidence <= 30 for MODIS.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch and process, but do not upsert into Postgres")
    parser.add_argument("--quiet", action="store_true", help="Reduce debug output")
    return parser.parse_args()


def log(enabled: bool, message: str) -> None:
    if enabled:
        print(message, flush=True)


def read_env_value(name: str) -> str | None:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip().strip("'\"")

    env_path = ROOT / ".env"
    if not env_path.exists():
        return None

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        if key.strip() == name and raw_value.strip():
            return raw_value.strip().strip("'\"")
    return None


def resolve_map_key(raw_map_key: str | None) -> str:
    value = (raw_map_key or "").strip().strip("'\"")
    if value:
        return value

    env_value = read_env_value("NASA_FIRMS_MAP_KEY")
    if env_value:
        return env_value

    raise ValueError("Missing NASA FIRMS map key. Set NASA_FIRMS_MAP_KEY in .env or pass --map-key.")


def build_firms_url(
    map_key: str,
    source_product: str,
    area: str,
    day_range: int,
    detection_date: str,
) -> str:
    return f"{FIRMS_AREA_CSV_URL}/{map_key}/{source_product}/{area}/{day_range}/{detection_date}"


def mask_firms_url(url: str, map_key: str) -> str:
    return url.replace(f"/{map_key}/", "/***/")


def mask_firms_error(error: Exception, map_key: str) -> str:
    return str(error).replace(map_key, "***")


class FirmsFetchError(RuntimeError):
    pass


def parse_decimal(raw: Any) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_required_decimal(raw: Any, field_name: str) -> float:
    value = parse_decimal(raw)
    if value is None:
        raise ValueError(f"Invalid decimal value for {field_name}: {raw!r}")
    return value


def first_decimal(row: dict[str, str], field_names: tuple[str, ...]) -> float | None:
    for field_name in field_names:
        value = parse_decimal(row.get(field_name))
        if value is not None:
            return value
    return None


def is_low_confidence(confidence: str) -> bool:
    if confidence == "l":
        return True
    numeric_confidence = parse_decimal(confidence)
    return numeric_confidence is not None and numeric_confidence <= 30


def parse_acquisition_time(raw: Any) -> int:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("Missing acq_time")
    value = int(text)
    hours = value // 100
    minutes = value % 100
    if hours > 23 or minutes > 59:
        raise ValueError(f"Invalid acq_time: {raw!r}")
    return value


def build_acquired_at_utc(detection_date: str, acquisition_time: int) -> str:
    hours = acquisition_time // 100
    minutes = acquisition_time % 100
    utc_dt = datetime.combine(date.fromisoformat(detection_date), time(hours, minutes), tzinfo=timezone.utc)
    return utc_dt.isoformat()


def build_acquired_at_el(detection_date: str, acquisition_time: int) -> str:
    hours = acquisition_time // 100
    minutes = acquisition_time % 100
    utc_dt = datetime.combine(date.fromisoformat(detection_date), time(hours, minutes), tzinfo=timezone.utc)
    return utc_dt.astimezone(GREECE_TIMEZONE).replace(tzinfo=None).isoformat()


def fetch_firms_rows(
    map_key: str,
    source_product: str,
    area: str,
    day_range: int,
    detection_date: str,
    verbose: bool = True,
    retries: int = 3,
    retry_delay: float = 10.0,
) -> list[dict[str, str]]:
    url = build_firms_url(
        map_key=map_key,
        source_product=source_product,
        area=area,
        day_range=day_range,
        detection_date=detection_date,
    )
    log(verbose, f"[FIRMS] fetching source_product={source_product} area={area} day_range={day_range} date={detection_date}")
    attempts = max(1, retries)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            break
        except requests.RequestException as exc:
            last_error = exc
            masked_url = mask_firms_url(url, map_key)
            masked_error = mask_firms_error(exc, map_key)
            if attempt >= attempts:
                raise FirmsFetchError(
                    f"FIRMS fetch failed after {attempts} attempt(s) for "
                    f"source_product={source_product} url={masked_url}: {masked_error}"
                ) from exc
            log(
                verbose,
                f"[FIRMS] fetch_retry source_product={source_product} attempt={attempt}/{attempts} "
                f"sleep_seconds={retry_delay} error={masked_error}",
            )
            if retry_delay > 0:
                time_module.sleep(retry_delay)
    else:
        masked_error = mask_firms_error(last_error, map_key) if last_error else "unknown error"
        raise FirmsFetchError(f"FIRMS fetch failed for source_product={source_product}: {masked_error}")

    text = response.text.strip()
    if not text:
        return []

    rows = list(csv.DictReader(text.splitlines()))
    if rows and "latitude" not in rows[0]:
        raise ValueError(f"Unexpected FIRMS CSV columns: {list(rows[0].keys())}")
    return rows


def load_municipality_lookup(db_path: str | None, dry_run: bool) -> dict[str, str]:
    if dry_run:
        return {}
    return load_normalized_name_lookup(db_path)


def find_containing_municipality(point: Point, municipalities: gpd.GeoDataFrame, spatial_index):
    candidate_idx = list(spatial_index.intersection(point.bounds))
    if not candidate_idx:
        return None

    for _, municipality in municipalities.iloc[candidate_idx].iterrows():
        if municipality.geometry.contains(point) or municipality.geometry.touches(point):
            return municipality
    return None


def normalize_firms_row(
    row: dict[str, str],
    source_product: str,
    source_area: str,
    normalized_name_lookup: dict[str, str],
    municipality_row,
) -> dict[str, Any]:
    detection_date = str(row.get("acq_date") or "").strip()
    acquisition_time = parse_acquisition_time(row.get("acq_time"))
    latitude = parse_required_decimal(row.get("latitude"), "latitude")
    longitude = parse_required_decimal(row.get("longitude"), "longitude")
    municipality_key = str(municipality_row["municipality_code"]).strip()
    municipality_name = str(municipality_row["name"]).strip()

    return {
        "source_product": source_product,
        "source_area": source_area,
        "detection_date": detection_date,
        "acquisition_time_utc": acquisition_time,
        "acquired_at": build_acquired_at_utc(detection_date, acquisition_time),
        "acquired_at_el": build_acquired_at_el(detection_date, acquisition_time),
        "latitude": latitude,
        "longitude": longitude,
        "bright_ti4": first_decimal(row, ("bright_ti4", "brightness")),
        "scan": parse_decimal(row.get("scan")),
        "track": parse_decimal(row.get("track")),
        "satellite": str(row.get("satellite") or "").strip(),
        "instrument": str(row.get("instrument") or "").strip(),
        "confidence": str(row.get("confidence") or "").strip(),
        "version": str(row.get("version") or "").strip() or None,
        "bright_ti5": first_decimal(row, ("bright_ti5", "bright_t31")),
        "frp": parse_decimal(row.get("frp")),
        "daynight": str(row.get("daynight") or "").strip(),
        "municipality_key": municipality_key,
        "municipality_normalized_value": normalized_name_lookup.get(municipality_key, municipality_name),
        "municipality_match_method": "point_in_municipality",
        "is_in_greece": True,
        "raw": dict(row),
    }


def process_rows(
    rows: list[dict[str, str]],
    source_product: str,
    source_area: str,
    municipalities: gpd.GeoDataFrame,
    normalized_name_lookup: dict[str, str],
    exclude_low_confidence: bool,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    min_lon, min_lat, max_lon, max_lat = GREECE_BBOX
    spatial_index = municipalities.sindex
    processed: list[dict[str, Any]] = []
    stats = {
        "source_rows": len(rows),
        "bbox_rows": 0,
        "low_confidence_dropped_rows": 0,
        "non_greece_rows": 0,
        "invalid_rows": 0,
        "processed_rows": 0,
    }

    for row in rows:
        try:
            latitude = parse_required_decimal(row.get("latitude"), "latitude")
            longitude = parse_required_decimal(row.get("longitude"), "longitude")
        except ValueError:
            stats["invalid_rows"] += 1
            continue

        if not (min_lat <= latitude <= max_lat and min_lon <= longitude <= max_lon):
            continue
        stats["bbox_rows"] += 1

        confidence = str(row.get("confidence") or "").strip()
        if exclude_low_confidence and is_low_confidence(confidence):
            stats["low_confidence_dropped_rows"] += 1
            continue

        municipality_row = find_containing_municipality(Point(longitude, latitude), municipalities, spatial_index)
        if municipality_row is None:
            stats["non_greece_rows"] += 1
            continue

        try:
            processed.append(
                normalize_firms_row(
                    row=row,
                    source_product=source_product,
                    source_area=source_area,
                    normalized_name_lookup=normalized_name_lookup,
                    municipality_row=municipality_row,
                )
            )
        except ValueError:
            stats["invalid_rows"] += 1

    stats["processed_rows"] = len(processed)
    return processed, stats


def annotate_latest_pass(rows: list[dict[str, Any]]) -> None:
    latest_by_product: dict[str, str] = {}

    for row in rows:
        source_product = str(row["source_product"])
        acquired_at = str(row["acquired_at"])
        if source_product not in latest_by_product or acquired_at > latest_by_product[source_product]:
            latest_by_product[source_product] = acquired_at

    for row in rows:
        latest = latest_by_product[str(row["source_product"])]
        row["is_latest_pass"] = row["acquired_at"] == latest


def merge_stats(stats_items: list[dict[str, int]]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for stats in stats_items:
        for key, value in stats.items():
            merged[key] = merged.get(key, 0) + value
    return merged


def save_snapshot(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "source_product",
        "source_area",
        "detection_date",
        "acquisition_time_utc",
        "acquired_at",
        "acquired_at_el",
        "is_latest_pass",
        "latitude",
        "longitude",
        "bright_ti4",
        "scan",
        "track",
        "satellite",
        "instrument",
        "confidence",
        "version",
        "bright_ti5",
        "frp",
        "daynight",
        "municipality_key",
        "municipality_normalized_value",
        "municipality_match_method",
        "is_in_greece",
        "raw",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            output_row = dict(row)
            output_row["raw"] = json.dumps(output_row["raw"], ensure_ascii=False)
            writer.writerow(output_row)


def update_latest_pass_flags(cur, source_products: list[str]) -> None:
    for source_product in source_products:
        cur.execute(
            """
            WITH latest_pass AS (
              SELECT MAX(acquired_at) AS max_acquired_at
              FROM public.firms_active_fire_detections
              WHERE source_product = %s
            )
            UPDATE public.firms_active_fire_detections AS detection
            SET
              is_latest_pass = detection.acquired_at = latest_pass.max_acquired_at
            FROM latest_pass
            WHERE detection.source_product = %s
            """,
            (source_product, source_product),
        )


def sync_current_fire_coordinates_from_firms(cur) -> int:
    cur.execute("SELECT public.sync_current_fire_coordinates_from_firms()")
    result = cur.fetchone()
    return int(result[0] or 0) if result else 0


def upsert_firms_detections(rows: list[dict[str, Any]], source_products: list[str], db_path: str | None) -> int:
    if not rows:
        db_url = resolve_database_url(db_path)
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        update_latest_pass_flags(cur, source_products)
        sync_current_fire_coordinates_from_firms(cur)
        conn.commit()
        cur.close()
        conn.close()
        return 0

    db_url = resolve_database_url(db_path)
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    upserted = 0
    for row in rows:
        cur.execute(
            """
            INSERT INTO public.firms_active_fire_detections (
              source_product,
              source_area,
              detection_date,
              acquisition_time_utc,
              acquired_at,
              acquired_at_el,
              is_latest_pass,
              latitude,
              longitude,
              bright_ti4,
              scan,
              track,
              satellite,
              instrument,
              confidence,
              version,
              bright_ti5,
              frp,
              daynight,
              municipality_key,
              municipality_normalized_value,
              municipality_match_method,
              is_in_greece,
              raw
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (
              source_product,
              satellite,
              instrument,
              detection_date,
              acquisition_time_utc,
              latitude,
              longitude
            )
            DO UPDATE SET
              source_area = EXCLUDED.source_area,
              acquired_at = EXCLUDED.acquired_at,
              acquired_at_el = EXCLUDED.acquired_at_el,
              is_latest_pass = EXCLUDED.is_latest_pass,
              bright_ti4 = EXCLUDED.bright_ti4,
              scan = EXCLUDED.scan,
              track = EXCLUDED.track,
              confidence = EXCLUDED.confidence,
              version = EXCLUDED.version,
              bright_ti5 = EXCLUDED.bright_ti5,
              frp = EXCLUDED.frp,
              daynight = EXCLUDED.daynight,
              municipality_key = EXCLUDED.municipality_key,
              municipality_normalized_value = EXCLUDED.municipality_normalized_value,
              municipality_match_method = EXCLUDED.municipality_match_method,
              is_in_greece = EXCLUDED.is_in_greece,
              raw = EXCLUDED.raw,
              updated_at = NOW()
            """,
            (
                row["source_product"],
                row["source_area"],
                row["detection_date"],
                row["acquisition_time_utc"],
                row["acquired_at"],
                row["acquired_at_el"],
                row["is_latest_pass"],
                row["latitude"],
                row["longitude"],
                row["bright_ti4"],
                row["scan"],
                row["track"],
                row["satellite"],
                row["instrument"],
                row["confidence"],
                row["version"],
                row["bright_ti5"],
                row["frp"],
                row["daynight"],
                row["municipality_key"],
                row["municipality_normalized_value"],
                row["municipality_match_method"],
                row["is_in_greece"],
                Json(row["raw"]),
            ),
        )
        upserted += 1

    update_latest_pass_flags(cur, source_products)
    synced_current_fires = sync_current_fire_coordinates_from_firms(cur)
    conn.commit()
    cur.close()
    conn.close()
    print(f"[FIRMS] synced_current_fire_coordinates rows={synced_current_fires}")
    return upserted


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    confidence_counts: dict[str, int] = {}
    daynight_counts: dict[str, int] = {}
    municipality_counts: dict[str, int] = {}
    frp_sum = 0.0
    frp_max: float | None = None

    for row in rows:
        confidence = str(row["confidence"])
        daynight = str(row["daynight"])
        municipality = str(row["municipality_normalized_value"])
        confidence_counts[confidence] = confidence_counts.get(confidence, 0) + 1
        daynight_counts[daynight] = daynight_counts.get(daynight, 0) + 1
        municipality_counts[municipality] = municipality_counts.get(municipality, 0) + 1
        if row["frp"] is not None:
            frp = float(row["frp"])
            frp_sum += frp
            frp_max = frp if frp_max is None else max(frp_max, frp)

    return {
        "confidence_counts": confidence_counts,
        "daynight_counts": daynight_counts,
        "municipality_counts": municipality_counts,
        "frp_sum": round(frp_sum, 2),
        "frp_max": frp_max,
    }


def main() -> None:
    args = parse_args()
    verbose = not args.quiet
    detection_date = (
        date.fromisoformat(args.date).isoformat()
        if args.date
        else datetime.now(timezone.utc).date().isoformat()
    )
    map_key = resolve_map_key(args.map_key)

    requested_source_products = args.source_product or list(DEFAULT_SOURCE_PRODUCTS)
    successful_source_products: list[str] = []
    raw_rows_by_product: dict[str, list[dict[str, str]]] = {}
    fetched_rows_by_product: dict[str, int] = {}
    fetch_errors: dict[str, str] = {}

    for source_product in requested_source_products:
        try:
            rows = fetch_firms_rows(
                map_key=map_key,
                source_product=source_product,
                area=args.area,
                day_range=args.day_range,
                detection_date=detection_date,
                verbose=verbose,
                retries=args.fetch_retries,
                retry_delay=args.fetch_retry_delay,
            )
        except FirmsFetchError as exc:
            fetch_errors[source_product] = str(exc)
            if not args.skip_on_fetch_error:
                raise
            log(verbose, f"[FIRMS] fetch_skipped source_product={source_product} error={exc}")
            continue

        successful_source_products.append(source_product)
        raw_rows_by_product[source_product] = rows
        fetched_rows_by_product[source_product] = len(rows)
        log(verbose, f"[FIRMS] fetched_rows source_product={source_product} rows={len(rows)}")

    if fetch_errors and not successful_source_products and args.skip_on_fetch_error:
        print(
            json.dumps(
                {
                    "date": detection_date,
                    "source_products": requested_source_products,
                    "successful_source_products": successful_source_products,
                    "area": args.area,
                    "day_range": args.day_range,
                    "fetched_rows_by_product": fetched_rows_by_product,
                    "processed_rows_by_product": {},
                    "stats": {},
                    "summary": {},
                    "upserted_rows": 0,
                    "dry_run": args.dry_run,
                    "snapshot_csv": str(args.output_csv),
                    "fetch_errors": fetch_errors,
                    "skipped_due_to_fetch_error": True,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    municipalities = load_municipalities(args.geojson)
    normalized_name_lookup = load_municipality_lookup(args.db_path, args.dry_run)
    processed_rows: list[dict[str, Any]] = []
    stats_items: list[dict[str, int]] = []
    processed_rows_by_product: dict[str, int] = {}

    for source_product, rows in raw_rows_by_product.items():
        product_rows, product_stats = process_rows(
            rows=rows,
            source_product=source_product,
            source_area=args.area,
            municipalities=municipalities,
            normalized_name_lookup=normalized_name_lookup,
            exclude_low_confidence=args.exclude_low_confidence,
        )
        processed_rows.extend(product_rows)
        stats_items.append(product_stats)
        processed_rows_by_product[source_product] = len(product_rows)

    annotate_latest_pass(processed_rows)
    stats = merge_stats(stats_items)
    save_snapshot(processed_rows, args.output_csv)
    log(verbose, f"[FIRMS] snapshot_saved={args.output_csv}")

    upserted = 0
    if not args.dry_run:
        upserted = upsert_firms_detections(processed_rows, successful_source_products, args.db_path)
        log(verbose, f"[FIRMS] upserted_rows={upserted}")

    print(json.dumps({
        "date": detection_date,
        "source_products": requested_source_products,
        "successful_source_products": successful_source_products,
        "area": args.area,
        "day_range": args.day_range,
        "fetched_rows_by_product": fetched_rows_by_product,
        "processed_rows_by_product": processed_rows_by_product,
        "stats": stats,
        "summary": summarize(processed_rows),
        "upserted_rows": upserted,
        "dry_run": args.dry_run,
        "snapshot_csv": str(args.output_csv),
        "fetch_errors": fetch_errors,
        "skipped_due_to_fetch_error": False,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except FirmsFetchError as exc:
        print(f"[FIRMS] error={exc}", file=sys.stderr)
        raise SystemExit(1) from exc
