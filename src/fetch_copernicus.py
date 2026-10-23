from __future__ import annotations

import argparse
import ast
import csv
import gzip
import json
import math
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg2
import requests
from psycopg2.extras import Json

try:
    from src.map_copernicus_to_municipalities import (
        DEFAULT_GEOJSON,
        ROOT,
        assign_municipalities,
        load_municipalities,
        load_normalized_name_lookup,
        parse_centroid,
        parse_shape,
        resolve_database_url,
    )
except ModuleNotFoundError:
    from map_copernicus_to_municipalities import (
        DEFAULT_GEOJSON,
        ROOT,
        assign_municipalities,
        load_municipalities,
        load_normalized_name_lookup,
        parse_centroid,
        parse_shape,
        resolve_database_url,
    )

API_URL = "https://api.effis.emergency.copernicus.eu/rest/2/burntareas/current/"
DEFAULT_OUTPUT_CSV = ROOT / "data" / "fires" / "copernicus_latest.csv"
DEFAULT_BACKUP_DIR = ROOT / "backupDB" / "copernicus"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile a complete Copernicus burnt-area snapshot with Postgres.",
    )
    parser.add_argument("--start-date", default="2024-01-01", help="Inclusive firedate lower bound (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="Inclusive firedate upper bound (YYYY-MM-DD)")
    parser.add_argument("--country", default="EL", help="Country code filter for Copernicus API")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON, help="Municipalities GeoJSON")
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV, help="Local snapshot CSV output")
    parser.add_argument("--db-path", type=str, default=None, help="Optional DATABASE_URL override")
    parser.add_argument("--full-refresh", action="store_true", help="Compatibility option: every run now fetches the full window")
    parser.add_argument("--lookback-days", type=int, default=7, help="Deprecated compatibility option; timestamp-only updates are no longer used")
    parser.add_argument("--backup-dir", type=Path, default=DEFAULT_BACKUP_DIR, help="Directory for verified pre-sync database backups")
    parser.add_argument("--quiet", action="store_true", help="Reduce debug output")
    return parser.parse_args()


def log(enabled: bool, message: str) -> None:
    if enabled:
        print(message, flush=True)


def resolve_fetch_window(
    start_date: str,
    end_date: str,
    db_path: str | None,
    full_refresh: bool,
    lookback_days: int,
):
    # EFFIS can replace IDs or revise geometry/area without advancing lastupdate.
    # Only a complete snapshot can reconcile both kinds of change reliably.
    snapshot_bounds(start_date, end_date)
    return {
        "mode": "full_refresh",
        "firedate_gte": start_date,
        "firedate_lte": end_date,
        "lastupdate_gte": None,
        "latest_lastupdate_in_db": None,
    }


def snapshot_bounds(start_date: str, end_date: str) -> tuple[datetime, datetime]:
    start = datetime.combine(date.fromisoformat(start_date), datetime.min.time(), timezone.utc)
    end = datetime.combine(date.fromisoformat(end_date) + timedelta(days=1), datetime.min.time(), timezone.utc)
    if start >= end:
        raise ValueError("Copernicus start date must not be after end date")
    return start, end


def snapshot_page_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    expected = urlparse(API_URL)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.netloc != expected.netloc
        or parsed.path != expected.path
        or parsed.fragment
    ):
        raise ValueError("Unexpected Copernicus pagination URL")
    # The service sometimes publishes an http next link; never downgrade HTTPS.
    return urlunparse(parsed._replace(scheme="https"))


def validate_snapshot_rows(rows: list[dict], start_date: str, end_date: str, country: str) -> None:
    start, end = snapshot_bounds(start_date, end_date)
    if not rows:
        raise ValueError("Empty Copernicus snapshot; refusing to change existing data")
    seen: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Invalid Copernicus record")
        source_id = row.get("id")
        if isinstance(source_id, bool) or not isinstance(source_id, int) or source_id <= 0:
            raise ValueError("Invalid Copernicus source ID")
        if source_id in seen:
            raise ValueError(f"Duplicate Copernicus source ID: {source_id}")
        seen.add(source_id)
        if row.get("country") != country:
            raise ValueError(f"Copernicus country mismatch for ID {source_id}")
        try:
            firedate = datetime.fromisoformat(str(row["firedate"]).replace("Z", "+00:00"))
            if firedate.tzinfo is None or not start <= firedate < end:
                raise ValueError("outside requested UTC window")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid Copernicus firedate for ID {source_id}") from exc
        if "area_ha" not in row:
            raise ValueError(f"Missing Copernicus area for ID {source_id}")
        if row["area_ha"] is not None:
            try:
                area = Decimal(str(row["area_ha"]))
            except InvalidOperation as exc:
                raise ValueError(f"Invalid Copernicus area for ID {source_id}") from exc
            if not area.is_finite() or area < 0:
                raise ValueError(f"Invalid Copernicus area for ID {source_id}")
        geometry = parse_shape(row.get("shape"))
        if geometry is not None and geometry.geom_type not in {"Polygon", "MultiPolygon"}:
            raise ValueError(f"Invalid Copernicus perimeter type for ID {source_id}")
        if geometry is None:
            geometry = parse_centroid(row.get("centroid"))
        if geometry is None:
            raise ValueError(f"Missing Copernicus geometry for ID {source_id}")
        if geometry.is_empty or not geometry.is_valid or not all(math.isfinite(value) for value in geometry.bounds):
            raise ValueError(f"Invalid Copernicus geometry for ID {source_id}")


def fetch_copernicus_rows(
    start_date: str,
    end_date: str,
    country: str = "EL",
    verbose: bool = True,
) -> list[dict]:
    start, end = snapshot_bounds(start_date, end_date)
    params = {
        "country": country,
        "firedate__gte": start.isoformat(),
        "firedate__lte": (end - timedelta(microseconds=1)).isoformat(),
        "ordering": "-id",
        "limit": 100,
    }
    headers = {
        "Accept": "application/json",
        "Origin": "https://forest-fire.emergency.copernicus.eu",
        "Referer": "https://forest-fire.emergency.copernicus.eu/",
        "User-Agent": "Mozilla/5.0",
    }

    rows: list[dict] = []
    next_url: str | None = API_URL
    visited: set[str] = set()
    total: int | None = None
    page = 0

    while next_url:
        next_url = snapshot_page_url(next_url)
        if next_url in visited:
            raise ValueError("Repeated Copernicus pagination URL")
        visited.add(next_url)
        page += 1
        log(verbose, f"[COPERNICUS] fetching page={page} url={next_url}")
        response = requests.get(
            next_url,
            params=params if next_url == API_URL else None,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Invalid Copernicus response")
        count = payload.get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("Missing or invalid Copernicus response count")
        if total is not None and count != total:
            raise ValueError("Copernicus count changed during pagination; retry a fresh snapshot")
        total = count
        page_rows = payload.get("results")
        if not isinstance(page_rows, list):
            raise ValueError("Invalid Copernicus results")
        rows.extend(page_rows)
        log(verbose, f"[COPERNICUS] page={page} rows={len(page_rows)} total_rows={len(rows)}")
        if "next" not in payload or (payload["next"] is not None and not isinstance(payload["next"], str)):
            raise ValueError("Missing or invalid Copernicus next page")
        next_url = payload["next"]
        if next_url is not None and (not next_url or not page_rows or len(rows) >= total):
            raise ValueError("Invalid Copernicus pagination progress")

    if len(rows) != total:
        raise ValueError(f"Incomplete Copernicus snapshot: got {len(rows)}, expected {total}")
    validate_snapshot_rows(rows, start_date, end_date, country)
    return rows


def filter_rows_by_firedate(rows: list[dict], start_date: str, end_date: str) -> tuple[list[dict], int]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    kept: list[dict] = []
    dropped = 0

    for row in rows:
        raw_firedate = str(row.get("firedate") or "").strip()
        if not raw_firedate:
            dropped += 1
            continue
        try:
            fire_dt = datetime.fromisoformat(raw_firedate.replace("Z", "+00:00"))
        except ValueError:
            dropped += 1
            continue
        fire_day = fire_dt.date()
        if fire_day < start or fire_day > end:
            dropped += 1
            continue
        kept.append(row)

    return kept, dropped


def rows_to_geodataframe(rows: list[dict]):
    import geopandas as gpd

    geometry = []
    methods = []
    normalized_rows = []
    for row in rows:
        fire_shape = parse_shape(row.get("shape"))
        fire_centroid = parse_centroid(row.get("centroid"))
        if fire_shape is not None:
            geometry.append(fire_shape)
            methods.append("shape")
        else:
            geometry.append(fire_centroid)
            methods.append("centroid")
        normalized_rows.append(dict(row))

    gdf = gpd.GeoDataFrame(normalized_rows, geometry=geometry, crs="EPSG:4326")
    gdf["match_geometry"] = methods
    return gdf[gdf.geometry.notnull()].copy()


def _parse_bbox(raw):
    if raw is None:
        return None
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            values = json.loads(text)
        except json.JSONDecodeError:
            values = [float(part.strip()) for part in text.strip("[]").split(",") if part.strip()]
    elif isinstance(raw, (list, tuple)):
        values = list(raw)
    else:
        return None

    if len(values) != 4:
        return None
    try:
        return [float(v) for v in values]
    except (TypeError, ValueError):
        return None


def _parse_json_value(raw):
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    text = str(raw).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(text)
        except Exception:
            return None


def _parse_timestamp(raw):
    text = str(raw or "").strip()
    if not text:
        return None
    return text


def _parse_decimal(raw):
    text = "" if raw is None else str(raw).strip()
    if not text:
        return None
    try:
        value = float(text)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def _parse_bool(raw):
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def save_snapshot(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _upsert_copernicus_rows(cur, enriched) -> int:
    upserted = 0
    for _, row in enriched.iterrows():
        copernicus_id = int(row["id"])
        cur.execute(
            """
            INSERT INTO public.copernicus (
              copernicus_id,
              centroid,
              bbox,
              shape,
              country,
              countryful,
              province,
              commune,
              firedate,
              area_ha,
              broadlea,
              conifer,
              mixed,
              scleroph,
              transit,
              othernatlc,
              agriareas,
              artifsurf,
              otherlc,
              percna2k,
              lastupdate,
              lastfiredate,
              noneu,
              municipality_key,
              municipality_normalized_value,
              municipality_match_method,
              municipality_overlap_ratio
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (copernicus_id)
            DO UPDATE SET
              centroid = EXCLUDED.centroid,
              bbox = EXCLUDED.bbox,
              shape = EXCLUDED.shape,
              country = EXCLUDED.country,
              countryful = EXCLUDED.countryful,
              province = EXCLUDED.province,
              commune = EXCLUDED.commune,
              firedate = EXCLUDED.firedate,
              area_ha = EXCLUDED.area_ha,
              broadlea = EXCLUDED.broadlea,
              conifer = EXCLUDED.conifer,
              mixed = EXCLUDED.mixed,
              scleroph = EXCLUDED.scleroph,
              transit = EXCLUDED.transit,
              othernatlc = EXCLUDED.othernatlc,
              agriareas = EXCLUDED.agriareas,
              artifsurf = EXCLUDED.artifsurf,
              otherlc = EXCLUDED.otherlc,
              percna2k = EXCLUDED.percna2k,
              lastupdate = EXCLUDED.lastupdate,
              lastfiredate = EXCLUDED.lastfiredate,
              noneu = EXCLUDED.noneu,
              municipality_key = EXCLUDED.municipality_key,
              municipality_normalized_value = EXCLUDED.municipality_normalized_value,
              municipality_match_method = EXCLUDED.municipality_match_method,
              municipality_overlap_ratio = EXCLUDED.municipality_overlap_ratio,
              updated_at = NOW()
            """,
            (
                copernicus_id,
                Json(_parse_json_value(row.get("centroid"))) if row.get("centroid") else None,
                _parse_bbox(row.get("bbox")),
                Json(_parse_json_value(row.get("shape"))) if row.get("shape") else None,
                row.get("country"),
                row.get("countryful"),
                row.get("province"),
                row.get("commune"),
                _parse_timestamp(row.get("firedate")),
                _parse_decimal(row.get("area_ha")),
                _parse_decimal(row.get("broadlea")),
                _parse_decimal(row.get("conifer")),
                _parse_decimal(row.get("mixed")),
                _parse_decimal(row.get("scleroph")),
                _parse_decimal(row.get("transit")),
                _parse_decimal(row.get("othernatlc")),
                _parse_decimal(row.get("agriareas")),
                _parse_decimal(row.get("artifsurf")),
                _parse_decimal(row.get("otherlc")),
                _parse_decimal(row.get("percna2k")),
                _parse_timestamp(row.get("lastupdate")),
                _parse_timestamp(row.get("lastfiredate")),
                _parse_bool(row.get("noneu")),
                row.get("municipality_key"),
                row.get("municipality_normalized_value"),
                row.get("municipality_match_method"),
                _parse_decimal(row.get("municipality_overlap_ratio")),
            ),
        )
        cur.execute(
            "DELETE FROM public.copernicus_municipality WHERE copernicus_id = %s",
            (copernicus_id,),
        )
        municipality_matches = row.get("municipality_matches")
        if isinstance(municipality_matches, list):
            cur.executemany(
                """
                INSERT INTO public.copernicus_municipality (
                  copernicus_id,
                  municipality_key,
                  match_method,
                  overlap_ratio,
                  overlap_area_ha
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (copernicus_id, municipality_key)
                DO UPDATE SET
                  match_method = EXCLUDED.match_method,
                  overlap_ratio = EXCLUDED.overlap_ratio,
                  overlap_area_ha = EXCLUDED.overlap_area_ha,
                  updated_at = NOW()
                """,
                [
                    (
                        copernicus_id,
                        match["municipality_key"],
                        match["match_method"],
                        match["overlap_ratio"],
                        match["overlap_area_ha"],
                    )
                    for match in municipality_matches
                ],
            )
        upserted += 1

    return upserted


def validate_enriched_snapshot(enriched, source_ids: set[int]) -> None:
    enriched_ids = [int(value) for value in enriched["id"]]
    if len(enriched_ids) != len(source_ids) or set(enriched_ids) != source_ids:
        raise ValueError("Copernicus geometry/mapping processing changed snapshot IDs; refusing reconciliation")


def backup_copernicus(cur, backup_dir: Path, metadata: dict) -> Path:
    """Persist and verify complete before-images of both affected tables."""
    backup: dict = {"metadata": metadata, "tables": {}}
    for table in ("copernicus", "copernicus_municipality"):
        # Names are fixed here, never supplied by a user or an API response.
        cur.execute(f"SELECT * FROM public.{table} ORDER BY 1")
        columns = [column[0] for column in cur.description]
        backup["tables"][table] = [dict(zip(columns, row)) for row in cur.fetchall()]
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = backup_dir / f"copernicus-{stamp}-before.json.gz"
    temporary = path.with_suffix(".gz.tmp")
    try:
        with gzip.open(temporary, "xt", encoding="utf-8") as stream:
            json.dump(backup, stream, ensure_ascii=False, default=str)
        with gzip.open(temporary, "rt", encoding="utf-8") as stream:
            verified = json.load(stream)
        if verified != json.loads(json.dumps(backup, ensure_ascii=False, default=str)):
            raise RuntimeError("Copernicus backup content verification failed")
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return path


def upsert_copernicus(
    enriched,
    db_path: str | None,
    *,
    start_date: str,
    end_date: str,
    country: str = "EL",
    backup_dir: Path = DEFAULT_BACKUP_DIR,
) -> dict:
    records = [
        {key: None if isinstance(value, float) and math.isnan(value) else value for key, value in row.items()}
        for row in enriched.to_dict("records")
    ]
    validate_snapshot_rows(records, start_date, end_date, country)
    source_ids = {int(row["id"]) for row in records}
    validate_enriched_snapshot(enriched, source_ids)
    start, end = snapshot_bounds(start_date, end_date)
    expected_area = sum((Decimal(str(row["area_ha"] or 0)) for row in records), Decimal(0))
    conn = psycopg2.connect(resolve_database_url(db_path))
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '15s'")
            cur.execute("SET LOCAL statement_timeout = '180s'")
            # Readers continue to work; writers cannot change either table between
            # the backup, updates, scoped deletion, and verification.
            cur.execute("LOCK TABLE public.copernicus, public.copernicus_municipality IN SHARE ROW EXCLUSIVE MODE")
            scope = (country, start, end, sorted(source_ids))
            cur.execute(
                """
                SELECT copernicus_id FROM public.copernicus
                WHERE country = %s AND firedate >= %s AND firedate < %s
                  AND NOT (copernicus_id = ANY(%s))
                ORDER BY copernicus_id
                """,
                scope,
            )
            stale_ids = [row[0] for row in cur.fetchall()]
            print(json.dumps({
                "event": "copernicus_reconciliation_plan",
                "country": country,
                "start_date": start_date,
                "end_date": end_date,
                "source_rows": len(source_ids),
                "source_area_ha": str(expected_area),
                "removed_ids": stale_ids,
            }), flush=True)
            metadata = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "source": API_URL,
                "country": country,
                "start_date": start_date,
                "end_date": end_date,
                "source_ids": sorted(source_ids),
                "source_area_ha": str(expected_area),
                "removed_ids": stale_ids,
            }
            backup_path = backup_copernicus(cur, backup_dir, metadata)
            upserted = _upsert_copernicus_rows(cur, enriched)
            cur.execute(
                """
                DELETE FROM public.copernicus
                WHERE country = %s AND firedate >= %s AND firedate < %s
                  AND NOT (copernicus_id = ANY(%s))
                RETURNING copernicus_id
                """,
                scope,
            )
            removed_ids = sorted(row[0] for row in cur.fetchall())
            if removed_ids != stale_ids:
                raise RuntimeError("Copernicus removed IDs differ from backed-up plan")
            cur.execute(
                """
                SELECT copernicus_id, area_ha FROM public.copernicus
                WHERE country = %s AND firedate >= %s AND firedate < %s
                ORDER BY copernicus_id
                """,
                (country, start, end),
            )
            stored = cur.fetchall()
            stored_area = sum((row[1] or Decimal(0) for row in stored), Decimal(0))
            if [row[0] for row in stored] != sorted(source_ids) or stored_area != expected_area:
                raise RuntimeError("Copernicus post-sync IDs or area differ from complete source snapshot")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "upserted_rows": upserted,
        "removed_rows": len(removed_ids),
        "removed_ids": removed_ids,
        "area_ha": str(stored_area),
        "backup_path": str(backup_path),
    }


def main() -> None:
    args = parse_args()
    verbose = not args.quiet
    window = resolve_fetch_window(
        start_date=args.start_date,
        end_date=args.end_date,
        db_path=args.db_path,
        full_refresh=args.full_refresh,
        lookback_days=args.lookback_days,
    )
    log(verbose, f"[COPERNICUS] fetch_mode={window['mode']}")
    log(verbose, f"[COPERNICUS] firedate_gte={window['firedate_gte']} firedate_lte={window['firedate_lte']}")
    if window["latest_lastupdate_in_db"]:
        log(verbose, f"[COPERNICUS] latest_lastupdate_in_db={window['latest_lastupdate_in_db']}")
    if window["lastupdate_gte"]:
        log(verbose, f"[COPERNICUS] lastupdate_gte={window['lastupdate_gte']}")

    rows = fetch_copernicus_rows(
        window["firedate_gte"],
        window["firedate_lte"],
        args.country,
        verbose=verbose,
    )
    log(verbose, f"[COPERNICUS] fetched_rows={len(rows)}")
    validate_snapshot_rows(rows, args.start_date, args.end_date, args.country)
    source_ids = {int(row["id"]) for row in rows}

    fires = rows_to_geodataframe(rows)
    validate_enriched_snapshot(fires, source_ids)
    log(verbose, f"[COPERNICUS] rows_with_geometry={len(fires)}")
    municipalities = load_municipalities(args.geojson)
    normalized_name_lookup = load_normalized_name_lookup(args.db_path)
    enriched = assign_municipalities(fires, municipalities, normalized_name_lookup)
    validate_enriched_snapshot(enriched, source_ids)
    matched = int(enriched["municipality_key"].notna().sum())
    log(verbose, f"[COPERNICUS] municipality_matched={matched}/{len(enriched)}")
    synced = upsert_copernicus(
        enriched, args.db_path,
        start_date=args.start_date, end_date=args.end_date, country=args.country,
        backup_dir=args.backup_dir,
    )
    log(verbose, f"[COPERNICUS] upserted_rows={synced['upserted_rows']} removed_rows={synced['removed_rows']}")
    save_snapshot(rows, args.output_csv)
    log(verbose, f"[COPERNICUS] snapshot_saved={args.output_csv}")

    total = len(enriched)
    print(json.dumps({
        "window": {
            "mode": window["mode"],
            "start_date": window["firedate_gte"],
            "end_date": window["firedate_lte"],
            "lastupdate_gte": window["lastupdate_gte"],
        },
        "fetched_rows": len(rows),
        "rows_with_geometry": total,
        "matched_rows": matched,
        "unmatched_rows": total - matched,
        **synced,
        "snapshot_csv": str(args.output_csv),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
