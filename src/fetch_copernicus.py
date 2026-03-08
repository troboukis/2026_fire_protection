from __future__ import annotations

import argparse
import ast
import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import requests
from psycopg2.extras import Json

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Copernicus burnt areas for Greece, map to municipalities, and upsert into Postgres.",
    )
    parser.add_argument("--start-date", default="2024-01-01", help="Inclusive firedate lower bound (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="Inclusive firedate upper bound (YYYY-MM-DD)")
    parser.add_argument("--country", default="EL", help="Country code filter for Copernicus API")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON, help="Municipalities GeoJSON")
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV, help="Local snapshot CSV output")
    parser.add_argument("--db-path", type=str, default=None, help="Optional DATABASE_URL override")
    parser.add_argument("--full-refresh", action="store_true", help="Ignore DB state and fetch the full window")
    parser.add_argument("--lookback-days", type=int, default=7, help="Overlap window for incremental updates")
    parser.add_argument("--quiet", action="store_true", help="Reduce debug output")
    return parser.parse_args()


def log(enabled: bool, message: str) -> None:
    if enabled:
        print(message, flush=True)


def get_latest_lastupdate(db_path: str | None):
    db_url = resolve_database_url(db_path)
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("SELECT MAX(lastupdate) FROM public.copernicus")
    value = cur.fetchone()[0]
    cur.close()
    conn.close()
    return value


def resolve_fetch_window(
    start_date: str,
    end_date: str,
    db_path: str | None,
    full_refresh: bool,
    lookback_days: int,
):
    if full_refresh:
        return {
            "mode": "full_refresh",
            "firedate_gte": start_date,
            "firedate_lte": end_date,
            "lastupdate_gte": None,
            "latest_lastupdate_in_db": None,
        }

    latest_lastupdate = get_latest_lastupdate(db_path)
    if latest_lastupdate is None:
        return {
            "mode": "bootstrap",
            "firedate_gte": start_date,
            "firedate_lte": end_date,
            "lastupdate_gte": None,
            "latest_lastupdate_in_db": None,
        }

    if latest_lastupdate.tzinfo is None:
        latest_lastupdate = latest_lastupdate.replace(tzinfo=timezone.utc)

    overlap_start = latest_lastupdate - timedelta(days=max(0, lookback_days))
    overlap_floor = datetime.fromisoformat(f"{start_date}T00:00:00+00:00")
    effective_start = max(overlap_start, overlap_floor)
    return {
        "mode": "incremental",
        "firedate_gte": start_date,
        "firedate_lte": end_date,
        "lastupdate_gte": effective_start.isoformat(),
        "latest_lastupdate_in_db": latest_lastupdate.isoformat(),
    }


def fetch_copernicus_rows(
    start_date: str,
    end_date: str,
    country: str = "EL",
    lastupdate_gte: str | None = None,
    verbose: bool = True,
) -> list[dict]:
    params = {
        "country": country,
        "firedate__gte": f"{start_date}T00:00:00",
        "firedate__lte": f"{end_date}T23:59:59",
        "ordering": "-lastupdate,-area_ha",
        "limit": 100,
    }
    if lastupdate_gte:
        params["lastupdate__gte"] = lastupdate_gte
    headers = {
        "Accept": "application/json",
        "Origin": "https://forest-fire.emergency.copernicus.eu",
        "Referer": "https://forest-fire.emergency.copernicus.eu/",
        "User-Agent": "Mozilla/5.0",
    }

    rows: list[dict] = []
    next_url: str | None = API_URL
    page = 0

    while next_url:
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
        page_rows = payload.get("results", [])
        rows.extend(page_rows)
        log(verbose, f"[COPERNICUS] page={page} rows={len(page_rows)} total_rows={len(rows)}")
        next_url = payload.get("next")

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
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return float(text)
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


def upsert_copernicus(enriched, db_path: str | None) -> int:
    db_url = resolve_database_url(db_path)
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    upserted = 0
    for _, row in enriched.iterrows():
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
                int(row["id"]),
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
        upserted += 1

    conn.commit()
    cur.close()
    conn.close()
    return upserted


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
        lastupdate_gte=window["lastupdate_gte"],
        verbose=verbose,
    )
    log(verbose, f"[COPERNICUS] fetched_rows={len(rows)}")
    rows, dropped_out_of_window = filter_rows_by_firedate(rows, window["firedate_gte"], window["firedate_lte"])
    if dropped_out_of_window:
        log(verbose, f"[COPERNICUS] dropped_out_of_window_rows={dropped_out_of_window}")
    log(verbose, f"[COPERNICUS] rows_after_firedate_filter={len(rows)}")
    save_snapshot(rows, args.output_csv)
    log(verbose, f"[COPERNICUS] snapshot_saved={args.output_csv}")

    fires = rows_to_geodataframe(rows)
    log(verbose, f"[COPERNICUS] rows_with_geometry={len(fires)}")
    municipalities = load_municipalities(args.geojson)
    normalized_name_lookup = load_normalized_name_lookup(args.db_path)
    enriched = assign_municipalities(fires, municipalities, normalized_name_lookup)
    matched = int(enriched["municipality_key"].notna().sum())
    log(verbose, f"[COPERNICUS] municipality_matched={matched}/{len(enriched)}")
    upserted = upsert_copernicus(enriched, args.db_path)
    log(verbose, f"[COPERNICUS] upserted_rows={upserted}")

    total = len(enriched)
    print(json.dumps({
        "window": {
            "mode": window["mode"],
            "start_date": window["firedate_gte"],
            "end_date": window["firedate_lte"],
            "lastupdate_gte": window["lastupdate_gte"],
        },
        "fetched_rows": len(rows),
        "dropped_out_of_window_rows": dropped_out_of_window,
        "rows_with_geometry": total,
        "matched_rows": matched,
        "unmatched_rows": total - matched,
        "upserted_rows": upserted,
        "snapshot_csv": str(args.output_csv),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
