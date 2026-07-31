#!/usr/bin/env python3
"""Fallback ingestion of forest-fire posts from @pyrosvestiki on X."""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
from openai import OpenAI
from psycopg2.extras import RealDictCursor, execute_values

try:
    from src.fetch_112_greece import (
        DEFAULT_GEOJSON,
        MunicipalityMatcher,
        clean_text,
        distance_km,
        geocode_place,
        resolve_database_url,
        resolve_env,
        x_get,
    )
except ModuleNotFoundError:
    from fetch_112_greece import (
        DEFAULT_GEOJSON,
        MunicipalityMatcher,
        clean_text,
        distance_km,
        geocode_place,
        resolve_database_url,
        resolve_env,
        x_get,
    )


X_USERNAME = "pyrosvestiki"
X_SOURCE = "fireservice_x"
CURRENT_FIRES_TABLE = "public.current_fires"
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_PATH = ROOT / "logs" / "x_pyrosvestiki_enrichment_cache.json"
ATHENS_TZ = ZoneInfo("Europe/Athens")
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_X_STALE_HOURS = 24
DEFAULT_WEB_STALE_HOURS = 24
SPATIAL_MATCH_RADIUS_KM = 7.0
FOREST_TERMS = (
    "ΔΑΣΙΚ",
    "ΑΓΡΟΤΟΔΑΣΙΚ",
    "ΧΟΡΤΟΛΙΒΑΔ",
    "ΧΑΜΗΛΗ ΒΛΑΣΤΗΣΗ",
    "ΥΠΑΙΘΡ",
)


@dataclass(frozen=True)
class XFirePost:
    post_id: str
    text: str
    created_at: str
    replied_to_id: str | None


def log(message: str) -> None:
    print(f"[pyrosvestiki_fires] {message}", flush=True)


def normalize_greek(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value or "")
    without_marks = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", without_marks.upper()).strip()


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def x_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def replied_to_id(item: dict[str, Any]) -> str | None:
    for reference in item.get("referenced_tweets", []):
        if reference.get("type") == "replied_to" and reference.get("id"):
            return str(reference["id"])
    return None


def post_from_payload(item: dict[str, Any]) -> XFirePost:
    return XFirePost(
        post_id=str(item["id"]),
        text=clean_text(item.get("text")),
        created_at=str(item.get("created_at") or ""),
        replied_to_id=replied_to_id(item),
    )


def fetch_posts(
    bearer_token: str,
    username: str,
    *,
    start_time: datetime,
    end_time: datetime | None = None,
) -> list[XFirePost]:
    user = x_get(f"/users/by/username/{username}", bearer_token, {"user.fields": "id,username"})
    user_id = str(user["data"]["id"])
    posts: list[XFirePost] = []
    pagination_token: str | None = None

    while True:
        params: dict[str, Any] = {
            "start_time": x_timestamp(start_time),
            "max_results": 100,
            "tweet.fields": "created_at,referenced_tweets",
            "exclude": "retweets",
        }
        if end_time:
            params["end_time"] = x_timestamp(end_time)
        if pagination_token:
            params["pagination_token"] = pagination_token
        payload = x_get(f"/users/{user_id}/tweets", bearer_token, params)
        posts.extend(post_from_payload(item) for item in payload.get("data", []))
        pagination_token = payload.get("meta", {}).get("next_token")
        if not pagination_token:
            break

    return sorted(posts, key=lambda post: (parse_timestamp(post.created_at), int(post.post_id)))


def fetch_context_posts(
    bearer_token: str,
    target_posts: list[XFirePost],
    *,
    max_depth: int = 4,
) -> dict[str, XFirePost]:
    posts = {post.post_id: post for post in target_posts}
    pending = {post.replied_to_id for post in target_posts if post.replied_to_id and post.replied_to_id not in posts}

    for _ in range(max_depth):
        if not pending:
            break
        ids = sorted(pending)
        pending = set()
        for offset in range(0, len(ids), 100):
            payload = x_get(
                "/tweets",
                bearer_token,
                {
                    "ids": ",".join(ids[offset:offset + 100]),
                    "tweet.fields": "created_at,referenced_tweets",
                },
            )
            for item in payload.get("data", []):
                post = post_from_payload(item)
                posts[post.post_id] = post
                if post.replied_to_id and post.replied_to_id not in posts:
                    pending.add(post.replied_to_id)
    return posts


def root_post_id(post: XFirePost, posts_by_id: dict[str, XFirePost]) -> str:
    current = post
    seen = {post.post_id}
    while current.replied_to_id and current.replied_to_id not in seen:
        seen.add(current.replied_to_id)
        parent = posts_by_id.get(current.replied_to_id)
        if parent is None:
            return current.replied_to_id
        current = parent
    return current.post_id


def likely_fire_post(post: XFirePost, posts_by_id: dict[str, XFirePost]) -> bool:
    texts = [post.text]
    if post.replied_to_id and post.replied_to_id in posts_by_id:
        texts.append(posts_by_id[post.replied_to_id].text)
    normalized = normalize_greek(" ".join(texts))
    aggregate_or_forecast = (
        "ΕΚΔΗΛΩΘΗΚΑΝ ΤΟ ΤΕΛΕΥΤΑΙΟ 24ΩΡΟ",
        "ΧΑΡΤΗΣ ΠΡΟΒΛΕΨΗΣ ΚΙΝΔΥΝΟΥ",
    )
    return (
        "ΠΥΡΚΑΓ" in normalized
        and not any(term in normalized for term in aggregate_or_forecast)
    )


def extraction_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "is_forest_fire": {"type": "boolean"},
            "location_name": {"type": ["string", "null"]},
            "regional_unit": {"type": ["string", "null"]},
            "region": {"type": ["string", "null"]},
            "municipality_or_context": {"type": ["string", "null"]},
            "geocode_query": {"type": ["string", "null"]},
            "fuel_type": {"type": ["string", "null"]},
            "status": {
                "type": ["string", "null"],
                "enum": ["ΣΕ ΕΞΕΛΙΞΗ", "ΜΕΡΙΚΟΣ ΕΛΕΓΧΟΣ", "ΠΛΗΡΗΣ ΕΛΕΓΧΟΣ", "ΛΗΞΗ", None],
            },
        },
        "required": [
            "is_forest_fire",
            "location_name",
            "regional_unit",
            "region",
            "municipality_or_context",
            "geocode_query",
            "fuel_type",
            "status",
        ],
    }


def extract_fire(client: OpenAI, post: XFirePost, context: list[XFirePost]) -> dict[str, Any]:
    context_text = "\n".join(
        f"- {item.created_at}: {item.text}"
        for item in context
        if item.post_id != post.post_id
    ) or "(δεν υπάρχει)"
    prompt = f"""
Αξιολόγησε αν η τρέχουσα ανάρτηση του επίσημου λογαριασμού του Πυροσβεστικού
Σώματος αφορά ένα συγκεκριμένο πραγματικό συμβάν δασικής, αγροτοδασικής,
χορτολιβαδικής πυρκαγιάς ή πυρκαγιάς σε χαμηλή βλάστηση στην Ελλάδα.

Απόρριψε διασώσεις, αστικές/κτιριακές πυρκαγιές, χάρτες κινδύνου, προγνώσεις,
ανακοινώσεις τύπου και συγκεντρωτικά πλήθη πυρκαγιών χωρίς ένα συγκεκριμένο συμβάν.
Μια ενημέρωση ή ανακοίνωση ελέγχου σε reply thread είναι σχετική όταν το context
δείχνει ότι το αρχικό συμβάν ήταν δασική πυρκαγιά.

Για σχετικό συμβάν:
- εξήγαγε μία συγκεκριμένη κύρια τοποθεσία,
- φτιάξε ακριβές Google geocode query που τελειώνει σε "Ελλάδα",
- κανονικοποίησε fuel_type σε σύντομη ελληνική περιγραφή,
- status: "ΜΕΡΙΚΟΣ ΕΛΕΓΧΟΣ", "ΠΛΗΡΗΣ ΕΛΕΓΧΟΣ" ή "ΛΗΞΗ" μόνο όταν δηλώνεται
  ρητά· διαφορετικά "ΣΕ ΕΞΕΛΙΞΗ".
- μην επινοήσεις διοικητική πληροφορία που δεν προκύπτει αξιόπιστα.

Context προηγούμενων αναρτήσεων:
{context_text}

Τρέχουσα ανάρτηση:
{post.created_at}: {post.text}
""".strip()
    response = client.responses.create(
        model="gpt-5-mini",
        input=prompt,
        text={
            "format": {
                "type": "json_schema",
                "name": "pyrosvestiki_forest_fire",
                "schema": extraction_schema(),
                "strict": True,
            }
        },
    )
    parsed = json.loads(response.output_text)
    if not isinstance(parsed, dict):
        raise ValueError("OpenAI extraction did not return an object")
    return parsed


def context_chain(post: XFirePost, posts_by_id: dict[str, XFirePost]) -> list[XFirePost]:
    chain = [post]
    current = post
    seen = {post.post_id}
    while current.replied_to_id and current.replied_to_id not in seen:
        seen.add(current.replied_to_id)
        parent = posts_by_id.get(current.replied_to_id)
        if parent is None:
            break
        chain.append(parent)
        current = parent
    return list(reversed(chain))


def enrich_extraction(
    extracted: dict[str, Any],
    google_api_key: str,
    matcher: MunicipalityMatcher,
) -> dict[str, Any]:
    enriched = dict(extracted)
    query = clean_text(extracted.get("geocode_query"))
    if not query:
        return enriched
    geocoding = geocode_place(google_api_key, query)
    enriched["geocoding"] = geocoding
    lat = geocoding.get("lat")
    lon = geocoding.get("lon")
    if lat is None or lon is None:
        return enriched
    municipality = matcher.match(float(lat), float(lon))
    if municipality:
        enriched["municipality_key"], enriched["municipality_name"] = municipality
    return enriched


def load_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_cache(path: Path, cache: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def load_processed_post_ids(conn) -> set[str]:
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT raw FROM {CURRENT_FIRES_TABLE} WHERE source = %s AND raw IS NOT NULL",
            (X_SOURCE,),
        )
        processed: set[str] = set()
        for (raw,) in cur.fetchall():
            try:
                payload = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
            for post in payload.get("posts", []):
                post_id = clean_text(post.get("post_id"))
                if post_id:
                    processed.add(post_id)
        return processed
    finally:
        cur.close()


def merge_incident_update(current: dict[str, Any] | None, update: dict[str, Any]) -> dict[str, Any]:
    if current is None:
        return update
    merged = dict(current)
    for key, value in update.items():
        if value not in (None, "", []):
            merged[key] = value
    merged["first_seen_at"] = min(current["first_seen_at"], update["first_seen_at"])
    merged["last_seen_at"] = max(current["last_seen_at"], update["last_seen_at"])
    merged["source_post_id"] = update["source_post_id"]
    merged["source_url"] = update["source_url"]
    merged["raw_posts"] = [*current.get("raw_posts", []), *update.get("raw_posts", [])]
    return merged


def build_incidents(
    target_posts: list[XFirePost],
    posts_by_id: dict[str, XFirePost],
    extractions: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    incidents: dict[str, dict[str, Any]] = {}
    for post in target_posts:
        extracted = extractions.get(post.post_id)
        if not extracted or not extracted.get("is_forest_fire"):
            continue
        root_id = root_post_id(post, posts_by_id)
        root = posts_by_id.get(root_id, post)
        posted_at = parse_timestamp(post.created_at)
        first_seen_at = parse_timestamp(root.created_at)
        status = clean_text(extracted.get("status")) or "ΣΕ ΕΞΕΛΙΞΗ"
        geocoding = extracted.get("geocoding") or {}
        row = {
            "root_post_id": root_id,
            "incident_key": f"x-pyrosvestiki-{root_id}",
            "first_seen_at": first_seen_at,
            "last_seen_at": posted_at,
            "is_current": status not in {"ΠΛΗΡΗΣ ΕΛΕΓΧΟΣ", "ΛΗΞΗ"},
            "category": "ΔΑΣΙΚΕΣ ΠΥΡΚΑΓΙΕΣ",
            "region": clean_text(extracted.get("region")) or None,
            "regional_unit": clean_text(extracted.get("regional_unit")) or None,
            "municipality_key": clean_text(extracted.get("municipality_key")) or None,
            "municipality_normalized_value": clean_text(extracted.get("municipality_name")) or None,
            "municipality_raw": (
                clean_text(extracted.get("municipality_name"))
                or clean_text(extracted.get("municipality_or_context"))
                or clean_text(extracted.get("location_name"))
            ),
            "fuel_type": clean_text(extracted.get("fuel_type")) or "ΔΑΣΙΚΗ ΕΚΤΑΣΗ",
            "start_date": first_seen_at.astimezone(ATHENS_TZ).date(),
            "days_burning": (posted_at.astimezone(ATHENS_TZ).date() - first_seen_at.astimezone(ATHENS_TZ).date()).days + 1,
            "status_updated_at": posted_at,
            "lat": geocoding.get("lat"),
            "lon": geocoding.get("lon"),
            "formatted_address": clean_text(geocoding.get("formatted_address")) or None,
            "place_id": clean_text(geocoding.get("place_id")) or None,
            "geocode_query": clean_text(extracted.get("geocode_query")) or None,
            "geocoded_at": datetime.now(timezone.utc) if geocoding.get("lat") is not None else None,
            "status": status,
            "source": X_SOURCE,
            "source_account": f"@{X_USERNAME}",
            "source_post_id": post.post_id,
            "source_url": f"https://x.com/{X_USERNAME}/status/{post.post_id}",
            "raw_posts": [{
                "post_id": post.post_id,
                "created_at": post.created_at,
                "text": post.text,
                "replied_to_id": post.replied_to_id,
                "extracted": extracted,
            }],
        }
        incidents[root_id] = merge_incident_update(incidents.get(root_id), row)
    return sorted(incidents.values(), key=lambda row: row["last_seen_at"])


def select_existing_incident_key(conn, row: dict[str, Any]) -> str:
    deterministic_key = row["incident_key"]
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"SELECT incident_key FROM {CURRENT_FIRES_TABLE} WHERE incident_key = %s",
            (deterministic_key,),
        )
        exact = cur.fetchone()
        if exact:
            return str(exact["incident_key"])
        if row.get("lat") is None or row.get("lon") is None:
            return deterministic_key
        cur.execute(
            f"""
            SELECT incident_key, lat, lon
            FROM {CURRENT_FIRES_TABLE}
            WHERE is_current IS TRUE
              AND lat IS NOT NULL
              AND lon IS NOT NULL
              AND start_date BETWEEN %s::date - 1 AND %s::date + 1
            """,
            (row["start_date"], row["start_date"]),
        )
        matches = []
        point = (float(row["lat"]), float(row["lon"]))
        for candidate in cur.fetchall():
            candidate_point = (float(candidate["lat"]), float(candidate["lon"]))
            if distance_km(point, candidate_point) <= SPATIAL_MATCH_RADIUS_KM:
                matches.append(str(candidate["incident_key"]))
        return matches[0] if len(matches) == 1 else deterministic_key
    finally:
        cur.close()


def upsert_incidents(
    conn,
    incidents: list[dict[str, Any]],
    *,
    x_stale_hours: int,
    web_stale_hours: int,
) -> None:
    for row in incidents:
        row["incident_key"] = select_existing_incident_key(conn, row)

    payload = [
        (
            row["incident_key"],
            row["first_seen_at"],
            row["last_seen_at"],
            row["is_current"],
            row["category"],
            row["region"],
            row["regional_unit"],
            row["municipality_key"],
            row["municipality_normalized_value"],
            row["municipality_raw"],
            row["fuel_type"],
            row["start_date"],
            row["days_burning"],
            row["status_updated_at"],
            row["lat"],
            row["lon"],
            row["formatted_address"],
            row["place_id"],
            row["geocode_query"],
            row["geocoded_at"],
            row["status"],
            json.dumps({"source": X_SOURCE, "posts": row["raw_posts"]}, ensure_ascii=False),
            row["source"],
            row["source_account"],
            row["source_post_id"],
            row["source_url"],
        )
        for row in incidents
    ]
    cur = conn.cursor()
    try:
        if payload:
            execute_values(
                cur,
                f"""
                INSERT INTO {CURRENT_FIRES_TABLE} (
                  incident_key, first_seen_at, last_seen_at, is_current, category,
                  region, regional_unit, municipality_key, municipality_normalized_value,
                  municipality_raw, fuel_type, start_date, days_burning, status_updated_at,
                  lat, lon, formatted_address, place_id, geocode_query, geocoded_at,
                  status, raw, source, source_account, source_post_id, source_url
                ) VALUES %s
                ON CONFLICT (incident_key) DO UPDATE SET
                  first_seen_at = LEAST(current_fires.first_seen_at, EXCLUDED.first_seen_at),
                  last_seen_at = GREATEST(current_fires.last_seen_at, EXCLUDED.last_seen_at),
                  is_current = EXCLUDED.is_current,
                  category = EXCLUDED.category,
                  region = COALESCE(EXCLUDED.region, current_fires.region),
                  regional_unit = COALESCE(EXCLUDED.regional_unit, current_fires.regional_unit),
                  municipality_key = COALESCE(EXCLUDED.municipality_key, current_fires.municipality_key),
                  municipality_normalized_value = COALESCE(
                    EXCLUDED.municipality_normalized_value,
                    current_fires.municipality_normalized_value
                  ),
                  municipality_raw = COALESCE(EXCLUDED.municipality_raw, current_fires.municipality_raw),
                  fuel_type = COALESCE(EXCLUDED.fuel_type, current_fires.fuel_type),
                  start_date = LEAST(current_fires.start_date, EXCLUDED.start_date),
                  days_burning = EXCLUDED.days_burning,
                  status_updated_at = EXCLUDED.status_updated_at,
                  lat = COALESCE(EXCLUDED.lat, current_fires.lat),
                  lon = COALESCE(EXCLUDED.lon, current_fires.lon),
                  formatted_address = COALESCE(EXCLUDED.formatted_address, current_fires.formatted_address),
                  place_id = COALESCE(EXCLUDED.place_id, current_fires.place_id),
                  geocode_query = COALESCE(EXCLUDED.geocode_query, current_fires.geocode_query),
                  geocoded_at = COALESCE(EXCLUDED.geocoded_at, current_fires.geocoded_at),
                  status = EXCLUDED.status,
                  raw = CASE
                    WHEN current_fires.source = 'fireservice_x' THEN
                      jsonb_set(
                        EXCLUDED.raw::jsonb,
                        '{{posts}}',
                        COALESCE(current_fires.raw::jsonb -> 'posts', '[]'::jsonb)
                          || COALESCE(EXCLUDED.raw::jsonb -> 'posts', '[]'::jsonb)
                      )::text
                    ELSE EXCLUDED.raw
                  END,
                  source = EXCLUDED.source,
                  source_account = EXCLUDED.source_account,
                  source_post_id = EXCLUDED.source_post_id,
                  source_url = EXCLUDED.source_url
                """,
                payload,
            )
        cur.execute(
            f"""
            UPDATE {CURRENT_FIRES_TABLE}
            SET is_current = FALSE
            WHERE source = %s
              AND is_current IS TRUE
              AND last_seen_at < now() - make_interval(hours => %s)
            """,
            (X_SOURCE, x_stale_hours),
        )
        cur.execute(
            f"""
            UPDATE {CURRENT_FIRES_TABLE}
            SET is_current = FALSE
            WHERE source = 'fireservice_live_page'
              AND is_current IS TRUE
              AND last_seen_at < now() - make_interval(hours => %s)
            """,
            (web_stale_hours,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest forest-fire posts from @pyrosvestiki as a 500 fallback.")
    parser.add_argument("--username", default=X_USERNAME)
    parser.add_argument("--start-time", help="Inclusive ISO-8601 start time; default is 24 hours ago.")
    parser.add_argument("--end-time", help="Exclusive ISO-8601 end time.")
    parser.add_argument("--db-path", default=None, help="Optional DATABASE_URL override.")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON)
    parser.add_argument("--x-stale-hours", type=int, default=DEFAULT_X_STALE_HOURS)
    parser.add_argument("--web-stale-hours", type=int, default=DEFAULT_WEB_STALE_HOURS)
    parser.add_argument("--workers", type=int, default=3, help="Concurrent LLM/geocoding workers.")
    parser.add_argument("--cache-path", type=Path, default=DEFAULT_CACHE_PATH)
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    start_time = parse_timestamp(args.start_time) if args.start_time else now - timedelta(hours=DEFAULT_LOOKBACK_HOURS)
    end_time = parse_timestamp(args.end_time) if args.end_time else None

    conn = None
    try:
        bearer_token = resolve_env("X_BEARER_TOKEN")
        openai_client = OpenAI(api_key=resolve_env("OPENAI_API_KEY"))
        google_api_key = resolve_env("GOOGLE_GEOCODING_API_KEY")
        matcher = MunicipalityMatcher(args.geojson)
        conn = None if args.dry_run else psycopg2.connect(resolve_database_url(args.db_path))
        processed_post_ids = load_processed_post_ids(conn) if conn is not None else set()
        cache = {} if args.no_cache else load_cache(args.cache_path)
        target_posts = fetch_posts(
            bearer_token,
            args.username,
            start_time=start_time,
            end_time=end_time,
        )
        posts_by_id = fetch_context_posts(bearer_token, target_posts)
        candidates = [
            post
            for post in target_posts
            if post.post_id not in processed_post_ids and likely_fire_post(post, posts_by_id)
        ]
        log(
            f"fetched_posts={len(target_posts)} already_processed={len(processed_post_ids)} "
            f"candidate_posts={len(candidates)}"
        )

        extractions: dict[str, dict[str, Any]] = {}
        pending: list[XFirePost] = []
        for post in candidates:
            cached = cache.get(post.post_id)
            if cached:
                extractions[post.post_id] = cached
                log(f"cache_hit post_id={post.post_id}")
            else:
                pending.append(post)

        def process_post(post: XFirePost) -> tuple[XFirePost, dict[str, Any]]:
            extracted = extract_fire(openai_client, post, context_chain(post, posts_by_id))
            if extracted.get("is_forest_fire"):
                extracted = enrich_extraction(extracted, google_api_key, matcher)
            return post, extracted

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {executor.submit(process_post, post): post for post in pending}
            for future in as_completed(futures):
                post, extracted = future.result()
                extractions[post.post_id] = extracted
                if not args.no_cache:
                    cache[post.post_id] = extracted
                    save_cache(args.cache_path, cache)
                if extracted.get("is_forest_fire"):
                    log(
                        f"accept post_id={post.post_id} "
                        f"location={clean_text(extracted.get('location_name')) or '-'} "
                        f"status={clean_text(extracted.get('status')) or '-'}"
                    )
                else:
                    log(f"skip post_id={post.post_id}")

        incidents = build_incidents(target_posts, posts_by_id, extractions)
        if args.dry_run:
            print(json.dumps(incidents, ensure_ascii=False, indent=2, default=str))
        else:
            assert conn is not None
            upsert_incidents(
                conn,
                incidents,
                x_stale_hours=args.x_stale_hours,
                web_stale_hours=args.web_stale_hours,
            )
        log(f"stored_incidents={0 if args.dry_run else len(incidents)} dry_run={args.dry_run}")
        return 0
    except Exception as exc:
        print(f"ERROR: pyrosvestiki fallback failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
