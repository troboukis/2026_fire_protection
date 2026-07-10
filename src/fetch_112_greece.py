from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
import psycopg2
import requests
from dotenv import load_dotenv
from openai import OpenAI
from psycopg2.extras import Json, RealDictCursor
from shapely.geometry import Point

try:
    from src.map_copernicus_to_municipalities import DEFAULT_GEOJSON, ROOT, WORKING_CRS, load_municipalities, resolve_database_url
except ModuleNotFoundError:
    from map_copernicus_to_municipalities import DEFAULT_GEOJSON, ROOT, WORKING_CRS, load_municipalities, resolve_database_url


load_dotenv(ROOT / ".env")
load_dotenv(ROOT.parent / ".env")

X_API_BASE = "https://api.x.com/2"
X_USERNAME = "112Greece"
STATE_PATH = ROOT / "logs" / "x_112_greece_state.json"
GOOGLE_GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"
REQUEST_TIMEOUT = 30
CURRENT_FIRES_TABLE = "public.current_fires"
NOTICE_TABLE = 'public."112_notice"'
SPATIAL_FIRE_MATCH_RADIUS_KM = 10.0


@dataclass(frozen=True)
class XPost:
    post_id: str
    text: str
    created_at: str | None


@dataclass(frozen=True)
class FireMatch:
    incident_key: str
    municipality_key: str


class MunicipalityMatcher:
    def __init__(self, geojson_path: Path):
        municipalities = load_municipalities(geojson_path)
        self.municipalities = municipalities.to_crs(WORKING_CRS)
        self.sindex = self.municipalities.sindex

    def match(self, lat: float, lon: float) -> tuple[str, str] | None:
        point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(WORKING_CRS).iloc[0]
        candidate_idx = list(self.sindex.intersection(point.bounds))
        if not candidate_idx:
            return None
        candidates = self.municipalities.iloc[candidate_idx]
        containing = candidates[candidates.contains(point)]
        if containing.empty:
            containing = candidates[candidates.intersects(point)]
        if containing.empty:
            return None
        row = containing.iloc[0]
        return str(row["municipality_code"]).strip(), str(row["name"]).strip()


def log(message: str) -> None:
    print(f"[112_greece] {message}", flush=True)


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_greek(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value or "")
    without_marks = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", without_marks.upper()).strip()


def normalize_112_activation_prefix(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "")
    text = re.sub(r"([0-9])\ufe0f?\u20e3", r"\1", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^[^A-Za-zΑ-Ωα-ωΆ-ώ]+", "", text).strip()
    return normalize_greek(text)


def starts_with_greek_112_activation(text: str) -> bool:
    return normalize_112_activation_prefix(text).startswith("ΕΝΕΡΓΟΠΟΙΗΣΗ 112")


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def resolve_env(name: str, *, required: bool = True) -> str | None:
    value = (os.getenv(name) or "").strip().strip("'\"")
    if value:
        return value
    if required:
        raise ValueError(f"Missing {name}")
    return None


def x_get(path: str, bearer_token: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.get(
        f"{X_API_BASE}{path}",
        headers={"Authorization": f"Bearer {bearer_token}"},
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def resolve_x_user_id(bearer_token: str, username: str) -> str:
    payload = x_get(f"/users/by/username/{username}", bearer_token, {"user.fields": "id,username,name"})
    user_id = payload.get("data", {}).get("id")
    if not user_id:
        raise ValueError(f"X user id not found for @{username}: {payload}")
    return str(user_id)


def fetch_recent_posts(
    bearer_token: str,
    user_id: str,
    *,
    since_id: str | None,
) -> list[XPost]:
    page_size = 100
    pagination_token: str | None = None
    posts: list[XPost] = []

    while True:
        page = fetch_recent_posts_page(
            bearer_token,
            user_id,
            since_id=since_id,
            pagination_token=pagination_token,
            page_size=page_size,
        )
        posts.extend(page["posts"])
        pagination_token = page["next_token"]
        if not pagination_token:
            break

    return sorted(posts, key=lambda item: int(item.post_id))


def fetch_recent_posts_page(
    bearer_token: str,
    user_id: str,
    *,
    since_id: str | None,
    pagination_token: str | None,
    page_size: int,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "max_results": page_size,
        "tweet.fields": "created_at,entities,referenced_tweets",
        "exclude": "retweets,replies",
    }
    if since_id:
        params["since_id"] = since_id
    if pagination_token:
        params["pagination_token"] = pagination_token

    payload = x_get(f"/users/{user_id}/tweets", bearer_token, params)
    posts = [
        XPost(
            post_id=str(item["id"]),
            text=clean_text(item.get("text")),
            created_at=item.get("created_at"),
        )
        for item in payload.get("data", [])
        if item.get("id") and clean_text(item.get("text"))
    ]
    return {
        "posts": posts,
        "next_token": payload.get("meta", {}).get("next_token"),
    }


def extraction_schema() -> dict[str, Any]:
    place = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "name": {"type": "string"},
            "regional_unit": {"type": ["string", "null"]},
            "municipality_or_context": {"type": ["string", "null"]},
            "geocode_query": {"type": "string"},
        },
        "required": ["name", "regional_unit", "municipality_or_context", "geocode_query"],
    }
    instruction = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "instruction_text": {"type": "string"},
            "from_places": {"type": "array", "items": place},
            "to_places": {"type": "array", "items": place},
        },
        "required": ["instruction_text", "from_places", "to_places"],
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "is_112_notice": {"type": "boolean"},
            "notice_type": {"type": ["string", "null"]},
            "affected_places": {"type": "array", "items": place},
            "instructions": {"type": "array", "items": instruction},
        },
        "required": ["is_112_notice", "notice_type", "affected_places", "instructions"],
    }


def extract_112_instructions(client: OpenAI, text: str) -> dict[str, Any]:
    prompt = f"""
Εξήγαγε περιοχές και οδηγίες πολιτών από ανακοίνωση του 112.

Κανόνες:
1. Επέστρεψε JSON μόνο σύμφωνα με το schema.
2. Το affected_places περιέχει τις περιοχές στις οποίες αναφέρεται ο κίνδυνος/η πυρκαγιά/η προειδοποίηση, ακόμη και όταν δεν υπάρχει οδηγία μετακίνησης.
3. Κάθε ξεχωριστό "αν βρίσκεστε ... απομακρυνθείτε προς ..." είναι ξεχωριστή instruction.
4. Το from_places περιέχει όλες τις περιοχές εκκίνησης/κινδύνου για οδηγίες μετακίνησης.
5. Το to_places περιέχει όλες τις περιοχές προορισμού/κατεύθυνσης.
6. Αν αναφέρεται περιφερειακή ενότητα ή νομός, βάλε την ως regional_unit σε κάθε σχετικό μέρος.
7. Το geocode_query πρέπει να είναι σύντομο και κατάλληλο για Google Geocoding στην Ελλάδα.
8. Μην επινοείς περιοχές που δεν υπάρχουν στο κείμενο.
9. Αν υπάρχει κείμενο όπως "Δασική πυρκαγιά στην περιοχή Δερβένι της Περιφερειακής Ενότητας Θεσσαλονίκης", βάλε το Δερβένι, Θεσσαλονίκη στο affected_places, ακόμη κι αν το instructions είναι [].
10. Για παράδειγμα, "Φιλοθέη και Θυμαριώνα της Π.Ε. Κορίνθου προς Ξυλοκέριζα" δίνει:
   from Φιλοθέη, Κόρινθος και Θυμαριώνα, Κόρινθος, to Ξυλοκέριζα, Κόρινθος.

Κείμενο:
{text}
""".strip()
    response = client.responses.create(
        model="gpt-5-mini",
        input=prompt,
        text={
            "format": {
                "type": "json_schema",
                "name": "x_112_notice_instructions",
                "schema": extraction_schema(),
                "strict": True,
            }
        },
    )
    parsed = json.loads(response.output_text)
    if not isinstance(parsed, dict):
        raise ValueError("OpenAI extraction did not return an object")
    return parsed


def geocode_place(google_api_key: str, query: str) -> dict[str, Any]:
    response = requests.get(
        GOOGLE_GEOCODING_URL,
        params={"address": query, "key": google_api_key, "region": "gr", "language": "el"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    result = payload.get("results", [{}])[0] if payload.get("status") == "OK" else {}
    location = result.get("geometry", {}).get("location", {})
    return {
        "status": payload.get("status"),
        "query": query,
        "lat": location.get("lat"),
        "lon": location.get("lng"),
        "formatted_address": result.get("formatted_address"),
        "place_id": result.get("place_id"),
    }


def geocode_extracted_instructions(
    extracted: dict[str, Any],
    google_api_key: str,
    municipality_matcher: MunicipalityMatcher,
) -> dict[str, Any]:
    geocode_cache: dict[str, dict[str, Any]] = {}
    enriched = json.loads(json.dumps(extracted, ensure_ascii=False))

    def enrich_place(place: dict[str, Any]) -> None:
        query = clean_text(place.get("geocode_query"))
        if not query:
            return
        if query not in geocode_cache:
            geocode_cache[query] = geocode_place(google_api_key, query)
        place["geocoding"] = geocode_cache[query]
        lat_lon = place_lat_lon(place)
        if lat_lon is None:
            return
        municipality = municipality_matcher.match(*lat_lon)
        if municipality:
            municipality_key, municipality_name = municipality
            place["municipality_key"] = municipality_key
            place["municipality_name"] = municipality_name

    for place in enriched.get("affected_places", []):
        enrich_place(place)

    for instruction in enriched.get("instructions", []):
        instruction["path"] = {
            "from": [place.get("geocode_query") for place in instruction.get("from_places", [])],
            "to": [place.get("geocode_query") for place in instruction.get("to_places", [])],
        }
        for role in ("from_places", "to_places"):
            for place in instruction.get(role, []):
                enrich_place(place)
    return enriched


def place_lat_lon(place: dict[str, Any]) -> tuple[float, float] | None:
    geocoding = place.get("geocoding") or {}
    lat = geocoding.get("lat")
    lon = geocoding.get("lon")
    if lat is None or lon is None:
        return None
    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None


def extracted_places(enriched: dict[str, Any]) -> list[dict[str, Any]]:
    places: list[dict[str, Any]] = list(enriched.get("affected_places", []))
    for instruction in enriched.get("instructions", []):
        places.extend(instruction.get("from_places", []))
        places.extend(instruction.get("to_places", []))
    return places


def fire_relevant_places(enriched: dict[str, Any]) -> list[dict[str, Any]]:
    places: list[dict[str, Any]] = list(enriched.get("affected_places", []))
    for instruction in enriched.get("instructions", []):
        places.extend(instruction.get("from_places", []))
    return places


def notice_municipality_keys(enriched: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for place in extracted_places(enriched):
        key = clean_text(place.get("municipality_key"))
        if key and key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def notice_fire_coordinates(enriched: dict[str, Any]) -> list[tuple[float, float]]:
    coordinates: list[tuple[float, float]] = []
    seen: set[tuple[float, float]] = set()
    for place in fire_relevant_places(enriched):
        lat_lon = place_lat_lon(place)
        if lat_lon is None:
            continue
        rounded = (round(lat_lon[0], 6), round(lat_lon[1], 6))
        if rounded in seen:
            continue
        seen.add(rounded)
        coordinates.append(lat_lon)
    return coordinates


def distance_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1 = a
    lat2, lon2 = b
    radius_km = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    haversine = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return 2 * radius_km * math.atan2(math.sqrt(haversine), math.sqrt(1 - haversine))


def find_current_fire_match(conn, enriched: dict[str, Any]) -> FireMatch | None:
    municipality_keys = notice_municipality_keys(enriched)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if municipality_keys:
            cur.execute(
                f"""
                SELECT
                  incident_key,
                  municipality_key
                FROM {CURRENT_FIRES_TABLE}
                WHERE is_current IS TRUE
                  AND status = ANY(%(active_statuses)s)
                  AND municipality_key = ANY(%(municipality_keys)s)
                """,
                {
                    "active_statuses": ["ΣΕ ΕΞΕΛΙΞΗ", "ΜΕΡΙΚΟΣ ΕΛΕΓΧΟΣ"],
                    "municipality_keys": municipality_keys,
                },
            )
            candidates = [dict(row) for row in cur.fetchall()]
            if len(candidates) == 1:
                candidate = candidates[0]
                return FireMatch(
                    incident_key=str(candidate["incident_key"]),
                    municipality_key=str(candidate["municipality_key"]),
                )
            if len(candidates) > 1:
                log(f"skip_ambiguous_municipality_match matches={candidates}")
                return None

        notice_coordinates = notice_fire_coordinates(enriched)
        if not notice_coordinates:
            return None

        cur.execute(
            f"""
            SELECT
              incident_key,
              municipality_key,
              lat,
              lon
            FROM {CURRENT_FIRES_TABLE}
            WHERE is_current IS TRUE
              AND status = ANY(%(active_statuses)s)
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            """,
            {"active_statuses": ["ΣΕ ΕΞΕΛΙΞΗ", "ΜΕΡΙΚΟΣ ΕΛΕΓΧΟΣ"]},
        )
        spatial_candidates = []
        for row in cur.fetchall():
            candidate = dict(row)
            fire_coordinates = (float(candidate["lat"]), float(candidate["lon"]))
            nearest_km = min(distance_km(point, fire_coordinates) for point in notice_coordinates)
            if nearest_km <= SPATIAL_FIRE_MATCH_RADIUS_KM:
                spatial_candidates.append((nearest_km, candidate))
    finally:
        cur.close()

    if len(spatial_candidates) != 1:
        if len(spatial_candidates) > 1:
            matches = [
                {
                    "incident_key": candidate["incident_key"],
                    "municipality_key": candidate["municipality_key"],
                    "distance_km": round(nearest_km, 2),
                }
                for nearest_km, candidate in sorted(spatial_candidates, key=lambda item: item[0])
            ]
            log(f"skip_ambiguous_spatial_match matches={matches}")
        return None
    nearest_km, candidate = spatial_candidates[0]
    log(
        "spatial_fire_match "
        f"incident_key={candidate['incident_key']} municipality_key={candidate['municipality_key']} "
        f"distance_km={nearest_km:.2f}"
    )
    return FireMatch(
        incident_key=str(candidate["incident_key"]),
        municipality_key=str(candidate["municipality_key"]),
    )


def parse_posted_at(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def upsert_112_notice(conn, post: XPost, enriched: dict[str, Any], match: FireMatch | None) -> None:
    now = datetime.now(timezone.utc)
    post_url = f"https://x.com/{X_USERNAME}/status/{post.post_id}"
    raw = {
        "source": "x",
        "account": X_USERNAME,
        "post_id": post.post_id,
        "post_url": post_url,
        "text": post.text,
        "created_at": post.created_at,
    }
    cur = conn.cursor()
    cur.execute(
        f"""
        INSERT INTO {NOTICE_TABLE} (
          notice_id,
          current_fire_incident_key,
          source,
          account,
          post_id,
          post_url,
          posted_at,
          fetched_at,
          notice_type,
          notice_text,
          instructions_geocoded,
          municipality_keys,
          matched_municipality_key,
          raw
        )
        VALUES (
          %(notice_id)s,
          %(current_fire_incident_key)s,
          'x',
          %(account)s,
          %(post_id)s,
          %(post_url)s,
          %(posted_at)s,
          %(fetched_at)s,
          %(notice_type)s,
          %(notice_text)s,
          %(instructions_geocoded)s,
          %(municipality_keys)s,
          %(matched_municipality_key)s,
          %(raw)s
        )
        ON CONFLICT (post_id) DO UPDATE SET
          current_fire_incident_key = EXCLUDED.current_fire_incident_key,
          account = EXCLUDED.account,
          post_url = EXCLUDED.post_url,
          posted_at = EXCLUDED.posted_at,
          fetched_at = EXCLUDED.fetched_at,
          notice_type = EXCLUDED.notice_type,
          notice_text = EXCLUDED.notice_text,
          instructions_geocoded = EXCLUDED.instructions_geocoded,
          municipality_keys = EXCLUDED.municipality_keys,
          matched_municipality_key = EXCLUDED.matched_municipality_key,
          raw = EXCLUDED.raw
        """,
        {
            "notice_id": f"x-112-{post.post_id}",
            "current_fire_incident_key": match.incident_key if match else None,
            "account": X_USERNAME,
            "post_id": post.post_id,
            "post_url": post_url,
            "posted_at": parse_posted_at(post.created_at),
            "fetched_at": now,
            "notice_type": enriched.get("notice_type"),
            "notice_text": post.text,
            "instructions_geocoded": Json(enriched),
            "municipality_keys": notice_municipality_keys(enriched),
            "matched_municipality_key": match.municipality_key if match else None,
            "raw": Json(raw),
        },
    )
    conn.commit()
    cur.close()


def process_post(
    post: XPost,
    *,
    openai_client: OpenAI,
    google_api_key: str,
    municipality_matcher: MunicipalityMatcher,
    conn,
    dry_run: bool,
) -> bool:
    if not starts_with_greek_112_activation(post.text):
        log(f"skip_not_greek_112_activation post_id={post.post_id}")
        return False

    log(f"extract_start post_id={post.post_id}")
    extracted = extract_112_instructions(openai_client, post.text)
    extracted["is_112_notice"] = True

    log(f"geocode_start post_id={post.post_id}")
    enriched = geocode_extracted_instructions(extracted, google_api_key, municipality_matcher)
    enriched["source"] = {
        "platform": "x",
        "account": X_USERNAME,
        "post_id": post.post_id,
        "post_url": f"https://x.com/{X_USERNAME}/status/{post.post_id}",
        "created_at": post.created_at,
    }
    enriched["notice_text"] = post.text
    enriched["processed_at_utc"] = datetime.now(timezone.utc).isoformat()

    if dry_run:
        print(json.dumps(enriched, ensure_ascii=False, indent=2))
        return True

    match = find_current_fire_match(conn, enriched)
    if not match:
        log(f"store_unmatched_notice post_id={post.post_id}")
    else:
        log(
            "store_matched_notice "
            f"post_id={post.post_id} incident_key={match.incident_key} municipality_key={match.municipality_key}"
        )
    upsert_112_notice(conn, post, enriched, match)
    return True


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Poll @112Greece X posts, extract/geocode instructions, and store notices in public."112_notice".',
    )
    parser.add_argument("--username", default=X_USERNAME, help="X username to poll. Default: 112Greece")
    parser.add_argument("--state-path", type=Path, default=STATE_PATH, help="JSON state path for since_id")
    parser.add_argument("--db-path", default=None, help="Optional DATABASE_URL override")
    parser.add_argument("--sample-text", default=None, help="Process this text instead of calling X")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON, help="Municipalities GeoJSON")
    parser.add_argument("--dry-run", action="store_true", help="Print extracted/geocoded JSON and skip DB writes")
    parser.add_argument("--no-state", action="store_true", help="Do not read or update since_id state")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    global X_USERNAME
    X_USERNAME = args.username

    try:
        openai_client = OpenAI(api_key=resolve_env("OPENAI_API_KEY"))
        google_api_key = resolve_env("GOOGLE_GEOCODING_API_KEY")
        municipality_matcher = MunicipalityMatcher(args.geojson)
        conn = None if args.dry_run else psycopg2.connect(resolve_database_url(args.db_path))

        state = {} if args.no_state else load_state(args.state_path)
        if args.sample_text:
            posts = [XPost(post_id="sample", text=args.sample_text, created_at=datetime.now(timezone.utc).isoformat())]
        else:
            bearer_token = resolve_env("X_BEARER_TOKEN")
            user_id = str(state.get("user_id") or resolve_x_user_id(bearer_token, args.username))
            state["user_id"] = user_id
            posts = fetch_recent_posts(
                bearer_token,
                user_id,
                since_id=None if args.no_state else state.get("last_seen_id"),
            )

        processed = 0
        try:
            for post in posts:
                if process_post(
                    post,
                    openai_client=openai_client,
                    google_api_key=google_api_key,
                    municipality_matcher=municipality_matcher,
                    conn=conn,
                    dry_run=args.dry_run,
                ):
                    processed += 1
        finally:
            if conn:
                conn.close()

        if posts and not args.dry_run and not args.no_state and not args.sample_text:
            state["last_seen_id"] = max([str(state.get("last_seen_id") or "0")] + [post.post_id for post in posts], key=int)
            save_state(args.state_path, state)

        log(f"fetched={len(posts)} processed={processed}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
