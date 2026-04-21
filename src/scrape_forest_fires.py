#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scrape_forest_fires.py
----------------------
Scrapes active/live forest fire events (ΔΑΣΙΚΕΣ ΠΥΡΚΑΓΙΕΣ) from the Hellenic
Fire Service live-events page:

    https://www.fireservice.gr/el/energa-symvanta/

Output: upserts the cumulative latest-state dataset into `public.current_fires`.

Requirements:
    pip install requests beautifulsoup4 lxml psycopg2-binary

Usage:
    python scrape_forest_fires.py
    # optional:
    python scrape_forest_fires.py --all      # keep every category, not only forest fires
    python scrape_forest_fires.py --db-path  # override DATABASE_URL
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import os
import re
import sys
import unicodedata
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import psycopg2
import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from bs4.exceptions import FeatureNotFound
from psycopg2.extras import RealDictCursor, execute_values

URL = "https://www.fireservice.gr/el/energa-symvanta/"
ROOT = Path(__file__).resolve().parents[1]
FIRE_MUNICIPALITY_MAPPING_PATH = ROOT / "data" / "mappings" / "fire_municipality_mapping.csv"
REGION_TO_MUNICIPALITIES_PATH = ROOT / "data" / "mappings" / "region_to_municipalities.csv"
MUNICIPALITY_NORMALIZATION_PATH = ROOT / "municipality_normalization.py"
CURRENT_FIRES_TABLE = "public.current_fires"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "el-GR,el;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# Greek keywords that identify a forest/wildland fire in the category column.
FOREST_KEYWORDS = (
    "ΔΑΣΙΚ",       # ΔΑΣΙΚΗ / ΔΑΣΙΚΕΣ ΠΥΡΚΑΓΙΕΣ / ΔΑΣΙΚΗ ΠΥΡΚΑΓΙΑ
    "ΑΓΡΟΤΟΔΑΣΙΚ", # ΑΓΡΟΤΟΔΑΣΙΚΗ ΠΥΡΚΑΓΙΑ (agro-forest)
    "ΥΠΑΙΘΡΟΥ",    # ΠΥΡΚΑΓΙΑ ΥΠΑΙΘΡΟΥ
    "ΧΟΡΤΟΛΙΒ",    # ΧΟΡΤΟΛΙΒΑΔΙΚΗ ΠΥΡΚΑΓΙΑ
)


def strip_accents(s: str) -> str:
    """Remove Greek diacritics so matching is tone-insensitive."""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper()


def is_forest_fire(category: str) -> bool:
    """Return True if the category looks like a forest/wildland fire."""
    c = strip_accents(category or "")
    return any(k in c for k in FOREST_KEYWORDS)


def clean(text: object | None) -> str:
    if text is None:
        return ""
    if isinstance(text, str):
        raw = text
    else:
        raw = str(text)
    if not raw:
        return ""
    return re.sub(r"\s+", " ", raw).strip()


def _load_normalize_municipality():
    spec = importlib.util.spec_from_file_location(
        "fire_protection_municipality_normalization",
        MUNICIPALITY_NORMALIZATION_PATH,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {MUNICIPALITY_NORMALIZATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.normalizeMunicipality


normalize_municipality_base = _load_normalize_municipality()


def normalize_region_value(value: str | None) -> str:
    return re.sub(r"^ΠΕΡΙΦΕΡΕΙΑ\s+", "", strip_accents(value or "")).strip()


def normalize_municipality_value(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = normalize_municipality_base(text)
    normalized = strip_accents(normalized)
    normalized = normalized.replace("(", "").replace(")", "")
    normalized = re.sub(r"\s*-\s*", " - ", normalized)
    return clean(normalized)


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def resolve_database_url(db_path: str | None) -> str:
    def normalize_database_url(raw: str | None) -> str:
        value = str(raw or "").strip().strip("'\"")
        if not value:
            return ""
        if value.startswith("DATABASE_URL="):
            value = value.split("=", 1)[1].strip().strip("'\"")
        return value

    if db_path:
        normalized = normalize_database_url(db_path)
        if normalized:
            return normalized

    env_value = normalize_database_url(os.getenv("DATABASE_URL"))
    if env_value:
        return env_value

    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == "DATABASE_URL" and value.strip():
                normalized = normalize_database_url(value)
                if normalized:
                    return normalized

    raise ValueError("Δεν βρέθηκε DATABASE_URL ούτε δόθηκε db_path.")


@lru_cache(maxsize=1)
def municipality_lookup() -> tuple[dict[str, dict[str, str]], dict[str, list[dict[str, str]]]]:
    canonical_by_key: dict[str, dict[str, str]] = {}
    alias_lookup: dict[str, list[dict[str, str]]] = {}

    for row in _csv_rows(REGION_TO_MUNICIPALITIES_PATH):
        municipality_key = clean(row.get("municipality_id"))
        municipality_name = clean(row.get("municipality_name"))
        region = clean(row.get("region_id"))
        if not municipality_key or not municipality_name or not region:
            continue
        canonical = {
            "municipality_key": municipality_key,
            "municipality_normalized_value": normalize_municipality_value(municipality_name),
            "region": region,
        }
        canonical_by_key[municipality_key] = canonical
        for alias_value in (municipality_name, row.get("pdf_municipality_name")):
            alias = normalize_municipality_value(alias_value)
            if alias:
                alias_lookup.setdefault(alias, []).append(canonical)

    for row in _csv_rows(FIRE_MUNICIPALITY_MAPPING_PATH):
        raw_key = clean(row.get("municipality_id"))
        if not raw_key:
            continue
        municipality_key = raw_key[:-2] if raw_key.endswith(".0") else raw_key
        canonical = canonical_by_key.get(municipality_key)
        if not canonical:
            municipality_name = clean(row.get("municipality_name"))
            region = clean(row.get("region_id"))
            if not municipality_name or not region:
                continue
            canonical = {
                "municipality_key": municipality_key,
                "municipality_normalized_value": normalize_municipality_value(municipality_name),
                "region": region,
            }
            canonical_by_key[municipality_key] = canonical
        for alias_value in (row.get("fire_name"), row.get("normalized")):
            alias = normalize_municipality_value(alias_value)
            if alias:
                alias_lookup.setdefault(alias, []).append(canonical)

    return canonical_by_key, alias_lookup


def _matching_alias_rows(alias: str, region: str) -> list[dict[str, str]]:
    _, alias_lookup = municipality_lookup()
    candidates = alias_lookup.get(alias, [])
    if not candidates:
        return []
    if region:
        region_matches = [row for row in candidates if normalize_region_value(row["region"]) == region]
        if region_matches:
            candidates = region_matches

    deduped: dict[str, dict[str, str]] = {}
    for row in candidates:
        deduped[row["municipality_key"]] = row
    return list(deduped.values())


def _select_municipality_match(alias: str, matches: list[dict[str, str]]) -> dict[str, str] | None:
    if len(matches) == 1:
        return matches[0]

    exact_canonical_matches = [
        row
        for row in matches
        if normalize_municipality_value(row.get("municipality_normalized_value")) == alias
    ]
    if len(exact_canonical_matches) == 1:
        return exact_canonical_matches[0]

    return None


def resolve_municipality(municipality_raw: str | None, region: str | None) -> dict[str, str]:
    region_norm = normalize_region_value(region)
    municipality_norm = normalize_municipality_value(municipality_raw)
    if not municipality_norm:
        return {
            "municipality_key": "",
            "municipality_normalized_value": "",
            "municipality_raw": clean(municipality_raw),
        }

    parts = [part for part in municipality_norm.split(" - ") if part]
    for size in range(len(parts), 0, -1):
        alias = " - ".join(parts[:size])
        matches = _matching_alias_rows(alias, region_norm)
        match = _select_municipality_match(alias, matches)
        if match:
            return {
                "municipality_key": match["municipality_key"],
                "municipality_normalized_value": match["municipality_normalized_value"],
                "municipality_raw": clean(municipality_raw),
            }

    matches = _matching_alias_rows(municipality_norm, region_norm)
    match = _select_municipality_match(municipality_norm, matches)
    if match:
        return {
            "municipality_key": match["municipality_key"],
            "municipality_normalized_value": match["municipality_normalized_value"],
            "municipality_raw": clean(municipality_raw),
        }

    return {
        "municipality_key": "",
        "municipality_normalized_value": municipality_norm,
        "municipality_raw": clean(municipality_raw),
    }


def load_db_municipality_normalized_lookup(conn) -> dict[str, str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT municipality_key, municipality_normalized_value
        FROM public.municipality_normalized_name
        WHERE municipality_key IS NOT NULL
          AND municipality_normalized_value IS NOT NULL
        """
    )
    rows = cur.fetchall()
    cur.close()
    return {
        clean(municipality_key): clean(municipality_normalized_value)
        for municipality_key, municipality_normalized_value in rows
        if municipality_key is not None and municipality_normalized_value is not None
    }


def enrich_events_with_municipalities(
    events: Iterable[dict],
    db_normalized_lookup: dict[str, str] | None = None,
) -> list[dict]:
    enriched: list[dict] = []
    for event in events:
        municipality_info = resolve_municipality(event.get("municipality", ""), event.get("region", ""))
        row = dict(event)
        row["municipality_key"] = municipality_info["municipality_key"]
        row["municipality_raw"] = municipality_info["municipality_raw"]
        row["municipality_normalized_value"] = (
            (db_normalized_lookup or {}).get(municipality_info["municipality_key"])
            or municipality_info["municipality_normalized_value"]
        )
        enriched.append(row)
    return enriched


def compute_days_burning(start: str | None, scraped_on: date | None = None) -> str:
    text = clean(start)
    if not text:
        return ""
    try:
        start_date = datetime.strptime(text, "%d/%m/%Y").date()
    except ValueError:
        return ""
    current_date = scraped_on or date.today()
    if start_date > current_date:
        return ""
    return str((current_date - start_date).days + 1)


def compute_status_updated_at(raw: str | None, scraped_at: datetime) -> str:
    text = clean(raw)
    if not text:
        return ""

    match = re.search(
        r"Τελευταία\s+Ενημέρωση\s+πριν\s+από\s+(\d+)\s+"
        r"(δευτερόλεπτα|δευτερόλεπτο|λεπτά|λεπτό|ώρες|ώρα|ημέρες|ημέρα)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""

    amount = int(match.group(1))
    unit = strip_accents(match.group(2))

    if unit.startswith("ΔΕΥΤΕΡΟ"):
        delta = timedelta(seconds=amount)
    elif unit.startswith("ΛΕΠ"):
        delta = timedelta(minutes=amount)
    elif unit.startswith("ΩΡ"):
        delta = timedelta(hours=amount)
    elif unit.startswith("ΗΜΕΡ"):
        delta = timedelta(days=amount)
    else:
        return ""

    return (scraped_at - delta).isoformat(timespec="seconds")


def normalize_identity_value(value: str | None) -> str:
    return clean(strip_accents(value or ""))


def build_incident_base(row: dict) -> str:
    return "||".join(
        [
            normalize_identity_value(row.get("category")),
            normalize_region_value(row.get("region")),
            clean(row.get("municipality_key")),
            normalize_municipality_value(row.get("municipality_normalized_value") or row.get("municipality_raw")),
            normalize_municipality_value(row.get("municipality_raw")),
            normalize_identity_value(row.get("fuel_type")),
        ]
    )


def build_incident_key(base: str, start: str | None) -> str:
    payload = f"{base}||{clean(start) or 'NO_START'}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def parse_iso_datetime(value: str | None) -> datetime:
    text = clean(value)
    if not text:
        return datetime.min
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.min


def materialize_event_row(event: dict, scraped_at: datetime) -> dict[str, str]:
    row = dict(event)
    row["last_seen_at"] = scraped_at.isoformat(timespec="seconds")
    row["days_burning"] = compute_days_burning(row.get("start"), scraped_at.date())
    row["status_updated_at"] = compute_status_updated_at(row.get("raw"), scraped_at)
    return row


def normalize_existing_incident_row(row: dict) -> dict[str, str]:
    normalized_row: dict[str, str] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            normalized_row[str(key)] = value.replace(microsecond=0).isoformat()
        elif isinstance(value, date):
            normalized_row[str(key)] = value.strftime("%d/%m/%Y")
        elif isinstance(value, bool):
            normalized_row[str(key)] = "true" if value else "false"
        else:
            normalized_row[str(key)] = clean(value)

    base = build_incident_base(normalized_row)
    normalized_row["incident_key"] = clean(normalized_row.get("incident_key")) or build_incident_key(
        base,
        normalized_row.get("start"),
    )
    observed_at = clean(normalized_row.get("last_seen_at")) or clean(normalized_row.get("first_seen_at"))
    normalized_row["first_seen_at"] = clean(normalized_row.get("first_seen_at")) or observed_at
    normalized_row["last_seen_at"] = clean(normalized_row.get("last_seen_at")) or observed_at
    normalized_row["is_current"] = clean(normalized_row.get("is_current")).lower() or "false"
    return normalized_row


def choose_existing_incident_key(base: str, candidates: list[dict[str, str]]) -> str:
    if not candidates:
        return build_incident_key(base, "")
    ranked = sorted(
        candidates,
        key=lambda row: (
            parse_iso_datetime(row.get("last_seen_at")),
            parse_iso_datetime(row.get("status_updated_at")),
            parse_iso_datetime(row.get("first_seen_at")),
        ),
        reverse=True,
    )
    return clean(ranked[0].get("incident_key")) or build_incident_key(base, ranked[0].get("start"))


def merge_with_existing(
    current_events: list[dict],
    existing_rows: list[dict],
    scraped_at: datetime | None = None,
) -> list[dict[str, str]]:
    scrape_time = scraped_at or datetime.now()
    normalized_existing_rows = [normalize_existing_incident_row(row) for row in existing_rows]
    existing_by_key = {
        row["incident_key"]: dict(row)
        for row in normalized_existing_rows
        if clean(row.get("incident_key"))
    }
    existing_by_base: dict[str, list[dict[str, str]]] = {}
    for row in normalized_existing_rows:
        existing_by_base.setdefault(build_incident_base(row), []).append(row)

    merged_by_key = {key: dict(row) for key, row in existing_by_key.items()}
    current_keys: set[str] = set()

    for event in current_events:
        row = materialize_event_row(event, scrape_time)
        base = build_incident_base(row)
        start = clean(row.get("start"))
        incident_key = (
            build_incident_key(base, start)
            if start
            else choose_existing_incident_key(base, existing_by_base.get(base, []))
        )
        current_keys.add(incident_key)

        existing = merged_by_key.get(incident_key, {})
        merged = dict(existing)
        merged.update(row)
        observed_at = clean(row.get("last_seen_at")) or scrape_time.isoformat(timespec="seconds")
        merged["incident_key"] = incident_key
        merged["first_seen_at"] = clean(existing.get("first_seen_at")) or clean(existing.get("last_seen_at")) or observed_at
        merged["last_seen_at"] = observed_at
        merged["is_current"] = "true"
        merged_by_key[incident_key] = merged

    for incident_key, row in list(merged_by_key.items()):
        if incident_key not in current_keys:
            row["is_current"] = "false"
            observed_at = clean(row.get("last_seen_at")) or clean(row.get("first_seen_at"))
            row["first_seen_at"] = clean(row.get("first_seen_at")) or observed_at
            row["last_seen_at"] = clean(row.get("last_seen_at")) or observed_at
            merged_by_key[incident_key] = row

    return sorted(
        merged_by_key.values(),
        key=lambda row: (
            clean(row.get("is_current")).lower() == "true",
            parse_iso_datetime(row.get("last_seen_at")),
            parse_iso_datetime(row.get("status_updated_at")),
            datetime.strptime(row["start"], "%d/%m/%Y") if clean(row.get("start")) else datetime.min,
            clean(row.get("incident_key")),
        ),
        reverse=True,
    )


def fetch(url: str = URL, timeout: int = 30) -> str:
    session = requests.Session()
    session.headers.update(HEADERS)
    current_url = url

    # The public page currently embeds the live incidents in an iframe.
    for _ in range(2):
        resp = session.get(current_url, timeout=timeout)
        resp.raise_for_status()
        # The page is served as UTF-8; enforce it to avoid mojibake.
        resp.encoding = resp.encoding or resp.apparent_encoding or "utf-8"
        html = resp.text
        soup = make_soup(html)
        iframe = soup.find("iframe", src=True)
        if not iframe or "symvanta" not in iframe["src"].lower():
            return html, resp.url
        current_url = urljoin(resp.url, iframe["src"])

    return html, resp.url


def make_soup(html: str) -> BeautifulSoup:
    for parser in ("lxml", "html.parser"):
        try:
            return BeautifulSoup(html, parser)
        except FeatureNotFound:
            continue
    return BeautifulSoup(html, "html.parser")


def _header_map(header_cells: list[str]) -> dict[str, int]:
    """Map semantic field names to column indexes using header text heuristics."""
    mapping: dict[str, int] = {}
    for i, h in enumerate(header_cells):
        norm = strip_accents(h)
        if any(w in norm for w in ("ΚΑΤΗΓΟΡΙΑ", "ΕΙΔΟΣ")):
            mapping.setdefault("category", i)
        elif any(w in norm for w in ("ΝΟΜΟΣ", "ΠΕΡΙΦ. ΕΝΟΤΗΤΑ", "ΠΕΡΙΦΕΡΕΙΑΚΗ")):
            mapping.setdefault("regional_unit", i)
        elif any(w in norm for w in ("ΠΕΡΙΦΕΡΕΙΑ", "REGION")):
            mapping.setdefault("region", i)
        elif any(w in norm for w in ("ΔΗΜΟΣ",)):
            mapping.setdefault("municipality", i)
        elif any(w in norm for w in ("ΤΟΠΟΘΕΣΙΑ", "ΤΟΠΟΣ", "ΠΕΡΙΟΧΗ")):
            mapping.setdefault("fuel_type", i)
        elif any(w in norm for w in ("ΕΝΑΡΞΗ", "ΗΜ/ΝΙΑ", "ΗΜΕΡΟΜΗΝΙΑ", "ΩΡΑ")):
            mapping.setdefault("start", i)
        elif any(w in norm for w in ("ΚΑΤΑΣΤΑΣΗ", "STATUS")):
            mapping.setdefault("status", i)
        elif "ΣΥΜΒΑΝ" in norm:
            mapping.setdefault("category", i)
    return mapping


STATUS_BY_PANEL_CLASS = {
    "panel-red": "ΣΕ ΕΞΕΛΙΞΗ",
    "panel-yellow": "ΜΕΡΙΚΟΣ ΕΛΕΓΧΟΣ",
    "panel-green": "ΠΛΗΡΗΣ ΕΛΕΓΧΟΣ",
    "bg-info": "ΛΗΞΗ",
}


def parse_status_label(text: str) -> str:
    cleaned = clean(text)
    if not cleaned:
        return ""
    m = re.match(r"^([A-ZΑ-ΩΆ-Ώ\s]+?)\s*\(\d+\)$", cleaned)
    if m:
        return clean(m.group(1))
    return ""


def parse_tabbed_events(soup: BeautifulSoup) -> list[dict]:
    events: list[dict] = []

    for section in soup.select("div.tabcontent"):
        heading = section.find(["h2", "h3", "h4"])
        category = clean(heading.get_text(" ", strip=True)) if heading else ""
        if not category:
            continue

        current_status = ""
        for child in section.children:
            if isinstance(child, NavigableString):
                status = parse_status_label(str(child))
                if status:
                    current_status = status
                continue
            if not isinstance(child, Tag):
                continue
            if child.name == "h2" or child.name == "h3" or child.name == "h4":
                continue

            block_status = current_status or infer_status_from_block(child)
            event = parse_tabbed_event_block(child, category, block_status)
            if event:
                events.append(event)

    return events


def infer_status_from_block(block: Tag) -> str:
    classes = set(block.get("class", []))
    for class_name, status in STATUS_BY_PANEL_CLASS.items():
        if class_name in classes:
            return status
    return ""


def parse_tabbed_event_block(block: Tag, category: str, status: str) -> dict | None:
    panel_heading = block.find("div", class_="panel-heading") if block.name == "div" else None
    container = panel_heading or block
    table = container.find("table")
    if not table:
        return None

    tds = table.find_all("td")
    primary = tds[0] if tds else None
    timing = tds[1] if len(tds) > 1 else None
    if primary is None:
        return None

    primary_lines = [clean(s) for s in primary.stripped_strings if clean(s)]
    region = ""
    municipality = ""
    fuel_type = ""
    if primary_lines:
        region = re.sub(r"^ΠΕΡΙΦΕΡΕΙΑ\s+", "", primary_lines[0], flags=re.IGNORECASE)
    if len(primary_lines) > 1:
        municipality = re.sub(r"^Δ\.\s*", "", primary_lines[1], flags=re.IGNORECASE)
    if len(primary_lines) > 2:
        fuel_type = " / ".join(primary_lines[2:])

    timing_text = clean(timing.get_text(" ", strip=True)) if timing else ""
    start_match = re.search(r"ΕΝΑΡΞΗ\s+(\d{2}/\d{2}/\d{4})", timing_text)
    start = start_match.group(1) if start_match else ""

    raw = clean(container.get_text(" ", strip=True))
    return {
        "category": category,
        "region": region,
        "regional_unit": "",
        "municipality": municipality,
        "fuel_type": fuel_type,
        "start": start,
        "status": status,
        "raw": raw,
    }


def parse_events(html: str) -> list[dict]:
    """Parse every event row from the page and return a list of dicts.

    The page has historically been rendered as an HTML <table>. This parser
    tries the table first; if no table is present it falls back to any
    repeating card-style elements that look event-like.
    """
    soup = make_soup(html)
    events: list[dict] = []

    # --- Strategy 0: current tabbed incident layout ------------------------
    tabbed_events = parse_tabbed_events(soup)
    if tabbed_events:
        return tabbed_events

    # --- Strategy 1: HTML tables -------------------------------------------
    for table in soup.find_all("table"):
        headers = [clean(th.get_text()) for th in table.find_all("th")]
        if not headers:
            # Some tables put the header in the first row as <td>
            first_row = table.find("tr")
            if first_row:
                headers = [clean(td.get_text()) for td in first_row.find_all(["td", "th"])]
        hmap = _header_map(headers)
        if (
            not hmap
            or "category" not in hmap
            and "fuel_type" not in hmap
            and "municipality" not in hmap
        ):
            continue  # not an events table

        for tr in table.find_all("tr")[1:]:
            cells = [clean(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            if not cells or all(not c for c in cells):
                continue
            row = {
                "category":       cells[hmap["category"]]       if "category"       in hmap and hmap["category"]       < len(cells) else "",
                "region":         cells[hmap["region"]]         if "region"         in hmap and hmap["region"]         < len(cells) else "",
                "regional_unit":  cells[hmap["regional_unit"]]  if "regional_unit"  in hmap and hmap["regional_unit"]  < len(cells) else "",
                "municipality":   cells[hmap["municipality"]]   if "municipality"   in hmap and hmap["municipality"]   < len(cells) else "",
                "fuel_type":      cells[hmap["fuel_type"]]      if "fuel_type"      in hmap and hmap["fuel_type"]      < len(cells) else "",
                "start":          cells[hmap["start"]]          if "start"          in hmap and hmap["start"]          < len(cells) else "",
                "status":         cells[hmap["status"]]         if "status"         in hmap and hmap["status"]         < len(cells) else "",
                "raw":            " | ".join(cells),
            }
            events.append(row)

    if events:
        return events

    # --- Strategy 2: repeating card / list items ---------------------------
    # Fallback for when the site renders each event as a <div>/<li> block.
    candidates = soup.select(
        "div.views-row, li.views-row, article, div.event, div.incident, "
        "div.symvan, div[class*='event'], div[class*='symvan']"
    )
    seen_raw: set[str] = set()
    for el in candidates:
        text = clean(el.get_text(" ", strip=True))
        if not text or text in seen_raw:
            continue
        seen_raw.add(text)
        row = {
            "category": "",
            "region": "",
            "regional_unit": "",
            "municipality": "",
            "fuel_type": "",
            "start": "",
            "status": "",
            "raw": text,
        }

        # Try to pick well-known labels out of the block.
        for label_key, field in (
            ("Κατηγορία", "category"),
            ("Είδος", "category"),
            ("Περιφέρεια", "region"),
            ("Περιφερειακή", "regional_unit"),
            ("Νομός", "regional_unit"),
            ("Δήμος", "municipality"),
            ("Τοποθεσία", "fuel_type"),
            ("Τοπος", "fuel_type"),
            ("Ημ", "start"),
            ("Ώρα", "start"),
            ("Έναρξη", "start"),
            ("Κατάσταση", "status"),
        ):
            m = re.search(
                rf"{label_key}[^A-Za-zΑ-Ωα-ω0-9]{{0,4}}([^|•·\n]+?)(?=\s{{2,}}|$|\||•|·|\n)",
                text,
            )
            if m and not row[field]:
                row[field] = clean(m.group(1))
        events.append(row)

    return events


def filter_forest(events: Iterable[dict]) -> list[dict]:
    return [e for e in events if is_forest_fire(e.get("category", "") or e.get("raw", ""))]


def ensure_current_fires_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CURRENT_FIRES_TABLE} (
          incident_key TEXT PRIMARY KEY,
          first_seen_at TIMESTAMPTZ NOT NULL,
          last_seen_at TIMESTAMPTZ NOT NULL,
          is_current BOOLEAN NOT NULL DEFAULT TRUE,
          category TEXT NOT NULL,
          region TEXT,
          regional_unit TEXT,
          municipality_key TEXT,
          municipality_normalized_value TEXT,
          municipality_raw TEXT,
          fuel_type TEXT,
          start_date DATE,
          days_burning INTEGER,
          status_updated_at TIMESTAMPTZ,
          status TEXT,
          raw TEXT
        )
        """
    )
    cur.execute(
        f"""
        ALTER TABLE {CURRENT_FIRES_TABLE}
          DROP COLUMN IF EXISTS scraped_at,
          DROP COLUMN IF EXISTS created_at,
          DROP COLUMN IF EXISTS updated_at
        """
    )
    cur.execute(f"ALTER TABLE {CURRENT_FIRES_TABLE} ENABLE ROW LEVEL SECURITY")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_current_fires_is_current ON {CURRENT_FIRES_TABLE} (is_current)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_current_fires_municipality_key ON {CURRENT_FIRES_TABLE} (municipality_key)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_current_fires_status ON {CURRENT_FIRES_TABLE} (status)")
    conn.commit()
    cur.close()


def load_existing_incidents_db(conn) -> list[dict[str, str]]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        f"""
        SELECT
          incident_key,
          first_seen_at,
          last_seen_at,
          is_current,
          category,
          region,
          regional_unit,
          municipality_key,
          municipality_normalized_value,
          municipality_raw,
          fuel_type,
          start_date AS start,
          days_burning,
          status_updated_at,
          status,
          raw
        FROM {CURRENT_FIRES_TABLE}
        """
    )
    rows = list(cur.fetchall())
    cur.close()
    return [dict(row) for row in rows]


def parse_iso_datetime_or_none(value: str | None) -> datetime | None:
    text = clean(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def parse_start_date_or_none(value: str | None) -> date | None:
    text = clean(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%d/%m/%Y").date()
    except ValueError:
        return None


def parse_int_or_none(value: str | None) -> int | None:
    text = clean(value)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def upsert_current_fires(conn, rows: list[dict[str, str]]) -> None:
    payload = [
        (
            clean(row.get("incident_key")),
            parse_iso_datetime_or_none(row.get("first_seen_at")),
            parse_iso_datetime_or_none(row.get("last_seen_at")),
            clean(row.get("is_current")).lower() == "true",
            clean(row.get("category")),
            clean(row.get("region")) or None,
            clean(row.get("regional_unit")) or None,
            clean(row.get("municipality_key")) or None,
            clean(row.get("municipality_normalized_value")) or None,
            clean(row.get("municipality_raw")) or None,
            clean(row.get("fuel_type")) or None,
            parse_start_date_or_none(row.get("start")),
            parse_int_or_none(row.get("days_burning")),
            parse_iso_datetime_or_none(row.get("status_updated_at")),
            clean(row.get("status")) or None,
            clean(row.get("raw")) or None,
        )
        for row in rows
    ]

    cur = conn.cursor()
    execute_values(
        cur,
        f"""
        INSERT INTO {CURRENT_FIRES_TABLE} (
          incident_key,
          first_seen_at,
          last_seen_at,
          is_current,
          category,
          region,
          regional_unit,
          municipality_key,
          municipality_normalized_value,
          municipality_raw,
          fuel_type,
          start_date,
          days_burning,
          status_updated_at,
          status,
          raw
        ) VALUES %s
        ON CONFLICT (incident_key) DO UPDATE SET
          first_seen_at = EXCLUDED.first_seen_at,
          last_seen_at = EXCLUDED.last_seen_at,
          is_current = EXCLUDED.is_current,
          category = EXCLUDED.category,
          region = EXCLUDED.region,
          regional_unit = EXCLUDED.regional_unit,
          municipality_key = EXCLUDED.municipality_key,
          municipality_normalized_value = EXCLUDED.municipality_normalized_value,
          municipality_raw = EXCLUDED.municipality_raw,
          fuel_type = EXCLUDED.fuel_type,
          start_date = EXCLUDED.start_date,
          days_burning = EXCLUDED.days_burning,
          status_updated_at = EXCLUDED.status_updated_at,
          status = EXCLUDED.status,
          raw = EXCLUDED.raw
        """,
        payload,
    )
    conn.commit()
    cur.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Scrape active forest fires from fireservice.gr")
    p.add_argument("--all", action="store_true", help="Keep every category, not only forest fires")
    p.add_argument("--url", default=URL, help="Source URL (override for testing)")
    p.add_argument("--db-path", type=str, default=None, help="Optional DATABASE_URL override")
    args = p.parse_args(argv)

    try:
        html, _ = fetch(args.url)
    except requests.RequestException as exc:
        print(f"ERROR: failed to fetch {args.url}: {exc}", file=sys.stderr)
        return 2

    try:
        db_url = resolve_database_url(args.db_path)
        conn = psycopg2.connect(db_url)
    except Exception as exc:
        print(f"ERROR: failed to connect to database: {exc}", file=sys.stderr)
        return 3

    try:
        ensure_current_fires_table(conn)
        normalized_name_lookup = load_db_municipality_normalized_lookup(conn)
        all_events = enrich_events_with_municipalities(parse_events(html), normalized_name_lookup)
        current_events = all_events if args.all else filter_forest(all_events)
        existing_rows = load_existing_incidents_db(conn)
        events = merge_with_existing(current_events, existing_rows)
        upsert_current_fires(conn, events)
    finally:
        conn.close()

    print(f"Fetched {len(all_events)} total event(s); kept {len(current_events)} after filtering.")
    print(f"Stored {len(events)} cumulative incident(s).")
    print(f"Upserted into: {CURRENT_FIRES_TABLE}")

    if not all_events:
        print(
            "WARNING: no events were parsed. The site layout may have changed — "
            "inspect the HTML and adjust parse_events() accordingly.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
