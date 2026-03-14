from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingest.stage2_load_erd import t_up
from src.map_copernicus_to_municipalities import resolve_database_url


ORG_MAP_CSV = ROOT / "data" / "mappings" / "org_to_municipality.csv"
EXPANDED_CSV = ROOT / "data" / "mappings" / "final_entity_mapping_expanded.csv"


def _norm(value: str | None) -> str:
    return t_up(value) or ""


def load_scope_lookup() -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    by_source_key: dict[str, set[str]] = defaultdict(set)
    by_value: dict[str, set[str]] = defaultdict(set)
    with ORG_MAP_CSV.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            authority_level = (row.get("authority_level") or "").strip()
            org_type = (row.get("org_type") or "").strip()
            org_name_clean = (row.get("org_name_clean") or "").strip()
            if not authority_level or not org_name_clean:
                continue
            if org_type:
                by_source_key[_norm(f"{org_type}::{org_name_clean}")].add(authority_level)
                by_value[_norm(f"{org_type} {org_name_clean}")].add(authority_level)
            by_value[_norm(org_name_clean)].add(authority_level)
    return by_source_key, by_value


def load_expanded_national_source_keys() -> set[str]:
    coverage_by_source_key: dict[str, set[str]] = defaultdict(set)
    with EXPANDED_CSV.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if (row.get("source_entity_type") or "").strip() != "organization":
                continue
            source_key = (row.get("source_key") or "").strip()
            municipality_key = (row.get("municipality_id") or "").strip()
            if source_key and municipality_key:
                coverage_by_source_key[source_key].add(municipality_key)
    if not coverage_by_source_key:
        return set()
    max_coverage = max(len(values) for values in coverage_by_source_key.values())
    return {
        source_key
        for source_key, values in coverage_by_source_key.items()
        if len(values) >= max_coverage - 1
    }


def build_updates(conn) -> list[tuple[str, str]]:
    by_source_key, by_value = load_scope_lookup()
    expanded_national_source_keys = load_expanded_national_source_keys()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT organization_key, organization_value, organization_normalized_value, source_key, authority_scope
        FROM public.organization
        """
    )
    updates: list[tuple[str, str]] = []
    for organization_key, organization_value, organization_normalized_value, source_key, authority_scope in cur.fetchall():
        if str(authority_scope or "").strip():
            continue
        matches: list[set[str]] = []
        source_key_norm = _norm(source_key)
        value_norm = _norm(organization_value)
        normalized_norm = _norm(organization_normalized_value)
        if source_key_norm:
            matches.append(by_source_key.get(source_key_norm, set()))
        if value_norm:
            matches.append(by_value.get(value_norm, set()))
        if normalized_norm:
            matches.append(by_value.get(normalized_norm, set()))
        resolved = None
        for candidate in matches:
            if len(candidate) == 1:
                resolved = next(iter(candidate))
                break
        if resolved is None and source_key and str(source_key).strip() in expanded_national_source_keys:
            resolved = "national"
        if resolved:
            updates.append((resolved, organization_key))
    cur.close()
    return updates


def main() -> None:
    conn = psycopg2.connect(resolve_database_url(None))
    updates = build_updates(conn)
    cur = conn.cursor()
    execute_batch(
        cur,
        """
        UPDATE public.organization
        SET authority_scope = %s,
            updated_at = NOW()
        WHERE organization_key = %s
        """,
        updates,
        page_size=500,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"updated_rows={len(updates)}")


if __name__ == "__main__":
    main()
