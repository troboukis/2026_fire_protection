from __future__ import annotations

import csv
import sys
from pathlib import Path
from collections import defaultdict

import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingest.stage2_load_erd import t_up
from src.map_copernicus_to_municipalities import resolve_database_url

CSV_PATH = ROOT / "data" / "mappings" / "org_to_municipality_coverage.csv"
UNRESOLVED_CSV_PATH = ROOT / "logs" / "org_municipality_coverage_unresolved.csv"


def _norm(value: str | None) -> str:
    return t_up(value) or ""


def load_organization_lookup(conn) -> dict[str, dict[str, set[str]]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT organization_key, organization_value, organization_normalized_value, source_key
        FROM public.organization
        """
    )
    by_source_key: dict[str, set[str]] = defaultdict(set)
    by_value: dict[str, set[str]] = defaultdict(set)
    for organization_key, organization_value, organization_normalized_value, source_key in cur.fetchall():
        key = str(organization_key).strip()
        if not key:
            continue
        for candidate in (_norm(source_key),):
            if candidate:
                by_source_key[candidate].add(key)
        for candidate in (_norm(organization_value), _norm(organization_normalized_value)):
            if candidate:
                by_value[candidate].add(key)
    cur.close()
    return {
        "by_source_key": by_source_key,
        "by_value": by_value,
    }


def resolve_organization_key(
    org_type: str,
    org_name_clean: str,
    lookup: dict[str, dict[str, set[str]]],
) -> str | None:
    composite_source_key = _norm(f"{org_type}::{org_name_clean}") if org_type else ""
    full_label = _norm(f"{org_type} {org_name_clean}") if org_type else ""
    raw_name = _norm(org_name_clean)

    candidates: list[set[str]] = []
    if composite_source_key:
        candidates.append(lookup["by_source_key"].get(composite_source_key, set()))
    if full_label:
        candidates.append(lookup["by_value"].get(full_label, set()))
    if raw_name:
        candidates.append(lookup["by_value"].get(raw_name, set()))

    for matches in candidates:
        if len(matches) == 1:
            return next(iter(matches))
    return None


def build_rows(conn) -> tuple[list[tuple[str, str | None, str, str | None, str | None, str | None, str]], list[dict[str, str]]]:
    lookup = load_organization_lookup(conn)
    deduped: dict[tuple[str, str], tuple[str, str | None, str, str | None, str | None, str | None, str]] = {}
    unresolved: list[dict[str, str]] = []
    with CSV_PATH.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            org_name = (row.get("org_name_clean") or "").strip()
            org_type = (row.get("org_type") or "").strip()
            municipality_key = (row.get("municipality_id") or "").strip()
            if not org_name or not municipality_key:
                continue
            org_key = resolve_organization_key(org_type, org_name, lookup)
            if not org_key:
                unresolved.append({
                    "org_type": org_type,
                    "org_name_clean": org_name,
                    "region_id": (row.get("region_id") or "").strip(),
                    "municipality_id": municipality_key,
                    "municipality_name": (row.get("municipality_name") or "").strip(),
                    "authority_level": (row.get("authority_level") or "").strip(),
                    "coverage_method": (row.get("coverage_method") or "").strip(),
                })
                continue
            region_key = (row.get("region_id") or "").strip() or None
            authority_scope = (row.get("authority_level") or "").strip() or None
            coverage_method = (row.get("coverage_method") or "").strip() or None
            source_org_type = org_type or None
            deduped.setdefault(
                (org_key, municipality_key),
                (
                    org_key,
                    region_key,
                    municipality_key,
                    authority_scope,
                    coverage_method,
                    source_org_type,
                    org_name,
                ),
            )
    return list(deduped.values()), unresolved


def write_unresolved_report(rows: list[dict[str, str]]) -> None:
    UNRESOLVED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "org_type",
        "org_name_clean",
        "region_id",
        "municipality_id",
        "municipality_name",
        "authority_level",
        "coverage_method",
    ]
    with UNRESOLVED_CSV_PATH.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    db_url = resolve_database_url(None)
    conn = psycopg2.connect(db_url)
    rows, unresolved = build_rows(conn)
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE public.org_municipality_coverage")
    execute_values(
        cur,
        """
        INSERT INTO public.org_municipality_coverage (
          organization_key,
          region_key,
          municipality_key,
          authority_scope,
          coverage_method,
          source_org_type,
          source_org_name
        ) VALUES %s
        ON CONFLICT (organization_key, municipality_key) DO UPDATE SET
          region_key = EXCLUDED.region_key,
          authority_scope = EXCLUDED.authority_scope,
          coverage_method = EXCLUDED.coverage_method,
          source_org_type = EXCLUDED.source_org_type,
          source_org_name = EXCLUDED.source_org_name,
          updated_at = NOW()
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()
    write_unresolved_report(unresolved)

    print(f"loaded_rows={len(rows)}")
    print(f"unresolved_rows={len(unresolved)}")
    print(f"unresolved_report={UNRESOLVED_CSV_PATH}")


if __name__ == "__main__":
    main()
