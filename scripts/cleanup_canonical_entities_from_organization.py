from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from municipality_normalization import normalizeMunicipality
from src.map_copernicus_to_municipalities import resolve_database_url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove canonical municipality/region entities from public.organization while keeping decentralized organizations.",
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Default is dry-run.")
    return parser.parse_args()


def _norm(value: str | None) -> str:
    return " ".join(str(value or "").strip().upper().split())


def _region_aliases(rows: list[tuple[str, str]]) -> set[str]:
    aliases: set[str] = set()
    for region_key, region_value in rows:
        for candidate in (region_key, region_value):
            norm = _norm(candidate)
            if not norm:
                continue
            aliases.add(norm)
            aliases.add(_norm(f"ΠΕΡΙΦΕΡΕΙΑ {norm}"))
    return aliases


def _municipality_aliases(rows: list[tuple[str, str]]) -> set[str]:
    aliases: set[str] = set()
    for municipality_key, municipality_value in rows:
        for candidate in (municipality_key, municipality_value, normalizeMunicipality(municipality_value)):
            norm = _norm(candidate)
            if not norm:
                continue
            aliases.add(norm)
            aliases.add(_norm(f"ΔΗΜΟΣ {norm}"))
    return aliases


def classify_cleanup_keys(conn) -> tuple[set[str], set[str]]:
    cur = conn.cursor()
    cur.execute("SELECT region_key, region_value FROM public.region")
    region_rows = [(str(a or ""), str(b or "")) for a, b in cur.fetchall()]
    cur.execute("SELECT municipality_key, municipality_value FROM public.municipality")
    municipality_rows = [(str(a or ""), str(b or "")) for a, b in cur.fetchall()]
    cur.execute(
        """
        SELECT organization_key, organization_value, organization_normalized_value, authority_scope, source_key
        FROM public.organization
        """
    )
    org_rows = cur.fetchall()
    cur.close()

    region_aliases = _region_aliases(region_rows)
    municipality_aliases = _municipality_aliases(municipality_rows)

    municipality_keys: set[str] = set()
    region_keys: set[str] = set()

    for organization_key, organization_value, organization_normalized_value, authority_scope, source_key in org_rows:
        org_key = str(organization_key or "").strip()
        if not org_key:
            continue

        value_norms = {
            _norm(organization_value),
            _norm(organization_normalized_value),
        }
        value_norms.discard("")
        source_key_norm = _norm(source_key)
        scope = _norm(authority_scope)

        if source_key_norm.startswith("ΔΗΜΟΣ::") or scope == "MUNICIPALITY" or any(v in municipality_aliases for v in value_norms):
            municipality_keys.add(org_key)
            continue

        if source_key_norm.startswith("ΠΕΡΙΦΕΡΕΙΑ::"):
            region_keys.add(org_key)
            continue

        if scope == "REGION" and any(v in region_aliases for v in value_norms):
            region_keys.add(org_key)
            continue

    return municipality_keys, region_keys


def count_refs(cur, key_set: set[str], table: str) -> int:
    if not key_set:
        return 0
    cur.execute(
        f"SELECT COUNT(*) FROM public.{table} WHERE organization_key = ANY(%s)",
        (list(key_set),),
    )
    return int(cur.fetchone()[0])


def main() -> None:
    args = parse_args()
    db_url = resolve_database_url(None)
    conn = psycopg2.connect(db_url)
    municipality_keys, region_keys = classify_cleanup_keys(conn)
    cleanup_keys = municipality_keys | region_keys

    cur = conn.cursor()
    summary = {
        "municipality_org_keys_to_remove": len(municipality_keys),
        "region_org_keys_to_remove": len(region_keys),
        "total_org_keys_to_remove": len(cleanup_keys),
        "references": {
            "procurement": count_refs(cur, cleanup_keys, "procurement"),
            "diavgeia": count_refs(cur, cleanup_keys, "diavgeia"),
            "fund": count_refs(cur, cleanup_keys, "fund"),
            "org_municipality_coverage": count_refs(cur, cleanup_keys, "org_municipality_coverage"),
        },
        "applied": bool(args.apply),
    }

    if not args.apply:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        cur.close()
        conn.close()
        return

    if cleanup_keys:
        keys = list(cleanup_keys)
        for table in ("procurement", "diavgeia", "fund", "region", "municipality", "org_municipality_coverage"):
            cur.execute(
                f"""
                UPDATE public.{table}
                SET organization_key = NULL
                WHERE organization_key = ANY(%s)
                """,
                (keys,),
            )
        cur.execute(
            """
            DELETE FROM public.organization
            WHERE organization_key = ANY(%s)
            """,
            (keys,),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
