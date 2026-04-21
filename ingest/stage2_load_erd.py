"""
Stage 2 loader for ERD schema.

Loads these sources into the new ERD tables:
- data/raw_procurements.csv -> procurement (+ cpv)
- data/2026_diavgeia.csv -> diavgeia, payment, bridge tables
- data/fires/fire_incidents_unified.csv -> forest_fire
- data/funding/municipal_funding.csv -> fund
- data/mappings/org_to_municipality.csv + region_to_municipalities.csv -> municipality/region seed + mappings

Usage:
  .fireprotection/bin/python ingest/stage2_load_erd.py --dry-run
  .fireprotection/bin/python ingest/stage2_load_erd.py
"""

from __future__ import annotations

import argparse
import ast
from collections import Counter
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import runpy

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from municipality_normalization import normalizeMunicipality
from src.map_copernicus_to_municipalities import resolve_database_url

load_dotenv()

_kimdis_module = runpy.run_path(str(REPO / "src" / "fetch_kimdis_procurements.py"))
DEFAULT_CPVS: dict[str, str] = _kimdis_module["DEFAULT_CPVS"]
DEFAULT_EXCLUDE_KEYWORDS: list[str] = _kimdis_module["DEFAULT_EXCLUDE_KEYWORDS"]

RAW_CSV = REPO / "data" / "raw_procurements.csv"
DIAV_CSV = REPO / "data" / "2026_diavgeia.csv"
FIRE_CSV = REPO / "data" / "fires" / "fire_incidents_unified.csv"
FUND_CSV = REPO / "data" / "funding" / "municipal_funding.csv"
ORG_MAP_CSV = REPO / "data" / "mappings" / "org_to_municipality.csv"
REGION_MAP_CSV = REPO / "data" / "mappings" / "region_to_municipalities.csv"
EXPANDED_MAP_CSV = REPO / "data" / "mappings" / "final_entity_mapping_expanded.csv"

FIRE_KEY_COLUMNS = (
    "municipality_key",
    "region_key",
    "year",
    "date_start",
    "date_end",
    "nomos",
    "area_name",
    "lat",
    "lon",
    "burned_forest_stremata",
    "burned_woodland_stremata",
    "burned_grassland_stremata",
    "burned_grove_stremata",
    "burned_other_stremata",
    "burned_total_stremata",
    "burned_total_ha",
    "source",
)


@dataclass
class CsvBundle:
    raw: pd.DataFrame
    diav: pd.DataFrame
    fire: pd.DataFrame
    fund: pd.DataFrame
    org_map: pd.DataFrame
    region_map: pd.DataFrame
    expanded_map: pd.DataFrame


def t(val) -> str | None:
    s = str(val or "").strip()
    if not s or s.lower() in {"nan", "none", "nat"}:
        return None
    return s


def t_up(val) -> str | None:
    s = t(val)
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip().upper()


def norm_key(val) -> str | None:
    s = t_up(val)
    if s is None:
        return None
    # Normalize punctuation and separators so naming variants match reliably.
    s = re.sub(r"[^0-9A-ZΑ-Ω]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    return s


def b(val) -> bool | None:
    s = t(val)
    if s is None:
        return None
    sl = s.lower()
    if sl in {"true", "t", "1", "yes"}:
        return True
    if sl in {"false", "f", "0", "no"}:
        return False
    return None


def i(val) -> int | None:
    s = t(val)
    if s is None:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def dec(val) -> float | None:
    s = t(val)
    if s is None:
        return None
    # Greek style support: 10.925,00
    if "," in s and re.search(r"\d\.\d{3},\d{1,2}", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def date_iso(val) -> str | None:
    s = t(val)
    if s is None:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def ts_iso(val) -> str | None:
    s = t(val)
    if s is None:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return None


def parse_any_datetime(val) -> datetime | None:
    dt_iso = ts_iso(val)
    if dt_iso is not None:
        try:
            return datetime.strptime(dt_iso, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    d_iso = date_iso(val)
    if d_iso is not None:
        try:
            return datetime.strptime(d_iso, "%Y-%m-%d")
        except ValueError:
            return None

    return None


def first_list_item(val) -> str | None:
    s = t(val)
    if s is None:
        return None
    if not s.startswith("["):
        return s
    try:
        arr = ast.literal_eval(s)
        if isinstance(arr, list) and arr:
            item = arr[0]
            return t(item)
    except Exception:
        return None
    return None


def parse_serialized_list(val) -> list:
    s = t(val)
    if s is None:
        return []
    if not s.startswith("["):
        return [s]
    for loader in (json.loads, ast.literal_eval):
        try:
            parsed = loader(s)
        except Exception:
            continue
        if isinstance(parsed, list):
            return parsed
    return []


def unique_nonempty_strings(values: Iterable[str | None]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = t(value)
        if cleaned is None or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def raw_contracting_members(raw_row: pd.Series) -> list[tuple[str | None, str | None]]:
    members: list[tuple[str | None, str | None]] = []

    for item in parse_serialized_list(raw_row.get("contractingMembers_details")):
        if not isinstance(item, dict):
            continue
        member = (t(item.get("vatNumber")), t(item.get("name")))
        if member[0] is not None or member[1] is not None:
            members.append(member)

    if not members:
        afms = [t(value) for value in parse_serialized_list(raw_row.get("contractingMembers_vatNumbers"))]
        names = [t(value) for value in parse_serialized_list(raw_row.get("contractingMembers_names"))]
        for index in range(max(len(afms), len(names))):
            afm = afms[index] if index < len(afms) else None
            name = names[index] if index < len(names) else None
            if afm is not None or name is not None:
                members.append((afm, name))

    if not members:
        legacy_member = (t(raw_row.get("firstMember_vatNumber")), t(raw_row.get("firstMember_name")))
        if legacy_member[0] is not None or legacy_member[1] is not None:
            members.append(legacy_member)

    out: list[tuple[str | None, str | None]] = []
    seen: set[tuple[str | None, str | None]] = set()
    for member in members:
        if member in seen:
            continue
        seen.add(member)
        out.append(member)
    return out


def read_csvs(limit: int | None = None) -> CsvBundle:
    def read(path: Path) -> pd.DataFrame:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        if limit:
            return df.head(limit)
        return df

    return CsvBundle(
        raw=read(RAW_CSV),
        diav=read(DIAV_CSV),
        fire=read(FIRE_CSV),
        fund=read(FUND_CSV),
        org_map=read(ORG_MAP_CSV),
        region_map=read(REGION_MAP_CSV),
        expanded_map=read(EXPANDED_MAP_CSV),
    )


def apply_procurement_chain_dedup(raw: pd.DataFrame) -> pd.DataFrame:
    deduped = raw.copy()
    prev_refs = {
        ref.strip()
        for ref in deduped["prevReferenceNo"].tolist()
        if isinstance(ref, str) and ref.strip()
    }
    reference_numbers = deduped["referenceNumber"].fillna("").astype(str).str.strip()
    has_next_ref = deduped["nextRefNo"].fillna("").astype(str).str.strip() != ""
    superseded_by_prev_ref = reference_numbers.isin(prev_refs)
    deduped.loc[superseded_by_prev_ref | has_next_ref, "totalCostWithoutVAT"] = "0"
    return deduped


def affected_reference_numbers_for_row(raw_row: pd.Series) -> set[str]:
    affected: set[str] = set()
    prev_ref = t(raw_row.get("prevReferenceNo"))
    reference_number = t(raw_row.get("referenceNumber"))
    next_ref = t(raw_row.get("nextRefNo"))
    if prev_ref:
        affected.add(prev_ref)
    if reference_number and next_ref:
        affected.add(reference_number)
    return affected


def zero_superseded_payment_amounts(cur, affected_references: set[str]) -> int:
    refs = sorted(ref for ref in affected_references if ref)
    if not refs:
        return 0

    cur.execute(
        """
        UPDATE public.payment py
        SET amount_without_vat = 0
        FROM public.procurement p
        JOIN (
          SELECT unnest(%s::text[]) AS reference_number
        ) affected
          ON affected.reference_number = p.reference_number
        WHERE py.procurement_id = p.id
          AND py.amount_without_vat IS DISTINCT FROM 0
        """,
        (refs,),
    )
    return cur.rowcount


def build_maps(
    org_map: pd.DataFrame,
    expanded_map: pd.DataFrame,
) -> dict[tuple[str, str], tuple[str | None, str | None]]:
    out: dict[tuple[str, str], tuple[str | None, str | None]] = {}

    def put(
        org_type: str | None,
        org_name: str | None,
        municipality_id: str | None,
        region_id: str | None,
    ) -> None:
        if (not org_type and not org_name) or (not municipality_id and not region_id):
            return
        value = (municipality_id, region_id)

        # Exact key.
        exact_key = (org_type or "", org_name or "")
        if exact_key not in out:
            out[exact_key] = value

        # Name-only fallback.
        if org_name:
            name_only_key = ("", org_name)
            if name_only_key not in out:
                out[name_only_key] = value

        # Punctuation-normalized variants.
        norm_name = norm_key(org_name)
        norm_type = norm_key(org_type)
        if norm_name:
            norm_name_key = ("", norm_name)
            if norm_name_key not in out:
                out[norm_name_key] = value
            if norm_type:
                norm_exact_key = (norm_type, norm_name)
                if norm_exact_key not in out:
                    out[norm_exact_key] = value

    for _, r in org_map.iterrows():
        org_type = t_up(r.get("org_type")) or ""
        org_name = t_up(r.get("org_name_clean")) or ""
        value = (t(r.get("municipality_id")), t(r.get("region_id")))
        put(org_type, org_name, value[0], value[1])

    # Rich alias source: expanded mapping has many organization name variations
    # already linked to municipality_id/region_id.
    exp_org = expanded_map[expanded_map["source_entity_type"] == "organization"]
    for _, r in exp_org.iterrows():
        municipality_id = t(r.get("municipality_id"))
        region_id = t(r.get("region_id"))
        source_value = t_up(r.get("source_value"))
        normalized_value = t_up(r.get("normalized_value"))
        put("", source_value, municipality_id, region_id)
        put("", normalized_value, municipality_id, region_id)

    return out


def seed_region_rows(b: CsvBundle) -> list[tuple]:
    out: list[tuple] = []
    seen_pairs: set[tuple[str, str]] = set()

    # Canonical + observed region labels from expanded mapping.
    for _, r in b.expanded_map.iterrows():
        region_key = t(r.get("region_id"))
        if not region_key:
            continue
        region_value = t(r.get("region_id")) or region_key
        region_normalized = t_up(region_value) or region_value
        k = (region_key, region_value)
        if k in seen_pairs:
            continue
        seen_pairs.add(k)
        out.append((
            region_key,
            region_value,
            region_normalized,
            t(r.get("source_system")),
            t(r.get("source_key")),
        ))

    # Backfill from region mapping file.
    for _, r in b.region_map.iterrows():
        region_key = t(r.get("region_id"))
        if not region_key:
            continue
        region_value = t(r.get("region_id")) or region_key
        region_normalized = t_up(region_value) or region_value
        k = (region_key, region_value)
        if k in seen_pairs:
            continue
        seen_pairs.add(k)
        out.append((region_key, region_value, region_normalized, "region_to_municipalities", None))

    # Backfill from org->municipality map.
    for _, r in b.org_map.iterrows():
        region_key = t(r.get("region_id"))
        if not region_key:
            continue
        region_value = t(r.get("region_id")) or region_key
        region_normalized = t_up(region_value) or region_value
        k = (region_key, region_value)
        if k in seen_pairs:
            continue
        seen_pairs.add(k)
        out.append((region_key, region_value, region_normalized, "org_to_municipality", None))

    return out


def seed_municipality_rows(b: CsvBundle) -> list[tuple]:
    region_by_municipality: dict[str, str] = {}
    for _, r in b.region_map.iterrows():
        municipality_key = t(r.get("municipality_id"))
        region_key = t(r.get("region_id"))
        if municipality_key and region_key and municipality_key not in region_by_municipality:
            region_by_municipality[municipality_key] = region_key

    out: list[tuple] = []
    seen_pairs: set[tuple[str, str]] = set()

    # Main source for municipality variations.
    # Strict mode: no fallbacks. We only accept explicit municipality_id,
    # source_value, and normalized_value from final_entity_mapping_expanded.csv.
    muni_rows = b.expanded_map[b.expanded_map["source_entity_type"].isin(["municipality", "municipality_name"])]
    for _, r in muni_rows.iterrows():
        municipality_key = t(r.get("municipality_id"))
        municipality_value = t(r.get("source_value"))
        municipality_normalized = t(r.get("normalized_value"))
        if not municipality_key or not municipality_value or not municipality_normalized:
            continue
        k = (municipality_key, municipality_value)
        if k in seen_pairs:
            continue
        seen_pairs.add(k)
        out.append((
            municipality_key,
            municipality_value,
            municipality_normalized,
            region_by_municipality.get(municipality_key),
            t(r.get("source_system")),
            t(r.get("source_key")),
        ))

    return out


def build_region_lookup(region_seed_rows: list[tuple]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for region_key, region_value, region_norm, *_ in region_seed_rows:
        rk = t_up(region_key)
        rv = t_up(region_value)
        rn = t_up(region_norm)
        if rk:
            lookup[rk] = region_key
        if rv:
            lookup[rv] = region_key
        if rn and rn not in lookup:
            lookup[rn] = region_key
    return lookup


def build_municipality_lookup(muni_seed_rows: list[tuple]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for municipality_key, municipality_value, municipality_norm, *_ in muni_seed_rows:
        mk = t_up(municipality_key)
        mv = t_up(municipality_value)
        mn = t_up(municipality_norm)
        mval_norm = normalizeMunicipality(municipality_value)
        mnorm_norm = normalizeMunicipality(municipality_norm)
        if mk:
            lookup[mk] = municipality_key
        if mv:
            lookup[mv] = municipality_key
        if mn and mn not in lookup:
            lookup[mn] = municipality_key
        if mval_norm and mval_norm not in lookup:
            lookup[mval_norm] = municipality_key
        if mnorm_norm and mnorm_norm not in lookup:
            lookup[mnorm_norm] = municipality_key
    return lookup


def build_municipality_region_lookup(muni_seed_rows: list[tuple]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for municipality_key, _, _, region_key, *_ in muni_seed_rows:
        mk = t(municipality_key)
        rk = t(region_key)
        if mk and rk and mk not in lookup:
            lookup[mk] = rk
    return lookup


def build_municipality_alias_lookup(muni_seed_rows: list[tuple]) -> dict[str, set[str]]:
    lookup: dict[str, set[str]] = {}
    for municipality_key, municipality_value, municipality_norm, *_ in muni_seed_rows:
        mk = t(municipality_key)
        if not mk:
            continue
        aliases = lookup.setdefault(mk, set())
        for candidate in (
            municipality_value,
            municipality_norm,
            normalizeMunicipality(municipality_value),
            normalizeMunicipality(municipality_norm),
        ):
            norm = t_up(candidate)
            if norm:
                aliases.add(norm)
    return lookup


def build_org_municipality_coverage_lookup(expanded_map: pd.DataFrame) -> dict[str, list[str]]:
    lookup: dict[str, list[str]] = {}

    def add(label: str | None, municipality_key: str | None) -> None:
        norm = t_up(label)
        mk = t(municipality_key)
        if not norm or not mk:
            return
        values = lookup.setdefault(norm, [])
        if mk not in values:
            values.append(mk)

    exp_org = expanded_map[expanded_map["source_entity_type"] == "organization"]
    for _, r in exp_org.iterrows():
        municipality_key = t(r.get("municipality_id"))
        add(r.get("source_value"), municipality_key)
        add(r.get("normalized_value"), municipality_key)
    return lookup


def resolve_municipality_from_context(
    context_values: list[str | None],
    candidate_keys: list[str],
    municipality_alias_lookup: dict[str, set[str]],
) -> str | None:
    haystack = " ".join(t_up(value) or "" for value in context_values if t(value))
    if not haystack:
        return None
    matches: list[str] = []
    for municipality_key in candidate_keys:
        aliases = municipality_alias_lookup.get(municipality_key, set())
        if any(alias and len(alias) >= 4 and alias in haystack for alias in aliases):
            if municipality_key not in matches:
                matches.append(municipality_key)
    if len(matches) == 1:
        return matches[0]
    return None


def organization_key_from_normalized(normalized_value: str) -> str:
    return f"org_{hashlib.sha1(normalized_value.encode('utf-8')).hexdigest()[:20]}"


def organization_key_from_afm(afm: str) -> str:
    return f"org_afm_{afm}"


def default_organization_normalized_value(value: str | None) -> str | None:
    return t_up(value)


def authority_scope_from_notes(notes: str | None) -> str | None:
    note = t(notes) or ""
    if not note.startswith("org_type="):
        return None
    org_type = note.split("=", 1)[1].strip().upper()
    if org_type == "ΔΗΜΟΣ":
        return "municipality"
    if org_type == "ΠΕΡΙΦΕΡΕΙΑ":
        return "region"
    if org_type == "ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ":
        return "decentralized"
    if org_type == "ΥΠΟΥΡΓΕΙΟ":
        return "national"
    return None


def authority_scope_key_map(org_map: pd.DataFrame) -> dict[str, str]:
    out: dict[str, str] = {}

    def put_candidate(label: str | None, authority_scope: str) -> None:
        value = t(label)
        if not value:
            return
        org_key = organization_key_from_normalized(t_up(value) or value)
        if org_key not in out:
            out[org_key] = authority_scope

    for _, r in org_map.iterrows():
        authority_level = t(r.get("authority_level"))
        if authority_level not in {"municipality", "region", "decentralized", "national"}:
            continue
        org_type = t(r.get("org_type")) or ""
        org_name_clean = t(r.get("org_name_clean")) or ""
        put_candidate(org_name_clean, authority_level)
        if org_type and org_name_clean:
            put_candidate(f"{org_type} {org_name_clean}", authority_level)

    return out


def canonical_region_aliases(region_seed_rows: list[tuple]) -> set[str]:
    aliases: set[str] = set()
    for region_key, region_value, region_norm, *_ in region_seed_rows:
        for candidate in (region_key, region_value, region_norm):
            norm = t_up(candidate)
            if not norm:
                continue
            aliases.add(norm)
            aliases.add(t_up(f"ΠΕΡΙΦΕΡΕΙΑ {norm}") or norm)
    return aliases


def canonical_municipality_aliases(muni_seed_rows: list[tuple]) -> set[str]:
    aliases: set[str] = set()
    for municipality_key, municipality_value, municipality_norm, *_ in muni_seed_rows:
        for candidate in (
            municipality_key,
            municipality_value,
            municipality_norm,
            normalizeMunicipality(municipality_value),
            normalizeMunicipality(municipality_norm),
        ):
            norm = t_up(candidate)
            if not norm:
                continue
            aliases.add(norm)
            aliases.add(t_up(f"ΔΗΜΟΣ {norm}") or norm)
    return aliases


def seed_organization_rows(b: CsvBundle) -> list[tuple]:
    best_rows: dict[tuple[str, str], tuple[str, str, str, str | None, str | None, str | None]] = {}
    scope_by_org_key = authority_scope_key_map(b.org_map)
    region_aliases = canonical_region_aliases(seed_region_rows(b))
    municipality_aliases = canonical_municipality_aliases(seed_municipality_rows(b))
    org_rows = b.expanded_map[b.expanded_map["source_entity_type"] == "organization"]
    for _, r in org_rows.iterrows():
        org_value = t(r.get("source_value"))
        if not org_value:
            continue
        normalized = t(r.get("normalized_value")) or t_up(org_value) or org_value
        organization_key = organization_key_from_normalized(t_up(normalized) or normalized)
        authority_scope = authority_scope_from_notes(t(r.get("notes"))) or scope_by_org_key.get(organization_key)
        source_key = t(r.get("source_key"))
        value_norms = {t_up(org_value), t_up(normalized)}
        value_norms.discard(None)
        source_key_norm = t_up(source_key) or ""

        if authority_scope == "municipality":
            continue
        if source_key_norm.startswith("ΔΗΜΟΣ::") or any(v in municipality_aliases for v in value_norms):
            continue
        if source_key_norm.startswith("ΠΕΡΙΦΕΡΕΙΑ::") or any(v in region_aliases for v in value_norms):
            continue

        dedup_key = (org_value, normalized)
        existing = best_rows.get(dedup_key)
        if existing is None:
            best_rows[dedup_key] = (
                organization_key,
                org_value,
                normalized,
                authority_scope,
                t(r.get("source_system")),
                source_key,
            )
            continue
        if existing[3] is None and authority_scope is not None:
            best_rows[dedup_key] = (
                existing[0],
                existing[1],
                existing[2],
                authority_scope,
                existing[4],
                existing[5],
            )
    return list(best_rows.values())


def build_organization_lookup(org_seed_rows: list[tuple]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for org_key, org_value, org_norm, *_ in org_seed_rows:
        v = t_up(org_value)
        n = t_up(org_norm)
        if v:
            lookup[v] = org_key
        if n and n not in lookup:
            lookup[n] = org_key
    return lookup


def build_organization_afm_lookup(
    organization_metadata_rows: list[tuple[str, str | None, str | None, str | None, str | None, str | None]],
) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for organization_key, organization_afm, *_ in organization_metadata_rows:
        afm = clean_digits(organization_afm, expected_length=9)
        if afm and afm not in lookup:
            lookup[afm] = organization_key
    return lookup


def organization_lookup_candidates(val: str | None) -> list[str]:
    raw = t(val)
    if raw is None:
        return []

    candidates: list[str] = []

    def add(candidate: str | None) -> None:
        normalized = t_up(candidate)
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    add(raw)
    add(normalizeMunicipality(raw))

    # Handle common municipal prefixes/cases that normalizeMunicipality does not strip.
    prefix_stripped = re.sub(r"^\s*(ΔΗΜΟΥ|ΔΗΜΟΣ|ΔΗΜΟ|Δ\.)\s+", "", t_up(raw) or "")
    add(prefix_stripped)
    add(normalizeMunicipality(prefix_stripped))

    return candidates


def canonical_owner_scope_from_candidates(
    org_candidates: list[str],
    region_candidates: list[str],
    org_key_resolved: str | None,
    organization_lookup: dict[str, str],
    municipality_lookup: dict[str, str],
    region_lookup: dict[str, str],
) -> str | None:
    if org_key_resolved:
        return "organization"
    for candidate in org_candidates:
        if candidate and organization_lookup.get(candidate):
            return "organization"
    for candidate in org_candidates:
        if candidate and municipality_lookup.get(candidate):
            return "municipality"
    for candidate in region_candidates:
        if candidate and region_lookup.get(candidate):
            return "region"
    return None


def region_lookup_candidates(val: str | None) -> list[str]:
    candidates = organization_lookup_candidates(val)
    raw = t(val)
    if raw is None:
        return candidates

    def add(candidate: str | None) -> None:
        normalized = t_up(candidate)
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    prefix_stripped = re.sub(r"^\s*ΠΕΡΙΦΕΡΕΙΑ\s+", "", t_up(raw) or "")
    add(prefix_stripped)
    add(f"ΠΕΡΙΦΕΡΕΙΑ {prefix_stripped}" if prefix_stripped else None)

    # Extract region labels from organization names such as
    # "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ... ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ ..."
    # but use them only for region attribution, not organization renaming.
    raw_upper = t_up(raw) or ""
    raw_upper_no_parens = re.sub(r"\([^)]*\)", " ", raw_upper)
    raw_upper_no_parens = re.sub(r"\s+", " ", raw_upper_no_parens).strip()
    region_match = re.search(r"\bΠΕΡΙΦΕΡΕΙΑΣ\s+([A-ZΑ-Ω0-9 .\-]+)", raw_upper_no_parens)
    if region_match:
        region_label = re.sub(r"\s+", " ", region_match.group(1)).strip(" .-")
        region_label = re.sub(r"\b(Α\.?\s*Ε\.?|Μ\.?\s*Α\.?\s*Ε\.?|Ι\.?\s*Κ\.?\s*Ε\.?)\b\s*$", "", region_label).strip(" .-")
        add(region_label)
        add(f"ΠΕΡΙΦΕΡΕΙΑ {region_label}")

    return candidates


def procurement_rows(
    raw: pd.DataFrame,
    org_map: dict[tuple[str, str], tuple[str | None, str | None]],
    organization_lookup: dict[str, str],
    region_lookup: dict[str, str],
    municipality_lookup: dict[str, str],
    municipality_region_lookup: dict[str, str],
    municipality_alias_lookup: dict[str, set[str]],
    org_municipality_coverage_lookup: dict[str, list[str]],
    organization_afm_lookup: dict[str, str] | None = None,
    created_organization_rows: dict[tuple[str, str], tuple[str, str, str, str | None, str | None, str | None]] | None = None,
    auto_create_organizations: bool = False,
) -> list[tuple]:
    out = []
    for _, r in raw.iterrows():
        region_key, org_key_resolved, municipality_key, canonical_owner_scope = resolve_procurement_context(
            r=r,
            org_map=org_map,
            organization_lookup=organization_lookup,
            region_lookup=region_lookup,
            municipality_lookup=municipality_lookup,
            municipality_region_lookup=municipality_region_lookup,
            municipality_alias_lookup=municipality_alias_lookup,
            org_municipality_coverage_lookup=org_municipality_coverage_lookup,
            organization_afm_lookup=organization_afm_lookup,
            created_organization_rows=created_organization_rows,
            auto_create_organizations=auto_create_organizations,
        )
        out.append((
            t(r.get("title")),
            t(r.get("referenceNumber")),
            t(r.get("prevReferenceNo")),
            t(r.get("noticeReferenceNumber")),
            t(r.get("nextRefNo")),
            b(r.get("nextExtended")),
            b(r.get("nextModified")),
            ts_iso(r.get("submissionDate")),
            date_iso(r.get("contractSignedDate")),
            date_iso(r.get("startDate")),
            b(r.get("noEndDate")),
            date_iso(r.get("endDate")),
            b(r.get("cancelled")),
            date_iso(r.get("cancellationDate")),
            t(r.get("cancellationType")),
            t(r.get("cancellationReason")),
            t(r.get("decisionRelatedAda")),
            t(r.get("contractNumber")),
            t(r.get("organizationVatNumber")),
            b(r.get("greekOrganizationVatNumber")),
            t(r.get("diavgeiaADA")),
            dec(r.get("budget")),
            dec(r.get("contractBudget")),
            i(r.get("bidsSubmitted")),
            i(r.get("maxBidsSubmitted")),
            i(r.get("numberOfSections")),
            t(r.get("centralGovernmentAuthority")),
            t(r.get("procedureType_key")),
            t(r.get("procedureType_value")),
            t(r.get("awardProcedure")),
            t(r.get("centralizedMarkets")),
            t(r.get("contractType")),
            t(r.get("assignCriteria")),
            t(r.get("classificationOfPublicLawOrganization")),
            t(r.get("typeOfContractingAuthority")),
            t(r.get("contractingAuthorityActivity")),
            i(r.get("contractDuration")),
            t(r.get("contractDurationUnitOfMeasure")),
            t(r.get("contractRelatedADA")),
            t(r.get("fundingDetails_cofund")),
            t(r.get("fundingDetails_selfFund")),
            t(r.get("fundingDetails_espa")),
            t(r.get("fundingDetails_regularBudget")),
            t(r.get("unitsOperator")),
            t(r.get("shortDescriptions")),
            t(r.get("greenContracts")),
            t(r.get("auctionRefNo")),
            None,
            region_key,
            org_key_resolved,
            municipality_key,
            canonical_owner_scope,
        ))
    return out


def resolve_procurement_context(
    r: pd.Series,
    org_map: dict[tuple[str, str], tuple[str | None, str | None]],
    organization_lookup: dict[str, str],
    region_lookup: dict[str, str],
    municipality_lookup: dict[str, str],
    municipality_region_lookup: dict[str, str],
    municipality_alias_lookup: dict[str, set[str]],
    org_municipality_coverage_lookup: dict[str, list[str]],
    organization_afm_lookup: dict[str, str] | None = None,
    created_organization_rows: dict[tuple[str, str], tuple[str, str, str, str | None, str | None, str | None]] | None = None,
    auto_create_organizations: bool = False,
) -> tuple[str | None, str | None, str | None, str | None]:
    org_value_raw = t(r.get("organization_value"))
    org_candidates = organization_lookup_candidates(org_value_raw)
    region_candidates = region_lookup_candidates(org_value_raw)
    city_candidates = organization_lookup_candidates(t(r.get("nutsCity")))
    organization_afm_lookup = organization_afm_lookup or {}
    org_name = org_candidates[0] if org_candidates else ""
    org_type = t_up(r.get("typeOfContractingAuthority")) or ""
    municipality_key_raw, region_key_raw = org_map.get((org_type, org_name), (None, None))
    if municipality_key_raw is None and region_key_raw is None and org_name:
        municipality_key_raw, region_key_raw = org_map.get(("", org_name), (None, None))
    if municipality_key_raw is None and region_key_raw is None and org_name:
        municipality_key_raw, region_key_raw = org_map.get((norm_key(org_type) or "", norm_key(org_name) or ""), (None, None))
    if municipality_key_raw is None and region_key_raw is None and org_name:
        municipality_key_raw, region_key_raw = org_map.get(("", norm_key(org_name) or ""), (None, None))
    if municipality_key_raw is None and region_key_raw is None:
        for candidate in org_candidates[1:]:
            municipality_key_raw, region_key_raw = org_map.get((org_type, candidate), (None, None))
            if municipality_key_raw is None and region_key_raw is None:
                municipality_key_raw, region_key_raw = org_map.get(("", candidate), (None, None))
            if municipality_key_raw is None and region_key_raw is None:
                municipality_key_raw, region_key_raw = org_map.get(
                    (norm_key(org_type) or "", norm_key(candidate) or ""),
                    (None, None),
                )
            if municipality_key_raw is None and region_key_raw is None:
                municipality_key_raw, region_key_raw = org_map.get(("", norm_key(candidate) or ""), (None, None))
            if municipality_key_raw is not None or region_key_raw is not None:
                break

    municipality_key = municipality_lookup.get(t_up(municipality_key_raw) or "", municipality_key_raw)
    region_key = region_lookup.get(t_up(region_key_raw) or "", region_key_raw)
    if municipality_key is None:
        for candidate in org_candidates:
            municipality_key = municipality_lookup.get(candidate)
            if municipality_key is not None:
                break
    if region_key is None:
        for candidate in region_candidates:
            region_key = region_lookup.get(candidate)
            if region_key is not None:
                break
    org_key_resolved = None
    organization_afm = clean_digits(r.get("organizationVatNumber"), expected_length=9)
    if organization_afm:
        org_key_resolved = organization_afm_lookup.get(organization_afm)
    for candidate in org_candidates:
        if org_key_resolved is not None:
            break
        org_key_resolved = organization_lookup.get(candidate)
    if org_key_resolved and org_value_raw:
        normalized = default_organization_normalized_value(org_value_raw) or org_value_raw
        if created_organization_rows is not None:
            created_key = (org_key_resolved, org_value_raw)
            if created_key not in created_organization_rows and t_up(org_value_raw) not in organization_lookup:
                created_organization_rows[created_key] = (
                    org_key_resolved,
                    org_value_raw,
                    normalized,
                    None,
                    "raw_procurements",
                    t(r.get("organization_key")) or t(r.get("referenceNumber")),
                )
        normalized_value = t_up(normalized)
        if normalized_value and normalized_value not in organization_lookup:
            organization_lookup[normalized_value] = org_key_resolved
    canonical_owner_scope = canonical_owner_scope_from_candidates(
        org_candidates=org_candidates,
        region_candidates=region_candidates,
        org_key_resolved=org_key_resolved,
        organization_lookup=organization_lookup,
        municipality_lookup=municipality_lookup,
        region_lookup=region_lookup,
    )
    municipality_key_from_city = None
    for candidate in city_candidates:
        municipality_key_from_city = municipality_lookup.get(candidate)
        if municipality_key_from_city is not None:
            break
    organization_candidate_municipalities: list[str] = []
    for candidate in org_candidates:
        for municipality_candidate in org_municipality_coverage_lookup.get(candidate, []):
            if municipality_candidate not in organization_candidate_municipalities:
                organization_candidate_municipalities.append(municipality_candidate)
    municipality_key_from_context = resolve_municipality_from_context(
        context_values=[t(r.get("nutsCity")), t(r.get("title")), t(r.get("shortDescriptions"))],
        candidate_keys=organization_candidate_municipalities,
        municipality_alias_lookup=municipality_alias_lookup,
    )
    if canonical_owner_scope == "organization" and municipality_key_from_city:
        city_region_key = municipality_region_lookup.get(municipality_key_from_city)
        if region_key is None and city_region_key is not None:
            region_key = city_region_key
        if city_region_key is None or region_key is None or city_region_key == region_key:
            municipality_key = municipality_key_from_city
    if canonical_owner_scope == "organization" and municipality_key_from_context:
        context_region_key = municipality_region_lookup.get(municipality_key_from_context)
        if region_key is None and context_region_key is not None:
            region_key = context_region_key
        if context_region_key is None or region_key is None or context_region_key == region_key:
            municipality_key = municipality_key_from_context

    if (
        org_key_resolved is None
        and canonical_owner_scope is None
        and auto_create_organizations
        and org_value_raw
    ):
        normalized = default_organization_normalized_value(org_value_raw) or org_value_raw
        org_key_resolved = organization_key_from_afm(organization_afm) if organization_afm else organization_key_from_normalized(normalized)
        canonical_owner_scope = "organization"
        if created_organization_rows is not None:
            created_organization_rows.setdefault(
                (org_key_resolved, org_value_raw),
                (
                    org_key_resolved,
                    org_value_raw,
                    normalized,
                    None,
                    "raw_procurements",
                    t(r.get("organization_key")) or t(r.get("referenceNumber")),
                ),
            )
        normalized_value = t_up(normalized)
        if normalized_value and normalized_value not in organization_lookup:
            organization_lookup[normalized_value] = org_key_resolved
        if organization_afm:
            organization_afm_lookup[organization_afm] = org_key_resolved

    return region_key, org_key_resolved, municipality_key, canonical_owner_scope


def clean_digits(value: str | None, expected_length: int | None = None) -> str | None:
    raw = t(value)
    if raw is None:
        return None
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return None
    if expected_length is not None and len(digits) != expected_length:
        return None
    return digits


def clean_nuts_code(value: str | None) -> str | None:
    raw = t_up(value)
    if raw is None:
        return None
    raw = re.sub(r"\s+", "", raw)
    if "|" in raw:
        return None
    if re.fullmatch(r"EL\d{3}", raw):
        return raw
    if re.fullmatch(r"EL\d{2}", raw):
        return raw
    if re.fullmatch(r"EL\d", raw):
        return raw
    return None


def clean_nuts_text(value: str | None, uppercase: bool = False) -> str | None:
    raw = t_up(value) if uppercase else t(value)
    if raw is None:
        return None
    if "|" in raw:
        return None
    raw = re.sub(r"\s+", " ", raw).strip()
    if not raw:
        return None
    return raw


def nuts_code_specificity(code: str | None) -> int:
    if code is None:
        return 0
    if re.fullmatch(r"EL\d{3}", code):
        return 3
    if re.fullmatch(r"EL\d{2}", code):
        return 2
    if re.fullmatch(r"EL\d", code):
        return 1
    return 0


def is_generic_nuts_value(value: str | None) -> bool:
    normalized = t_up(value) or ""
    return normalized in {"ΕΛΛΑΔΑ", "ΒΟΡΕΙΑ ΕΛΛΑΔΑ", "ΝΟΤΙΑ ΕΛΛΑΔΑ"}


def select_counter_value(counter: Counter[str]) -> str | None:
    if not counter:
        return None
    return max(counter.items(), key=lambda item: (item[1], len(item[0]), item[0]))[0]


def select_nuts_pair(counter: Counter[tuple[str, str]]) -> tuple[str | None, str | None]:
    if not counter:
        return None, None
    best_pair, _ = max(
        counter.items(),
        key=lambda item: (
            nuts_code_specificity(item[0][0]),
            0 if is_generic_nuts_value(item[0][1]) else 1,
            item[1],
            len(item[0][1]),
            item[0][0],
            item[0][1],
        ),
    )
    return best_pair


def select_region_nuts_pair(counter: Counter[tuple[str, str]]) -> tuple[str | None, str | None]:
    if not counter:
        return None, None
    region_level_items = [
        item
        for item in counter.items()
        if nuts_code_specificity(item[0][0]) == 2
    ]
    if not region_level_items:
        return None, None
    best_pair, _ = max(
        region_level_items,
        key=lambda item: (
            0 if is_generic_nuts_value(item[0][1]) else 1,
            item[1],
            len(item[0][1]),
            item[0][0],
            item[0][1],
        ),
    )
    return best_pair


def _build_metadata_counters(
    raw: pd.DataFrame,
    org_map: dict[tuple[str, str], tuple[str | None, str | None]],
    organization_lookup: dict[str, str],
    region_lookup: dict[str, str],
    municipality_lookup: dict[str, str],
    municipality_region_lookup: dict[str, str],
    municipality_alias_lookup: dict[str, set[str]],
    org_municipality_coverage_lookup: dict[str, list[str]],
    target_scope: str,
    organization_afm_lookup: dict[str, str] | None = None,
) -> tuple[
    dict[str, Counter[str]],
    dict[str, Counter[str]],
    dict[str, Counter[str]],
    dict[str, Counter[str]],
    dict[str, Counter[tuple[str, str]]],
]:
    source_organization_key_counters: dict[str, Counter[str]] = {}
    afm_counters: dict[str, Counter[str]] = {}
    postal_code_counters: dict[str, Counter[str]] = {}
    city_counters: dict[str, Counter[str]] = {}
    nuts_pair_counters: dict[str, Counter[tuple[str, str]]] = {}

    for _, r in raw.iterrows():
        region_key, org_key_resolved, municipality_key, canonical_owner_scope = resolve_procurement_context(
            r=r,
            org_map=org_map,
            organization_lookup=organization_lookup,
            region_lookup=region_lookup,
            municipality_lookup=municipality_lookup,
            municipality_region_lookup=municipality_region_lookup,
            municipality_alias_lookup=municipality_alias_lookup,
            org_municipality_coverage_lookup=org_municipality_coverage_lookup,
            organization_afm_lookup=organization_afm_lookup,
        )
        if canonical_owner_scope != target_scope:
            continue
        target_key = None
        if target_scope == "municipality":
            target_key = municipality_key
        elif target_scope == "region":
            target_key = region_key
        elif target_scope == "organization":
            target_key = org_key_resolved
        if target_key is None:
            continue

        source_organization_key = t(r.get("organization_key"))
        if source_organization_key:
            source_organization_key_counters.setdefault(target_key, Counter())[source_organization_key] += 1

        municipality_afm = clean_digits(r.get("organizationVatNumber"), expected_length=9)
        if municipality_afm:
            afm_counters.setdefault(target_key, Counter())[municipality_afm] += 1

        nuts_postal_code = clean_digits(r.get("nutsPostalCode"), expected_length=5)
        if nuts_postal_code:
            postal_code_counters.setdefault(target_key, Counter())[nuts_postal_code] += 1

        nuts_city = clean_nuts_text(r.get("nutsCity"), uppercase=True)
        if nuts_city:
            city_counters.setdefault(target_key, Counter())[nuts_city] += 1

        nuts_code_key = clean_nuts_code(r.get("nutsCode_key"))
        nuts_code_value = clean_nuts_text(r.get("nutsCode_value"))
        if nuts_code_key and nuts_code_value:
            nuts_pair_counters.setdefault(target_key, Counter())[(nuts_code_key, nuts_code_value)] += 1

    return (
        source_organization_key_counters,
        afm_counters,
        postal_code_counters,
        city_counters,
        nuts_pair_counters,
    )


def build_municipality_metadata_rows(
    raw: pd.DataFrame,
    org_map: dict[tuple[str, str], tuple[str | None, str | None]],
    organization_lookup: dict[str, str],
    region_lookup: dict[str, str],
    municipality_lookup: dict[str, str],
    municipality_region_lookup: dict[str, str],
    municipality_alias_lookup: dict[str, set[str]],
    org_municipality_coverage_lookup: dict[str, list[str]],
    organization_afm_lookup: dict[str, str] | None = None,
) -> list[tuple[str, str | None, str | None, str | None, str | None, str | None, str | None]]:
    (
        source_organization_key_counters,
        afm_counters,
        postal_code_counters,
        city_counters,
        nuts_pair_counters,
    ) = _build_metadata_counters(
        raw=raw,
        org_map=org_map,
        organization_lookup=organization_lookup,
        region_lookup=region_lookup,
        municipality_lookup=municipality_lookup,
        municipality_region_lookup=municipality_region_lookup,
        municipality_alias_lookup=municipality_alias_lookup,
        org_municipality_coverage_lookup=org_municipality_coverage_lookup,
        target_scope="municipality",
        organization_afm_lookup=organization_afm_lookup,
    )

    municipality_keys = set(source_organization_key_counters)
    municipality_keys.update(afm_counters)
    municipality_keys.update(postal_code_counters)
    municipality_keys.update(city_counters)
    municipality_keys.update(nuts_pair_counters)

    out: list[tuple[str, str | None, str | None, str | None, str | None, str | None, str | None]] = []
    for municipality_key in sorted(municipality_keys):
        nuts_code_key, nuts_code_value = select_nuts_pair(nuts_pair_counters.get(municipality_key, Counter()))
        out.append((
            municipality_key,
            select_counter_value(source_organization_key_counters.get(municipality_key, Counter())),
            select_counter_value(afm_counters.get(municipality_key, Counter())),
            select_counter_value(postal_code_counters.get(municipality_key, Counter())),
            select_counter_value(city_counters.get(municipality_key, Counter())),
            nuts_code_value,
            nuts_code_key,
        ))
    return out


def build_region_metadata_rows(
    raw: pd.DataFrame,
    org_map: dict[tuple[str, str], tuple[str | None, str | None]],
    organization_lookup: dict[str, str],
    region_lookup: dict[str, str],
    municipality_lookup: dict[str, str],
    municipality_region_lookup: dict[str, str],
    municipality_alias_lookup: dict[str, set[str]],
    org_municipality_coverage_lookup: dict[str, list[str]],
    organization_afm_lookup: dict[str, str] | None = None,
) -> list[tuple[str, str | None, str | None, str | None, str | None, str | None, str | None]]:
    (
        source_organization_key_counters,
        afm_counters,
        postal_code_counters,
        city_counters,
        nuts_pair_counters,
    ) = _build_metadata_counters(
        raw=raw,
        org_map=org_map,
        organization_lookup=organization_lookup,
        region_lookup=region_lookup,
        municipality_lookup=municipality_lookup,
        municipality_region_lookup=municipality_region_lookup,
        municipality_alias_lookup=municipality_alias_lookup,
        org_municipality_coverage_lookup=org_municipality_coverage_lookup,
        target_scope="region",
        organization_afm_lookup=organization_afm_lookup,
    )

    region_keys = set(source_organization_key_counters)
    region_keys.update(afm_counters)
    region_keys.update(postal_code_counters)
    region_keys.update(city_counters)
    region_keys.update(nuts_pair_counters)

    out: list[tuple[str, str | None, str | None, str | None, str | None, str | None, str | None]] = []
    for region_key in sorted(region_keys):
        nuts_code_key, nuts_code_value = select_region_nuts_pair(nuts_pair_counters.get(region_key, Counter()))
        out.append((
            region_key,
            select_counter_value(source_organization_key_counters.get(region_key, Counter())),
            select_counter_value(afm_counters.get(region_key, Counter())),
            select_counter_value(postal_code_counters.get(region_key, Counter())),
            select_counter_value(city_counters.get(region_key, Counter())),
            nuts_code_value,
            nuts_code_key,
        ))
    return out


def build_organization_metadata_rows(
    raw: pd.DataFrame,
    org_map: dict[tuple[str, str], tuple[str | None, str | None]],
    organization_lookup: dict[str, str],
    region_lookup: dict[str, str],
    municipality_lookup: dict[str, str],
    municipality_region_lookup: dict[str, str],
    municipality_alias_lookup: dict[str, set[str]],
    org_municipality_coverage_lookup: dict[str, list[str]],
    organization_afm_lookup: dict[str, str] | None = None,
) -> list[tuple[str, str | None, str | None, str | None, str | None, str | None]]:
    (
        _source_organization_key_counters,
        afm_counters,
        postal_code_counters,
        city_counters,
        nuts_pair_counters,
    ) = _build_metadata_counters(
        raw=raw,
        org_map=org_map,
        organization_lookup=organization_lookup,
        region_lookup=region_lookup,
        municipality_lookup=municipality_lookup,
        municipality_region_lookup=municipality_region_lookup,
        municipality_alias_lookup=municipality_alias_lookup,
        org_municipality_coverage_lookup=org_municipality_coverage_lookup,
        target_scope="organization",
        organization_afm_lookup=organization_afm_lookup,
    )

    organization_keys = set(afm_counters)
    organization_keys.update(postal_code_counters)
    organization_keys.update(city_counters)
    organization_keys.update(nuts_pair_counters)

    out: list[tuple[str, str | None, str | None, str | None, str | None, str | None]] = []
    for organization_key in sorted(organization_keys):
        nuts_code_key, nuts_code_value = select_nuts_pair(nuts_pair_counters.get(organization_key, Counter()))
        out.append((
            organization_key,
            select_counter_value(afm_counters.get(organization_key, Counter())),
            select_counter_value(postal_code_counters.get(organization_key, Counter())),
            select_counter_value(city_counters.get(organization_key, Counter())),
            nuts_code_value,
            nuts_code_key,
        ))
    return out


def diav_rows(
    diav: pd.DataFrame,
    org_map: dict[tuple[str, str], tuple[str | None, str | None]],
    organization_lookup: dict[str, str],
    region_lookup: dict[str, str],
    municipality_lookup: dict[str, str],
) -> list[tuple]:
    out = []
    for _, r in diav.iterrows():
        org_type = t_up(r.get("org_type")) or ""
        org_name = t_up(r.get("org_name_clean")) or ""
        municipality_key_raw, region_key_raw = org_map.get((org_type, org_name), (None, None))
        municipality_key = municipality_lookup.get(t_up(municipality_key_raw) or "", municipality_key_raw)
        region_key = region_lookup.get(t_up(region_key_raw) or "", region_key_raw)
        org_candidates = [t(r.get("org_name_clean")), t(r.get("organization")), t(r.get("org"))]
        org_key_resolved = None
        for c in org_candidates:
            cu = t_up(c)
            if cu and cu in organization_lookup:
                org_key_resolved = organization_lookup[cu]
                break
        out.append((
            region_key,
            org_key_resolved,
            municipality_key,
            t(r.get("ada")),
            t(r.get("protocolNumber")),
            ts_iso(r.get("submissionTimestamp")),
            ts_iso(r.get("publishTimestamp")),
            t(r.get("status")),
            b(r.get("nonRevokable")),
            t(r.get("documentUrl")),
            t(r.get("subject")),
            t(r.get("documentType")),
            t(r.get("versionComment")),
            t(r.get("thematicCategories")),
            t(r.get("organization")),
            t(r.get("cooperatingOrganizations")),
            t(r.get("unitIds")),
            t(r.get("org")),
            t(r.get("org_type")),
            t(r.get("org_name_clean")),
            t(r.get("spending_signers")),
            t(r.get("spending_contractors_afm")),
            t(r.get("spending_contractors_name")),
            t(r.get("spending_contractors_value")),
            t(r.get("decisionType")),
        ))
    return out


def payment_row_from_raw(raw_row: pd.Series, procurement_id: int) -> tuple:
    members = raw_contracting_members(raw_row)
    beneficiary_names = unique_nonempty_strings(name for _, name in members)
    beneficiary_vat_numbers = unique_nonempty_strings(afm for afm, _ in members)
    beneficiary_name = " | ".join(beneficiary_names) if beneficiary_names else None
    beneficiary_vat_number = " | ".join(beneficiary_vat_numbers) if beneficiary_vat_numbers else None
    submission_ts = ts_iso(raw_row.get("submissionDate"))
    fiscal_year = int(submission_ts[:4]) if submission_ts else None
    return (
        procurement_id,                              # procurement_id
        None,                                        # diavgeia_document_type_decision_uid
        None,                                        # diavgeia_id
        len(members) if members else None,           # beneficiaries_count
        t(raw_row.get("signers")),                   # signers
        beneficiary_name,                            # beneficiary_name
        beneficiary_vat_number,                      # beneficiary_vat_number
        dec(raw_row.get("totalCostWithVAT")),        # amount_with_vat
        dec(raw_row.get("totalCostWithoutVAT")),     # amount_without_vat
        None,                                        # kae_ale (not provided in raw CSV)
        fiscal_year,                                 # fiscal_year
        t(raw_row.get("fundingDetails_regularBudget")),  # budget_category
        beneficiary_name or beneficiary_vat_number,  # counter_party
        t(raw_row.get("paymentRefNo")),              # payment_ref_no
    )


def forest_fire_rows(fire: pd.DataFrame) -> list[tuple]:
    out = []
    for _, r in fire.iterrows():
        out.append((
            t(r.get("municipality_id")),
            None,
            i(r.get("year")),
            date_iso(r.get("date_start")),
            date_iso(r.get("date_end")),
            t(r.get("nomos")),
            t(r.get("area_name")),
            dec(r.get("lat")),
            dec(r.get("lon")),
            dec(r.get("burned_forest_stremata")),
            dec(r.get("burned_woodland_stremata")),
            dec(r.get("burned_grassland_stremata")),
            dec(r.get("burned_grove_stremata")),
            dec(r.get("burned_other_stremata")),
            dec(r.get("burned_total_stremata")),
            dec(r.get("burned_total_ha")),
            t(r.get("source")),
        ))
    return out


def fund_rows(fund: pd.DataFrame) -> list[tuple]:
    out = []
    for _, r in fund.iterrows():
        out.append((
            t(r.get("region_key")) or t(r.get("region_id")),
            t(r.get("organization_key")),
            t(r.get("municipality_key")) or t(r.get("municipality_code")) or t(r.get("municipality_id")),
            i(r.get("year")),
            t(r.get("allocation_type")),
            t(r.get("recipient_type")),
            t(r.get("recipient_raw")),
            t(r.get("nomos")),
            dec(r.get("amount_eur")),
            t(r.get("source_file")),
            t(r.get("source_ada")),
        ))
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stage 2 ERD loader")
    p.add_argument("--dry-run", action="store_true", help="prepare rows only; do not write DB")
    p.add_argument("--limit", type=int, default=None, help="limit rows per source for smoke test")
    p.add_argument(
        "--tables",
        type=str,
        default="region,municipality,organization,diavgeia_document_type,procurement,cpv,diavgeia,payment,forest_fire,diavgeia_procurement,beneficiary",
        help=(
            "comma-separated stages: region,municipality,organization,diavgeia_document_type,"
            "procurement,cpv,diavgeia,payment,forest_fire,fund,diavgeia_procurement,beneficiary "
            "(default excludes static fund table)"
        ),
    )
    p.add_argument(
        "--allow-static-fund-reload",
        action="store_true",
        help="Allow writes to public.fund. Disabled by default because fund is treated as static.",
    )
    p.add_argument(
        "--reprocess-existing-procurement",
        action="store_true",
        help=(
            "Reprocess existing procurement identities (refresh keys and regenerate CPV/payment rows). "
            "Default is incremental mode: skip existing and ingest only new procurement rows."
        ),
    )
    return p.parse_args()


def execute_values(cur, sql: str, rows: Iterable[tuple], page_size: int = 1000):
    rows = list(rows)
    if not rows:
        return
    psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _normalize_keyword_for_sql(keyword: str) -> str:
    """Strip accents and non-alphanumeric chars to match delete_procurements_by_keywords logic."""
    import unicodedata as _ud
    decomposed = _ud.normalize("NFD", keyword)
    without_marks = "".join(ch for ch in decomposed if _ud.category(ch) != "Mn")
    folded = without_marks.casefold().replace("ς", "σ")
    return "".join(ch for ch in folded if ch.isalnum())


def prune_excluded_procurements(cur, conn, keywords: list[str], dry_run: bool) -> int:
    """Delete procurement rows whose title or short_descriptions match any exclude keyword."""
    prepared = [_normalize_keyword_for_sql(k) for k in keywords if k.strip()]
    prepared = list(dict.fromkeys(kw for kw in prepared if kw))  # dedupe, preserve order
    if not prepared:
        return 0

    cur.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'unaccent')")
    use_unaccent = bool(cur.fetchone()[0])

    GREEK_ACCENTED_SOURCE = "άέήίϊΐόύϋΰώς"
    GREEK_ACCENTED_TARGET = "αεηιιιουυυωσ"

    def _col_expr(col: str) -> str:
        base = f"LOWER(COALESCE(p.{col}, ''))"
        if use_unaccent:
            base = f"UNACCENT({base})"
        else:
            base = f"TRANSLATE({base}, '{GREEK_ACCENTED_SOURCE}', '{GREEK_ACCENTED_TARGET}')"
        return f"REGEXP_REPLACE({base}, '[^[:alnum:]]+', '', 'g')"

    columns = ("title", "short_descriptions")
    groups = []
    params: list[str] = []
    for kw in prepared:
        checks = []
        for col in columns:
            checks.append(f"{_col_expr(col)} LIKE %s")
            params.append(f"%{kw}%")
        groups.append("(" + " OR ".join(checks) + ")")

    where_sql = " OR ".join(groups)

    cur.execute(f"SELECT COUNT(*) FROM public.procurement AS p WHERE {where_sql}", params)
    count = int(cur.fetchone()[0])
    if count == 0:
        return 0

    if dry_run:
        log(f"[dry-run] prune_excluded_procurements: would delete {count} rows matching exclude keywords")
        return count

    cur.execute("SET CONSTRAINTS ALL DEFERRED")
    cur.execute(
        f"""
        WITH matched AS (SELECT p.id FROM public.procurement AS p WHERE {where_sql})
        DELETE FROM public.procurement AS p USING matched WHERE p.id = matched.id
        """,
        params,
    )
    deleted = cur.rowcount
    conn.commit()
    log(f"Pruned {deleted} excluded procurement rows from DB")
    return deleted


def norm_uid_part(v) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    return s


def procurement_source_uid(
    reference_number,
    diavgeia_ada,
    contract_number,
    title,
    submission_at,
    organization_key,
) -> str:
    # Priority order mirrors source uniqueness reality.
    ref = norm_uid_part(reference_number)
    ada = norm_uid_part(diavgeia_ada)
    cn = norm_uid_part(contract_number)
    ttl = norm_uid_part(title)
    sub = norm_uid_part(submission_at)
    org = norm_uid_part(organization_key)
    if ref:
        return f"ref:{ref}"
    if ada:
        return f"ada:{ada}"
    if cn:
        return f"cn:{cn}|org:{org}"
    return f"fallback:{org}|{sub}|{ttl}"


def procurement_uid_from_proc_row(p: tuple) -> str:
    # tuple indexes from procurement_rows()
    return procurement_source_uid(
        reference_number=p[1],
        diavgeia_ada=p[20],
        contract_number=p[17],
        title=p[0],
        submission_at=p[7],
        organization_key=p[49],
    )


def key_norm(v):
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    if v is None:
        return None
    return str(v)


def forest_fire_row_digest(row: tuple) -> str:
    payload = {column: key_norm(value) for column, value in zip(FIRE_KEY_COLUMNS, row, strict=True)}
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return hashlib.md5(encoded.encode("utf-8")).hexdigest()


def dedupe_forest_fire_rows(rows: Iterable[tuple]) -> tuple[list[tuple], int]:
    unique_rows: list[tuple] = []
    seen_digests: set[str] = set()
    skipped_duplicates = 0

    for row in rows:
        digest = forest_fire_row_digest(row)
        if digest in seen_digests:
            skipped_duplicates += 1
            continue
        seen_digests.add(digest)
        unique_rows.append(row)

    return unique_rows, skipped_duplicates


def load_existing_forest_fire_digests(cur) -> set[str]:
    cur.execute(
        """
        SELECT md5(row_to_json(x)::text) AS fire_digest
        FROM (
          SELECT
            municipality_key,
            region_key,
            year::text AS year,
            date_start::text AS date_start,
            date_end::text AS date_end,
            nomos,
            area_name,
            lat::text AS lat,
            lon::text AS lon,
            burned_forest_stremata::text AS burned_forest_stremata,
            burned_woodland_stremata::text AS burned_woodland_stremata,
            burned_grassland_stremata::text AS burned_grassland_stremata,
            burned_grove_stremata::text AS burned_grove_stremata,
            burned_other_stremata::text AS burned_other_stremata,
            burned_total_stremata::text AS burned_total_stremata,
            burned_total_ha::text AS burned_total_ha,
            source
          FROM public.forest_fire
        ) AS x
        """
    )
    return {row[0] for row in cur.fetchall()}


def main() -> None:
    log("Stage 2 ingest started")
    args = parse_args()
    log("Reading CSVs...")
    bundle = read_csvs(limit=args.limit)
    log("Building lookup maps...")
    org_map = build_maps(bundle.org_map, bundle.expanded_map)

    region_seed = seed_region_rows(bundle)
    muni_seed = seed_municipality_rows(bundle)
    org_seed = seed_organization_rows(bundle)
    region_lookup = build_region_lookup(region_seed)
    municipality_lookup = build_municipality_lookup(muni_seed)
    municipality_region_lookup = build_municipality_region_lookup(muni_seed)
    municipality_alias_lookup = build_municipality_alias_lookup(muni_seed)
    org_municipality_coverage_lookup = build_org_municipality_coverage_lookup(bundle.expanded_map)
    organization_lookup = build_organization_lookup(org_seed)
    bundle.raw = apply_procurement_chain_dedup(bundle.raw)
    preliminary_organization_metadata_rows = build_organization_metadata_rows(
        raw=bundle.raw,
        org_map=org_map,
        organization_lookup=organization_lookup,
        region_lookup=region_lookup,
        municipality_lookup=municipality_lookup,
        municipality_region_lookup=municipality_region_lookup,
        municipality_alias_lookup=municipality_alias_lookup,
        org_municipality_coverage_lookup=org_municipality_coverage_lookup,
    )
    organization_afm_lookup = build_organization_afm_lookup(preliminary_organization_metadata_rows)
    municipality_metadata_rows = build_municipality_metadata_rows(
        raw=bundle.raw,
        org_map=org_map,
        organization_lookup=organization_lookup,
        region_lookup=region_lookup,
        municipality_lookup=municipality_lookup,
        municipality_region_lookup=municipality_region_lookup,
        municipality_alias_lookup=municipality_alias_lookup,
        org_municipality_coverage_lookup=org_municipality_coverage_lookup,
        organization_afm_lookup=organization_afm_lookup,
    )
    region_metadata_rows = build_region_metadata_rows(
        raw=bundle.raw,
        org_map=org_map,
        organization_lookup=organization_lookup,
        region_lookup=region_lookup,
        municipality_lookup=municipality_lookup,
        municipality_region_lookup=municipality_region_lookup,
        municipality_alias_lookup=municipality_alias_lookup,
        org_municipality_coverage_lookup=org_municipality_coverage_lookup,
        organization_afm_lookup=organization_afm_lookup,
    )

    created_organization_rows: dict[tuple[str, str], tuple[str, str, str, str | None, str | None, str | None]] = {}
    procurement = procurement_rows(
        bundle.raw,
        org_map,
        organization_lookup,
        region_lookup,
        municipality_lookup,
        municipality_region_lookup,
        municipality_alias_lookup,
        org_municipality_coverage_lookup,
        organization_afm_lookup=organization_afm_lookup,
        created_organization_rows=created_organization_rows,
        auto_create_organizations=True,
    )
    if created_organization_rows:
        org_seed.extend(created_organization_rows.values())
        organization_lookup = build_organization_lookup(org_seed)
    organization_metadata_rows = build_organization_metadata_rows(
        raw=bundle.raw,
        org_map=org_map,
        organization_lookup=organization_lookup,
        region_lookup=region_lookup,
        municipality_lookup=municipality_lookup,
        municipality_region_lookup=municipality_region_lookup,
        municipality_alias_lookup=municipality_alias_lookup,
        org_municipality_coverage_lookup=org_municipality_coverage_lookup,
        organization_afm_lookup=organization_afm_lookup,
    )
    diav = diav_rows(bundle.diav, org_map, organization_lookup, region_lookup, municipality_lookup)
    fires = forest_fire_rows(bundle.fire)
    funds = fund_rows(bundle.fund)

    print("Prepared rows:")
    print(f"  region seed:       {len(region_seed)}")
    print(f"  region meta:       {len(region_metadata_rows)}")
    print(f"  municipality seed: {len(muni_seed)}")
    print(f"  municipality meta: {len(municipality_metadata_rows)}")
    print(f"  organization seed: {len(org_seed)}")
    print(f"  organization meta: {len(organization_metadata_rows)}")
    print(f"  procurement:       {len(procurement)}")
    print(f"  diavgeia:          {len(diav)}")
    print(f"  payment:           derived from raw_procurements rows")
    print(f"  forest_fire:       {len(fires)}")
    print(f"  fund:              {len(funds)}")

    if args.dry_run:
        log("Dry run finished")
        return

    selected_tables = {x.strip() for x in args.tables.split(",") if x.strip()}
    all_tables = {
        "region",
        "municipality",
        "organization",
        "diavgeia_document_type",
        "procurement",
        "cpv",
        "diavgeia",
        "payment",
        "forest_fire",
        "fund",
        "diavgeia_procurement",
        "beneficiary",
    }
    if "all" in selected_tables:
        selected_tables = set(all_tables)
    unknown = selected_tables - all_tables
    if unknown:
        raise ValueError(f"Unknown tables in --tables: {', '.join(sorted(unknown))}")
    if "fund" in selected_tables and not args.allow_static_fund_reload:
        raise ValueError(
            "fund reload is disabled by default. "
            "Use --tables ... without fund, or pass --allow-static-fund-reload for an intentional one-off reload."
        )
    if ("cpv" in selected_tables or "payment" in selected_tables) and "procurement" not in selected_tables:
        raise ValueError("cpv/payment require procurement in the same run (use --tables including procurement)")

    db_url = resolve_database_url(os.environ.get("DATABASE_URL"))
    log("Connecting to database...")
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Seed dimensions first.
        if "region" in selected_tables:
            log(f"Seeding region rows ({len(region_seed)})...")
            execute_values(
                cur,
                """
                INSERT INTO public.region (
                  region_key, region_value, region_normalized_value, source_system, source_key
                ) VALUES %s
                ON CONFLICT (region_key, region_value) DO UPDATE SET
                  region_normalized_value = EXCLUDED.region_normalized_value,
                  source_system = EXCLUDED.source_system,
                  source_key = EXCLUDED.source_key
                """,
                region_seed,
            )
            execute_values(
                cur,
                """
                UPDATE public.region AS r
                SET organization_key = COALESCE(src.organization_key, r.organization_key),
                    region_afm = COALESCE(src.region_afm, r.region_afm),
                    nuts_postal_code = COALESCE(src.nuts_postal_code, r.nuts_postal_code),
                    nuts_postal_city = COALESCE(src.nuts_postal_city, r.nuts_postal_city),
                    nuts_code_value = COALESCE(src.nuts_code_value, r.nuts_code_value),
                    nuts_code_key = COALESCE(src.nuts_code_key, r.nuts_code_key),
                    updated_at = NOW()
                FROM (VALUES %s) AS src (
                  region_key,
                  organization_key,
                  region_afm,
                  nuts_postal_code,
                  nuts_postal_city,
                  nuts_code_value,
                  nuts_code_key
                )
                WHERE r.region_key = src.region_key
                """,
                region_metadata_rows,
            )
            conn.commit()
            log("Region seed committed")
        if "municipality" in selected_tables:
            log(f"Seeding municipality rows ({len(muni_seed)})...")
            # Remove code-only placeholders from older fallback-based loads.
            cur.execute(
                """
                DELETE FROM public.municipality
                WHERE municipality_key IS NOT NULL
                  AND municipality_value = municipality_key
                """
            )
            execute_values(
                cur,
                """
                INSERT INTO public.municipality (
                  municipality_key, municipality_value, municipality_normalized_value, region_key, source_system, source_key
                ) VALUES %s
                ON CONFLICT (municipality_key, municipality_value) DO UPDATE SET
                  municipality_normalized_value = EXCLUDED.municipality_normalized_value,
                  region_key = EXCLUDED.region_key,
                  source_system = EXCLUDED.source_system,
                  source_key = EXCLUDED.source_key
                """,
                muni_seed,
            )

            # Canonical municipality display names for frontend usage:
            # one row per municipality_key in municipality_normalized_name.
            cur.execute(
                """
                INSERT INTO public.municipality_normalized_name (
                  municipality_key, municipality_value, municipality_normalized_value
                )
                SELECT DISTINCT ON (m.municipality_key)
                  m.municipality_key,
                  m.municipality_value,
                  m.municipality_normalized_value
                FROM public.municipality AS m
                WHERE m.municipality_key IS NOT NULL
                  AND m.municipality_value IS NOT NULL
                  AND m.municipality_value <> m.municipality_key
                ORDER BY
                  m.municipality_key,
                  CASE m.source_system
                    WHEN 'region_to_municipalities' THEN 0
                    WHEN 'geo' THEN 1
                    ELSE 2
                  END,
                  m.id
                ON CONFLICT (municipality_key) DO UPDATE SET
                  municipality_value = EXCLUDED.municipality_value,
                  municipality_normalized_value = EXCLUDED.municipality_normalized_value,
                  updated_at = NOW()
                """
            )
            cur.execute("DELETE FROM public.municipality_normalized_name WHERE municipality_key IS NULL")
            cur.execute(
                """
                UPDATE public.municipality AS m
                SET municipality_normalized_name_id = n.id
                FROM public.municipality_normalized_name AS n
                WHERE m.municipality_key = n.municipality_key
                  AND (m.municipality_normalized_name_id IS DISTINCT FROM n.id)
                """
            )
            execute_values(
                cur,
                """
                UPDATE public.municipality AS m
                SET organization_key = COALESCE(src.organization_key, m.organization_key),
                    municipality_afm = COALESCE(src.municipality_afm, m.municipality_afm),
                    nuts_postal_code = COALESCE(src.nuts_postal_code, m.nuts_postal_code),
                    nuts_city = COALESCE(src.nuts_city, m.nuts_city),
                    nuts_code_value = COALESCE(src.nuts_code_value, m.nuts_code_value),
                    nuts_code_key = COALESCE(src.nuts_code_key, m.nuts_code_key),
                    updated_at = NOW()
                FROM (VALUES %s) AS src (
                  municipality_key,
                  organization_key,
                  municipality_afm,
                  nuts_postal_code,
                  nuts_city,
                  nuts_code_value,
                  nuts_code_key
                )
                WHERE m.municipality_key = src.municipality_key
                """,
                municipality_metadata_rows,
            )
            conn.commit()
            log("Municipality seed committed")
        if "organization" in selected_tables:
            log(f"Seeding organization rows ({len(org_seed)})...")
            execute_values(
                cur,
                """
                INSERT INTO public.organization (
                  organization_key, organization_value, organization_normalized_value, authority_scope, source_system, source_key
                ) VALUES %s
                ON CONFLICT (organization_key, organization_value) DO UPDATE SET
                  organization_normalized_value = EXCLUDED.organization_normalized_value,
                  authority_scope = COALESCE(EXCLUDED.authority_scope, public.organization.authority_scope),
                  source_system = EXCLUDED.source_system,
                  source_key = EXCLUDED.source_key
                """,
                org_seed,
            )
            execute_values(
                cur,
                """
                UPDATE public.organization AS o
                SET organization_afm = COALESCE(src.organization_afm, o.organization_afm),
                    nuts_postal_code = COALESCE(src.nuts_postal_code, o.nuts_postal_code),
                    nuts_city = COALESCE(src.nuts_city, o.nuts_city),
                    nuts_code_value = COALESCE(src.nuts_code_value, o.nuts_code_value),
                    nuts_code_key = COALESCE(src.nuts_code_key, o.nuts_code_key),
                    updated_at = NOW()
                FROM (VALUES %s) AS src (
                  organization_key,
                  organization_afm,
                  nuts_postal_code,
                  nuts_city,
                  nuts_code_value,
                  nuts_code_key
                )
                WHERE o.organization_key = src.organization_key
                """,
                organization_metadata_rows,
            )
            conn.commit()
            log("Organization seed committed")

        # Decision types dictionary.
        dt_rows = sorted({(t(r.get("decisionType")), t(r.get("decisionType"))) for _, r in bundle.diav.iterrows() if t(r.get("decisionType"))})
        if "diavgeia_document_type" in selected_tables:
            log(f"Seeding decision types ({len(dt_rows)})...")
            execute_values(cur, "INSERT INTO public.diavgeia_document_type (decision_uid, decision_type) VALUES %s ON CONFLICT (decision_uid) DO UPDATE SET decision_type = EXCLUDED.decision_type", dt_rows)
            conn.commit()
            log("Decision types seed committed")

        # Insert procurement rows one by one to capture procurement.id and insert CPV rows per procurement.
        procurement_insert_sql = """
            INSERT INTO public.procurement (
              title, reference_number, prev_reference_no, notice_reference_number,
              next_ref_no, next_extended, next_modified,
              submission_at, contract_signed_date, start_date, no_end_date,
              end_date, cancelled, cancellation_date, cancellation_type, cancellation_reason,
              decision_related_ada, contract_number, organization_vat_number, greek_organization_vat_number,
              diavgeia_ada, budget, contract_budget, bids_submitted, max_bids_submitted, number_of_sections,
              central_government_authority, procedure_type_key, procedure_type_value, award_procedure,
              centralized_markets, contract_type, assign_criteria,
              classification_of_public_law_organization, type_of_contracting_authority,
              contracting_authority_activity, contract_duration, contract_duration_unit_of_measure,
              contract_related_ada, funding_details_cofund, funding_details_self_fund, funding_details_espa,
              funding_details_regular_budget, units_operator, short_descriptions, green_contracts,
              auction_ref_no, ingested_at, region_key, organization_key, municipality_key, canonical_owner_scope
            ) VALUES (
              %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s,
              %s, %s,
              %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (reference_number) DO UPDATE SET
              title = EXCLUDED.title,
              prev_reference_no = EXCLUDED.prev_reference_no,
              notice_reference_number = EXCLUDED.notice_reference_number,
              next_ref_no = EXCLUDED.next_ref_no,
              next_extended = EXCLUDED.next_extended,
              next_modified = EXCLUDED.next_modified,
              submission_at = EXCLUDED.submission_at,
              contract_signed_date = EXCLUDED.contract_signed_date,
              start_date = EXCLUDED.start_date,
              no_end_date = EXCLUDED.no_end_date,
              end_date = EXCLUDED.end_date,
              cancelled = EXCLUDED.cancelled,
              cancellation_date = EXCLUDED.cancellation_date,
              cancellation_type = EXCLUDED.cancellation_type,
              cancellation_reason = EXCLUDED.cancellation_reason,
              decision_related_ada = EXCLUDED.decision_related_ada,
              contract_number = EXCLUDED.contract_number,
              organization_vat_number = EXCLUDED.organization_vat_number,
              greek_organization_vat_number = EXCLUDED.greek_organization_vat_number,
              diavgeia_ada = EXCLUDED.diavgeia_ada,
              budget = EXCLUDED.budget,
              contract_budget = EXCLUDED.contract_budget,
              bids_submitted = EXCLUDED.bids_submitted,
              max_bids_submitted = EXCLUDED.max_bids_submitted,
              number_of_sections = EXCLUDED.number_of_sections,
              central_government_authority = EXCLUDED.central_government_authority,
              procedure_type_key = EXCLUDED.procedure_type_key,
              procedure_type_value = EXCLUDED.procedure_type_value,
              award_procedure = EXCLUDED.award_procedure,
              centralized_markets = EXCLUDED.centralized_markets,
              contract_type = EXCLUDED.contract_type,
              assign_criteria = EXCLUDED.assign_criteria,
              classification_of_public_law_organization = EXCLUDED.classification_of_public_law_organization,
              type_of_contracting_authority = EXCLUDED.type_of_contracting_authority,
              contracting_authority_activity = EXCLUDED.contracting_authority_activity,
              contract_duration = EXCLUDED.contract_duration,
              contract_duration_unit_of_measure = EXCLUDED.contract_duration_unit_of_measure,
              contract_related_ada = EXCLUDED.contract_related_ada,
              funding_details_cofund = EXCLUDED.funding_details_cofund,
              funding_details_self_fund = EXCLUDED.funding_details_self_fund,
              funding_details_espa = EXCLUDED.funding_details_espa,
              funding_details_regular_budget = EXCLUDED.funding_details_regular_budget,
              units_operator = EXCLUDED.units_operator,
              short_descriptions = EXCLUDED.short_descriptions,
              green_contracts = EXCLUDED.green_contracts,
              auction_ref_no = EXCLUDED.auction_ref_no,
              ingested_at = EXCLUDED.ingested_at,
              region_key = EXCLUDED.region_key,
              organization_key = EXCLUDED.organization_key,
              municipality_key = EXCLUDED.municipality_key,
              canonical_owner_scope = EXCLUDED.canonical_owner_scope,
              updated_at = NOW()
            RETURNING id
        """

        cpv_rows_to_insert: list[tuple[str, str | None, int]] = []
        payment_rows_to_insert: list[tuple] = []
        affected_superseded_references: set[str] = set()
        missing_cpv_keys: set[str] = set()
        if "procurement" in selected_tables:
            log("Loading existing procurement identity map...")
            cur.execute(
                """
                SELECT id, reference_number, diavgeia_ada, contract_number, title, submission_at, organization_key
                FROM public.procurement
                """
            )
            existing_proc_by_uid: dict[str, int] = {}
            for eid, ref, ada, cn, ttl, sub, orgk in cur.fetchall():
                uid = procurement_source_uid(ref, ada, cn, ttl, sub, orgk)
                if uid not in existing_proc_by_uid:
                    existing_proc_by_uid[uid] = int(eid)
            log(f"Existing procurement identities: {len(existing_proc_by_uid)}")

            total_proc = len(procurement)
            log(f"Inserting procurement rows one-by-one ({total_proc}) and collecting CPV/payment...")
            inserted_proc = 0
            skipped_proc = 0
            updated_existing_proc = 0
            reprocessed_existing_proc = 0
            for n, ((idx, row), p) in enumerate(zip(bundle.raw.iterrows(), procurement), start=1):
                uid = procurement_uid_from_proc_row(p)
                if uid in existing_proc_by_uid:
                    procurement_id = existing_proc_by_uid[uid]
                    skipped_proc += 1
                    if args.reprocess_existing_procurement:
                        # Optional full reprocess for already-known procurement identities.
                        cur.execute(
                            """
                            UPDATE public.procurement
                            SET region_key = %s,
                                organization_key = %s,
                                municipality_key = %s
                            WHERE id = %s
                            """,
                            (p[48], p[49], p[50], procurement_id),
                        )
                        updated_existing_proc += cur.rowcount
                        reprocessed_existing_proc += 1
                else:
                    cur.execute(procurement_insert_sql, p)
                    procurement_id = cur.fetchone()[0]
                    existing_proc_by_uid[uid] = procurement_id
                    inserted_proc += 1

                affected_superseded_references.update(affected_reference_numbers_for_row(row))
                keys = [x.strip() for x in str(row.get("cpv_keys") or "").split("|")] if t(row.get("cpv_keys")) else []
                values = [x.strip() for x in str(row.get("cpv_values") or "").split("|")] if t(row.get("cpv_values")) else []
                for idx_key, key in enumerate(keys):
                    key_clean = t(key)
                    if not key_clean:
                        continue
                    value_from_raw = t(values[idx_key]) if idx_key < len(values) else None
                    value_clean = value_from_raw or DEFAULT_CPVS.get(key_clean)
                    if value_clean is None:
                        missing_cpv_keys.add(key_clean)
                    cpv_rows_to_insert.append((key_clean, value_clean, procurement_id))

                payment_rows_to_insert.append(payment_row_from_raw(row, procurement_id))
                if n % 500 == 0 or n == total_proc:
                    log(f"Procurement progress: {n}/{total_proc}")
            conn.commit()
            log(
                "Procurement stage committed "
                f"(inserted={inserted_proc}, existing_skipped={skipped_proc}, "
                f"reprocessed_existing={reprocessed_existing_proc}, updated_existing={updated_existing_proc})"
            )
            prune_excluded_procurements(cur, conn, DEFAULT_EXCLUDE_KEYWORDS, dry_run=args.dry_run)

        if "cpv" in selected_tables:
            log(f"Upserting CPV rows ({len(cpv_rows_to_insert)})...")
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO public.cpv (cpv_key, cpv_value, procurement_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (procurement_id, cpv_key)
                DO UPDATE SET cpv_value = EXCLUDED.cpv_value
                """,
                cpv_rows_to_insert,
                page_size=1000,
            )
            conn.commit()
            log("CPV upsert committed")
        if missing_cpv_keys:
            print(f"Warning: {len(missing_cpv_keys)} CPV keys missing value in BOTH raw csv cpv_values and DEFAULT_CPVS.")
            sample_missing = sorted(missing_cpv_keys)[:20]
            print("Sample missing CPVs:", ", ".join(sample_missing))

        if "diavgeia" in selected_tables:
            diav_with_ada = [r for r in diav if t(r[3])]
            diav_without_ada = [r for r in diav if not t(r[3])]

            log(f"Upserting diavgeia rows with ADA ({len(diav_with_ada)})...")
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO public.diavgeia (
                  region_key, organization_key, municipality_key, ada, protocol_number,
                  submission_timestamp, publish_timestamp, status, non_revokable, document_url,
                  subject, document_type, version_comment, thematic_categories, organization,
                  cooperating_organizations, unit_ids, org, org_type, org_name_clean,
                  spending_signers, spending_contractors_afm, spending_contractors_name,
                  spending_contractors_value, diavgeia_document_type_decision_uid
                ) VALUES (
                  %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s
                )
                ON CONFLICT (ada) DO UPDATE SET
                  region_key = EXCLUDED.region_key,
                  organization_key = EXCLUDED.organization_key,
                  municipality_key = EXCLUDED.municipality_key,
                  protocol_number = EXCLUDED.protocol_number,
                  submission_timestamp = EXCLUDED.submission_timestamp,
                  publish_timestamp = EXCLUDED.publish_timestamp,
                  status = EXCLUDED.status,
                  non_revokable = EXCLUDED.non_revokable,
                  document_url = EXCLUDED.document_url,
                  subject = EXCLUDED.subject,
                  document_type = EXCLUDED.document_type,
                  version_comment = EXCLUDED.version_comment,
                  thematic_categories = EXCLUDED.thematic_categories,
                  organization = EXCLUDED.organization,
                  cooperating_organizations = EXCLUDED.cooperating_organizations,
                  unit_ids = EXCLUDED.unit_ids,
                  org = EXCLUDED.org,
                  org_type = EXCLUDED.org_type,
                  org_name_clean = EXCLUDED.org_name_clean,
                  spending_signers = EXCLUDED.spending_signers,
                  spending_contractors_afm = EXCLUDED.spending_contractors_afm,
                  spending_contractors_name = EXCLUDED.spending_contractors_name,
                  spending_contractors_value = EXCLUDED.spending_contractors_value,
                  diavgeia_document_type_decision_uid = EXCLUDED.diavgeia_document_type_decision_uid
                """,
                diav_with_ada,
                page_size=500,
            )
            if diav_without_ada:
                log(f"Inserting diavgeia rows without ADA ({len(diav_without_ada)}) with NOT EXISTS guard...")
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    INSERT INTO public.diavgeia (
                      region_key, organization_key, municipality_key, ada, protocol_number,
                      submission_timestamp, publish_timestamp, status, non_revokable, document_url,
                      subject, document_type, version_comment, thematic_categories, organization,
                      cooperating_organizations, unit_ids, org, org_type, org_name_clean,
                      spending_signers, spending_contractors_afm, spending_contractors_name,
                      spending_contractors_value, diavgeia_document_type_decision_uid
                    )
                    SELECT
                      %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                      SELECT 1
                      FROM public.diavgeia d
                      WHERE d.ada IS NULL
                        AND COALESCE(d.protocol_number, '') = COALESCE(%s, '')
                        AND COALESCE(d.submission_timestamp::text, '') = COALESCE(%s::text, '')
                        AND COALESCE(d.publish_timestamp::text, '') = COALESCE(%s::text, '')
                        AND COALESCE(d.subject, '') = COALESCE(%s, '')
                        AND COALESCE(d.org_name_clean, '') = COALESCE(%s, '')
                        AND COALESCE(d.document_url, '') = COALESCE(%s, '')
                    )
                    """,
                    [
                        (
                            *r,
                            r[4],   # protocol_number
                            r[5],   # submission_timestamp
                            r[6],   # publish_timestamp
                            r[10],  # subject
                            r[19],  # org_name_clean
                            r[9],   # document_url
                        )
                        for r in diav_without_ada
                    ],
                    page_size=200,
                )
            conn.commit()
            log("Diavgeia upsert committed")

        if "payment" in selected_tables:
            log(f"Upserting payment rows ({len(payment_rows_to_insert)})...")
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO public.payment (
                  procurement_id, diavgeia_document_type_decision_uid, diavgeia_id,
                  beneficiaries_count, signers, beneficiary_name, beneficiary_vat_number,
                  amount_with_vat, amount_without_vat, kae_ale, fiscal_year,
                  budget_category, counter_party, payment_ref_no
                ) VALUES (
                  %s, %s, %s,
                  %s, %s, %s, %s,
                  %s, %s, %s, %s,
                  %s, %s, %s
                )
                ON CONFLICT (procurement_id) DO UPDATE SET
                  diavgeia_document_type_decision_uid = EXCLUDED.diavgeia_document_type_decision_uid,
                  diavgeia_id = EXCLUDED.diavgeia_id,
                  beneficiaries_count = EXCLUDED.beneficiaries_count,
                  signers = EXCLUDED.signers,
                  beneficiary_name = EXCLUDED.beneficiary_name,
                  beneficiary_vat_number = EXCLUDED.beneficiary_vat_number,
                  amount_with_vat = EXCLUDED.amount_with_vat,
                  amount_without_vat = EXCLUDED.amount_without_vat,
                  kae_ale = EXCLUDED.kae_ale,
                  fiscal_year = EXCLUDED.fiscal_year,
                  budget_category = EXCLUDED.budget_category,
                  counter_party = EXCLUDED.counter_party,
                  payment_ref_no = EXCLUDED.payment_ref_no
                """,
                payment_rows_to_insert,
                page_size=1000,
            )
            log("Backfilling procurement.payment_id...")
            cur.execute(
                """
                UPDATE public.procurement p
                SET payment_id = py.id
                FROM public.payment py
                WHERE py.procurement_id = p.id
                  AND p.payment_id IS DISTINCT FROM py.id
                """
            )
            zeroed_payments = zero_superseded_payment_amounts(cur, affected_superseded_references)
            conn.commit()
            log(f"Payment upsert/backfill committed (zeroed_superseded_payments={zeroed_payments})")

        if "forest_fire" in selected_tables:
            fires_unique, skipped_fire_in_batch = dedupe_forest_fire_rows(fires)
            log("Loading existing forest_fire key digests...")
            existing_fire_keys = load_existing_forest_fire_digests(cur)
            fires_to_insert: list[tuple] = []
            skipped_fire_existing = 0
            for r in fires_unique:
                k = forest_fire_row_digest(r)
                if k in existing_fire_keys:
                    skipped_fire_existing += 1
                    continue
                existing_fire_keys.add(k)
                fires_to_insert.append(r)
            log(
                "Inserting forest_fire rows "
                f"({len(fires_to_insert)})... skipped existing: {skipped_fire_existing}, "
                f"skipped batch duplicates: {skipped_fire_in_batch}"
            )
            execute_values(
                cur,
                """
                INSERT INTO public.forest_fire (
                  municipality_key, region_key, year, date_start, date_end, nomos,
                  area_name, lat, lon, burned_forest_stremata, burned_woodland_stremata,
                  burned_grassland_stremata, burned_grove_stremata, burned_other_stremata,
                  burned_total_stremata, burned_total_ha, source
                ) VALUES %s
                """,
                fires_to_insert,
            )
            conn.commit()
            log("forest_fire insert committed")

        if "fund" in selected_tables:
            log("Loading existing fund keys...")
            cur.execute(
                """
                SELECT region_key, organization_key, municipality_key, year, allocation_type,
                       recipient_type, recipient_raw, nomos, amount_eur, source_file, source_ada
                FROM public.fund
                """
            )
            existing_fund_keys = {tuple(key_norm(x) for x in row) for row in cur.fetchall()}
            funds_to_insert: list[tuple] = []
            skipped_fund = 0
            for r in funds:
                k = tuple(key_norm(x) for x in r)
                if k in existing_fund_keys:
                    skipped_fund += 1
                    continue
                existing_fund_keys.add(k)
                funds_to_insert.append(r)
            log(f"Inserting fund rows ({len(funds_to_insert)})... skipped existing: {skipped_fund}")
            execute_values(
                cur,
                """
                INSERT INTO public.fund (
                  region_key, organization_key, municipality_key, year, allocation_type,
                  recipient_type, recipient_raw, nomos, amount_eur, source_file, source_ada
                ) VALUES %s
                """,
                funds_to_insert,
            )
            conn.commit()
            log("fund insert committed")

        # Bridge: diavgeia <-> procurement via ADA match.
        if "diavgeia_procurement" in selected_tables:
            log("Building diavgeia_procurement bridge...")
            cur.execute(
                """
                INSERT INTO public.diavgeia_procurement (diavgeia_id, procurement_id)
                SELECT d.id, p.id
                FROM public.procurement p
                JOIN public.diavgeia d ON d.ada = p.diavgeia_ada
                ON CONFLICT DO NOTHING
                """
            )
            conn.commit()
            log("diavgeia_procurement bridge committed")

        # Beneficiaries come from both KIMDIS-derived payment rows and Diavgeia payment beneficiaries.
        if "beneficiary" in selected_tables:
            beneficiary_candidates: dict[str, tuple[datetime, int, str | None]] = {}

            def register_beneficiary_candidate(
                afm: str | None,
                name: str | None,
                happened_at: datetime | None,
                source_priority: int,
            ) -> None:
                afm_clean = t(afm)
                if afm_clean is None:
                    return
                name_clean = t(name)
                candidate_key = (
                    happened_at if happened_at is not None else datetime.min,
                    source_priority,
                    name_clean,
                )
                existing = beneficiary_candidates.get(afm_clean)
                if existing is None or candidate_key > existing:
                    beneficiary_candidates[afm_clean] = candidate_key

            diav_bene_rows = []
            for _, r in bundle.diav.iterrows():
                ada = t(r.get("ada"))
                happened_at = parse_any_datetime(r.get("publishTimestamp")) or parse_any_datetime(r.get("submissionTimestamp"))
                afms = [t(value) for value in parse_serialized_list(r.get("payment_beneficiary_afm"))]
                names = [t(value) for value in parse_serialized_list(r.get("payment_beneficiary_name"))]
                for index in range(max(len(afms), len(names))):
                    afm = afms[index] if index < len(afms) else None
                    name = names[index] if index < len(names) else None
                    if ada and afm:
                        diav_bene_rows.append((ada, afm, name))
                    register_beneficiary_candidate(afm, name, happened_at, 1)

            for _, r in bundle.raw.iterrows():
                happened_at = parse_any_datetime(r.get("contractSignedDate")) or parse_any_datetime(r.get("submissionDate"))
                for afm, name in raw_contracting_members(r):
                    register_beneficiary_candidate(afm, name, happened_at, 2)

            cur.execute(
                """
                SELECT
                  py.id,
                  p.reference_number,
                  p.diavgeia_ada,
                  p.contract_number,
                  p.title,
                  p.submission_at,
                  p.organization_key
                FROM public.payment py
                JOIN public.procurement p
                  ON p.id = py.procurement_id
                """
            )
            payment_id_by_uid = {
                procurement_source_uid(reference_number, diavgeia_ada, contract_number, title, submission_at, organization_key): int(payment_id)
                for payment_id, reference_number, diavgeia_ada, contract_number, title, submission_at, organization_key in cur.fetchall()
            }
            payment_bene_rows: list[tuple[int, str]] = []
            for _, r in bundle.raw.iterrows():
                payment_id = payment_id_by_uid.get(
                    procurement_source_uid(
                        r.get("referenceNumber"),
                        r.get("diavgeiaADA"),
                        r.get("contractNumber"),
                        r.get("title"),
                        ts_iso(r.get("submissionDate")),
                        r.get("organization_key"),
                    )
                )
                if payment_id is None:
                    continue
                for afm, _ in raw_contracting_members(r):
                    afm_clean = t(afm)
                    if afm_clean is not None:
                        payment_bene_rows.append((payment_id, afm_clean))

            beneficiary_upserts = [
                (afm, candidate[2])
                for afm, candidate in beneficiary_candidates.items()
            ]

            if beneficiary_upserts:
                log(
                    f"Upserting beneficiaries ({len(beneficiary_upserts)}) "
                    f"using most recent available name per AFM..."
                )
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    INSERT INTO public.beneficiary (beneficiary_vat_number, beneficiary_name)
                    VALUES (%s, %s)
                    ON CONFLICT (beneficiary_vat_number) DO UPDATE
                    SET beneficiary_name = COALESCE(EXCLUDED.beneficiary_name, public.beneficiary.beneficiary_name)
                    """,
                    beneficiary_upserts,
                    page_size=500,
                )

            if payment_bene_rows:
                log(f"Upserting payment_beneficiary bridge rows ({len(payment_bene_rows)})...")
                payment_ids = sorted({payment_id for payment_id, _ in payment_bene_rows})
                cur.execute(
                    """
                    DELETE FROM public.payment_beneficiary
                    WHERE payment_id = ANY(%s::bigint[])
                    """,
                    (payment_ids,),
                )
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    INSERT INTO public.payment_beneficiary (payment_id, beneficiary_vat_number)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    payment_bene_rows,
                    page_size=1000,
                )

            if diav_bene_rows:
                log(f"Upserting diavgeia_beneficiary bridge rows ({len(diav_bene_rows)})...")
                execute_values(
                    cur,
                    """
                    INSERT INTO public.diavgeia_beneficiary (diavgeia_id, beneficiary_vat_number)
                    SELECT d.id, v.beneficiary_vat_number
                    FROM (VALUES %s) AS v(ada, beneficiary_vat_number)
                    JOIN public.diavgeia d ON d.ada = v.ada
                    ON CONFLICT DO NOTHING
                    """,
                    [(ada, afm) for ada, afm, _ in diav_bene_rows if afm],
                )

            conn.commit()
            log("Beneficiary upsert/bridges committed")

        log("Selected table stages completed")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
