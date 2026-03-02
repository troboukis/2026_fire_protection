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
import hashlib
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import runpy

load_dotenv()

REPO = Path(__file__).resolve().parent.parent
DEFAULT_CPVS: dict[str, str] = runpy.run_path(str(REPO / "src" / "fetch_kimdis_procurements.py"))["DEFAULT_CPVS"]

RAW_CSV = REPO / "data" / "raw_procurements.csv"
DIAV_CSV = REPO / "data" / "2026_diavgeia.csv"
FIRE_CSV = REPO / "data" / "fires" / "fire_incidents_unified.csv"
FUND_CSV = REPO / "data" / "funding" / "municipal_funding.csv"
ORG_MAP_CSV = REPO / "data" / "mappings" / "org_to_municipality.csv"
REGION_MAP_CSV = REPO / "data" / "mappings" / "region_to_municipalities.csv"
EXPANDED_MAP_CSV = REPO / "data" / "mappings" / "final_entity_mapping_expanded.csv"


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
    out: list[tuple] = []
    seen_pairs: set[tuple[str, str]] = set()

    # Main source for municipality variations.
    muni_rows = b.expanded_map[b.expanded_map["source_entity_type"].isin(["municipality", "municipality_name"])]
    for _, r in muni_rows.iterrows():
        municipality_key = t(r.get("municipality_id"))
        if not municipality_key:
            continue
        municipality_value = t(r.get("source_value")) or t(r.get("municipality_name")) or municipality_key
        municipality_normalized = t(r.get("normalized_value")) or t_up(municipality_value) or municipality_value
        k = (municipality_key, municipality_value)
        if k in seen_pairs:
            continue
        seen_pairs.add(k)
        out.append((
            municipality_key,
            municipality_value,
            municipality_normalized,
            t(r.get("source_system")),
            t(r.get("source_key")),
        ))

    # Add canonical municipality labels from region map.
    for _, r in b.region_map.iterrows():
        municipality_key = t(r.get("municipality_id"))
        if not municipality_key:
            continue
        municipality_value = t(r.get("municipality_name")) or municipality_key
        municipality_normalized = t_up(municipality_value) or municipality_value
        k = (municipality_key, municipality_value)
        if k in seen_pairs:
            continue
        seen_pairs.add(k)
        out.append((
            municipality_key,
            municipality_value,
            municipality_normalized,
            "region_to_municipalities",
            t(r.get("pdf_geo_code")),
        ))

    # Ensure keys present even if no name known.
    for df, col, source in (
        (b.org_map, "municipality_id", "org_to_municipality"),
        (b.fire, "municipality_id", "fire_incidents"),
        (b.fund, "municipality_code", "municipal_funding"),
    ):
        if col not in df.columns:
            continue
        for v in df[col].tolist():
            municipality_key = t(v)
            if not municipality_key:
                continue
            municipality_value = municipality_key
            municipality_normalized = municipality_key
            k = (municipality_key, municipality_value)
            if k in seen_pairs:
                continue
            seen_pairs.add(k)
            out.append((municipality_key, municipality_value, municipality_normalized, source, None))

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
        if mk:
            lookup[mk] = municipality_key
        if mv:
            lookup[mv] = municipality_key
        if mn and mn not in lookup:
            lookup[mn] = municipality_key
    return lookup


def organization_key_from_normalized(normalized_value: str) -> str:
    return f"org_{hashlib.sha1(normalized_value.encode('utf-8')).hexdigest()[:20]}"


def seed_organization_rows(b: CsvBundle) -> list[tuple]:
    out: list[tuple] = []
    seen_pairs: set[tuple[str, str]] = set()
    org_rows = b.expanded_map[b.expanded_map["source_entity_type"] == "organization"]
    for _, r in org_rows.iterrows():
        org_value = t(r.get("source_value"))
        if not org_value:
            continue
        normalized = t(r.get("normalized_value")) or t_up(org_value) or org_value
        dedup_key = (org_value, normalized)
        if dedup_key in seen_pairs:
            continue
        seen_pairs.add(dedup_key)
        out.append((
            organization_key_from_normalized(t_up(normalized) or normalized),
            org_value,
            normalized,
            t(r.get("source_system")),
            t(r.get("source_key")),
        ))
    return out


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


def procurement_rows(
    raw: pd.DataFrame,
    org_map: dict[tuple[str, str], tuple[str | None, str | None]],
    organization_lookup: dict[str, str],
    region_lookup: dict[str, str],
    municipality_lookup: dict[str, str],
) -> list[tuple]:
    out = []
    for _, r in raw.iterrows():
        org_name = t_up(r.get("organization_value")) or ""
        org_type = t_up(r.get("typeOfContractingAuthority")) or ""
        municipality_key_raw, region_key_raw = org_map.get((org_type, org_name), (None, None))
        if municipality_key_raw is None and region_key_raw is None and org_name:
            municipality_key_raw, region_key_raw = org_map.get(("", org_name), (None, None))
        if municipality_key_raw is None and region_key_raw is None and org_name:
            municipality_key_raw, region_key_raw = org_map.get((norm_key(org_type) or "", norm_key(org_name) or ""), (None, None))
        if municipality_key_raw is None and region_key_raw is None and org_name:
            municipality_key_raw, region_key_raw = org_map.get(("", norm_key(org_name) or ""), (None, None))
        municipality_key = municipality_lookup.get(t_up(municipality_key_raw) or "", municipality_key_raw)
        region_key = region_lookup.get(t_up(region_key_raw) or "", region_key_raw)
        org_value_raw = t(r.get("organization_value"))
        org_key_resolved = organization_lookup.get(t_up(org_value_raw) or "")
        out.append((
            t(r.get("title")),
            t(r.get("referenceNumber")),
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
    beneficiary_name = t(raw_row.get("firstMember_name"))
    submission_ts = ts_iso(raw_row.get("submissionDate"))
    fiscal_year = int(submission_ts[:4]) if submission_ts else None
    return (
        procurement_id,                              # procurement_id
        None,                                        # diavgeia_document_type_decision_uid
        None,                                        # diavgeia_id
        1 if beneficiary_name else None,             # beneficiaries_count
        t(raw_row.get("signers")),                   # signers
        beneficiary_name,                            # beneficiary_name
        t(raw_row.get("firstMember_vatNumber")),     # beneficiary_vat_number
        dec(raw_row.get("totalCostWithVAT")),        # amount_with_vat
        dec(raw_row.get("totalCostWithoutVAT")),     # amount_without_vat
        None,                                        # kae_ale (not provided in raw CSV)
        fiscal_year,                                 # fiscal_year
        t(raw_row.get("fundingDetails_regularBudget")),  # budget_category
        beneficiary_name,                            # counter_party
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
            t(r.get("region_id")),
            None,
            t(r.get("municipality_code")),
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
        default="all",
        help=(
            "comma-separated stages: region,municipality,organization,diavgeia_document_type,"
            "procurement,cpv,diavgeia,payment,forest_fire,fund,diavgeia_procurement,beneficiary "
            "(default: all)"
        ),
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
        diavgeia_ada=p[15],
        contract_number=p[12],
        title=p[0],
        submission_at=p[2],
        organization_key=p[44],
    )


def key_norm(v):
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    if v is None:
        return None
    return str(v)


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
    organization_lookup = build_organization_lookup(org_seed)

    procurement = procurement_rows(bundle.raw, org_map, organization_lookup, region_lookup, municipality_lookup)
    diav = diav_rows(bundle.diav, org_map, organization_lookup, region_lookup, municipality_lookup)
    fires = forest_fire_rows(bundle.fire)
    funds = fund_rows(bundle.fund)

    print("Prepared rows:")
    print(f"  region seed:       {len(region_seed)}")
    print(f"  municipality seed: {len(muni_seed)}")
    print(f"  organization seed: {len(org_seed)}")
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
    if ("cpv" in selected_tables or "payment" in selected_tables) and "procurement" not in selected_tables:
        raise ValueError("cpv/payment require procurement in the same run (use --tables including procurement)")

    db_url = os.environ["DATABASE_URL"]
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
            conn.commit()
            log("Region seed committed")
        if "municipality" in selected_tables:
            log(f"Seeding municipality rows ({len(muni_seed)})...")
            execute_values(
                cur,
                """
                INSERT INTO public.municipality (
                  municipality_key, municipality_value, municipality_normalized_value, source_system, source_key
                ) VALUES %s
                ON CONFLICT (municipality_key, municipality_value) DO UPDATE SET
                  municipality_normalized_value = EXCLUDED.municipality_normalized_value,
                  source_system = EXCLUDED.source_system,
                  source_key = EXCLUDED.source_key
                """,
                muni_seed,
            )
            conn.commit()
            log("Municipality seed committed")
        if "organization" in selected_tables:
            log(f"Seeding organization rows ({len(org_seed)})...")
            execute_values(
                cur,
                """
                INSERT INTO public.organization (
                  organization_key, organization_value, organization_normalized_value, source_system, source_key
                ) VALUES %s
                ON CONFLICT (organization_key, organization_value) DO UPDATE SET
                  organization_normalized_value = EXCLUDED.organization_normalized_value,
                  source_system = EXCLUDED.source_system,
                  source_key = EXCLUDED.source_key
                """,
                org_seed,
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
              title, reference_number, submission_at, contract_signed_date, start_date, no_end_date,
              end_date, cancelled, cancellation_date, cancellation_type, cancellation_reason,
              decision_related_ada, contract_number, organization_vat_number, greek_organization_vat_number,
              diavgeia_ada, budget, contract_budget, bids_submitted, max_bids_submitted, number_of_sections,
              central_government_authority, procedure_type_key, procedure_type_value, award_procedure,
              centralized_markets, contract_type, assign_criteria,
              classification_of_public_law_organization, type_of_contracting_authority,
              contracting_authority_activity, contract_duration, contract_duration_unit_of_measure,
              contract_related_ada, funding_details_cofund, funding_details_self_fund, funding_details_espa,
              funding_details_regular_budget, units_operator, short_descriptions, green_contracts,
              auction_ref_no, ingested_at, region_key, organization_key, municipality_key
            ) VALUES (
              %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s,
              %s, %s,
              %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s, %s
            )
            ON CONFLICT (reference_number) DO UPDATE SET
              title = EXCLUDED.title,
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
              updated_at = NOW()
            RETURNING id
        """

        cpv_rows_to_insert: list[tuple[str, str | None, int]] = []
        payment_rows_to_insert: list[tuple] = []
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
                process_row = True
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
                            (p[43], p[44], p[45], procurement_id),
                        )
                        updated_existing_proc += cur.rowcount
                        reprocessed_existing_proc += 1
                    else:
                        process_row = False
                else:
                    cur.execute(procurement_insert_sql, p)
                    procurement_id = cur.fetchone()[0]
                    existing_proc_by_uid[uid] = procurement_id
                    inserted_proc += 1

                if process_row:
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
            conn.commit()
            log("Payment upsert/backfill committed")

        if "forest_fire" in selected_tables:
            log("Loading existing forest_fire keys...")
            cur.execute(
                """
                SELECT municipality_key, region_key, year, date_start, date_end, nomos,
                       area_name, lat, lon, burned_forest_stremata, burned_woodland_stremata,
                       burned_grassland_stremata, burned_grove_stremata, burned_other_stremata,
                       burned_total_stremata, burned_total_ha, source
                FROM public.forest_fire
                """
            )
            existing_fire_keys = {tuple(key_norm(x) for x in row) for row in cur.fetchall()}
            fires_to_insert: list[tuple] = []
            skipped_fire = 0
            for r in fires:
                k = tuple(key_norm(x) for x in r)
                if k in existing_fire_keys:
                    skipped_fire += 1
                    continue
                existing_fire_keys.add(k)
                fires_to_insert.append(r)
            log(f"Inserting forest_fire rows ({len(fires_to_insert)})... skipped existing: {skipped_fire}")
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

        # Bridge: diavgeia <-> beneficiary (from payment beneficiary AFM in CSV).
        bene_rows = []
        for _, r in bundle.diav.iterrows():
            ada = t(r.get("ada"))
            afm = first_list_item(r.get("payment_beneficiary_afm")) or t(r.get("payment_beneficiary_afm"))
            name = first_list_item(r.get("payment_beneficiary_name")) or t(r.get("payment_beneficiary_name"))
            if ada and afm:
                bene_rows.append((ada, afm, name))
        if "beneficiary" in selected_tables and bene_rows:
            log(f"Upserting beneficiaries ({len(bene_rows)}) and diavgeia_beneficiary bridge...")
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO public.beneficiary (beneficiary_vat_number, beneficiary_name)
                VALUES (%s, %s)
                ON CONFLICT (beneficiary_vat_number) DO UPDATE
                SET beneficiary_name = COALESCE(EXCLUDED.beneficiary_name, public.beneficiary.beneficiary_name)
                """,
                [(x[1], x[2]) for x in bene_rows],
                page_size=500,
            )
            execute_values(
                cur,
                """
                INSERT INTO public.diavgeia_beneficiary (diavgeia_id, beneficiary_vat_number)
                SELECT d.id, v.beneficiary_vat_number
                FROM (VALUES %s) AS v(ada, beneficiary_vat_number)
                JOIN public.diavgeia d ON d.ada = v.ada
                ON CONFLICT DO NOTHING
                """,
                [(x[0], x[1]) for x in bene_rows],
            )
            conn.commit()
            log("Beneficiary upsert/bridge committed")

        log("Selected table stages completed")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
