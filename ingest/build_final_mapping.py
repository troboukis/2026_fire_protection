"""
build_final_mapping.py
----------------------
Builds a unified cross-source mapping artifact that connects:
- Organizations from Diavgeia
- Organizations from raw procurements
- Municipalities from funding
- Municipalities from fires
- Municipalities from geo source-of-truth

Outputs:
- data/mappings/final_entity_mapping.csv
  (one row per source entity, compact coverage list)
- data/mappings/final_entity_mapping_expanded.csv
  (one row per source entity x municipality_id)
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Iterable
from difflib import SequenceMatcher

import pandas as pd

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_DIR / "data"
MAPPINGS_DIR = DATA_DIR / "mappings"
ARCHIVED_DIR = MAPPINGS_DIR / "archived"

DIAVGEIA_PATH = DATA_DIR / "2026_diavgeia_filtered.csv"
PROC_PATH = DATA_DIR / "raw_procurements.csv"
FUNDING_PATH = DATA_DIR / "funding" / "municipal_funding.csv"
FIRES_PATH = DATA_DIR / "fires" / "fire_incidents_unified.csv"
GEO_PATH = DATA_DIR / "geo" / "municipalities.geojson"

ORG_SINGLE_PATH = MAPPINGS_DIR / "org_to_municipality.csv"
ORG_COVERAGE_PATH = MAPPINGS_DIR / "org_to_municipality_coverage.csv"
REGION_REF_PATH = MAPPINGS_DIR / "region_to_municipalities.csv"
REVIEW_FILLED_PATH = ARCHIVED_DIR / "raw_unmapped_orgs_review_filled.csv"

OUT_COMPACT = MAPPINGS_DIR / "final_entity_mapping.csv"
OUT_EXPANDED = MAPPINGS_DIR / "final_entity_mapping_expanded.csv"


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", str(s)) if unicodedata.category(c) != "Mn")


def normalize_text(s: str) -> str:
    s = strip_accents(str(s or "")).upper().strip()
    s = s.replace("–", "-")
    s = re.sub(r"[\[\]\(\)\{\}\'\"`.,]", " ", s)
    s = re.sub(r"\s*[-/]\s*", " - ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


PREFIXES = (
    "ΔΗΜΟΣ ",
    "ΔΗΜΟΥ ",
    "Δ ",
    "Δ. ",
    "∆ΗΜΟΣ ",
    "∆ΗΜΟΥ ",
    "∆. ",
    "ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ ",
    "ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ",
    "ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ",
    "ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ",
    "ΠΕΡΙΦΕΡΕΙΑ ",
    "ΠΕΡΙΦΕΡΕΙΑΣ ",
    "ΥΠΟΥΡΓΕΙΟ ",
    "ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ ",
)


def strip_known_prefixes(norm_text: str) -> str:
    s = norm_text
    for p in PREFIXES:
        if s.startswith(p):
            return s[len(p):].strip()
    return s


def clean_fires_municipality(val: str) -> str:
    s = str(val or "").strip()
    s = re.sub(r"^\[", "", s)
    s = re.sub(r"\]$", "", s)
    s = s.strip("'\" ")
    return s


def parse_ids(raw: str) -> list[str]:
    s = str(raw or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(";") if x.strip()]


def load_geo() -> tuple[pd.DataFrame, dict[str, str], dict[str, str]]:
    gj = json.loads(GEO_PATH.read_text())
    rows = []
    for ft in gj.get("features", []):
        props = ft.get("properties", {})
        mid = str(props.get("municipality_code", "")).strip()
        name = str(props.get("name", "")).strip()
        if not mid or not name:
            continue
        rows.append({"municipality_id": mid, "municipality_name": name})

    geo_df = pd.DataFrame(rows).drop_duplicates().sort_values("municipality_id").reset_index(drop=True)

    by_norm = {}
    for _, r in geo_df.iterrows():
        by_norm[normalize_text(r["municipality_name"])] = r["municipality_id"]

    id_to_name = {r["municipality_id"]: r["municipality_name"] for _, r in geo_df.iterrows()}
    return geo_df, by_norm, id_to_name


def load_region_lookup() -> tuple[dict[str, list[str]], dict[str, str], pd.DataFrame]:
    ref = pd.read_csv(REGION_REF_PATH, dtype=str).fillna("")
    ref["region_id"] = ref["region_id"].astype(str).str.strip()
    ref["municipality_id"] = ref["municipality_id"].astype(str).str.strip()
    ref = ref[(ref["region_id"] != "") & (ref["municipality_id"] != "")]

    region_to_munis: dict[str, list[str]] = (
        ref.groupby("region_id")["municipality_id"]
        .apply(lambda s: sorted(set(s.tolist())))
        .to_dict()
    )

    muni_to_region = (
        ref.drop_duplicates(subset=["municipality_id"])
        .set_index("municipality_id")["region_id"]
        .to_dict()
    )
    return region_to_munis, muni_to_region, ref


def build_org_coverage_index(
    all_municipality_ids: list[str],
    region_to_munis: dict[str, list[str]],
) -> dict[str, list[str]]:
    cov = pd.read_csv(ORG_COVERAGE_PATH, dtype=str).fillna("")
    cov["org_name_clean"] = cov["org_name_clean"].astype(str).str.strip()
    cov["municipality_id"] = cov["municipality_id"].astype(str).str.strip()

    index: dict[str, set[str]] = {}
    for _, r in cov.iterrows():
        key = normalize_text(r["org_name_clean"])
        mid = r["municipality_id"]
        if not key or not mid:
            continue
        index.setdefault(key, set()).add(mid)

    # Enrich from archived manual review file if present
    if REVIEW_FILLED_PATH.exists():
        rev = pd.read_csv(REVIEW_FILLED_PATH, dtype=str).fillna("")
        for _, r in rev.iterrows():
            org_name = str(r.get("org_name_clean", "")).strip()
            if not org_name:
                continue
            key = normalize_text(org_name)
            t = str(r.get("target_mapping_type", "")).strip().lower()

            mids: list[str] = []
            if t == "municipality":
                mids = parse_ids(r.get("target_municipality_id", ""))
            elif t == "region":
                for reg in parse_ids(r.get("target_region_id", "")):
                    mids.extend(region_to_munis.get(reg, []))
            elif t == "decentralized":
                mids = parse_ids(r.get("target_coverage_municipality_ids", ""))
                if not mids:
                    for reg in parse_ids(r.get("target_region_id", "")):
                        mids.extend(region_to_munis.get(reg, []))
            elif t == "national":
                mids = all_municipality_ids
            elif t in {"skip", "other", ""}:
                mids = []

            if mids:
                index.setdefault(key, set()).update(mids)

    return {k: sorted(v) for k, v in index.items()}


def build_municipality_alias_index(
    geo_name_norm_to_id: dict[str, str],
    region_ref_df: pd.DataFrame,
) -> dict[str, str]:
    idx = dict(geo_name_norm_to_id)

    # Add municipality aliases from org single mapping where direct municipality_id exists.
    single = pd.read_csv(ORG_SINGLE_PATH, dtype=str).fillna("")
    for _, r in single.iterrows():
        mid = str(r.get("municipality_id", "")).strip()
        if not mid:
            continue
        name = str(r.get("org_name_clean", "")).strip()
        if not name:
            continue
        idx[normalize_text(name)] = mid
        stripped = strip_known_prefixes(normalize_text(name))
        if stripped:
            idx.setdefault(stripped, mid)

    # Add aliases from region reference helper columns (PDF / legacy labels).
    for _, r in region_ref_df.iterrows():
        mid = str(r.get("municipality_id", "")).strip()
        if not mid:
            continue
        for col in ("municipality_name", "pdf_municipality_name"):
            raw = str(r.get(col, "")).strip()
            if not raw:
                continue
            n = normalize_text(raw)
            idx.setdefault(n, mid)
            s = strip_known_prefixes(n)
            if s:
                idx.setdefault(s, mid)

    return idx


def resolve_org_coverage_ids(org_name: str, org_cov_index: dict[str, list[str]]) -> tuple[list[str], str]:
    raw = str(org_name or "").strip()
    raw_variants = [
        raw,
        re.sub(r"\([^)]*\)", " ", raw),  # drop parenthetical acronyms
        raw.replace("&", " ΚΑΙ "),
        re.sub(r"\([^)]*\)", " ", raw).replace("&", " ΚΑΙ "),
    ]

    seen: set[str] = set()
    norm_variants: list[str] = []
    for v in raw_variants:
        nv = normalize_text(v)
        if nv and nv not in seen:
            seen.add(nv)
            norm_variants.append(nv)
            sv = strip_known_prefixes(nv)
            if sv and sv not in seen:
                seen.add(sv)
                norm_variants.append(sv)

    for n in norm_variants:
        if n in org_cov_index:
            return org_cov_index[n], "org_coverage_exact_or_variant"

    return [], "unmapped"


def resolve_municipality_id(name: str, muni_alias_index: dict[str, str]) -> tuple[str, str]:
    n = normalize_text(name)
    if n in muni_alias_index:
        return muni_alias_index[n], "municipality_exact"

    stripped = strip_known_prefixes(n)
    if stripped in muni_alias_index:
        return muni_alias_index[stripped], "municipality_prefix_stripped"

    return "", "unmapped"


def resolve_municipality_id_fuzzy(
    name: str,
    muni_alias_index: dict[str, str],
    id_to_name: dict[str, str],
) -> tuple[str, str]:
    """Fuzzy municipality fallback used only when direct matching fails."""
    n = normalize_text(name)
    if not n:
        return "", "unmapped"

    # Fast exact-style retries first.
    mid, strategy = resolve_municipality_id(n, muni_alias_index)
    if mid:
        return mid, strategy

    candidates = []
    for alias_norm, candidate_id in muni_alias_index.items():
        if not alias_norm:
            continue
        score = SequenceMatcher(None, n, alias_norm).ratio()
        candidates.append((score, alias_norm, candidate_id))

    if not candidates:
        return "", "unmapped"

    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, _best_alias, best_id = candidates[0]
    second_score = candidates[1][0] if len(candidates) > 1 else 0.0

    # Conservative acceptance to avoid false positives.
    if best_score >= 0.90 and (best_score - second_score) >= 0.04:
        return best_id, "municipality_fuzzy_high_confidence"

    # Slightly lower threshold if candidate's official municipality name is
    # a strong fuzzy match too.
    official = normalize_text(id_to_name.get(best_id, ""))
    off_score = SequenceMatcher(None, n, official).ratio() if official else 0.0
    if best_score >= 0.86 and off_score >= 0.86 and (best_score - second_score) >= 0.05:
        return best_id, "municipality_fuzzy_official_match"

    return "", "unmapped"


def compress_rows(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    group_cols = [
        "source_system",
        "source_entity_type",
        "source_key",
        "source_value",
        "normalized_value",
        "match_strategy",
        "notes",
    ]

    def uniq_join(values: Iterable[str]) -> str:
        return ";".join(sorted({str(v).strip() for v in values if str(v).strip()}))

    out = (
        df.groupby(group_cols, dropna=False)
        .agg(
            target_coverage_municipality_ids=("municipality_id", uniq_join),
            target_coverage_municipality_names=("municipality_name", uniq_join),
            target_coverage_region_ids=("region_id", uniq_join),
            coverage_size=("municipality_id", lambda s: len({x for x in s if str(x).strip()})),
        )
        .reset_index()
    )
    out["is_mapped"] = (out["coverage_size"] > 0).astype(int)
    return out


def main() -> None:
    geo_df, geo_name_norm_to_id, id_to_name = load_geo()
    region_to_munis, muni_to_region, region_ref_df = load_region_lookup()

    all_municipality_ids = sorted(geo_df["municipality_id"].astype(str).unique().tolist())
    org_cov_index = build_org_coverage_index(all_municipality_ids, region_to_munis)
    muni_alias_index = build_municipality_alias_index(geo_name_norm_to_id, region_ref_df)

    expanded_rows: list[dict] = []

    # 1) Geo municipalities (source of truth)
    for _, r in geo_df.iterrows():
        mid = r["municipality_id"]
        expanded_rows.append({
            "source_system": "geo",
            "source_entity_type": "municipality",
            "source_key": mid,
            "source_value": r["municipality_name"],
            "normalized_value": normalize_text(r["municipality_name"]),
            "municipality_id": mid,
            "municipality_name": r["municipality_name"],
            "region_id": muni_to_region.get(mid, ""),
            "match_strategy": "geo_source_of_truth",
            "notes": "",
        })

    # 2) Diavgeia organizations
    d = pd.read_csv(DIAVGEIA_PATH, dtype=str).fillna("")
    d_pairs = d[["org_type", "org_name_clean"]].drop_duplicates()
    for _, r in d_pairs.iterrows():
        org_name = str(r["org_name_clean"]).strip()
        org_type = str(r["org_type"]).strip()
        mids, strategy = resolve_org_coverage_ids(org_name, org_cov_index)
        if mids:
            for mid in mids:
                expanded_rows.append({
                    "source_system": "diavgeia",
                    "source_entity_type": "organization",
                    "source_key": f"{org_type}::{org_name}",
                    "source_value": org_name,
                    "normalized_value": normalize_text(org_name),
                    "municipality_id": mid,
                    "municipality_name": id_to_name.get(mid, ""),
                    "region_id": muni_to_region.get(mid, ""),
                    "match_strategy": strategy,
                    "notes": f"org_type={org_type}",
                })
        else:
            expanded_rows.append({
                "source_system": "diavgeia",
                "source_entity_type": "organization",
                "source_key": f"{org_type}::{org_name}",
                "source_value": org_name,
                "normalized_value": normalize_text(org_name),
                "municipality_id": "",
                "municipality_name": "",
                "region_id": "",
                "match_strategy": strategy,
                "notes": f"org_type={org_type}",
            })

    # 3) Procurements organizations
    p = pd.read_csv(PROC_PATH, dtype=str).fillna("")
    p_pairs = p[["organization_key", "organization_value"]].drop_duplicates()
    for _, r in p_pairs.iterrows():
        org_name = str(r["organization_value"]).strip()
        org_key = str(r["organization_key"]).strip()
        mids, strategy = resolve_org_coverage_ids(org_name, org_cov_index)
        if mids:
            for mid in mids:
                expanded_rows.append({
                    "source_system": "procurements",
                    "source_entity_type": "organization",
                    "source_key": org_key,
                    "source_value": org_name,
                    "normalized_value": normalize_text(org_name),
                    "municipality_id": mid,
                    "municipality_name": id_to_name.get(mid, ""),
                    "region_id": muni_to_region.get(mid, ""),
                    "match_strategy": strategy,
                    "notes": "organization_value",
                })
        else:
            expanded_rows.append({
                "source_system": "procurements",
                "source_entity_type": "organization",
                "source_key": org_key,
                "source_value": org_name,
                "normalized_value": normalize_text(org_name),
                "municipality_id": "",
                "municipality_name": "",
                "region_id": "",
                "match_strategy": strategy,
                "notes": "organization_value",
            })

    # 4) Funding recipients
    f = pd.read_csv(FUNDING_PATH, dtype=str).fillna("")
    f_pairs = f[["recipient_type", "recipient_raw"]].drop_duplicates()
    for _, r in f_pairs.iterrows():
        raw_name = str(r["recipient_raw"]).strip()
        recipient_type = str(r["recipient_type"]).strip()
        source_key = f"{recipient_type}::{raw_name}"

        # Municipal recipients are mapped by municipality name heuristics.
        if recipient_type == "δήμος":
            mid, strategy = resolve_municipality_id(raw_name, muni_alias_index)
            if not mid:
                mid, strategy = resolve_municipality_id_fuzzy(raw_name, muni_alias_index, id_to_name)
            expanded_rows.append({
                "source_system": "funding",
                "source_entity_type": "municipality_name",
                "source_key": source_key,
                "source_value": raw_name,
                "normalized_value": normalize_text(raw_name),
                "municipality_id": mid,
                "municipality_name": id_to_name.get(mid, "") if mid else "",
                "region_id": muni_to_region.get(mid, "") if mid else "",
                "match_strategy": strategy,
                "notes": f"recipient_type={recipient_type}",
            })
            continue

        # Non-municipal recipients (e.g. σύνδεσμος): map as organization coverage.
        mids, strategy = resolve_org_coverage_ids(raw_name, org_cov_index)
        if mids:
            for mid in mids:
                expanded_rows.append({
                    "source_system": "funding",
                    "source_entity_type": "organization",
                    "source_key": source_key,
                    "source_value": raw_name,
                    "normalized_value": normalize_text(raw_name),
                    "municipality_id": mid,
                    "municipality_name": id_to_name.get(mid, ""),
                    "region_id": muni_to_region.get(mid, ""),
                    "match_strategy": strategy,
                    "notes": f"recipient_type={recipient_type}",
                })
            continue

        # Fallback to municipality-style match if organization coverage is absent.
        mid, strategy = resolve_municipality_id(raw_name, muni_alias_index)
        if not mid:
            mid, strategy = resolve_municipality_id_fuzzy(raw_name, muni_alias_index, id_to_name)
        expanded_rows.append({
            "source_system": "funding",
            "source_entity_type": "municipality_name",
            "source_key": source_key,
            "source_value": raw_name,
            "normalized_value": normalize_text(raw_name),
            "municipality_id": mid,
            "municipality_name": id_to_name.get(mid, "") if mid else "",
            "region_id": muni_to_region.get(mid, "") if mid else "",
            "match_strategy": strategy,
            "notes": f"recipient_type={recipient_type}",
        })

    # 5) Fires municipalities (municipality_raw)
    fires = pd.read_csv(FIRES_PATH, dtype=str).fillna("")
    fires["municipality_clean"] = fires["municipality_raw"].map(clean_fires_municipality)
    if "municipality_id" in fires.columns:
        fire_pairs = fires[["municipality_clean", "municipality_id"]].drop_duplicates()
    else:
        fire_pairs = fires[["municipality_clean"]].drop_duplicates()
        fire_pairs["municipality_id"] = ""
    for _, r in fire_pairs.iterrows():
        raw_name = str(r["municipality_clean"]).strip()
        if not raw_name:
            continue
        precomputed_mid = str(r.get("municipality_id", "") or "").strip()
        if precomputed_mid:
            expanded_rows.append({
                "source_system": "fires",
                "source_entity_type": "municipality_name",
                "source_key": raw_name,
                "source_value": raw_name,
                "normalized_value": normalize_text(raw_name),
                "municipality_id": precomputed_mid,
                "municipality_name": id_to_name.get(precomputed_mid, ""),
                "region_id": muni_to_region.get(precomputed_mid, ""),
                "match_strategy": "fires_precomputed_municipality_id",
                "notes": "municipality_raw",
            })
            continue
        mid, strategy = resolve_municipality_id(raw_name, muni_alias_index)
        if not mid:
            mid, strategy = resolve_municipality_id_fuzzy(raw_name, muni_alias_index, id_to_name)
        expanded_rows.append({
            "source_system": "fires",
            "source_entity_type": "municipality_name",
            "source_key": raw_name,
            "source_value": raw_name,
            "normalized_value": normalize_text(raw_name),
            "municipality_id": mid,
            "municipality_name": id_to_name.get(mid, "") if mid else "",
            "region_id": muni_to_region.get(mid, "") if mid else "",
            "match_strategy": strategy,
            "notes": "municipality_raw",
        })

    expanded = pd.DataFrame(expanded_rows)
    expanded = expanded[
        [
            "source_system",
            "source_entity_type",
            "source_key",
            "source_value",
            "normalized_value",
            "municipality_id",
            "municipality_name",
            "region_id",
            "match_strategy",
            "notes",
        ]
    ]

    compact = compress_rows(expanded_rows)

    compact.to_csv(OUT_COMPACT, index=False)
    expanded.to_csv(OUT_EXPANDED, index=False)

    # Console summary
    print(f"[done] wrote {OUT_COMPACT}")
    print(f"[done] wrote {OUT_EXPANDED}")

    for src in ["diavgeia", "procurements", "funding", "fires", "geo"]:
        s = compact[compact["source_system"] == src]
        if s.empty:
            print(f"{src:12s}: 0 rows")
            continue
        mapped = int((s["is_mapped"] == 1).sum())
        total = int(len(s))
        print(f"{src:12s}: mapped {mapped}/{total} ({mapped/total*100:.1f}%)")


if __name__ == "__main__":
    main()
