"""
normalize_fires_municipalities.py
--------------------------------
Populate canonical municipality mapping in fires dataset.

Input:
  data/fires/fire_incidents_unified.csv

Output:
  - Overwrites input CSV adding:
      municipality_id
      municipality_name_canonical
      municipality_match_strategy
  - Writes diagnostics:
      data/mappings/fires_municipality_unmapped.csv
"""

from __future__ import annotations

import json
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

REPO_DIR = Path(__file__).resolve().parent.parent
FIRES_PATH = REPO_DIR / "data" / "fires" / "fire_incidents_unified.csv"
GEO_PATH = REPO_DIR / "data" / "geo" / "municipalities.geojson"
REGION_REF_PATH = REPO_DIR / "data" / "mappings" / "region_to_municipalities.csv"
ORG_SINGLE_PATH = REPO_DIR / "data" / "mappings" / "org_to_municipality.csv"
UNMAPPED_OUT = REPO_DIR / "data" / "mappings" / "fires_municipality_unmapped.csv"

PREFIXES = (
    "ΔΗΜΟΣ ",
    "ΔΗΜΟΥ ",
    "Δ ",
    "Δ. ",
    "∆ΗΜΟΣ ",
    "∆ΗΜΟΥ ",
    "∆. ",
)


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", str(s)) if unicodedata.category(c) != "Mn")


def normalize_text(s: str) -> str:
    s = strip_accents(str(s or "")).upper().strip()
    s = s.replace("–", "-")
    s = re.sub(r"[\[\]\(\)\{\}\'\"`.,]", " ", s)
    s = re.sub(r"\s*[-/]\s*", " - ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def strip_prefixes(n: str) -> str:
    s = n
    for p in PREFIXES:
        if s.startswith(p):
            return s[len(p):].strip()
    return s


def clean_municipality_raw(val: str) -> str:
    s = str(val or "").strip()
    s = re.sub(r"^\[", "", s)
    s = re.sub(r"\]$", "", s)
    s = s.strip("'\" ")
    return s


def build_geo_index() -> tuple[dict[str, str], dict[str, str]]:
    gj = json.loads(GEO_PATH.read_text())
    by_norm: dict[str, str] = {}
    id_to_name: dict[str, str] = {}

    for ft in gj.get("features", []):
        props = ft.get("properties", {})
        mid = str(props.get("municipality_code", "")).strip()
        name = str(props.get("name", "")).strip()
        if not mid or not name:
            continue
        by_norm[normalize_text(name)] = mid
        id_to_name[mid] = name

    return by_norm, id_to_name


def build_alias_index(geo_by_norm: dict[str, str]) -> dict[str, str]:
    idx = dict(geo_by_norm)

    ref = pd.read_csv(REGION_REF_PATH, dtype=str).fillna("")
    for _, r in ref.iterrows():
        mid = str(r.get("municipality_id", "")).strip()
        if not mid:
            continue
        for col in ("municipality_name", "pdf_municipality_name"):
            raw = str(r.get(col, "")).strip()
            if not raw:
                continue
            n = normalize_text(raw)
            idx.setdefault(n, mid)
            s = strip_prefixes(n)
            if s:
                idx.setdefault(s, mid)

    single = pd.read_csv(ORG_SINGLE_PATH, dtype=str).fillna("")
    for _, r in single.iterrows():
        mid = str(r.get("municipality_id", "")).strip()
        if not mid:
            continue
        raw = str(r.get("org_name_clean", "")).strip()
        if not raw:
            continue
        n = normalize_text(raw)
        idx.setdefault(n, mid)
        s = strip_prefixes(n)
        if s:
            idx.setdefault(s, mid)

    return idx


def resolve_id(raw_name: str, alias_index: dict[str, str], id_to_name: dict[str, str]) -> tuple[str, str]:
    n = normalize_text(raw_name)
    if not n:
        return "", "empty"

    if n in alias_index:
        return alias_index[n], "exact"

    s = strip_prefixes(n)
    if s in alias_index:
        return alias_index[s], "prefix_stripped"

    # Conservative fuzzy fallback
    candidates = []
    for alias_norm, candidate_id in alias_index.items():
        score = SequenceMatcher(None, n, alias_norm).ratio()
        candidates.append((score, alias_norm, candidate_id))

    if not candidates:
        return "", "unmapped"

    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, _best_alias, best_id = candidates[0]
    second_score = candidates[1][0] if len(candidates) > 1 else 0.0
    if best_score >= 0.90 and (best_score - second_score) >= 0.04:
        return best_id, "fuzzy_high_confidence"

    official = normalize_text(id_to_name.get(best_id, ""))
    off_score = SequenceMatcher(None, n, official).ratio() if official else 0.0
    if best_score >= 0.86 and off_score >= 0.86 and (best_score - second_score) >= 0.05:
        return best_id, "fuzzy_official_match"

    return "", "unmapped"


def main() -> None:
    df = pd.read_csv(FIRES_PATH, dtype=str).fillna("")

    geo_by_norm, id_to_name = build_geo_index()
    alias_index = build_alias_index(geo_by_norm)

    cleaned_raw_values: list[str] = []
    municipality_ids: list[str] = []
    municipality_names: list[str] = []
    strategies: list[str] = []

    for _, r in df.iterrows():
        raw = clean_municipality_raw(r.get("municipality_raw", ""))
        cleaned_raw_values.append(raw)
        mid, strat = resolve_id(raw, alias_index, id_to_name)
        municipality_ids.append(mid)
        municipality_names.append(id_to_name.get(mid, "") if mid else "")
        strategies.append(strat)

    # Persist cleaned raw municipality string (e.g. remove literal [] wrappers).
    df["municipality_raw"] = cleaned_raw_values
    df["municipality_id"] = municipality_ids
    df["municipality_name_canonical"] = municipality_names
    df["municipality_match_strategy"] = strategies
    df.to_csv(FIRES_PATH, index=False)

    unmapped = (
        df[df["municipality_id"].astype(str).str.strip() == ""]
        .groupby("municipality_raw", dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values("row_count", ascending=False)
    )
    unmapped.to_csv(UNMAPPED_OUT, index=False)

    total_rows = len(df)
    mapped_rows = int((df["municipality_id"].astype(str).str.strip() != "").sum())

    unique_raw = df["municipality_raw"].astype(str).map(clean_municipality_raw)
    unique_total = int(unique_raw.nunique())
    unique_mapped = int(unique_raw[df["municipality_id"].astype(str).str.strip() != ""].nunique())

    print(f"[done] updated {FIRES_PATH}")
    print(f"[done] wrote unmapped diagnostics {UNMAPPED_OUT}")
    print(f"rows mapped: {mapped_rows}/{total_rows} ({(mapped_rows/total_rows*100):.1f}%)")
    print(f"unique municipality_raw mapped: {unique_mapped}/{unique_total} ({(unique_mapped/unique_total*100):.1f}%)")
    print("\nmatch strategy counts:")
    print(df["municipality_match_strategy"].value_counts().to_string())


if __name__ == "__main__":
    main()
