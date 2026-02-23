"""
build_region_municipality_reference.py
--------------------------------------
Build `data/mappings/region_to_municipalities.csv` from ELSTAT population XLSX
(`mon_plith_2021.xlsx`) by parsing the hierarchy:

- level 3 -> ΠΕΡΙΦΕΡΕΙΑ ...
- level 5 -> ΔΗΜΟΣ ...

Then matches PDF municipality names to this project's municipality GeoJSON IDs
using the same normalization / overrides as `build_org_mapping.py`.

This file is used as the reference dataset for expanding:
org -> region(s) -> municipalities
in `ingest/build_org_mapping.py`.
"""

from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pandas as pd

from build_org_mapping import (
    GEOJSON_PATH,
    REPO_DIR,
    DIMOS_OVERRIDES,
    canonicalize_region_name,
    norm,
)

OUTPUT_PATH = REPO_DIR / "data" / "mappings" / "region_to_municipalities.csv"
DEFAULT_XLSX = Path.home() / "Downloads" / "mon_plith_2021.xlsx"

# ELSTAT Excel municipality names (genitive/honorific forms) -> our GeoJSON municipality_code.
PDF_MUNICIPALITY_OVERRIDES: dict[str, str | None] = {
    "ΗΡΩΙΚΗΣ ΠΟΛΕΩΣ ΝΑΟΥΣΑΣ": "9025",
    "ΑΡΓΟΥΣ ΟΡΕΣΤΙΚΟΥ": "9065",  # GeoJSON municipality name: Ορεστίδος
    "ΣΤΥΛΙΔΟΣ": "9164",
    "ΑΛΙΑΡΤΟΥ - ΘΕΣΠΙΕΩΝ": "9142",  # GeoJSON uses older 'Αλιάρτου'
    "ΔΙΣΤΟΜΟΥ - ΑΡΑΧΟΒΑΣ - ΑΝΤΙΚΥΡΑΣ": "9143",
    "ΗΛΙΟΥΠΟΛΕΩΣ": "9191",
    "ΝΕΑΣ ΦΙΛΑΔΕΛΦΕΙΑΣ - ΝΕΑΣ ΧΑΛΚΗΔΟΝΟΣ": "9193",
    "ΠΕΤΡΟΥΠΟΛΕΩΣ": "9184",
    "ΝΙΚΑΙΑΣ - ΑΓΙΟΥ ΙΩΑΝΝΗ ΡΕΝΤΗ": "9204",  # GeoJSON abbreviates 'Ι.'
    "ΣΑΛΑΜΙΝΟΣ": "9211",
    "ΗΡΩΙΚΗΣ ΝΗΣΟΥ ΨΑΡΩΝ": "9267",
    "ΗΡΩΙΚΗΣ ΝΗΣΟΥ ΚΑΣΟΥ": "9281",
    "ΝΑΞΟΥ ΚΑΙ ΜΙΚΡΩΝ ΚΥΚΛΑΔΩΝ": "9292",  # GeoJSON uses '&'
}

NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pr": "http://schemas.openxmlformats.org/package/2006/relationships",
}


def _load_shared_strings(z: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in z.namelist():
        return []
    root = ET.fromstring(z.read("xl/sharedStrings.xml"))
    out: list[str] = []
    for si in root.findall("a:si", NS):
        out.append("".join(t.text or "" for t in si.findall(".//a:t", NS)))
    return out


def _sheet_targets(z: zipfile.ZipFile) -> dict[str, str]:
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels.findall("pr:Relationship", NS)}
    out = {}
    for s in wb.findall("a:sheets/a:sheet", NS):
        rid = s.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        out[s.attrib["name"]] = rel_map[rid]
    return out


def _parse_sheet_rows(z: zipfile.ZipFile, sheet_target: str, shared: list[str]) -> list[list[str]]:
    path = "xl/" + sheet_target.replace("\\", "/")
    root = ET.fromstring(z.read(path))
    rows_out: list[list[str]] = []
    for row in root.findall(".//a:sheetData/a:row", NS):
        # Sparse cells: map by column letters so A/C always land correctly.
        cell_map: dict[str, str] = {}
        for c in row.findall("a:c", NS):
            ref = c.attrib.get("r", "")
            col = "".join(ch for ch in ref if ch.isalpha())
            t = c.attrib.get("t")
            v = c.find("a:v", NS)
            isel = c.find("a:is", NS)
            if t == "s" and v is not None:
                val = shared[int(v.text)]
            elif t == "inlineStr" and isel is not None:
                val = "".join(tn.text or "" for tn in isel.findall(".//a:t", NS))
            elif v is not None:
                val = v.text or ""
            else:
                val = ""
            cell_map[col] = val
        if cell_map:
            rows_out.append([cell_map.get("A", ""), cell_map.get("B", ""), cell_map.get("C", "")])
    return rows_out


def parse_elstat_region_municipality_rows(xlsx_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(xlsx_path) as z:
        shared = _load_shared_strings(z)
        sheets = _sheet_targets(z)
        sheet_name = "ΜΟΝΙΜΟΣ 2021_ΠΕΡΙΦ-ΠΕ-ΔΗΜΟΙ"
        if sheet_name not in sheets:
            raise ValueError(f"Sheet not found: {sheet_name!r}. Available: {list(sheets)}")
        rows = _parse_sheet_rows(z, sheets[sheet_name], shared)

    current_region_name: str | None = None
    parsed = []
    for level_raw, geo_code, desc in rows:
        level = str(level_raw).strip()
        desc = str(desc).strip()
        geo_code = str(geo_code).strip()
        if not level or not desc:
            continue
        if level == "3" and desc.startswith("ΠΕΡΙΦΕΡΕΙΑ "):
            region_label = re.sub(r"^ΠΕΡΙΦΕΡΕΙΑ\s+", "", desc).strip()
            current_region_name = canonicalize_region_name(region_label) or canonicalize_region_name(desc) or region_label
            continue
        if level != "5":
            continue
        if current_region_name is None:
            continue
        if desc == "ΑΓΙΟ ΟΡΟΣ (ΑΥΤΟΔΙΟΙΚΗΤΟ)":
            continue
        if not desc.startswith("ΔΗΜΟΣ "):
            continue
        muni_name_pdf = re.sub(r"^ΔΗΜΟΣ\s+", "", desc).strip()
        parsed.append(
            {
                "region_id": current_region_name,
                "pdf_municipality_name": muni_name_pdf,
                "pdf_geo_code": geo_code,
            }
        )
    out = pd.DataFrame(parsed).drop_duplicates()
    return out


def load_geojson_lookup() -> tuple[dict[str, tuple[str, str]], dict[str, str]]:
    with open(GEOJSON_PATH) as f:
        gj = json.load(f)
    muni_lookup: dict[str, tuple[str, str]] = {}
    code_to_name: dict[str, str] = {}
    for ft in gj["features"]:
        name = ft["properties"]["name"]
        code = ft["properties"]["municipality_code"]
        if not code:
            continue
        code = str(code)
        muni_lookup[norm(name)] = (name, code)
        code_to_name[code] = name
    return muni_lookup, code_to_name


def match_pdf_municipalities_to_geojson(pdf_df: pd.DataFrame) -> pd.DataFrame:
    muni_lookup, code_to_name = load_geojson_lookup()

    rows = []
    unmatched = 0
    for _, r in pdf_df.iterrows():
        pdf_name = str(r["pdf_municipality_name"]).strip()
        key = norm(pdf_name)
        municipality_id = None
        match_method = ""
        notes = ""

        if key in muni_lookup:
            municipality_id = muni_lookup[key][1]
            match_method = "exact_name"
        elif pdf_name in PDF_MUNICIPALITY_OVERRIDES:
            municipality_id = PDF_MUNICIPALITY_OVERRIDES[pdf_name]
            match_method = "pdf_name_override"
            if municipality_id is None:
                notes = "PDF_MUNICIPALITY_OVERRIDES -> None (missing in GeoJSON)"
        elif pdf_name in DIMOS_OVERRIDES:
            municipality_id = DIMOS_OVERRIDES[pdf_name]
            match_method = "dimos_override"
            if municipality_id is None:
                notes = "DIMOS_OVERRIDES -> None (missing in GeoJSON / non-municipality)"
        else:
            match_method = "unmatched"
            unmatched += 1

        rows.append(
            {
                "municipality_id": municipality_id or "",
                "municipality_name": code_to_name.get(str(municipality_id), "") if municipality_id else "",
                "region_id": r["region_id"],
                "source": "ELSTAT mon_plith_2021.xlsx",
                "notes": notes,
                "pdf_municipality_name": pdf_name,
                "pdf_geo_code": r["pdf_geo_code"],
                "match_method": match_method,
            }
        )

    out = pd.DataFrame(rows)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", type=Path, default=DEFAULT_XLSX, help="Path to mon_plith_2021.xlsx")
    ap.add_argument("--output", type=Path, default=OUTPUT_PATH, help="Output CSV path")
    args = ap.parse_args()

    if not args.xlsx.exists():
        raise FileNotFoundError(f"XLSX not found: {args.xlsx}")

    pdf_df = parse_elstat_region_municipality_rows(args.xlsx)
    matched = match_pdf_municipalities_to_geojson(pdf_df)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    matched.to_csv(args.output, index=False)

    total = len(matched)
    ok = int((matched["municipality_id"].astype(str).str.strip() != "").sum())
    unresolved = total - ok
    print(f"[done] {total} rows -> {args.output}")
    print(f"  matched municipality_id: {ok}")
    print(f"  unresolved:             {unresolved}")

    if unresolved:
        print("\nUnresolved examples:")
        print(
            matched.loc[matched["municipality_id"].astype(str).str.strip() == "", ["region_id", "pdf_municipality_name", "match_method", "notes"]]
            .head(20)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
