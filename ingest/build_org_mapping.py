"""
build_org_mapping.py
--------------------
Builds data/mappings/org_to_municipality.csv by matching Diavgeia organizations
to municipality IDs from municipalities.geojson.

Strategy:
  1. Auto-match ΔΗΜΟΣ by accent-stripping (296/319 match)
  2. Apply hardcoded overrides for name variants + Kleisthenis splits
  3. Map municipal subsidiaries (ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ etc.) to parent municipality
  4. Assign authority level for regional/national bodies
  5. Flag unmatched entries for manual review

Kleisthenis 2019 note:
  Some municipalities in 2018-2025 Diavgeia data were created by the 2019 Kleisthenis
  reform (splitting Kallikratis municipalities). These are mapped to their Kallikratis
  parent geometry since the GeoJSON uses Kallikratis boundaries.
"""

from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import pandas as pd

REPO_DIR = Path(__file__).resolve().parent.parent
DIAVGEIA_CSV = REPO_DIR / "data" / "2026_diavgeia_filtered.csv"
GEOJSON_PATH = REPO_DIR / "data" / "geo" / "municipalities.geojson"
OUTPUT_PATH = REPO_DIR / "data" / "mappings" / "org_to_municipality.csv"
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", str(s))
        if unicodedata.category(c) != "Mn"
    )


def norm(s: str) -> str:
    return strip_accents(s).upper().strip()


# ---------------------------------------------------------------------------
# Manual overrides: org_name_clean → municipality_code (from GeoJSON)
# None = not a municipality or not in GeoJSON
# ---------------------------------------------------------------------------

DIMOS_OVERRIDES: dict[str, str | None] = {
    # ── Latin A instead of Greek A ──────────────────────────────────────────
    "AΓΙΟΥ ΝΙΚΟΛΑΟΥ": "9310",  # → Αγίου Νικολάου

    # ── Different Greek genitive form ───────────────────────────────────────
    "ΜΕΤΑΜΟΡΦΩΣΗΣ": "9173",    # → Μεταμορφώσεως

    # ── Abbreviated name in GeoJSON ─────────────────────────────────────────
    "ΛΟΥΤΡΑΚΙΟΥ - ΠΕΡΑΧΩΡΑΣ - ΑΓΙΩΝ ΘΕΟΔΩΡΩΝ": "9244",  # → Λουτρακίου - Αγ. Θεοδώρων

    # ── Full vs abbreviated (GeoJSON drops ΝΕΑΣ prefix) ─────────────────────
    "ΝΕΑΣ ΦΙΛΑΔΕΛΦΕΙΑΣ - ΝΕΑΣ ΧΑΛΚΗΔΟΝΑΣ": "9193",  # → Φιλαδελφείας - Χαλκηδόνος

    # ── Spelling variant (ΦΕΡΑΙΟΥ vs ΦΕΡΡΑΙΟΥ) ─────────────────────────────
    "ΡΗΓΑ ΦΕΡΑΙΟΥ": "9108",  # → Ρήγα Φερραίου

    # ── Kleisthenis extended name → Kallikratis ─────────────────────────────
    "ΤΡΟΙΖΗΝΙΑΣ - ΜΕΘΑΝΩΝ": "9213",  # → Τροιζηνίας

    # ── With parenthetical qualifier ─────────────────────────────────────────
    "ΗΡΑΚΛΕΙΟΥ (ΚΡΗΤΗΣ)": "9305",  # → Ηρακλείου (Κρήτης, not Σερρών)

    # ── GeoJSON uses old pre-Kallikratis name ───────────────────────────────
    "ΜΕΤΕΩΡΩΝ": "9112",  # GeoJSON: Καλαμπάκας (same municipality after Kallikratis)

    # ── Kleisthenis 2019: Κέρκυρα split into 3 ──────────────────────────────
    "ΒΟΡΕΙΑΣ ΚΕΡΚΥΡΑΣ":                          "9118",  # → Κέρκυρας
    "ΝΟΤΙΑΣ ΚΕΡΚΥΡΑΣ":                           "9118",  # → Κέρκυρας
    "ΚΕΝΤΡΙΚΗΣ ΚΕΡΚΥΡΑΣ ΚΑΙ ΔΙΑΠΟΝΤΙΩΝ ΝΗΣΩΝ":  "9118",  # → Κέρκυρας

    # ── Kleisthenis 2019: Σερβίων-Βελβεντού split into 2 ───────────────────
    "ΒΕΛΒΕΝΤΟΥ": "9069",  # → Σερβίων-Βελβεντού
    "ΣΕΡΒΙΩΝ":   "9069",  # → Σερβίων-Βελβεντού

    # ── Kleisthenis 2019: Λέσβος new/renamed municipalities ─────────────────
    "ΔΥΤΙΚΗΣ ΛΕΣΒΟΥ": "9261",  # → Λέσβου
    "ΜΥΤΙΛΗΝΗΣ":      "9261",  # → Λέσβου (absorbed in Kallikratis)

    # ── Kleisthenis 2019: Σάμος split ───────────────────────────────────────
    "ΑΝΑΤΟΛΙΚΗΣ ΣΑΜΟΥ": "9264",  # → Σάμου
    "ΔΥΤΙΚΗΣ ΣΑΜΟΥ":    "9264",  # → Σάμου

    # ── Kleisthenis 2019: Κεφαλονιά split into 4 ────────────────────────────
    "ΑΡΓΟΣΤΟΛΙΟΥ": "9120",  # → Κεφαλονιάς
    "ΣΑΜΗΣ":       "9120",  # → Κεφαλονιάς
    "ΛΗΞΟΥΡΙΟΥ":   "9120",  # → Κεφαλονιάς

    # ── Kleisthenis 2019: Μώλου-Αγ.Κωνσταντίνου split ──────────────────────
    "ΚΑΜΕΝΩΝ ΒΟΥΡΛΩΝ": "9163",  # → Μώλου-Αγ.Κωνσταντίνου

    # ── Not a municipality (company mislabeled as ΔΗΜΟΣ) ────────────────────
    "ΑΘΗΝΑΙΩΝ ΑΝΩΝΥΜΗ ΑΝΑΠΤΥΞΙΑΚΗ ΕΤΑΙΡΕΙΑ ΜΗΧΑΝΟΓΡΑΦΗΣΗΣ & ΕΠΙΧΕΙΡΗΣΙΑΚΩΝ ΜΟΝΑΔΩΝ ΟΤΑ": None,

    # ── Genuinely missing from GeoJSON shapefile ─────────────────────────────
    "ΑΡΓΟΥΣ ΟΡΕΣΤΙΚΟΥ": None,  # Δήμος Άργους Ορεστικού (Kastoria) — not in shapefile
}

# Municipal subsidiaries → parent municipality_code
SUBSIDIARY_MAP: dict[tuple[str, str], str | None] = {
    # (org_type, org_name_clean) → municipality_code
    ("ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ", "ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΧΑΝΙΩΝ"):            "9325",  # Χανίων
    ("ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ", "ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΚΑΒΑΛΑΣ"): "9012",  # Καβάλας
    ("ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ", "ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΠΑΓΓΑΙΟΥ"):           "9014",  # Παγγαίου
    ("ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ", "ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΠΑΤΡΑΣ ΔΕΥΑΠ"):       "9134",  # Πατρεών
    ("ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ", "ΥΔΡΕΥΣΗΣ - ΑΠΟΧΕΤΕΥΣΗΣ ΔΡΑΜΑΣ"):           "9002",  # Δράμας

    ("ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ", "ΒΟΡΕΙΑΣ ΚΥΝΟΥΡΙΑΣ"):   "9237",  # Βορείας Κυνουρίας
    ("ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ", "ΝΕΑΣ ΠΡΟΠΟΝΤΙΔΑΣ"):     "9058",  # Νέας Προποντίδας
    ("ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ", "ΠΡΕΒΕΖΑΣ"):             "9090",  # Πρέβεζας
    ("ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ", "ΝΑΥΠΛΙΟΥ"):             "9236",  # Ναυπλιέων

    ("ΔΗΜΟΤΙΚΟ ΒΡΕΦΟΚΟΜΕΙΟ", "ΑΘΗΝΩΝ"):                   "9186",  # Αθηναίων

    ("ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ", "ΚΕΡΚΥΡΑΣ ΔΗ.ΠΕ.ΘΕ.Κ."):     "9118",  # Κέρκυρας
    ("ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ", "ΚΑΒΑΛΑΣ-ΔΗ.ΠΕ.ΘΕ. ΚΑΒΑΛΑΣ"): "9012",  # Καβάλας
}

# Authority levels by org_type
AUTHORITY_LEVEL: dict[str, str] = {
    "ΔΗΜΟΣ":                                    "municipality",
    "ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ":                      "municipality",
    "ΔΗΜΟΤΙΚΟ ΒΡΕΦΟΚΟΜΕΙΟ":                     "municipality",
    "ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ":                 "municipality",
    "ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ":             "municipality",
    "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ":                           "syndicate",
    "ΠΕΡΙΦΕΡΕΙΑ":                                "region",
    "ΠΕΡΙΦΕΡΕΙΑΚΟ ΤΑΜΕΙΟ ΑΝΑΠΤΥΞΗΣ":            "region",
    "ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ":   "region",
    "ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ":                    "decentralized",
    "ΥΠΟΥΡΓΕΙΟ":                                 "national",
    "ΑΛΛΟΣ ΦΟΡΕΑΣ":                              "other",
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # Load data
    df = pd.read_csv(DIAVGEIA_CSV, low_memory=False)
    with open(GEOJSON_PATH) as f:
        gj = json.load(f)

    # Build municipality lookup: normalized_name → (original_name, code)
    muni_lookup: dict[str, tuple[str, str]] = {}
    for ft in gj["features"]:
        name = ft["properties"]["name"]
        code = ft["properties"]["municipality_code"]
        muni_lookup[norm(name)] = (name, code)

    # Unique org pairs with doc count
    pairs = (
        df.groupby(["org_type", "org_name_clean"])
        .size()
        .reset_index(name="doc_count")
    )

    rows = []
    match_stats = {"exact": 0, "override": 0, "subsidiary": 0, "unmatched": 0}

    for _, row in pairs.iterrows():
        org_type: str = row["org_type"]
        org_name: str = row["org_name_clean"]
        doc_count: int = row["doc_count"]

        authority = AUTHORITY_LEVEL.get(org_type, "other")
        municipality_id: str | None = None
        region_id: str | None = None
        match_method = "unmatched"
        notes = ""

        if org_type == "ΔΗΜΟΣ":
            # 1. Try exact match (accent-stripped)
            key = norm(org_name)
            if key in muni_lookup:
                municipality_id = muni_lookup[key][1]
                match_method = "exact"
                match_stats["exact"] += 1

            # 2. Try manual override
            elif org_name in DIMOS_OVERRIDES:
                municipality_id = DIMOS_OVERRIDES[org_name]
                match_method = "manual"
                notes = "name variant or Kleisthenis→Kallikratis parent" if municipality_id else "not in GeoJSON or not a municipality"
                match_stats["override"] += 1

            else:
                match_method = "unmatched"
                match_stats["unmatched"] += 1
                notes = "TODO: manual review required"

        elif (org_type, org_name) in SUBSIDIARY_MAP:
            municipality_id = SUBSIDIARY_MAP[(org_type, org_name)]
            match_method = "manual"
            notes = f"subsidiary of municipality {municipality_id}"
            match_stats["subsidiary"] += 1

        elif org_type == "ΠΕΡΙΦΕΡΕΙΑ":
            region_id = org_name
            match_method = "exact"

        elif org_type in ("ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ", "ΥΠΟΥΡΓΕΙΟ", "ΑΛΛΟΣ ΦΟΡΕΑΣ",
                           "ΠΕΡΙΦΕΡΕΙΑΚΟ ΤΑΜΕΙΟ ΑΝΑΠΤΥΞΗΣ",
                           "ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ",
                           "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ"):
            match_method = "exact"  # authority level assigned by type

        rows.append({
            "org_type":        org_type,
            "org_name_clean":  org_name,
            "doc_count":       doc_count,
            "authority_level": authority,
            "municipality_id": municipality_id,
            "region_id":       region_id,
            "match_method":    match_method,
            "notes":           notes,
        })

    out = pd.DataFrame(rows).sort_values(["org_type", "org_name_clean"])
    out.to_csv(OUTPUT_PATH, index=False)

    print(f"[done] {len(out)} org entries → {OUTPUT_PATH}")
    print(f"\nMatch stats (ΔΗΜΟΣ):")
    print(f"  exact:      {match_stats['exact']}")
    print(f"  override:   {match_stats['override']}")
    print(f"  subsidiary: {match_stats['subsidiary']}")
    print(f"  unmatched:  {match_stats['unmatched']}")

    print(f"\nBy authority level:")
    print(out.groupby("authority_level")[["org_name_clean"]].count().rename(
        columns={"org_name_clean": "orgs"}
    ).to_string())

    unmatched = out[out["match_method"] == "unmatched"]
    if not unmatched.empty:
        print(f"\nUnmatched entries ({len(unmatched)}) — require manual review:")
        for _, r in unmatched.iterrows():
            print(f"  [{r['org_type']}] {r['org_name_clean']!r}")

    null_muni = out[
        (out["authority_level"] == "municipality") &
        out["municipality_id"].isna()
    ]
    if not null_muni.empty:
        print(f"\nMunicipal-level orgs with no municipality_id ({len(null_muni)}):")
        for _, r in null_muni.iterrows():
            print(f"  [{r['org_type']}] {r['org_name_clean']!r} — {r['notes']}")


if __name__ == "__main__":
    main()
