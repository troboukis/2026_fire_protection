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
import re
import unicodedata
from pathlib import Path

import pandas as pd

REPO_DIR = Path(__file__).resolve().parent.parent
DIAVGEIA_CSV = REPO_DIR / "data" / "2026_diavgeia_filtered.csv"
GEOJSON_PATH = REPO_DIR / "data" / "geo" / "municipalities.geojson"
OUTPUT_PATH = REPO_DIR / "data" / "mappings" / "org_to_municipality.csv"
REGION_MUNI_REF_PATH = REPO_DIR / "data" / "mappings" / "region_to_municipalities.csv"
COVERAGE_OUTPUT_PATH = REPO_DIR / "data" / "mappings" / "org_to_municipality_coverage.csv"
REGION_MUNI_TEMPLATE_PATH = REPO_DIR / "data" / "mappings" / "region_to_municipalities_template.csv"
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
    "ΑΘΗΝΑΙΩΝ ΑΝΩΝΥΜΗ ΑΝΑΠΤΥΞΙΑΚΗ ΕΤΑΙΡΕΙΑ ΜΗΧΑΝΟΓΡΑΦΗΣΗΣ & ΕΠΙΧΕΙΡΗΣΙΑΚΩΝ ΜΟΝΑΔΩΝ ΟΤΑ": "9186",

    # ── Genuinely missing from GeoJSON shapefile ─────────────────────────────
    "ΑΡΓΟΥΣ ΟΡΕΣΤΙΚΟΥ": "9065",  # GeoJSON municipality name: Ορεστίδος
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

# Canonical region names (current administrative regions)
CANONICAL_REGIONS = [
    "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ",
    "ΑΤΤΙΚΗΣ",
    "ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ",
    "ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ",
    "ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ",
    "ΗΠΕΙΡΟΥ",
    "ΘΕΣΣΑΛΙΑΣ",
    "ΙΟΝΙΩΝ ΝΗΣΩΝ",
    "ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ",
    "ΚΡΗΤΗΣ",
    "ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ",
    "ΠΕΛΟΠΟΝΝΗΣΟΥ",
    "ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ",
]

REGION_ALIASES: dict[str, str] = {
    "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ - ΘΡΑΚΗΣ": "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ",
    "ΠΕΡΙΦΕΡΕΙΑΣ ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΘΡΑΚΗΣ": "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ",
    "ΜΑΚΕΔΟΝΙΑΣ – ΘΡΑΚΗΣ": "ΜΑΚΕΔΟΝΙΑΣ - ΘΡΑΚΗΣ",  # decentralized org variant (handled below)
}

DECENTRALIZED_TO_REGIONS: dict[str, list[str]] = {
    "ΑΤΤΙΚΗΣ": ["ΑΤΤΙΚΗΣ"],
    "ΑΙΓΑΙΟΥ": ["ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ", "ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ"],
    "ΚΡΗΤΗΣ": ["ΚΡΗΤΗΣ"],
    "ΘΕΣΣΑΛΙΑΣ - ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ": ["ΘΕΣΣΑΛΙΑΣ", "ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ"],
    "ΗΠΕΙΡΟΥ - ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ": ["ΗΠΕΙΡΟΥ", "ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ"],
    "ΜΑΚΕΔΟΝΙΑΣ - ΘΡΑΚΗΣ": ["ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ", "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ"],
    "ΠΕΛΟΠΟΝΝΗΣΟΥ, ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ ΚΑΙ ΙΟΝΙΟΥ": ["ΠΕΛΟΠΟΝΝΗΣΟΥ", "ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ", "ΙΟΝΙΩΝ ΝΗΣΩΝ"],
}

# Manual municipality coverage overrides for orgs that operate below region level
# but above a single municipality (e.g. PE-level development organizations).
MANUAL_ORG_MUNICIPALITY_COVERAGE: dict[tuple[str, str], list[str]] = {
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΑΝΑΠΤΥΞΙΑΚΗ ΑΡΚΑΔΙΑΣ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ",
    ): [
        "9241",  # Τρίπολης
        "9237",  # Βόρειας Κυνουρίας
        "9238",  # Γορτυνίας
        "9239",  # Μεγαλόπολης
        "9240",  # Νότιας Κυνουρίας
    ],
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΑΝΑΠΤΥΞΙΑΚΗ ΕΤΑΙΡΕΙΑ ΣΕΡΡΩΝ ΑΕ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΝΟΜΟΥ ΣΕΡΡΩΝ",
    ): [
        "9054",  # Σερρών
        "9049",  # Αμφίπολης
        "9050",  # Βισαλτίας
        "9051",  # Εμμανουήλ Παππά
        "9052",  # Ηράκλειας
        "9053",  # Νέας Ζίχνης
        "9055",  # Σιντικής
    ],
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΑΝΑΠΤΥΞΙΑΚΗ ΗΡΑΚΛΕΙΟΥ ΑΝΑΠΤΥΞΙΑΚΗ ΑΕ ΟΤΑ",
    ): [
        "9305",  # Ηρακλείου
        "9302",  # Αρχανών - Αστερουσίων
        "9303",  # Βιάννου
        "9304",  # Γόρτυνας
        "9306",  # Μαλεβιζίου
        "9307",  # Μινώα Πεδιάδας
        "9308",  # Φαιστού
        "9309",  # Χερσονήσου
    ],
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΜΑΓΝΗΣΙΑΣ ΑΕ",
    ): [
        "9105",  # Βόλου
        "9104",  # Αλμυρού
        "9106",  # Ζαγοράς - Μουρεσίου
        "9107",  # Νοτίου Πηλίου
        "9108",  # Ρήγα Φερραίου
    ],
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΝΟΜΟΥ ΛΑΡΙΣΑΣ ΑΕΝΟΛ ΑΕ",
    ): [
        "9100",  # Λαρισαίων
        "9097",  # Αγιάς
        "9098",  # Ελασσόνας
        "9099",  # Κιλελέρ
        "9101",  # Τεμπών
        "9102",  # Τυρνάβου
        "9103",  # Φαρσάλων
    ],
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΑΝΑΠΤΥΞΙΑΚΗ ΠΡΟΟΠΤΙΚΗ Μ.Α.Ε. - ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΔΗΜΟΥ ΓΑΛΑΤΣΙΟΥ Α.Ε.",
    ): [
        "9188",  # Γαλατσίου
    ],
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΘΡΙΑΣΙΟ Α Ε",
    ): [
        "9229",  # Ελευσίνας
        "9228",  # Ασπροπύργου
        "9230",  # Μάνδρας - Ειδυλλίας
        "9232",  # Φυλής
    ],
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ",
    ): [
        "9219",  # Λαυρεωτικής
        "9225",  # Σαρωνικού
    ],
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΙΑΣ",
    ): [
        "9213",  # Τροιζηνίας (covers Τροιζηνίας-Μεθάνων in current GeoJSON)
        "9210",  # Πόρου
    ],
    (
        "ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ",
        "ΤΡΙΗΡΗΣ ΑΝΑΠΤΥΞΙΑΚΗ Α Ε ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ",
    ): ["*"],  # whole-country coverage (all municipalities in reference)
    (
        "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ",
        "ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ (Σ.Π.Α.Π.)",
    ): [
        "9168",  # Αμαρουσίου
        "9169",  # Βριλησσίων
        "9188",  # Γαλατσίου
        "9217",  # Διονύσου
        "9171",  # Κηφισιάς
        "9220",  # Μαραθώνος
        "9223",  # Παλλήνης
        "9176",  # Πεντέλης
        "9172",  # Λυκόβρυσης - Πεύκης
        "9224",  # Ραφήνας - Πικερμίου
        "9226",  # Σπάτων - Αρτέμιδος
        "9178",  # Χαλανδρίου
    ],
    (
        "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ",
        "ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΩΝ ΤΟΥΡΚΟΒΟΥΝΙΩΝ",
    ): [
        "9186",  # Αθηναίων
        "9188",  # Γαλατσίου
        "9174",  # Νέας Ιωνίας
        "9177",  # Φιλοθέης - Ψυχικού
    ],
    (
        "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ",
        "ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ «ΣΥΝ ΠΑ»",
    ): [
        "9215",  # Αχαρνών
        "9232",  # Φυλής
        "9228",  # Ασπροπύργου
        "9217",  # Διονύσου
        "9227",  # Ωρωπού
    ],
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ",
    ): [
        "9167",  # Αγίας Παρασκευής
        "9216",  # Βάρης - Βούλας - Βουλιαγμένης
        "9187",  # Βύρωνος (Βύρωνα)
        "9196",  # Γλυφάδας
        "9197",  # Ελληνικού - Αργυρούπολης
        "9190",  # Ζωγράφου
        "9191",  # Ηλιούπολης
        "9192",  # Καισαριανής
        "9218",  # Κρωπίας
        "9222",  # Παιανίας
        "9175",  # Παπάγου - Χολαργού
    ],
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΔΙΚΤΥΟ ΣΥΝΕΡΓΑΣΙΑΣ ΔΗΜΩΝ ΠΕΡΙΦΕΡΕΙΑΚΗΣ ΕΝΟΤΗΤΑΣ ΝΗΣΩΝ ΑΤΤΙΚΗΣ",
    ): [
        "9207",  # Αγκιστρίου
        "9208",  # Αίγινας
        "9209",  # Κυθήρων
        "9210",  # Πόρου
        "9211",  # Σαλαμίνας
        "9212",  # Σπετσών
        "9213",  # Τροιζηνίας (covers Τροιζηνίας-Μεθάνων)
        "9214",  # Ύδρας
    ],
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΛΑΥΡΙΟΥ Α.Ε",
    ): [
        "9219",  # Λαυρεωτικής
    ],
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "Δ.Ε.Υ.Α ΧΕΡΣΟΝΗΣΟΥ ΚΡΗΤΗΣ",
    ): [
        "9309",  # Χερσονήσου
    ],
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΕΑΚ ΧΑΝΙΩΝ",
    ): [
        "9325",  # Χανίων
    ],
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΟΡΓΑΝΙΣΜΟΣ ΦΥΣΙΚΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΚΛΙΜΑΤΙΚΗΣ ΑΛΛΑΓΗΣ",
    ): ["*"],  # whole-country coverage
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ",
    ): ["*"],  # whole-country coverage
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΤΑΜΕΙΟ ΑΞΙΟΠΟΙΗΣΗΣ ΙΔΙΩΤΙΚΗΣ ΠΕΡΙΟΥΣΙΑΣ ΤΟΥ ΔΗΜΟΣΙΟΥ ΑΕ",
    ): ["*"],  # whole-country coverage
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΕΛΛΗΝΙΚΗ ΕΤΑΙΡΕΙΑ ΣΥΜΜΕΤΟΧΩΝ ΚΑΙ ΠΕΡΙΟΥΣΙΑΣ ΑΕ",
    ): ["*"],  # whole-country coverage
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΡΥΘΜΙΣΤΙΚΗ ΑΡΧΗ ΑΠΟΒΛΗΤΩΝ ΕΝΕΡΓΕΙΑΣ ΚΑΙ ΥΔΑΤΩΝ",
    ): ["*"],  # whole-country coverage
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΔΙΑΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΟΡΓΑΝΩΣΗΣ ΔΙΑΧΕΙΡΗΣΗΣ ΑΠΟΡΡΙΜΑΤΩΝ ΛΙΒΑΔΕΙΑΣ ΑΕ ΟΤΑ",
    ): [
        "9145",  # Λεβαδέων
        "9142",  # Αλιάρτου
        "9143",  # Διστόμου-Αράχοβας - Αντίκυρας
        "9144",  # Θηβαίων
        "9146",  # Ορχομενού
        "9147",  # Τανάγρας
    ],
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΚΑΤΑΣΤΗΜΑ ΚΡΑΤΗΣΗΣ ΛΑΡΙΣΑΣ",
    ): [
        "9100",  # Λαρισαίων
    ],
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΕΤΑΙΡΕΙΑ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΘΕΣΣΑΛΟΝΙΚΗΣ ΠΑΓΙΩΝ (ΕΥΑΘ ΠΑΓΙΩΝ)",
    ): [
        "9031",  # Θεσσαλονίκης
    ],
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΓΑΙΑ ΟΣΕ ΑΕ",
    ): ["*"],  # whole-country coverage
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΕΘΝΙΚΟ ΣΥΣΤΗΜΑ ΥΠΟΔΟΜΩΝ ΠΟΙΟΤΗΤΑΣ (Ε.Σ.Υ.Π.)",
    ): ["*"],  # whole-country coverage
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΚΕΝΤΡΟ ΔΙΑΦΥΛΑΞΗΣ ΑΓΙΟΡΕΙΤΙΚΗΣ ΚΛΗΡΟΝΟΜΙΑΣ (Κ.Δ.Α.Κ.)",
    ): ["*"],  # whole-country coverage (per requested policy)
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΟΡΓΑΝΙΣΜΟΣ ΑΣΤΙΚΩΝ ΣΥΓΚΟΙΝΩΝΙΩΝ ΘΕΣΣΑΛΟΝΙΚΗΣ",
    ): ["*"],  # whole-country coverage (per requested policy)
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΟΡΓΑΝΙΣΜΟΣ ΣΙΔΗΡΟΔΡΟΜΩΝ ΕΛΛΑΔΑΣ (ΟΣΕ)",
    ): ["*"],  # whole-country coverage
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΠΡΟΕΔΡΙΑ ΤΗΣ ΚΥΒΕΡΝΗΣΗΣ",
    ): ["*"],  # whole-country coverage
    (
        "ΑΛΛΟΣ ΦΟΡΕΑΣ",
        "ΤΕΧΝΙΚΟ ΕΠΙΜΕΛΗΤΗΡΙΟ ΕΛΛΑΔΑΣ",
    ): ["*"],  # whole-country coverage
}

# Manual region coverage overrides for orgs that should expand to all municipalities
# of one or more regions through `region_to_municipalities.csv`.
MANUAL_ORG_REGION_COVERAGE: dict[tuple[str, str], list[str]] = {
    ("ΑΛΛΟΣ ΦΟΡΕΑΣ", "ΠΕΡΙΦΕΡΕΙΑΚΗ ΕΝΩΣΗ ΔΗΜΩΝ (ΠΕΔ) ΗΠΕΙΡΟΥ"): ["ΗΠΕΙΡΟΥ"],
    ("ΑΛΛΟΣ ΦΟΡΕΑΣ", "ΠΕΡΙΦΕΡΕΙΑΚΗ ΕΝΩΣΗ ΔΗΜΩΝ ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ"): ["ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ"],
}


def canonicalize_region_name(name: str) -> str | None:
    """Map region-like org_name_clean values to canonical region names."""
    if not isinstance(name, str):
        return None
    n = name.strip()
    n = re.sub(r"\s+", " ", n)
    n = n.replace("–", "-")
    n = re.sub(r"\s*-\s*", " - ", n)
    n = re.sub(r"\s+", " ", n).strip()

    # remove non-semantic parenthetical suffixes, e.g. "(ΠΑΛΑΙΑ ΚΡΑΤΙΚΗ)"
    n = re.sub(r"\s*\([^)]*\)\s*$", "", n).strip()

    # Strip common prefixes in regional entities names
    for prefix in (
        "ΠΕΡΙΦΕΡΕΙΑΣ ",
        "ΠΕΡΙΦΕΡΕΙΑ ",
    ):
        if n.startswith(prefix):
            n = n[len(prefix):].strip()
            break

    n = REGION_ALIASES.get(n, n)
    if n in CANONICAL_REGIONS:
        return n
    return None


def decentralized_coverage_regions(org_name_clean: str) -> list[str]:
    """Return covered canonical regions for an Αποκεντρωμένη Διοίκηση."""
    if not isinstance(org_name_clean, str):
        return []
    n = re.sub(r"\s+", " ", org_name_clean.replace("–", "-")).strip()
    return DECENTRALIZED_TO_REGIONS.get(n, [])


def build_region_municipality_template(muni_lookup: dict[str, tuple[str, str]]) -> pd.DataFrame:
    """Template to be manually/external filled with region_id per municipality."""
    rows = []
    for _, (muni_name, muni_id) in sorted(muni_lookup.items(), key=lambda x: (str(x[1][0]), str(x[1][1]))):
        rows.append({
            "municipality_id": str(muni_id),
            "municipality_name": str(muni_name),
            "region_id": "",
            "source": "",
            "notes": "",
        })
    return pd.DataFrame(rows)


def maybe_build_org_coverage(mapping_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Expand orgs with region coverage into municipality coverage rows.

    Requires `data/mappings/region_to_municipalities.csv` with columns:
    - municipality_id
    - municipality_name (optional)
    - region_id
    """
    if not REGION_MUNI_REF_PATH.exists():
        return None

    ref = pd.read_csv(REGION_MUNI_REF_PATH, dtype=str).fillna("")
    required = {"municipality_id", "region_id"}
    if not required.issubset(ref.columns):
        raise ValueError(
            f"{REGION_MUNI_REF_PATH} missing required columns: {sorted(required - set(ref.columns))}"
        )

    ref["region_id"] = ref["region_id"].astype(str).str.strip()
    ref["municipality_id"] = ref["municipality_id"].astype(str).str.strip()
    ref = ref[(ref["region_id"] != "") & (ref["municipality_id"] != "")]

    region_to_munis: dict[str, list[str]] = (
        ref.groupby("region_id")["municipality_id"].apply(lambda s: sorted(set(s.tolist()))).to_dict()
    )
    muni_name_lookup = {}
    if "municipality_name" in ref.columns:
        muni_name_lookup = (
            ref.drop_duplicates(subset=["municipality_id"])
            .set_index("municipality_id")["municipality_name"]
            .to_dict()
        )

    coverage_rows = []
    for _, r in mapping_df.iterrows():
        org_type = r.get("org_type")
        org_name = r.get("org_name_clean")
        manual_key = (str(org_type), str(org_name))
        mapped_municipality_id = str(r.get("municipality_id", "") or "").strip()

        if manual_key in MANUAL_ORG_MUNICIPALITY_COVERAGE:
            manual_ids = MANUAL_ORG_MUNICIPALITY_COVERAGE[manual_key]
            if manual_ids == ["*"]:
                manual_ids = sorted(set(ref["municipality_id"].tolist()))
            for municipality_id in manual_ids:
                region_row = ref.loc[ref["municipality_id"] == str(municipality_id)].head(1)
                region_id = region_row.iloc[0]["region_id"] if not region_row.empty else ""
                coverage_rows.append({
                    "org_type": org_type,
                    "org_name_clean": org_name,
                    "authority_level": r.get("authority_level"),
                    "region_id": region_id,
                    "municipality_id": municipality_id,
                    "municipality_name": muni_name_lookup.get(municipality_id, ""),
                    "coverage_method": "manual_municipality_list",
                })
            continue

        # Generic fallback: if the org is already mapped to a single municipality
        # in org_to_municipality.csv, emit one coverage row automatically.
        if mapped_municipality_id and mapped_municipality_id.lower() != "nan":
            region_row = ref.loc[ref["municipality_id"] == mapped_municipality_id].head(1)
            region_id = region_row.iloc[0]["region_id"] if not region_row.empty else ""
            coverage_rows.append({
                "org_type": org_type,
                "org_name_clean": org_name,
                "authority_level": r.get("authority_level"),
                "region_id": region_id,
                "municipality_id": mapped_municipality_id,
                "municipality_name": muni_name_lookup.get(mapped_municipality_id, ""),
                "coverage_method": "mapped_municipality_id",
            })
            continue

        if str(r.get("authority_level", "")) == "national":
            for municipality_id in sorted(set(ref["municipality_id"].tolist())):
                region_row = ref.loc[ref["municipality_id"] == str(municipality_id)].head(1)
                region_id = region_row.iloc[0]["region_id"] if not region_row.empty else ""
                coverage_rows.append({
                    "org_type": org_type,
                    "org_name_clean": org_name,
                    "authority_level": r.get("authority_level"),
                    "region_id": region_id,
                    "municipality_id": municipality_id,
                    "municipality_name": muni_name_lookup.get(municipality_id, ""),
                    "coverage_method": "authority_national_all_municipalities",
                })
            continue

        if manual_key in MANUAL_ORG_REGION_COVERAGE:
            for region_id in MANUAL_ORG_REGION_COVERAGE[manual_key]:
                for municipality_id in region_to_munis.get(region_id, []):
                    coverage_rows.append({
                        "org_type": org_type,
                        "org_name_clean": org_name,
                        "authority_level": r.get("authority_level"),
                        "region_id": region_id,
                        "municipality_id": municipality_id,
                        "municipality_name": muni_name_lookup.get(municipality_id, ""),
                        "coverage_method": "manual_region_list",
                    })
            continue

        coverage_region_ids = str(r.get("coverage_region_ids", "") or "").strip()
        if not coverage_region_ids:
            continue

        for region_id in [x.strip() for x in coverage_region_ids.split(";") if x.strip()]:
            for municipality_id in region_to_munis.get(region_id, []):
                coverage_rows.append({
                    "org_type": org_type,
                    "org_name_clean": org_name,
                    "authority_level": r.get("authority_level"),
                    "region_id": region_id,
                    "municipality_id": municipality_id,
                    "municipality_name": muni_name_lookup.get(municipality_id, ""),
                    "coverage_method": r.get("match_method", ""),
                })

    if not coverage_rows:
        return pd.DataFrame(columns=[
            "org_type", "org_name_clean", "authority_level", "region_id",
            "municipality_id", "municipality_name", "coverage_method"
        ])

    cov = pd.DataFrame(coverage_rows).drop_duplicates()
    return cov.sort_values(["org_type", "org_name_clean", "municipality_id"]).reset_index(drop=True)


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

    # Always (re)write a template for municipality->region reference completion.
    build_region_municipality_template(muni_lookup).to_csv(REGION_MUNI_TEMPLATE_PATH, index=False)

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
        coverage_region_ids: list[str] = []
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
            region_id = canonicalize_region_name(org_name) or org_name
            coverage_region_ids = [region_id] if isinstance(region_id, str) else []
            match_method = "exact" if coverage_region_ids else "manual_review"
            if not coverage_region_ids:
                notes = "TODO: unresolved region canonicalization"

        elif org_type in ("ΠΕΡΙΦΕΡΕΙΑΚΟ ΤΑΜΕΙΟ ΑΝΑΠΤΥΞΗΣ", "ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ"):
            region_id = canonicalize_region_name(org_name)
            coverage_region_ids = [region_id] if isinstance(region_id, str) else []
            match_method = "rule_region_from_name" if coverage_region_ids else "manual_review"
            if not coverage_region_ids:
                notes = "TODO: unresolved region from org_name_clean"

        elif org_type == "ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ":
            coverage_region_ids = decentralized_coverage_regions(org_name)
            region_id = coverage_region_ids[0] if len(coverage_region_ids) == 1 else None
            match_method = "rule_decentralized_regions" if coverage_region_ids else "manual_review"
            if not coverage_region_ids:
                notes = "TODO: unresolved decentralized coverage"

        elif org_type in ("ΥΠΟΥΡΓΕΙΟ", "ΑΛΛΟΣ ΦΟΡΕΑΣ",
                           "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ"):
            match_method = "exact"  # authority level assigned by type

        rows.append({
            "org_type":        org_type,
            "org_name_clean":  org_name,
            "doc_count":       doc_count,
            "authority_level": authority,
            "municipality_id": municipality_id,
            "region_id":       region_id,
            "coverage_region_ids": ";".join(coverage_region_ids) if coverage_region_ids else "",
            "coverage_region_count": len(coverage_region_ids),
            "match_method":    match_method,
            "notes":           notes,
        })

    out = pd.DataFrame(rows).sort_values(["org_type", "org_name_clean"])
    out.to_csv(OUTPUT_PATH, index=False)
    coverage = maybe_build_org_coverage(out)
    if coverage is not None:
        coverage.to_csv(COVERAGE_OUTPUT_PATH, index=False)

    print(f"[done] {len(out)} org entries → {OUTPUT_PATH}")
    print(f"[done] template → {REGION_MUNI_TEMPLATE_PATH}")
    if coverage is not None:
        print(f"[done] {len(coverage)} coverage rows → {COVERAGE_OUTPUT_PATH}")
    else:
        print(f"[info] coverage expansion skipped (missing {REGION_MUNI_REF_PATH.name})")
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
