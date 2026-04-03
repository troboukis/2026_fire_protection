#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE_DIR = REPO_ROOT / "data" / "funding"
DEFAULT_OUTPUT = REPO_ROOT / "data" / "funding" / "municipal_funding.csv"
MUNICIPALITY_DB_PATH = REPO_ROOT / "backupDB" / "municipality.csv"
ORGANIZATION_DB_PATH = REPO_ROOT / "backupDB" / "organization.csv"

ZERO = Decimal("0")

ACCENT_TRANSLATION = str.maketrans(
    {
        "Ά": "Α",
        "Έ": "Ε",
        "Ή": "Η",
        "Ί": "Ι",
        "Ό": "Ο",
        "Ύ": "Υ",
        "Ώ": "Ω",
        "Ϊ": "Ι",
        "Ϋ": "Υ",
        "ά": "Α",
        "έ": "Ε",
        "ή": "Η",
        "ί": "Ι",
        "ό": "Ο",
        "ύ": "Υ",
        "ώ": "Ω",
        "ϊ": "Ι",
        "ϋ": "Υ",
        "ΐ": "Ι",
        "ΰ": "Υ",
    }
)

KNOWN_SYNDESMOS_NOMOI = (
    "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ",
    "ΠΕΙΡΑΙΩΣ",
    "ΑΘΗΝΩΝ",
    "ΣΑΜΟΥ",
)

MUNICIPALITY_ROW_RE = re.compile(
    r"^\s*(\d+)\s+(\d{5})\s+(.+?)\s{2,}([0-9A-ZΑ-ΩΆΈΉΊΌΎΏΪΫ .'\-]+?)\s+(\d[\d\.,]+)\s*$"
)
MUNICIPALITY_ROW_NO_AMOUNT_RE = re.compile(
    r"^\s*(\d+)\s+(\d{5})\s+(.+?)\s{2,}([0-9A-ZΑ-ΩΆΈΉΊΌΎΏΪΫ .'\-]+?)\s*$"
)
MUNICIPALITY_ROW_NO_CODE_RE = re.compile(r"^\s*(\d+)\s+(.+?)\s+(\d[\d\.,]+)\s*$")
MUNICIPALITY_CODE_NOMOS_RE = re.compile(r"^\s*(\d{5})\s+([0-9A-ZΑ-ΩΆΈΉΊΌΎΏΪΫ .'\-]+?)\s*$")
AMOUNT_RE = re.compile(r"(\d[\d\.]*,\d{2}|\d[\d\.]+)\s*$")
ADA_RE = re.compile(r"Α[Δ∆]Α[:\s]+([0-9A-ZΑ-ΩΆΈΉΊΌΎΏΪΫ]+-[0-9A-ZΑ-ΩΆΈΉΊΌΎΏΪΫ]+)")

ADA_OVERRIDES = {
    "Απόφαση-ΥΠ.ΕΣ.-26709-29.03.2023.pdf": "ΕΨΚ446ΜΤΛ6-ΡΞΥ",
}

MUNICIPALITY_NAME_OVERRIDES = {
    "ΑΝΝΑΣ": "ΜΑΝΤΟΥΔΙΟΥ ΛΙΜΝΗΣ ΑΓΙΑΣ ΑΝΝΑΣ",
    "ΔΙΣΤΟΜΟΥ ΑΡΑΧΟΒΑΣ": "ΔΙΣΤΟΜΟΥ ΑΡΑΧΟΒΑΣ ΑΝΤΙΚΥΡΑΣ",
    "ΚΕΝΤΡΙΚΗΣ ΚΕΡΚΥΡΑΣ": "ΚΕΝΤΡΙΚΗΣ ΚΕΡΚΥΡΑΣ ΚΑΙ ΔΙΑΠΟΝΤΙΩΝ ΝΗΣΩΝ",
    "ΚΕΝΤΡΙΚΗΣ ΚΕΡΚΥΡΑΣ ΚΑΙ": "ΚΕΝΤΡΙΚΗΣ ΚΕΡΚΥΡΑΣ ΚΑΙ ΔΙΑΠΟΝΤΙΩΝ ΝΗΣΩΝ",
    "ΚΕΡΚΥΡΑΙΩΝ": "ΚΕΡΚΥΡΑΣ",
    "ΚΕΦΑΛΛΗΝΙΑΣ": "ΚΕΦΑΛΟΝΙΑΣ",
    "ΜΑΝΤΟΥ ΑΝΝΑΣ": "ΜΑΝΤΟΥΔΙΟΥ ΛΙΜΝΗΣ ΑΓΙΑΣ ΑΝΝΑΣ",
    "ΜΕΓΑΝΗΣΟΥ": "ΜΕΓΑΝΗΣΙΟΥ",
    "ΝΙΚΑΙΑΣ ΑΓΙΟΥ ΙΩΑΝΝΟΥ ΡΕΝΤΗ": "ΝΙΚΑΙΑΣ ΑΓΙΟΥ ΙΩΑΝΝΗ ΡΕΝΤΗ",
    "ΠΡΕΒΕΖΗΣ": "ΠΡΕΒΕΖΑΣ",
    "ΣΤΥΛΙΔΟΣ": "ΣΤΥΛΙΔΑΣ",
}

SYNDESMOS_KEY_OVERRIDES = {
    "ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ ΑΣΔΑ": "org_f5dcc01e8b1e9096b75c",
    "ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ": "org_4e5878340eeacee52cb2",
    "ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΑΣ": "org_7877f6ea11565a0f807f",
    "ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΙΑΣ": "org_7877f6ea11565a0f807f",
    "ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ ΠΕΙΡΑΙΑ ΠΕΣΥΔΑΠ": "org_5cb2a10ee6ca5691e7fb",
    "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ ΣΥΝΠΑ": "org_ffefd3e4799ef6ddbe5a",
    "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ ΣΥΝ ΠΑ": "org_ffefd3e4799ef6ddbe5a",
    "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΛΑΣΗΣ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΤΗΣ ΠΕΡΙΟΧΗΣ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ ΑΤΤΙΚΗΣ ΣΠΑΠ": "org_287ebee5d6e5e055a518",
    "ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ": "org_2b83794cca39d51971af",
    "ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ ΣΠΑΥ": "org_2b83794cca39d51971af",
}


@dataclass(frozen=True)
class Row:
    year: int
    allocation_type: str
    recipient_type: str
    recipient_raw: str
    nomos: str
    amount_eur: Decimal
    source_file: str
    source_ada: str
    region_key: str = ""
    organization_key: str = ""
    municipality_key: str = ""


@dataclass(frozen=True)
class DocumentSpec:
    filename: str
    year: int
    allocation_type: str
    recipient_type: str
    parser: str
    expected_rows: int
    expected_total_eur: Decimal

# When a new funding PDF is added under data/funding:
# 1. Add a DocumentSpec entry here so the file is actually ingested.
# 2. Pick the parser and expected row/total values from the source decision.
# 3. If names do not resolve cleanly, extend MUNICIPALITY_NAME_OVERRIDES or
#    SYNDESMOS_KEY_OVERRIDES so municipality_key / organization_key match the DB.
# 4. Re-run scripts/build_fund_csv.py and confirm there are no unexpected warnings.
DOC_SPECS: tuple[DocumentSpec, ...] = (
    DocumentSpec("apof15717-110516.pdf", 2016, "τακτική", "δήμος", "municipality_text", 319, Decimal("16890000")),
    DocumentSpec("apof12471-130516.pdf", 2016, "τακτική", "σύνδεσμος", "manual_syndesmos", 8, Decimal("1510000")),
    DocumentSpec("ΨΠΝ3465ΧΘ7-3ΡΥ.pdf", 2017, "τακτική", "δήμος", "municipality_text", 319, Decimal("16910000")),
    DocumentSpec("apof15124-15052017.pdf", 2017, "τακτική", "σύνδεσμος", "manual_syndesmos", 7, Decimal("1490000")),
    DocumentSpec("7ΚΧΨ465ΧΘ7-Ξ20.pdf", 2017, "έκτακτη", "δήμος", "prose_single", 1, Decimal("10000")),
    DocumentSpec("6ΖΜ9465ΧΘ7-8ΡΖ_τακτική_δήμοι_2018.pdf", 2018, "τακτική", "δήμος", "municipality_text", 319, Decimal("16910000")),
    DocumentSpec("649Χ465ΧΘ7-ΡΝΒ_τακτική_σύνδεσμοι_ΟΤΑ_2018.pdf", 2018, "τακτική", "σύνδεσμος", "manual_syndesmos", 7, Decimal("1490000")),
    DocumentSpec("6ΔΖΟ465ΧΘ7-368_τακτική_δήμοι_2019.pdf", 2019, "τακτική", "δήμος", "municipality_text", 319, Decimal("16910000")),
    DocumentSpec("Ψ0ΜΣ465ΧΘ7-Υ78_τακτική_σύνδεσμοι_ΟΤΑ_2019.pdf", 2019, "τακτική", "σύνδεσμος", "manual_syndesmos", 7, Decimal("1490000")),
    DocumentSpec("ΩΚΓΔ46ΜΤΛ6-ΝΣ2_τακτική_δήμοι_2020.pdf", 2020, "τακτική", "δήμος", "municipality_text", 326, Decimal("16910000")),
    DocumentSpec("ΩΨΖΝ46ΜΤΛ6-Κ0Δ_τακτική_σύνδεσμοι_ΟΤΑ_2020.pdf", 2020, "τακτική", "σύνδεσμος", "manual_syndesmos", 7, Decimal("1490000")),
    DocumentSpec("9Γ6Ξ46ΜΤΛ6-Χ2Ι_τακτική_δήμοι_2021.pdf", 2021, "τακτική", "δήμος", "municipality_text", 326, Decimal("16910000")),
    DocumentSpec("617Ρ46ΜΤΛ6-ΝΧ5_τακτική_σύνδεσμοι_δήμων_2021.pdf", 2021, "τακτική", "σύνδεσμος", "manual_syndesmos", 7, Decimal("1490000")),
    DocumentSpec("6ΠΝ946ΜΤΛ6-ΑΧ8_τακτική_δήμοι_2022.pdf", 2022, "τακτική", "δήμος", "municipality_text", 326, Decimal("16910000")),
    DocumentSpec("Ψ7146ΜΤΛ6-ΖΥΡ_συμπληρωματική_δήμοι_σύνδεσμοι_δήμων_2022.pdf", 2022, "συμπληρωματική", "δήμος", "manual_scan_municipality", 48, Decimal("5230000")),
    DocumentSpec("Ψ7146ΜΤΛ6-ΖΥΡ_συμπληρωματική_δήμοι_σύνδεσμοι_δήμων_2022.pdf", 2022, "συμπληρωματική", "σύνδεσμος", "manual_scan_syndesmos", 7, Decimal("390000")),
    DocumentSpec("ΨΛΦ146ΜΤΛ6-4ΣΘ_συμπληρωματική_δήμοι_2022.pdf", 2022, "συμπληρωματική", "δήμος", "municipality_text", 7, Decimal("110000")),
    DocumentSpec("ΨΧ0Υ46ΜΤΛ6-89Σ_συμπληρωματική_δήμοι_2022.pdf", 2022, "συμπληρωματική", "δήμος", "municipality_text", 3, Decimal("150000")),
    DocumentSpec("Απόφαση-ΥΠ.ΕΣ.-26709-29.03.2023.pdf", 2023, "τακτική", "δήμος", "municipality_text", 332, Decimal("23140000")),
    DocumentSpec("εγκυκλιος-υπ.-εσωτερικών_2024.pdf", 2024, "τακτική", "δήμος", "municipality_text", 332, Decimal("27010000")),
    DocumentSpec("apof59051-20240813.pdf", 2024, "έκτακτη", "δήμος", "municipality_text", 8, Decimal("4700000")),
    DocumentSpec("apof12856-20250311.pdf", 2025, "τακτική", "δήμος", "municipality_text", 332, Decimal("37750000")),
    DocumentSpec("apof14727-20260317.pdf", 2026, "τακτική", "δήμος", "municipality_text", 332, Decimal("47500000")),
    DocumentSpec("apof14730-20260317.pdf", 2026, "τακτική", "σύνδεσμος", "manual_syndesmos", 7, Decimal("2500000")),
)


# This PDF is an image-only scan with no embedded text layer. The rows were
# transcribed directly from the source document so the generator can stay
# deterministic without depending on brittle OCR heuristics.
SCANNED_2022_MUNICIPALITY_ROWS: tuple[tuple[str, str, str, str], ...] = (
    ("51103", "ΕΠΙΔΑΥΡΟΥ", "ΑΡΓΟΛΙΔΑΣ", "120000"),
    ("59702", "ΑΣΠΡΟΠΥΡΓΟΥ", "ΑΤΤΙΚΗΣ", "70000"),
    ("59604", "ΑΧΑΡΝΩΝ", "ΑΤΤΙΚΗΣ", "100000"),
    ("59601", "ΔΙΟΝΥΣΟΥ", "ΑΤΤΙΚΗΣ", "140000"),
    ("59704", "ΕΛΕΥΣΙΝΑΣ", "ΑΤΤΙΚΗΣ", "100000"),
    ("59523", "ΚΗΦΙΣΙΑΣ", "ΑΤΤΙΚΗΣ", "100000"),
    ("59612", "ΚΡΩΠΙΑΣ", "ΑΤΤΙΚΗΣ", "140000"),
    ("59613", "ΛΑΥΡΕΩΤΙΚΗΣ", "ΑΤΤΙΚΗΣ", "140000"),
    ("59707", "ΜΑΝΔΡΑΣ-ΕΙΔΥΛΛΙΑΣ", "ΑΤΤΙΚΗΣ", "100000"),
    ("59614", "ΜΑΡΑΘΩΝΟΣ", "ΑΤΤΙΚΗΣ", "100000"),
    ("59708", "ΜΕΓΑΡΕΩΝ", "ΑΤΤΙΚΗΣ", "100000"),
    ("59617", "ΠΑΙΑΝΙΑΣ", "ΑΤΤΙΚΗΣ", "140000"),
    ("59814", "ΠΟΡΟΥ", "ΑΤΤΙΚΗΣ", "100000"),
    ("59619", "ΡΑΦΗΝΑΣ-ΠΙΚΕΡΜΙΟΥ", "ΑΤΤΙΚΗΣ", "140000"),
    ("59610", "ΣΑΡΩΝΙΚΟΥ", "ΑΤΤΙΚΗΣ", "140000"),
    ("59701", "ΦΥΛΗΣ", "ΑΤΤΙΚΗΣ", "100000"),
    ("59646", "ΩΡΩΠΟΥ", "ΑΤΤΙΚΗΣ", "180000"),
    ("50305", "ΔΙΣΤΟΜΟΥ-ΑΡΑΧΟΒΑΣ-ΑΝΤΙΚΥΡΑΣ", "ΒΟΙΩΤΙΑΣ", "100000"),
    ("50318", "ΤΑΝΑΓΡΑΣ", "ΒΟΙΩΤΙΑΣ", "100000"),
    ("58123", "ΡΟΔΟΥ", "ΔΩΔΕΚΑΝΗΣΟΥ", "100000"),
    ("57113", "ΣΑΜΟΘΡΑΚΗΣ", "ΕΒΡΟΥ", "30000"),
    ("50417", "ΔΙΡΦΥΩΝ-ΜΕΣΣΑΠΙΩΝ", "ΕΥΒΟΙΑΣ", "120000"),
    ("50410", "ΙΣΤΙΑΙΑΣ-ΑΙΔΗΨΟΥ", "ΕΥΒΟΙΑΣ", "100000"),
    ("50411", "ΚΑΡΥΣΤΟΥ", "ΕΥΒΟΙΑΣ", "100000"),
    ("50420", "ΚΥΜΗΣ-ΑΛΙΒΕΡΙΟΥ", "ΕΥΒΟΙΑΣ", "100000"),
    ("50409", "ΜΑΝΤΟΥΔΙΟΥ-ΛΙΜΝΗΣ-ΑΓΙΑΣ ΑΝΝΑΣ", "ΕΥΒΟΙΑΣ", "100000"),
    ("50425", "ΣΚΥΡΟΥ", "ΕΥΒΟΙΑΣ", "100000"),
    ("52105", "ΖΑΚΥΝΘΟΥ", "ΖΑΚΥΝΘΟΥ", "90000"),
    ("51418", "ΑΝΔΡΙΤΣΑΙΝΑΣ-ΚΡΕΣΤΕΝΩΝ", "ΗΛΕΙΑΣ", "120000"),
    ("51405", "ΑΡΧΑΙΑΣ ΟΛΥΜΠΙΑΣ", "ΗΛΕΙΑΣ", "140000"),
    ("51410", "ΖΑΧΑΡΩΣ", "ΗΛΕΙΑΣ", "120000"),
    ("51402", "ΗΛΙΔΑΣ", "ΗΛΕΙΑΣ", "110000"),
    ("51417", "ΠΥΡΓΟΥ", "ΗΛΕΙΑΣ", "110000"),
    ("55443", "ΩΡΑΙΟΚΑΣΤΡΟΥ", "ΘΕΣΣΑΛΟΝΙΚΗΣ", "180000"),
    ("55503", "ΘΑΣΟΥ", "ΚΑΒΑΛΑΣ", "120000"),
    ("52306", "ΑΡΓΟΣΤΟΛΙΟΥ", "ΚΕΦΑΛΛΗΝΙΑΣ", "50000"),
    ("52304", "ΙΘΑΚΗΣ", "ΚΕΦΑΛΛΗΝΙΑΣ", "100000"),
    ("51505", "ΚΟΡΙΝΘΙΩΝ", "ΚΟΡΙΝΘΙΑΣ", "130000"),
    ("51506", "ΛΟΥΤΡΑΚΙΟΥ-ΠΕΡΑΧΩΡΑΣ-ΑΓΙΩΝ ΘΕΟΔΩΡΩΝ", "ΚΟΡΙΝΘΙΑΣ", "100000"),
    ("51617", "ΣΠΑΡΤΗΣ", "ΛΑΚΩΝΙΑΣ", "100000"),
    ("58316", "ΔΥΤΙΚΗΣ ΛΕΣΒΟΥ", "ΛΕΣΒΟΥ", "90000"),
    ("58315", "ΜΥΤΙΛΗΝΗΣ", "ΛΕΣΒΟΥ", "90000"),
    ("54307", "ΖΑΓΟΡΑΣ-ΜΟΥΡΕΣΙΟΥ", "ΜΑΓΝΗΣΙΑΣ", "65000"),
    ("54304", "ΝΟΤΙΟΥ ΠΗΛΙΟΥ", "ΜΑΓΝΗΣΙΑΣ", "75000"),
    ("58400", "ΑΝΑΤΟΛΙΚΗΣ ΣΑΜΟΥ", "ΣΑΜΟΥ", "100000"),
    ("58410", "ΔΥΤΙΚΗΣ ΣΑΜΟΥ", "ΣΑΜΟΥ", "100000"),
    ("50703", "ΛΟΚΡΩΝ", "ΦΘΙΩΤΙΔΑΣ", "100000"),
    ("56405", "ΚΑΣΣΑΝΔΡΑΣ", "ΧΑΛΚΙΔΙΚΗΣ", "180000"),
)

SCANNED_2022_SYNDESMOS_ROWS: tuple[tuple[str, str, str], ...] = (
    (
        "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ & ΚΟΙΝΟΤΗΤΩΝ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΛΑΣΗΣ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΤΗΣ ΠΕΡΙΟΧΗΣ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ ΑΤΤΙΚΗΣ (ΣΠΑΠ)",
        "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ",
        "80000",
    ),
    ("ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ", "ΑΘΗΝΩΝ", "70000"),
    (
        "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ (ΣΥΝΠΑ)",
        "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ",
        "60000",
    ),
    ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)", "ΑΘΗΝΩΝ", "60000"),
    ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΑΣ", "ΠΕΙΡΑΙΩΣ", "20000"),
    ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "50000"),
    ("ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ - ΠΕΙΡΑΙΑ (ΠΕΣΥΔΑΠ)", "ΑΘΗΝΩΝ", "50000"),
)

MANUAL_SYNDESMOS_DOC_ROWS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "apof12471-130516.pdf": (
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ & ΚΟΙΝΟΤΗΤΩΝ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΛΑΣΗΣ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΤΗΣ ΠΕΡΙΟΧΗΣ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ ΑΤΤΙΚΗΣ (ΣΠΑΠ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ (ΣΠΑΥ)", "ΑΘΗΝΩΝ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ (ΣΥΝΠΑ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "280000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)", "ΑΘΗΝΩΝ", "250000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΑΣ", "ΠΕΙΡΑΙΩΣ", "40000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "150000"),
        ("ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ - ΠΕΙΡΑΙΑ (ΠΕΣΥΔΑΠ)", "ΑΘΗΝΩΝ", "175000"),
        ("ΣΥΝΔΕΣΜΟΣ ΟΤΑ ΔΗΜΩΝ ΒΑΘΕΩΣ ΠΥΘΑΓΟΡΕΙΟΥ", "ΣΑΜΟΥ", "15000"),
    ),
    "apof15124-15052017.pdf": (
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ & ΚΟΙΝΟΤΗΤΩΝ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΛΑΣΗΣ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΤΗΣ ΠΕΡΙΟΧΗΣ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ ΑΤΤΙΚΗΣ (ΣΠΑΠ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ (ΣΠΑΥ)", "ΑΘΗΝΩΝ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ (ΣΥΝΠΑ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "280000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)", "ΑΘΗΝΩΝ", "250000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΑΣ", "ΠΕΙΡΑΙΩΣ", "35000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "150000"),
        ("ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ - ΠΕΙΡΑΙΑ (ΠΕΣΥΔΑΠ)", "ΑΘΗΝΩΝ", "175000"),
    ),
    "649Χ465ΧΘ7-ΡΝΒ_τακτική_σύνδεσμοι_ΟΤΑ_2018.pdf": (
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ & ΚΟΙΝΟΤΗΤΩΝ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΛΑΣΗΣ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΤΗΣ ΠΕΡΙΟΧΗΣ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ ΑΤΤΙΚΗΣ (ΣΠΑΠ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ (ΣΠΑΥ)", "ΑΘΗΝΩΝ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ (ΣΥΝΠΑ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "280000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)", "ΑΘΗΝΩΝ", "250000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΑΣ", "ΠΕΙΡΑΙΩΣ", "35000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "150000"),
        ("ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ - ΠΕΙΡΑΙΑ (ΠΕΣΥΔΑΠ)", "ΑΘΗΝΩΝ", "175000"),
    ),
    "Ψ0ΜΣ465ΧΘ7-Υ78_τακτική_σύνδεσμοι_ΟΤΑ_2019.pdf": (
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ & ΚΟΙΝΟΤΗΤΩΝ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΛΑΣΗΣ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΤΗΣ ΠΕΡΙΟΧΗΣ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ ΑΤΤΙΚΗΣ (ΣΠΑΠ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ (ΣΠΑΥ)", "ΑΘΗΝΩΝ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ (ΣΥΝΠΑ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "280000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)", "ΑΘΗΝΩΝ", "250000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΑΣ", "ΠΕΙΡΑΙΩΣ", "35000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "150000"),
        ("ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ - ΠΕΙΡΑΙΑ (ΠΕΣΥΔΑΠ)", "ΑΘΗΝΩΝ", "175000"),
    ),
    "ΩΨΖΝ46ΜΤΛ6-Κ0Δ_τακτική_σύνδεσμοι_ΟΤΑ_2020.pdf": (
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ & ΚΟΙΝΟΤΗΤΩΝ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΛΑΣΗΣ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΤΗΣ ΠΕΡΙΟΧΗΣ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ ΑΤΤΙΚΗΣ (ΣΠΑΠ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ (ΣΠΑΥ)", "ΑΘΗΝΩΝ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ (ΣΥΝΠΑ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "280000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)", "ΑΘΗΝΩΝ", "250000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΑΣ", "ΠΕΙΡΑΙΩΣ", "35000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "150000"),
        ("ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ - ΠΕΙΡΑΙΑ (ΠΕΣΥΔΑΠ)", "ΑΘΗΝΩΝ", "175000"),
    ),
    "617Ρ46ΜΤΛ6-ΝΧ5_τακτική_σύνδεσμοι_δήμων_2021.pdf": (
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ & ΚΟΙΝΟΤΗΤΩΝ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΛΑΣΗΣ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΤΗΣ ΠΕΡΙΟΧΗΣ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ ΑΤΤΙΚΗΣ (ΣΠΑΠ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ (ΣΠΑΥ)", "ΑΘΗΝΩΝ", "300000"),
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ (ΣΥΝΠΑ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "280000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)", "ΑΘΗΝΩΝ", "250000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΑΣ", "ΠΕΙΡΑΙΩΣ", "35000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "150000"),
        ("ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ - ΠΕΙΡΑΙΑ (ΠΕΣΥΔΑΠ)", "ΑΘΗΝΩΝ", "175000"),
    ),
    "apof14730-20260317.pdf": (
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ & ΚΟΙΝΟΤΗΤΩΝ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΛΑΣΗΣ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΤΗΣ ΠΕΡΙΟΧΗΣ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ ΑΤΤΙΚΗΣ (ΣΠΑΠ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "440000"),
        ("ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ (ΣΠΑΥ)", "ΑΘΗΝΩΝ", "440000"),
        ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ (ΣΥΝΠΑ)", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "440000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)", "ΑΘΗΝΩΝ", "390000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΙΑΣ", "ΠΕΙΡΑΙΩΣ", "170000"),
        ("ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ", "ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ", "335000"),
        ("ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ - ΠΕΙΡΑΙΑ (ΠΕΣΥΔΑΠ)", "ΑΘΗΝΩΝ", "285000"),
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build backupDB/fund.csv from funding PDFs.")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR, help="Directory with funding PDFs.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="CSV file to write.")
    parser.add_argument(
        "--timestamp",
        help="UTC timestamp to write into created_at/updated_at. Defaults to current UTC.",
    )
    return parser.parse_args()


def normalize_text(text: str) -> str:
    replacements = {
        "\u00a0": " ",
        "\ufeff": "",
        "∆": "Δ",
        "−": "-",
        "‐": "-",
        "‑": "-",
        "–": "-",
        "—": "-",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_lookup_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "")
    normalized = normalize_text(normalized).translate(ACCENT_TRANSLATION).upper()
    normalized = normalized.replace("&", " ΚΑΙ ")
    normalized = re.sub(r"[()«»/]", " ", normalized)
    normalized = re.sub(r"\bΔΗΜΟΣ\b", " ", normalized)
    normalized = re.sub(r"\bΔΗΜΟΥ\b", " ", normalized)
    normalized = re.sub(r"\bΔ\.\b", " ", normalized)
    normalized = re.sub(r"\bΔ\b", " ", normalized)
    normalized = re.sub(r"[^A-ZΑ-Ω0-9]+", " ", normalized)
    return normalize_spaces(normalized)


def parse_amount(value: str) -> Decimal:
    stripped = value.strip()
    if "," in stripped:
        normalized = stripped.replace(".", "").replace(",", ".")
        return Decimal(normalized)

    if stripped.count(".") > 1:
        parts = stripped.split(".")
        if len(parts[-1]) == 2:
            normalized = "".join(parts[:-1]) + "." + parts[-1]
        else:
            normalized = "".join(parts)
        return Decimal(normalized)

    if stripped.count(".") == 1:
        left, right = stripped.split(".", 1)
        if len(right) == 3:
            return Decimal(left + right)

    normalized = stripped
    return Decimal(normalized)


def format_amount(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def run_command(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return normalize_text(completed.stdout)


def read_pdf_text(pdf_path: Path) -> str:
    return run_command(["pdftotext", "-layout", "-nopgbrk", str(pdf_path), "-"])


def extract_ada(text: str, filename: str) -> str:
    if filename in ADA_OVERRIDES:
        return ADA_OVERRIDES[filename]

    match = ADA_RE.search(text)
    if match:
        return match.group(1)

    stem = Path(filename).stem
    prefix = stem.split("_", 1)[0]
    if re.fullmatch(r"[0-9A-ZΑ-ΩΆΈΉΊΌΎΏΪΫ]+-[0-9A-ZΑ-ΩΆΈΉΊΌΎΏΪΫ]+", prefix):
        return prefix
    if re.fullmatch(r"[0-9A-ZΑ-ΩΆΈΉΊΌΎΏΪΫ]+-[0-9A-ZΑ-ΩΆΈΉΊΌΎΏΪΫ]+", stem):
        return stem
    return ""


def parse_municipality_text(spec: DocumentSpec, text: str) -> list[Row]:
    rows: list[Row] = []
    source_ada = extract_ada(text, spec.filename)
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        raw_line = lines[index]
        compact = normalize_spaces(raw_line)
        previous_compact = normalize_spaces(lines[index - 1]) if index > 0 else ""
        if (
            not compact
            or "Σύνολο -" in compact
            or compact == "ΣΥΝΟΛΟ"
            or "ΓΕΝΙΚΟ ΑΘΡΟΙΣΜΑ" in compact
            or ("Σύνολο -" in previous_compact and not re.match(r"^\s*\d+\s+\d{5}\s+", raw_line))
        ):
            index += 1
            continue
        match = MUNICIPALITY_ROW_RE.match(raw_line)
        if match:
            municipality_key = match.group(2)
            recipient_raw = normalize_spaces(match.group(3))
            nomos = normalize_spaces(match.group(4))
            amount_eur = parse_amount(match.group(5))
            index += 1
        else:
            amount_match = AMOUNT_RE.search(compact)
            next_line = lines[index + 1] if index + 1 < len(lines) else ""
            next_match = MUNICIPALITY_ROW_NO_AMOUNT_RE.match(next_line)
            if not amount_match or not next_match or re.match(r"^\s*\d+\s+\d{5}\s+", raw_line):
                broken_current_match = MUNICIPALITY_ROW_NO_CODE_RE.match(raw_line)
                code_nomos_match = MUNICIPALITY_CODE_NOMOS_RE.match(next_line)
                trailing_name = normalize_spaces(lines[index + 2]) if index + 2 < len(lines) else ""
                if not broken_current_match or not code_nomos_match or not trailing_name:
                    index += 1
                    continue

                municipality_key = code_nomos_match.group(1)
                recipient_raw = normalize_spaces(f"{broken_current_match.group(2)} {trailing_name}")
                nomos = normalize_spaces(code_nomos_match.group(2))
                amount_eur = parse_amount(broken_current_match.group(3))
                index += 3
            else:
                municipality_key = next_match.group(2)
                recipient_raw = normalize_spaces(f"{raw_line[: amount_match.start()].strip()} {next_match.group(3)}")
                nomos = normalize_spaces(next_match.group(4))
                amount_eur = parse_amount(amount_match.group(1))
                index += 2

        rows.append(
            Row(
                year=spec.year,
                allocation_type=spec.allocation_type,
                recipient_type=spec.recipient_type,
                recipient_raw=recipient_raw,
                nomos=nomos,
                municipality_key=municipality_key,
                amount_eur=amount_eur,
                source_file=spec.filename,
                source_ada=source_ada,
            )
        )
    
    return rows


def parse_syndesmos_block(block: str) -> tuple[str, str, Decimal]:
    compact = normalize_spaces(block)
    amount_match = AMOUNT_RE.search(compact)
    if not amount_match:
        raise ValueError(f"Could not locate amount in syndesmos row: {compact}")
    amount_eur = parse_amount(amount_match.group(1))
    prefix = compact[: amount_match.start()].strip()

    for nomos in KNOWN_SYNDESMOS_NOMOI:
        idx = prefix.rfind(nomos)
        if idx == -1:
            continue
        name = normalize_spaces(prefix[:idx])
        if not name:
            continue
        return name, nomos, amount_eur

    raise ValueError(f"Could not split syndesmos row into name/nomos: {compact}")


def parse_syndesmos_text(spec: DocumentSpec, text: str) -> list[Row]:
    rows: list[Row] = []
    source_ada = extract_ada(text, spec.filename)
    current_parts: list[str] = []
    in_table = False

    for raw_line in text.splitlines():
        line = normalize_spaces(raw_line)
        if not line:
            continue
        if "ΟΝΟΜΑ ΣΥΝΔΕΣΜΟΥ" in line or "ΟΝΟΜΑΣΙΑ ΣΥΝΔΕΣΜΟΥ" in line:
            in_table = True
            continue
        if not in_table:
            continue
        if line.startswith("ΑΔΑ:") or line == "A/A":
            continue
        if "ΚΑΤΑΝΟΜΗ ΠΙΣΤΩΣΕΩΝ ΑΝΑ ΣΥΝΔΕΣΜΟ" in line or "ΚΑΤΑΝΟΜΗ ΑΝΑ ΣΥΝΔΕΣΜΟ" in line:
            continue
        if re.fullmatch(r"\[?\d+\]?", line):
            continue
        if line.startswith("ΣΥΝΟΛΟ"):
            break
        current_parts.append(re.sub(r"^\d+\s+", "", line))
        if not AMOUNT_RE.search(line):
            continue

        name, nomos, amount_eur = parse_syndesmos_block(" ".join(current_parts))
        rows.append(
            Row(
                year=spec.year,
                allocation_type=spec.allocation_type,
                recipient_type=spec.recipient_type,
                recipient_raw=name,
                nomos=nomos,
                amount_eur=amount_eur,
                source_file=spec.filename,
                source_ada=source_ada,
            )
        )
        current_parts = []

    return rows


def parse_prose_single(spec: DocumentSpec, text: str) -> list[Row]:
    source_ada = extract_ada(text, spec.filename)
    amount_match = re.search(r"ποσού\s+(\d[\d\.\,]+)\s*€", text)
    if not amount_match:
        raise ValueError(f"Could not find amount in {spec.filename}")

    municipality_name = ""
    patterns = (
        r"Επιχορήγηση του\s+Δ[ήη][µμ]?ου\s+(.+?)\s+με ποσό",
        r"στον\s+Δ[ήη][µμ]?ο\s+(.+?)\s+για την",
    )
    for pattern in patterns:
        municipality_match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if municipality_match:
            municipality_name = normalize_spaces(municipality_match.group(1))
            break

    if not municipality_name:
        raise ValueError(f"Could not find municipality in {spec.filename}")

    return [
        Row(
            year=spec.year,
            allocation_type=spec.allocation_type,
            recipient_type=spec.recipient_type,
            recipient_raw=f"ΔΗΜΟΣ {municipality_name.upper()}",
            nomos="",
            amount_eur=parse_amount(amount_match.group(1)),
            source_file=spec.filename,
            source_ada=source_ada,
        )
    ]


def parse_manual_scan_municipality(spec: DocumentSpec) -> list[Row]:
    source_ada = extract_ada("", spec.filename)
    return [
        Row(
            year=spec.year,
            allocation_type=spec.allocation_type,
            recipient_type=spec.recipient_type,
            recipient_raw=recipient_raw,
            nomos=nomos,
            municipality_key=municipality_key,
            amount_eur=Decimal(amount_text),
            source_file=spec.filename,
            source_ada=source_ada,
        )
        for municipality_key, recipient_raw, nomos, amount_text in SCANNED_2022_MUNICIPALITY_ROWS
    ]


def parse_manual_scan_syndesmos(spec: DocumentSpec) -> list[Row]:
    source_ada = extract_ada("", spec.filename)
    return [
        Row(
            year=spec.year,
            allocation_type=spec.allocation_type,
            recipient_type=spec.recipient_type,
            recipient_raw=recipient_raw,
            nomos=nomos,
            amount_eur=Decimal(amount_text),
            source_file=spec.filename,
            source_ada=source_ada,
        )
        for recipient_raw, nomos, amount_text in SCANNED_2022_SYNDESMOS_ROWS
    ]


def parse_manual_syndesmos(spec: DocumentSpec, source_ada: str) -> list[Row]:
    rows = MANUAL_SYNDESMOS_DOC_ROWS.get(spec.filename)
    if rows is None:
        raise ValueError(f"No manual syndesmos mapping for {spec.filename}")
    return [
        Row(
            year=spec.year,
            allocation_type=spec.allocation_type,
            recipient_type=spec.recipient_type,
            recipient_raw=recipient_raw,
            nomos=nomos,
            amount_eur=Decimal(amount_text),
            source_file=spec.filename,
            source_ada=source_ada,
        )
        for recipient_raw, nomos, amount_text in rows
    ]


def parse_document(spec: DocumentSpec, source_dir: Path) -> list[Row]:
    pdf_path = source_dir / spec.filename
    if not pdf_path.exists():
        raise FileNotFoundError(f"Missing source PDF: {pdf_path}")

    if spec.parser == "manual_scan_municipality":
        rows = parse_manual_scan_municipality(spec)
    elif spec.parser == "manual_scan_syndesmos":
        rows = parse_manual_scan_syndesmos(spec)
    elif spec.parser == "manual_syndesmos":
        text = read_pdf_text(pdf_path)
        rows = parse_manual_syndesmos(spec, extract_ada(text, spec.filename))
    else:
        text = read_pdf_text(pdf_path)
        if spec.parser == "municipality_text":
            rows = parse_municipality_text(spec, text)
        elif spec.parser == "syndesmos_text":
            rows = parse_syndesmos_text(spec, text)
        elif spec.parser == "prose_single":
            rows = parse_prose_single(spec, text)
        else:
            raise ValueError(f"Unsupported parser kind: {spec.parser}")

    validate_rows(spec, rows)
    return rows


def validate_rows(spec: DocumentSpec, rows: list[Row]) -> None:
    if len(rows) != spec.expected_rows:
        raise ValueError(
            f"{spec.filename}: expected {spec.expected_rows} rows, parsed {len(rows)}"
        )

    total = sum((row.amount_eur for row in rows), start=ZERO)
    if total != spec.expected_total_eur:
        raise ValueError(
            f"{spec.filename}: expected total {spec.expected_total_eur}, parsed {total}"
        )


def load_municipality_lookup() -> dict[str, list[dict[str, str]]]:
    lookup: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    with MUNICIPALITY_DB_PATH.open(encoding="utf-8-sig", newline="") as fh:
        for raw_row in csv.DictReader(fh):
            entry = {
                "municipality_key": raw_row["municipality_key"],
                "region_key": raw_row["region_key"],
            }
            for value in (raw_row["municipality_value"], raw_row["municipality_normalized_value"]):
                alias = normalize_lookup_key(value)
                if alias and entry not in lookup[alias]:
                    lookup[alias].append(entry)
    return dict(lookup)


def load_organization_lookup() -> tuple[dict[str, list[str]], set[str]]:
    lookup: defaultdict[str, list[str]] = defaultdict(list)
    keys: set[str] = set()
    with ORGANIZATION_DB_PATH.open(encoding="utf-8-sig", newline="") as fh:
        for raw_row in csv.DictReader(fh):
            organization_key = raw_row["organization_key"]
            keys.add(organization_key)
            for value in (raw_row["organization_value"], raw_row["organization_normalized_value"]):
                alias = normalize_lookup_key(value)
                if alias and organization_key not in lookup[alias]:
                    lookup[alias].append(organization_key)
    return dict(lookup), keys


def resolve_municipality(row: Row, lookup: dict[str, list[dict[str, str]]]) -> Row:
    normalized_name = normalize_lookup_key(row.recipient_raw)
    normalized_name = MUNICIPALITY_NAME_OVERRIDES.get(normalized_name, normalized_name)
    candidates = lookup.get(normalized_name, [])
    if not candidates:
        raise ValueError(
            f"Could not resolve municipality_key for '{row.recipient_raw}' ({row.nomos}) from municipality.csv"
        )

    unique_candidates = {
        (candidate["municipality_key"], candidate["region_key"]) for candidate in candidates
    }
    if len(unique_candidates) > 1:
        normalized_nomos = normalize_lookup_key(row.nomos)
        if normalized_name == "ΗΡΑΚΛΕΙΟΥ":
            if normalized_nomos in {"ΑΘΗΝΩΝ", "ΑΤΤΙΚΗΣ"}:
                municipality_key = "9170"
            elif normalized_nomos == "ΗΡΑΚΛΕΙΟΥ":
                municipality_key = "9305"
            else:
                raise ValueError(
                    f"Ambiguous municipality_key for '{row.recipient_raw}' with nomos '{row.nomos}'"
                )
            selected = next(
                candidate for candidate in candidates if candidate["municipality_key"] == municipality_key
            )
        else:
            raise ValueError(
                f"Ambiguous municipality_key for '{row.recipient_raw}' ({row.nomos}): {sorted(unique_candidates)}"
            )
    else:
        selected = candidates[0]

    return replace(
        row,
        municipality_key=selected["municipality_key"],
        region_key=selected["region_key"],
    )


def resolve_syndesmos(
    row: Row,
    organization_lookup: dict[str, list[str]],
    organization_keys: set[str],
) -> Row:
    normalized_name = normalize_lookup_key(row.recipient_raw)
    organization_key = SYNDESMOS_KEY_OVERRIDES.get(normalized_name, "")
    if not organization_key:
        matches = organization_lookup.get(normalized_name, [])
        if len(matches) == 1:
            organization_key = matches[0]

    if organization_key and organization_key not in organization_keys:
        raise ValueError(
            f"Configured organization_key {organization_key} for '{row.recipient_raw}' is missing from organization.csv"
        )

    return replace(row, organization_key=organization_key)


def enrich_rows(rows: list[Row]) -> list[Row]:
    municipality_lookup = load_municipality_lookup()
    organization_lookup, organization_keys = load_organization_lookup()
    unresolved_syndesmos: set[str] = set()
    enriched_rows: list[Row] = []

    for row in rows:
        if row.recipient_type == "δήμος":
            enriched_rows.append(resolve_municipality(row, municipality_lookup))
            continue

        enriched = resolve_syndesmos(row, organization_lookup, organization_keys)
        if not enriched.organization_key:
            unresolved_syndesmos.add(row.recipient_raw)
        enriched_rows.append(enriched)

    if unresolved_syndesmos:
        print(
            "warning: unresolved organization_key for syndesmos: "
            + ", ".join(sorted(unresolved_syndesmos)),
            file=sys.stderr,
        )

    return enriched_rows


def timestamp_value(raw: str | None) -> str:
    if raw:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).isoformat(sep=" ")
    return datetime.now(timezone.utc).isoformat(sep=" ")


def write_csv(rows: list[Row], output_path: Path, created_updated_at: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "id",
        "region_key",
        "organization_key",
        "municipality_key",
        "year",
        "allocation_type",
        "recipient_type",
        "recipient_raw",
        "nomos",
        "amount_eur",
        "source_file",
        "source_ada",
        "created_at",
        "updated_at",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for idx, row in enumerate(rows, start=1):
            writer.writerow(
                {
                    "id": idx,
                    "region_key": row.region_key,
                    "organization_key": row.organization_key,
                    "municipality_key": row.municipality_key,
                    "year": row.year,
                    "allocation_type": row.allocation_type,
                    "recipient_type": row.recipient_type,
                    "recipient_raw": row.recipient_raw,
                    "nomos": row.nomos,
                    "amount_eur": format_amount(row.amount_eur),
                    "source_file": row.source_file,
                    "source_ada": row.source_ada,
                    "created_at": created_updated_at,
                    "updated_at": created_updated_at,
                }
            )


def build_rows(source_dir: Path) -> list[Row]:
    rows: list[Row] = []
    for spec in DOC_SPECS:
        rows.extend(parse_document(spec, source_dir))

    rows = enrich_rows(rows)

    return sorted(
        rows,
        key=lambda row: (
            row.year,
            row.source_file,
            row.recipient_type,
            row.municipality_key or "99999",
            row.recipient_raw,
        ),
    )


def main() -> int:
    args = parse_args()
    try:
        rows = build_rows(args.source_dir.resolve())
        created_updated_at = timestamp_value(args.timestamp)
        write_csv(rows, args.output.resolve(), created_updated_at)
    except Exception as exc:
        print(f"municipal_funding.csv build failed: {exc}", file=sys.stderr)
        return 1

    grand_total = sum((row.amount_eur for row in rows), start=ZERO)
    print(f"rows={len(rows)} total_eur={grand_total} output={args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
