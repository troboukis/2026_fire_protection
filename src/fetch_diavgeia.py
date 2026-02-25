"""
fetch_diavgeia.py
-----------------
Incrementally fetches fire-protection related decisions from Diavgeia
and appends new records to data/2026_diavgeia.csv.

State is tracked in state/state.json (last successful fetch timestamp).
Run this script periodically to keep the dataset up to date.
"""

import json
import re
import ast
import unicodedata
from urllib.parse import quote
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from IPython.display import clear_output  # noqa: F401 (safe to remove if not in Jupyter)

try:
    from .pdf_pipeline import extract_document_code, parse_pdf_file_to_row
except ImportError:  # script execution fallback: `python src/fetch_diavgeia.py`
    from pdf_pipeline import extract_document_code, parse_pdf_file_to_row

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent

DATA_DIR = REPO_DIR / "data"
STATE_DIR = REPO_DIR / "state"
LOG_DIR = REPO_DIR / "logs"
CSV_PATH = DATA_DIR / "2026_diavgeia.csv"
STATE_FILE = STATE_DIR / "state.json"
RUN_LOG_FILE = LOG_DIR / "fetch_runs.csv"
LEGACY_CSV_PATH = REPO_DIR / "2026_diavgeia.csv"
LEGACY_STATE_FILE = REPO_DIR / "state.json"
ATHENS_TZ = ZoneInfo("Europe/Athens")
PDF_DIR = REPO_DIR / "pdf"
PDF_DOWNLOAD_TIMEOUT = 60
PDF_EMBED_COLUMNS = [
    "pdf_file_name",
    "pdf_download_status",
    "pdf_download_error",
    "pdf_parse_status",
    "pdf_parse_error",
    "pdf_page_count",
    "pdf_text",
    "pdf_text_length",
]

SEARCH_URL = "https://diavgeia.gov.gr/luminapi/api/search"
DECISION_VIEW_URL = "https://diavgeia.gov.gr/luminapi/api/decisions/view"
KEYWORDS = ["πυροπροστ", "αποψιλ", "δασοπροστ", "αντιπυρ"]
PAGE_SIZE = 100
SPENDING_APPROVAL_LABEL = "ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ"
COMMITMENT_LABEL = "ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ"
DIRECT_ASSIGNMENT_LABEL = "ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ"
PAYMENT_FINALIZATION_LABEL = "ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ"
SPENDING_ENRICHMENT_COLUMNS = [
    "spending_signers",
    "spending_contractors_afm",
    "spending_contractors_name",
    "spending_contractors_value",
    "spending_contractors_currency",
    "spending_contractors_count",
    "spending_contractors_details",
    "spending_enrichment_status",
    "spending_enrichment_error",
]
COMMITMENT_ENRICHMENT_COLUMNS = [
    "commitment_signers",
    "commitment_fiscal_year",
    "commitment_budget_category",
    "commitment_counterparty",
    "commitment_amount_with_vat",
    "commitment_remaining_available_credit",
    "commitment_kae_ale_number",
    "commitment_remaining_kae_ale",
    "commitment_lines_count",
    "commitment_lines_details",
    "commitment_enrichment_status",
    "commitment_enrichment_error",
]
DIRECT_ENRICHMENT_COLUMNS = [
    "direct_signers",
    "direct_afm",
    "direct_name",
    "direct_value",
    "direct_related_commitment",
    "direct_see_also",
    "direct_people_count",
    "direct_people_details",
    "direct_enrichment_status",
    "direct_enrichment_error",
]
PAYMENT_ENRICHMENT_COLUMNS = [
    "payment_signers",
    "payment_beneficiary_afm",
    "payment_beneficiary_name",
    "payment_value",
    "payment_related_commitment_or_spending",
    "payment_see_also",
    "payment_beneficiaries_count",
    "payment_beneficiaries_details",
    "payment_enrichment_status",
    "payment_enrichment_error",
]
ENRICHMENT_SINGLETON_COLLAPSE_COLUMNS = [
    # spending
    "spending_signers",
    "spending_contractors_afm",
    "spending_contractors_name",
    "spending_contractors_value",
    "spending_contractors_currency",
    # commitment
    "commitment_signers",
    "commitment_counterparty",
    "commitment_amount_with_vat",
    "commitment_remaining_available_credit",
    "commitment_kae_ale_number",
    "commitment_remaining_kae_ale",
    # direct
    "direct_signers",
    "direct_afm",
    "direct_name",
    "direct_related_commitment",
    "direct_see_also",
    # payment
    "payment_signers",
    "payment_beneficiary_afm",
    "payment_beneficiary_name",
    "payment_value",
    "payment_related_commitment_or_spending",
    "payment_see_also",
]

# Ordered list: (prefix, clean org_type label).
# More-specific / longer prefixes MUST come before shorter ones.
ORG_PREFIXES = [
    ("ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ",                  "Αποκεντρωμένη Διοίκηση"),
    ("ΠΕΡΙΦΕΡΕΙΑΚΟ ΤΑΜΕΙΟ ΑΝΑΠΤΥΞΗΣ",           "Περιφερειακό Ταμείο Ανάπτυξης"),
    ("ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ",  "Κέντρο Κοινωνικής Πρόνοιας Περιφέρειας"),
    ("ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ",                         "Σύνδεσμος Δήμων"),
    ("ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ",                "Δημοτικό Λιμενικό Ταμείο"),
    ("ΔΗΜΟΤΙΚΟ ΒΡΕΦΟΚΟΜΕΙΟ",                    "Δημοτικό Βρεφοκομείο"),
    ("ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ",            "Δημοτικό Περιφερειακό Θέατρο"),
    ("ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ",                     "Δημοτική Επιχείρηση"),
    ("ΠΕΡΙΦΕΡΕΙΑ",                              "Περιφέρεια"),
    ("ΥΠΟΥΡΓΕΙΟ",                               "Υπουργείο"),
    ("ΔΗΜΟΣ",                                   "Δήμος"),
]

# Exclude known non-target organizations from the fire-protection dataset.
# Matching is done against normalized `org_name_clean` (uppercase, no accents, collapsed spaces).
EXCLUDED_ORG_NAME_CLEAN_RAW = [
    "ΕΘΝΙΚΟ ΘΕΑΤΡΟ",
    "ΕΛΛΗΝΙΚΗ ΡΑΔΙΟΦΩΝΙΑ ΤΗΛΕΟΡΑΣΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ (Ε.Ρ.Τ. Α.Ε.)",
    "ΗΛΕΚΤΡΟΝΙΚΟΣ ΕΘΝΙΚΟΣ ΦΟΡΕΑΣ ΚΟΙΝΩΝΙΚΗΣ ΑΣΦΑΛΙΣΗΣ",
    "ΕΛΛΗΝΙΚΟΣ ΓΕΩΡΓΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ-ΔΗΜΗΤΡΑ",
    "ΔΗΜΟΚΡΙΤΕΙΟ ΠΑΝΕΠΙΣΤΗΜΙΟ ΘΡΑΚΗΣ",
    "ΕΛΛΗΝΙΚΟ ΜΕΣΟΓΕΙΑΚΟ ΠΑΝΕΠΙΣΤΗΜΙΟ",
    "ΓΕΝΙΚΟ ΝΟΣΟΚΟΜΕΙΟ ΘΕΣΣΑΛΟΝΙΚΗΣ «Γ. ΠΑΠΑΝΙΚΟΛΑΟΥ»",
    "ΕΛΛΗΝΙΚΑ ΑΜΥΝΤΙΚΑ ΣΥΣΤΗΜΑΤΑ ΑΒΕΕ",
    "ΚΕΝΤΡΟ ΘΕΡΑΠΕΙΑΣ ΕΞΑΡΤΗΜΕΝΩΝ ΑΤΟΜΩΝ (ΚΕ.Θ.Ε.Α.)",
    "ΕΠΙΜΕΛΗΤΗΡΙΟ ΛΑΚΩΝΙΑΣ",
    "ΕΘΝΙΚΗ ΛΥΡΙΚΗ ΣΚΗΝΗ",
    "ΠΕΡ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΠΑΙΔΩΝ 'ΑΓΛΑΙΑ ΚΥΡΙΑΚΟΥ'",
    "ΝΟΜ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ - Κ.Υ ΙΕΡΑΠΕΤΡΑΣ",
    "ΓΕΝΙΚΟ ΝΟΣΟΚΟΜΕΙΟ ΒΕΝΙΖΕΛΕΙΟ ΠΑΝΑΝΕΙΟ",
    "ΙΔΡΥΜΑ ΤΕΧΝΟΛΟΓΙΑΣ ΚΑΙ ΕΡΕΥΝΑΣ (ΙΤΕ)",
    "ΣΥΝΔΕΣΜΟΣ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ & ΑΛΛΗΛΕΓΓΥΗΣ ΚΕΡΚΥΡΑΣ",
    "ΠΕΡ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΘΕΣΣΑΛΟΝΙΚΗΣ 'ΙΠΠΟΚΡΑΤΕΙO'",
    "ΕΘΝΙΚΗ ΕΠΙΤΡΟΠΗ ΤΗΛΕΠΙΚΟΙΝΩΝΙΩΝ & ΤΑΧΥΔΡΟΜΕΙΩΝ (ΕΕΤΤ)",
    "ΠΑΝΕΠΙΣΤΗΜΙΑΚΟ ΓΕΝ. ΝΟΣΟΚ. 'ΑΤΤΙΚΟΝ'",
    "ΠΕΡΙΦ. ΠΑΝΕΠΙΣΤ. ΓΕΝ. ΝΟΣΟΚ. ΠΑΤΡΩΝ",
    "ΑΡΙΣΤΟΤΕΛΕΙΟ ΠΑΝΕΠΙΣΤΗΜΙΟ ΘΕΣ/ΝΙΚΗΣ",
    "ΓΕΝΙΚΟ ΝΟΣΟΚΟΜΕΙΟ ΑΤΤΙΚΗΣ (ΚΑΤ)",
    "ΠΕΡΙΦ. ΠΑΝΕΠΙΣΤ. ΓΕΝ. ΝΟΣΟΚ. ΗΡΑΚΛΕΙΟΥ",
    "ΠΕΡ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΠΕΙΡΑΙΩΣ 'ΤΖΑΝΕΙΟ'",
    "ΣΧΟΛΙΚΕΣ ΕΠΙΤΡΟΠΕΣ ΠΡΩΤΟΒΑΘΜΙΑΣ ΚΑΙ ΔΕΥΤΕΡΟΒΑΘΜΙΑΣ ΕΚΠΑΙΔΕΥΣΗΣ ΔΗΜΟΥ ΧΑΝΙΩΝ",
    "ΚΡΑΤΙΚΟ ΘΕΑΤΡΟ ΒΟΡΕΙΟΥ ΕΛΛΑΔΟΣ",
    "ΠΑΝΕΠΙΣΤΗΜΙΟ ΘΕΣΣΑΛΙΑΣ",
    "ΓΕΝΙΚΟ ΝΟΣΟΚΟΜΕΙΟ ΠΑΙΔΩΝ ΠΕΝΤΕΛΗΣ",
    "ΑΡΕΙΟΣ ΠΑΓΟΣ (ΑΠ)",
    "ΠΑΝΕΠΙΣΤΗΜΙΑΚΟ ΓΕΝΙΚΟ ΝΟΣΟΚΟΜΕΙΟ ΕΒΡΟΥ",
    "ΑΝΩΤΑΤΟ ΣΥΜΒΟΥΛΙΟ ΕΠΙΛΟΓΗΣ ΠΡΟΣΩΠΙΚΟΥ (ΑΣΕΠ)",
    "ΝΟΜ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΒΟΛΟΥ 'ΑΧΙΛΛΟΠΟΥΛΕIO'",
    "ΓΕΝΙΚΟ ΝΟΣΟΚΟΜΕΙΟ ΗΛΕΙΑΣ",
    "ΝΟΜ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΚΑΣΤΟΡΙΑΣ",
    "ΝΟΜ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΑΘΗΝΩΝ 'Η ΕΛΠΙΣ'",
    "ΝΟΜ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΠΑΙΔΩΝ ΠΑΤΡΩΝ 'ΚΑΡΑΜΑΝΔΑΝΕΙΟ'",
    "ΟΡΓΑΝΙΣΜΟΣ ΠΟΛΙΤΙΣΜΟΥ ΑΘΛΗΤΙΣΜΟΥ ΚΑΙ ΝΕΟΛΑΙΑΣ ΔΗΜΟΥ ΑΘΗΝΑΙΩΝ",
    "ΑΝΑΠΤΥΞΗ ΑΘΛΗΤΙΣΜΟΥ ΗΡΑΚΛΕΙΟΥ - ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΟΤΑ",
    "ΝΟΜ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΑΜΦΙΣΣΑΣ",
    "ΝΟΜ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ - Κ.Υ ΝΕΑΠΟΛΗΣ ΚΡΗΤΗΣ",
    "ΠΕΡ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΠΑΤΡΩΝ 'ΑΓΙΟΣ ΑΝΔΡΕΑΣ'",
    "ΝΟΜ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙΟ ΚΑΒΑΛΑΣ",
]


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    """Load persisted state; fall back to scanning the CSV if no state file."""
    if LEGACY_STATE_FILE.exists() and not STATE_FILE.exists():
        with open(LEGACY_STATE_FILE) as f:
            legacy_state = json.load(f)
        print("[state] Found legacy root-level state.json; using it for this run.")
        return legacy_state

    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)

    # First run: derive last_fetch from the existing CSV
    csv_source = CSV_PATH if CSV_PATH.exists() else LEGACY_CSV_PATH
    if csv_source.exists():
        df = pd.read_csv(csv_source, usecols=["submissionTimestamp"])
        df["submissionTimestamp"] = pd.to_datetime(
            df["submissionTimestamp"], dayfirst=True, errors="coerce"
        )
        last_ts = df["submissionTimestamp"].max()
        if pd.notna(last_ts):
            ts_str = last_ts.strftime("%d/%m/%Y %H:%M:%S")
            print(f"[state] No state file found. Derived last fetch from CSV: {ts_str}")
            return {"last_fetch": ts_str}

    # No CSV either — fetch everything
    print("[state] No state file and no CSV found. Will fetch all available data.")
    return {"last_fetch": None}


def save_state(state: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    print(f"[state] Saved: {state}")


def append_run_log(
    run_started_at: str,
    fetched_records: int,
    rows_added: int,
    csv_updated: bool,
    success: bool,
    error: str = "",
) -> None:
    """Append one execution record to logs/fetch_runs.csv."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    error_value = error.strip() if isinstance(error, str) else ""
    has_error = bool(error_value)

    # One-time migration for older logs that used a UTC column name.
    if RUN_LOG_FILE.exists():
        existing_log = pd.read_csv(RUN_LOG_FILE)
        if "run_started_at_utc" in existing_log.columns and "run_started_at_athens" not in existing_log.columns:
            existing_log = existing_log.rename(columns={"run_started_at_utc": "run_started_at_athens"})
        if "error_message" not in existing_log.columns:
            existing_log["error_message"] = "NONE"
        existing_log["error_message"] = existing_log["error_message"].fillna("").astype(str).str.strip()
        existing_log.loc[existing_log["error_message"] == "", "error_message"] = "NONE"
        existing_log["error"] = existing_log["error_message"] != "NONE"
        existing_log.to_csv(RUN_LOG_FILE, index=False)

    row = pd.DataFrame(
        [
            {
                "run_started_at_athens": run_started_at,
                "fetched_records": fetched_records,
                "rows_added": rows_added,
                "csv_updated": csv_updated,
                "success": success,
                "error": has_error,
                "error_message": error_value if has_error else "NONE",
            }
        ]
    )
    row.to_csv(RUN_LOG_FILE, mode="a", header=not RUN_LOG_FILE.exists(), index=False)
    print(f"[log] Appended run record → {RUN_LOG_FILE}")


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_new_decisions(since: datetime | None) -> list[dict]:
    """
    Fetch decisions from Diavgeia newer than `since`.
    Pages sorted by most-recent first; stops as soon as all records on a page
    are older than the cutoff.
    """
    q_keywords = ", ".join(f'"{kw}"' for kw in KEYWORDS)
    now = datetime.now()

    def build_params(page: int) -> dict:
        p = {
            "q": f"q:[{q_keywords}]",
            "sort": "recent",
            "size": PAGE_SIZE,
            "page": page,
        }
        if since is not None:
            dt_from = since.strftime("%Y-%m-%dT%H:%M:%S")
            dt_to = now.strftime("%Y-%m-%dT23:59:59")
            p["fq"] = f"submissionTimestamp:[DT({dt_from}) TO DT({dt_to})]"
        return p

    results: list[dict] = []
    page = 0

    # First request — learn total
    r = requests.get(SEARCH_URL, params=build_params(0), headers={"Accept": "application/json"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    total = data["info"]["total"]
    pages = -(-total // PAGE_SIZE)  # ceiling division
    print(f"[fetch] API reports {total} results across {pages} pages (since {since})")

    while True:
        if page > 0:
            r = requests.get(SEARCH_URL, params=build_params(page), headers={"Accept": "application/json"}, timeout=30)
            r.raise_for_status()
            data = r.json()

        batch = data.get("decisions", [])
        if not batch:
            break

        excluded_in_batch = 0
        filtered_batch = []
        for rec in batch:
            if record_has_excluded_org(rec):
                excluded_in_batch += 1
                continue
            filtered_batch.append(rec)
        batch = filtered_batch

        # Client-side cutoff check (guards against APIs that ignore from_date)
        if since is not None:
            new_in_batch = []
            stop = False
            for rec in batch:
                ts = rec.get("submissionTimestamp", "")
                try:
                    rec_dt = datetime.strptime(ts, "%d/%m/%Y %H:%M:%S")
                except ValueError:
                    rec_dt = None

                if rec_dt is None or rec_dt > since:
                    new_in_batch.append(rec)
                else:
                    stop = True  # hit old data; no need to go further

            results.extend(new_in_batch)
            excl_note = f", excluded orgs: {excluded_in_batch}" if excluded_in_batch else ""
            print(
                f"[fetch] page {page + 1}/{pages} — {len(new_in_batch)} new records "
                f"(total so far: {len(results)}){excl_note}"
            )
            if stop:
                print("[fetch] Reached records older than cutoff. Stopping.")
                break
        else:
            results.extend(batch)
            excl_note = f", excluded orgs: {excluded_in_batch}" if excluded_in_batch else ""
            print(
                f"[fetch] page {page + 1}/{pages} — {len(batch)} records "
                f"(total so far: {len(results)}){excl_note}"
            )

        page += 1
        if page >= pages:
            break

    return results


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_org(org: str) -> tuple[str, str]:
    """
    Return (org_type, org_name_clean) for a Greek government org label.

    Examples
    --------
    "ΔΗΜΟΣ ΑΡΤΑΙΩΝ"          -> ("Δήμος", "ΑΡΤΑΙΩΝ")
    "ΥΠΟΥΡΓΕΙΟ ΠΑΙΔΕΙΑΣ"     -> ("Υπουργείο", "ΠΑΙΔΕΙΑΣ")
    "ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ ΑΤΤΙΚΗΣ" -> ("Αποκεντρωμένη Διοίκηση", "ΑΤΤΙΚΗΣ")
    "ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ"         -> ("Άλλος Φορέας", "ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ")
    """
    if not isinstance(org, str):
        return (pd.NA, pd.NA)

    org_stripped = org.strip()
    org_upper = org_stripped.upper()

    def matches_prefix_token(text: str, prefix: str) -> bool:
        """Match prefix only when it is a whole token, not partial word."""
        if not text.startswith(prefix):
            return False
        if len(text) == len(prefix):
            return True
        return text[len(prefix)] in {" ", "-", ",", "."}

    for prefix, label in ORG_PREFIXES:
        if matches_prefix_token(org_upper, prefix):
            name_clean = org_stripped[len(prefix):].strip(" -,")
            return (label, name_clean)

    # Handle specific typo pattern like "ΔΗΜΟ ΑΡΓΟΥΣ" without matching "ΔΗΜΟΤΙΚΟ ..."
    if re.match(r"^ΔΗΜΟ(?:\s|[-,])", org_upper):
        name_clean = org_stripped[4:].strip(" -,")
        return ("Δήμος", name_clean)

    # Catch development agencies/organizations that do not match explicit prefixes.
    if "ΑΝΑΠΤΥΞΙΑΚ" in org_upper:
        return ("Αναπτυξιακός Οργανισμός", org_stripped)

    # No known prefix matched
    return ("Άλλος Φορέας", org_stripped)


def extract_org_label(value) -> str:
    """Extract organization label from API dicts or CSV stringified values."""
    if isinstance(value, dict):
        label = value.get("label")
        return label if isinstance(label, str) else pd.NA

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return pd.NA

        # CSV often stores dicts as Python literal strings with single quotes.
        if text.startswith("{") and text.endswith("}"):
            for parser in (ast.literal_eval, json.loads):
                try:
                    parsed = parser(text)
                    if isinstance(parsed, dict):
                        label = parsed.get("label")
                        if isinstance(label, str):
                            return label
                except (ValueError, SyntaxError, json.JSONDecodeError, TypeError):
                    continue

            # Last-resort regex extraction for malformed dict-like strings.
            match = re.search(r"['\"]label['\"]\s*:\s*['\"]([^'\"]+)['\"]", text)
            if match:
                return match.group(1)

        return text

    return pd.NA


def parse_structured_value(value):
    """Parse dict/list values that may arrive as Python/JSON strings."""
    if isinstance(value, (dict, list)):
        return value

    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return value

    if (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]")):
        for parser in (ast.literal_eval, json.loads):
            try:
                return parser(text)
            except (ValueError, SyntaxError, json.JSONDecodeError, TypeError):
                continue
    return value


def extract_label(value):
    """Extract a 'label' string from a dict/list/stringified payload."""
    parsed = parse_structured_value(value)
    if isinstance(parsed, dict):
        label = parsed.get("label")
        return label if isinstance(label, str) else pd.NA
    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict) and isinstance(item.get("label"), str):
                return item["label"]
            if isinstance(item, str) and item.strip():
                return item.strip()
        return pd.NA
    if isinstance(parsed, str):
        return parsed.strip() or pd.NA
    return pd.NA


def extract_labels_list(value):
    """Extract list of labels from thematicCategories-like payload."""
    parsed = parse_structured_value(value)
    labels: list[str] = []

    if isinstance(parsed, dict):
        label = parsed.get("label")
        if isinstance(label, str) and label.strip():
            labels.append(label.strip())
    elif isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict):
                label = item.get("label")
                if isinstance(label, str) and label.strip():
                    labels.append(label.strip())
            elif isinstance(item, str) and item.strip():
                labels.append(item.strip())
    elif isinstance(parsed, str) and parsed.strip():
        labels.append(parsed.strip())

    # Keep order, remove duplicates
    deduped = list(dict.fromkeys(labels))
    return deduped if deduped else pd.NA


def normalize_upper_no_accents(value):
    """Uppercase and remove diacritics from text values."""
    if not isinstance(value, str):
        return value

    normalized = unicodedata.normalize("NFD", value.upper())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_org_name_key(value) -> str | None:
    """Canonical key for org_name_clean comparisons (case/accents/spacing-insensitive)."""
    text = normalize_upper_no_accents(value)
    if not isinstance(text, str):
        return None
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


EXCLUDED_ORG_NAME_CLEAN_KEYS = {
    key
    for key in (normalize_org_name_key(v) for v in EXCLUDED_ORG_NAME_CLEAN_RAW)
    if key
}


def is_excluded_org_name_clean(value) -> bool:
    """True if normalized org_name_clean belongs to the configured exclusion list."""
    key = normalize_org_name_key(value)
    return bool(key and key in EXCLUDED_ORG_NAME_CLEAN_KEYS)


def filter_excluded_org_rows(df: pd.DataFrame, *, context: str = "rows") -> pd.DataFrame:
    """Drop rows whose org_name_clean is in the exclusion list (safety-net for persisted CSVs)."""
    if "org_name_clean" not in df.columns or df.empty:
        return df

    mask = df["org_name_clean"].apply(is_excluded_org_name_clean)
    dropped = int(mask.sum())
    if dropped:
        print(f"[org-filter] Dropped {dropped} excluded {context}.", flush=True)
    return df.loc[~mask].copy()


def normalize_org_name_by_type(org_type, org_name):
    """Apply type-specific normalization rules to org_name_clean."""
    if not isinstance(org_type, str) or not isinstance(org_name, str):
        return org_name

    name = re.sub(r"\s+", " ", org_name).strip(" ,")
    name = re.sub(r"\s*,\s*", ", ", name)

    if org_type == "ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ":
        # Remove connector boilerplate left after prefix split.
        name = re.sub(r"^ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ\s+ΓΙΑ ΤΗΝ\s+", "", name)
        name = re.sub(r"^ΓΙΑ ΤΗΝ\s+", "", name)
        name = name.strip(" ,")
        return name

    if org_type == "ΥΠΟΥΡΓΕΙΟ":
        ministry_aliases = {
            "ΠΕΡΙΒΑΛΛΟΝΤΟΣ, ΕΝΕΡΓΕΙΑΣ ΚΑΙ ΚΛΙΜΑΤΙΚΗΣ ΑΛΛΑΓΗΣ": "ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ",
            "ΥΠΟΔΟΜΩΝ, ΜΕΤΑΦΟΡΩΝ ΚΑΙ ΔΙΚΤΥΩΝ": "ΥΠΟΔΟΜΩΝ ΚΑΙ ΜΕΤΑΦΟΡΩΝ",
            "ΕΣΩΤΕΡΙΚΩΝ ΚΑΙ ΔΙΟΙΚΗΤΙΚΗΣ ΑΝΑΣΥΓΚΡΟΤΗΣΗΣ": "ΕΣΩΤΕΡΙΚΩΝ",
            "ΠΑΙΔΕΙΑΣ, ΕΡΕΥΝΑΣ ΚΑΙ ΘΡΗΣΚΕΥΜΑΤΩΝ": "ΠΑΙΔΕΙΑΣ ΚΑΙ ΘΡΗΣΚΕΥΜΑΤΩΝ",
        }
        return ministry_aliases.get(name, name)

    return name


def normalize_org_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize org classification columns to uppercase without accents."""
    df = df.copy()
    for col in ("org_type", "org_name_clean"):
        if col in df.columns:
            df[col] = df[col].apply(normalize_upper_no_accents)
    if {"org_type", "org_name_clean"}.issubset(df.columns):
        df["org_name_clean"] = df.apply(
            lambda r: normalize_org_name_by_type(r["org_type"], r["org_name_clean"]), axis=1
        )
    return df


def recompute_org_classification(df: pd.DataFrame) -> pd.DataFrame:
    """Rebuild org_type/org_name_clean from org labels to fix legacy bad rows."""
    df = df.copy()
    if "org" not in df.columns:
        return normalize_org_columns(df)

    classifications = df["org"].apply(classify_org)
    df["org_type"] = classifications.apply(lambda t: t[0])
    df["org_name_clean"] = classifications.apply(lambda t: t[1])
    return normalize_org_columns(df)


def record_has_excluded_org(rec: dict) -> bool:
    """Check API search record against excluded org_name_clean list."""
    org_label = extract_org_label(rec.get("organization"))
    if not isinstance(org_label, str):
        return False
    org_type, org_name_clean = classify_org(org_label)
    if isinstance(org_type, str):
        org_type = normalize_upper_no_accents(org_type)
    if isinstance(org_name_clean, str):
        org_name_clean = normalize_upper_no_accents(org_name_clean)
        org_name_clean = normalize_org_name_by_type(org_type, org_name_clean)
    return is_excluded_org_name_clean(org_name_clean)


def normalize_decision_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize decisionType and thematicCategories label columns."""
    df = df.copy()
    if "decisionType" in df.columns:
        df["decisionType"] = df["decisionType"].apply(extract_label)
    if "thematicCategories" in df.columns:
        df["thematicCategories"] = df["thematicCategories"].apply(extract_labels_list)
    return df


def add_subject_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived boolean flags based on normalized subject text.

    subject_has_anatrop_or_anaklis = True if subject contains:
    - ανατροπ*
    - ανακλησ*
    (accent-insensitive)

    subject_has_budget_balance_report_terms = True if subject contains:
    - προϋπολογισμ*
    - ισολογισμ*
    - απολογισμ*
    (accent-insensitive)
    """
    df = df.copy()
    if "subject" not in df.columns:
        return df

    def _flag_subject(value) -> bool:
        text = normalize_upper_no_accents(value)
        if not isinstance(text, str):
            return False
        return ("ΑΝΑΤΡΟΠ" in text) or ("ΑΝΑΚΛΗΣ" in text)

    def _flag_budget_terms(value) -> bool:
        text = normalize_upper_no_accents(value)
        if not isinstance(text, str):
            return False
        return (
            ("ΠΡΟΥΠΟΛΟΓΙΣΜ" in text)
            or ("ΙΣΟΛΟΓΙΣΜ" in text)
            or ("ΑΠΟΛΟΓΙΣΜ" in text)
        )

    df["subject_has_anatrop_or_anaklis"] = df["subject"].apply(_flag_subject)
    df["subject_has_budget_balance_report_terms"] = df["subject"].apply(_flag_budget_terms)
    return df


def build_decision_view_url(ada: str) -> str:
    """Build the decision view API URL for a given ADA."""
    return f"{DECISION_VIEW_URL}/{quote(str(ada).strip(), safe='')}"


def fetch_decision_view_by_ada(ada: str, session: requests.Session | None = None, timeout: int = 30) -> dict:
    """Fetch full decision payload from Diavgeia decisions/view endpoint."""
    url = build_decision_view_url(ada)
    client = session or requests
    response = client.get(url, headers={"Accept": "application/json"}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def meta_list_to_dict(meta_value) -> dict:
    """Flatten Diavgeia `meta` list (list of one-key dicts) into a plain dict."""
    parsed = parse_structured_value(meta_value)
    if not isinstance(parsed, list):
        return {}

    meta_map: dict = {}
    for item in parsed:
        if isinstance(item, dict):
            meta_map.update(item)
    return meta_map


def collapse_singleton_list(values):
    """
    Normalize list-like extracted values for CSV storage.

    - `[]` -> `pd.NA`
    - `[x]` -> `x`
    - `[x, y, ...]` -> unchanged list
    """
    if isinstance(values, list):
        if len(values) == 0:
            return pd.NA
        if len(values) == 1:
            return values[0]
    return values


def normalize_enrichment_singleton_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse singleton lists across all enrichment business columns.

    Handles both in-memory Python lists and CSV stringified lists via parse_structured_value().
    """
    df = df.copy()
    for col in ENRICHMENT_SINGLETON_COLLAPSE_COLUMNS:
        if col not in df.columns:
            continue
        df[col] = df[col].apply(
            lambda v: collapse_singleton_list(parse_structured_value(v))
        )
    return df


def coerce_columns_to_object(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Force selected columns to object dtype so row-level assignment can store
    strings/lists/dicts safely even when pandas loaded pyarrow-backed dtypes.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("object")
    return df


def extract_spending_approval_fields_from_decision(decision_payload: dict) -> dict:
    """
    Extract signers + all contractor fields from a full decisions/view payload.

    Returned values are lists for contractor-related columns (one element per contractor).
    """
    meta_map = meta_list_to_dict(decision_payload.get("meta"))

    signers = meta_map.get("Υπογράφοντες", [])
    if not isinstance(signers, list):
        signers = [signers] if signers else []

    contractors = meta_map.get("Στοιχεία αναδόχων", []) or []
    if not isinstance(contractors, list):
        contractors = []

    contractor_rows: list[dict] = []
    afms: list[str | None] = []
    names: list[str | None] = []
    values: list[str | None] = []
    currencies: list[str | None] = []

    for contractor in contractors:
        contractor = contractor or {}
        if not isinstance(contractor, dict):
            continue

        afm_info = contractor.get("ΑΦΜ / Επωνυμία", {}) or {}
        amount_info = contractor.get("Ποσό δαπάνης", {}) or {}
        if not isinstance(afm_info, dict):
            afm_info = {}
        if not isinstance(amount_info, dict):
            amount_info = {}

        afm = afm_info.get("ΑΦΜ")
        name = afm_info.get("Επωνυμία")
        value = amount_info.get("Αξία")
        currency = amount_info.get("Νόμισμα")

        afms.append(afm)
        names.append(name)
        values.append(value)
        currencies.append(currency)
        contractor_rows.append(
            {
                "ΑΦΜ": afm,
                "Επωνυμία": name,
                "Αξία": value,
                "Νόμισμα": currency,
            }
        )

    return {
        "spending_signers": collapse_singleton_list(signers),
        "spending_contractors_afm": collapse_singleton_list(afms),
        "spending_contractors_name": collapse_singleton_list(names),
        "spending_contractors_value": collapse_singleton_list(values),
        "spending_contractors_currency": collapse_singleton_list(currencies),
        "spending_contractors_count": len(contractor_rows),
        "spending_contractors_details": contractor_rows,
        "spending_enrichment_status": "ok",
        "spending_enrichment_error": "",
    }


def ensure_spending_enrichment_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure CSV dataframe has the spending-approval enrichment columns."""
    df = df.copy()
    defaults = {
        "spending_signers": pd.NA,
        "spending_contractors_afm": pd.NA,
        "spending_contractors_name": pd.NA,
        "spending_contractors_value": pd.NA,
        "spending_contractors_currency": pd.NA,
        "spending_contractors_count": pd.NA,
        "spending_contractors_details": pd.NA,
        "spending_enrichment_status": pd.NA,
        "spending_enrichment_error": pd.NA,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def extract_commitment_fields_from_decision(decision_payload: dict) -> dict:
    """
    Extract fields from `ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ` decisions/view payload.

    `Ποσό και ΚΑΕ/ΑΛΕ` may contain multiple rows; all row-level values are stored as lists.
    """
    meta_map = meta_list_to_dict(decision_payload.get("meta"))

    signers = meta_map.get("Υπογράφοντες", [])
    if not isinstance(signers, list):
        signers = [signers] if signers else []

    fiscal_year = meta_map.get("Οικονομικό Έτος")
    budget_category = meta_map.get("Κατηγορία Προϋπολογισμού")

    lines = meta_map.get("Ποσό και ΚΑΕ/ΑΛΕ", []) or []
    if not isinstance(lines, list):
        lines = []

    total_amount = meta_map.get("Συνολικό ποσό", {}) or {}
    if not isinstance(total_amount, dict):
        total_amount = {}

    counterparties: list[Any] = []
    amounts_with_vat: list[Any] = []
    remaining_available_credit: list[Any] = []
    kae_ale_numbers: list[Any] = []
    remaining_kae_ale: list[Any] = []
    line_details: list[dict[str, Any]] = []

    for line in lines:
        if not isinstance(line, dict):
            continue

        counterparty = line.get("ΑΦΜ / Επωνυμία")
        kae_ale_number = line.get("Αριθμός ΚΑΕ/ΑΛΕ")
        amount_with_vat = line.get("Ποσό με ΦΠΑ")
        remaining_credit = line.get("Υπόλοιπο διαθέσιμης πίστωσης")
        remaining_kae = line.get("Υπόλοιπο ΚΑΕ/ΑΛΕ")

        counterparties.append(counterparty)
        kae_ale_numbers.append(kae_ale_number)
        amounts_with_vat.append(amount_with_vat)
        remaining_available_credit.append(remaining_credit)
        remaining_kae_ale.append(remaining_kae)
        line_details.append(
            {
                "ΑΦΜ / Επωνυμία": counterparty,
                "Ποσό με ΦΠΑ": amount_with_vat,
                "Υπόλοιπο διαθέσιμης πίστωσης": remaining_credit,
                "Αριθμός ΚΑΕ/ΑΛΕ": kae_ale_number,
                "Υπόλοιπο ΚΑΕ/ΑΛΕ": remaining_kae,
            }
        )

    # Some commitments have no line items but do include "Συνολικό ποσό".
    # Preserve that amount in commitment_amount_with_vat so downstream amount
    # derivation can still work.
    if len(amounts_with_vat) == 0:
        total_amount_value = total_amount.get("Ποσό")
        if total_amount_value not in (None, "", []):
            amounts_with_vat = [total_amount_value]

    return {
        "commitment_signers": collapse_singleton_list(signers),
        "commitment_fiscal_year": fiscal_year,
        "commitment_budget_category": budget_category,
        "commitment_counterparty": collapse_singleton_list(counterparties),
        "commitment_amount_with_vat": collapse_singleton_list(amounts_with_vat),
        "commitment_remaining_available_credit": collapse_singleton_list(remaining_available_credit),
        "commitment_kae_ale_number": collapse_singleton_list(kae_ale_numbers),
        "commitment_remaining_kae_ale": collapse_singleton_list(remaining_kae_ale),
        "commitment_lines_count": len(line_details),
        "commitment_lines_details": line_details,
        "commitment_enrichment_status": "ok",
        "commitment_enrichment_error": "",
    }


def ensure_commitment_enrichment_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure CSV dataframe has the commitment enrichment columns."""
    df = df.copy()
    defaults = {
        "commitment_signers": pd.NA,
        "commitment_fiscal_year": pd.NA,
        "commitment_budget_category": pd.NA,
        "commitment_counterparty": pd.NA,
        "commitment_amount_with_vat": pd.NA,
        "commitment_remaining_available_credit": pd.NA,
        "commitment_kae_ale_number": pd.NA,
        "commitment_remaining_kae_ale": pd.NA,
        "commitment_lines_count": pd.NA,
        "commitment_lines_details": pd.NA,
        "commitment_enrichment_status": pd.NA,
        "commitment_enrichment_error": pd.NA,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def extract_direct_assignment_fields_from_decision(decision_payload: dict) -> dict:
    """
    Extract fields from direct assignment decisions/view payload.

    Keeps all persons (AFM/Name) as lists; helper `direct_people_details` preserves structured rows.
    """
    meta_map = meta_list_to_dict(decision_payload.get("meta"))

    signers = meta_map.get("Υπογράφοντες", [])
    if not isinstance(signers, list):
        signers = [signers] if signers else []

    people = meta_map.get("ΑΦΜ / Επωνυμία προσώπου / προσώπων", []) or []
    if not isinstance(people, list):
        people = []

    afms: list[Any] = []
    names: list[Any] = []
    people_details: list[dict[str, Any]] = []
    for person in people:
        if not isinstance(person, dict):
            continue
        afm = person.get("ΑΦΜ")
        name = person.get("Επωνυμία")
        afms.append(afm)
        names.append(name)
        people_details.append({"ΑΦΜ": afm, "Επωνυμία": name})

    amount = meta_map.get("Ποσό", {}) or {}
    if not isinstance(amount, dict):
        amount = {}

    related_commitment = meta_map.get("Σχετ. Ανάληψη υποχρέωσης")
    see_also = meta_map.get("Δείτε επίσης και ..")

    return {
        "direct_signers": collapse_singleton_list(signers),
        "direct_afm": collapse_singleton_list(afms),
        "direct_name": collapse_singleton_list(names),
        "direct_value": amount.get("Αξία"),
        "direct_related_commitment": related_commitment,
        "direct_see_also": see_also,
        "direct_people_count": len(people_details),
        "direct_people_details": people_details,
        "direct_enrichment_status": "ok",
        "direct_enrichment_error": "",
    }


def ensure_direct_enrichment_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure CSV dataframe has the direct-assignment enrichment columns."""
    df = df.copy()
    defaults = {
        "direct_signers": pd.NA,
        "direct_afm": pd.NA,
        "direct_name": pd.NA,
        "direct_value": pd.NA,
        "direct_related_commitment": pd.NA,
        "direct_see_also": pd.NA,
        "direct_people_count": pd.NA,
        "direct_people_details": pd.NA,
        "direct_enrichment_status": pd.NA,
        "direct_enrichment_error": pd.NA,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def extract_payment_finalization_fields_from_decision(decision_payload: dict) -> dict:
    """
    Extract fields from `ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ` decisions/view payload.

    Keeps all beneficiaries as lists and also preserves structured details.
    """
    meta_map = meta_list_to_dict(decision_payload.get("meta"))

    signers = meta_map.get("Υπογράφοντες", [])
    if not isinstance(signers, list):
        signers = [signers] if signers else []

    beneficiaries = meta_map.get("Στοιχεία δικαιούχων", []) or []
    if not isinstance(beneficiaries, list):
        beneficiaries = []

    afms: list[Any] = []
    names: list[Any] = []
    values: list[Any] = []
    beneficiary_details: list[dict[str, Any]] = []
    for beneficiary in beneficiaries:
        if not isinstance(beneficiary, dict):
            continue

        afm_info = beneficiary.get("ΑΦΜ / Επωνυμία", {}) or {}
        amount_info = beneficiary.get("Ποσό δαπάνης", {}) or {}
        if not isinstance(afm_info, dict):
            afm_info = {}
        if not isinstance(amount_info, dict):
            amount_info = {}

        afm = afm_info.get("ΑΦΜ")
        name = afm_info.get("Επωνυμία")
        value = amount_info.get("Αξία")

        afms.append(afm)
        names.append(name)
        values.append(value)
        beneficiary_details.append(
            {
                "ΑΦΜ": afm,
                "Επωνυμία": name,
                "Αξία": value,
            }
        )

    return {
        "payment_signers": collapse_singleton_list(signers),
        "payment_beneficiary_afm": collapse_singleton_list(afms),
        "payment_beneficiary_name": collapse_singleton_list(names),
        "payment_value": collapse_singleton_list(values),
        "payment_related_commitment_or_spending": meta_map.get("Σχετ. Ανάληψη Υποχρέωσης/Έγκριση Δαπάνης"),
        "payment_see_also": meta_map.get("Δείτε επίσης και .."),
        "payment_beneficiaries_count": len(beneficiary_details),
        "payment_beneficiaries_details": beneficiary_details,
        "payment_enrichment_status": "ok",
        "payment_enrichment_error": "",
    }


def ensure_payment_enrichment_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure CSV dataframe has the payment-finalization enrichment columns."""
    df = df.copy()
    defaults = {
        "payment_signers": pd.NA,
        "payment_beneficiary_afm": pd.NA,
        "payment_beneficiary_name": pd.NA,
        "payment_value": pd.NA,
        "payment_related_commitment_or_spending": pd.NA,
        "payment_see_also": pd.NA,
        "payment_beneficiaries_count": pd.NA,
        "payment_beneficiaries_details": pd.NA,
        "payment_enrichment_status": pd.NA,
        "payment_enrichment_error": pd.NA,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def enrich_spending_approvals(
    df: pd.DataFrame,
    session: requests.Session | None = None,
    timeout: int = 30,
    overwrite_existing: bool = False,
    progress_every: int = 10,
) -> pd.DataFrame:
    """
    For rows with decisionType == ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ, fetch decisions/view and add contractor/signer info.

    Safe for both newly fetched API rows and existing CSV rows.
    """
    df = ensure_spending_enrichment_columns(df)
    df = coerce_columns_to_object(df, SPENDING_ENRICHMENT_COLUMNS)
    if "decisionType" not in df.columns or "ada" not in df.columns:
        return df

    # Ensure label normalization first so matching is reliable.
    df = normalize_decision_columns(df)

    target_mask = df["decisionType"] == SPENDING_APPROVAL_LABEL
    if not target_mask.any():
        return df

    if not overwrite_existing and "spending_enrichment_status" in df.columns:
        existing_status = df["spending_enrichment_status"].astype("string").fillna("")
        target_mask = target_mask & (existing_status.str.strip() == "")
        if not target_mask.any():
            return df

    target_indices = list(df.index[target_mask])
    total_targets = len(target_indices)
    print(
        f"[spending] start targets={total_targets} overwrite_existing={overwrite_existing} timeout={timeout}s",
        flush=True,
    )

    client = session or requests.Session()
    created_session = session is None

    ok_count = 0
    error_count = 0
    skipped_count = 0
    progress_every = max(1, int(progress_every))

    try:
        for processed, idx in enumerate(target_indices, start=1):
            ada = df.at[idx, "ada"]
            if not isinstance(ada, str) or not ada.strip():
                df.at[idx, "spending_enrichment_status"] = "skip_missing_ada"
                df.at[idx, "spending_enrichment_error"] = "missing ada"
                skipped_count += 1
                continue

            try:
                payload = fetch_decision_view_by_ada(ada=ada, session=client, timeout=timeout)
                extracted = extract_spending_approval_fields_from_decision(payload)
                for col, value in extracted.items():
                    df.at[idx, col] = value
                ok_count += 1
            except Exception as exc:
                df.at[idx, "spending_enrichment_status"] = "error"
                df.at[idx, "spending_enrichment_error"] = f"{type(exc).__name__}: {exc}"
                error_count += 1
                print(f"[spending][error] ada={ada} -> {type(exc).__name__}: {exc}")

            if processed % progress_every == 0 or processed == total_targets:
                print(
                    f"[spending][progress] {processed}/{total_targets} "
                    f"ok={ok_count} error={error_count} skipped={skipped_count}",
                    flush=True,
                )
    finally:
        if created_session:
            client.close()

    print(
        f"[spending] done processed={total_targets} ok={ok_count} error={error_count} skipped={skipped_count}",
        flush=True,
    )
    return df


def enrich_commitment_decisions(
    df: pd.DataFrame,
    session: requests.Session | None = None,
    timeout: int = 30,
    overwrite_existing: bool = False,
    progress_every: int = 10,
) -> pd.DataFrame:
    """
    For rows with decisionType == ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ, fetch decisions/view and add commitment fields.
    """
    df = ensure_commitment_enrichment_columns(df)
    df = coerce_columns_to_object(df, COMMITMENT_ENRICHMENT_COLUMNS)
    if "decisionType" not in df.columns or "ada" not in df.columns:
        return df

    df = normalize_decision_columns(df)
    target_mask = df["decisionType"] == COMMITMENT_LABEL
    if not target_mask.any():
        return df

    if not overwrite_existing and "commitment_enrichment_status" in df.columns:
        existing_status = df["commitment_enrichment_status"].astype("string").fillna("")
        target_mask = target_mask & (existing_status.str.strip() == "")
        if not target_mask.any():
            return df

    target_indices = list(df.index[target_mask])
    total_targets = len(target_indices)
    print(
        f"[commitment] start targets={total_targets} overwrite_existing={overwrite_existing} timeout={timeout}s",
        flush=True,
    )

    client = session or requests.Session()
    created_session = session is None
    ok_count = 0
    error_count = 0
    skipped_count = 0
    progress_every = max(1, int(progress_every))

    try:
        for processed, idx in enumerate(target_indices, start=1):
            ada = df.at[idx, "ada"]
            if not isinstance(ada, str) or not ada.strip():
                df.at[idx, "commitment_enrichment_status"] = "skip_missing_ada"
                df.at[idx, "commitment_enrichment_error"] = "missing ada"
                skipped_count += 1
                continue

            try:
                payload = fetch_decision_view_by_ada(ada=ada, session=client, timeout=timeout)
                extracted = extract_commitment_fields_from_decision(payload)
                for col, value in extracted.items():
                    df.at[idx, col] = value
                ok_count += 1
            except Exception as exc:
                df.at[idx, "commitment_enrichment_status"] = "error"
                df.at[idx, "commitment_enrichment_error"] = f"{type(exc).__name__}: {exc}"
                error_count += 1
                print(f"[commitment][error] ada={ada} -> {type(exc).__name__}: {exc}")

            if processed % progress_every == 0 or processed == total_targets:
                print(
                    f"[commitment][progress] {processed}/{total_targets} "
                    f"ok={ok_count} error={error_count} skipped={skipped_count}",
                    flush=True,
                )
    finally:
        if created_session:
            client.close()

    print(
        f"[commitment] done processed={total_targets} ok={ok_count} error={error_count} skipped={skipped_count}",
        flush=True,
    )
    return df


def enrich_direct_assignment_decisions(
    df: pd.DataFrame,
    session: requests.Session | None = None,
    timeout: int = 30,
    overwrite_existing: bool = False,
    progress_every: int = 10,
) -> pd.DataFrame:
    """
    For rows with decisionType == DIRECT_ASSIGNMENT_LABEL, fetch decisions/view and add direct_* fields.
    """
    df = ensure_direct_enrichment_columns(df)
    df = coerce_columns_to_object(df, DIRECT_ENRICHMENT_COLUMNS)
    if "decisionType" not in df.columns or "ada" not in df.columns:
        return df

    df = normalize_decision_columns(df)
    target_mask = df["decisionType"] == DIRECT_ASSIGNMENT_LABEL
    if not target_mask.any():
        return df

    if not overwrite_existing and "direct_enrichment_status" in df.columns:
        existing_status = df["direct_enrichment_status"].astype("string").fillna("")
        target_mask = target_mask & (existing_status.str.strip() == "")
        if not target_mask.any():
            return df

    target_indices = list(df.index[target_mask])
    total_targets = len(target_indices)
    print(
        f"[direct] start targets={total_targets} overwrite_existing={overwrite_existing} timeout={timeout}s",
        flush=True,
    )

    client = session or requests.Session()
    created_session = session is None
    ok_count = 0
    error_count = 0
    skipped_count = 0
    progress_every = max(1, int(progress_every))

    try:
        for processed, idx in enumerate(target_indices, start=1):
            ada = df.at[idx, "ada"]
            if not isinstance(ada, str) or not ada.strip():
                df.at[idx, "direct_enrichment_status"] = "skip_missing_ada"
                df.at[idx, "direct_enrichment_error"] = "missing ada"
                skipped_count += 1
                continue

            try:
                payload = fetch_decision_view_by_ada(ada=ada, session=client, timeout=timeout)
                extracted = extract_direct_assignment_fields_from_decision(payload)
                for col, value in extracted.items():
                    df.at[idx, col] = value
                ok_count += 1
            except Exception as exc:
                df.at[idx, "direct_enrichment_status"] = "error"
                df.at[idx, "direct_enrichment_error"] = f"{type(exc).__name__}: {exc}"
                error_count += 1
                print(f"[direct][error] ada={ada} -> {type(exc).__name__}: {exc}")

            if processed % progress_every == 0 or processed == total_targets:
                print(
                    f"[direct][progress] {processed}/{total_targets} "
                    f"ok={ok_count} error={error_count} skipped={skipped_count}",
                    flush=True,
                )
    finally:
        if created_session:
            client.close()

    print(
        f"[direct] done processed={total_targets} ok={ok_count} error={error_count} skipped={skipped_count}",
        flush=True,
    )
    return df


def enrich_payment_finalization_decisions(
    df: pd.DataFrame,
    session: requests.Session | None = None,
    timeout: int = 30,
    overwrite_existing: bool = False,
    progress_every: int = 10,
) -> pd.DataFrame:
    """
    For rows with decisionType == PAYMENT_FINALIZATION_LABEL, fetch decisions/view and add payment_* fields.
    """
    df = ensure_payment_enrichment_columns(df)
    df = coerce_columns_to_object(df, PAYMENT_ENRICHMENT_COLUMNS)
    if "decisionType" not in df.columns or "ada" not in df.columns:
        return df

    df = normalize_decision_columns(df)
    target_mask = df["decisionType"] == PAYMENT_FINALIZATION_LABEL
    if not target_mask.any():
        return df

    if not overwrite_existing and "payment_enrichment_status" in df.columns:
        existing_status = df["payment_enrichment_status"].astype("string").fillna("")
        target_mask = target_mask & (existing_status.str.strip() == "")
        if not target_mask.any():
            return df

    target_indices = list(df.index[target_mask])
    total_targets = len(target_indices)
    print(
        f"[payment] start targets={total_targets} overwrite_existing={overwrite_existing} timeout={timeout}s",
        flush=True,
    )

    client = session or requests.Session()
    created_session = session is None
    ok_count = 0
    error_count = 0
    skipped_count = 0
    progress_every = max(1, int(progress_every))

    try:
        for processed, idx in enumerate(target_indices, start=1):
            ada = df.at[idx, "ada"]
            if not isinstance(ada, str) or not ada.strip():
                df.at[idx, "payment_enrichment_status"] = "skip_missing_ada"
                df.at[idx, "payment_enrichment_error"] = "missing ada"
                skipped_count += 1
                continue

            try:
                payload = fetch_decision_view_by_ada(ada=ada, session=client, timeout=timeout)
                extracted = extract_payment_finalization_fields_from_decision(payload)
                for col, value in extracted.items():
                    df.at[idx, col] = value
                ok_count += 1
            except Exception as exc:
                df.at[idx, "payment_enrichment_status"] = "error"
                df.at[idx, "payment_enrichment_error"] = f"{type(exc).__name__}: {exc}"
                error_count += 1
                print(f"[payment][error] ada={ada} -> {type(exc).__name__}: {exc}")

            if processed % progress_every == 0 or processed == total_targets:
                print(
                    f"[payment][progress] {processed}/{total_targets} "
                    f"ok={ok_count} error={error_count} skipped={skipped_count}",
                    flush=True,
                )
    finally:
        if created_session:
            client.close()

    print(
        f"[payment] done processed={total_targets} ok={ok_count} error={error_count} skipped={skipped_count}",
        flush=True,
    )
    return df


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add org, org_type, org_name_clean columns to a raw Diavgeia DataFrame."""

    df = df.copy()
    if "organization" not in df.columns:
        raise KeyError("Expected column 'organization' in dataframe")

    df["org"] = df["organization"].apply(extract_org_label)
    df = recompute_org_classification(df)

    df = normalize_decision_columns(df)
    df = enrich_spending_approvals(df)
    df = enrich_commitment_decisions(df)
    df = enrich_direct_assignment_decisions(df)
    df = enrich_payment_finalization_decisions(df)
    df = normalize_enrichment_singleton_columns(df)
    return df


def backfill_spending_approval_columns(
    csv_path: Path | str = CSV_PATH,
    *,
    overwrite_existing: bool = False,
    timeout: int = 30,
    progress_every: int = 10,
    save: bool = True,
) -> dict[str, int]:
    """
    Backfill decision-view enrichment columns for an existing CSV.

    Usable from notebooks:
        from src.fetch_diavgeia import backfill_spending_approval_columns
        backfill_spending_approval_columns()
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    before = ensure_spending_enrichment_columns(df.copy())
    before = ensure_commitment_enrichment_columns(before)
    before = ensure_direct_enrichment_columns(before)
    before = ensure_payment_enrichment_columns(before)
    before_status = before["spending_enrichment_status"].astype("string").fillna("").str.strip()
    before_commitment_status = before["commitment_enrichment_status"].astype("string").fillna("").str.strip()
    before_direct_status = before["direct_enrichment_status"].astype("string").fillna("").str.strip()
    before_payment_status = before["payment_enrichment_status"].astype("string").fillna("").str.strip()

    enriched = enrich_spending_approvals(
        df,
        timeout=timeout,
        overwrite_existing=overwrite_existing,
        progress_every=progress_every,
    )
    enriched = enrich_commitment_decisions(
        enriched,
        timeout=timeout,
        overwrite_existing=overwrite_existing,
        progress_every=progress_every,
    )
    enriched = enrich_direct_assignment_decisions(
        enriched,
        timeout=timeout,
        overwrite_existing=overwrite_existing,
        progress_every=progress_every,
    )
    enriched = enrich_payment_finalization_decisions(
        enriched,
        timeout=timeout,
        overwrite_existing=overwrite_existing,
        progress_every=progress_every,
    )

    after = ensure_spending_enrichment_columns(enriched)
    after = ensure_commitment_enrichment_columns(after)
    after = ensure_direct_enrichment_columns(after)
    after = ensure_payment_enrichment_columns(after)
    after = normalize_enrichment_singleton_columns(after)
    after = add_subject_flags(after)
    after = recompute_org_classification(after)
    after = filter_excluded_org_rows(after, context="rows during backfill")
    after_status = after["spending_enrichment_status"].astype("string").fillna("").str.strip()
    after_commitment_status = after["commitment_enrichment_status"].astype("string").fillna("").str.strip()
    after_direct_status = after["direct_enrichment_status"].astype("string").fillna("").str.strip()
    after_payment_status = after["payment_enrichment_status"].astype("string").fillna("").str.strip()

    if save:
        after.to_csv(csv_path, index=False)
        print(f"[spending] Backfilled CSV saved -> {csv_path}")

    return {
        "rows_total": int(len(after)),
        "rows_spending_approvals": int((after["decisionType"] == SPENDING_APPROVAL_LABEL).sum())
        if "decisionType" in after.columns else 0,
        "rows_commitments": int((after["decisionType"] == COMMITMENT_LABEL).sum())
        if "decisionType" in after.columns else 0,
        "rows_direct_assignments": int((after["decisionType"] == DIRECT_ASSIGNMENT_LABEL).sum())
        if "decisionType" in after.columns else 0,
        "rows_payment_finalizations": int((after["decisionType"] == PAYMENT_FINALIZATION_LABEL).sum())
        if "decisionType" in after.columns else 0,
        "rows_newly_enriched": int(((before_status == "") & (after_status == "ok")).sum()),
        "rows_commitments_newly_enriched": int(
            ((before_commitment_status == "") & (after_commitment_status == "ok")).sum()
        ),
        "rows_direct_newly_enriched": int(
            ((before_direct_status == "") & (after_direct_status == "ok")).sum()
        ),
        "rows_payment_newly_enriched": int(
            ((before_payment_status == "") & (after_payment_status == "ok")).sum()
        ),
        "rows_ok_total": int((after_status == "ok").sum()),
        "rows_commitments_ok_total": int((after_commitment_status == "ok").sum()),
        "rows_direct_ok_total": int((after_direct_status == "ok").sum()),
        "rows_payment_ok_total": int((after_payment_status == "ok").sum()),
        "rows_error_total": int((after_status == "error").sum()),
        "rows_commitments_error_total": int((after_commitment_status == "error").sum()),
        "rows_direct_error_total": int((after_direct_status == "error").sum()),
        "rows_payment_error_total": int((after_payment_status == "error").sum()),
    }


# ---------------------------------------------------------------------------
# Persist
# ---------------------------------------------------------------------------

def append_to_csv(new_records: list[dict]) -> int:
    """Enrich and append new records to the CSV. Returns count of rows added."""
    if not new_records:
        print("[csv] Nothing to append.")
        return 0

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    new_df = enrich_dataframe(pd.DataFrame(new_records))
    new_df = enrich_with_pdf_content(new_df)

    # API returns dicts/lists for some columns; CSV stores them as strings.
    # Stringify any remaining dict/list values so both sides are comparable.
    for col in new_df.columns:
        new_df[col] = new_df[col].apply(
            lambda x: str(x) if isinstance(x, (dict, list)) else x
        )

    if CSV_PATH.exists():
        existing = pd.read_csv(CSV_PATH)
        existing = ensure_pdf_embed_columns(existing)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    # Keep old rows aligned with current normalization rules.
    combined = normalize_decision_columns(combined)
    combined = add_subject_flags(combined)
    combined = recompute_org_classification(combined)
    combined = ensure_spending_enrichment_columns(combined)
    combined = ensure_commitment_enrichment_columns(combined)
    combined = ensure_direct_enrichment_columns(combined)
    combined = ensure_payment_enrichment_columns(combined)
    combined = normalize_enrichment_singleton_columns(combined)
    combined = filter_excluded_org_rows(combined, context="rows before CSV save")

    original_len = len(existing) if CSV_PATH.exists() else 0

    # Drop exact duplicate rows. Some enrichment columns can contain Python lists/dicts
    # after normalization, which are unhashable and break pandas.drop_duplicates().
    # Build a hashable comparison view for duplicate detection, but keep original values.
    dedupe_view = combined.copy()
    for col in dedupe_view.columns:
        dedupe_view[col] = dedupe_view[col].apply(
            lambda x: json.dumps(x, ensure_ascii=False, sort_keys=True, default=str)
            if isinstance(x, (dict, list))
            else x
        )

    duplicate_mask = dedupe_view.duplicated()
    combined = combined.loc[~duplicate_mask].reset_index(drop=True)

    added = len(combined) - original_len
    if added <= 0:
        print("[csv] All fetched records already exist in CSV (no new rows).")
        return 0

    combined.to_csv(CSV_PATH, index=False)
    print(f"[csv] Appended {len(new_df)} new rows → {CSV_PATH}")
    return len(new_df)


def ensure_pdf_embed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure PDF embed columns exist so old CSVs concatenate cleanly."""
    for col in PDF_EMBED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def _download_pdf_to_path(document_url: str, pdf_path: Path, timeout: int = PDF_DOWNLOAD_TIMEOUT) -> None:
    """Download a Diavgeia PDF to local storage atomically."""
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = pdf_path.with_suffix(".pdf.part")
    try:
        with requests.get(document_url, stream=True, timeout=(10, timeout)) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)
        tmp_path.replace(pdf_path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def enrich_with_pdf_content(df: pd.DataFrame) -> pd.DataFrame:
    """
    Download/parse PDFs for new rows and embed parsed text into the dataset.

    This runs only for newly fetched rows before merge into the main CSV.
    """
    if df.empty:
        return ensure_pdf_embed_columns(df)

    df = ensure_pdf_embed_columns(df)
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    total = len(df)
    for i, idx in enumerate(df.index, start=1):
        ada = str(df.at[idx, "ada"] if "ada" in df.columns else "").strip()
        document_url = str(df.at[idx, "documentUrl"] if "documentUrl" in df.columns else "").strip()

        if i == 1 or i % 25 == 0 or i == total:
            print(f"[pdf][progress] {i}/{total}", flush=True)

        if not document_url or document_url.lower() == "nan":
            df.at[idx, "pdf_download_status"] = "missing_document_url"
            df.at[idx, "pdf_parse_status"] = "skipped"
            continue

        code = extract_document_code(document_url) or ada
        if not code:
            df.at[idx, "pdf_download_status"] = "invalid_document_url"
            df.at[idx, "pdf_download_error"] = "Could not extract document code"
            df.at[idx, "pdf_parse_status"] = "skipped"
            continue

        pdf_path = PDF_DIR / f"{code}.pdf"
        df.at[idx, "pdf_file_name"] = pdf_path.name

        try:
            if pdf_path.exists():
                df.at[idx, "pdf_download_status"] = "exists"
            else:
                _download_pdf_to_path(document_url, pdf_path)
                df.at[idx, "pdf_download_status"] = "downloaded"
        except Exception as exc:
            df.at[idx, "pdf_download_status"] = "error"
            df.at[idx, "pdf_download_error"] = f"{type(exc).__name__}: {exc}"
            df.at[idx, "pdf_parse_status"] = "skipped"
            continue

        parsed_row, _page_count, parse_err = parse_pdf_file_to_row(str(pdf_path))
        if parse_err:
            df.at[idx, "pdf_parse_status"] = "error"
            df.at[idx, "pdf_parse_error"] = parse_err
            continue

        pdf_text = str(parsed_row.get("text") or "")
        df.at[idx, "pdf_parse_status"] = "ok"
        df.at[idx, "pdf_page_count"] = str(parsed_row.get("page_count", "") or "")
        df.at[idx, "pdf_text"] = pdf_text
        df.at[idx, "pdf_text_length"] = str(len(pdf_text))

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Diavgeia incremental fetch")
    print("=" * 60)
    run_started_at = datetime.now(ATHENS_TZ).strftime("%Y-%m-%d %H:%M:%S %z")
    fetched_records = 0
    added = 0
    csv_updated = False
    success = False
    error = ""

    try:
        state = load_state()
        last_fetch_str = state.get("last_fetch")

        since: datetime | None = None
        if last_fetch_str:
            try:
                since = datetime.strptime(last_fetch_str, "%d/%m/%Y %H:%M:%S")
            except ValueError:
                since = datetime.strptime(last_fetch_str, "%d/%m/%Y")
            print(f"[main] Fetching decisions submitted after: {since}")
        else:
            print("[main] No cutoff date — fetching ALL available data.")

        new_records = fetch_new_decisions(since=since)
        fetched_records = len(new_records)
        added = append_to_csv(new_records)
        csv_updated = added > 0

        if new_records:
            # Find the most recent submissionTimestamp among fetched records
            timestamps = []
            for rec in new_records:
                ts = rec.get("submissionTimestamp", "")
                try:
                    timestamps.append(datetime.strptime(ts, "%d/%m/%Y %H:%M:%S"))
                except ValueError:
                    pass
            if timestamps:
                new_last = max(timestamps)
                state["last_fetch"] = new_last.strftime("%d/%m/%Y %H:%M:%S")
                save_state(state)

        success = True
        print(f"\n[done] {added} new rows added to dataset.")
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        print(f"[error] {error}")
        raise
    finally:
        append_run_log(
            run_started_at=run_started_at,
            fetched_records=fetched_records,
            rows_added=added,
            csv_updated=csv_updated,
            success=success,
            error=error,
        )


if __name__ == "__main__":
    main()
