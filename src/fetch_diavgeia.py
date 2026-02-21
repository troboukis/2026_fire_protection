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
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from IPython.display import clear_output  # noqa: F401 (safe to remove if not in Jupyter)

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

SEARCH_URL = "https://diavgeia.gov.gr/luminapi/api/search"
KEYWORDS = ["πυροπροστ", "αποψιλ", "δασοπροστ", "αντιπυρ"]
PAGE_SIZE = 100

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
            print(f"[fetch] page {page + 1}/{pages} — {len(new_in_batch)} new records (total so far: {len(results)})")
            if stop:
                print("[fetch] Reached records older than cutoff. Stopping.")
                break
        else:
            results.extend(batch)
            print(f"[fetch] page {page + 1}/{pages} — {len(batch)} records (total so far: {len(results)})")

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


def normalize_decision_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize decisionType and thematicCategories label columns."""
    df = df.copy()
    if "decisionType" in df.columns:
        df["decisionType"] = df["decisionType"].apply(extract_label)
    if "thematicCategories" in df.columns:
        df["thematicCategories"] = df["thematicCategories"].apply(extract_labels_list)
    return df


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add org, org_type, org_name_clean columns to a raw Diavgeia DataFrame."""

    df = df.copy()
    if "organization" not in df.columns:
        raise KeyError("Expected column 'organization' in dataframe")

    df["org"] = df["organization"].apply(extract_org_label)
    df = recompute_org_classification(df)

    df = normalize_decision_columns(df)
    return df


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

    # API returns dicts/lists for some columns; CSV stores them as strings.
    # Stringify any remaining dict/list values so both sides are comparable.
    for col in new_df.columns:
        new_df[col] = new_df[col].apply(
            lambda x: str(x) if isinstance(x, (dict, list)) else x
        )

    if CSV_PATH.exists():
        existing = pd.read_csv(CSV_PATH)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    # Keep old rows aligned with current normalization rules.
    combined = normalize_decision_columns(combined)
    combined = recompute_org_classification(combined)

    original_len = len(existing) if CSV_PATH.exists() else 0

    # Drop exact duplicate rows. The same ADA can legitimately appear with
    # different decisionType values (kept), but fully identical rows are dropped.
    combined = combined.drop_duplicates()

    added = len(combined) - original_len
    if added <= 0:
        print("[csv] All fetched records already exist in CSV (no new rows).")
        return 0

    combined.to_csv(CSV_PATH, index=False)
    print(f"[csv] Appended {len(new_df)} new rows → {CSV_PATH}")
    return len(new_df)


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
