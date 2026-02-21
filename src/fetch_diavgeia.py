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
CSV_PATH = DATA_DIR / "2026_diavgeia.csv"
STATE_FILE = STATE_DIR / "state.json"
LEGACY_CSV_PATH = REPO_DIR / "2026_diavgeia.csv"
LEGACY_STATE_FILE = REPO_DIR / "state.json"

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
    ("ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ",                     "Δημοτική Επιχείρηση"),
    ("ΠΕΡΙΦΕΡΕΙΑ",                              "Περιφέρεια"),
    ("ΥΠΟΥΡΓΕΙΟ",                               "Υπουργείο"),
    ("ΔΗΜΟΣ",                                   "Δήμος"),
    ("ΔΗΜΟ",                                    "Δήμος"),   # handles typos like "ΔΗΜΟ ΑΡΓΟΥΣ"
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

    for prefix, label in ORG_PREFIXES:
        if org_stripped.upper().startswith(prefix):
            name_clean = org_stripped[len(prefix):].strip(" -,")
            return (label, name_clean)

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


def normalize_upper_no_accents(value):
    """Uppercase and remove diacritics from text values."""
    if not isinstance(value, str):
        return value

    normalized = unicodedata.normalize("NFD", value.upper())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_org_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize org classification columns to uppercase without accents."""
    df = df.copy()
    for col in ("org_type", "org_name_clean"):
        if col in df.columns:
            df[col] = df[col].apply(normalize_upper_no_accents)
    return df


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add org, org_type, org_name_clean columns to a raw Diavgeia DataFrame."""

    df = df.copy()
    if "organization" not in df.columns:
        raise KeyError("Expected column 'organization' in dataframe")

    df["org"] = df["organization"].apply(extract_org_label)

    classifications = df["org"].apply(classify_org)
    df["org_type"] = classifications.apply(lambda t: normalize_upper_no_accents(t[0]))
    df["org_name_clean"] = classifications.apply(lambda t: normalize_upper_no_accents(t[1]))

    return normalize_org_columns(df)


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
    combined = normalize_org_columns(combined)

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

    added = append_to_csv(new_records)

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

    print(f"\n[done] {added} new rows added to dataset.")


if __name__ == "__main__":
    main()
