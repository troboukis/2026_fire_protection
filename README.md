# fire_protection_2026

Project structure:

```text
.
├── data/
│   └── 2026_diavgeia.csv
├── src/
│   └── fetch_diavgeia.py
├── state/
│   └── state.json
├── logs/
│   └── fetch_runs.csv
└── fetch_diavgeia.py
```

- `src/fetch_diavgeia.py`: core fetch + enrich + persist logic
- `data/2026_diavgeia.csv`: dataset output
- `state/state.json`: incremental fetch checkpoint
- `logs/fetch_runs.csv`: run history (timestamp, fetched count, CSV update flag, success/error)
- `fetch_diavgeia.py`: root launcher for backward-compatible execution

Run:

```bash
python fetch_diavgeia.py
```

One-command local fetch + git sync:

```bash
./scripts/run_fetch_and_sync.sh
```

Script behavior:
- auto-commits any existing local changes first
- pulls latest `origin/main` with rebase
- runs `fetch_diavgeia.py`
- commits changed artifacts (`data/`, `state/`, `logs/`)
- pushes to `origin/main`

## Local PDF pipeline

PDFs are stored locally in `pdf/` (excluded from git via `.gitignore`).
Each filename is derived from `documentUrl` as the code after `/doc/`, with `.pdf` suffix.
Example: `https://diavgeia.gov.gr/doc/9ΚΠΣΩ1Ε-ΕΑ0` -> `pdf/9ΚΠΣΩ1Ε-ΕΑ0.pdf`.

Run full pipeline (download missing + build page dataset):

```bash
./.fireprotection/bin/python src/pdf_pipeline.py
```

Download only:

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --download-only
```

Build page dataset only:

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --build-only
```

Useful options:
- `--limit 100` to test on a subset
- `--source-csv data/2026_diavgeia.csv`
- `--pdf-dir pdf`
- `--pages-dataset data/pdf_pages_dataset.csv`

Run logs are appended to `logs/pdf_pipeline_runs.csv` and include:
- download counters (`downloaded`, `skipped_existing`, `failed_downloads`)
- parsing counters (`parsed_pdfs`, `parsed_pages`, `parse_errors`)
- `success` and `error_message`

## Daily automated collection (GitHub Actions)

A workflow is included at `.github/workflows/daily-fetch.yml` and runs:

- every day at `03:00` UTC
- on manual trigger (`workflow_dispatch`)

To enable automation:

```bash
git add .
git commit -m "chore: setup daily Diavgeia automation"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
```

Then in GitHub:

1. Open the repository `Settings` -> `Actions` -> `General`
2. Ensure actions are allowed and workflow permissions allow read/write
3. Open the `Actions` tab and run `Daily Diavgeia Fetch` once manually

## Database schema (Supabase/Postgres)

The initial relational schema is in `sql/001_init_schema.sql`.
It creates:
- `organization` (one organization to many records)
- `record` (main records, each linked to one organization)
- `file` (one-to-one with `record` via `ada`)

To create tables:

1. Open your Supabase project
2. Go to `SQL Editor`
3. Paste and run the contents of `sql/001_init_schema.sql`

## Data Processing Documentation

This section documents exactly how `src/fetch_diavgeia.py` handles data from Diavgeia and writes output to `data/2026_diavgeia.csv`.

### 1) Ingestion and incremental behavior

- Data source endpoint: `https://diavgeia.gov.gr/luminapi/api/search`
- Query terms are controlled by `KEYWORDS`.
- Pagination is used (`PAGE_SIZE=100`).
- Incremental cutoff comes from `state/state.json` key `last_fetch`.
- If `state/state.json` does not exist:
  - The script tries to derive the latest timestamp from `data/2026_diavgeia.csv`.
  - If no CSV exists, it fetches all available data.
- After a successful run with new records, `last_fetch` is updated to the maximum fetched `submissionTimestamp`.

### 2) Data enrichment pipeline

Each fetched batch is converted into a dataframe and enriched before save.

Main enrichments:
- `org`: extracted from `organization.label`
- `org_type`, `org_name_clean`: derived by organization classification
- `decisionType`: converted to label-only string
- `thematicCategories`: converted to list of label-only strings

The script supports both:
- raw API dict/list values
- CSV stringified dict/list values (legacy rows)

### 3) Field parsing helpers

The script uses robust parsing helpers to avoid crashes and inconsistent shapes:

- `extract_org_label(value)`
  - Handles dict payloads from API (`{"label": ...}`).
  - Handles stringified dicts from CSV.

- `parse_structured_value(value)`
  - Tries parsing dict/list represented as strings.
  - Supports both Python-literal style and JSON style.

- `extract_label(value)`
  - Normalizes single label fields (used by `decisionType`).

- `extract_labels_list(value)`
  - Normalizes list-like fields (used by `thematicCategories`).
  - Deduplicates while preserving original order.

### 4) Organization classification logic

Classification is prefix-based and order-sensitive (`ORG_PREFIXES`).

Important safeguards:
- Prefixes are matched as **whole tokens**, not partial words.
  - This prevents bad truncation like `ΔΗΜΟΤΙΚΟ -> ΤΙΚΟ`.
- A special typo rule handles forms like `ΔΗΜΟ ΑΡΓΟΥΣ` without incorrectly matching `ΔΗΜΟΤΙΚΟ ...`.

Current explicit categories include:
- `ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ`
- `ΠΕΡΙΦΕΡΕΙΑΚΟ ΤΑΜΕΙΟ ΑΝΑΠΤΥΞΗΣ`
- `ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ`
- `ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ`
- `ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ`
- `ΔΗΜΟΤΙΚΟ ΒΡΕΦΟΚΟΜΕΙΟ`
- `ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ`
- `ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ`
- `ΠΕΡΙΦΕΡΕΙΑ`
- `ΥΠΟΥΡΓΕΙΟ`
- `ΔΗΜΟΣ`
- fallback: `ΑΛΛΟΣ ΦΟΡΕΑΣ`

### 5) Name normalization rules

After classification:
- text is converted to uppercase
- accents/diacritics are removed

Type-specific rules:

- For `ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ`:
  - removes leading boilerplate:
    - `ΓΙΑ ΤΗΝ ...`
    - `ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ...`

- For `ΥΠΟΥΡΓΕΙΟ`:
  - applies conservative canonical mappings for known historical variants
  - example:
    - `ΠΕΡΙΒΑΛΛΟΝΤΟΣ, ΕΝΕΡΓΕΙΑΣ ΚΑΙ ΚΛΙΜΑΤΙΚΗΣ ΑΛΛΑΓΗΣ` -> `ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ`
    - `ΥΠΟΔΟΜΩΝ, ΜΕΤΑΦΟΡΩΝ ΚΑΙ ΔΙΚΤΥΩΝ` -> `ΥΠΟΔΟΜΩΝ ΚΑΙ ΜΕΤΑΦΟΡΩΝ`
    - `ΕΣΩΤΕΡΙΚΩΝ ΚΑΙ ΔΙΟΙΚΗΤΙΚΗΣ ΑΝΑΣΥΓΚΡΟΤΗΣΗΣ` -> `ΕΣΩΤΕΡΙΚΩΝ`
    - `ΠΑΙΔΕΙΑΣ, ΕΡΕΥΝΑΣ ΚΑΙ ΘΡΗΣΚΕΥΜΑΤΩΝ` -> `ΠΑΙΔΕΙΑΣ ΚΑΙ ΘΡΗΣΚΕΥΜΑΤΩΝ`

### 6) Legacy correction behavior

When appending new data:
- existing CSV rows are re-normalized, not just new rows
- this ensures old formatting/classification issues are corrected over time
- deduplication is then applied (`drop_duplicates`)

### 7) Run logging

Each run appends one row to `logs/fetch_runs.csv` with:
- `run_started_at_athens`
- `fetched_records`
- `rows_added`
- `csv_updated`
- `success`
- `error` (boolean; `False` on success, `True` on failure)
- `error_message` (`NONE` on success)

### 8) Operational notes

- GitHub Action commits updated artifacts:
  - `data/2026_diavgeia.csv`
  - `state/state.json`
  - `logs/fetch_runs.csv`
- Schedule is `03:00 UTC` daily.
- If fetch fails, run log is still persisted and workflow is marked failed.
