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
- runs `fetch_diavgeia.py` (PDF download is disabled by default)
- commits changed artifacts (`data/`, `state/`, `logs/`)
- pushes to `origin/main`

Useful flags for `run_fetch_and_sync.sh`:
- `DOWNLOAD_DIAVGEIA_PDFS=1`: enable Diavgeia PDF download + parse during fetch step
- `RUN_DB_INGEST=1`: run DB ingestion scripts (including `ingest_raw_procurements.py`)

Examples:

```bash
./scripts/run_fetch_and_sync.sh
DOWNLOAD_DIAVGEIA_PDFS=1 ./scripts/run_fetch_and_sync.sh
RUN_DB_INGEST=1 ./scripts/run_fetch_and_sync.sh
DOWNLOAD_DIAVGEIA_PDFS=1 RUN_DB_INGEST=1 ./scripts/run_fetch_and_sync.sh
```

## Local PDF pipeline

PDFs are stored locally in `pdf/` (excluded from git via `.gitignore`).
Each filename is derived from `documentUrl` as the code after `/doc/`, with `.pdf` suffix.
Example: `https://diavgeia.gov.gr/doc/9ΚΠΣΩ1Ε-ΕΑ0` -> `pdf/9ΚΠΣΩ1Ε-ΕΑ0.pdf`.

The pipeline does two steps:
- download missing PDFs from `documentUrl`
- parse local PDFs and build `data/pdf_pages_dataset.csv`

Current parser output is **one row per PDF** (not one row per page):
- `ada`
- `file_name`
- `page_count`
- `text` (all pages concatenated)
- `text_length`
- `parse_error`

### Command reference (`src/pdf_pipeline.py`)

Base command:

```bash
./.fireprotection/bin/python src/pdf_pipeline.py
```

1. Full pipeline (download missing PDFs + build dataset)

```bash
./.fireprotection/bin/python src/pdf_pipeline.py
```

What it does:
- reads source records from `data/2026_diavgeia.csv`
- downloads only missing PDFs into `pdf/`
- parses local PDFs
- writes aggregated text dataset to `data/pdf_pages_dataset.csv`
- appends run stats to `logs/pdf_pipeline_runs.csv`

2. Download only (no parsing / no dataset rebuild)

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --download-only
```

What it does:
- reads `documentUrl` values from the source CSV
- downloads only PDFs that do not already exist in `pdf/`
- skips dataset generation
- still logs the run

3. Build only (parse existing local PDFs only)

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --build-only
```

What it does:
- does not download anything
- parses PDFs already present in `pdf/`
- rewrites `data/pdf_pages_dataset.csv`
- logs parsing counters

4. Test on a small subset (`--limit`)

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --limit 100
```

What it does:
- limits both download scanning and PDF parsing to the first `100` records/files
- useful for smoke tests and debugging

5. Faster local parsing with multiple workers (`--workers`)

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --build-only --workers 4
```

What it does:
- parses PDFs in parallel (processes)
- speeds up the build step on multi-core machines
- only affects the parsing/build step (downloads are still sequential)

6. Increase HTTP timeout for slow downloads (`--timeout`)

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --download-only --timeout 120
```

What it does:
- increases PDF download read timeout (seconds)
- useful for slow network responses / large files

7. Use custom input/output paths

```bash
./.fireprotection/bin/python src/pdf_pipeline.py \
  --source-csv data/2026_diavgeia.csv \
  --pdf-dir pdf \
  --pages-dataset data/pdf_pages_dataset.csv
```

What it does:
- overrides default source CSV / PDF storage directory / output dataset path

8. Common combinations

Download a subset only:

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --download-only --limit 50
```

Build a subset with parallel parsing:

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --build-only --limit 200 --workers 6
```

Run full pipeline with custom timeout and parallel build:

```bash
./.fireprotection/bin/python src/pdf_pipeline.py --timeout 120 --workers 4
```

### CLI flags and what each does

- `--source-csv <path>`: source CSV containing at least `ada` and `documentUrl`
- `--pdf-dir <path>`: local folder where PDFs are stored/read
- `--pages-dataset <path>`: output CSV path for parsed PDF text dataset (one row per PDF)
- `--limit <N>`: process only the first `N` records/files (useful for testing)
- `--workers <N>`: number of worker processes for PDF parsing (`--build-only` or full run build step)
- `--download-only`: run only the download step
- `--build-only`: run only the parsing/dataset build step
- `--timeout <seconds>`: HTTP read timeout for PDF downloads (connect timeout is fixed at 10s)

Note:
- `--download-only` and `--build-only` are mutually exclusive (cannot be used together)

### Run logging

Every run appends one row to `logs/pdf_pipeline_runs.csv`, including:
- download counters (`records_scanned`, `downloaded`, `skipped_existing`, `skipped_missing_url`, `failed_downloads`)
- parsing counters (`pdf_files_seen`, `parsed_pdfs`, `parsed_pages`, `parse_errors`)
- `success`
- `error_message`

## Local Relevance Filter (Subject + PDF)

This is a **separate local-only post-processing step** that runs **after**:

1. `fetch_diavgeia.py` (raw records + decision-type enrichments, optional PDF embed)
2. `pdf_pipeline.py` (download + parse PDFs)

It checks whether each record is relevant to forest-fire prevention/suppression using:
- the decision `subject`
- the parsed PDF text (looked up by `ada`)

If at least one keyword is found in either source, the row is marked relevant and included in the filtered dataset.

### Why this is local-only

This step depends on local PDF artifacts and parsed PDF text, which are large and operationally unsuitable for GitHub Actions in this project.

Local-only components:
- PDF downloading (`src/pdf_pipeline.py`)
- PDF parsing (`src/pdf_pipeline.py`)
- Relevance filtering (`src/filter_relevance.py`)

`src/filter_relevance.py` includes a CI guard and will refuse to run in CI/GitHub Actions unless explicitly overridden with `--allow-ci`.

### Strategy (simple boolean rule)

For each row in `data/2026_diavgeia.csv`:

- `subject_match = any(keyword in normalized(subject))`
- `pdf_match = any(keyword in normalized(pdf_text_for_same_ada))`
- `is_relevant = subject_match OR pdf_match`

No scoring / ranking is used.

### Important implementation detail (no dataframe join)

To avoid inflating the raw dataset or doing a heavy merge:

- the script reads `data/pdf_pages_dataset.csv` using only columns `ada` and `text`
- builds an in-memory lookup dictionary: `ada -> text`
- checks PDF text per row using `ada`

This means:
- no large text join into `data/2026_diavgeia.csv`
- no duplication of PDF text inside the raw dataset

### Text normalization used before matching

Both keywords and text (`subject`, PDF text) are normalized before matching:
- lowercase
- remove Greek tonos/diacritics
- normalize final sigma (`ς -> σ`)
- replace punctuation/symbols with spaces
- collapse multiple spaces

This allows matching regardless of:
- accents (e.g. `δασικών` vs `δασικων`)
- uppercase/lowercase
- punctuation differences

### Inputs / Outputs (spec)

Inputs:
- `data/2026_diavgeia.csv` (raw dataset)
- `data/pdf_pages_dataset.csv` (parsed PDF text dataset, one row per PDF; must include `ada`, `text`)

Outputs:
- updates `data/2026_diavgeia.csv` by adding/updating relevance columns
- writes `data/2026_diavgeia_filtered.csv` (only `is_relevant == True`)
- appends run metrics to `logs/relevance_filter_runs.csv`

Database feed source:
- `data/2026_diavgeia_filtered.csv`

### Relevance columns added to `data/2026_diavgeia.csv`

- `subject_match` (`True/False`)
- `pdf_match` (`True/False`)
- `pdf_available_for_filter` (`True/False`)
  - `True` if parsed PDF text exists for that `ada`
  - `False` if no parsed PDF text is available (missing PDF / parse failure / no row)
- `is_relevant` (`True/False`)
- `matched_keywords_subject`
  - matched keyword(s) from `subject`
  - cleanup rule: `[] -> empty`, `[x] -> x`, `[x,y] -> list`
- `matched_keywords_pdf`
  - matched keyword(s) from PDF text
  - same cleanup rule as above

### Filtered dataset (`data/2026_diavgeia_filtered.csv`)

Contains:
- all columns from `data/2026_diavgeia.csv`
- only rows where `is_relevant == True`

Recommended use:
- use this file as the source for database ingestion

### Command reference (`src/filter_relevance.py`)

Base command (local):

```bash
./.fireprotection/bin/python src/filter_relevance.py
```

What it does:
- loads raw dataset
- loads parsed PDF text lookup by `ada`
- computes relevance columns in raw dataset
- writes filtered dataset

Custom paths:

```bash
./.fireprotection/bin/python src/filter_relevance.py \
  --input-csv data/2026_diavgeia.csv \
  --pdf-pages-dataset data/pdf_pages_dataset.csv \
  --filtered-output data/2026_diavgeia_filtered.csv \
  --log-csv logs/relevance_filter_runs.csv
```

Progress frequency:

```bash
./.fireprotection/bin/python src/filter_relevance.py --progress-every 100
```

CI override (not recommended):

```bash
./.fireprotection/bin/python src/filter_relevance.py --allow-ci
```

### Relevance filter run log (`logs/relevance_filter_runs.csv`)

Each run appends one row including:
- `run_started_at_local`
- `input_csv`
- `pdf_pages_dataset`
- `filtered_output_csv`
- `keywords_count`
- `rows_total`
- `rows_relevant`
- `rows_not_relevant`
- `rows_subject_match`
- `rows_pdf_match`
- `rows_pdf_available`
- `filtered_rows_written`
- `success`
- `error_message`

### Keyword list source

The keyword list is defined in:
- `src/filter_relevance.py` -> `RELEVANCE_KEYWORDS`

Update that list to refine recall/precision. After any keyword change, re-run the relevance filter locally to regenerate:
- `data/2026_diavgeia.csv` relevance columns
- `data/2026_diavgeia_filtered.csv`

### Recommended local pipeline order

1. `./.fireprotection/bin/python fetch_diavgeia.py`
2. `./.fireprotection/bin/python src/pdf_pipeline.py` (or `--build-only` if PDFs already downloaded)
3. `./.fireprotection/bin/python src/filter_relevance.py`

### Operational notes / limitations

- If PDF text extraction failed (or the PDF is unavailable), `pdf_match` may be `False` even for a relevant record.
- This is why the filter checks both `subject` and PDF text.
- The raw dataset remains the audit source; the filtered dataset is the operational source for DB ingestion.

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
PDF download/parse during fetch is controlled by `DOWNLOAD_DIAVGEIA_PDFS`:
- default (`0`): skip PDF download/parse in `fetch_diavgeia.py`
- set to `1`: download/parse PDFs and embed text/status columns in fetched rows

Main enrichments:
- `org`: extracted from `organization.label`
- `org_type`, `org_name_clean`: derived by organization classification
- `decisionType`: converted to label-only string
- `thematicCategories`: converted to list of label-only strings
- `subject_has_anatrop_or_anaklis` (`True/False`): derived boolean flag from `subject`
  - `True` when subject contains `ανατροπ*` or `ανακλησ*` (accent-insensitive)
- `subject_has_budget_balance_report_terms` (`True/False`): derived boolean flag from `subject`
  - `True` when subject contains `προϋπολογισμ*`, `ισολογισμ*`, or `απολογισμ*` (accent-insensitive)
- `org_name_clean` exclusion list (dataset scope cleanup)
  - rows whose normalized `org_name_clean` matches a configured blacklist are dropped from the dataset
  - applied both during fetch (API batch filtering) and before CSV save (safety net)

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
- Fetch logs may report API totals larger than CSV additions because excluded organizations are skipped after retrieval.
- Schedule is `03:00 UTC` daily.
- If fetch fails, run log is still persisted and workflow is marked failed.

### 9) Decision-Type `decisions/view` enrichment formats (detailed)

For selected `decisionType` values, `src/fetch_diavgeia.py` performs an extra API call to:

- `https://diavgeia.gov.gr/luminapi/api/decisions/view/{ada}`

The response contains a `meta` field (list of one-key dictionaries). The script flattens that list and extracts type-specific fields into dedicated CSV columns.

Important storage note:
- In memory, many extracted values are Python lists/dicts.
- In `data/2026_diavgeia.csv`, they are stored as stringified values (because CSV has no native nested types).

Quick summary table:

| `decisionType` | Column prefix | Main extracted entities |
|---|---|---|
| `ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ` | `spending_*` | signers + contractors (AFM, name, amount, currency) |
| `ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ` | `commitment_*` | signers + fiscal/budget fields + `Ποσό και ΚΑΕ/ΑΛΕ` lines |
| `ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ` | `direct_*` | signers + persons (AFM/name) + amount + references |
| `ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ` | `payment_*` | signers + beneficiaries (AFM/name/value) + references |

#### A) `ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ` (Spending approval)

Relevant `meta` keys used:
- `Υπογράφοντες`
- `Στοιχεία αναδόχων` (list)
  - each item may include:
    - `ΑΦΜ / Επωνυμία` -> `{ΑΦΜ, Επωνυμία, ...}`
    - `Ποσό δαπάνης` -> `{Αξία, Νόμισμα}`

Collected columns:
- `spending_signers`: list from `Υπογράφοντες`
- `spending_contractors_afm`: list of contractor AFM values
- `spending_contractors_name`: list of contractor names (`Επωνυμία`)
- `spending_contractors_value`: list of expense amounts (`Αξία`)
- `spending_contractors_currency`: list of currencies (`Νόμισμα`)
- `spending_contractors_count`: number of contractor rows extracted
- `spending_contractors_details`: list of dicts with `{ΑΦΜ, Επωνυμία, Αξία, Νόμισμα}`

Status / audit columns:
- `spending_enrichment_status`: `ok`, `error`, or `skip_missing_ada`
- `spending_enrichment_error`: error text when status is `error`

#### B) `ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ` (Commitment / obligation assumption)

Relevant `meta` keys used:
- `Υπογράφοντες`
- `Οικονομικό Έτος`
- `Κατηγορία Προϋπολογισμού`
- `Συνολικό ποσό` (fallback when `Ποσό και ΚΑΕ/ΑΛΕ` is empty)
- `Ποσό και ΚΑΕ/ΑΛΕ` (list)
  - each item may include:
    - `ΑΦΜ / Επωνυμία`
    - `Αριθμός ΚΑΕ/ΑΛΕ`
    - `Ποσό με ΦΠΑ`
    - `Υπόλοιπο διαθέσιμης πίστωσης`
    - `Υπόλοιπο ΚΑΕ/ΑΛΕ`

Collected columns:
- `commitment_signers`: list from `Υπογράφοντες`
- `commitment_fiscal_year`: `Οικονομικό Έτος`
- `commitment_budget_category`: `Κατηγορία Προϋπολογισμού`
- `commitment_counterparty`: list from `ΑΦΜ / Επωνυμία` (one per line in `Ποσό και ΚΑΕ/ΑΛΕ`)
- `commitment_amount_with_vat`: list of `Ποσό με ΦΠΑ`
- `commitment_remaining_available_credit`: list of `Υπόλοιπο διαθέσιμης πίστωσης`
- `commitment_kae_ale_number`: list of `Αριθμός ΚΑΕ/ΑΛΕ`
- `commitment_remaining_kae_ale`: list of `Υπόλοιπο ΚΑΕ/ΑΛΕ`
- `commitment_lines_count`: number of rows in `Ποσό και ΚΑΕ/ΑΛΕ`
- `commitment_lines_details`: list of dicts preserving all extracted row-level fields

Status / audit columns:
- `commitment_enrichment_status`: `ok`, `error`, or `skip_missing_ada`
- `commitment_enrichment_error`: error text when status is `error`

#### C) `ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ` (Direct assignment)

Relevant `meta` keys used:
- `Υπογράφοντες`
- `ΑΦΜ / Επωνυμία προσώπου / προσώπων` (list)
  - each item may include `ΑΦΜ`, `Επωνυμία`
- `Ποσό` -> `{Αξία, Νόμισμα}` (currently only `Αξία` is stored)
- `Σχετ. Ανάληψη υποχρέωσης`
- `Δείτε επίσης και ..`

Collected columns (requested `direct_*` naming):
- `direct_signers`: list from `Υπογράφοντες`
- `direct_afm`: list of AFM values
- `direct_name`: list of names (`Επωνυμία`)
- `direct_value`: amount value from `Ποσό -> Αξία`
- `direct_related_commitment`: `Σχετ. Ανάληψη υποχρέωσης`
- `direct_see_also`: `Δείτε επίσης και ..`

Helper columns:
- `direct_people_count`: number of persons in `ΑΦΜ / Επωνυμία προσώπου / προσώπων`
- `direct_people_details`: list of dicts with `{ΑΦΜ, Επωνυμία}`
- `direct_enrichment_status`
- `direct_enrichment_error`

#### D) `ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ` (Payment finalization)

Relevant `meta` keys used:
- `Υπογράφοντες`
- `Στοιχεία δικαιούχων` (list)
  - each item may include:
    - `ΑΦΜ / Επωνυμία` -> `{ΑΦΜ, Επωνυμία, ...}`
    - `Ποσό δαπάνης` -> `{Αξία, Νόμισμα}`
- `Σχετ. Ανάληψη Υποχρέωσης/Έγκριση Δαπάνης`
- `Δείτε επίσης και ..`

Collected columns:
- `payment_signers`: list from `Υπογράφοντες`
- `payment_beneficiary_afm`: list of beneficiary AFM values
- `payment_beneficiary_name`: list of beneficiary names (`Επωνυμία`)
- `payment_value`: list of beneficiary expense amounts (`Αξία`)
- `payment_related_commitment_or_spending`: `Σχετ. Ανάληψη Υποχρέωσης/Έγκριση Δαπάνης`
- `payment_see_also`: `Δείτε επίσης και ..`

Helper columns:
- `payment_beneficiaries_count`: number of beneficiary rows
- `payment_beneficiaries_details`: list of dicts with `{ΑΦΜ, Επωνυμία, Αξία}`
- `payment_enrichment_status`
- `payment_enrichment_error`

#### Enrichment execution behavior

- Decision-type enrichment runs automatically during `fetch_diavgeia.py` for new rows.
- PDF enrichment in `fetch_diavgeia.py` runs only when `DOWNLOAD_DIAVGEIA_PDFS=1`.
- Existing CSV rows can be backfilled from a notebook via:
  - `fetch_diavgeia.backfill_spending_approval_columns(...)`
- Root-level `fetch_diavgeia.py` is a thin wrapper (exports `main` only). For backfill helpers, import from `src/` with `insert(0, ...)` so the root wrapper is not imported first:
  - `python -c "import sys; sys.path.insert(0, 'src'); from fetch_diavgeia import backfill_spending_approval_columns; backfill_spending_approval_columns(...)"`  
- The backfill currently processes all supported types above (despite the legacy function name).
- Progress is printed during enrichment (`[spending]`, `[commitment]`, `[direct]`, `[payment]` start/progress/done lines).

## Procurement DB ingestion (raw KIMDIS + Diavgeia layers)

The web-app now uses `data/raw_procurements.csv` (KIMDIS contracts) as the main procurement dataset.

Raw dataset pipeline:
- collection script: `fetch_kimdis_procurements.py` (wrapper) / `src/fetch_kimdis_procurements.py`
- source API: `https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract`
- output files:
  - `data/raw_items_backup.json` (raw API backup)
  - `data/raw_procurements.csv` (filtered tabular dataset)
- DB table: `public.raw_procurements`

Run raw collection manually:

```bash
python fetch_kimdis_procurements.py
```

Incremental behavior (Diavgeia-style):
- uses `state/kimdis_state.json` with `last_fetch`
- if state is missing, derives last fetch from max `submissionDate` in `data/raw_procurements.csv`
- fetches from the effective start date forward and then merges with existing CSV using dedupe
- use `--full-refresh` to ignore state and refetch the whole window
- CSV merge dedupe strategy is full-row dedupe after normalizing list/dict values to stable JSON strings

Rebuild CSV from existing backup only (no API call):

```bash
python fetch_kimdis_procurements.py --from-backup
```

Force a full refetch:

```bash
python fetch_kimdis_procurements.py --full-refresh --start-date 2024-01-01
```

KIMDIS fetch flags:
- `--request-wait-seconds <float>`: wait between API requests (default `1.0`)
- `--retry-sleep-seconds <int>`: base retry sleep in seconds for backoff (default `5`)
- `--request-timeout <int>`: HTTP timeout per request in seconds (default `60`)
- `--max-window-days <int>`: date span per API window (default `180`)
- `--state-file <path>`: incremental state file path (default `state/kimdis_state.json`)
- `--backup-json <path>`: raw API backup JSON path
- `--output-csv <path>`: output CSV path
- `--log-csv <path>`: run log CSV path

Examples:

```bash
# Full refresh with default 1s per-request wait
python fetch_kimdis_procurements.py --full-refresh --start-date 2024-01-01

# Slower request pace + longer timeout
python fetch_kimdis_procurements.py --full-refresh --start-date 2024-01-01 --request-wait-seconds 2 --request-timeout 120
```

Ingest raw CSV into Supabase:

```bash
python ingest/ingest_raw_procurements.py
```

Dry-run parse check (no DB write):

```bash
python ingest/ingest_raw_procurements.py --dry-run
```

Diavgeia procurement tables are still kept for future extensions.

The web-app procurement layer now uses a two-table model:

- `public.procurement_decisions`: one row per `ADA` (header-level metadata)
- `public.procurement_decision_lines`: multiple rows per `ADA` (amounts / counterparties / line details)

Why:
- some Diavgeia decisions contain multiple amounts and/or multiple beneficiaries/contractors
- keeping only one `amount_eur`/contractor per `ADA` loses detail

### Migrations to run in Supabase

Run these in `SQL Editor` (once per database):

1. `sql/004_procurement_subject_flags.sql`
   - adds subject-derived boolean flags to `procurement_decisions`
2. `sql/005_procurement_decision_lines.sql`
   - creates line-level table `procurement_decision_lines`
3. `sql/006_org_municipality_coverage.sql`
   - creates `org_municipality_coverage` (org -> all municipalities covered)
4. `sql/007_raw_procurements.sql`
   - creates `raw_procurements` (main KIMDIS raw contracts table)
5. `sql/008_raw_procurements_views.sql`
   - creates `v_raw_procurements_municipality` (frontend-friendly municipality-linked raw procurements view)
6. `sql/009_raw_procurements_hero_stats_fn.sql`
   - creates RPC function `get_raw_procurements_hero_stats(...)` used by homepage Hero KPIs
7. `sql/010_raw_procurements_cumulative_curve_fn.sql`
   - creates RPC function `get_raw_procurements_cumulative_curve(...)` for the homepage cumulative line chart

### Procurement ingest commands

From the project root:

```bash
python ingest/ingest_raw_procurements.py
python ingest/ingest_procurement.py
python ingest/ingest_procurement_lines.py
python ingest/ingest_org_municipality_coverage.py
```

Behavior:
- `ingest_raw_procurements.py` truncates + reloads `public.raw_procurements` from `data/raw_procurements.csv`
- `ingest_procurement.py` loads header-level rows from `data/2026_diavgeia_filtered.csv`
- `ingest_procurement_lines.py` expands line-level detail from:
  - `spending_contractors_details`
  - `payment_beneficiaries_details`
  - `commitment_lines_details` (with fallback from `commitment_*` columns when line details are missing)
  - `direct_people_details` + `direct_value`
- `ingest_org_municipality_coverage.py` loads `data/mappings/org_to_municipality_coverage.csv`
  into `public.org_municipality_coverage` (truncate + reload)

### Org coverage mapping (many-to-many org -> municipality)

The project now keeps a dedicated coverage mapping for organizations that affect
multiple municipalities (e.g. regions, decentralized administrations, syndicates,
development organizations, national bodies).

- Source CSV: `data/mappings/org_to_municipality_coverage.csv`
- DB table: `public.org_municipality_coverage`

This is different from `data/mappings/org_to_municipality.csv`:
- `org_to_municipality.csv` is a header-level / best single-match mapping
- `org_to_municipality_coverage.csv` stores full coverage (one org -> many municipalities)

Coverage rows are built from:
- deterministic hierarchy rules (`ΠΕΡΙΦΕΡΕΙΑ`, `ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ`, etc.)
- local region reference (`data/mappings/region_to_municipalities.csv`)
- manual municipality lists for special cases
- national-level whole-country expansion (`["*"]`)
- fallback to single `municipality_id` when available in `org_to_municipality.csv`

Important note for direct assignments:
- if one direct assignment has multiple persons but one total amount, the amount is placed only on the first line row to avoid double-counting in aggregates

### Frontend amount behavior (municipality procurement panel)

The frontend (`MunicipalityPanel`) now aggregates procurement amounts per `ADA` from `procurement_decision_lines` and uses that total in the UI.

- header rows still come from `procurement_decisions`
- amounts are summed from line rows when available
- fallback to `procurement_decisions.amount_eur` is used only when no line rows exist

### When you change the filtered dataset scope (example: keep only 2024+)

`ingest_procurement.py` uses UPSERT and does not delete older rows automatically.

If you shrink `data/2026_diavgeia_filtered.csv` (for example to `issueDate >= 2024`), first clear both procurement tables in Supabase, then re-ingest:

```sql
TRUNCATE TABLE
  public.procurement_decision_lines,
  public.procurement_decisions
RESTART IDENTITY;
```

Then rerun:

```bash
python ingest/ingest_procurement.py
python ingest/ingest_procurement_lines.py
python ingest/ingest_org_municipality_coverage.py
```

## Dataset scope note (local files)

- `data/2026_diavgeia.csv` currently stores only records with `issueDate >= 2024`
- `data/2026_diavgeia_filtered.csv` is also kept at `issueDate >= 2024`
- a local archival copy of the full raw range was saved as `data/2026_diavgeia_from_2009.csv` (ignored in git)
