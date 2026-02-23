# Data Cleaning Decisions

Καταγραφή αποφάσεων για καθαρισμό/διόρθωση δεδομένων ώστε να υπάρχει audit trail.

## Principles

- `raw` source data (`data/2026_diavgeia.csv`) παραμένει ανέγγιχτο όταν το λάθος προέρχεται από τη Διαύγεια.
- Οι διορθώσεις εφαρμόζονται σε derived layers (π.χ. filtered CSV, ingest προς DB, reporting).
- Κάθε διόρθωση πρέπει να είναι τεκμηριωμένη και αναπαραγώγιμη.

## Decisions

### 1. Diavgeia amount data-entry errors (x100 anomalies)

- Παρατηρήθηκαν περιπτώσεις όπου ποσά στη Διαύγεια είναι καταχωρημένα με λάθος (πιθανό x100), π.χ.:
- `8.755,24` εμφανίζεται ως `875.524,00`
- `58.752,94` εμφανίζεται ως `5.875.294,00`
- Απόφαση:
- Δεν αλλάζουμε το raw `data/2026_diavgeia.csv`.
- Διορθώσεις θα γίνονται μόνο σε derived layer / ingest με τεκμηριωμένο override ή validator-assisted review.
- Κατάσταση:
- Υπάρχει validator εργαλείο για σύγκριση raw ποσού με parsed PDF text (`src/validate_diavgeia_amounts.py`).

### 2. Amount validation against PDF text

- Δημιουργήθηκε notebook-friendly validator για έλεγχο ποσών από raw CSV έναντι parsed PDF text.
- Αρχείο:
- `src/validate_diavgeia_amounts.py`
- Σκοπός:
- Exact string match στο PDF text
- Αν όχι, αναζήτηση candidate ποσών με ίδια digits και διαφορετικούς separators
- Σημείωση:
- `pdf_lookup` είναι `dict[str, str]` (`ADA -> parsed PDF text`), όχι DataFrame.

### 3. Commitment amounts missing when line items are empty

- Πρόβλημα:
- Σε `ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ`, κάποια records έχουν άδειο `Ποσό και ΚΑΕ/ΑΛΕ`, αλλά διαθέτουν `Συνολικό ποσό`.
- Απόφαση:
- Στο enrichment extraction για commitments, γίνεται fallback στο `Συνολικό ποσό -> Ποσό` όταν λείπουν line items.
- Υλοποίηση:
- `src/fetch_diavgeia.py` (`extract_commitment_fields_from_decision`)

### 4. Pandas dtype safety during backfill enrichment

- Πρόβλημα:
- Backfill crash από pandas/pyarrow dtypes όταν γίνονται row assignments με lists/dicts/strings.
- Απόφαση:
- Πριν τα enrichment writes, τα enrichment columns γίνονται cast σε `object`.
- Υλοποίηση:
- `src/fetch_diavgeia.py` (`coerce_columns_to_object`)

### 5. Excluded organizations by `org_name_clean` (domain scope cleanup)

- Πρόβλημα:
- Ορισμένοι φορείς (π.χ. πανεπιστήμια/νοσοκομεία/πολιτιστικοί φορείς) εμφανίζονται από keyword hits αλλά είναι εκτός του επιθυμητού scope της ανάλυσης.
- Απόφαση:
- Διατηρούμε explicit exclusion list με βάση `org_name_clean`.
- Οι γραμμές αυτές αφαιρούνται από τα datasets και στο εξής απορρίπτονται κατά το fetch/save flow.
- Scope:
- `raw` / `filtered` CSVs και μελλοντικά fetch batches
- Υλοποίηση:
- `src/fetch_diavgeia.py` (`EXCLUDED_ORG_NAME_CLEAN_RAW`, `filter_excluded_org_rows`, `record_has_excluded_org`)

## Open / Next

- Προσθήκη manual override registry για ποσά (π.χ. `ADA -> corrected amount`) σε separate CSV/YAML.
- Εφαρμογή overrides στο ingest προς `procurement_decisions.amount_eur`.
- Προαιρετικά: αποθήκευση `amount_raw_text`, `amount_corrected`, `correction_reason` στη βάση.

## How To Update This File

- Πρόσθεσε νέα απόφαση με:
- `Problem`
- `Decision`
- `Scope` (raw / filtered / ingest / DB / frontend)
- `Implementation` (file/function)
- `Date`
