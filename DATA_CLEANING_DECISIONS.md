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

### 6. KIMDIS contract chains (`prevReferenceNo` / `nextRefNo`) and double counting

- Πρόβλημα:
- Το KIMDIS raw feed περιέχει αλυσίδες συμβάσεων όπου μια παλιότερη σύμβαση τροποποιείται / επεκτείνεται / αντικαθίσταται από νέα σύμβαση με νέο `referenceNumber`.
- Αν αθροίζονται όλα τα rows όπως είναι, γίνεται double counting στα ποσά και inflated `contract_count`.
- Απόφαση:
- Δεν αλλάζουμε το raw `data/raw_procurements.csv`.
- Στο ingest layer μηδενίζεται το `amount_without_vat` για superseded συμβάσεις:
- όταν ένα `referenceNumber` εμφανίζεται ως `prevReferenceNo` σε νεότερο row
- όταν το ίδιο το row έχει μη κενό `nextRefNo`
- Στα frontend RPCs, οι superseded συμβάσεις εξαιρούνται και από τα counts/lists:
- exclude αν `next_ref_no IS NOT NULL`
- exclude αν υπάρχει άλλο procurement με `prev_reference_no = reference_number`
- Scope:
- ingest / DB / frontend
- Υλοποίηση:
- `ingest/stage2_load_erd.py`
- `sql/contracts_page_rpc.sql`
- `sql/021_region_contracts_rpc.sql`
- `sql/024_municipality_contracts_rpc.sql`
- `sql/featured_records_rpc.sql`
- `sql/hero_section_rpc.sql`

### 7. Regional organizations must remain `organization` entities

- Πρόβλημα:
- Regional organizations όπως ο `ΑΟΠΙΝ` πρέπει να εμφανίζονται ως organizations με `authority_scope='region'`, όχι να υποβιβάζονται σε canonical region rows.
- Όταν λείπει το `organization_key`, τα RPCs πέφτουν σε region fallback labels όπως `ΠΕΡΙΦΕΡΕΙΑ ΙΟΝΙΩΝ ΝΗΣΩΝ`.
- Απόφαση:
- Στο organization seeding αποκλείονται μόνο canonical δήμοι / canonical περιφέρειες.
- Δεν αποκλείονται organizations μόνο και μόνο επειδή το `authority_scope` τους είναι `region` ή `decentralized`.
- Scope:
- ingest / DB / frontend display
- Υλοποίηση:
- `ingest/stage2_load_erd.py` (`seed_organization_rows`)
- `scripts/backfill_procurement_geography.py`

### 8. One raw amount column for every Diavgeia decision type

- Πρόβλημα:
- Το `public.diavgeia.spending_contractors_value` τροφοδοτούνταν μόνο από τις `ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ`, ενώ το raw CSV περιέχει ποσά και για αναλήψεις υποχρέωσης, αναθέσεις και οριστικοποιήσεις πληρωμής.
- Απόφαση:
- Η βάση διατηρεί μία μόνο στήλη ποσού, την υπάρχουσα `spending_contractors_value TEXT`.
- Η επιχειρησιακή σημασία του ποσού προκύπτει από το `diavgeia_document_type_decision_uid`.
- Το raw ποσό διατηρείται ως string χωρίς μετατροπή, άθροιση ή αλλαγή των διαχωριστικών. Οι λίστες πολλαπλών ποσών διατηρούνται ως raw string.
- Ο loader αντιγράφει τη μοναδική μη κενή τιμή από τις εξής raw πηγές:
- `spending_contractors_value`
- `commitment_amount_with_vat`
- `direct_value`
- `payment_value`
- Scope:
- raw enrichment / ingest / DB backfill
- Υλοποίηση:
- `src/diavgeia_amounts.py`
- `ingest/stage2_load_erd.py` (`diav_rows`)
- `scripts/backfill_diavgeia_amounts.py`

### 9. KIMDIS contract amount override for `26SYMV019592516`

- Problem:
- The KIMDIS contract API returns `29,920,000.00` without VAT and `37,100,800.00` with VAT, exactly x1000.
- The signed contract and the related award `26AWRD019555701` confirm `29,920.00` without VAT and `37,100.80` with VAT.
- Decision:
- Keep `data/raw_procurements.csv` unchanged.
- Apply a reference-specific correction from a reviewed override registry before contract-chain deduplication.
- Scope:
- ingest / DB
- Implementation:
- `data/mappings/procurement_amount_overrides.csv`
- `ingest/stage2_load_erd.py` (`apply_procurement_amount_overrides`)
- Date:
- 2026-08-07

### 10. Fire safety of buildings and facilities is out of scope

- Problem:
- The Diavgeia keyword search ("πυροπροστασία") also returns the fire safety of buildings: certificates and studies for nurseries and schools, hospital fire systems, extinguishers of municipal buildings. The existing subject exclusions (`REQUESTED_EXCLUDE_KEYWORDS`) did not catch wordings such as «πιστοποιητικό πυροπροστασίας», «σχολ. μονάδες», «παιδικός σταθμός» or «νοσοκομείο».
- Decision:
- Keep such records out of ingestion, for both Diavgeia decisions and KIMDIS contracts. Text about outdoor work, vegetation, settlements or natural areas keeps a record even when a building is mentioned.
- Delete the 79 payment finalizations of 2026 already in the database that match the rule (their beneficiary rows and contract links were deleted with them). Existing contracts were not deleted.
- Scope:
- fetch / ingest / DB / reporting
- Implementation:
- `src/keyword_exclusions.py` (`building_fire_safety_match`, `is_building_fire_safety`)
- `src/fetch_diavgeia.py` (search filter, `filter_excluded_org_rows`), `ingest/stage2_load_erd.py` (`diav_rows`), `src/fetch_kimdis_procurements.py` (`ProcurementCollector.is_excluded`)
- `scripts/delete_diavgeia_building_fire_safety.py` (dry run by default, JSON backup before `--apply`); backup of the 2026-10-19 deletion: `backupDB/diavgeia-building-fire-safety-20261019-152128-before.json` (local, not in git)
- `scripts/reports/yearly-report-2026/build_yearly_report.py` applies the same rule to records that predate it
- Date:
- 2026-10-19

Update, 2026-10-20:
- The separate building-fire-safety heuristics described above were removed at the user's request, including their building, facility/body, and outdoor-exception lists. The dedicated building-cleanup script is retired and stops without connecting to the database.
- Keyword exclusions now have one source: `DEFAULT_EXCLUDE_KEYWORDS` in `src/keyword_exclusions.py`, used by both collectors and stage2 ingestion. It merges the existing keyword catalogs, removes normalized duplicates, and preserves the existing school and other explicit exclusions.
- All keyword phrases use ordered token prefixes with up to two intervening words between consecutive roots. This also applies to the former KIMDIS-only phrases that previously required adjacent words.
- Collection review is performed into a separate `exports/` directory, retaining raw API candidates, accepted/rejected records and the matching terms. These code changes do not apply any database cleanup.

Update, 2026-10-21:
- At the user's request, the former KIMDIS-only phrases again require adjacent words. The original common root terms retain their allowance of up to two intervening words. Both settings live beside their terms in the single `DEFAULT_EXCLUDE_KEYWORDS` policy in `src/keyword_exclusions.py`, shared by collection and ingestion.
- The previously approved removal of `αναδασωσ` is preserved. No database cleanup was run.

### 11. Anogeia deadline extensions without KIMDIS chain metadata

- Problem: the 2024-onward backfill for CPV `77231000-8` returned the original contract `25SYMV016668307` and two deadline-extension decisions, `25SYMV017249656` and `25SYMV017327213`, each repeating the same EUR 8,064.52 net value while leaving `prevReferenceNo` / `nextRefNo` empty.
- Evidence: the [original signed contract](https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/25SYMV016668307), protocol 2778/15-04-2025, fixes the total at EUR 10,000 including VAT. The [committee decision](https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/25SYMV017249656) and [mayor decision](https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/25SYMV017327213) only extend the deadline to 30/05/2025 and add no contract value.
- Decision: retain the original contract's net value and set the two extension rows' derived `amount_without_vat` to zero through the reviewed override registry. The zero is an accounting exclusion from totals, not a claim that the original contract had no value. Preserve raw API amounts, gross values, and the source's empty chain fields.
- Scope: ingest / DB; only newly collected records. Existing database records are unchanged.
- Implementation: `data/mappings/procurement_amount_overrides.csv`, applied by `ingest/stage2_load_erd.py` before chain deduplication.
- Date: 2026-10-20.

### 12. Additional KIMDIS categories with explicit fire-title evidence

- Problem: reviewed Diavgeia references identified fire-protection contracts under broad KIMDIS categories absent from collection.
- Decision: add the following as secondary categories, requiring explicit fire-related wording in the contract title even when a primary CPV is also present:
  - Υπηρεσίες εκπόνησης τεχνικών μελετών (`71320000-7`).
  - Μηχανήματα χωματουργικών εργασιών (`43210000-8`).
  - Καθαρισμός και επεξεργασία εδάφους (`90522400-6`).
  - Λειτουργία χώρου απόθεσης απορριμμάτων (`90530000-1`).
  - Κάμερες ασφαλείας (`35125300-2`).
  - Γεωργικά μηχανήματα (`16000000-5`).
  - Εξοπλισμός πυρόσβεσης (`35110000-8`).
  - Υπηρεσίες επιτήρησης (`79714000-2`).
- General civil-protection wording alone does not qualify these categories. The existing cancellation, organization, school, green-maintenance and building-fire-safety exclusions continue to apply.
- The backfill exposed fire safety of hangars, generic alarm systems, port installations, a police headquarters, wastewater-treatment installations and underground parking. The existing facility exclusion now recognizes those descriptions and the Greek stem `λιμεν`; explicit forest/outdoor context retains its existing priority.
- Four explicitly approved abbreviated titles have reviewed exceptions: `24SYMV015942964`, `26SYMV019097903`, `26SYMV019305098`, `26SYMV019416651`. Each exception requires the exact reference, normalized title, complete CPV set and a matching description phrase. It bypasses only the title gate. The vegetation-cutting equipment section I qualifies; section IV `26SYMV019305186` remains excluded.
- Scope: collection / 2024-onward backfill / ingestion. The other 17 missing references reviewed on 2026-10-20 remain excluded.
- Implementation: `src/fetch_kimdis_procurements.py` and `data/mappings/kimdis_reviewed_title_overrides.json`.
- Date: 2026-10-20.

## Open / Next

- Προαιρετικά: αποθήκευση `amount_raw_text`, `amount_corrected`, `correction_reason` στη βάση.

## How To Update This File

- Πρόσθεσε νέα απόφαση με:
- `Problem`
- `Decision`
- `Scope` (raw / filtered / ingest / DB / frontend)
- `Implementation` (file/function)
- `Date`
