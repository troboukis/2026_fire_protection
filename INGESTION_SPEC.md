# Ingestion Spec (Table-by-Table)

This file is the source of truth for how data is ingested into the database.

Rule from March 1, 2026:
- For each table instruction you provide:
1. code is updated first
2. this file is updated second

Rule from March 2, 2026:
- Ingestion is incremental by default.
- Reruns must not create duplicates.
- Full reingest is only after DB reset + running `sql/001_schema_erd.sql`.

## Code Location

Current main loader:
- `ingest/stage2_load_erd.py`

Main class:
- `CsvBundle` (holds source DataFrames)

CLI control:
- `--tables` for explicit table-by-table execution.

Shared parsing helpers:
- `t`, `t_up`, `b`, `i`, `dec`, `date_iso`, `ts_iso`, `first_list_item`

---

## Table: `cpv`

Status:
- Implemented according to latest instruction.

Source:
- `data/raw_procurements.csv`
- Canonical CPV dictionary from `src/fetch_kimdis_procurements.py`:
  - `DEFAULT_CPVS` (fallback only), built as:
    - fire-protection seed CPVs
    - plus `data/mappings/cpv_dictionary_extended.csv` (overrides on key overlap)

Ingestion behavior:
1. Insert procurement rows first and capture `procurement.id`.
2. For each raw procurement row, split `cpv_keys` by `|`.
3. Split `cpv_values` by `|` from the same row.
4. For each split CPV key (same positional index as `cpv_values`):
   - insert one row in `cpv` with:
     - `procurement_id` = inserted procurement id
     - `cpv_key` = split key
     - `cpv_value` = value from raw `cpv_values` at the same index (primary source)
     - fallback to `DEFAULT_CPVS[cpv_key]` only when raw value is missing/empty
5. If CPV value is missing in both raw `cpv_values` and `DEFAULT_CPVS`:
   - row is still inserted with `cpv_value = NULL`
   - key is logged as missing (count + sample)
6. Upsert rule:
   - unique key: `(procurement_id, cpv_key)`
   - on conflict: update `cpv_value`

Responsible code:
- Import: `DEFAULT_CPVS` loaded from `src/fetch_kimdis_procurements.py` (fallback map)
  - builder uses `_load_extended_cpvs()` and `EXTENDED_CPV_CSV`
- Function: `main()` in `ingest/stage2_load_erd.py`
  - CPV insert block: `INSERT INTO public.cpv (cpv_key, cpv_value, procurement_id)`

---

## Table: `procurement`

Status:
- Implemented, pending your table-by-table review.

Source:
- `data/raw_procurements.csv`
- Mapping assist: `data/mappings/org_to_municipality.csv`
- Organization dictionary: `data/mappings/final_entity_mapping_expanded.csv`

Ingestion behavior (organization key):
1. `organization` table is seeded from `final_entity_mapping_expanded.csv` rows where `source_entity_type='organization'`.
2. Each distinct `(source_value, normalized_value)` pair is stored as one row in `organization`:
   - `organization_value` = source variation text
   - `organization_normalized_value` = mapped normalized value
   - `organization_key` = deterministic key generated from `organization_normalized_value`
3. This means one `organization_key` can appear in multiple `organization` rows (one row per observed variation).
4. During procurement ingest, loader reads `raw_procurements.organization_value`.
5. It resolves `organization_key` by exact uppercase-normalized match against seeded `organization_value` / `organization_normalized_value`.
6. Resolved `organization_key` is stored in `procurement.organization_key`.
7. Incremental identity check before insert:
   - identity priority: `reference_number` -> `diavgeia_ada` -> `(contract_number, organization_key)` -> fallback `(organization_key, submission_at, title)`
   - if identity already exists, row is not reinserted; existing `procurement.id` is reused for dependent tables.
8. Organization seed exclusions:
   - canonical municipality entities are excluded from `organization`
   - canonical region entities are excluded from `organization`
   - regional / decentralized organizations remain in `organization` with their `authority_scope`
   - example: `ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ... (ΑΟΠΙΝ) Α.Ε.` must remain an organization row with `authority_scope='region'`

Responsible functions:
- `build_maps()`
- `seed_organization_rows()`
- `build_organization_lookup()`
- `procurement_rows()`
- `main()` (insert loop with `RETURNING id`)

---

## Table: `diavgeia`

Status:
- Implemented, pending your table-by-table review.

Source:
- `data/2026_diavgeia.csv`
- Mapping assist: `data/mappings/org_to_municipality.csv`
- Municipality aliases: `data/mappings/final_entity_mapping_expanded.csv`
- Organization dictionary: `data/mappings/final_entity_mapping_expanded.csv`

Ingestion behavior:
1. Loader resolves `region_key`, `organization_key`, and `municipality_key` with `resolve_diavgeia_context()`.
2. Exact organization mapping from `org_to_municipality.csv` has priority.
3. For direct municipality authorities (`org_type = 'ΔΗΜΟΣ'`), unresolved names fall back to the canonical municipality lookup built from `final_entity_mapping_expanded.csv`.
4. Resolved `organization_key` is stored in `diavgeia.organization_key`.
5. Incremental behavior:
   - rows with non-null `ada`: upsert on `ada`
   - rows with null `ada`: insert with `NOT EXISTS` guard to avoid rerun duplicates.
6. `diavgeia.spending_contractors_value` is the single DB-facing raw amount field for every supported decision type. The loader copies, without numeric normalization, the one non-empty amount from these raw source columns:
   - `spending_contractors_value` (`ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ`)
   - `commitment_amount_with_vat` (`ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ`)
   - `direct_value` (`ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ`)
   - `payment_value` (`ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ`)
   The raw dataset currently has exactly one populated amount source per row. The decision type remains in `diavgeia_document_type_decision_uid`, which defines the amount's business meaning.

Responsible functions:
- `resolve_diavgeia_context()`
- `diav_rows()`
- `main()` (`INSERT ... ON CONFLICT (ada) DO UPDATE`)

---

## Table: `payment`

Status:
- Implemented from `raw_procurements.csv` per latest instruction.

Source:
- `data/raw_procurements.csv`

Ingestion behavior:
1. A payment row is created per inserted procurement row.
2. `payment.procurement_id` is set to the inserted `procurement.id`.
3. Field mapping from raw CSV:
   - `signers` <- `signers`
   - `beneficiary_name` <- `firstMember_name`
   - `beneficiary_vat_number` <- `firstMember_vatNumber`
   - `amount_with_vat` <- `totalCostWithVAT`
   - `amount_without_vat` <- `totalCostWithoutVAT`
   - `fiscal_year` <- year extracted from `submissionDate`
   - `budget_category` <- `fundingDetails_regularBudget`
   - `counter_party` <- `firstMember_name`
   - `payment_ref_no` <- `paymentRefNo`
   - `kae_ale` <- `NULL` (not present in `raw_procurements.csv`)
4. Upsert rule:
   - unique key: `payment.procurement_id`
   - on conflict: update all mapped payment fields.
5. Contract-chain deduplication for amount metrics:
   - raw `data/raw_procurements.csv` remains unchanged
   - in the ingest dataframe, `totalCostWithoutVAT` is treated as zero for superseded contracts:
     - if `referenceNumber` appears in another row's `prevReferenceNo`
     - if the row itself has non-empty `nextRefNo`
6. After payment upsert, `procurement.payment_id` is backfilled from `payment.id`.
7. Incremental cleanup:
   - after payment upsert, loader zeroes `payment.amount_without_vat` only for affected references in the current batch
   - affected refs are:
     - the new rows' `prevReferenceNo`
     - the new rows' own `referenceNumber` when they have `nextRefNo`

Responsible functions:
- `payment_row_from_raw()`
- `apply_procurement_chain_dedup()`
- `affected_reference_numbers_for_row()`
- `zero_superseded_payment_amounts()`
- `main()` (payment upsert + procurement backfill)

---

## Table: `forest_fire`

Status:
- Implemented, pending your table-by-table review.

Source:
- `data/fires/fire_incidents_unified.csv`

Incremental behavior:
- Existing `forest_fire` business-key tuples are loaded from DB.
- Only non-existing rows are inserted on rerun.

Responsible functions:
- `forest_fire_rows()`
- `main()`

---

## Table: `fund`

Status:
- Implemented, pending your table-by-table review.

Source:
- `data/funding/municipal_funding.csv`

Incremental behavior:
- Existing `fund` business-key tuples are loaded from DB.
- Only non-existing rows are inserted on rerun.

Responsible functions:
- `fund_rows()`
- `main()`

---

## Table: `news_fires`

Status:
- Implemented.

Source:
- News listing pages from Καθημερινή, News247, in.gr, ΕφΣυν, and Πρώτο Θέμα.
- Responsible script: `src/fetch_news_fires.py`.
- Runner/workflow:
  - `scripts/run_news_fires_and_push_state.sh`
  - `.github/workflows/news-fires-refresh.yml`

State:
- `logs/news_fires_state.json`
- The state tracks the newest listing article boundary per source and the successful run's `seen_urls`.
- The state file is tracked so GitHub Actions can restore the incremental boundary from checkout.

Ingestion behavior:
1. Start from the first listing page for each source.
2. Paginate only until the saved boundary article is found; `MAX_LISTING_PAGES` is a safety cap.
3. Apply title filtering before article-body scraping.
4. Fetch article body only for title matches.
5. Apply secondary body filtering to keep only fire-related articles.
6. Extract one geographic fire area with an LLM.
7. Geocode the area with Google Geocoding.
8. Map coordinates to a municipality by point-in-polygon.
9. Upsert one row per article into `public.news_fires`.

Stored fields:
- `id`
- `article_title`
- `source`
- `article_url`
- `image_url`
- `published_at`
- `scraped_at`
- `municipality_key`
- `municipality_name`
- `area`
- `geocode_query`
- `lat`
- `lon`

Uniqueness / rerun behavior:
- Unique key: `article_url`.
- Reruns must not create duplicates.
- `source` is stored as its configured display text (`Καθημερινή`, `News247`, `in.gr`, `ΕφΣυν`, `Πρώτο Θέμα`).
- If geocoding or municipality matching fails, keep the article row without coordinates/municipality fields when the article itself is valid.

Frontend API:
- `get_news_ticker_articles()` returns frontend-ready ticker rows.
- The RPC returns all today's articles when today has at least five articles; otherwise it returns the latest ten articles overall.
- The RPC joins active `public.current_fires` rows by municipality and returns `active_fire_incident_key` when an article refers to a municipality with an active fire.

Responsible code:
- `src/fetch_news_fires.py`
- `sql/055_create_news_fires.sql`
- `sql/059_news_ticker_articles_rpc.sql`
- `sql/900_frontend_api_setup.sql`

---

## Table: `diavgeia_procurement` (bridge)

Status:
- Implemented, pending your table-by-table review.

Link rule:
- primary link: `procurement.diavgeia_ada = diavgeia.ada`
- current practical note:
  - some KIMDIS rows do not populate `diavgeia_ada`
  - observed matches can also exist on `decision_related_ada` / `contract_related_ada`
  - bridge quality therefore depends on which ADA field is populated in the raw source

Responsible code:
- SQL block in `main()`

---

## Table: `diavgeia_beneficiary` (bridge)

Status:
- Implemented, pending your table-by-table review.

Source:
- Beneficiary AFM/name fields from `data/2026_diavgeia.csv`

Responsible code:
- `first_list_item()`
- SQL blocks in `main()` for `beneficiary` and `diavgeia_beneficiary`

---

## Seeded Key Tables (minimal seed)

Tables:
- `region`
- `municipality`
- `organization`
- `diavgeia_document_type`

### `region` seed behavior
- Source files:
  - `data/mappings/final_entity_mapping_expanded.csv`
  - `data/mappings/region_to_municipalities.csv`
  - `data/mappings/org_to_municipality.csv`
- One row is stored per `(region_key, region_value)` pair.
- `region_key` is the stable region code/text (`region_id`).
- `region_value` is the observed variant label.
- `region_normalized_value` is normalized uppercase value used for lookups.
- Multiple rows can share the same `region_key`.
- Upsert key: `(region_key, region_value)`.

### `municipality` seed behavior
- Source files:
  - `data/mappings/final_entity_mapping_expanded.csv`
  - `data/mappings/region_to_municipalities.csv`
  - fallback keys from `org_to_municipality`, fire, funding files
- One row is stored per `(municipality_key, municipality_value)` pair.
- `municipality_key` is the stable municipality code (`municipality_id`).
- `municipality_value` is the observed variant label.
- `municipality_normalized_value` is normalized uppercase value used for lookups.
- Multiple rows can share the same `municipality_key`.
- Upsert key: `(municipality_key, municipality_value)`.

### Key resolution during fact ingest (`procurement`, `diavgeia`)
- Before inserts, loader builds:
  - `region_lookup`: value/normalized/key -> `region_key`
  - `municipality_lookup`: value/normalized/key -> `municipality_key`
- Then:
  - `procurement_rows()` first gets mapped ids from `org_to_municipality`
  - `diav_rows()` first gets mapped ids from `org_to_municipality`; for `org_type = 'ΔΗΜΟΣ'`, it then falls back to direct municipality-name lookup
  - those raw ids are resolved through lookups
  - stored keys are stable (`region_key`, `municipality_key`)

Responsible functions:
- `seed_region_rows()`
- `seed_municipality_rows()`
- `build_region_lookup()`
- `build_municipality_lookup()`
- `seed_organization_rows()`
- `build_organization_lookup()`
- `main()` (decision type seed)
