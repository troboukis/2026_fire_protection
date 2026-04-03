# Municipal Fire Protection Funding Data

Place the following file in this directory.

---

## municipal_funding.csv

Yearly central government allocations to municipalities for fire protection purposes.

**Likely sources:**
- Ministry of Interior (Υπουργείο Εσωτερικών) — ΚΑΠ (Κεντρικοί Αυτοτελείς Πόροι) breakdown
- Ministry of Civil Protection (Γενική Γραμματεία Πολιτικής Προστασίας) — annual program allocations
- Diavgeia itself — search for decisions of type ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ at national level with keywords πυροπροστασία directed at municipalities
- Program "Αντώνης Τρίτσης" or similar EU co-financed programs with fire protection components

**Scope note:** Be explicit about what this dataset covers.
Options (choose one and document it in the methodology page):
- Fire-protection-specific transfers only (most defensible)
- General municipal development fund (ΠΔΕ/ΣΑΤΑ) that could include fire protection (broader but less precise)

---

## Required columns

| Column | Type | Description |
|---|---|---|
| `municipality_name` | string | Municipality name in Greek |
| `municipality_id` | string/int | Municipality id (for join) |
| `year` | integer | Fiscal year |
| `amount_allocated` | float | Amount allocated in euros |
| `program_name` | string | Funding program name/source |
| `source` | string | Document reference or data source |

**Expected filename:** `municipal_funding.csv`

---

## Notes

- If per-municipality breakdown is unavailable, regional-level totals are a fallback
- Multiple rows per municipality per year are fine (one row per program/source)
- The ingestion script will aggregate to municipality-year level for the accountability metric

## Maintenance note

Whenever a new PDF is added to [data/funding](/Users/troboukis/Code/fire_protection_2026/data/funding):

- Add it to `DOC_SPECS` in [build_fund_csv.py](/Users/troboukis/Code/fire_protection_2026/scripts/build_fund_csv.py)
- Set the correct parser and expected row count / total amount from the source document
- Rebuild [municipal_funding.csv](/Users/troboukis/Code/fire_protection_2026/data/funding/municipal_funding.csv)
- Check that municipality rows resolve to DB `municipality_key` values from [municipality.csv](/Users/troboukis/Code/fire_protection_2026/backupDB/municipality.csv)
- Check that syndesmos rows resolve to DB `organization_key` values from [organization.csv](/Users/troboukis/Code/fire_protection_2026/backupDB/organization.csv)
- If a new PDF introduces name variants or truncated names, extend the override maps in [build_fund_csv.py](/Users/troboukis/Code/fire_protection_2026/scripts/build_fund_csv.py) before ingesting to Supabase
