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
