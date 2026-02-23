# Data Mappings

This directory contains lookup/crosswalk tables used during ingestion.

---

## org_to_municipality.csv

The critical mapping that connects Diavgeia organization names (`org_name_clean` from
`2026_diavgeia_filtered.csv`) to municipality ids in the geographic dataset.

This is the most important file in this directory. Without it, procurement data cannot
be placed on the map.

**How to build it:**
1. Extract unique `(org_type, org_name_clean)` pairs from `data/2026_diavgeia_filtered.csv`
2. For `org_type == "Δήμος"`: match `org_name_clean` to municipality names in `data/geo/municipalities.geojson`
3. For `org_type == "Περιφέρεια"`: map to the list of municipalities within that region
4. For national authorities (Υπουργείο etc.): leave `municipality_id` null — these are attributed at national level

**Columns:**

| Column | Type | Description |
|---|---|---|
| `org_type` | string | From `2026_diavgeia_filtered.csv` |
| `org_name_clean` | string | From `2026_diavgeia_filtered.csv` |
| `municipality_id` | string/int | Matched municipality id (null for regional/national) |
| `region_id` | string/int | Matched region id (null for national) |
| `authority_level` | string | `municipality` / `region` / `decentralized` / `national` |
| `match_method` | string | `exact` / `fuzzy` / `manual` / `unmatched` |
| `notes` | string | Any disambiguation notes |

**Expected filename:** `org_to_municipality.csv`

---

## municipality_name_crosswalk.csv

Maps historical municipality names (pre-Kleisthenis / Kallikratis era) to current names.
Needed because fire incident data from 2000–2010 may use older boundary names.

**Columns:**

| Column | Type | Description |
|---|---|---|
| `historical_name` | string | Name used in source data |
| `current_municipality_id` | string/int | Current municipality id |
| `current_name` | string | Current official name |
| `reform` | string | `kallikratis` or `kleisthenis` |

**Expected filename:** `municipality_name_crosswalk.csv`
