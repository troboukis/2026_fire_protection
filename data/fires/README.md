# Forest Fire Incidents Data

Place the following file in this directory.

---

## fire_incidents.csv

Historical forest fire incidents in Greece, 2000–2025.

**Primary source:** EFFIS (European Forest Fire Information System)
- URL: https://effis.jrc.ec.europa.eu/applications/fire-history
- Download: Fire History layer → filter to Greece → export CSV
- Coverage: fires ≥ 30 ha typically; smaller fires may be missing

**Secondary / supplementary sources:**
- Greek Fire Service (Πυροσβεστικό Σώμα) annual reports
- WWF Greece fire database
- Copernicus Emergency Management Service (for major events with burn polygons)

---

## Required columns

| Column | Type | Description |
|---|---|---|
| `fire_id` | string | Unique identifier |
| `year` | integer | Year of fire |
| `date_start` | date (YYYY-MM-DD) | Fire start date |
| `date_end` | date (YYYY-MM-DD) | Fire end date (if known) |
| `municipality_name` | string | Municipality name in Greek (for join to geo data) |
| `municipality_id` | string/int | Municipality id (if available) |
| `regional_unit` | string | Περιφερειακή Ενότητα |
| `region` | string | Περιφέρεια |
| `hectares_burned` | float | Total area burned in hectares |
| `lat` | float | Approximate centroid latitude (WGS84) |
| `lon` | float | Approximate centroid longitude (WGS84) |
| `cause` | string | Cause if known (arson / lightning / unknown / accident) |
| `source` | string | Data source identifier (EFFIS / Πυροσβεστική / etc.) |

Columns not available from your source can be left empty — the ingestion script will handle nulls.

**Expected filename:** `fire_incidents.csv`

---

## Burn area polygons (optional, advanced)

If you can source GeoJSON polygons for major fires (Copernicus EMS or EFFIS burned area layer),
place them as `burn_polygons.geojson`. These enable more accurate municipality-level attribution
for fires that crossed boundaries.
