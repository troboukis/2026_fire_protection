# Forest Fire Incidents Data

## Copernicus synchronization

`src/fetch_copernicus.py` fetches the complete requested country/date window on
every run. EFFIS can remove or replace perimeter IDs and change area/geometry
without advancing `lastupdate`, so a timestamp lookback cannot keep this dataset
in sync. The existing `--full-refresh` and `--lookback-days` arguments remain
accepted for compatibility; all runs now use a full snapshot.

Before writing, the collector validates pagination counts, unique IDs, country,
UTC dates, areas and geometry. Empty, partial or invalid snapshots fail without
changing the database. Municipality processing must preserve every source ID.
Updates, municipality links, and removal of IDs absent from the source run in one
transaction. Removal is limited to the requested country and UTC date window.

Every sync first saves and verifies a complete compressed JSON backup of
`copernicus` and `copernicus_municipality` under `backupDB/copernicus/` (override
with `--backup-dir`). GitHub Actions preserves these backups for 30 days. A failed
backup or transaction leaves the previous database data in place; because the
workflow's collection step uses `continue-on-error`, later stages can still read
that previous data. The collector makes one request per page with a 30-second
timeout and no automatic retries; a subsequent run starts a new full snapshot.

For example, to reconcile only the 2026 Greece window:

```sh
python src/fetch_copernicus.py --start-date 2026-01-01 --end-date 2026-10-22
```

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
