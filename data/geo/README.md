# Geographic Data

Place the following files in this directory.

---

## municipalities.geojson

Greek municipality boundaries (Kleisthenis reform, current boundaries).

**Source:** geodata.gov.gr — search for "Καλλικράτης" or "Κλεισθένης" administrative boundaries.
Alternative: OKXE (Οργανισμός Κτηματολογίου και Χαρτογραφήσεων Ελλάδος).

**Required properties on each Feature:**
- `municipality_id` — unique integer or string id
- `name` — municipality name in Greek (e.g. `ΔΗΜΟΣ ΑΡΤΑΙΩΝ`)
- `name_en` — transliterated name in Latin characters (optional but useful)
- `region` — region name (Περιφέρεια)
- `regional_unit` — regional unit name (Περιφερειακή Ενότητα)
- `population` — latest census population (optional)
- `forest_coverage_ha` — forested area in hectares (optional, from Corine Land Cover)

**Expected filename:** `municipalities.geojson`

---

## regions.geojson

Greek regional boundaries (13 Περιφέρειες + Αττική).

**Source:** Same as above — geodata.gov.gr administrative boundaries.

**Required properties:**
- `region_id`
- `name` — region name in Greek
- `name_en`

**Expected filename:** `regions.geojson`

---

## Notes

- Coordinate system: WGS84 (EPSG:4326)
- Simplify geometry for web use (tippecanoe or mapshaper) — full cadastral precision is unnecessary and will slow map rendering
- Recommended simplification: tolerance ~0.001 degrees for municipality level
