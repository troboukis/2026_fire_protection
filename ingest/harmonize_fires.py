"""
harmonize_fires.py
------------------
Harmonizes fire incident data from three source formats into a single
unified CSV: data/fires/fire_incidents_unified.csv

Sources:
  - fires_geocoded_final_2000_2018.csv   (English columns, WGS84 lat/lon)
  - forest_fires_2019_geolocated.csv     (Greek columns, WGS84 lat/lng)
  - Dasikes_Pyrkagies_20XX.xls           (Greek columns, 2-row header,
                                          WGS84 X-ENGAGE/Y-ENGAGE)

Output columns:
  fire_id                   unique integer id (sequential)
  year                      integer
  date_start                YYYY-MM-DD
  date_end                  YYYY-MM-DD (nullable)
  nomos                     prefecture name (uppercase, normalized)
  municipality_raw          raw municipality name from source
  area_name                 specific location/area name
  lat                       WGS84 latitude (nullable)
  lon                       WGS84 longitude (nullable)
  burned_forest_stremata    Δάση burned area in stremata
  burned_woodland_stremata  Δασική Έκταση in stremata
  burned_grove_stremata     Άλση in stremata
  burned_grassland_stremata Χορτ/κές Εκτάσεις in stremata
  burned_other_stremata     remaining types (reeds, agricultural, etc.)
  burned_total_stremata     sum of all burned types
  burned_total_ha           total in hectares (stremata / 10)
  source                    source filename

Area units: 1 stremma = 0.1 hectare = 1,000 m²
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO_DIR = Path(__file__).resolve().parent.parent
FIRES_DIR = REPO_DIR / "data" / "fires"
OUTPUT_PATH = FIRES_DIR / "fire_incidents_unified.csv"


def safe_float(val) -> float | None:
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None


def safe_date(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    try:
        return pd.to_datetime(val, dayfirst=True).strftime("%Y-%m-%d")
    except Exception:
        return None


def burned_total(row: dict, cols: list[str]) -> float:
    return sum(float(row.get(c) or 0) for c in cols if safe_float(row.get(c)) is not None)


# ---------------------------------------------------------------------------
# 2000–2018
# ---------------------------------------------------------------------------

def parse_2000_2018() -> pd.DataFrame:
    path = FIRES_DIR / "fires_geocoded_final_2000_2018.csv"
    print(f"[2000-2018] reading {path.name} ...", flush=True)
    df = pd.read_csv(path, low_memory=False)

    area_cols = ["forest", "woodland", "grove", "grassland",
                 "reeds_swamps", "agricultural_land", "crop_residues", "waste_site"]

    rows = []
    for _, r in df.iterrows():
        forest    = safe_float(r.get("forest")) or 0.0
        woodland  = safe_float(r.get("woodland")) or 0.0
        grove     = safe_float(r.get("grove")) or 0.0
        grassland = safe_float(r.get("grassland")) or 0.0
        other     = sum(
            safe_float(r.get(c)) or 0.0
            for c in ["reeds_swamps", "agricultural_land", "crop_residues", "waste_site"]
        )
        total_str = safe_float(r.get("total"))
        if total_str is None:
            total_str = forest + woodland + grove + grassland + other

        lat = safe_float(r.get("latitude"))
        lon = safe_float(r.get("longitude"))
        # Discard (0, 0) coordinates
        if lat == 0.0 and lon == 0.0:
            lat, lon = None, None

        rows.append({
            "year":                      int(r["year"]) if pd.notna(r.get("year")) else None,
            "date_start":                safe_date(r.get("date_start")),
            "date_end":                  safe_date(r.get("date_off")),
            "nomos":                     str(r.get("nomos") or "").strip().upper(),
            "municipality_raw":          str(r.get("municipality") or "").strip(),
            "area_name":                 str(r.get("area") or "").strip(),
            "lat":                       lat,
            "lon":                       lon,
            "burned_forest_stremata":    forest,
            "burned_woodland_stremata":  woodland,
            "burned_grove_stremata":     grove,
            "burned_grassland_stremata": grassland,
            "burned_other_stremata":     other,
            "burned_total_stremata":     total_str,
            "burned_total_ha":           round(total_str / 10, 4),
            "source":                    path.name,
        })

    result = pd.DataFrame(rows)
    print(f"[2000-2018] {len(result)} rows", flush=True)
    return result


# ---------------------------------------------------------------------------
# 2019
# ---------------------------------------------------------------------------

def parse_2019() -> pd.DataFrame:
    path = FIRES_DIR / "forest_fires_2019_geolocated.csv"
    print(f"[2019] reading {path.name} ...", flush=True)
    df = pd.read_csv(path, low_memory=False)

    rows = []
    for _, r in df.iterrows():
        forest    = safe_float(r.get("Δάση")) or 0.0
        woodland  = safe_float(r.get("Δασική Έκταση")) or 0.0
        grove     = safe_float(r.get("Άλση")) or 0.0
        grassland = safe_float(r.get("Χορτ/κές Εκτάσεις")) or 0.0
        other     = sum(
            safe_float(r.get(c)) or 0.0
            for c in ["Καλάμια - Βάλτοι", "Γεωργικές Εκτάσεις",
                      "Υπολλείματα Καλλιεργειών", "Σκουπι-δότοποι"]
        )
        total_str = forest + woodland + grove + grassland + other

        lat = safe_float(r.get("lat"))
        lon = safe_float(r.get("lng"))
        if lat == 0.0 and lon == 0.0:
            lat, lon = None, None

        date_start = safe_date(r.get("Ημερ/νία Έναρξης"))
        year = 2019
        if date_start:
            try:
                year = int(date_start[:4])
            except Exception:
                pass

        rows.append({
            "year":                      year,
            "date_start":                date_start,
            "date_end":                  safe_date(r.get("Ημερ/νία Κατασβεσης")),
            "nomos":                     str(r.get("Νομός") or "").strip().upper(),
            "municipality_raw":          str(r.get("Δήμος") or "").strip(),
            "area_name":                 str(r.get("Περιοχή") or "").strip(),
            "lat":                       lat,
            "lon":                       lon,
            "burned_forest_stremata":    forest,
            "burned_woodland_stremata":  woodland,
            "burned_grove_stremata":     grove,
            "burned_grassland_stremata": grassland,
            "burned_other_stremata":     other,
            "burned_total_stremata":     total_str,
            "burned_total_ha":           round(total_str / 10, 4),
            "source":                    path.name,
        })

    result = pd.DataFrame(rows)
    print(f"[2019] {len(result)} rows", flush=True)
    return result


# ---------------------------------------------------------------------------
# 2020–2024 XLS
# ---------------------------------------------------------------------------

XLS_FILES = [
    (2020, "Dasikes_Pyrkagies_2020.xls"),
    (2021, "Dasikes_Pyrkagies_2021.xls"),
    (2022, "Dasikes_Pyrkagies_2022_v1.7a.xls"),
    (2023, "Dasikes_Pyrkagies_2023_v1.8.xls"),
    (2024, "Dasikes_Pyrkagies_2024.xls"),
]


def parse_xls_year(nominal_year: int, filename: str) -> pd.DataFrame:
    path = FIRES_DIR / filename
    print(f"[{nominal_year}] reading {filename} ...", flush=True)

    # Row 0 is merged group headers, row 1 is actual column names
    df = pd.read_excel(path, sheet_name=0, header=1)
    df = df.dropna(how="all")

    rows = []
    for _, r in df.iterrows():
        forest    = safe_float(r.get("Δάση")) or 0.0
        woodland  = safe_float(r.get("Δασική Έκταση")) or 0.0
        grove     = safe_float(r.get("Άλση")) or 0.0
        grassland = safe_float(r.get("Χορτ/κές Εκτάσεις")) or 0.0
        other     = sum(
            safe_float(r.get(c)) or 0.0
            for c in ["Καλάμια - Βάλτοι", "Γεωργικές Εκτάσεις",
                      "Υπολλείματα Καλλιεργειών", "Σκουπι-δότοποι"]
        )
        total_str = forest + woodland + grove + grassland + other

        # X-ENGAGE = longitude, Y-ENGAGE = latitude (WGS84 decimal degrees)
        lon = safe_float(r.get("X-ENGAGE"))
        lat = safe_float(r.get("Y-ENGAGE"))
        if lat is not None and lon is not None and (lat == 0.0 or lon == 0.0):
            lat, lon = None, None

        date_start = safe_date(r.get("Ημερ/νία Έναρξης"))
        year = nominal_year
        if date_start:
            try:
                year = int(date_start[:4])
            except Exception:
                pass

        rows.append({
            "year":                      year,
            "date_start":                date_start,
            "date_end":                  safe_date(r.get("Ημερ/νία Κατασβεσης")),
            "nomos":                     str(r.get("Νομός") or "").strip().upper(),
            "municipality_raw":          str(r.get("Δήμος") or "").strip(),
            "area_name":                 str(r.get("Περιοχή") or "").strip(),
            "lat":                       lat,
            "lon":                       lon,
            "burned_forest_stremata":    forest,
            "burned_woodland_stremata":  woodland,
            "burned_grove_stremata":     grove,
            "burned_grassland_stremata": grassland,
            "burned_other_stremata":     other,
            "burned_total_stremata":     total_str,
            "burned_total_ha":           round(total_str / 10, 4),
            "source":                    filename,
        })

    result = pd.DataFrame(rows)
    print(f"[{nominal_year}] {len(result)} rows", flush=True)
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parts = [parse_2000_2018(), parse_2019()]
    for year, fname in XLS_FILES:
        parts.append(parse_xls_year(year, fname))

    combined = pd.concat(parts, ignore_index=True)

    # Sequential fire_id
    combined.insert(0, "fire_id", range(1, len(combined) + 1))

    # Drop rows with no meaningful data (zero area, no location, no date)
    meaningful = (
        combined["burned_total_stremata"] > 0
    ) | combined["lat"].notna()
    combined = combined[meaningful].copy()
    combined["fire_id"] = range(1, len(combined) + 1)

    combined.to_csv(OUTPUT_PATH, index=False)

    print(f"\n[done] {len(combined)} rows -> {OUTPUT_PATH}")
    print(f"Year range: {combined['year'].min()} - {combined['year'].max()}")
    print(f"Rows with coordinates: {combined['lat'].notna().sum()}")
    print(f"Rows with burned area > 0: {(combined['burned_total_stremata'] > 0).sum()}")
    print(f"\nBy year:")
    print(combined.groupby("year")["fire_id"].count().to_string())


if __name__ == "__main__":
    main()
