from __future__ import annotations

import csv
import sys
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.map_copernicus_to_municipalities import resolve_database_url


CSV_PATH = ROOT / "data" / "municipalities_data.csv"
TABLE_NAME = "public.municipality_fire_protection_data"

TEXT_COLUMNS = [
    "municipality_key",
    "dhmos",
    "municipality_normalized_name",
]

NUMERIC_COLUMNS = [
    "kpi_politikis_prostasias",
    "plithismos_synolikos",
    "plithismos_oreinos",
    "plithismos_hmioreinos",
    "plithismos_pedinos",
    "ektasi_km2",
    "ektasi_oreini_km2",
    "ektasi_hmioreini_km2",
    "ektasi_pedini_km2",
    "puknotita",
    "oxhmata_udrofora",
    "oxhmata_purosvestika",
    "sxedia_purkagies",
    "dilosis_katharis_plithos",
    "elegxoi_katopin_dilosis",
    "mi_symmorfosi_dilosis",
    "pososto_symmorfosis_dilosis",
    "elegxoi_aytepaggelti",
    "mi_symmorfosi_aytepaggelti",
    "kataggelies_plithos",
    "elegxoi_katopin_kataggelias",
    "mi_symmorfosi_kataggelias",
    "ektasi_vlastisis_pros_katharismo_ha",
    "katharismeni_ektasi_vlastisis_ha",
    "pososto_proliptikou_katharismou",
    "ypoleimmata_katharismwn_t",
    "dapani_puroprostasias_eur",
]

CSV_COLUMNS = TEXT_COLUMNS + NUMERIC_COLUMNS
INSERT_COLUMNS = CSV_COLUMNS + ["source_file"]


def parse_text(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def parse_numeric(value: str | None) -> Decimal | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    return Decimal(normalized)


def load_rows() -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    seen_keys: set[str] = set()
    with CSV_PATH.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        missing_columns = [column for column in CSV_COLUMNS if column not in (reader.fieldnames or [])]
        if missing_columns:
            raise ValueError(f"CSV is missing required columns: {missing_columns}")

        for raw in reader:
            municipality_key = parse_text(raw.get("municipality_key"))
            dhmos = parse_text(raw.get("dhmos"))
            municipality_normalized_name = parse_text(raw.get("municipality_normalized_name"))
            if not municipality_key or not dhmos or not municipality_normalized_name:
                raise ValueError(f"Missing required municipality identity fields in row: {raw}")
            if municipality_key in seen_keys:
                raise ValueError(f"Duplicate municipality_key in CSV: {municipality_key}")
            seen_keys.add(municipality_key)

            row: list[object] = [municipality_key, dhmos, municipality_normalized_name]
            row.extend(parse_numeric(raw.get(column)) for column in NUMERIC_COLUMNS)
            row.append(str(CSV_PATH.relative_to(ROOT)))
            rows.append(tuple(row))

    return rows


def main() -> None:
    rows = load_rows()
    conn = psycopg2.connect(resolve_database_url(None))
    cur = conn.cursor()
    execute_values(
        cur,
        f"""
        INSERT INTO {TABLE_NAME} (
          {", ".join(INSERT_COLUMNS)}
        ) VALUES %s
        ON CONFLICT (municipality_key) DO UPDATE SET
          dhmos = EXCLUDED.dhmos,
          municipality_normalized_name = EXCLUDED.municipality_normalized_name,
          kpi_politikis_prostasias = EXCLUDED.kpi_politikis_prostasias,
          plithismos_synolikos = EXCLUDED.plithismos_synolikos,
          plithismos_oreinos = EXCLUDED.plithismos_oreinos,
          plithismos_hmioreinos = EXCLUDED.plithismos_hmioreinos,
          plithismos_pedinos = EXCLUDED.plithismos_pedinos,
          ektasi_km2 = EXCLUDED.ektasi_km2,
          ektasi_oreini_km2 = EXCLUDED.ektasi_oreini_km2,
          ektasi_hmioreini_km2 = EXCLUDED.ektasi_hmioreini_km2,
          ektasi_pedini_km2 = EXCLUDED.ektasi_pedini_km2,
          puknotita = EXCLUDED.puknotita,
          oxhmata_udrofora = EXCLUDED.oxhmata_udrofora,
          oxhmata_purosvestika = EXCLUDED.oxhmata_purosvestika,
          sxedia_purkagies = EXCLUDED.sxedia_purkagies,
          dilosis_katharis_plithos = EXCLUDED.dilosis_katharis_plithos,
          elegxoi_katopin_dilosis = EXCLUDED.elegxoi_katopin_dilosis,
          mi_symmorfosi_dilosis = EXCLUDED.mi_symmorfosi_dilosis,
          pososto_symmorfosis_dilosis = EXCLUDED.pososto_symmorfosis_dilosis,
          elegxoi_aytepaggelti = EXCLUDED.elegxoi_aytepaggelti,
          mi_symmorfosi_aytepaggelti = EXCLUDED.mi_symmorfosi_aytepaggelti,
          kataggelies_plithos = EXCLUDED.kataggelies_plithos,
          elegxoi_katopin_kataggelias = EXCLUDED.elegxoi_katopin_kataggelias,
          mi_symmorfosi_kataggelias = EXCLUDED.mi_symmorfosi_kataggelias,
          ektasi_vlastisis_pros_katharismo_ha = EXCLUDED.ektasi_vlastisis_pros_katharismo_ha,
          katharismeni_ektasi_vlastisis_ha = EXCLUDED.katharismeni_ektasi_vlastisis_ha,
          pososto_proliptikou_katharismou = EXCLUDED.pososto_proliptikou_katharismou,
          ypoleimmata_katharismwn_t = EXCLUDED.ypoleimmata_katharismwn_t,
          dapani_puroprostasias_eur = EXCLUDED.dapani_puroprostasias_eur,
          source_file = EXCLUDED.source_file,
          updated_at = NOW()
        """,
        rows,
        page_size=200,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"loaded_rows={len(rows)}")


if __name__ == "__main__":
    main()
