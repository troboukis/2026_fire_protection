BEGIN;

CREATE TABLE IF NOT EXISTS public.municipality_fire_protection_data (
  id                                  BIGSERIAL PRIMARY KEY,
  municipality_key                    TEXT NOT NULL,
  dhmos                               TEXT NOT NULL,
  municipality_normalized_name        TEXT NOT NULL,
  kpi_politikis_prostasias            NUMERIC,
  plithismos_synolikos                NUMERIC,
  plithismos_oreinos                  NUMERIC,
  plithismos_hmioreinos               NUMERIC,
  plithismos_pedinos                  NUMERIC,
  ektasi_km2                          NUMERIC,
  ektasi_oreini_km2                   NUMERIC,
  ektasi_hmioreini_km2                NUMERIC,
  ektasi_pedini_km2                   NUMERIC,
  puknotita                           NUMERIC,
  oxhmata_udrofora                    NUMERIC,
  oxhmata_purosvestika                NUMERIC,
  sxedia_purkagies                    NUMERIC,
  dilosis_katharis_plithos            NUMERIC,
  elegxoi_katopin_dilosis             NUMERIC,
  mi_symmorfosi_dilosis               NUMERIC,
  pososto_symmorfosis_dilosis         NUMERIC,
  elegxoi_aytepaggelti                NUMERIC,
  mi_symmorfosi_aytepaggelti          NUMERIC,
  kataggelies_plithos                 NUMERIC,
  elegxoi_katopin_kataggelias         NUMERIC,
  mi_symmorfosi_kataggelias           NUMERIC,
  ektasi_vlastisis_pros_katharismo_ha NUMERIC,
  katharismeni_ektasi_vlastisis_ha    NUMERIC,
  pososto_proliptikou_katharismou     NUMERIC,
  ypoleimmata_katharismwn_t           NUMERIC,
  dapani_puroprostasias_eur           NUMERIC,
  source_file                         TEXT NOT NULL DEFAULT 'data/municipalities_data.csv',
  created_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_municipality_fire_protection_data_key
    UNIQUE (municipality_key),
  CONSTRAINT fk_municipality_fire_protection_data_municipality_key
    FOREIGN KEY (municipality_key)
    REFERENCES public.municipality_normalized_name(municipality_key)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
  CONSTRAINT chk_municipality_fire_protection_data_key
    CHECK (btrim(municipality_key) <> ''),
  CONSTRAINT chk_municipality_fire_protection_data_kpi
    CHECK (
      kpi_politikis_prostasias IS NULL
      OR (kpi_politikis_prostasias >= 0 AND kpi_politikis_prostasias <= 1)
    )
);

CREATE INDEX IF NOT EXISTS idx_municipality_fire_protection_data_name
  ON public.municipality_fire_protection_data (municipality_normalized_name);

DROP TRIGGER IF EXISTS trg_municipality_fire_protection_data_updated_at
  ON public.municipality_fire_protection_data;
CREATE TRIGGER trg_municipality_fire_protection_data_updated_at
BEFORE UPDATE ON public.municipality_fire_protection_data
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

GRANT SELECT ON public.municipality_fire_protection_data TO anon, authenticated, service_role;

COMMIT;
