BEGIN;

CREATE TABLE IF NOT EXISTS public.copernicus (
  id                            BIGSERIAL PRIMARY KEY,
  copernicus_id                 BIGINT NOT NULL,
  centroid                      JSONB,
  bbox                          NUMERIC(12, 6)[],
  shape                         JSONB,
  country                       TEXT,
  countryful                    TEXT,
  province                      TEXT,
  commune                       TEXT,
  firedate                      TIMESTAMPTZ,
  area_ha                       NUMERIC(12, 2),
  broadlea                      NUMERIC(8, 5),
  conifer                       NUMERIC(8, 5),
  mixed                         NUMERIC(8, 5),
  scleroph                      NUMERIC(8, 5),
  transit                       NUMERIC(8, 5),
  othernatlc                    NUMERIC(8, 5),
  agriareas                     NUMERIC(8, 5),
  artifsurf                     NUMERIC(8, 5),
  otherlc                       NUMERIC(8, 5),
  percna2k                      NUMERIC(8, 5),
  lastupdate                    TIMESTAMPTZ,
  lastfiredate                  TIMESTAMPTZ,
  noneu                         BOOLEAN,
  municipality_key              TEXT,
  municipality_normalized_value TEXT,
  municipality_match_method     TEXT,
  municipality_overlap_ratio    NUMERIC(10, 6),
  created_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_copernicus_copernicus_id
    UNIQUE (copernicus_id),
  CONSTRAINT fk_copernicus_municipality_key
    FOREIGN KEY (municipality_key)
    REFERENCES public.municipality_normalized_name(municipality_key)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
  CONSTRAINT chk_copernicus_bbox_len
    CHECK (bbox IS NULL OR array_length(bbox, 1) = 4),
  CONSTRAINT chk_copernicus_area_ha
    CHECK (area_ha IS NULL OR area_ha >= 0),
  CONSTRAINT chk_copernicus_overlap_ratio
    CHECK (
      municipality_overlap_ratio IS NULL
      OR (municipality_overlap_ratio >= 0 AND municipality_overlap_ratio <= 1)
    )
);

CREATE INDEX IF NOT EXISTS idx_copernicus_firedate
  ON public.copernicus (firedate DESC);

CREATE INDEX IF NOT EXISTS idx_copernicus_lastupdate
  ON public.copernicus (lastupdate DESC);

CREATE INDEX IF NOT EXISTS idx_copernicus_municipality_key
  ON public.copernicus (municipality_key);

CREATE INDEX IF NOT EXISTS idx_copernicus_province
  ON public.copernicus (province);

DROP TRIGGER IF EXISTS trg_copernicus_updated_at ON public.copernicus;
CREATE TRIGGER trg_copernicus_updated_at
BEFORE UPDATE ON public.copernicus
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

COMMIT;
