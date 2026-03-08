BEGIN;

CREATE TABLE IF NOT EXISTS public.works (
  id                    BIGSERIAL PRIMARY KEY,
  reference_number      TEXT NOT NULL,
  point_name_raw        TEXT,
  point_name_canonical  TEXT,
  work                  TEXT,
  lat                   NUMERIC(10, 6),
  lon                   NUMERIC(10, 6),
  page                  INTEGER,
  pages                 INTEGER[],
  excerpt               TEXT,
  formatted_address     TEXT,
  place_id              TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_works_reference_number
    FOREIGN KEY (reference_number)
    REFERENCES public.procurement(reference_number)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT chk_works_lat
    CHECK (lat IS NULL OR (lat >= -90 AND lat <= 90)),
  CONSTRAINT chk_works_lon
    CHECK (lon IS NULL OR (lon >= -180 AND lon <= 180)),
  CONSTRAINT chk_works_page
    CHECK (page IS NULL OR page > 0)
);

CREATE INDEX IF NOT EXISTS idx_works_reference_number
  ON public.works (reference_number);

CREATE INDEX IF NOT EXISTS idx_works_point_name_canonical
  ON public.works (point_name_canonical);

CREATE INDEX IF NOT EXISTS idx_works_place_id
  ON public.works (place_id);

DROP TRIGGER IF EXISTS trg_works_updated_at ON public.works;
CREATE TRIGGER trg_works_updated_at
BEFORE UPDATE ON public.works
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

COMMIT;
