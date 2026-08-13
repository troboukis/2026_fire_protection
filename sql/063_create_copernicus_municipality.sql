BEGIN;

CREATE TABLE IF NOT EXISTS public.copernicus_municipality (
  copernicus_id    BIGINT NOT NULL,
  municipality_key TEXT NOT NULL,
  match_method      TEXT NOT NULL,
  overlap_ratio     NUMERIC(10, 6) NOT NULL,
  overlap_area_ha   NUMERIC(12, 2),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (copernicus_id, municipality_key),
  CONSTRAINT fk_copernicus_municipality_fire
    FOREIGN KEY (copernicus_id)
    REFERENCES public.copernicus(copernicus_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT fk_copernicus_municipality_municipality
    FOREIGN KEY (municipality_key)
    REFERENCES public.municipality_normalized_name(municipality_key)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT chk_copernicus_municipality_overlap_ratio
    CHECK (overlap_ratio > 0 AND overlap_ratio <= 1),
  CONSTRAINT chk_copernicus_municipality_overlap_area
    CHECK (overlap_area_ha IS NULL OR overlap_area_ha >= 0)
);

CREATE INDEX IF NOT EXISTS idx_copernicus_municipality_key
  ON public.copernicus_municipality (municipality_key, copernicus_id);

-- Preserve the current single-municipality results until a full Copernicus
-- refresh recalculates every polygon against every municipality.
INSERT INTO public.copernicus_municipality (
  copernicus_id,
  municipality_key,
  match_method,
  overlap_ratio,
  overlap_area_ha
)
SELECT
  copernicus_id,
  municipality_key,
  COALESCE(municipality_match_method, 'legacy_primary'),
  COALESCE(NULLIF(municipality_overlap_ratio, 0), 1),
  CASE
    WHEN area_ha IS NULL THEN NULL
    ELSE ROUND(area_ha * COALESCE(NULLIF(municipality_overlap_ratio, 0), 1), 2)
  END
FROM public.copernicus
WHERE municipality_key IS NOT NULL
ON CONFLICT (copernicus_id, municipality_key) DO NOTHING;

ALTER TABLE public.copernicus_municipality ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS public_read_copernicus_municipality
  ON public.copernicus_municipality;
CREATE POLICY public_read_copernicus_municipality
ON public.copernicus_municipality
FOR SELECT
TO anon, authenticated
USING (true);

CREATE OR REPLACE VIEW public.copernicus_municipality_fire
WITH (security_invoker = true) AS
SELECT
  cm.copernicus_id,
  cm.municipality_key,
  cm.match_method,
  cm.overlap_ratio,
  cm.overlap_area_ha,
  c.firedate,
  c.area_ha AS total_area_ha,
  c.centroid,
  c.shape
FROM public.copernicus_municipality cm
JOIN public.copernicus c
  ON c.copernicus_id = cm.copernicus_id;

GRANT SELECT ON public.copernicus_municipality
  TO anon, authenticated, service_role;
GRANT SELECT ON public.copernicus_municipality_fire
  TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
