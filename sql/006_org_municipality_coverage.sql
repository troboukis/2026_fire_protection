-- 006_org_municipality_coverage.sql
-- Stores org -> municipality coverage mappings (many-to-many).

BEGIN;

CREATE TABLE IF NOT EXISTS public.org_municipality_coverage (
  id              BIGSERIAL PRIMARY KEY,
  org_type        TEXT NOT NULL,
  org_name_clean  TEXT NOT NULL,
  authority_level TEXT,
  region_id       TEXT,
  municipality_id TEXT NOT NULL REFERENCES public.municipalities(id) ON DELETE CASCADE,
  municipality_name TEXT,
  coverage_method TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_org_municipality_coverage UNIQUE (org_type, org_name_clean, municipality_id)
);

CREATE INDEX IF NOT EXISTS idx_org_cov_org
  ON public.org_municipality_coverage (org_name_clean);

CREATE INDEX IF NOT EXISTS idx_org_cov_org_type
  ON public.org_municipality_coverage (org_type);

CREATE INDEX IF NOT EXISTS idx_org_cov_municipality
  ON public.org_municipality_coverage (municipality_id);

CREATE INDEX IF NOT EXISTS idx_org_cov_region
  ON public.org_municipality_coverage (region_id);

ALTER TABLE public.org_municipality_coverage ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public_read" ON public.org_municipality_coverage
  FOR SELECT USING (true);

COMMIT;
