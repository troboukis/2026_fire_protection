-- 002_webapp_schema.sql
-- Web app tables for fire-protection-greece
-- Run this after 001_init_schema.sql
-- Requires PostGIS (enabled by default on Supabase)

-- Enable PostGIS extension (must run before any geometry types are used)
CREATE EXTENSION IF NOT EXISTS postgis;

BEGIN;

-- ---------------------------------------------------------------------------
-- municipalities
-- Base geographic layer. id = municipality_code from municipalities.geojson
-- Entries with null municipality_code (e.g. Mount Athos) are excluded.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.municipalities (
  id            TEXT PRIMARY KEY,        -- e.g. '9186' (from Kallikratis shapefile)
  name          TEXT NOT NULL,           -- official name in Greek, e.g. 'Αθηναίων'
  geom          GEOMETRY(MultiPolygon, 4326),
  forest_ha     NUMERIC,                 -- forested area in hectares (may be null)
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_municipalities_geom
  ON public.municipalities USING GIST (geom);

-- ---------------------------------------------------------------------------
-- fire_incidents
-- Harmonized fire data 2000–2024 from three source formats.
-- municipality_id is null initially; can be linked later via spatial join or
-- name matching.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.fire_incidents (
  fire_id                    INTEGER PRIMARY KEY,
  year                       SMALLINT,
  date_start                 DATE,
  date_end                   DATE,
  nomos                      TEXT,                -- prefecture name (uppercase)
  municipality_raw           TEXT,                -- raw name from source data
  municipality_id            TEXT REFERENCES public.municipalities(id),
  area_name                  TEXT,
  lat                        NUMERIC(10, 6),
  lon                        NUMERIC(10, 6),
  burned_forest_stremata     NUMERIC,
  burned_woodland_stremata   NUMERIC,
  burned_grove_stremata      NUMERIC,
  burned_grassland_stremata  NUMERIC,
  burned_other_stremata      NUMERIC,
  burned_total_stremata      NUMERIC,
  burned_total_ha            NUMERIC,
  source                     TEXT,
  created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fire_year
  ON public.fire_incidents (year);

CREATE INDEX IF NOT EXISTS idx_fire_municipality
  ON public.fire_incidents (municipality_id);

CREATE INDEX IF NOT EXISTS idx_fire_coords
  ON public.fire_incidents (lat, lon)
  WHERE lat IS NOT NULL AND lon IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fire_total_ha
  ON public.fire_incidents (burned_total_ha DESC);

-- ---------------------------------------------------------------------------
-- funding_allocations
-- KAP (Κεντρικοί Αυτοτελείς Πόροι) fire protection fund distributions
-- from Ministry of Interior PDFs, 2016–2025 (2023 missing from collection).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.funding_allocations (
  id               BIGSERIAL PRIMARY KEY,
  year             SMALLINT    NOT NULL,
  allocation_type  TEXT        NOT NULL,   -- τακτική / συμπληρωματική / έκτακτη
  recipient_type   TEXT        NOT NULL,   -- δήμος / σύνδεσμος
  recipient_raw    TEXT        NOT NULL,   -- name as in PDF (uppercase)
  nomos            TEXT,
  municipality_id  TEXT REFERENCES public.municipalities(id),
  amount_eur       NUMERIC(12, 2) NOT NULL,
  source_ada       TEXT,                   -- ADA of the ministerial decision
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_funding_municipality
  ON public.funding_allocations (municipality_id);

CREATE INDEX IF NOT EXISTS idx_funding_year
  ON public.funding_allocations (year);

-- ---------------------------------------------------------------------------
-- procurement_decisions
-- Diavgeia fire-protection-relevant procurement decisions, 2018–2025.
-- Linked to municipalities via org_to_municipality mapping.
-- authority_level: municipality / region / decentralized / national / other
-- amount_eur: derived at ingest time from the richest available amount field
--   (spending > commitment > direct > payment, depending on decision type).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.procurement_decisions (
  ada              TEXT PRIMARY KEY,
  org_type         TEXT        NOT NULL,
  org_name_clean   TEXT        NOT NULL,
  authority_level  TEXT,
  municipality_id  TEXT REFERENCES public.municipalities(id),
  region_name      TEXT,                   -- for region-level decisions
  issue_date       DATE,
  subject          TEXT,
  document_url     TEXT,
  decision_type    TEXT,
  subject_has_anatrop_or_anaklis BOOLEAN,
  subject_has_budget_balance_report_terms BOOLEAN,
  matched_keywords TEXT[],
  amount_eur       NUMERIC(14, 2),
  contractor_name  TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_procurement_municipality
  ON public.procurement_decisions (municipality_id);

CREATE INDEX IF NOT EXISTS idx_procurement_date
  ON public.procurement_decisions (issue_date);

CREATE INDEX IF NOT EXISTS idx_procurement_authority
  ON public.procurement_decisions (authority_level);

CREATE INDEX IF NOT EXISTS idx_procurement_org
  ON public.procurement_decisions (org_name_clean);

CREATE INDEX IF NOT EXISTS idx_procurement_subject_anatrop_anaklis
  ON public.procurement_decisions (subject_has_anatrop_or_anaklis);

CREATE INDEX IF NOT EXISTS idx_procurement_subject_budget_terms
  ON public.procurement_decisions (subject_has_budget_balance_report_terms);

-- ---------------------------------------------------------------------------
-- Row Level Security: public read-only (no auth required for reads)
-- ---------------------------------------------------------------------------

ALTER TABLE public.municipalities       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.fire_incidents       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.funding_allocations  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.procurement_decisions ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public_read" ON public.municipalities
  FOR SELECT USING (true);

CREATE POLICY "public_read" ON public.fire_incidents
  FOR SELECT USING (true);

CREATE POLICY "public_read" ON public.funding_allocations
  FOR SELECT USING (true);

CREATE POLICY "public_read" ON public.procurement_decisions
  FOR SELECT USING (true);

-- ---------------------------------------------------------------------------
-- Convenience views for the web app
-- ---------------------------------------------------------------------------

-- Per-municipality fire summary (total burned area and incident count per year)
CREATE OR REPLACE VIEW public.v_municipality_fire_summary AS
SELECT
  municipality_id,
  year,
  COUNT(*)                         AS incident_count,
  SUM(burned_total_ha)             AS total_burned_ha,
  MAX(burned_total_ha)             AS max_single_fire_ha
FROM public.fire_incidents
WHERE municipality_id IS NOT NULL
GROUP BY municipality_id, year;

-- Per-municipality funding summary
CREATE OR REPLACE VIEW public.v_municipality_funding_summary AS
SELECT
  municipality_id,
  year,
  SUM(amount_eur)                  AS total_allocated_eur,
  COUNT(*)                         AS allocation_count
FROM public.funding_allocations
WHERE municipality_id IS NOT NULL
GROUP BY municipality_id, year;

-- Per-municipality procurement summary
CREATE OR REPLACE VIEW public.v_municipality_procurement_summary AS
SELECT
  municipality_id,
  EXTRACT(YEAR FROM issue_date)::INT  AS year,
  COUNT(*)                            AS decision_count,
  SUM(amount_eur)                     AS total_amount_eur
FROM public.procurement_decisions
WHERE municipality_id IS NOT NULL
GROUP BY municipality_id, EXTRACT(YEAR FROM issue_date)::INT;

COMMIT;
