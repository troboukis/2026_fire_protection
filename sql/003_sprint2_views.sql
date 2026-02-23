-- 003_sprint2_views.sql
-- Sprint 2: server-side aggregate views + spatial join for fire history layer.
-- Run this in the Supabase SQL editor (Dashboard → SQL Editor).

-- ---------------------------------------------------------------------------
-- 1. Global per-year fire summary
--    Used by the web app for the editorial copy subtitle and year selector.
--    Replaces the broken client-side 84k-row paginated fetch.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.v_global_fire_summary AS
SELECT
  year,
  COUNT(*)                         AS incident_count,
  SUM(burned_total_stremata)       AS total_burned_stremata,
  SUM(burned_total_ha)             AS total_burned_ha
FROM public.fire_incidents
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year;

-- ---------------------------------------------------------------------------
-- 2. Per-municipality all-time fire totals
--    Used for the choropleth (severity map overlay).
--    Requires municipality_id to be populated (see step 3 below).
--
--    pct_of_national = each municipality's share of total Greek burned area
--    2000-2024.  Values sum to 100% across all municipalities.
--    Municipalities with zero fires do not appear (default cream map color).
-- ---------------------------------------------------------------------------

-- NOTE: CREATE OR REPLACE cannot drop columns; use DROP + CREATE on schema changes.
DROP VIEW IF EXISTS public.v_municipality_fire_totals;
CREATE VIEW public.v_municipality_fire_totals AS
SELECT
  f.municipality_id,
  SUM(f.incident_count)  AS total_incidents,
  SUM(f.total_burned_ha) AS total_burned_ha,
  MAX(f.year)            AS last_fire_year,
  ROUND(
    (SUM(f.total_burned_ha)
     / NULLIF(SUM(SUM(f.total_burned_ha)) OVER (), 0)
     * 100)::numeric,
    4
  )                      AS pct_of_national
FROM public.v_municipality_fire_summary f
WHERE f.municipality_id IS NOT NULL
GROUP BY f.municipality_id;

GRANT SELECT ON public.v_municipality_fire_totals TO anon, authenticated;

-- ---------------------------------------------------------------------------
-- 3. Spatial join: link fire incidents to municipalities via PostGIS
--    This sets municipality_id on every row where lat/lon falls inside a
--    municipality boundary.  Run once; safe to re-run (WHERE ... IS NULL).
--
--    Expected result: ~70-80% of rows get linked (fires with valid coords).
--    Fires with NULL / offshore coords stay unlinked.
-- ---------------------------------------------------------------------------

UPDATE public.fire_incidents fi
SET municipality_id = m.id
FROM public.municipalities m
WHERE ST_Within(
  ST_SetSRID(ST_Point(fi.lon::double precision, fi.lat::double precision), 4326),
  m.geom
)
AND fi.lat IS NOT NULL
AND fi.lon IS NOT NULL
AND fi.municipality_id IS NULL;

-- Verify: check how many rows got linked
-- SELECT COUNT(*) FILTER (WHERE municipality_id IS NOT NULL) AS linked,
--        COUNT(*) AS total
-- FROM public.fire_incidents;
