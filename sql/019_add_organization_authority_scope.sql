BEGIN;

ALTER TABLE public.organization
  ADD COLUMN IF NOT EXISTS authority_scope TEXT;

DROP VIEW IF EXISTS public.works_enriched;

CREATE VIEW public.works_enriched AS
SELECT
  w.id,
  w.reference_number,
  p.id AS procurement_id,
  p.municipality_key,
  p.region_key,
  p.organization_key,
  o.organization_normalized_value,
  COALESCE(o.authority_scope, 'other') AS authority_scope,
  p.contract_signed_date,
  p.title,
  w.point_name_raw,
  w.point_name_canonical,
  w.work,
  w.lat,
  w.lon,
  w.page,
  w.pages,
  w.excerpt,
  w.formatted_address,
  w.place_id,
  w.created_at,
  w.updated_at
FROM public.works w
JOIN public.procurement p
  ON p.reference_number = w.reference_number
LEFT JOIN public.organization o
  ON o.organization_key = p.organization_key;

COMMIT;
