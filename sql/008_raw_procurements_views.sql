-- 008_raw_procurements_views.sql
-- Views that expose raw_procurements in a frontend-friendly shape.

BEGIN;

-- Municipality-linked procurement rows via org_municipality_coverage.
CREATE OR REPLACE VIEW public.v_raw_procurements_municipality AS
SELECT
  r.id AS raw_id,
  COALESCE(NULLIF(r.diavgeia_ada, ''), NULLIF(r.reference_number, ''), r.id::TEXT) AS procurement_id,
  r.reference_number,
  r.organization_value,
  r.submission_at AS issue_date,
  r.contract_signed_date,
  r.title AS subject,
  r.procedure_type_value AS decision_type,
  COALESCE(r.total_cost_without_vat, r.total_cost_with_vat, r.contract_budget, r.budget) AS amount_eur,
  CASE
    WHEN NULLIF(r.diavgeia_ada, '') IS NOT NULL THEN 'https://diavgeia.gov.gr/doc/' || r.diavgeia_ada
    ELSE NULL
  END AS document_url,
  r.first_member_name AS contractor_name,
  c.municipality_id,
  c.authority_level,
  c.org_type,
  c.org_name_clean
FROM public.raw_procurements r
JOIN public.org_municipality_coverage c
  ON UPPER(TRIM(r.organization_value)) = c.org_name_clean;

COMMIT;
