DROP FUNCTION IF EXISTS public.get_latest_funding_year_municipality_spend();

CREATE OR REPLACE FUNCTION public.get_latest_funding_year_municipality_spend()
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH latest_funding_year AS (
  SELECT MAX(f.year)::int AS year_main
  FROM public.fund f
  WHERE f.year IS NOT NULL
),
payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(COALESCE(py.amount_without_vat, 0)) AS amount_without_vat
  FROM public.payment py
  JOIN latest_funding_year ly
    ON ly.year_main IS NOT NULL
   AND py.fiscal_year = ly.year_main
  WHERE py.procurement_id IS NOT NULL
    AND py.amount_without_vat IS NOT NULL
  GROUP BY py.procurement_id
),
proc_ranked AS (
  SELECT
    p.id AS procurement_id,
    pa.amount_without_vat,
    COALESCE(p.canonical_owner_scope, '') AS canonical_owner_scope,
    COALESCE(org.authority_scope, 'other') AS authority_scope,
    COALESCE(org.organization_normalized_value, org.organization_value, '') AS organization_name,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(p.reference_number), ''),
        NULLIF(BTRIM(p.diavgeia_ada), ''),
        NULLIF(BTRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT
      o.organization_normalized_value,
      o.organization_value,
      o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = p.reference_number
    )
),
classified AS (
  SELECT
    pr.procurement_id,
    pr.amount_without_vat,
    (
      pr.canonical_owner_scope = 'municipality'
      OR pr.authority_scope = 'municipality'
    ) AS is_municipality,
    (
      NULLIF(BTRIM(pr.organization_name), '') IS NOT NULL
      AND (
        pr.organization_name ILIKE '%συνδεσμ%'
        OR pr.organization_name ILIKE '%σύνδεσμ%'
      )
    ) AS is_syndesmos
  FROM proc_ranked pr
  WHERE pr.rn = 1
)
SELECT jsonb_build_object(
  'latest_funding_year', (SELECT ly.year_main FROM latest_funding_year ly),
  'municipality_amount', COALESCE((
    SELECT ROUND(SUM(c.amount_without_vat), 2)
    FROM classified c
    WHERE c.is_municipality
  ), 0),
  'syndesmos_amount', COALESCE((
    SELECT ROUND(SUM(c.amount_without_vat), 2)
    FROM classified c
    WHERE c.is_syndesmos
  ), 0),
  'total_amount', COALESCE((
    SELECT ROUND(SUM(c.amount_without_vat), 2)
    FROM classified c
    WHERE c.is_municipality OR c.is_syndesmos
  ), 0),
  'municipality_procurement_count', COALESCE((
    SELECT COUNT(*)
    FROM classified c
    WHERE c.is_municipality
  ), 0),
  'syndesmos_procurement_count', COALESCE((
    SELECT COUNT(*)
    FROM classified c
    WHERE c.is_syndesmos
  ), 0),
  'total_procurement_count', COALESCE((
    SELECT COUNT(*)
    FROM classified c
    WHERE c.is_municipality OR c.is_syndesmos
  ), 0)
);
$$;

GRANT EXECUTE ON FUNCTION public.get_latest_funding_year_municipality_spend() TO anon, authenticated, service_role;
