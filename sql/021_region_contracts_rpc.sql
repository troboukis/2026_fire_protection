-- Canonical region contracts source for the frontend region panel.
--
-- Current implementation rules:
-- - includes procurements attributed to the selected region via procurement.region_key
-- - includes canonical region-owned procurements via procurement.canonical_owner_scope
-- - includes region-scoped / decentralized organizations
-- - excludes national organizations
-- - deduplicates by business identity, keeping the latest row

CREATE OR REPLACE FUNCTION public.get_region_contracts(
  p_region_key text,
  p_year integer,
  p_limit integer DEFAULT NULL,
  p_offset integer DEFAULT 0
)
RETURNS TABLE (
  procurement_id bigint,
  contract_signed_date date,
  organization_key text,
  organization_value text,
  authority_scope text,
  title text,
  procedure_type_value text,
  beneficiary_name text,
  amount_without_vat numeric,
  diavgeia_ada text,
  reference_number text
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(py.amount_without_vat) AS amount_without_vat,
    STRING_AGG(DISTINCT NULLIF(TRIM(py.beneficiary_name), ''), ' | ') AS beneficiary_name
  FROM public.payment py
  GROUP BY py.procurement_id
),
direct_region AS (
  SELECT
    p.id AS procurement_id,
    p.contract_signed_date,
    p.organization_key,
    COALESCE(
      org.organization_normalized_value,
      org.organization_value,
      CASE
        WHEN reg.region_normalized_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_normalized_value)
        WHEN reg.region_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_value)
        ELSE p.organization_key
      END
    ) AS organization_value,
    CASE
      WHEN p.canonical_owner_scope = 'region' THEN 'region'
      ELSE COALESCE(org.authority_scope, 'other')
    END AS authority_scope,
    p.title,
    public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
    pa.beneficiary_name,
    COALESCE(pa.amount_without_vat, p.contract_budget, p.budget) AS amount_without_vat,
    p.diavgeia_ada,
    p.reference_number,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(TRIM(p.reference_number), ''),
        NULLIF(TRIM(p.diavgeia_ada), ''),
        NULLIF(TRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT
      r.region_normalized_value,
      r.region_value
    FROM public.region r
    WHERE r.region_key = p.region_key
    ORDER BY r.id
    LIMIT 1
  ) reg ON TRUE
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
    AND p.contract_signed_date IS NOT NULL
    AND EXTRACT(YEAR FROM p.contract_signed_date) = p_year
    AND p.region_key = p_region_key
    AND (
      p.canonical_owner_scope = 'region'
      OR COALESCE(org.authority_scope, 'other') IN ('region', 'decentralized')
    )
),
coverage_region AS (
  SELECT
    p.id AS procurement_id,
    p.contract_signed_date,
    p.organization_key,
    COALESCE(org.organization_normalized_value, org.organization_value, p.organization_key) AS organization_value,
    COALESCE(org.authority_scope, 'other') AS authority_scope,
    p.title,
    public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
    pa.beneficiary_name,
    COALESCE(pa.amount_without_vat, p.contract_budget, p.budget) AS amount_without_vat,
    p.diavgeia_ada,
    p.reference_number,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(TRIM(p.reference_number), ''),
        NULLIF(TRIM(p.diavgeia_ada), ''),
        NULLIF(TRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  JOIN public.org_municipality_coverage omc
    ON omc.organization_key = p.organization_key
   AND omc.region_key = p_region_key
  LEFT JOIN payment_agg pa
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
    AND p.contract_signed_date IS NOT NULL
    AND EXTRACT(YEAR FROM p.contract_signed_date) = p_year
    AND COALESCE(org.authority_scope, 'other') IN ('region', 'decentralized')
),
unioned AS (
  SELECT *, 0 AS source_priority FROM direct_region
  WHERE rn = 1
  UNION ALL
  SELECT *, 1 AS source_priority FROM coverage_region
  WHERE rn = 1
),
base AS (
  SELECT *
  FROM (
    SELECT
      u.*,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
          NULLIF(TRIM(u.reference_number), ''),
          NULLIF(TRIM(u.diavgeia_ada), ''),
          CONCAT_WS('|', COALESCE(u.organization_key, ''), COALESCE(u.title, ''), COALESCE(u.contract_signed_date::text, ''))
        )
        ORDER BY u.source_priority ASC, u.procurement_id DESC
      ) AS keep_rn
    FROM unioned u
  ) dedup
  WHERE keep_rn = 1
)
SELECT
  procurement_id,
  contract_signed_date,
  organization_key,
  organization_value,
  authority_scope,
  title,
  procedure_type_value,
  beneficiary_name,
  amount_without_vat,
  diavgeia_ada,
  reference_number
FROM base
ORDER BY contract_signed_date DESC NULLS LAST, procurement_id DESC
OFFSET GREATEST(p_offset, 0)
LIMIT CASE
  WHEN p_limit IS NULL THEN NULL
  ELSE GREATEST(p_limit, 1)
END;
$$;

GRANT EXECUTE ON FUNCTION public.get_region_contracts(text, integer, integer, integer) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_region_contract_count(
  p_region_key text,
  p_year integer
)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT COUNT(*)::bigint
  FROM public.get_region_contracts(p_region_key, p_year, NULL, 0);
$$;

GRANT EXECUTE ON FUNCTION public.get_region_contract_count(text, integer) TO anon, authenticated, service_role;
