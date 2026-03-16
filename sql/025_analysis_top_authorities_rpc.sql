CREATE OR REPLACE FUNCTION public.get_analysis_top_authorities(
  p_year integer DEFAULT NULL,
  p_year_start integer DEFAULT 2024,
  p_limit integer DEFAULT 8
)
RETURNS TABLE (
  authority_key text,
  authority_name text,
  authority_kind text,
  contracts bigint,
  total_m numeric
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH pay_by_proc AS (
  SELECT
    py.procurement_id,
    SUM(COALESCE(py.amount_without_vat, 0)) AS amount_without_vat
  FROM public.payment py
  GROUP BY py.procurement_id
),
proc_ranked AS (
  SELECT
    p.id,
    p.reference_number,
    p.title,
    p.contract_signed_date,
    p.contract_number,
    p.diavgeia_ada,
    p.organization_key,
    p.municipality_key,
    p.region_key,
    p.canonical_owner_scope,
    COALESCE(pb.amount_without_vat, 0) AS amount_without_vat,
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
  LEFT JOIN pay_by_proc pb
    ON pb.procurement_id = p.id
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND p.contract_signed_date IS NOT NULL
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND p.contract_signed_date >= make_date(COALESCE(p_year, p_year_start), 1, 1)
    AND p.contract_signed_date <= CASE
      WHEN p_year IS NULL THEN CURRENT_DATE
      ELSE make_date(p_year, 12, 31)
    END
),
base AS (
  SELECT
    pr.id,
    pr.amount_without_vat,
    CASE
      WHEN pr.canonical_owner_scope = 'municipality' AND NULLIF(BTRIM(pr.municipality_key), '') IS NOT NULL
        THEN 'municipality:' || BTRIM(pr.municipality_key)
      WHEN pr.canonical_owner_scope = 'region' AND NULLIF(BTRIM(pr.region_key), '') IS NOT NULL
        THEN 'region:' || BTRIM(pr.region_key)
      WHEN pr.canonical_owner_scope = 'organization' AND NULLIF(BTRIM(pr.organization_key), '') IS NOT NULL
        THEN 'organization:' || BTRIM(pr.organization_key)
      ELSE NULL
    END AS authority_key,
    CASE
      WHEN pr.canonical_owner_scope = 'municipality' AND NULLIF(BTRIM(pr.municipality_key), '') IS NOT NULL
        THEN 'municipality'
      WHEN pr.canonical_owner_scope = 'region' AND NULLIF(BTRIM(pr.region_key), '') IS NOT NULL
        THEN 'region'
      WHEN pr.canonical_owner_scope = 'organization' AND NULLIF(BTRIM(pr.organization_key), '') IS NOT NULL
        THEN 'organization'
      ELSE NULL
    END AS authority_kind,
    CASE
      WHEN pr.canonical_owner_scope = 'municipality' AND NULLIF(BTRIM(pr.municipality_key), '') IS NOT NULL
        THEN COALESCE(m.municipality_normalized_value, m.municipality_value, BTRIM(pr.municipality_key))
      WHEN pr.canonical_owner_scope = 'region' AND NULLIF(BTRIM(pr.region_key), '') IS NOT NULL
        THEN COALESCE(r.region_normalized_value, r.region_value, BTRIM(pr.region_key))
      WHEN pr.canonical_owner_scope = 'organization' AND NULLIF(BTRIM(pr.organization_key), '') IS NOT NULL
        THEN COALESCE(o.organization_normalized_value, o.organization_value, BTRIM(pr.organization_key))
      ELSE NULL
    END AS authority_name
  FROM proc_ranked pr
  LEFT JOIN LATERAL (
    SELECT m.municipality_normalized_value, m.municipality_value
    FROM public.municipality m
    WHERE m.municipality_key = pr.municipality_key
    ORDER BY m.id
    LIMIT 1
  ) m ON TRUE
  LEFT JOIN LATERAL (
    SELECT r.region_normalized_value, r.region_value
    FROM public.region r
    WHERE r.region_key = pr.region_key
    ORDER BY r.id
    LIMIT 1
  ) r ON TRUE
  LEFT JOIN LATERAL (
    SELECT o.organization_normalized_value, o.organization_value
    FROM public.organization o
    WHERE o.organization_key = pr.organization_key
    ORDER BY o.id
    LIMIT 1
  ) o ON TRUE
  WHERE pr.rn = 1
)
SELECT
  b.authority_key,
  b.authority_name,
  b.authority_kind,
  COUNT(*) AS contracts,
  ROUND((SUM(b.amount_without_vat) / 1000000.0)::numeric, 1) AS total_m
FROM base b
WHERE b.authority_key IS NOT NULL
  AND b.authority_name IS NOT NULL
GROUP BY b.authority_key, b.authority_name, b.authority_kind
ORDER BY SUM(b.amount_without_vat) DESC, COUNT(*) DESC, b.authority_name
LIMIT GREATEST(p_limit, 1);
$$;

GRANT EXECUTE ON FUNCTION public.get_analysis_top_authorities(integer, integer, integer)
TO anon, authenticated, service_role;
