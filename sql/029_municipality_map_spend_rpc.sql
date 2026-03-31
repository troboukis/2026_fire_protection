DROP FUNCTION IF EXISTS public.get_municipality_map_spend_per_100k(integer);

CREATE OR REPLACE FUNCTION public.get_municipality_map_spend_per_100k(
  p_year integer
)
RETURNS TABLE (
  municipality_key text,
  municipality_name text,
  population_total numeric,
  total_amount_without_vat numeric,
  amount_per_100k numeric,
  signed_current_count bigint,
  active_previous_count bigint
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(py.amount_without_vat) AS amount_without_vat
  FROM public.payment py
  WHERE py.amount_without_vat IS NOT NULL
  GROUP BY py.procurement_id
),
direct_municipality AS (
  SELECT
    p.id AS procurement_id,
    p.municipality_key AS target_municipality_key,
    pa.amount_without_vat,
    p.reference_number,
    p.diavgeia_ada,
    p.title,
    p.contract_signed_date,
    p.organization_key,
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
    SELECT o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND COALESCE(p.start_date, p.contract_signed_date) IS NOT NULL
    AND p.end_date IS NOT NULL
    AND COALESCE(p.start_date, p.contract_signed_date) <= make_date(p_year, 12, 31)
    AND p.end_date >= make_date(p_year, 1, 1)
    AND p.municipality_key IS NOT NULL
    AND (
      p.canonical_owner_scope = 'municipality'
      OR COALESCE(org.authority_scope, 'other') = 'municipality'
    )
),
attributed_organization AS (
  SELECT
    p.id AS procurement_id,
    p.municipality_key AS target_municipality_key,
    pa.amount_without_vat,
    p.reference_number,
    p.diavgeia_ada,
    p.title,
    p.contract_signed_date,
    p.organization_key,
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
    SELECT o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND COALESCE(p.start_date, p.contract_signed_date) IS NOT NULL
    AND p.end_date IS NOT NULL
    AND COALESCE(p.start_date, p.contract_signed_date) <= make_date(p_year, 12, 31)
    AND p.end_date >= make_date(p_year, 1, 1)
    AND p.municipality_key IS NOT NULL
    AND p.organization_key IS NOT NULL
    AND COALESCE(p.canonical_owner_scope, '') = 'organization'
    AND COALESCE(org.authority_scope, 'other') = 'municipality'
),
coverage_organization AS (
  SELECT
    p.id AS procurement_id,
    omc.municipality_key AS target_municipality_key,
    pa.amount_without_vat,
    p.reference_number,
    p.diavgeia_ada,
    p.title,
    p.contract_signed_date,
    p.organization_key,
    ROW_NUMBER() OVER (
      PARTITION BY
        omc.municipality_key,
        COALESCE(
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
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND COALESCE(p.start_date, p.contract_signed_date) IS NOT NULL
    AND p.end_date IS NOT NULL
    AND COALESCE(p.start_date, p.contract_signed_date) <= make_date(p_year, 12, 31)
    AND p.end_date >= make_date(p_year, 1, 1)
    AND COALESCE(org.authority_scope, 'other') = 'municipality'
),
unioned AS (
  SELECT *, 0 AS source_priority FROM direct_municipality WHERE rn = 1
  UNION ALL
  SELECT *, 1 AS source_priority FROM coverage_organization WHERE rn = 1
  UNION ALL
  SELECT *, 2 AS source_priority FROM attributed_organization WHERE rn = 1
),
dedup AS (
  SELECT *
  FROM (
    SELECT
      u.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          u.target_municipality_key,
          COALESCE(
            NULLIF(TRIM(u.reference_number), ''),
            NULLIF(TRIM(u.diavgeia_ada), ''),
            CONCAT_WS('|', COALESCE(u.organization_key, ''), COALESCE(u.title, ''), COALESCE(u.contract_signed_date::text, ''))
          )
        ORDER BY u.source_priority ASC, u.procurement_id DESC
      ) AS keep_rn
    FROM unioned u
  ) ranked
  WHERE keep_rn = 1
),
municipality_stats AS (
  SELECT
    d.target_municipality_key AS municipality_key,
    SUM(COALESCE(d.amount_without_vat, 0)) AS total_amount_without_vat,
    COUNT(*) FILTER (
      WHERE d.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
    )::bigint AS signed_current_count,
    COUNT(*) FILTER (
      WHERE d.contract_signed_date < make_date(p_year, 1, 1)
    )::bigint AS active_previous_count
  FROM dedup d
  GROUP BY d.target_municipality_key
)
SELECT
  mfpd.municipality_key,
  mfpd.dhmos AS municipality_name,
  mfpd.plithismos_synolikos AS population_total,
  COALESCE(ms.total_amount_without_vat, 0) AS total_amount_without_vat,
  ROUND(
    (
      COALESCE(ms.total_amount_without_vat, 0) * 100000.0
    ) / NULLIF(mfpd.plithismos_synolikos, 0),
    2
  ) AS amount_per_100k
  ,
  COALESCE(ms.signed_current_count, 0) AS signed_current_count,
  COALESCE(ms.active_previous_count, 0) AS active_previous_count
FROM public.municipality_fire_protection_data mfpd
LEFT JOIN municipality_stats ms
  ON ms.municipality_key = mfpd.municipality_key
WHERE mfpd.plithismos_synolikos IS NOT NULL
  AND mfpd.plithismos_synolikos > 0
ORDER BY amount_per_100k DESC NULLS LAST, total_amount_without_vat DESC, mfpd.dhmos;
$$;

GRANT EXECUTE ON FUNCTION public.get_municipality_map_spend_per_100k(integer) TO anon, authenticated, service_role;
