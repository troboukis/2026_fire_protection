-- Fast, deduplicated contracts page source for frontend.

CREATE OR REPLACE FUNCTION public.get_contracts_page(
  p_q text DEFAULT NULL,
  p_org text DEFAULT NULL,
  p_procedure text DEFAULT NULL,
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL,
  p_min_amount numeric DEFAULT NULL,
  p_page integer DEFAULT 1,
  p_page_size integer DEFAULT 50
)
RETURNS TABLE (
  id bigint,
  contract_signed_date date,
  organization_value text,
  title text,
  reference_number text,
  cpv_value text,
  procedure_type_value text,
  beneficiary_name text,
  amount_without_vat numeric,
  diavgeia_ada text,
  total_count bigint
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
cpv_agg AS (
  SELECT
    c.procurement_id,
    STRING_AGG(
      DISTINCT NULLIF(TRIM(c.cpv_value), ''),
      ' | '
      ORDER BY NULLIF(TRIM(c.cpv_value), '')
    ) AS cpv_value
  FROM public.cpv c
  GROUP BY c.procurement_id
),
base AS (
  SELECT
    p.id,
    p.contract_signed_date,
    p.start_date,
    p.end_date,
    p.no_end_date,
    p.title,
    public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
    p.diavgeia_ada,
    p.reference_number,
    p.contract_number,
    p.organization_key,
    COALESCE(pa.amount_without_vat, p.contract_budget, p.budget) AS amount_without_vat,
    pa.beneficiary_name,
    COALESCE(
      org.organization_value,
      CASE
        WHEN muni.municipality_normalized_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_normalized_value)
        WHEN reg.region_normalized_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_normalized_value)
        WHEN muni.municipality_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_value)
        WHEN reg.region_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_value)
        ELSE NULL
      END
    ) AS organization_value,
    ca.cpv_value,
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
  LEFT JOIN payment_agg pa ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT
      m.municipality_normalized_value,
      m.municipality_value
    FROM public.municipality m
    WHERE m.municipality_key = p.municipality_key
    ORDER BY m.id
    LIMIT 1
  ) muni ON TRUE
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
    SELECT o.organization_normalized_value AS organization_value
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  LEFT JOIN cpv_agg ca ON ca.procurement_id = p.id
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
),
dedup AS (
  SELECT *
  FROM base
  WHERE rn = 1
),
filtered AS (
  SELECT *
  FROM dedup d
  WHERE (
      p_date_from IS NULL
      OR COALESCE(
        CASE
          WHEN COALESCE(d.no_end_date, FALSE) THEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, 12, 31)
          ELSE d.end_date
        END,
        COALESCE(d.start_date, d.contract_signed_date)
      ) >= p_date_from
    )
    AND (
      p_date_to IS NULL
      OR COALESCE(d.start_date, d.contract_signed_date) <= p_date_to
    )
    AND (p_min_amount IS NULL OR COALESCE(d.amount_without_vat, 0) >= p_min_amount)
    AND (p_procedure IS NULL OR p_procedure = '' OR d.procedure_type_value = p_procedure)
    AND (p_org IS NULL OR p_org = '' OR COALESCE(d.organization_value, '') ILIKE '%' || p_org || '%')
    AND (
      p_q IS NULL OR p_q = '' OR
      upper(
        translate(
          CONCAT_WS(
            ' ',
            COALESCE(d.title, ''),
            COALESCE(d.organization_value, ''),
            COALESCE(d.beneficiary_name, ''),
            COALESCE(d.cpv_value, ''),
            COALESCE(d.reference_number, '')
          ),
          'ΆΈΉΊΪΌΎΫΏάέήίϊΐόύϋΰώ',
          'ΑΕΗΙΙΟΥΥΩΑΕΗΙΙΙΟΥΥΥΩ'
        )
      ) LIKE '%' || upper(
        translate(
          p_q,
          'ΆΈΉΊΪΌΎΫΏάέήίϊΐόύϋΰώ',
          'ΑΕΗΙΙΟΥΥΩΑΕΗΙΙΙΟΥΥΥΩ'
        )
      ) || '%'
    )
),
counted AS (
  SELECT f.*, COUNT(*) OVER () AS total_count
  FROM filtered f
)
SELECT
  id,
  contract_signed_date,
  organization_value,
  title,
  reference_number,
  cpv_value,
  procedure_type_value,
  beneficiary_name,
  amount_without_vat,
  diavgeia_ada,
  total_count
FROM counted
ORDER BY contract_signed_date DESC NULLS LAST, id DESC
OFFSET GREATEST((p_page - 1) * p_page_size, 0)
LIMIT GREATEST(p_page_size, 1);
$$;

GRANT EXECUTE ON FUNCTION public.get_contracts_page(text, text, text, date, date, numeric, integer, integer) TO anon, authenticated, service_role;
