-- Fast, deduplicated contracts page source for frontend.

DROP FUNCTION IF EXISTS public.get_contracts_page(text, text, text, date, date, numeric, integer, integer);

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
  prev_reference_no text,
  next_ref_no text,
  cpv_value text,
  procedure_type_value text,
  beneficiary_name text,
  beneficiary_vat_number text,
  beneficiary_gemi text,
  amount_without_vat numeric,
  diavgeia_ada text,
  cancelled boolean,
  is_modified boolean,
  total_count bigint
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
WITH RECURSIVE raw_query_terms AS (
  SELECT parsed.raw_term
  FROM regexp_split_to_table(
    regexp_replace(LEFT(COALESCE(p_q, ''), 500), E'\\s*!\\s*', '&!', 'g'),
    E'\\s*&\\s*'
  ) WITH ORDINALITY AS parsed(raw_term, ordinal)
  WHERE parsed.ordinal <= 10
),
query_terms AS (
  SELECT
    upper(
      translate(
        LEFT(BTRIM(CASE WHEN LEFT(raw_term, 1) = '!' THEN SUBSTRING(raw_term FROM 2) ELSE raw_term END), 100),
        'ΆΈΉΊΪΌΎΫΏάέήίϊΐόύϋΰώ',
        'ΑΕΗΙΙΟΥΥΩΑΕΗΙΙΙΟΥΥΥΩ'
      )
    ) AS value,
    LEFT(raw_term, 1) = '!' AS excluded
  FROM raw_query_terms
  WHERE BTRIM(CASE WHEN LEFT(raw_term, 1) = '!' THEN SUBSTRING(raw_term FROM 2) ELSE raw_term END) <> ''
),
payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(py.amount_without_vat) AS amount_without_vat,
    STRING_AGG(DISTINCT NULLIF(TRIM(py.beneficiary_name), ''), ' | ') AS beneficiary_name,
    STRING_AGG(DISTINCT NULLIF(TRIM(py.beneficiary_vat_number), ''), ' | ') AS beneficiary_vat_number,
    STRING_AGG(DISTINCT NULLIF(TRIM(b.gemi), ''), ' | ') AS beneficiary_gemi
  FROM public.payment py
  LEFT JOIN public.beneficiary b
    ON b.beneficiary_vat_number = py.beneficiary_vat_number
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
    COALESCE(p.cancelled, FALSE) AS cancelled,
    (
      NULLIF(TRIM(p.next_ref_no), '') IS NOT NULL
      OR COALESCE(p.next_modified, FALSE)
      OR COALESCE(p.next_extended, FALSE)
      OR EXISTS (
        SELECT 1
        FROM public.procurement p2
        WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
      )
    ) AS is_modified,
    p.reference_number,
    p.prev_reference_no,
    p.next_ref_no,
    p.contract_number,
    p.organization_key,
    COALESCE(pa.amount_without_vat, p.contract_budget, p.budget) AS amount_without_vat,
    pa.beneficiary_name,
    pa.beneficiary_vat_number,
    pa.beneficiary_gemi,
    COALESCE(
      org.organization_value,
      CASE
        WHEN p.canonical_owner_scope = 'region' THEN COALESCE(
          CASE WHEN reg.region_normalized_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_normalized_value) END,
          CASE WHEN reg.region_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_value) END,
          CASE WHEN muni.municipality_normalized_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_normalized_value) END,
          CASE WHEN muni.municipality_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_value) END
        )
        ELSE COALESCE(
          CASE WHEN muni.municipality_normalized_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_normalized_value) END,
          CASE WHEN reg.region_normalized_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_normalized_value) END,
          CASE WHEN muni.municipality_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_value) END,
          CASE WHEN reg.region_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_value) END
        )
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
),
dedup AS (
  SELECT *
  FROM base
  WHERE rn = 1
),
eligible AS (
  SELECT
    d.*,
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
    ) AS searchable_text
  FROM dedup d
  WHERE (p_date_from IS NULL OR d.contract_signed_date >= p_date_from)
    AND (p_date_to IS NULL OR d.contract_signed_date <= p_date_to)
    AND (p_min_amount IS NULL OR COALESCE(d.amount_without_vat, 0) >= p_min_amount)
    AND (p_procedure IS NULL OR p_procedure = '' OR d.procedure_type_value = p_procedure)
    AND (p_org IS NULL OR p_org = '' OR COALESCE(d.organization_value, '') ILIKE '%' || p_org || '%')
),
chain_members AS (
  SELECT
    e.id,
    e.reference_number,
    e.prev_reference_no,
    e.next_ref_no
  FROM eligible e
  WHERE BTRIM(COALESCE(p_q, '')) <> ''
    AND NOT EXISTS (
      SELECT 1
      FROM query_terms qt
      WHERE (NOT qt.excluded AND STRPOS(e.searchable_text, qt.value) = 0)
         OR (qt.excluded AND STRPOS(e.searchable_text, qt.value) > 0)
    )

  UNION

  SELECT
    neighbor.id,
    neighbor.reference_number,
    neighbor.prev_reference_no,
    neighbor.next_ref_no
  FROM chain_members member
  JOIN dedup neighbor
    ON NULLIF(BTRIM(neighbor.reference_number), '') = NULLIF(BTRIM(member.prev_reference_no), '')
    OR NULLIF(BTRIM(neighbor.prev_reference_no), '') = NULLIF(BTRIM(member.reference_number), '')
    OR NULLIF(BTRIM(neighbor.reference_number), '') = NULLIF(BTRIM(member.next_ref_no), '')
    OR NULLIF(BTRIM(neighbor.next_ref_no), '') = NULLIF(BTRIM(member.reference_number), '')
),
filtered AS (
  SELECT e.*
  FROM eligible e
  WHERE p_q IS NULL
    OR p_q = ''
    OR EXISTS (
      SELECT 1
      FROM chain_members member
      WHERE member.id = e.id
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
  prev_reference_no,
  next_ref_no,
  cpv_value,
  procedure_type_value,
  beneficiary_name,
  beneficiary_vat_number,
  beneficiary_gemi,
  amount_without_vat,
  diavgeia_ada,
  cancelled,
  is_modified,
  total_count
FROM counted
ORDER BY contract_signed_date DESC NULLS LAST, id DESC
OFFSET (LEAST(GREATEST(COALESCE(p_page, 1), 1), 10000) - 1)
  * LEAST(GREATEST(COALESCE(p_page_size, 50), 1), 10000)
LIMIT LEAST(GREATEST(COALESCE(p_page_size, 50), 1), 10000);
$$;

GRANT EXECUTE ON FUNCTION public.get_contracts_page(text, text, text, date, date, numeric, integer, integer) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_contracts_page_snapshot(
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
SELECT jsonb_build_object(
  'rows', COALESCE(
    jsonb_agg(
      to_jsonb(r) - 'total_count'
      ORDER BY r.contract_signed_date DESC NULLS LAST, r.id DESC
    ),
    '[]'::jsonb
  )
)
FROM public.get_contracts_page(
  NULL, NULL, NULL, p_date_from, p_date_to, NULL, 1, 10000
) r;
$$;

GRANT EXECUTE ON FUNCTION public.get_contracts_page_snapshot(date, date) TO anon, authenticated, service_role;
