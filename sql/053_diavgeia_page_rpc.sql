DROP FUNCTION IF EXISTS public.get_diavgeia_page(text, date, date, integer, integer);
DROP FUNCTION IF EXISTS public.get_diavgeia_page(text, date, date, text, integer, integer);

CREATE OR REPLACE FUNCTION public.get_diavgeia_page(
  p_q text DEFAULT NULL,
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL,
  p_municipality_key text DEFAULT NULL,
  p_page integer DEFAULT 1,
  p_page_size integer DEFAULT 50
)
RETURNS TABLE (
  id bigint,
  org_type text,
  org_name_clean text,
  subject text,
  decision_date timestamptz,
  ada text,
  diavgeia_document_type_decision_uid text,
  document_url text,
  spending_contractors_value text,
  municipality_key text,
  protocol_number text,
  thematic_categories text,
  spending_signers text,
  spending_contractors_name text,
  spending_contractors_afm text,
  min_decision_date timestamptz,
  max_decision_date timestamptz,
  total_count bigint
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
WITH raw_query_terms AS (
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
base AS (
  SELECT
    d.id,
    d.org_type,
    d.org_name_clean,
    d.subject,
    COALESCE(d.publish_timestamp, d.submission_timestamp) AS decision_date,
    d.ada,
    d.diavgeia_document_type_decision_uid,
    d.document_url,
    d.spending_contractors_value,
    d.municipality_key,
    d.protocol_number,
    d.thematic_categories,
    d.spending_signers,
    d.spending_contractors_name,
    d.spending_contractors_afm,
    upper(
      translate(
        CONCAT_WS(
          ' ',
          COALESCE(d.subject, ''),
          COALESCE(d.org_type, ''),
          COALESCE(d.org_name_clean, ''),
          COALESCE(d.ada, ''),
          COALESCE(d.diavgeia_document_type_decision_uid, '')
        ),
        'ΆΈΉΊΪΌΎΫΏάέήίϊΐόύϋΰώ',
        'ΑΕΗΙΙΟΥΥΩΑΕΗΙΙΙΟΥΥΥΩ'
      )
    ) AS searchable_text
  FROM public.diavgeia d
),
filtered AS (
  SELECT *
  FROM base b
  WHERE (p_date_from IS NULL OR b.decision_date::date >= p_date_from)
    AND (p_date_to IS NULL OR b.decision_date::date <= p_date_to)
    AND (p_municipality_key IS NULL OR p_municipality_key = '' OR EXISTS (
      SELECT 1
      FROM public.diavgeia d
      WHERE d.id = b.id
        AND d.municipality_key = p_municipality_key
    ))
    AND NOT EXISTS (
      SELECT 1
      FROM query_terms qt
      WHERE (NOT qt.excluded AND STRPOS(b.searchable_text, qt.value) = 0)
         OR (qt.excluded AND STRPOS(b.searchable_text, qt.value) > 0)
    )
),
counted AS (
  SELECT
    f.*,
    MIN(f.decision_date) OVER () AS min_decision_date,
    MAX(f.decision_date) OVER () AS max_decision_date,
    COUNT(*) OVER () AS total_count
  FROM filtered f
)
SELECT
  id,
  org_type,
  org_name_clean,
  subject,
  decision_date,
  ada,
  diavgeia_document_type_decision_uid,
  document_url,
  spending_contractors_value,
  municipality_key,
  protocol_number,
  thematic_categories,
  spending_signers,
  spending_contractors_name,
  spending_contractors_afm,
  min_decision_date,
  max_decision_date,
  total_count
FROM counted
ORDER BY decision_date DESC NULLS LAST, id DESC
OFFSET (LEAST(GREATEST(COALESCE(p_page, 1), 1), 10000) - 1)
  * LEAST(GREATEST(COALESCE(p_page_size, 50), 1), 50000)
LIMIT LEAST(GREATEST(COALESCE(p_page_size, 50), 1), 50000);
$$;

GRANT EXECUTE ON FUNCTION public.get_diavgeia_page(text, date, date, text, integer, integer) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_diavgeia_page_snapshot(
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL,
  p_municipality_key text DEFAULT NULL
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
      to_jsonb(r) - 'total_count' - 'min_decision_date' - 'max_decision_date'
      ORDER BY r.decision_date DESC NULLS LAST, r.id DESC
    ),
    '[]'::jsonb
  )
)
FROM public.get_diavgeia_page(
  NULL, p_date_from, p_date_to, p_municipality_key, 1, 50000
) r;
$$;

GRANT EXECUTE ON FUNCTION public.get_diavgeia_page_snapshot(date, date, text) TO anon, authenticated, service_role;
