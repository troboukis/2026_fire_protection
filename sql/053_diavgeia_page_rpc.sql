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
  min_decision_date timestamptz,
  max_decision_date timestamptz,
  total_count bigint
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH base AS (
  SELECT
    d.id,
    d.org_type,
    d.org_name_clean,
    d.subject,
    COALESCE(d.publish_timestamp, d.submission_timestamp) AS decision_date,
    d.ada,
    d.diavgeia_document_type_decision_uid,
    d.document_url,
    d.spending_contractors_value
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
    AND (
      p_q IS NULL OR p_q = '' OR
      upper(
        translate(
          CONCAT_WS(
            ' ',
            COALESCE(b.subject, ''),
            COALESCE(b.org_type, ''),
            COALESCE(b.org_name_clean, ''),
            COALESCE(b.ada, ''),
            COALESCE(b.diavgeia_document_type_decision_uid, '')
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
  min_decision_date,
  max_decision_date,
  total_count
FROM counted
ORDER BY decision_date DESC NULLS LAST, id DESC
OFFSET GREATEST((p_page - 1) * p_page_size, 0)
LIMIT GREATEST(p_page_size, 1);
$$;

GRANT EXECUTE ON FUNCTION public.get_diavgeia_page(text, date, date, text, integer, integer) TO anon, authenticated, service_role;
