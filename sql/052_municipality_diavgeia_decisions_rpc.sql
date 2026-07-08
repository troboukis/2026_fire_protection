DROP FUNCTION IF EXISTS public.get_municipality_diavgeia_decisions(text);
DROP FUNCTION IF EXISTS public.get_municipality_diavgeia_decisions(text, integer);

CREATE OR REPLACE FUNCTION public.get_municipality_diavgeia_decisions(
  p_municipality_key text,
  p_year integer
)
RETURNS TABLE (
  diavgeia_id bigint,
  org_type text,
  org_name_clean text,
  subject text,
  decision_date timestamptz,
  ada text,
  diavgeia_document_type_decision_uid text,
  spending_contractors_value text,
  document_url text
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
AS $$
SELECT
  d.id AS diavgeia_id,
  d.org_type,
  d.org_name_clean,
  d.subject,
  COALESCE(d.publish_timestamp, d.submission_timestamp) AS decision_date,
  d.ada,
  d.diavgeia_document_type_decision_uid,
  d.spending_contractors_value,
  d.document_url
FROM public.diavgeia d
WHERE d.municipality_key = p_municipality_key
  AND COALESCE(d.publish_timestamp, d.submission_timestamp) >= make_timestamptz(p_year, 1, 1, 0, 0, 0)
  AND COALESCE(d.publish_timestamp, d.submission_timestamp) < make_timestamptz(p_year + 1, 1, 1, 0, 0, 0)
ORDER BY
  COALESCE(d.publish_timestamp, d.submission_timestamp) DESC NULLS LAST,
  d.id DESC;
$$;

GRANT EXECUTE ON FUNCTION public.get_municipality_diavgeia_decisions(text, integer) TO anon, authenticated, service_role;
