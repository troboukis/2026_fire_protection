-- Query-time normalization for procurement procedure labels.
-- Keeps raw DB values unchanged.

CREATE OR REPLACE FUNCTION public.normalize_procedure_type(p_val text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_val IS NULL OR btrim(p_val) = '' THEN '—'
    WHEN p_val ILIKE 'Απευθείας ανάθεση%' THEN 'Απευθείας ανάθεση'
    ELSE p_val
  END
$$;

GRANT EXECUTE ON FUNCTION public.normalize_procedure_type(text) TO anon, authenticated, service_role;

