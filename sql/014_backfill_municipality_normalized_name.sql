BEGIN;

ALTER TABLE public.municipality_normalized_name
ADD COLUMN IF NOT EXISTS municipality_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_municipality_normalized_name_municipality_key
ON public.municipality_normalized_name (municipality_key);

WITH canonical AS (
  SELECT DISTINCT ON (m.municipality_key)
    m.municipality_key,
    m.municipality_value,
    m.municipality_normalized_value
  FROM public.municipality AS m
  WHERE m.municipality_key IS NOT NULL
    AND m.municipality_value IS NOT NULL
    AND m.municipality_value <> m.municipality_key
  ORDER BY
    m.municipality_key,
    CASE m.source_system
      WHEN 'region_to_municipalities' THEN 0
      WHEN 'geo' THEN 1
      ELSE 2
    END,
    m.id
)
INSERT INTO public.municipality_normalized_name (
  municipality_key,
  municipality_value,
  municipality_normalized_value
)
SELECT
  c.municipality_key,
  c.municipality_value,
  c.municipality_normalized_value
FROM canonical AS c
ON CONFLICT (municipality_key) DO UPDATE SET
  municipality_value = EXCLUDED.municipality_value,
  municipality_normalized_value = EXCLUDED.municipality_normalized_value,
  updated_at = NOW();

UPDATE public.municipality AS m
SET municipality_normalized_name_id = n.id
FROM public.municipality_normalized_name AS n
WHERE m.municipality_key = n.municipality_key
  AND (m.municipality_normalized_name_id IS DISTINCT FROM n.id);

COMMIT;
