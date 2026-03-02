BEGIN;

-- Normalizes Greek municipality names for deterministic joins.
CREATE OR REPLACE FUNCTION public.norm_gr_name(v text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT NULLIF(
    btrim(
      regexp_replace(
        regexp_replace(
          upper(
            translate(
              COALESCE(v, ''),
              'ΆΈΉΊΪΌΎΫΏάέήίϊΐόύϋΰώ',
              'ΑΕΗΙΙΟΥΥΩΑΕΗΙΙΙΟΥΥΥΩ'
            )
          ),
          '[^A-ZΑ-Ω0-9 ]',
          ' ',
          'g'
        ),
        '\s+',
        ' ',
        'g'
      )
    ),
    ''
  );
$$;

WITH canonical AS (
  SELECT
    norm_gr_name(COALESCE(m.municipality_normalized_value, m.municipality_value)) AS norm_name,
    m.municipality_key
  FROM public.municipality m
  WHERE m.municipality_key IS NOT NULL
),
unique_canonical AS (
  SELECT
    norm_name,
    MIN(municipality_key) AS municipality_key
  FROM canonical
  WHERE norm_name IS NOT NULL
  GROUP BY norm_name
  HAVING COUNT(DISTINCT municipality_key) = 1
),
to_update AS (
  SELECT
    m.id,
    u.municipality_key
  FROM public.municipality m
  JOIN unique_canonical u
    ON u.norm_name = norm_gr_name(COALESCE(m.municipality_normalized_value, m.municipality_value))
  WHERE m.municipality_key IS NULL
)
UPDATE public.municipality m
SET municipality_key = t.municipality_key
FROM to_update t
WHERE m.id = t.id;

COMMIT;

-- Validation: unresolved rows after backfill.
SELECT
  COUNT(*) AS municipality_rows_still_null_key
FROM public.municipality
WHERE municipality_key IS NULL;

-- Validation: sample lookup for the user case (Κιλκις).
SELECT
  id,
  municipality_key,
  municipality_value,
  municipality_normalized_value,
  source_system,
  source_key
FROM public.municipality
WHERE
  norm_gr_name(COALESCE(municipality_normalized_value, municipality_value))
  LIKE '%ΚΙΛΚΙΣ%'
ORDER BY municipality_key NULLS LAST, id;

