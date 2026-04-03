DROP FUNCTION IF EXISTS public.get_municipality_map_funding_per_100k(integer);

CREATE OR REPLACE FUNCTION public.get_municipality_map_funding_per_100k(
  p_year integer
)
RETURNS TABLE (
  municipality_key text,
  municipality_name text,
  population_total numeric,
  total_amount_eur numeric,
  amount_per_100k numeric
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH municipality_funding AS (
  SELECT
    f.municipality_key,
    SUM(COALESCE(f.amount_eur, 0)) AS total_amount_eur
  FROM public.fund f
  WHERE f.year = p_year
    AND NULLIF(TRIM(f.municipality_key), '') IS NOT NULL
  GROUP BY f.municipality_key
)
SELECT
  mfpd.municipality_key,
  mfpd.dhmos AS municipality_name,
  mfpd.plithismos_synolikos AS population_total,
  COALESCE(mf.total_amount_eur, 0) AS total_amount_eur,
  ROUND(
    (
      COALESCE(mf.total_amount_eur, 0) * 100000.0
    ) / NULLIF(mfpd.plithismos_synolikos, 0),
    2
  ) AS amount_per_100k
FROM public.municipality_fire_protection_data mfpd
LEFT JOIN municipality_funding mf
  ON mf.municipality_key = mfpd.municipality_key
WHERE mfpd.plithismos_synolikos IS NOT NULL
  AND mfpd.plithismos_synolikos > 0
ORDER BY amount_per_100k DESC NULLS LAST, total_amount_eur DESC, mfpd.dhmos;
$$;

GRANT EXECUTE ON FUNCTION public.get_municipality_map_funding_per_100k(integer) TO anon, authenticated, service_role;
