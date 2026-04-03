DROP FUNCTION IF EXISTS public.get_homepage_funding(integer, integer);

CREATE OR REPLACE FUNCTION public.get_homepage_funding(
  p_year_main integer,
  p_year_start integer DEFAULT 2016
)
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH filtered_funding AS (
  SELECT
    f.year::int AS year,
    COALESCE(f.amount_eur, 0) AS amount_eur,
    LOWER(BTRIM(COALESCE(f.allocation_type, ''))) AS allocation_type_norm,
    LOWER(BTRIM(COALESCE(f.recipient_type, ''))) AS recipient_type_norm
  FROM public.fund f
  WHERE f.year IS NOT NULL
    AND f.year BETWEEN p_year_start AND p_year_main
    AND LOWER(BTRIM(COALESCE(f.recipient_type, ''))) IN ('δήμος', 'σύνδεσμος', 'σύνδεσμος δήμων')
),
latest_year AS (
  SELECT COALESCE(MAX(ff.year), p_year_main) AS year_main
  FROM filtered_funding ff
),
aggregated_funding AS (
  SELECT
    ff.year,
    ROUND(SUM(CASE WHEN ff.allocation_type_norm = 'τακτική' THEN ff.amount_eur ELSE 0 END), 2) AS regular_amount,
    ROUND(SUM(CASE WHEN ff.allocation_type_norm = 'τακτική' THEN 0 ELSE ff.amount_eur END), 2) AS emergency_amount,
    ROUND(SUM(CASE WHEN ff.recipient_type_norm = 'δήμος' THEN ff.amount_eur ELSE 0 END), 2) AS municipality_amount,
    ROUND(SUM(CASE WHEN ff.recipient_type_norm IN ('σύνδεσμος', 'σύνδεσμος δήμων') THEN ff.amount_eur ELSE 0 END), 2) AS syndesmos_amount,
    ROUND(SUM(ff.amount_eur), 2) AS total_amount
  FROM filtered_funding ff
  GROUP BY ff.year
),
history AS (
  SELECT
    years.year,
    COALESCE(af.regular_amount, 0) AS regular_amount,
    COALESCE(af.emergency_amount, 0) AS emergency_amount,
    COALESCE(af.municipality_amount, 0) AS municipality_amount,
    COALESCE(af.syndesmos_amount, 0) AS syndesmos_amount,
    COALESCE(af.total_amount, 0) AS total_amount
  FROM generate_series(p_year_start, p_year_main) AS years(year)
  LEFT JOIN aggregated_funding af
    ON af.year = years.year
  ORDER BY years.year
)
SELECT jsonb_build_object(
  'year_main', (SELECT ly.year_main FROM latest_year ly),
  'year_previous', (SELECT ly.year_main - 1 FROM latest_year ly),
  'history_start_year', p_year_start,
  'current_total', COALESCE((SELECT h.total_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'previous_total', COALESCE((SELECT h.total_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main - 1), 0),
  'current_regular_amount', COALESCE((SELECT h.regular_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'current_emergency_amount', COALESCE((SELECT h.emergency_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'current_municipality_amount', COALESCE((SELECT h.municipality_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'current_syndesmos_amount', COALESCE((SELECT h.syndesmos_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'history', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'year', h.year,
        'regular_amount', h.regular_amount,
        'emergency_amount', h.emergency_amount,
        'municipality_amount', h.municipality_amount,
        'syndesmos_amount', h.syndesmos_amount,
        'total_amount', h.total_amount
      )
      ORDER BY h.year
    )
    FROM history h
  ), '[]'::jsonb)
);
$$;

GRANT EXECUTE ON FUNCTION public.get_homepage_funding(integer, integer) TO anon, authenticated, service_role;
