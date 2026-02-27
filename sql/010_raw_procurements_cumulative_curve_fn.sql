-- 010_raw_procurements_cumulative_curve_fn.sql
-- RPC for daily cumulative spending lines.
-- p_year_main stops at latest available date in data.
-- Prior years (p_year_start to p_year_main-1) run until 31 Dec.

BEGIN;

DROP FUNCTION IF EXISTS public.get_raw_procurements_cumulative_curve(DATE, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS public.get_raw_procurements_cumulative_curve(DATE, INTEGER, INTEGER);

CREATE FUNCTION public.get_raw_procurements_cumulative_curve(
  p_as_of_date  DATE    DEFAULT CURRENT_DATE,
  p_year_main   INTEGER DEFAULT EXTRACT(YEAR FROM CURRENT_DATE)::INT,
  p_year_start  INTEGER DEFAULT 2024
)
RETURNS TABLE (
  series_year INTEGER,
  point_date DATE,
  day_of_year INTEGER,
  year_days INTEGER,
  cumulative_amount NUMERIC(18,2)
)
LANGUAGE SQL
STABLE
AS $$
WITH years AS (
  SELECT generate_series(p_year_start, p_year_main) AS yr
),
normalized AS (
  SELECT
    EXTRACT(YEAR FROM submission_at)::INT AS yr,
    submission_at::DATE AS event_date,
    total_cost_without_vat::NUMERIC AS amount_eur
  FROM public.raw_procurements
),
ranges AS (
  SELECT
    y.yr,
    MAKE_DATE(y.yr, 1, 1) AS start_date,
    CASE
      WHEN y.yr = p_year_main THEN
        LEAST(
          COALESCE(
            (
              SELECT MAX(n.event_date)
              FROM normalized n
              WHERE n.yr = p_year_main
                AND n.event_date IS NOT NULL
            ),
            MAKE_DATE(p_year_main, 1, 1)
          ),
          p_as_of_date
        )
      ELSE MAKE_DATE(y.yr, 12, 31)
    END AS end_date,
    EXTRACT(DOY FROM MAKE_DATE(y.yr, 12, 31))::INT AS days_in_year
  FROM years y
),
calendar AS (
  SELECT
    r.yr,
    gs::DATE AS point_date,
    EXTRACT(DOY FROM gs)::INT AS doy,
    r.days_in_year
  FROM ranges r
  CROSS JOIN LATERAL generate_series(r.start_date, r.end_date, INTERVAL '1 day') AS gs
),
daily_amounts AS (
  SELECT
    yr,
    event_date,
    SUM(amount_eur)::NUMERIC(18,2) AS day_amount
  FROM normalized
  WHERE event_date IS NOT NULL
    AND amount_eur IS NOT NULL
  GROUP BY yr, event_date
),
cumulative AS (
  SELECT
    c.yr,
    c.point_date,
    c.doy,
    c.days_in_year,
    SUM(COALESCE(d.day_amount, 0)) OVER (
      PARTITION BY c.yr
      ORDER BY c.point_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )::NUMERIC(18,2) AS cumulative_amount
  FROM calendar c
  LEFT JOIN daily_amounts d
    ON d.yr = c.yr
   AND d.event_date = c.point_date
)
SELECT
  yr AS series_year,
  point_date,
  doy AS day_of_year,
  days_in_year AS year_days,
  cumulative_amount
FROM cumulative
ORDER BY yr ASC, point_date ASC;
$$;

COMMIT;
