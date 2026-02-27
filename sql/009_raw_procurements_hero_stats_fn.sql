-- 009_raw_procurements_hero_stats_fn.sql
-- Lightweight RPC for homepage hero stats (no full-table payload to frontend).

BEGIN;

DROP FUNCTION IF EXISTS public.get_raw_procurements_hero_stats(INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS public.get_raw_procurements_hero_stats(INTEGER, INTEGER, INTEGER, DATE);

CREATE FUNCTION public.get_raw_procurements_hero_stats(
  p_year_main  INTEGER DEFAULT 2026,
  p_year_prev1 INTEGER DEFAULT 2025,
  p_year_prev2 INTEGER DEFAULT 2024,
  p_as_of_date DATE DEFAULT CURRENT_DATE
)
RETURNS TABLE (
  period_main_start DATE,
  period_main_end DATE,
  total_main NUMERIC(18,2),
  total_prev1 NUMERIC(18,2),
  total_prev2 NUMERIC(18,2),
  total_main_vs_prev1_pct NUMERIC(10,2),
  top_contract_type TEXT,
  top_contract_type_count BIGINT,
  top_contract_type_prev1_count BIGINT,
  top_contract_type_vs_prev1_pct NUMERIC(10,2),
  top_cpv_text TEXT,
  top_cpv_count BIGINT,
  top_cpv_prev1_count BIGINT,
  top_cpv_vs_prev1_pct NUMERIC(10,2)
)
LANGUAGE SQL
STABLE
AS $$
WITH normalized AS (
  SELECT
    EXTRACT(YEAR FROM submission_at)::INT AS yr,
    submission_at AS event_ts,
    submission_at::DATE AS event_date,
    total_cost_without_vat::NUMERIC AS amount_eur,
    NULLIF(BTRIM(procedure_type_key), '') AS procedure_type_key_norm,
    NULLIF(BTRIM(procedure_type_value), '') AS procedure_type,
    cpv_values
  FROM public.raw_procurements
),
periods AS (
  SELECT
    p_year_main AS yr_main,
    MAKE_DATE(p_year_main, 1, 1) AS main_start,
    p_as_of_date AS main_end
),
in_window AS (
  SELECT n.*
  FROM normalized n
  WHERE n.event_ts IS NOT NULL
    AND n.yr IN (p_year_main, p_year_prev1, p_year_prev2)
    AND n.event_ts >= MAKE_DATE(n.yr, 1, 1)::TIMESTAMP
    AND n.event_ts <= MAKE_DATE(
      n.yr,
      EXTRACT(MONTH FROM p_as_of_date)::INT,
      LEAST(
        EXTRACT(DAY FROM p_as_of_date)::INT,
        EXTRACT(
          DAY FROM (
            DATE_TRUNC(
              'month',
              MAKE_DATE(n.yr, EXTRACT(MONTH FROM p_as_of_date)::INT, 1) + INTERVAL '1 month'
            ) - INTERVAL '1 day'
          )
        )::INT
      )
    )::TIMESTAMP
),
in_window_amount AS (
  SELECT *
  FROM in_window
  WHERE amount_eur IS NOT NULL
),
cpv_expanded AS (
  SELECT
    iw.yr,
    NULLIF(BTRIM(cpv_item), '') AS cpv_text
  FROM in_window iw
  CROSS JOIN LATERAL regexp_split_to_table(COALESCE(iw.cpv_values, ''), '\s*\|\s*') AS cpv_item
),
totals AS (
  SELECT yr, SUM(amount_eur)::NUMERIC(18,2) AS total_amount
  FROM in_window_amount
  GROUP BY yr
),
top_type_main AS (
  SELECT
    COALESCE(procedure_type_key_norm, procedure_type) AS procedure_bucket,
    COUNT(*)::BIGINT AS cnt
  FROM in_window_amount
  WHERE COALESCE(procedure_type_key_norm, procedure_type) IS NOT NULL
    AND yr = p_year_main
  GROUP BY COALESCE(procedure_type_key_norm, procedure_type)
  ORDER BY cnt DESC, procedure_bucket ASC
  LIMIT 1
),
top_type_main_label AS (
  SELECT
    iw.procedure_type,
    COUNT(*)::BIGINT AS cnt
  FROM in_window_amount iw
  WHERE iw.yr = p_year_main
    AND COALESCE(iw.procedure_type_key_norm, iw.procedure_type) = (SELECT procedure_bucket FROM top_type_main)
    AND iw.procedure_type IS NOT NULL
  GROUP BY iw.procedure_type
  ORDER BY cnt DESC, iw.procedure_type ASC
  LIMIT 1
),
top_type_prev AS (
  SELECT COUNT(*)::BIGINT AS cnt
  FROM in_window_amount
  WHERE COALESCE(procedure_type_key_norm, procedure_type) = (SELECT procedure_bucket FROM top_type_main)
    AND yr = p_year_prev1
),
top_cpv_main AS (
  SELECT cpv_text, COUNT(*)::BIGINT AS cnt
  FROM cpv_expanded
  WHERE cpv_text IS NOT NULL
    AND yr = p_year_main
  GROUP BY cpv_text
  ORDER BY cnt DESC, cpv_text ASC
  LIMIT 1
),
top_cpv_prev AS (
  SELECT COUNT(*)::BIGINT AS cnt
  FROM cpv_expanded
  WHERE cpv_text = (SELECT cpv_text FROM top_cpv_main)
    AND yr = p_year_prev1
)
SELECT
  (SELECT main_start FROM periods) AS period_main_start,
  (SELECT main_end FROM periods) AS period_main_end,
  COALESCE((SELECT total_amount FROM totals WHERE yr = p_year_main), 0)::NUMERIC(18,2) AS total_main,
  COALESCE((SELECT total_amount FROM totals WHERE yr = p_year_prev1), 0)::NUMERIC(18,2) AS total_prev1,
  COALESCE((SELECT total_amount FROM totals WHERE yr = p_year_prev2), 0)::NUMERIC(18,2) AS total_prev2,
  CASE
    WHEN COALESCE((SELECT total_amount FROM totals WHERE yr = p_year_prev1), 0) = 0 THEN NULL
    ELSE (
      (
        COALESCE((SELECT total_amount FROM totals WHERE yr = p_year_main), 0)
        - COALESCE((SELECT total_amount FROM totals WHERE yr = p_year_prev1), 0)
      )
      / COALESCE((SELECT total_amount FROM totals WHERE yr = p_year_prev1), 0)
      * 100
    )::NUMERIC(10,2)
  END AS total_main_vs_prev1_pct,
  COALESCE((SELECT procedure_type FROM top_type_main_label), COALESCE((SELECT procedure_bucket FROM top_type_main), '—')) AS top_contract_type,
  COALESCE((SELECT cnt FROM top_type_main), 0)::BIGINT AS top_contract_type_count,
  COALESCE((SELECT cnt FROM top_type_prev), 0)::BIGINT AS top_contract_type_prev1_count,
  CASE
    WHEN COALESCE((SELECT cnt FROM top_type_prev), 0) = 0 THEN NULL
    ELSE (
      (
        COALESCE((SELECT cnt FROM top_type_main), 0)
        - COALESCE((SELECT cnt FROM top_type_prev), 0)
      )::NUMERIC
      / COALESCE((SELECT cnt FROM top_type_prev), 0)
      * 100
    )::NUMERIC(10,2)
  END AS top_contract_type_vs_prev1_pct,
  COALESCE((SELECT cpv_text FROM top_cpv_main), '—') AS top_cpv_text,
  COALESCE((SELECT cnt FROM top_cpv_main), 0)::BIGINT AS top_cpv_count,
  COALESCE((SELECT cnt FROM top_cpv_prev), 0)::BIGINT AS top_cpv_prev1_count,
  CASE
    WHEN COALESCE((SELECT cnt FROM top_cpv_prev), 0) = 0 THEN NULL
    ELSE (
      (
        COALESCE((SELECT cnt FROM top_cpv_main), 0)
        - COALESCE((SELECT cnt FROM top_cpv_prev), 0)
      )::NUMERIC
      / COALESCE((SELECT cnt FROM top_cpv_prev), 0)
      * 100
    )::NUMERIC(10,2)
  END AS top_cpv_vs_prev1_pct;
$$;

COMMIT;
