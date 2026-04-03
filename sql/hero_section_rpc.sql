-- Hero section RPC based on:
-- - procurement.contract_signed_date (date axis and windows)
-- - payment.amount_without_vat (amount metric)
-- No diavgeia payment usage.

CREATE OR REPLACE FUNCTION public.get_hero_section_data(
  p_year_main integer,
  p_year_start integer DEFAULT 2024
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_as_of_date date;
  v_end_month int;
  v_end_day int;
  v_period_main_start date;
  v_period_main_end date;
  v_period_prev1_start date;
  v_period_prev1_end date;
  v_period_prev2_start date;
  v_period_prev2_end date;
  v_total_main numeric := 0;
  v_total_prev1 numeric := 0;
  v_total_prev2 numeric := 0;
  v_top_contract_type text := null;
  v_top_contract_type_count int := 0;
  v_top_contract_type_prev1_count int := 0;
  v_top_cpv_text text := null;
  v_top_cpv_count int := 0;
  v_top_cpv_prev1_count int := 0;
  v_curve jsonb := '[]'::jsonb;
BEGIN
  v_as_of_date := LEAST(CURRENT_DATE, make_date(p_year_main, 12, 31));
  IF v_as_of_date < make_date(p_year_main, 1, 1) THEN
    v_as_of_date := make_date(p_year_main, 1, 1);
  END IF;

  v_end_month := EXTRACT(MONTH FROM v_as_of_date);
  v_end_day := EXTRACT(DAY FROM v_as_of_date);

  v_period_main_start := make_date(p_year_main, 1, 1);
  v_period_main_end := v_as_of_date;

  v_period_prev1_start := make_date(p_year_main - 1, 1, 1);
  v_period_prev1_end := make_date(
    p_year_main - 1,
    v_end_month,
    LEAST(v_end_day, EXTRACT(DAY FROM (date_trunc('month', make_date(p_year_main - 1, v_end_month, 1)) + interval '1 month - 1 day'))::int)
  );

  v_period_prev2_start := make_date(p_year_main - 2, 1, 1);
  v_period_prev2_end := make_date(
    p_year_main - 2,
    v_end_month,
    LEAST(v_end_day, EXTRACT(DAY FROM (date_trunc('month', make_date(p_year_main - 2, v_end_month, 1)) + interval '1 month - 1 day'))::int)
  );

  WITH pay_by_proc AS (
    SELECT
      py.procurement_id,
      SUM(py.amount_without_vat) AS amount_without_vat
    FROM public.payment py
    WHERE py.amount_without_vat IS NOT NULL
    GROUP BY py.procurement_id
  ),
  proc_ranked AS (
    SELECT
      p.id,
      p.contract_signed_date,
      public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
        ORDER BY p.id DESC
      ) AS rn
    FROM public.procurement p
    JOIN pay_by_proc pp ON pp.procurement_id = p.id
    WHERE p.contract_signed_date IS NOT NULL
      AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.procurement p2
        WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
      )
  ),
  rows_base AS (
    SELECT
      pr.id,
      pr.contract_signed_date AS d,
      pr.procedure_type_value,
      pb.amount_without_vat
    FROM proc_ranked pr
    JOIN pay_by_proc pb ON pb.procurement_id = pr.id
    WHERE pr.rn = 1
  ),
  main_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_main_start AND v_period_main_end
  ),
  prev1_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_prev1_start AND v_period_prev1_end
  ),
  prev2_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_prev2_start AND v_period_prev2_end
  )
  SELECT
    COALESCE((SELECT SUM(amount_without_vat) FROM main_rows), 0),
    COALESCE((SELECT SUM(amount_without_vat) FROM prev1_rows), 0),
    COALESCE((SELECT SUM(amount_without_vat) FROM prev2_rows), 0)
  INTO v_total_main, v_total_prev1, v_total_prev2;

  WITH pay_by_proc AS (
    SELECT
      py.procurement_id,
      SUM(py.amount_without_vat) AS amount_without_vat
    FROM public.payment py
    WHERE py.amount_without_vat IS NOT NULL
    GROUP BY py.procurement_id
  ),
  proc_ranked AS (
    SELECT
      p.id,
      p.contract_signed_date,
      public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
        ORDER BY p.id DESC
      ) AS rn
    FROM public.procurement p
    JOIN pay_by_proc pp ON pp.procurement_id = p.id
    WHERE p.contract_signed_date IS NOT NULL
      AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.procurement p2
        WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
      )
  ),
  rows_base AS (
    SELECT
      pr.id,
      pr.contract_signed_date AS d,
      pr.procedure_type_value
    FROM proc_ranked pr
    WHERE pr.rn = 1
  ),
  main_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_main_start AND v_period_main_end
  ),
  prev1_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_prev1_start AND v_period_prev1_end
  ),
  ranked AS (
    SELECT COALESCE(procedure_type_value, '—') AS procedure_name, COUNT(*)::int AS cnt
    FROM main_rows
    GROUP BY COALESCE(procedure_type_value, '—')
    ORDER BY cnt DESC
    LIMIT 1
  )
  SELECT
    r.procedure_name,
    r.cnt,
    COALESCE((
      SELECT COUNT(*)::int
      FROM prev1_rows p1
      WHERE COALESCE(p1.procedure_type_value, '—') = r.procedure_name
    ), 0)
  INTO v_top_contract_type, v_top_contract_type_count, v_top_contract_type_prev1_count
  FROM ranked r;

  WITH pay_by_proc AS (
    SELECT py.procurement_id
    FROM public.payment py
    WHERE py.amount_without_vat IS NOT NULL
    GROUP BY py.procurement_id
  ),
  proc_ranked AS (
    SELECT
      p.id,
      p.contract_signed_date,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
        ORDER BY p.id DESC
      ) AS rn
    FROM public.procurement p
    JOIN pay_by_proc pp ON pp.procurement_id = p.id
    WHERE p.contract_signed_date IS NOT NULL
      AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.procurement p2
        WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
      )
  ),
  rows_base AS (
    SELECT pr.id, pr.contract_signed_date AS d
    FROM proc_ranked pr
    WHERE pr.rn = 1
  ),
  main_ids AS (
    SELECT id FROM rows_base WHERE d BETWEEN v_period_main_start AND v_period_main_end
  ),
  prev1_ids AS (
    SELECT id FROM rows_base WHERE d BETWEEN v_period_prev1_start AND v_period_prev1_end
  ),
  top_cpv AS (
    SELECT c.cpv_value, COUNT(*)::int AS cnt
    FROM public.cpv c
    JOIN main_ids m ON m.id = c.procurement_id
    WHERE c.cpv_value IS NOT NULL
    GROUP BY c.cpv_value
    ORDER BY cnt DESC
    LIMIT 1
  )
  SELECT
    t.cpv_value,
    t.cnt,
    COALESCE((
      SELECT COUNT(*)::int
      FROM public.cpv c2
      JOIN prev1_ids p1 ON p1.id = c2.procurement_id
      WHERE c2.cpv_value = t.cpv_value
    ), 0)
  INTO v_top_cpv_text, v_top_cpv_count, v_top_cpv_prev1_count
  FROM top_cpv t;

  WITH bounds AS (
    SELECT
      y AS series_year,
      make_date(y, 12, 31) AS year_full_end,
      CASE
        WHEN y = p_year_main THEN make_date(
          y,
          v_end_month,
          LEAST(v_end_day, EXTRACT(DAY FROM (date_trunc('month', make_date(y, v_end_month, 1)) + interval '1 month - 1 day'))::int)
        )
        ELSE make_date(y, 12, 31)
      END AS year_draw_end
    FROM generate_series(p_year_start, p_year_main) y
  ),
  pay_by_proc AS (
    SELECT
      py.procurement_id,
      SUM(py.amount_without_vat) AS amount_without_vat
    FROM public.payment py
    WHERE py.amount_without_vat IS NOT NULL
    GROUP BY py.procurement_id
  ),
  proc_ranked AS (
    SELECT
      p.id,
      p.contract_signed_date,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
        ORDER BY p.id DESC
      ) AS rn
    FROM public.procurement p
    JOIN pay_by_proc pp ON pp.procurement_id = p.id
    WHERE p.contract_signed_date IS NOT NULL
  ),
  rows_base AS (
    SELECT
      EXTRACT(YEAR FROM pr.contract_signed_date)::int AS series_year,
      pr.contract_signed_date AS point_date,
      pb.amount_without_vat AS amount_without_vat
    FROM proc_ranked pr
    JOIN pay_by_proc pb ON pb.procurement_id = pr.id
    WHERE pr.rn = 1
      AND EXTRACT(YEAR FROM pr.contract_signed_date)::int BETWEEN p_year_start AND p_year_main
  ),
  daily_agg AS (
    SELECT
      series_year,
      point_date,
      SUM(amount_without_vat) AS daily_amount
    FROM rows_base
    GROUP BY series_year, point_date
  ),
  cal AS (
    SELECT
      b.series_year,
      gs::date AS point_date,
      EXTRACT(DOY FROM gs)::int AS day_of_year,
      EXTRACT(DOY FROM b.year_full_end)::int AS year_days
    FROM bounds b
    CROSS JOIN LATERAL generate_series(make_date(b.series_year, 1, 1), b.year_draw_end, interval '1 day') gs
  ),
  curve AS (
    SELECT
      c.series_year,
      c.point_date,
      c.day_of_year,
      c.year_days,
      SUM(COALESCE(d.daily_amount, 0)) OVER (
        PARTITION BY c.series_year
        ORDER BY c.point_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS cumulative_amount
    FROM cal c
    LEFT JOIN daily_agg d
      ON d.series_year = c.series_year
     AND d.point_date = c.point_date
  )
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'series_year', series_year,
        'point_date', to_char(point_date, 'YYYY-MM-DD'),
        'day_of_year', day_of_year,
        'year_days', year_days,
        'cumulative_amount', cumulative_amount
      )
      ORDER BY series_year, day_of_year
    ),
    '[]'::jsonb
  )
  INTO v_curve
  FROM curve;

  RETURN jsonb_build_object(
    'period_main_start', to_char(v_period_main_start, 'YYYY-MM-DD'),
    'period_main_end', to_char(v_period_main_end, 'YYYY-MM-DD'),
    'total_main', v_total_main,
    'total_prev1', v_total_prev1,
    'total_prev2', v_total_prev2,
    'top_contract_type', v_top_contract_type,
    'top_contract_type_count', v_top_contract_type_count,
    'top_contract_type_prev1_count', v_top_contract_type_prev1_count,
    'top_cpv_text', v_top_cpv_text,
    'top_cpv_count', v_top_cpv_count,
    'top_cpv_prev1_count', v_top_cpv_prev1_count,
    'curve_points', v_curve
  );
END;
$$;

GRANT EXECUTE ON FUNCTION public.get_hero_section_data(integer, integer) TO anon, authenticated, service_role;
