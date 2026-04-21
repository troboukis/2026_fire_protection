-- =============================================================
-- Frontend API Setup (single-run bundle)
-- Purpose:
--   1) Create/refresh SQL functions used by frontend RPC calls.
--   2) Grant execute/select privileges for anon/authenticated/service_role.
--   3) Force PostgREST schema reload.
--   4) Provide quick verification queries.
--
-- Run this whole file in Supabase SQL Editor.
-- =============================================================

-- -------------------------------------------------------------
-- A) Procedure normalization function
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.normalize_procedure_type(p_val text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_val IS NULL OR btrim(p_val) = '' THEN '—'
    WHEN lower(btrim(p_val)) LIKE 'απευθείας ανάθεση%' THEN 'Απευθείας ανάθεση'
    ELSE btrim(p_val)
  END;
$$;

GRANT EXECUTE ON FUNCTION public.normalize_procedure_type(text) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- B) Hero section RPC
-- Source: sql/hero_section_rpc.sql
-- -------------------------------------------------------------
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
    FROM (
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
    ) b
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

-- -------------------------------------------------------------
-- C) Contracts page RPC
-- Source: sql/contracts_page_rpc.sql
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_contracts_page(
  p_q text DEFAULT NULL,
  p_org text DEFAULT NULL,
  p_procedure text DEFAULT NULL,
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL,
  p_min_amount numeric DEFAULT NULL,
  p_page integer DEFAULT 1,
  p_page_size integer DEFAULT 50
)
RETURNS TABLE (
  id bigint,
  contract_signed_date date,
  organization_value text,
  title text,
  reference_number text,
  cpv_value text,
  procedure_type_value text,
  beneficiary_name text,
  beneficiary_vat_number text,
  amount_without_vat numeric,
  diavgeia_ada text,
  total_count bigint
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(py.amount_without_vat) AS amount_without_vat,
    STRING_AGG(DISTINCT NULLIF(TRIM(py.beneficiary_name), ''), ' | ') AS beneficiary_name,
    STRING_AGG(DISTINCT NULLIF(TRIM(py.beneficiary_vat_number), ''), ' | ') AS beneficiary_vat_number
  FROM public.payment py
  GROUP BY py.procurement_id
),
cpv_agg AS (
  SELECT
    c.procurement_id,
    STRING_AGG(
      DISTINCT NULLIF(TRIM(c.cpv_value), ''),
      ' | '
      ORDER BY NULLIF(TRIM(c.cpv_value), '')
    ) AS cpv_value
  FROM public.cpv c
  GROUP BY c.procurement_id
),
base AS (
  SELECT
    p.id,
    p.contract_signed_date,
    p.title,
    public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
    p.diavgeia_ada,
    p.reference_number,
    p.contract_number,
    p.organization_key,
    COALESCE(pa.amount_without_vat, p.contract_budget, p.budget) AS amount_without_vat,
    pa.beneficiary_name,
    pa.beneficiary_vat_number,
    COALESCE(
      org.organization_value,
      CASE
        WHEN p.canonical_owner_scope = 'region' THEN COALESCE(
          CASE WHEN reg.region_normalized_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_normalized_value) END,
          CASE WHEN reg.region_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_value) END,
          CASE WHEN muni.municipality_normalized_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_normalized_value) END,
          CASE WHEN muni.municipality_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_value) END
        )
        ELSE COALESCE(
          CASE WHEN muni.municipality_normalized_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_normalized_value) END,
          CASE WHEN reg.region_normalized_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_normalized_value) END,
          CASE WHEN muni.municipality_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_value) END,
          CASE WHEN reg.region_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_value) END
        )
      END
    ) AS organization_value,
    ca.cpv_value,
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
  LEFT JOIN payment_agg pa ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT
      m.municipality_normalized_value,
      m.municipality_value
    FROM public.municipality m
    WHERE m.municipality_key = p.municipality_key
    ORDER BY m.id
    LIMIT 1
  ) muni ON TRUE
  LEFT JOIN LATERAL (
    SELECT
      r.region_normalized_value,
      r.region_value
    FROM public.region r
    WHERE r.region_key = p.region_key
    ORDER BY r.id
    LIMIT 1
  ) reg ON TRUE
  LEFT JOIN LATERAL (
    SELECT o.organization_normalized_value AS organization_value
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  LEFT JOIN cpv_agg ca ON ca.procurement_id = p.id
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
),
dedup AS (
  SELECT *
  FROM base
  WHERE rn = 1
),
filtered AS (
  SELECT *
  FROM dedup d
  WHERE (p_date_from IS NULL OR d.contract_signed_date >= p_date_from)
    AND (p_date_to IS NULL OR d.contract_signed_date <= p_date_to)
    AND (p_min_amount IS NULL OR COALESCE(d.amount_without_vat, 0) >= p_min_amount)
    AND (p_procedure IS NULL OR p_procedure = '' OR d.procedure_type_value = p_procedure)
    AND (p_org IS NULL OR p_org = '' OR COALESCE(d.organization_value, '') ILIKE '%' || p_org || '%')
    AND (
      p_q IS NULL OR p_q = '' OR
      upper(
        translate(
          CONCAT_WS(
            ' ',
            COALESCE(d.title, ''),
            COALESCE(d.organization_value, ''),
            COALESCE(d.beneficiary_name, ''),
            COALESCE(d.cpv_value, ''),
            COALESCE(d.reference_number, '')
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
  SELECT f.*, COUNT(*) OVER () AS total_count
  FROM filtered f
)
SELECT
  id,
  contract_signed_date,
  organization_value,
  title,
  reference_number,
  cpv_value,
  procedure_type_value,
  beneficiary_name,
  beneficiary_vat_number,
  amount_without_vat,
  diavgeia_ada,
  total_count
FROM counted
ORDER BY contract_signed_date DESC NULLS LAST, id DESC
OFFSET GREATEST((p_page - 1) * p_page_size, 0)
LIMIT GREATEST(p_page_size, 1);
$$;

GRANT EXECUTE ON FUNCTION public.get_contracts_page(text, text, text, date, date, numeric, integer, integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- D) Featured records RPC
-- Source: sql/featured_records_rpc.sql
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_featured_beneficiaries(
  p_year_main integer,
  p_limit integer DEFAULT 50
)
RETURNS TABLE (
  beneficiary_vat_number text,
  beneficiary_name text,
  organization text,
  total_amount numeric,
  contract_count integer,
  cpv text,
  start_date date,
  end_date date,
  duration_days integer,
  progress_pct numeric,
  signer text,
  relevant_contracts jsonb
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH proc_ranked AS (
  SELECT
    p.id AS procurement_id,
    p.organization_key,
    p.title,
    p.submission_at,
    p.contract_signed_date,
    p.short_descriptions,
    public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
    p.reference_number,
    p.contract_number,
    p.contract_budget,
    p.budget,
    p.assign_criteria,
    p.contract_type,
    p.award_procedure,
    p.units_operator,
    p.funding_details_cofund,
    p.funding_details_self_fund,
    p.funding_details_espa,
    p.funding_details_regular_budget,
    p.auction_ref_no,
    p.prev_reference_no,
    p.next_ref_no,
    p.contract_related_ada,
    p.organization_vat_number,
    p.start_date,
    p.end_date,
    p.diavgeia_ada,
    COALESCE(py.amount_without_vat, 0) AS amount_without_vat,
    py.amount_with_vat,
    pb.beneficiary_vat_number,
    COALESCE(
      NULLIF(TRIM(b.beneficiary_name), ''),
      NULLIF(TRIM(pb.beneficiary_vat_number), '')
    ) AS beneficiary_name,
    py.signers,
    py.payment_ref_no,
    ROW_NUMBER() OVER (
      PARTITION BY
        COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        ),
        pb.beneficiary_vat_number
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  JOIN public.payment py
    ON py.procurement_id = p.id
  JOIN public.payment_beneficiary pb
    ON pb.payment_id = py.id
  LEFT JOIN public.beneficiary b
    ON b.beneficiary_vat_number = pb.beneficiary_vat_number
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND p.contract_signed_date BETWEEN make_date(p_year_main, 1, 1) AND make_date(p_year_main, 12, 31)
    AND NULLIF(TRIM(pb.beneficiary_vat_number), '') IS NOT NULL
),
dedup_base AS (
  SELECT
    pr.procurement_id,
    pr.organization_key,
    pr.title,
    pr.submission_at,
    pr.contract_signed_date,
    pr.short_descriptions,
    pr.procedure_type_value,
    pr.reference_number,
    pr.contract_number,
    pr.contract_budget,
    pr.budget,
    pr.assign_criteria,
    pr.contract_type,
    pr.award_procedure,
    pr.units_operator,
    pr.funding_details_cofund,
    pr.funding_details_self_fund,
    pr.funding_details_espa,
    pr.funding_details_regular_budget,
    pr.auction_ref_no,
    pr.prev_reference_no,
    pr.next_ref_no,
    pr.contract_related_ada,
    pr.organization_vat_number,
    pr.start_date,
    pr.end_date,
    pr.diavgeia_ada,
    pr.amount_without_vat,
    pr.amount_with_vat,
    pr.beneficiary_vat_number,
    COALESCE(pr.beneficiary_name, pr.beneficiary_vat_number) AS beneficiary_name,
    COALESCE(NULLIF(TRIM(pr.signers), ''), '—') AS signers,
    pr.payment_ref_no
  FROM proc_ranked pr
  WHERE pr.rn = 1
),
beneficiary_totals AS (
  SELECT
    b.beneficiary_vat_number,
    SUM(b.amount_without_vat) AS total_amount,
    COUNT(*)::integer AS contract_count,
    MIN(b.start_date) AS start_date,
    MAX(b.end_date) AS end_date
  FROM dedup_base b
  GROUP BY b.beneficiary_vat_number
),
top_beneficiaries AS (
  SELECT
    bt.beneficiary_vat_number,
    bt.total_amount,
    bt.contract_count,
    bt.start_date,
    bt.end_date
  FROM beneficiary_totals bt
  ORDER BY bt.total_amount DESC, bt.contract_count DESC, bt.beneficiary_vat_number
  LIMIT GREATEST(COALESCE(p_limit, 50), 1)
),
base AS (
  SELECT
    db.procurement_id,
    db.organization_key,
    db.title,
    db.submission_at,
    db.contract_signed_date,
    db.short_descriptions,
    db.procedure_type_value,
    db.reference_number,
    db.contract_number,
    db.contract_budget,
    db.budget,
    db.assign_criteria,
    db.contract_type,
    db.award_procedure,
    db.units_operator,
    db.funding_details_cofund,
    db.funding_details_self_fund,
    db.funding_details_espa,
    db.funding_details_regular_budget,
    db.auction_ref_no,
    db.prev_reference_no,
    db.next_ref_no,
    db.contract_related_ada,
    db.organization_vat_number,
    db.start_date,
    db.end_date,
    db.diavgeia_ada,
    db.amount_without_vat,
    db.amount_with_vat,
    db.beneficiary_vat_number,
    db.beneficiary_name,
    db.signers,
    db.payment_ref_no
  FROM dedup_base db
  JOIN top_beneficiaries tb
    ON tb.beneficiary_vat_number = db.beneficiary_vat_number
),
org_lookup AS (
  SELECT DISTINCT ON (o.organization_key)
    o.organization_key,
    COALESCE(o.organization_normalized_value, o.organization_value, o.organization_key, '—') AS organization_value
  FROM public.organization o
  JOIN (
    SELECT DISTINCT b.organization_key
    FROM base b
    WHERE NULLIF(TRIM(b.organization_key), '') IS NOT NULL
  ) bo
    ON bo.organization_key = o.organization_key
  ORDER BY o.organization_key, o.id
),
cpv_dedup AS (
  SELECT DISTINCT
    c.procurement_id,
    NULLIF(TRIM(c.cpv_key), '') AS cpv_key,
    NULLIF(TRIM(c.cpv_value), '') AS cpv_value
  FROM public.cpv c
  JOIN (
    SELECT DISTINCT b.procurement_id
    FROM base b
  ) bp
    ON bp.procurement_id = c.procurement_id
  WHERE NULLIF(TRIM(c.cpv_key), '') IS NOT NULL
     OR NULLIF(TRIM(c.cpv_value), '') IS NOT NULL
),
cpv_agg AS (
  SELECT
    cd.procurement_id,
    jsonb_agg(
      jsonb_build_object(
        'code', COALESCE(cd.cpv_key, '—'),
        'label', COALESCE(cd.cpv_value, '—')
      )
      ORDER BY COALESCE(cd.cpv_value, ''), COALESCE(cd.cpv_key, '')
    ) AS cpv_items
  FROM cpv_dedup cd
  GROUP BY cd.procurement_id
),
base_enriched AS (
  SELECT
    b.procurement_id,
    b.organization_key,
    COALESCE(ol.organization_value, b.organization_key, '—') AS organization_value,
    b.title,
    b.submission_at,
    b.contract_signed_date,
    b.short_descriptions,
    b.procedure_type_value,
    b.reference_number,
    b.contract_number,
    b.contract_budget,
    b.budget,
    b.assign_criteria,
    b.contract_type,
    b.award_procedure,
    b.units_operator,
    b.funding_details_cofund,
    b.funding_details_self_fund,
    b.funding_details_espa,
    b.funding_details_regular_budget,
    b.auction_ref_no,
    b.prev_reference_no,
    b.next_ref_no,
    b.contract_related_ada,
    b.organization_vat_number,
    b.start_date,
    b.end_date,
    b.diavgeia_ada,
    b.amount_without_vat,
    b.amount_with_vat,
    b.beneficiary_vat_number,
    b.beneficiary_name,
    b.signers,
    b.payment_ref_no,
    COALESCE(ca.cpv_items, '[]'::jsonb) AS cpv_items
  FROM base b
  LEFT JOIN org_lookup ol
    ON ol.organization_key = b.organization_key
  LEFT JOIN cpv_agg ca
    ON ca.procurement_id = b.procurement_id
),
beneficiary_name_latest AS (
  SELECT
    b.beneficiary_vat_number,
    b.beneficiary_name,
    ROW_NUMBER() OVER (
      PARTITION BY b.beneficiary_vat_number
      ORDER BY b.contract_signed_date DESC NULLS LAST, b.procurement_id DESC, b.beneficiary_name
    ) AS rn
  FROM base_enriched b
),
beneficiary_name_ranked AS (
  SELECT
    bnl.beneficiary_vat_number,
    bnl.beneficiary_name,
    bnl.rn
  FROM beneficiary_name_latest bnl
),
beneficiary_org_totals AS (
  SELECT
    b.beneficiary_vat_number,
    b.organization_value,
    SUM(b.amount_without_vat) AS total_amount
  FROM base_enriched b
  GROUP BY b.beneficiary_vat_number, b.organization_value
),
beneficiary_org_ranked AS (
  SELECT
    bot.beneficiary_vat_number,
    bot.organization_value,
    ROW_NUMBER() OVER (
      PARTITION BY bot.beneficiary_vat_number
      ORDER BY bot.total_amount DESC, bot.organization_value
    ) AS rn
  FROM beneficiary_org_totals bot
),
beneficiary_signer_counts AS (
  SELECT
    b.beneficiary_vat_number,
    b.signers,
    COUNT(*) AS signer_count
  FROM base_enriched b
  WHERE NULLIF(TRIM(b.signers), '') IS NOT NULL
  GROUP BY b.beneficiary_vat_number, b.signers
),
beneficiary_signer_ranked AS (
  SELECT
    bsc.beneficiary_vat_number,
    bsc.signers,
    ROW_NUMBER() OVER (
      PARTITION BY bsc.beneficiary_vat_number
      ORDER BY bsc.signer_count DESC, bsc.signers
    ) AS rn
  FROM beneficiary_signer_counts bsc
),
beneficiary_cpv_counts AS (
  SELECT
    b.beneficiary_vat_number,
    cpv_item ->> 'label' AS cpv_label,
    COUNT(*) AS cpv_count
  FROM base_enriched b
  CROSS JOIN LATERAL jsonb_array_elements(b.cpv_items) cpv_item
  WHERE NULLIF(TRIM(cpv_item ->> 'label'), '') IS NOT NULL
  GROUP BY b.beneficiary_vat_number, cpv_item ->> 'label'
),
beneficiary_cpv_ranked AS (
  SELECT
    bcc.beneficiary_vat_number,
    bcc.cpv_label,
    ROW_NUMBER() OVER (
      PARTITION BY bcc.beneficiary_vat_number
      ORDER BY bcc.cpv_count DESC, bcc.cpv_label
    ) AS rn
  FROM beneficiary_cpv_counts bcc
),
relevant_ranked AS (
  SELECT
    b.beneficiary_vat_number,
    b.procurement_id,
    jsonb_build_object(
      'id', b.procurement_id,
      'organization', b.organization_value,
      'title', b.title,
      'submission_at', b.submission_at,
      'short_description', split_part(COALESCE(b.short_descriptions, ''), ' | ', 1),
      'procedure_type_value', b.procedure_type_value,
      'amount_without_vat', b.amount_without_vat,
      'amount_with_vat', b.amount_with_vat,
      'reference_number', b.reference_number,
      'contract_number', b.contract_number,
      'cpv_items', b.cpv_items,
      'contract_signed_date', b.contract_signed_date,
      'start_date', b.start_date,
      'end_date', b.end_date,
      'organization_vat_number', b.organization_vat_number,
      'beneficiary_vat_number', b.beneficiary_vat_number,
      'beneficiary_name', b.beneficiary_name,
      'signers', b.signers,
      'assign_criteria', b.assign_criteria,
      'contract_type', b.contract_type,
      'award_procedure', b.award_procedure,
      'units_operator', b.units_operator,
      'funding_details_cofund', b.funding_details_cofund,
      'funding_details_self_fund', b.funding_details_self_fund,
      'funding_details_espa', b.funding_details_espa,
      'funding_details_regular_budget', b.funding_details_regular_budget,
      'auction_ref_no', b.auction_ref_no,
      'prev_reference_no', b.prev_reference_no,
      'next_ref_no', b.next_ref_no,
      'contract_related_ada', b.contract_related_ada,
      'payment_ref_no', b.payment_ref_no,
      'budget', b.budget,
      'contract_budget', b.contract_budget,
      'diavgeia_ada', b.diavgeia_ada
    ) AS contract_json,
    ROW_NUMBER() OVER (
      PARTITION BY b.beneficiary_vat_number
      ORDER BY b.amount_without_vat DESC, b.contract_signed_date DESC NULLS LAST, b.procurement_id DESC
    ) AS rn
  FROM base_enriched b
),
relevant_agg AS (
  SELECT
    rr.beneficiary_vat_number,
    jsonb_agg(rr.contract_json ORDER BY rr.rn) AS relevant_contracts
  FROM relevant_ranked rr
  GROUP BY rr.beneficiary_vat_number
)
SELECT
  tb.beneficiary_vat_number,
  COALESCE(bnr.beneficiary_name, tb.beneficiary_vat_number) AS beneficiary_name,
  COALESCE(bor.organization_value, '—') AS organization,
  tb.total_amount,
  tb.contract_count,
  COALESCE(bcr.cpv_label, '—') AS cpv,
  tb.start_date,
  tb.end_date,
  CASE
    WHEN tb.start_date IS NULL OR tb.end_date IS NULL OR tb.end_date < tb.start_date THEN NULL
    ELSE (tb.end_date - tb.start_date + 1)
  END::integer AS duration_days,
  CASE
    WHEN tb.start_date IS NULL OR tb.end_date IS NULL OR tb.end_date <= tb.start_date THEN NULL
    WHEN CURRENT_DATE <= tb.start_date THEN 0
    WHEN CURRENT_DATE >= tb.end_date THEN 100
    ELSE ROUND((((CURRENT_DATE - tb.start_date)::numeric / NULLIF((tb.end_date - tb.start_date)::numeric, 0)) * 100)::numeric, 2)
  END AS progress_pct,
  COALESCE(bsr.signers, '—') AS signer,
  COALESCE(ra.relevant_contracts, '[]'::jsonb) AS relevant_contracts
FROM top_beneficiaries tb
LEFT JOIN beneficiary_name_ranked bnr
  ON bnr.beneficiary_vat_number = tb.beneficiary_vat_number
 AND bnr.rn = 1
LEFT JOIN beneficiary_org_ranked bor
  ON bor.beneficiary_vat_number = tb.beneficiary_vat_number
 AND bor.rn = 1
LEFT JOIN beneficiary_signer_ranked bsr
  ON bsr.beneficiary_vat_number = tb.beneficiary_vat_number
 AND bsr.rn = 1
LEFT JOIN beneficiary_cpv_ranked bcr
  ON bcr.beneficiary_vat_number = tb.beneficiary_vat_number
 AND bcr.rn = 1
LEFT JOIN relevant_agg ra
  ON ra.beneficiary_vat_number = tb.beneficiary_vat_number
ORDER BY tb.total_amount DESC, tb.contract_count DESC, tb.beneficiary_vat_number;
$$;

GRANT EXECUTE ON FUNCTION public.get_featured_beneficiaries(integer, integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- E) Municipality map spend RPC
-- Source: sql/029_municipality_map_spend_rpc.sql
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_municipality_map_spend_per_100k(integer);

CREATE OR REPLACE FUNCTION public.get_municipality_map_spend_per_100k(
  p_year integer
)
RETURNS TABLE (
  municipality_key text,
  municipality_name text,
  population_total numeric,
  total_amount_without_vat numeric,
  amount_per_100k numeric,
  signed_current_count bigint,
  active_previous_count bigint
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(py.amount_without_vat) AS amount_without_vat
  FROM public.payment py
  WHERE py.amount_without_vat IS NOT NULL
  GROUP BY py.procurement_id
),
direct_municipality AS (
  SELECT
    p.id AS procurement_id,
    p.municipality_key AS target_municipality_key,
    pa.amount_without_vat,
    p.reference_number,
    p.diavgeia_ada,
    p.title,
    p.contract_signed_date,
    p.organization_key,
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
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND p.contract_signed_date IS NOT NULL
    AND p.contract_signed_date <= make_date(p_year, 12, 31)
    AND (
      p.contract_signed_date >= make_date(p_year, 1, 1)
      OR (
        p.contract_signed_date < make_date(p_year, 1, 1)
        AND p.end_date IS NOT NULL
        AND p.end_date >= make_date(p_year, 1, 1)
      )
    )
    AND p.municipality_key IS NOT NULL
    AND (
      p.canonical_owner_scope = 'municipality'
      OR COALESCE(org.authority_scope, 'other') = 'municipality'
    )
),
attributed_organization AS (
  SELECT
    p.id AS procurement_id,
    p.municipality_key AS target_municipality_key,
    pa.amount_without_vat,
    p.reference_number,
    p.diavgeia_ada,
    p.title,
    p.contract_signed_date,
    p.organization_key,
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
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND p.contract_signed_date IS NOT NULL
    AND p.contract_signed_date <= make_date(p_year, 12, 31)
    AND (
      p.contract_signed_date >= make_date(p_year, 1, 1)
      OR (
        p.contract_signed_date < make_date(p_year, 1, 1)
        AND p.end_date IS NOT NULL
        AND p.end_date >= make_date(p_year, 1, 1)
      )
    )
    AND p.municipality_key IS NOT NULL
    AND p.organization_key IS NOT NULL
    AND COALESCE(p.canonical_owner_scope, '') = 'organization'
    AND COALESCE(org.authority_scope, 'other') = 'municipality'
),
coverage_organization AS (
  SELECT
    p.id AS procurement_id,
    omc.municipality_key AS target_municipality_key,
    pa.amount_without_vat,
    p.reference_number,
    p.diavgeia_ada,
    p.title,
    p.contract_signed_date,
    p.organization_key,
    ROW_NUMBER() OVER (
      PARTITION BY
        omc.municipality_key,
        COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  JOIN public.org_municipality_coverage omc
    ON omc.organization_key = p.organization_key
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND p.contract_signed_date IS NOT NULL
    AND p.contract_signed_date <= make_date(p_year, 12, 31)
    AND (
      p.contract_signed_date >= make_date(p_year, 1, 1)
      OR (
        p.contract_signed_date < make_date(p_year, 1, 1)
        AND p.end_date IS NOT NULL
        AND p.end_date >= make_date(p_year, 1, 1)
      )
    )
    AND COALESCE(org.authority_scope, 'other') = 'municipality'
),
unioned AS (
  SELECT *, 0 AS source_priority FROM direct_municipality WHERE rn = 1
  UNION ALL
  SELECT *, 1 AS source_priority FROM coverage_organization WHERE rn = 1
  UNION ALL
  SELECT *, 2 AS source_priority FROM attributed_organization WHERE rn = 1
),
dedup AS (
  SELECT *
  FROM (
    SELECT
      u.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          u.target_municipality_key,
          COALESCE(
            NULLIF(TRIM(u.reference_number), ''),
            NULLIF(TRIM(u.diavgeia_ada), ''),
            CONCAT_WS('|', COALESCE(u.organization_key, ''), COALESCE(u.title, ''), COALESCE(u.contract_signed_date::text, ''))
          )
        ORDER BY u.source_priority ASC, u.procurement_id DESC
      ) AS keep_rn
    FROM unioned u
  ) ranked
  WHERE keep_rn = 1
),
municipality_stats AS (
  SELECT
    d.target_municipality_key AS municipality_key,
    SUM(COALESCE(d.amount_without_vat, 0)) AS total_amount_without_vat,
    COUNT(*) FILTER (
      WHERE d.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
    )::bigint AS signed_current_count,
    COUNT(*) FILTER (
      WHERE d.contract_signed_date < make_date(p_year, 1, 1)
    )::bigint AS active_previous_count
  FROM dedup d
  GROUP BY d.target_municipality_key
)
SELECT
  mfpd.municipality_key,
  mfpd.dhmos AS municipality_name,
  mfpd.plithismos_synolikos AS population_total,
  COALESCE(ms.total_amount_without_vat, 0) AS total_amount_without_vat,
  ROUND(
    (
      COALESCE(ms.total_amount_without_vat, 0) * 100000.0
    ) / NULLIF(mfpd.plithismos_synolikos, 0),
    2
  ) AS amount_per_100k,
  COALESCE(ms.signed_current_count, 0) AS signed_current_count,
  COALESCE(ms.active_previous_count, 0) AS active_previous_count
FROM public.municipality_fire_protection_data mfpd
LEFT JOIN municipality_stats ms
  ON ms.municipality_key = mfpd.municipality_key
WHERE mfpd.plithismos_synolikos IS NOT NULL
  AND mfpd.plithismos_synolikos > 0
ORDER BY amount_per_100k DESC NULLS LAST, total_amount_without_vat DESC, mfpd.dhmos;
$$;

GRANT EXECUTE ON FUNCTION public.get_municipality_map_spend_per_100k(integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- F) Municipality funding map RPC
-- Source: sql/031_municipality_map_funding_rpc.sql
-- -------------------------------------------------------------
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

-- -------------------------------------------------------------
-- G) Homepage funding RPC
-- Source: sql/032_homepage_funding_rpc.sql
-- -------------------------------------------------------------
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

-- -------------------------------------------------------------
-- H) Environment Ministry dashboard RPC
-- Source: sql/030_environment_ministry_dashboard_rpc.sql
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_environment_ministry_dashboard(
  p_year integer DEFAULT 2026
)
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH ministry_org_keys AS (
  SELECT DISTINCT organization_key
  FROM public.organization
  WHERE NULLIF(TRIM(organization_key), '') IS NOT NULL
    AND (
      organization_key = '100015996'
      OR UPPER(BTRIM(COALESCE(organization_normalized_value, organization_value, ''))) = 'ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ'
    )
  UNION
  SELECT '100015996'
),
ministry_name AS (
  SELECT COALESCE(
    MAX(NULLIF(BTRIM(organization_normalized_value), '')),
    MAX(NULLIF(BTRIM(organization_value), '')),
    'Υπουργείο Περιβάλλοντος και Ενέργειας'
  ) AS value
  FROM public.organization
  WHERE organization_key IN (SELECT organization_key FROM ministry_org_keys)
),
payment_agg AS (
  SELECT
    py.procurement_id,
    MAX(COALESCE(py.amount_without_vat, 0)) AS amount_without_vat,
    MAX(COALESCE(py.amount_with_vat, 0)) AS amount_with_vat,
    COALESCE(
      STRING_AGG(
        DISTINCT COALESCE(NULLIF(BTRIM(b.beneficiary_name), ''), NULLIF(BTRIM(pb.beneficiary_vat_number), '')),
        ' | '
        ORDER BY COALESCE(NULLIF(BTRIM(b.beneficiary_name), ''), NULLIF(BTRIM(pb.beneficiary_vat_number), ''))
      ),
      STRING_AGG(DISTINCT NULLIF(BTRIM(py.beneficiary_name), ''), ' | ' ORDER BY NULLIF(BTRIM(py.beneficiary_name), ''))
    ) AS beneficiary_name,
    COALESCE(
      STRING_AGG(DISTINCT NULLIF(BTRIM(pb.beneficiary_vat_number), ''), ' | ' ORDER BY NULLIF(BTRIM(pb.beneficiary_vat_number), '')),
      STRING_AGG(DISTINCT NULLIF(BTRIM(py.beneficiary_vat_number), ''), ' | ' ORDER BY NULLIF(BTRIM(py.beneficiary_vat_number), ''))
    ) AS beneficiary_vat_number,
    STRING_AGG(DISTINCT NULLIF(BTRIM(py.signers), ''), ' | ' ORDER BY NULLIF(BTRIM(py.signers), '')) AS signers,
    STRING_AGG(DISTINCT NULLIF(BTRIM(py.payment_ref_no), ''), ' | ' ORDER BY NULLIF(BTRIM(py.payment_ref_no), '')) AS payment_ref_no,
    MAX(py.fiscal_year) AS fiscal_year
  FROM public.payment py
  LEFT JOIN public.payment_beneficiary pb
    ON pb.payment_id = py.id
  LEFT JOIN public.beneficiary b
    ON b.beneficiary_vat_number = pb.beneficiary_vat_number
  GROUP BY py.procurement_id
),
proc_ranked AS (
  SELECT
    p.id,
    p.title,
    p.submission_at,
    p.contract_signed_date,
    p.start_date,
    p.end_date,
    p.no_end_date,
    p.short_descriptions,
    public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
    p.reference_number,
    p.contract_number,
    p.contract_budget,
    p.budget,
    p.assign_criteria,
    p.contract_type,
    p.award_procedure,
    p.units_operator,
    p.funding_details_cofund,
    p.funding_details_self_fund,
    p.funding_details_espa,
    p.funding_details_regular_budget,
    p.auction_ref_no,
    p.contract_related_ada,
    p.prev_reference_no,
    p.next_ref_no,
    p.diavgeia_ada,
    p.organization_vat_number,
    p.organization_key,
    pa.amount_without_vat,
    pa.amount_with_vat,
    pa.beneficiary_name,
    pa.beneficiary_vat_number,
    pa.signers,
    pa.payment_ref_no,
    pa.fiscal_year,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(p.reference_number), ''),
        NULLIF(BTRIM(p.diavgeia_ada), ''),
        NULLIF(BTRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  WHERE p.organization_key IN (SELECT organization_key FROM ministry_org_keys)
    AND COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = p.reference_number
    )
),
proc_base AS (
  SELECT pr.*
  FROM proc_ranked pr
  WHERE pr.rn = 1
),
proc_app_window AS (
  SELECT *
  FROM proc_base
  WHERE contract_signed_date IS NOT NULL
    AND contract_signed_date >= DATE '2024-01-01'
    AND contract_signed_date <= LEAST(CURRENT_DATE, make_date(p_year, 12, 31))
),
signed_current_contracts AS (
  SELECT *
  FROM proc_base
  WHERE contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
),
current_year_beneficiaries AS (
  SELECT DISTINCT
    NULLIF(BTRIM(pb.beneficiary_vat_number), '') AS beneficiary_key
  FROM signed_current_contracts sc
  JOIN public.payment py
    ON py.procurement_id = sc.id
  JOIN public.payment_beneficiary pb
    ON pb.payment_id = py.id
  WHERE NULLIF(BTRIM(pb.beneficiary_vat_number), '') IS NOT NULL
),
relevant_contracts AS (
  SELECT
    pb.*,
    (
      pb.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
    ) AS signed_current,
    (
      pb.contract_signed_date < make_date(p_year, 1, 1)
    ) AS active_previous
  FROM proc_base pb
  WHERE (
    pb.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
  ) OR (
    pb.contract_signed_date < make_date(p_year, 1, 1)
    AND pb.end_date >= make_date(p_year, 1, 1)
  )
),
active_contracts AS (
  SELECT *
  FROM proc_base pb
  WHERE (
    pb.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
  ) OR (
    pb.contract_signed_date < make_date(p_year, 1, 1)
    AND pb.end_date >= make_date(p_year, 1, 1)
  )
),
cpv_dedup AS (
  SELECT DISTINCT
    c.procurement_id,
    COALESCE(NULLIF(BTRIM(c.cpv_key), ''), '—') AS cpv_key,
    COALESCE(NULLIF(BTRIM(c.cpv_value), ''), '—') AS cpv_value
  FROM public.cpv c
  JOIN proc_base pb
    ON pb.id = c.procurement_id
),
cpv_items AS (
  SELECT
    cd.procurement_id,
    jsonb_agg(
      jsonb_build_object(
        'code', cd.cpv_key,
        'label', cd.cpv_value
      )
      ORDER BY cd.cpv_value, cd.cpv_key
    ) AS items
  FROM cpv_dedup cd
  GROUP BY cd.procurement_id
),
current_year_top_cpvs AS (
  SELECT
    cd.cpv_value AS label,
    MIN(cd.cpv_key) AS code,
    COUNT(DISTINCT cd.procurement_id)::int AS count
  FROM cpv_dedup cd
  JOIN signed_current_contracts sc
    ON sc.id = cd.procurement_id
  WHERE cd.cpv_value <> '—'
  GROUP BY cd.cpv_value
  ORDER BY count DESC, label
  LIMIT 3
),
active_contract_top_cpvs AS (
  SELECT
    cd.cpv_value AS label,
    MIN(cd.cpv_key) AS code,
    COUNT(DISTINCT cd.procurement_id)::int AS count
  FROM cpv_dedup cd
  JOIN active_contracts ac
    ON ac.id = cd.procurement_id
  WHERE cd.cpv_value <> '—'
  GROUP BY cd.cpv_value
  ORDER BY count DESC, label
  LIMIT 3
),
work_points AS (
  SELECT DISTINCT
    w.id,
    w.procurement_id,
    w.lat,
    w.lon,
    COALESCE(NULLIF(BTRIM(w.work), ''), 'Εργασία πυροπροστασίας') AS work,
    COALESCE(
      NULLIF(BTRIM(w.point_name_canonical), ''),
      NULLIF(BTRIM(w.point_name_raw), ''),
      NULLIF(BTRIM(w.formatted_address), ''),
      'Χωρίς τοπωνύμιο'
    ) AS point_name,
    COALESCE(NULLIF(BTRIM(rc.title), ''), '—') AS contract_title,
    COALESCE(rc.amount_without_vat, COALESCE(rc.contract_budget, rc.budget, 0)) AS amount_without_vat,
    COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.beneficiary_name, ''), '|', 1)), ''), '—') AS beneficiary,
    COALESCE(NULLIF(BTRIM(rc.procedure_type_value), ''), '—') AS assignment_type
  FROM public.works_enriched w
  JOIN relevant_contracts rc
    ON rc.id = w.procurement_id
  WHERE w.lat IS NOT NULL
    AND w.lon IS NOT NULL
),
flow_source AS (
  SELECT *
  FROM relevant_contracts
  WHERE fiscal_year = p_year
  UNION ALL
  SELECT *
  FROM relevant_contracts
  WHERE NOT EXISTS (
    SELECT 1
    FROM relevant_contracts rx
    WHERE rx.fiscal_year = p_year
  )
),
flow_grouped AS (
  SELECT
    COALESCE(NULLIF(BTRIM(split_part(COALESCE(signers, ''), '|', 1)), ''), 'Χωρίς υπογράφοντα') AS signer,
    COALESCE(NULLIF(BTRIM(split_part(COALESCE(beneficiary_name, ''), '|', 1)), ''), 'Χωρίς δικαιούχο') AS beneficiary,
    SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0))) AS total_amount,
    COUNT(*)::int AS contract_count,
    (
      ARRAY_AGG(id ORDER BY COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)) DESC, contract_signed_date DESC NULLS LAST, id DESC)
    )[1] AS lead_procurement_id
  FROM flow_source
  GROUP BY 1, 2
),
featured_contracts AS (
  SELECT
    rc.id,
    jsonb_build_object(
      'id', rc.id,
      'who', (SELECT value FROM ministry_name),
      'what', COALESCE(NULLIF(BTRIM(rc.title), ''), '—'),
      'when', rc.submission_at,
      'why', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.short_descriptions, ''), '|', 1)), ''), '—'),
      'beneficiary', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.beneficiary_name, ''), '|', 1)), ''), '—'),
      'contract_type', COALESCE(NULLIF(BTRIM(rc.procedure_type_value), ''), '—'),
      'amount_without_vat', COALESCE(rc.amount_without_vat, COALESCE(rc.contract_budget, rc.budget, 0)),
      'amount_with_vat', rc.amount_with_vat,
      'reference_number', COALESCE(NULLIF(BTRIM(rc.reference_number), ''), '—'),
      'contract_number', COALESCE(NULLIF(BTRIM(rc.contract_number), ''), '—'),
      'cpv_items', COALESCE(ci.items, '[]'::jsonb),
      'contract_signed_date', rc.contract_signed_date,
      'start_date', rc.start_date,
      'end_date', rc.end_date,
      'no_end_date', COALESCE(rc.no_end_date, FALSE),
      'organization_vat_number', COALESCE(NULLIF(BTRIM(rc.organization_vat_number), ''), '—'),
      'beneficiary_vat_number', COALESCE(NULLIF(BTRIM(rc.beneficiary_vat_number), ''), '—'),
      'signers', COALESCE(NULLIF(BTRIM(rc.signers), ''), '—'),
      'assign_criteria', COALESCE(NULLIF(BTRIM(rc.assign_criteria), ''), '—'),
      'contract_kind', COALESCE(NULLIF(BTRIM(rc.contract_type), ''), '—'),
      'award_procedure', COALESCE(NULLIF(BTRIM(rc.award_procedure), ''), '—'),
      'units_operator', COALESCE(NULLIF(BTRIM(rc.units_operator), ''), '—'),
      'funding_cofund', COALESCE(NULLIF(BTRIM(rc.funding_details_cofund), ''), '—'),
      'funding_self', COALESCE(NULLIF(BTRIM(rc.funding_details_self_fund), ''), '—'),
      'funding_espa', COALESCE(NULLIF(BTRIM(rc.funding_details_espa), ''), '—'),
      'funding_regular', COALESCE(NULLIF(BTRIM(rc.funding_details_regular_budget), ''), '—'),
      'auction_ref_no', COALESCE(NULLIF(BTRIM(rc.auction_ref_no), ''), '—'),
      'payment_ref_no', COALESCE(NULLIF(BTRIM(rc.payment_ref_no), ''), '—'),
      'short_description', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.short_descriptions, ''), '|', 1)), ''), '—'),
      'raw_budget', rc.budget,
      'contract_budget', rc.contract_budget,
      'contract_related_ada', COALESCE(NULLIF(BTRIM(rc.contract_related_ada), ''), '—'),
      'previous_reference_number', COALESCE(NULLIF(BTRIM(rc.prev_reference_no), ''), '—'),
      'next_reference_number', COALESCE(NULLIF(BTRIM(rc.next_ref_no), ''), '—'),
      'diavgeia_ada', COALESCE(NULLIF(BTRIM(rc.diavgeia_ada), ''), '—'),
      'payment_fiscal_year', rc.fiscal_year,
      'primary_signer', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.signers, ''), '|', 1)), ''), 'Χωρίς υπογράφοντα'),
      'primary_beneficiary', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.beneficiary_name, ''), '|', 1)), ''), 'Χωρίς δικαιούχο'),
      'primary_beneficiary_vat_number', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.beneficiary_vat_number, ''), '|', 1)), ''), '—')
    ) AS payload,
    COALESCE(rc.amount_without_vat, COALESCE(rc.contract_budget, rc.budget, 0)) AS sort_amount,
    rc.contract_signed_date
  FROM relevant_contracts rc
  LEFT JOIN cpv_items ci
    ON ci.procurement_id = rc.id
),
recent_active_contracts AS (
  SELECT
    ac.id,
    jsonb_build_object(
      'id', ac.id,
      'who', (SELECT value FROM ministry_name),
      'what', COALESCE(NULLIF(BTRIM(ac.title), ''), '—'),
      'when', ac.submission_at,
      'why', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.short_descriptions, ''), '|', 1)), ''), '—'),
      'beneficiary', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.beneficiary_name, ''), '|', 1)), ''), '—'),
      'contract_type', COALESCE(NULLIF(BTRIM(ac.procedure_type_value), ''), '—'),
      'amount_without_vat', COALESCE(ac.amount_without_vat, COALESCE(ac.contract_budget, ac.budget, 0)),
      'amount_with_vat', ac.amount_with_vat,
      'reference_number', COALESCE(NULLIF(BTRIM(ac.reference_number), ''), '—'),
      'contract_number', COALESCE(NULLIF(BTRIM(ac.contract_number), ''), '—'),
      'cpv_items', COALESCE(ci.items, '[]'::jsonb),
      'contract_signed_date', ac.contract_signed_date,
      'start_date', ac.start_date,
      'end_date', ac.end_date,
      'no_end_date', COALESCE(ac.no_end_date, FALSE),
      'organization_vat_number', COALESCE(NULLIF(BTRIM(ac.organization_vat_number), ''), '—'),
      'beneficiary_vat_number', COALESCE(NULLIF(BTRIM(ac.beneficiary_vat_number), ''), '—'),
      'signers', COALESCE(NULLIF(BTRIM(ac.signers), ''), '—'),
      'assign_criteria', COALESCE(NULLIF(BTRIM(ac.assign_criteria), ''), '—'),
      'contract_kind', COALESCE(NULLIF(BTRIM(ac.contract_type), ''), '—'),
      'award_procedure', COALESCE(NULLIF(BTRIM(ac.award_procedure), ''), '—'),
      'units_operator', COALESCE(NULLIF(BTRIM(ac.units_operator), ''), '—'),
      'funding_cofund', COALESCE(NULLIF(BTRIM(ac.funding_details_cofund), ''), '—'),
      'funding_self', COALESCE(NULLIF(BTRIM(ac.funding_details_self_fund), ''), '—'),
      'funding_espa', COALESCE(NULLIF(BTRIM(ac.funding_details_espa), ''), '—'),
      'funding_regular', COALESCE(NULLIF(BTRIM(ac.funding_details_regular_budget), ''), '—'),
      'auction_ref_no', COALESCE(NULLIF(BTRIM(ac.auction_ref_no), ''), '—'),
      'payment_ref_no', COALESCE(NULLIF(BTRIM(ac.payment_ref_no), ''), '—'),
      'short_description', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.short_descriptions, ''), '|', 1)), ''), '—'),
      'raw_budget', ac.budget,
      'contract_budget', ac.contract_budget,
      'contract_related_ada', COALESCE(NULLIF(BTRIM(ac.contract_related_ada), ''), '—'),
      'previous_reference_number', COALESCE(NULLIF(BTRIM(ac.prev_reference_no), ''), '—'),
      'next_reference_number', COALESCE(NULLIF(BTRIM(ac.next_ref_no), ''), '—'),
      'diavgeia_ada', COALESCE(NULLIF(BTRIM(ac.diavgeia_ada), ''), '—'),
      'payment_fiscal_year', ac.fiscal_year,
      'primary_signer', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.signers, ''), '|', 1)), ''), 'Χωρίς υπογράφοντα'),
      'primary_beneficiary', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.beneficiary_name, ''), '|', 1)), ''), 'Χωρίς δικαιούχο'),
      'primary_beneficiary_vat_number', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.beneficiary_vat_number, ''), '|', 1)), ''), '—')
    ) AS payload,
    ac.contract_signed_date
  FROM active_contracts ac
  LEFT JOIN cpv_items ci
    ON ci.procurement_id = ac.id
),
featured_contracts_limited AS (
  SELECT *
  FROM featured_contracts
  ORDER BY sort_amount DESC, contract_signed_date DESC NULLS LAST, id DESC
  LIMIT 8
)
SELECT jsonb_build_object(
  'identification', jsonb_build_object(
    'organization_keys', (
      SELECT jsonb_agg(organization_key ORDER BY organization_key)
      FROM ministry_org_keys
    ),
    'rule', 'procurement.organization_key matches the canonical Environment Ministry organization key set'
  ),
  'ministry_name', (SELECT value FROM ministry_name),
  'total_spend', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM proc_app_window
  ), 0),
  'signed_2026_count', COALESCE((
    SELECT COUNT(*)::int
    FROM signed_current_contracts
  ), 0),
  'signed_current_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM signed_current_contracts
  ), 0),
  'active_carryover_count', COALESCE((
    SELECT COUNT(*)::int
    FROM relevant_contracts
    WHERE active_previous
  ), 0),
  'payment_flow_total', COALESCE((
    SELECT SUM(total_amount)
    FROM flow_grouped
  ), 0),
  'direct_award_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM proc_app_window
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
  ), 0),
  'direct_award_with_auction_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM proc_app_window
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
      AND NULLIF(BTRIM(auction_ref_no), '') IS NOT NULL
  ), 0),
  'direct_award_without_auction_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM proc_app_window
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
      AND NULLIF(BTRIM(auction_ref_no), '') IS NULL
  ), 0),
  'current_year_direct_award_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM signed_current_contracts
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
  ), 0),
  'current_year_direct_award_with_auction_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM signed_current_contracts
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
      AND NULLIF(BTRIM(auction_ref_no), '') IS NOT NULL
  ), 0),
  'current_year_direct_award_without_auction_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM signed_current_contracts
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
      AND NULLIF(BTRIM(auction_ref_no), '') IS NULL
  ), 0),
  'current_year_beneficiary_count', COALESCE((
    SELECT COUNT(*)::int
    FROM current_year_beneficiaries
  ), 0),
  'work_points', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', CONCAT(wp.id::text, '-', wp.procurement_id::text),
        'procurement_id', wp.procurement_id,
        'lat', wp.lat,
        'lon', wp.lon,
        'work', wp.work,
        'point_name', wp.point_name,
        'contract_title', wp.contract_title,
        'amount_without_vat', wp.amount_without_vat,
        'beneficiary', wp.beneficiary,
        'assignment_type', wp.assignment_type
      )
      ORDER BY wp.procurement_id DESC, wp.id DESC
    )
    FROM work_points wp
  ), '[]'::jsonb),
  'top_cpvs', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'label', tc.label,
        'code', tc.code,
        'count', tc.count,
        'share', CASE
          WHEN (SELECT COUNT(*) FROM active_contracts) = 0 THEN 0
          ELSE ROUND((tc.count::numeric / (SELECT COUNT(*)::numeric FROM active_contracts)), 4)
        END
      )
      ORDER BY tc.count DESC, tc.label
    )
    FROM active_contract_top_cpvs tc
  ), '[]'::jsonb),
  'current_year_top_cpvs', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'label', tc.label,
        'code', tc.code,
        'count', tc.count,
        'share', CASE
          WHEN (SELECT COUNT(*) FROM signed_current_contracts) = 0 THEN 0
          ELSE ROUND((tc.count::numeric / (SELECT COUNT(*)::numeric FROM signed_current_contracts)), 4)
        END
      )
      ORDER BY tc.count DESC, tc.label
    )
    FROM current_year_top_cpvs tc
  ), '[]'::jsonb),
  'active_contract_top_cpvs', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'label', tc.label,
        'code', tc.code,
        'count', tc.count,
        'share', CASE
          WHEN (SELECT COUNT(*) FROM active_contracts) = 0 THEN 0
          ELSE ROUND((tc.count::numeric / (SELECT COUNT(*)::numeric FROM active_contracts)), 4)
        END
      )
      ORDER BY tc.count DESC, tc.label
    )
    FROM active_contract_top_cpvs tc
  ), '[]'::jsonb),
  'flow_rows', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'signer', fg.signer,
        'beneficiary', fg.beneficiary,
        'total_amount', fg.total_amount,
        'contract_count', fg.contract_count,
        'ratio', CASE
          WHEN COALESCE((SELECT SUM(total_amount) FROM flow_grouped), 0) = 0 THEN 0
          ELSE ROUND(fg.total_amount / (SELECT SUM(total_amount) FROM flow_grouped), 4)
        END,
        'lead_contract', fc.payload
      )
      ORDER BY fg.total_amount DESC, fg.signer
    )
    FROM flow_grouped fg
    LEFT JOIN featured_contracts fc
      ON fc.id = fg.lead_procurement_id
  ), '[]'::jsonb),
  'featured_contracts', COALESCE((
    SELECT jsonb_agg(payload ORDER BY sort_amount DESC, contract_signed_date DESC NULLS LAST, id DESC)
    FROM featured_contracts_limited
  ), '[]'::jsonb),
  'recent_active_contracts', COALESCE((
    SELECT jsonb_agg(payload ORDER BY contract_signed_date DESC NULLS LAST, id DESC)
    FROM recent_active_contracts
  ), '[]'::jsonb)
);
$$;

GRANT EXECUTE ON FUNCTION public.get_environment_ministry_dashboard(integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- I) Latest funding-year municipality + syndesmos spend RPC
-- Source: sql/033_latest_funding_year_spend_rpc.sql
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_latest_funding_year_municipality_spend();

CREATE OR REPLACE FUNCTION public.get_latest_funding_year_municipality_spend()
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH latest_funding_year AS (
  SELECT MAX(f.year)::int AS year_main
  FROM public.fund f
  WHERE f.year IS NOT NULL
),
payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(COALESCE(py.amount_without_vat, 0)) AS amount_without_vat
  FROM public.payment py
  JOIN latest_funding_year ly
    ON ly.year_main IS NOT NULL
   AND py.fiscal_year = ly.year_main
  WHERE py.procurement_id IS NOT NULL
    AND py.amount_without_vat IS NOT NULL
  GROUP BY py.procurement_id
),
proc_ranked AS (
  SELECT
    p.id AS procurement_id,
    pa.amount_without_vat,
    COALESCE(p.canonical_owner_scope, '') AS canonical_owner_scope,
    COALESCE(org.authority_scope, 'other') AS authority_scope,
    COALESCE(org.organization_normalized_value, org.organization_value, '') AS organization_name,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(p.reference_number), ''),
        NULLIF(BTRIM(p.diavgeia_ada), ''),
        NULLIF(BTRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT
      o.organization_normalized_value,
      o.organization_value,
      o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = p.reference_number
    )
),
classified AS (
  SELECT
    pr.procurement_id,
    pr.amount_without_vat,
    (
      pr.canonical_owner_scope = 'municipality'
      OR pr.authority_scope = 'municipality'
    ) AS is_municipality,
    (
      NULLIF(BTRIM(pr.organization_name), '') IS NOT NULL
      AND (
        pr.organization_name ILIKE '%συνδεσμ%'
        OR pr.organization_name ILIKE '%σύνδεσμ%'
      )
    ) AS is_syndesmos
  FROM proc_ranked pr
  WHERE pr.rn = 1
)
SELECT jsonb_build_object(
  'latest_funding_year', (SELECT ly.year_main FROM latest_funding_year ly),
  'municipality_amount', COALESCE((
    SELECT ROUND(SUM(c.amount_without_vat), 2)
    FROM classified c
    WHERE c.is_municipality
  ), 0),
  'syndesmos_amount', COALESCE((
    SELECT ROUND(SUM(c.amount_without_vat), 2)
    FROM classified c
    WHERE c.is_syndesmos
  ), 0),
  'total_amount', COALESCE((
    SELECT ROUND(SUM(c.amount_without_vat), 2)
    FROM classified c
    WHERE c.is_municipality OR c.is_syndesmos
  ), 0),
  'municipality_procurement_count', COALESCE((
    SELECT COUNT(*)
    FROM classified c
    WHERE c.is_municipality
  ), 0),
  'syndesmos_procurement_count', COALESCE((
    SELECT COUNT(*)
    FROM classified c
    WHERE c.is_syndesmos
  ), 0),
  'total_procurement_count', COALESCE((
    SELECT COUNT(*)
    FROM classified c
    WHERE c.is_municipality OR c.is_syndesmos
  ), 0)
);
$$;

GRANT EXECUTE ON FUNCTION public.get_latest_funding_year_municipality_spend() TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- J) Homepage latest contracts RPC
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_latest_contract_cards(
  p_limit integer DEFAULT 15
)
RETURNS TABLE (
  procurement_id bigint,
  who text,
  title text,
  submission_at timestamptz,
  contract_signed_date date,
  short_description text,
  procedure_type_value text,
  beneficiary_name text,
  beneficiary_vat_number text,
  amount_without_vat numeric,
  amount_with_vat numeric,
  reference_number text,
  contract_number text,
  cpv_items jsonb,
  organization_vat_number text,
  signers text,
  assign_criteria text,
  contract_type text,
  award_procedure text,
  units_operator text,
  funding_details_cofund text,
  funding_details_self_fund text,
  funding_details_espa text,
  funding_details_regular_budget text,
  auction_ref_no text,
  payment_ref_no text,
  budget numeric,
  contract_budget numeric,
  contract_related_ada text,
  prev_reference_no text,
  next_ref_no text,
  diavgeia_ada text,
  start_date date,
  end_date date
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $frontend_latest_contracts$
WITH payment_first AS (
  SELECT DISTINCT ON (py.procurement_id)
    py.procurement_id,
    NULLIF(BTRIM(py.beneficiary_name), '') AS beneficiary_name,
    NULLIF(BTRIM(py.beneficiary_vat_number), '') AS beneficiary_vat_number,
    NULLIF(BTRIM(py.signers), '') AS signers,
    NULLIF(BTRIM(py.payment_ref_no), '') AS payment_ref_no,
    py.amount_without_vat,
    py.amount_with_vat
  FROM public.payment py
  ORDER BY py.procurement_id, py.id
),
cpv_dedup AS (
  SELECT DISTINCT
    c.procurement_id,
    COALESCE(NULLIF(BTRIM(c.cpv_key), ''), '—') AS cpv_key,
    COALESCE(NULLIF(BTRIM(c.cpv_value), ''), '—') AS cpv_value
  FROM public.cpv c
),
cpv_agg AS (
  SELECT
    cd.procurement_id,
    jsonb_agg(
      jsonb_build_object(
        'code', cd.cpv_key,
        'label', cd.cpv_value
      )
      ORDER BY cd.cpv_value, cd.cpv_key
    ) AS cpv_items
  FROM cpv_dedup cd
  GROUP BY cd.procurement_id
),
base AS (
  SELECT
    p.id AS procurement_id,
    p.title,
    p.submission_at,
    p.contract_signed_date,
    NULLIF(BTRIM(split_part(COALESCE(p.short_descriptions, ''), '|', 1)), '') AS short_description,
    p.procedure_type_value,
    pf.beneficiary_name,
    pf.beneficiary_vat_number,
    COALESCE(pf.amount_without_vat, p.contract_budget, p.budget) AS amount_without_vat,
    pf.amount_with_vat,
    p.reference_number,
    p.contract_number,
    COALESCE(ca.cpv_items, '[]'::jsonb) AS cpv_items,
    p.organization_vat_number,
    pf.signers,
    p.assign_criteria,
    p.contract_type,
    p.award_procedure,
    p.units_operator,
    p.funding_details_cofund,
    p.funding_details_self_fund,
    p.funding_details_espa,
    p.funding_details_regular_budget,
    p.auction_ref_no,
    pf.payment_ref_no,
    p.budget,
    p.contract_budget,
    p.contract_related_ada,
    p.prev_reference_no,
    p.next_ref_no,
    p.diavgeia_ada,
    p.start_date,
    p.end_date,
    p.canonical_owner_scope,
    COALESCE(org.organization_normalized_value, org.organization_value, p.organization_key) AS organization_name,
    COALESCE(org.authority_scope, 'other') AS organization_scope,
    NULLIF(BTRIM(mun.municipality_normalized_value), '') AS municipality_label,
    COALESCE(
      NULLIF(BTRIM(reg.region_normalized_value), ''),
      NULLIF(BTRIM(reg.region_value), '')
    ) AS region_label
  FROM public.procurement p
  LEFT JOIN payment_first pf
    ON pf.procurement_id = p.id
  LEFT JOIN cpv_agg ca
    ON ca.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT
      o.organization_normalized_value,
      o.organization_value,
      o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  LEFT JOIN LATERAL (
    SELECT
      m.municipality_normalized_value
    FROM public.municipality_normalized_name m
    WHERE m.municipality_key = p.municipality_key
    ORDER BY m.id
    LIMIT 1
  ) mun ON TRUE
  LEFT JOIN LATERAL (
    SELECT
      r.region_normalized_value,
      r.region_value
    FROM public.region r
    WHERE r.region_key = p.region_key
    ORDER BY r.id
    LIMIT 1
  ) reg ON TRUE
  WHERE p.submission_at IS NOT NULL
),
enriched AS (
  SELECT
    b.*,
    CASE
      WHEN lower(COALESCE(b.canonical_owner_scope, '')) = 'municipality' THEN COALESCE(
        CASE
          WHEN b.municipality_label IS NULL THEN NULL
          WHEN b.municipality_label LIKE 'ΔΗΜΟΣ %' THEN b.municipality_label
          ELSE CONCAT('ΔΗΜΟΣ ', b.municipality_label)
        END,
        b.organization_name,
        'Δήμος —'
      )
      WHEN lower(COALESCE(b.canonical_owner_scope, '')) = 'region' THEN COALESCE(
        CASE
          WHEN b.region_label IS NULL THEN NULL
          WHEN b.region_label LIKE 'ΠΕΡΙΦΕΡΕΙΑ %' THEN b.region_label
          ELSE CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', b.region_label)
        END,
        CASE
          WHEN b.municipality_label IS NULL THEN NULL
          WHEN b.municipality_label LIKE 'ΔΗΜΟΣ %' THEN b.municipality_label
          ELSE CONCAT('ΔΗΜΟΣ ', b.municipality_label)
        END,
        b.organization_name,
        'Περιφέρεια —'
      )
      WHEN lower(COALESCE(b.organization_scope, '')) = 'municipality' THEN COALESCE(
        CASE
          WHEN b.municipality_label IS NULL THEN NULL
          WHEN b.municipality_label LIKE 'ΔΗΜΟΣ %' THEN b.municipality_label
          ELSE CONCAT('ΔΗΜΟΣ ', b.municipality_label)
        END,
        b.organization_name,
        'Δήμος —'
      )
      WHEN lower(COALESCE(b.organization_scope, '')) IN ('region', 'decentralized') THEN COALESCE(
        CASE
          WHEN b.region_label IS NULL THEN NULL
          WHEN b.region_label LIKE 'ΠΕΡΙΦΕΡΕΙΑ %' THEN b.region_label
          ELSE CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', b.region_label)
        END,
        CASE
          WHEN b.municipality_label IS NULL THEN NULL
          WHEN b.municipality_label LIKE 'ΔΗΜΟΣ %' THEN b.municipality_label
          ELSE CONCAT('ΔΗΜΟΣ ', b.municipality_label)
        END,
        b.organization_name,
        'Περιφέρεια —'
      )
      ELSE COALESCE(b.organization_name, b.municipality_label, b.region_label, '—')
    END AS who
  FROM base b
),
ranked AS (
  SELECT
    e.*,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(e.reference_number), ''),
        NULLIF(BTRIM(e.contract_number), ''),
        NULLIF(BTRIM(COALESCE(e.contract_related_ada, e.diavgeia_ada)), ''),
        CONCAT_WS(
          '|',
          COALESCE(e.who, ''),
          COALESCE(e.title, ''),
          COALESCE(e.contract_signed_date::text, ''),
          COALESCE(e.amount_without_vat::text, '')
        )
      )
      ORDER BY e.submission_at DESC NULLS LAST, e.procurement_id DESC
    ) AS rn
  FROM enriched e
)
SELECT
  r.procurement_id,
  r.who,
  r.title,
  r.submission_at,
  r.contract_signed_date,
  r.short_description,
  r.procedure_type_value,
  r.beneficiary_name,
  r.beneficiary_vat_number,
  r.amount_without_vat,
  r.amount_with_vat,
  r.reference_number,
  r.contract_number,
  r.cpv_items,
  r.organization_vat_number,
  r.signers,
  r.assign_criteria,
  r.contract_type,
  r.award_procedure,
  r.units_operator,
  r.funding_details_cofund,
  r.funding_details_self_fund,
  r.funding_details_espa,
  r.funding_details_regular_budget,
  r.auction_ref_no,
  r.payment_ref_no,
  r.budget,
  r.contract_budget,
  r.contract_related_ada,
  r.prev_reference_no,
  r.next_ref_no,
  r.diavgeia_ada,
  r.start_date,
  r.end_date
FROM ranked r
WHERE r.rn = 1
ORDER BY r.submission_at DESC NULLS LAST, r.procurement_id DESC
LIMIT GREATEST(COALESCE(p_limit, 15), 1);
$frontend_latest_contracts$;

GRANT EXECUTE ON FUNCTION public.get_latest_contract_cards(integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- K) Municipality featured beneficiaries RPC
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_municipality_featured_beneficiaries(
  p_municipality_key text,
  p_year integer,
  p_limit integer DEFAULT 12
)
RETURNS TABLE (
  beneficiary_name text,
  beneficiary_vat_number text,
  organization text,
  total_amount numeric,
  contract_count integer,
  cpv text,
  start_date date,
  end_date date,
  duration_days integer,
  progress_pct numeric,
  signer text,
  relevant_contracts jsonb
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $frontend_municipality_featured$
WITH municipality_lookup AS (
  SELECT COALESCE(
    NULLIF(BTRIM(m.municipality_normalized_value), ''),
    NULLIF(BTRIM(m.municipality_value), ''),
    p_municipality_key
  ) AS municipality_label
  FROM public.municipality m
  WHERE m.municipality_key = p_municipality_key
  ORDER BY m.id
  LIMIT 1
),
payment_agg AS (
  SELECT
    py.procurement_id,
    COALESCE(
      NULLIF(BTRIM(py.beneficiary_vat_number), ''),
      CONCAT('name:', COALESCE(NULLIF(BTRIM(py.beneficiary_name), ''), '—'))
    ) AS beneficiary_key,
    NULLIF(BTRIM(py.beneficiary_vat_number), '') AS beneficiary_vat_number,
    COALESCE(
      NULLIF(BTRIM(py.beneficiary_name), ''),
      NULLIF(BTRIM(py.beneficiary_vat_number), ''),
      '—'
    ) AS beneficiary_name,
    SUM(COALESCE(py.amount_without_vat, 0)) AS amount_without_vat,
    SUM(COALESCE(py.amount_with_vat, 0)) AS amount_with_vat,
    (array_agg(NULLIF(BTRIM(py.signers), '') ORDER BY py.id) FILTER (WHERE NULLIF(BTRIM(py.signers), '') IS NOT NULL))[1] AS signers,
    (array_agg(NULLIF(BTRIM(py.payment_ref_no), '') ORDER BY py.id) FILTER (WHERE NULLIF(BTRIM(py.payment_ref_no), '') IS NOT NULL))[1] AS payment_ref_no
  FROM public.payment py
  GROUP BY
    py.procurement_id,
    COALESCE(
      NULLIF(BTRIM(py.beneficiary_vat_number), ''),
      CONCAT('name:', COALESCE(NULLIF(BTRIM(py.beneficiary_name), ''), '—'))
    ),
    NULLIF(BTRIM(py.beneficiary_vat_number), ''),
    COALESCE(
      NULLIF(BTRIM(py.beneficiary_name), ''),
      NULLIF(BTRIM(py.beneficiary_vat_number), ''),
      '—'
    )
),
cpv_dedup AS (
  SELECT DISTINCT
    c.procurement_id,
    COALESCE(NULLIF(BTRIM(c.cpv_key), ''), '—') AS cpv_key,
    COALESCE(NULLIF(BTRIM(c.cpv_value), ''), '—') AS cpv_value
  FROM public.cpv c
),
cpv_agg AS (
  SELECT
    cd.procurement_id,
    jsonb_agg(
      jsonb_build_object(
        'code', cd.cpv_key,
        'label', cd.cpv_value
      )
      ORDER BY cd.cpv_value, cd.cpv_key
    ) AS cpv_items
  FROM cpv_dedup cd
  GROUP BY cd.procurement_id
),
base AS (
  SELECT
    p.id AS procurement_id,
    p.title,
    p.submission_at,
    p.contract_signed_date,
    NULLIF(BTRIM(split_part(COALESCE(p.short_descriptions, ''), '|', 1)), '') AS short_description,
    p.procedure_type_value,
    p.reference_number,
    p.contract_number,
    p.contract_budget,
    p.budget,
    p.assign_criteria,
    p.contract_type,
    p.award_procedure,
    p.units_operator,
    p.funding_details_cofund,
    p.funding_details_self_fund,
    p.funding_details_espa,
    p.funding_details_regular_budget,
    p.auction_ref_no,
    p.prev_reference_no,
    p.next_ref_no,
    p.contract_related_ada,
    p.organization_vat_number,
    p.start_date,
    p.end_date,
    p.no_end_date,
    p.diavgeia_ada,
    COALESCE(org.organization_normalized_value, org.organization_value, ml.municipality_label, '—') AS organization_value,
    CASE
      WHEN COALESCE(p.canonical_owner_scope, '') = 'municipality' THEN 'municipality'
      ELSE COALESCE(org.authority_scope, 'other')
    END AS authority_scope,
    pa.beneficiary_key,
    pa.beneficiary_vat_number,
    pa.beneficiary_name,
    pa.amount_without_vat,
    pa.amount_with_vat,
    pa.signers,
    pa.payment_ref_no,
    COALESCE(ca.cpv_items, '[]'::jsonb) AS cpv_items
  FROM public.procurement p
  JOIN payment_agg pa
    ON pa.procurement_id = p.id
  CROSS JOIN municipality_lookup ml
  LEFT JOIN LATERAL (
    SELECT
      o.organization_normalized_value,
      o.organization_value,
      o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  LEFT JOIN cpv_agg ca
    ON ca.procurement_id = p.id
  WHERE p.municipality_key = p_municipality_key
    AND COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND p.contract_signed_date IS NOT NULL
    AND (
      p.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
      OR (
        p.contract_signed_date < make_date(p_year, 1, 1)
        AND COALESCE(p.no_end_date, FALSE) = FALSE
        AND p.end_date IS NOT NULL
        AND p.end_date >= make_date(p_year, 1, 1)
      )
    )
),
filtered AS (
  SELECT *
  FROM base
  WHERE authority_scope = 'municipality'
),
beneficiary_totals AS (
  SELECT
    f.beneficiary_key,
    SUM(f.amount_without_vat) AS total_amount,
    COUNT(DISTINCT f.procurement_id)::integer AS contract_count,
    MIN(f.start_date) AS start_date,
    MAX(f.end_date) AS end_date
  FROM filtered f
  GROUP BY f.beneficiary_key
),
top_beneficiaries AS (
  SELECT
    bt.beneficiary_key,
    bt.total_amount,
    bt.contract_count,
    bt.start_date,
    bt.end_date
  FROM beneficiary_totals bt
  ORDER BY bt.total_amount DESC, bt.contract_count DESC, bt.beneficiary_key
  LIMIT GREATEST(COALESCE(p_limit, 12), 1)
),
name_ranked AS (
  SELECT
    f.beneficiary_key,
    f.beneficiary_name,
    f.beneficiary_vat_number,
    ROW_NUMBER() OVER (
      PARTITION BY f.beneficiary_key
      ORDER BY f.contract_signed_date DESC NULLS LAST, f.procurement_id DESC
    ) AS rn
  FROM filtered f
),
org_totals AS (
  SELECT
    f.beneficiary_key,
    f.organization_value,
    SUM(f.amount_without_vat) AS total_amount
  FROM filtered f
  GROUP BY f.beneficiary_key, f.organization_value
),
org_ranked AS (
  SELECT
    ot.beneficiary_key,
    ot.organization_value,
    ROW_NUMBER() OVER (
      PARTITION BY ot.beneficiary_key
      ORDER BY ot.total_amount DESC, ot.organization_value
    ) AS rn
  FROM org_totals ot
),
signer_counts AS (
  SELECT
    f.beneficiary_key,
    f.signers,
    COUNT(*) AS signer_count
  FROM filtered f
  WHERE NULLIF(BTRIM(f.signers), '') IS NOT NULL
  GROUP BY f.beneficiary_key, f.signers
),
signer_ranked AS (
  SELECT
    sc.beneficiary_key,
    sc.signers,
    ROW_NUMBER() OVER (
      PARTITION BY sc.beneficiary_key
      ORDER BY sc.signer_count DESC, sc.signers
    ) AS rn
  FROM signer_counts sc
),
cpv_counts AS (
  SELECT
    f.beneficiary_key,
    cpv_item ->> 'label' AS cpv_label,
    COUNT(*) AS cpv_count
  FROM filtered f
  CROSS JOIN LATERAL jsonb_array_elements(f.cpv_items) cpv_item
  WHERE NULLIF(BTRIM(cpv_item ->> 'label'), '') IS NOT NULL
    AND cpv_item ->> 'label' <> '—'
  GROUP BY f.beneficiary_key, cpv_item ->> 'label'
),
cpv_ranked AS (
  SELECT
    cc.beneficiary_key,
    cc.cpv_label,
    ROW_NUMBER() OVER (
      PARTITION BY cc.beneficiary_key
      ORDER BY cc.cpv_count DESC, cc.cpv_label
    ) AS rn
  FROM cpv_counts cc
),
relevant_ranked AS (
  SELECT
    f.beneficiary_key,
    jsonb_build_object(
      'id', f.procurement_id,
      'organization', f.organization_value,
      'title', f.title,
      'submission_at', f.submission_at,
      'short_description', f.short_description,
      'procedure_type_value', f.procedure_type_value,
      'amount_without_vat', f.amount_without_vat,
      'amount_with_vat', f.amount_with_vat,
      'reference_number', f.reference_number,
      'contract_number', f.contract_number,
      'cpv_items', f.cpv_items,
      'contract_signed_date', f.contract_signed_date,
      'start_date', f.start_date,
      'end_date', f.end_date,
      'organization_vat_number', f.organization_vat_number,
      'beneficiary_vat_number', f.beneficiary_vat_number,
      'beneficiary_name', f.beneficiary_name,
      'signers', f.signers,
      'assign_criteria', f.assign_criteria,
      'contract_type', f.contract_type,
      'award_procedure', f.award_procedure,
      'units_operator', f.units_operator,
      'funding_details_cofund', f.funding_details_cofund,
      'funding_details_self_fund', f.funding_details_self_fund,
      'funding_details_espa', f.funding_details_espa,
      'funding_details_regular_budget', f.funding_details_regular_budget,
      'auction_ref_no', f.auction_ref_no,
      'payment_ref_no', f.payment_ref_no,
      'budget', f.budget,
      'contract_budget', f.contract_budget,
      'contract_related_ada', f.contract_related_ada,
      'prev_reference_no', f.prev_reference_no,
      'next_ref_no', f.next_ref_no,
      'diavgeia_ada', f.diavgeia_ada
    ) AS contract_json,
    ROW_NUMBER() OVER (
      PARTITION BY f.beneficiary_key
      ORDER BY f.amount_without_vat DESC, f.contract_signed_date DESC NULLS LAST, f.procurement_id DESC
    ) AS rn
  FROM filtered f
  JOIN top_beneficiaries tb
    ON tb.beneficiary_key = f.beneficiary_key
),
relevant_agg AS (
  SELECT
    rr.beneficiary_key,
    jsonb_agg(rr.contract_json ORDER BY rr.rn) AS relevant_contracts
  FROM relevant_ranked rr
  WHERE rr.rn <= 5
  GROUP BY rr.beneficiary_key
)
SELECT
  COALESCE(nr.beneficiary_name, '—') AS beneficiary_name,
  nr.beneficiary_vat_number,
  COALESCE(orx.organization_value, '—') AS organization,
  tb.total_amount,
  tb.contract_count,
  COALESCE(cr.cpv_label, '—') AS cpv,
  tb.start_date,
  tb.end_date,
  CASE
    WHEN tb.start_date IS NULL OR tb.end_date IS NULL OR tb.end_date < tb.start_date THEN NULL
    ELSE (tb.end_date - tb.start_date + 1)
  END::integer AS duration_days,
  CASE
    WHEN tb.start_date IS NULL OR tb.end_date IS NULL OR tb.end_date <= tb.start_date THEN NULL
    WHEN CURRENT_DATE <= tb.start_date THEN 0
    WHEN CURRENT_DATE >= tb.end_date THEN 100
    ELSE ROUND((((CURRENT_DATE - tb.start_date)::numeric / NULLIF((tb.end_date - tb.start_date)::numeric, 0)) * 100)::numeric, 2)
  END AS progress_pct,
  COALESCE(sr.signers, '—') AS signer,
  COALESCE(ra.relevant_contracts, '[]'::jsonb) AS relevant_contracts
FROM top_beneficiaries tb
LEFT JOIN name_ranked nr
  ON nr.beneficiary_key = tb.beneficiary_key
 AND nr.rn = 1
LEFT JOIN org_ranked orx
  ON orx.beneficiary_key = tb.beneficiary_key
 AND orx.rn = 1
LEFT JOIN signer_ranked sr
  ON sr.beneficiary_key = tb.beneficiary_key
 AND sr.rn = 1
LEFT JOIN cpv_ranked cr
  ON cr.beneficiary_key = tb.beneficiary_key
 AND cr.rn = 1
LEFT JOIN relevant_agg ra
  ON ra.beneficiary_key = tb.beneficiary_key
ORDER BY tb.total_amount DESC, tb.contract_count DESC, COALESCE(nr.beneficiary_name, tb.beneficiary_key);
$frontend_municipality_featured$;

GRANT EXECUTE ON FUNCTION public.get_municipality_featured_beneficiaries(text, integer, integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- L) Table/function grants for frontend reads
-- -------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon, authenticated, service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- M) Enable RLS and keep public frontend reads working
-- -------------------------------------------------------------
DO $frontend_enable_rls$
DECLARE
  table_name text;
BEGIN
  FOR table_name IN
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
  LOOP
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', table_name);
  END LOOP;
END
$frontend_enable_rls$;

DROP POLICY IF EXISTS public_read_procurement ON public.procurement;
CREATE POLICY public_read_procurement
ON public.procurement
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_payment ON public.payment;
CREATE POLICY public_read_payment
ON public.payment
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_cpv ON public.cpv;
CREATE POLICY public_read_cpv
ON public.cpv
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_organization ON public.organization;
CREATE POLICY public_read_organization
ON public.organization
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_region ON public.region;
CREATE POLICY public_read_region
ON public.region
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_municipality ON public.municipality;
CREATE POLICY public_read_municipality
ON public.municipality
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_municipality_normalized_name ON public.municipality_normalized_name;
CREATE POLICY public_read_municipality_normalized_name
ON public.municipality_normalized_name
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_municipality_fire_protection_data ON public.municipality_fire_protection_data;
CREATE POLICY public_read_municipality_fire_protection_data
ON public.municipality_fire_protection_data
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_forest_fire ON public.forest_fire;
CREATE POLICY public_read_forest_fire
ON public.forest_fire
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_fund ON public.fund;
CREATE POLICY public_read_fund
ON public.fund
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_copernicus ON public.copernicus;
CREATE POLICY public_read_copernicus
ON public.copernicus
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_works ON public.works;
CREATE POLICY public_read_works
ON public.works
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_current_fires ON public.current_fires;

-- -------------------------------------------------------------
-- N) Force PostgREST to reload schema cache
-- -------------------------------------------------------------
NOTIFY pgrst, 'reload schema';

-- -------------------------------------------------------------
-- O) Quick verification (run after setup)
-- -------------------------------------------------------------
-- 1) Check RPC functions exist
-- select p.proname, pg_get_function_arguments(p.oid) as args
-- from pg_proc p
-- join pg_namespace n on n.oid = p.pronamespace
-- where n.nspname = 'public'
--   and p.proname in ('get_hero_section_data', 'get_homepage_funding', 'get_contracts_page', 'get_featured_beneficiaries', 'get_municipality_featured_beneficiaries', 'get_latest_contract_cards', 'get_municipality_map_spend_per_100k', 'get_latest_funding_year_municipality_spend', 'normalize_procedure_type');

-- 2) Quick smoke test RPC calls
-- select public.get_hero_section_data(2026, 2024);
-- select public.get_homepage_funding(2026, 2016);
-- select * from public.get_contracts_page(NULL,NULL,NULL,NULL,NULL,NULL,1,5);
-- select * from public.get_featured_beneficiaries(2026, 5);
-- select * from public.get_municipality_featured_beneficiaries('9061', 2026, 5);
-- select * from public.get_latest_contract_cards(5);
-- select * from public.get_municipality_map_spend_per_100k(2026) limit 20;
-- select public.get_latest_funding_year_municipality_spend();
