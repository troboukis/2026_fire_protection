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
CREATE OR REPLACE FUNCTION public.normalize_procedure_type(p_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_value IS NULL OR btrim(p_value) = '' THEN '—'
    WHEN lower(btrim(p_value)) LIKE 'απευθείας ανάθεση%' THEN 'Απευθείας ανάθεση'
    ELSE btrim(p_value)
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
  )
  SELECT MAX(contract_signed_date)
  INTO v_as_of_date
  FROM proc_ranked
  WHERE rn = 1
    AND EXTRACT(YEAR FROM contract_signed_date) = p_year_main;

  IF v_as_of_date IS NULL THEN
    RETURN jsonb_build_object(
      'period_main_start', to_char(make_date(p_year_main, 1, 1), 'YYYY-MM-DD'),
      'period_main_end', to_char(make_date(p_year_main, 1, 1), 'YYYY-MM-DD'),
      'total_main', 0,
      'total_prev1', 0,
      'total_prev2', 0,
      'top_contract_type', null,
      'top_contract_type_count', 0,
      'top_contract_type_prev1_count', 0,
      'top_cpv_text', null,
      'top_cpv_count', 0,
      'top_cpv_prev1_count', 0,
      'curve_points', '[]'::jsonb
    );
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
  ),
  rows_base AS (
    SELECT
      pr.contract_signed_date AS d,
      pb.amount_without_vat
    FROM proc_ranked pr
    JOIN pay_by_proc pb ON pb.procurement_id = pr.id
    WHERE pr.rn = 1
      AND EXTRACT(YEAR FROM pr.contract_signed_date) BETWEEN p_year_start AND p_year_main
  ),
  years AS (
    SELECT generate_series(p_year_start, p_year_main) AS y
  ),
  days AS (
    SELECT generate_series(1, 366) AS day_of_year
  ),
  year_days AS (
    SELECT y, CASE WHEN (make_date(y, 12, 31) - make_date(y, 1, 1) + 1) = 366 THEN 366 ELSE 365 END AS year_days
    FROM years
  ),
  calendar AS (
    SELECT
      yd.y,
      yd.year_days,
      d.day_of_year,
      make_date(yd.y, 1, 1) + (d.day_of_year - 1) * interval '1 day' AS point_date
    FROM year_days yd
    JOIN days d ON d.day_of_year <= yd.year_days
  ),
  daily AS (
    SELECT d::date AS day_date, SUM(amount_without_vat) AS amount
    FROM rows_base
    GROUP BY d::date
  ),
  curve AS (
    SELECT
      c.y AS series_year,
      c.year_days,
      c.day_of_year,
      c.point_date::date AS point_date,
      COALESCE(SUM(dy.amount) OVER (
        PARTITION BY c.y
        ORDER BY c.day_of_year
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ), 0) AS cumulative_amount
    FROM calendar c
    LEFT JOIN daily dy ON dy.day_date = c.point_date::date
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'series_year', series_year,
    'year_days', year_days,
    'day_of_year', day_of_year,
    'point_date', to_char(point_date, 'YYYY-MM-DD'),
    'cumulative_amount', cumulative_amount
  ) ORDER BY series_year, day_of_year), '[]'::jsonb)
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
    STRING_AGG(DISTINCT NULLIF(TRIM(py.beneficiary_name), ''), ' | ') AS beneficiary_name
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
WITH cpv_dedup AS (
  SELECT DISTINCT
    c.procurement_id,
    NULLIF(TRIM(c.cpv_key), '') AS cpv_key,
    NULLIF(TRIM(c.cpv_value), '') AS cpv_value
  FROM public.cpv c
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
proc_ranked AS (
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
    p.units_operator,
    p.funding_details_cofund,
    p.funding_details_self_fund,
    p.funding_details_espa,
    p.funding_details_regular_budget,
    p.auction_ref_no,
    p.organization_vat_number,
    p.start_date,
    p.end_date,
    p.diavgeia_ada,
    py.amount_without_vat,
    py.amount_with_vat,
    py.beneficiary_vat_number,
    COALESCE(
      NULLIF(TRIM(py.beneficiary_name), ''),
      NULLIF(TRIM(b.beneficiary_name), '')
    ) AS beneficiary_name,
    py.signers,
    py.payment_ref_no,
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
  JOIN public.payment py
    ON py.procurement_id = p.id
  LEFT JOIN public.beneficiary b
    ON b.beneficiary_vat_number = py.beneficiary_vat_number
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND p.contract_signed_date BETWEEN make_date(p_year_main, 1, 1) AND make_date(p_year_main, 12, 31)
    AND NULLIF(TRIM(py.beneficiary_vat_number), '') IS NOT NULL
),
base AS (
  SELECT
    pr.procurement_id,
    pr.organization_key,
    COALESCE(org.organization_normalized_value, org.organization_value, pr.organization_key, '—') AS organization_value,
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
    pr.units_operator,
    pr.funding_details_cofund,
    pr.funding_details_self_fund,
    pr.funding_details_espa,
    pr.funding_details_regular_budget,
    pr.auction_ref_no,
    pr.organization_vat_number,
    pr.start_date,
    pr.end_date,
    pr.diavgeia_ada,
    COALESCE(pr.amount_without_vat, 0) AS amount_without_vat,
    pr.amount_with_vat,
    pr.beneficiary_vat_number,
    COALESCE(pr.beneficiary_name, pr.beneficiary_vat_number) AS beneficiary_name,
    COALESCE(NULLIF(TRIM(pr.signers), ''), '—') AS signers,
    pr.payment_ref_no,
    COALESCE(ca.cpv_items, '[]'::jsonb) AS cpv_items
  FROM proc_ranked pr
  LEFT JOIN LATERAL (
    SELECT o.organization_normalized_value, o.organization_value
    FROM public.organization o
    WHERE o.organization_key = pr.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  LEFT JOIN cpv_agg ca
    ON ca.procurement_id = pr.procurement_id
  WHERE pr.rn = 1
),
beneficiary_name_latest AS (
  SELECT
    b.beneficiary_vat_number,
    b.beneficiary_name,
    ROW_NUMBER() OVER (
      PARTITION BY b.beneficiary_vat_number
      ORDER BY b.contract_signed_date DESC NULLS LAST, b.procurement_id DESC, b.beneficiary_name
    ) AS rn
  FROM base b
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
  FROM base b
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
  FROM base b
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
  FROM base b
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
beneficiary_totals AS (
  SELECT
    b.beneficiary_vat_number,
    SUM(b.amount_without_vat) AS total_amount,
    COUNT(*)::integer AS contract_count,
    MIN(b.start_date) AS start_date,
    MAX(b.end_date) AS end_date
  FROM base b
  GROUP BY b.beneficiary_vat_number
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
      'units_operator', b.units_operator,
      'funding_details_cofund', b.funding_details_cofund,
      'funding_details_self_fund', b.funding_details_self_fund,
      'funding_details_espa', b.funding_details_espa,
      'funding_details_regular_budget', b.funding_details_regular_budget,
      'auction_ref_no', b.auction_ref_no,
      'payment_ref_no', b.payment_ref_no,
      'budget', b.budget,
      'contract_budget', b.contract_budget,
      'diavgeia_ada', b.diavgeia_ada
    ) AS contract_json,
    ROW_NUMBER() OVER (
      PARTITION BY b.beneficiary_vat_number
      ORDER BY b.amount_without_vat DESC, b.contract_signed_date DESC NULLS LAST, b.procurement_id DESC
    ) AS rn
  FROM base b
),
relevant_agg AS (
  SELECT
    rr.beneficiary_vat_number,
    jsonb_agg(rr.contract_json ORDER BY rr.rn) AS relevant_contracts
  FROM relevant_ranked rr
  WHERE rr.rn <= 5
  GROUP BY rr.beneficiary_vat_number
)
SELECT
  bt.beneficiary_vat_number,
  COALESCE(bnr.beneficiary_name, bt.beneficiary_vat_number) AS beneficiary_name,
  COALESCE(bor.organization_value, '—') AS organization,
  bt.total_amount,
  bt.contract_count,
  COALESCE(bcr.cpv_label, '—') AS cpv,
  bt.start_date,
  bt.end_date,
  CASE
    WHEN bt.start_date IS NULL OR bt.end_date IS NULL OR bt.end_date < bt.start_date THEN NULL
    ELSE (bt.end_date - bt.start_date + 1)
  END::integer AS duration_days,
  CASE
    WHEN bt.start_date IS NULL OR bt.end_date IS NULL OR bt.end_date <= bt.start_date THEN NULL
    WHEN CURRENT_DATE <= bt.start_date THEN 0
    WHEN CURRENT_DATE >= bt.end_date THEN 100
    ELSE ROUND((((CURRENT_DATE - bt.start_date)::numeric / NULLIF((bt.end_date - bt.start_date)::numeric, 0)) * 100)::numeric, 2)
  END AS progress_pct,
  COALESCE(bsr.signers, '—') AS signer,
  COALESCE(ra.relevant_contracts, '[]'::jsonb) AS relevant_contracts
FROM beneficiary_totals bt
LEFT JOIN beneficiary_name_ranked bnr
  ON bnr.beneficiary_vat_number = bt.beneficiary_vat_number
 AND bnr.rn = 1
LEFT JOIN beneficiary_org_ranked bor
  ON bor.beneficiary_vat_number = bt.beneficiary_vat_number
 AND bor.rn = 1
LEFT JOIN beneficiary_signer_ranked bsr
  ON bsr.beneficiary_vat_number = bt.beneficiary_vat_number
 AND bsr.rn = 1
LEFT JOIN beneficiary_cpv_ranked bcr
  ON bcr.beneficiary_vat_number = bt.beneficiary_vat_number
 AND bcr.rn = 1
LEFT JOIN relevant_agg ra
  ON ra.beneficiary_vat_number = bt.beneficiary_vat_number
ORDER BY bt.total_amount DESC, bt.contract_count DESC, bt.beneficiary_vat_number
LIMIT GREATEST(COALESCE(p_limit, 50), 1);
$$;

GRANT EXECUTE ON FUNCTION public.get_featured_beneficiaries(integer, integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- E) Table/function grants for frontend reads
-- -------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon, authenticated, service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- F) Force PostgREST to reload schema cache
-- -------------------------------------------------------------
NOTIFY pgrst, 'reload schema';

-- -------------------------------------------------------------
-- G) Quick verification (run after setup)
-- -------------------------------------------------------------
-- 1) Check RPC functions exist
-- select p.proname, pg_get_function_arguments(p.oid) as args
-- from pg_proc p
-- join pg_namespace n on n.oid = p.pronamespace
-- where n.nspname = 'public'
--   and p.proname in ('get_hero_section_data', 'get_contracts_page', 'get_featured_beneficiaries', 'normalize_procedure_type');

-- 2) Quick smoke test RPC calls
-- select public.get_hero_section_data(2026, 2024);
-- select * from public.get_contracts_page(NULL,NULL,NULL,NULL,NULL,NULL,1,5);
-- select * from public.get_featured_beneficiaries(2026, 5);
