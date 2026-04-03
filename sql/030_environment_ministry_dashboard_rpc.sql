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
