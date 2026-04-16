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
AS $$
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
$$;

GRANT EXECUTE ON FUNCTION public.get_municipality_featured_beneficiaries(text, integer, integer) TO anon, authenticated, service_role;
