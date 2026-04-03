CREATE INDEX IF NOT EXISTS idx_procurement_contract_signed_date
ON public.procurement (contract_signed_date DESC);

CREATE INDEX IF NOT EXISTS idx_procurement_prev_reference_no
ON public.procurement (prev_reference_no);

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
  WHERE rr.rn <= 5
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

GRANT EXECUTE ON FUNCTION public.get_featured_beneficiaries(integer, integer)
TO anon, authenticated, service_role;
