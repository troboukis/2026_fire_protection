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
AS $$
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
$$;

GRANT EXECUTE ON FUNCTION public.get_latest_contract_cards(integer) TO anon, authenticated, service_role;
