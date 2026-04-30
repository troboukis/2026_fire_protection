CREATE OR REPLACE FUNCTION public.get_contract_analysis(
  p_year_start integer DEFAULT 2024
)
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH first_payment AS (
  SELECT DISTINCT ON (py.procurement_id)
    py.procurement_id,
    COALESCE(py.amount_without_vat, 0) AS amount_without_vat
  FROM public.payment py
  ORDER BY py.procurement_id, py.id
),
cpv_by_proc AS (
  SELECT
    c.procurement_id,
    jsonb_agg(DISTINCT COALESCE(NULLIF(BTRIM(c.cpv_value), ''), NULLIF(BTRIM(c.cpv_key), ''))) FILTER (
      WHERE COALESCE(NULLIF(BTRIM(c.cpv_value), ''), NULLIF(BTRIM(c.cpv_key), '')) IS NOT NULL
    ) AS cpvs
  FROM public.cpv c
  GROUP BY c.procurement_id
),
proc_ranked AS (
  SELECT
    p.id,
    p.contract_signed_date,
    p.start_date,
    p.end_date,
    p.no_end_date,
    p.title,
    p.reference_number,
    p.contract_number,
    p.diavgeia_ada,
    p.contract_type,
    p.procedure_type_value,
    p.organization_key,
    p.municipality_key,
    p.region_key,
    p.canonical_owner_scope,
    COALESCE(fp.amount_without_vat, 0) AS amount_without_vat,
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
  LEFT JOIN first_payment fp
    ON fp.procurement_id = p.id
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = p.reference_number
    )
),
base AS (
  SELECT
    pr.id,
    pr.contract_signed_date,
    COALESCE(pr.start_date, pr.contract_signed_date) AS effective_start,
    CASE
      WHEN COALESCE(pr.no_end_date, FALSE) THEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, 12, 31)
      ELSE COALESCE(pr.end_date, COALESCE(pr.start_date, pr.contract_signed_date))
    END AS effective_end,
    COALESCE(o.organization_normalized_value, o.organization_value, NULLIF(BTRIM(pr.organization_key), ''), '—') AS org_name,
    CASE
      WHEN pr.canonical_owner_scope = 'municipality' THEN COALESCE('ΔΗΜΟΣ ' || NULLIF(BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value)), ''), COALESCE(o.organization_normalized_value, o.organization_value), 'Δήμος —')
      WHEN pr.canonical_owner_scope = 'region' THEN COALESCE('ΠΕΡΙΦΕΡΕΙΑ ' || NULLIF(BTRIM(COALESCE(r.region_normalized_value, r.region_value)), ''), 'ΔΗΜΟΣ ' || NULLIF(BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value)), ''), COALESCE(o.organization_normalized_value, o.organization_value), 'Περιφέρεια —')
      WHEN o.authority_scope = 'municipality' AND NULLIF(BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value)), '') IS NOT NULL THEN 'ΔΗΜΟΣ ' || BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value))
      WHEN o.authority_scope IN ('region', 'decentralized') AND NULLIF(BTRIM(COALESCE(r.region_normalized_value, r.region_value)), '') IS NOT NULL THEN 'ΠΕΡΙΦΕΡΕΙΑ ' || BTRIM(COALESCE(r.region_normalized_value, r.region_value))
      ELSE COALESCE(o.organization_normalized_value, o.organization_value, 'ΔΗΜΟΣ ' || NULLIF(BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value)), ''), 'ΠΕΡΙΦΕΡΕΙΑ ' || NULLIF(BTRIM(COALESCE(r.region_normalized_value, r.region_value)), ''), '—')
    END AS authority_label,
    CASE
      WHEN UPPER(COALESCE(pr.contract_type, '')) LIKE '%ΥΠΗΡΕΣ%' THEN 'Υπηρεσίες'
      WHEN UPPER(COALESCE(pr.contract_type, '')) LIKE '%ΠΡΟΜΗΘΕΙ%' THEN 'Προμήθειες'
      WHEN UPPER(COALESCE(pr.contract_type, '')) LIKE '%ΕΡΓ%' THEN 'Έργα'
      ELSE 'Λοιπές'
    END AS contract_type,
    CASE
      WHEN LOWER(COALESCE(pr.procedure_type_value, '')) LIKE '%απευθείας ανάθεση%' THEN 'Απευθείας Ανάθεση'
      WHEN LOWER(COALESCE(pr.procedure_type_value, '')) LIKE '%ανοιχτή%' THEN 'Ανοιχτή Διαδικασία'
      WHEN LOWER(COALESCE(pr.procedure_type_value, '')) LIKE '%διαπραγ%' THEN 'Διαπραγμάτευση'
      ELSE 'Άλλη'
    END AS procedure,
    pr.amount_without_vat AS amount,
    COALESCE(cpv.cpvs, '[]'::jsonb) AS cpvs
  FROM proc_ranked pr
  LEFT JOIN LATERAL (
    SELECT o.organization_normalized_value, o.organization_value, o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = pr.organization_key
    ORDER BY o.id
    LIMIT 1
  ) o ON TRUE
  LEFT JOIN LATERAL (
    SELECT m.municipality_normalized_value, m.municipality_value
    FROM public.municipality m
    WHERE m.municipality_key = pr.municipality_key
    ORDER BY m.id
    LIMIT 1
  ) m ON TRUE
  LEFT JOIN LATERAL (
    SELECT r.region_normalized_value, r.region_value
    FROM public.region r
    WHERE r.region_key = pr.region_key
    ORDER BY r.id
    LIMIT 1
  ) r ON TRUE
  LEFT JOIN cpv_by_proc cpv
    ON cpv.procurement_id = pr.id
  WHERE pr.rn = 1
),
windowed AS (
  SELECT *
  FROM base
  WHERE effective_start IS NOT NULL
    AND effective_start <= make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, 12, 31)
    AND effective_end >= make_date(p_year_start, 1, 1)
),
section_rows AS (
  SELECT jsonb_agg(
    jsonb_build_object(
      'signedDate', contract_signed_date,
      'effectiveStart', effective_start,
      'effectiveEnd', effective_end,
      'orgName', org_name,
      'authorityLabel', authority_label,
      'contractType', contract_type,
      'procedure', procedure,
      'amount', amount,
      'cpvs', cpvs
    )
    ORDER BY COALESCE(contract_signed_date, effective_start), id
  ) AS rows
  FROM windowed
)
SELECT jsonb_build_object(
  'sectionRows', COALESCE((SELECT rows FROM section_rows), '[]'::jsonb)
);
$$;

GRANT EXECUTE ON FUNCTION public.get_contract_analysis(integer)
TO anon, authenticated, service_role;
