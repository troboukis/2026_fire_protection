-- Supporting indexes for expensive homepage RPCs:
-- get_hero_section_data, get_latest_contract_cards,
-- get_featured_beneficiaries, get_latest_funding_year_municipality_spend.

CREATE INDEX IF NOT EXISTS idx_payment_procurement_id_id_cover
  ON public.payment (procurement_id, id)
  INCLUDE (
    amount_without_vat,
    amount_with_vat,
    beneficiary_name,
    beneficiary_vat_number,
    signers,
    payment_ref_no,
    fiscal_year
  );

CREATE INDEX IF NOT EXISTS idx_payment_fiscal_year_procurement_amount
  ON public.payment (fiscal_year, procurement_id)
  INCLUDE (amount_without_vat)
  WHERE amount_without_vat IS NOT NULL
    AND procurement_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_payment_beneficiary_payment_vat
  ON public.payment_beneficiary (payment_id, beneficiary_vat_number);

CREATE INDEX IF NOT EXISTS idx_payment_beneficiary_vat_payment
  ON public.payment_beneficiary (beneficiary_vat_number, payment_id);

CREATE INDEX IF NOT EXISTS idx_cpv_procurement_value_key
  ON public.cpv (procurement_id, cpv_value, cpv_key);

CREATE INDEX IF NOT EXISTS idx_procurement_submission_id_not_null
  ON public.procurement (submission_at DESC NULLS LAST, id DESC)
  WHERE submission_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_procurement_contract_signed_id_active
  ON public.procurement (contract_signed_date DESC NULLS LAST, id DESC)
  WHERE contract_signed_date IS NOT NULL
    AND COALESCE(cancelled, FALSE) = FALSE;

CREATE INDEX IF NOT EXISTS idx_procurement_prev_reference_no_trim
  ON public.procurement ((NULLIF(BTRIM(prev_reference_no), '')))
  WHERE NULLIF(BTRIM(prev_reference_no), '') IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_procurement_next_ref_no_trim
  ON public.procurement ((NULLIF(BTRIM(next_ref_no), '')))
  WHERE NULLIF(BTRIM(next_ref_no), '') IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_procurement_reference_contract_dedup
  ON public.procurement (
    reference_number,
    diavgeia_ada,
    contract_number,
    organization_key,
    contract_signed_date,
    id DESC
  );

CREATE INDEX IF NOT EXISTS idx_organization_key_id_cover
  ON public.organization (organization_key, id)
  INCLUDE (organization_normalized_value, organization_value, authority_scope);

CREATE INDEX IF NOT EXISTS idx_municipality_normalized_key_id_cover
  ON public.municipality_normalized_name (municipality_key, id)
  INCLUDE (municipality_normalized_value);

CREATE INDEX IF NOT EXISTS idx_region_key_id_cover
  ON public.region (region_key, id)
  INCLUDE (region_normalized_value, region_value);
