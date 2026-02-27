-- 007_raw_procurements.sql
-- Raw KIMDIS procurements table used by the web app.

CREATE TABLE IF NOT EXISTS public.raw_procurements (
  id                                             BIGSERIAL PRIMARY KEY,
  title                                          TEXT,
  reference_number                               TEXT,
  submission_at                                  TIMESTAMPTZ,
  contract_signed_date                           DATE,
  start_date                                     DATE,
  no_end_date                                    BOOLEAN,
  end_date                                       DATE,
  cancelled                                      BOOLEAN,
  cancellation_date                              DATE,
  cancellation_type                              TEXT,
  cancellation_reason                            TEXT,
  decision_related_ada                           TEXT,
  contract_number                                TEXT,
  organization_vat_number                        TEXT,
  greek_organization_vat_number                  BOOLEAN,
  diavgeia_ada                                   TEXT,
  budget                                         NUMERIC(18, 2),
  contract_budget                                NUMERIC(18, 2),
  bids_submitted                                 INTEGER,
  max_bids_submitted                             INTEGER,
  number_of_sections                             INTEGER,
  central_government_authority                   TEXT,
  nuts_code_key                                  TEXT,
  nuts_code_value                                TEXT,
  organization_key                               TEXT,
  organization_value                             TEXT,
  procedure_type_key                             TEXT,
  procedure_type_value                           TEXT,
  award_procedure                                TEXT,
  nuts_city                                      TEXT,
  nuts_postal_code                               TEXT,
  centralized_markets                            TEXT,
  contract_type                                  TEXT,
  assign_criteria                                TEXT,
  classification_of_public_law_organization      TEXT,
  type_of_contracting_authority                  TEXT,
  contracting_authority_activity                 TEXT,
  contract_duration                              INTEGER,
  contract_duration_unit_of_measure              TEXT,
  contract_related_ada                           TEXT,
  funding_details_cofund                         TEXT,
  funding_details_self_fund                      TEXT,
  funding_details_espa                           TEXT,
  funding_details_regular_budget                 TEXT,
  units_operator                                 TEXT,
  signers                                        TEXT,
  first_member_vat_number                        TEXT,
  first_member_name                              TEXT,
  total_cost_with_vat                            NUMERIC(18, 2),
  total_cost_without_vat                         NUMERIC(18, 2),
  short_descriptions                             TEXT,
  cpv_keys                                       TEXT,
  cpv_values                                     TEXT,
  green_contracts                                TEXT,
  auction_ref_no                                 TEXT,
  payment_ref_no                                 TEXT,
  ingested_at                                    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_procurements_submission_at
  ON public.raw_procurements (submission_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_procurements_reference_number
  ON public.raw_procurements (reference_number);

CREATE INDEX IF NOT EXISTS idx_raw_procurements_contract_number
  ON public.raw_procurements (contract_number);

CREATE INDEX IF NOT EXISTS idx_raw_procurements_org_vat
  ON public.raw_procurements (organization_vat_number);

ALTER TABLE public.raw_procurements ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public_read" ON public.raw_procurements
  FOR SELECT USING (TRUE);
