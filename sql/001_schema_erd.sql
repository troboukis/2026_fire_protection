-- 001_schema_erd.sql
-- ERD-aligned schema for Supabase/Postgres.
-- Source: fire-protection ERD shared on 2026-03-01.
-- Notes:
-- - Keeps your table structure, but normalizes naming and datatypes.
-- - Renames: forest-fire -> forest_fire, region_nurmalized_name -> region_normalized_name.
-- - Uses snake_case for FK columns (e.g. region_key, organization_key).

BEGIN;

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- Normalized name dictionaries
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.municipality_normalized_name (
  id                           BIGSERIAL PRIMARY KEY,
  municipality_key             TEXT NOT NULL UNIQUE,
  municipality_value           TEXT NOT NULL,
  municipality_normalized_value TEXT NOT NULL,
  created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.region_normalized_name (
  id                         BIGSERIAL PRIMARY KEY,
  region_value               TEXT NOT NULL,
  region_normalized_value    TEXT NOT NULL,
  created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.organization_normalized_name (
  id                          BIGSERIAL PRIMARY KEY,
  organization_value          TEXT NOT NULL,
  organization_normalized_name TEXT NOT NULL,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Dimensions / dictionaries
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.diavgeia_document_type (
  decision_uid                TEXT PRIMARY KEY,
  decision_type               TEXT NOT NULL,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.beneficiary (
  beneficiary_vat_number      TEXT PRIMARY KEY,
  beneficiary_name            TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Core entities
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.region (
  id                          BIGSERIAL PRIMARY KEY,
  region_key                  TEXT NOT NULL,
  region_value                TEXT NOT NULL,
  region_normalized_value     TEXT NOT NULL,
  source_system               TEXT,
  source_key                  TEXT,
  region_afm                  TEXT,
  nuts_postal_code            TEXT,
  nuts_postal_city            TEXT,
  nuts_code_value             TEXT,
  nuts_code_key               TEXT,
  region_normalized_name_id   BIGINT,
  organization_key            TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_region_key_value UNIQUE (region_key, region_value)
);

CREATE TABLE IF NOT EXISTS public.organization (
  id                          BIGSERIAL PRIMARY KEY,
  organization_key            TEXT NOT NULL,
  organization_value          TEXT NOT NULL,
  organization_normalized_value TEXT NOT NULL,
  authority_scope            TEXT,
  source_system               TEXT,
  source_key                  TEXT,
  organization_afm            TEXT,
  nuts_postal_code            TEXT,
  nuts_city                   TEXT,
  nuts_code_value             TEXT,
  nuts_code_key               TEXT,
  organization_normalized_name_id BIGINT,
  diavgeia_id                 BIGINT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_organization_key_value UNIQUE (organization_key, organization_value)
);

CREATE TABLE IF NOT EXISTS public.municipality (
  id                          BIGSERIAL PRIMARY KEY,
  municipality_key            TEXT NOT NULL,
  municipality_value          TEXT NOT NULL,
  municipality_normalized_value TEXT NOT NULL,
  source_system               TEXT,
  source_key                  TEXT,
  municipality_afm            TEXT,
  nuts_postal_code            TEXT,
  nuts_city                   TEXT,
  nuts_code_value             TEXT,
  nuts_code_key               TEXT,
  municipality_normalized_name_id BIGINT,
  region_key                  TEXT,
  organization_key            TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_municipality_key_value UNIQUE (municipality_key, municipality_value)
);

CREATE TABLE IF NOT EXISTS public.forest_fire (
  id                          BIGSERIAL PRIMARY KEY,
  municipality_key            TEXT,
  region_key                  TEXT,
  year                        SMALLINT,
  date_start                  DATE,
  date_end                    DATE,
  nomos                       TEXT,
  area_name                   TEXT,
  lat                         NUMERIC(10, 6),
  lon                         NUMERIC(10, 6),
  burned_forest_stremata      NUMERIC(14, 2),
  burned_woodland_stremata    NUMERIC(14, 2),
  burned_grassland_stremata   NUMERIC(14, 2),
  burned_grove_stremata       NUMERIC(14, 2),
  burned_other_stremata       NUMERIC(14, 2),
  burned_total_stremata       NUMERIC(14, 2),
  burned_total_ha             NUMERIC(14, 2),
  source                      TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_forest_fire_year CHECK (year IS NULL OR year BETWEEN 1900 AND 2100),
  CONSTRAINT chk_forest_fire_lat CHECK (lat IS NULL OR (lat >= -90 AND lat <= 90)),
  CONSTRAINT chk_forest_fire_lon CHECK (lon IS NULL OR (lon >= -180 AND lon <= 180))
);

CREATE TABLE IF NOT EXISTS public.fund (
  id                          BIGSERIAL PRIMARY KEY,
  region_key                  TEXT,
  organization_key            TEXT,
  municipality_key            TEXT,
  year                        SMALLINT,
  allocation_type             TEXT,
  recipient_type              TEXT,
  recipient_raw               TEXT,
  nomos                       TEXT,
  amount_eur                  NUMERIC(14, 2),
  source_file                 TEXT,
  source_ada                  TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_fund_year CHECK (year IS NULL OR year BETWEEN 1900 AND 2100),
  CONSTRAINT chk_fund_amount CHECK (amount_eur IS NULL OR amount_eur >= 0)
);

CREATE TABLE IF NOT EXISTS public.diavgeia (
  id                          BIGSERIAL PRIMARY KEY,
  region_key                  TEXT,
  organization_key            TEXT,
  municipality_key            TEXT,
  ada                         TEXT UNIQUE,
  protocol_number             TEXT,
  submission_timestamp        TIMESTAMPTZ,
  publish_timestamp           TIMESTAMPTZ,
  status                      TEXT,
  non_revokable               BOOLEAN,
  document_url                TEXT,
  subject                     TEXT,
  document_type               TEXT,
  version_comment             TEXT,
  thematic_categories         TEXT,
  organization                TEXT,
  cooperating_organizations   TEXT,
  unit_ids                    TEXT,
  org                         TEXT,
  org_type                    TEXT,
  org_name_clean              TEXT,
  spending_signers            TEXT,
  spending_contractors_afm    TEXT,
  spending_contractors_name   TEXT,
  spending_contractors_value  TEXT,
  diavgeia_document_type_decision_uid TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.payment (
  id                          BIGSERIAL PRIMARY KEY,
  procurement_id              BIGINT,
  diavgeia_document_type_decision_uid TEXT,
  diavgeia_id                 BIGINT,
  beneficiaries_count         INTEGER,
  signers                     TEXT,
  beneficiary_name            TEXT,
  beneficiary_vat_number      TEXT,
  amount_with_vat             NUMERIC(14, 2),
  amount_without_vat          NUMERIC(14, 2),
  kae_ale                     TEXT,
  fiscal_year                 SMALLINT,
  budget_category             TEXT,
  counter_party               TEXT,
  payment_ref_no              TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_payment_procurement UNIQUE (procurement_id),
  CONSTRAINT chk_payment_fiscal_year CHECK (fiscal_year IS NULL OR fiscal_year BETWEEN 1900 AND 2100),
  CONSTRAINT chk_payment_amount_with_vat CHECK (amount_with_vat IS NULL OR amount_with_vat >= 0),
  CONSTRAINT chk_payment_amount_without_vat CHECK (amount_without_vat IS NULL OR amount_without_vat >= 0)
);

CREATE TABLE IF NOT EXISTS public.procurement (
  id                          BIGSERIAL PRIMARY KEY,
  title                       TEXT,
  reference_number            TEXT,
  prev_reference_no           TEXT,
  notice_reference_number     TEXT,
  next_ref_no                 TEXT,
  next_extended               BOOLEAN,
  next_modified               BOOLEAN,
  submission_at               TIMESTAMPTZ,
  contract_signed_date        DATE,
  start_date                  DATE,
  no_end_date                 BOOLEAN,
  end_date                    DATE,
  cancelled                   BOOLEAN,
  cancellation_date           DATE,
  cancellation_type           TEXT,
  cancellation_reason         TEXT,
  decision_related_ada        TEXT,
  contract_number             TEXT,
  organization_vat_number     TEXT,
  greek_organization_vat_number BOOLEAN,
  diavgeia_ada                TEXT,
  budget                      NUMERIC(18, 2),
  contract_budget             NUMERIC(18, 2),
  bids_submitted              INTEGER,
  max_bids_submitted          INTEGER,
  number_of_sections          INTEGER,
  central_government_authority TEXT,
  procedure_type_key          TEXT,
  procedure_type_value        TEXT,
  award_procedure             TEXT,
  centralized_markets         TEXT,
  contract_type               TEXT,
  assign_criteria             TEXT,
  classification_of_public_law_organization TEXT,
  type_of_contracting_authority TEXT,
  contracting_authority_activity TEXT,
  contract_duration           INTEGER,
  contract_duration_unit_of_measure TEXT,
  contract_related_ada        TEXT,
  funding_details_cofund      TEXT,
  funding_details_self_fund   TEXT,
  funding_details_espa        TEXT,
  funding_details_regular_budget TEXT,
  units_operator              TEXT,
  short_descriptions          TEXT,
  green_contracts             TEXT,
  auction_ref_no              TEXT,
  ingested_at                 TIMESTAMPTZ,
  region_key                  TEXT,
  organization_key            TEXT,
  municipality_key            TEXT,
  canonical_owner_scope       TEXT,
  payment_id                  BIGINT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_procurement_reference_number UNIQUE (reference_number)
);

CREATE TABLE IF NOT EXISTS public.cpv (
  id                          BIGSERIAL PRIMARY KEY,
  cpv_key                     TEXT NOT NULL,
  cpv_value                   TEXT,
  procurement_id              BIGINT NOT NULL,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_cpv_procurement_key UNIQUE (procurement_id, cpv_key),
  CONSTRAINT fk_cpv_procurement
    FOREIGN KEY (procurement_id)
    REFERENCES public.procurement(id)
    ON DELETE CASCADE
);

-- ---------------------------------------------------------------------------
-- Bridge tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.diavgeia_beneficiary (
  diavgeia_id                 BIGINT NOT NULL,
  beneficiary_vat_number      TEXT NOT NULL,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (diavgeia_id, beneficiary_vat_number)
);

CREATE TABLE IF NOT EXISTS public.diavgeia_procurement (
  diavgeia_id                 BIGINT NOT NULL,
  procurement_id              BIGINT NOT NULL,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (diavgeia_id, procurement_id)
);

CREATE TABLE IF NOT EXISTS public.payment_beneficiary (
  payment_id                  BIGINT NOT NULL,
  beneficiary_vat_number      TEXT NOT NULL,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (payment_id, beneficiary_vat_number)
);

-- ---------------------------------------------------------------------------
-- Foreign keys (DEFERRABLE to tolerate cyclical relations during ingestion)
-- ---------------------------------------------------------------------------

ALTER TABLE public.region
  ADD CONSTRAINT fk_region_normalized_name
  FOREIGN KEY (region_normalized_name_id)
  REFERENCES public.region_normalized_name(id)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.organization
  ADD CONSTRAINT fk_organization_normalized_name
  FOREIGN KEY (organization_normalized_name_id)
  REFERENCES public.organization_normalized_name(id)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.organization
  ADD CONSTRAINT fk_organization_diavgeia
  FOREIGN KEY (diavgeia_id)
  REFERENCES public.diavgeia(id)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.municipality
  ADD CONSTRAINT fk_municipality_normalized_name
  FOREIGN KEY (municipality_normalized_name_id)
  REFERENCES public.municipality_normalized_name(id)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.diavgeia
  ADD CONSTRAINT fk_diavgeia_document_type
  FOREIGN KEY (diavgeia_document_type_decision_uid)
  REFERENCES public.diavgeia_document_type(decision_uid)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.payment
  ADD CONSTRAINT fk_payment_procurement
  FOREIGN KEY (procurement_id)
  REFERENCES public.procurement(id)
  ON DELETE CASCADE
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.payment
  ADD CONSTRAINT fk_payment_document_type
  FOREIGN KEY (diavgeia_document_type_decision_uid)
  REFERENCES public.diavgeia_document_type(decision_uid)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.payment
  ADD CONSTRAINT fk_payment_diavgeia
  FOREIGN KEY (diavgeia_id)
  REFERENCES public.diavgeia(id)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.procurement
  ADD CONSTRAINT fk_procurement_payment
  FOREIGN KEY (payment_id)
  REFERENCES public.payment(id)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.diavgeia_beneficiary
  ADD CONSTRAINT fk_diavgeia_beneficiary_diavgeia
  FOREIGN KEY (diavgeia_id)
  REFERENCES public.diavgeia(id)
  ON DELETE CASCADE;

ALTER TABLE public.diavgeia_beneficiary
  ADD CONSTRAINT fk_diavgeia_beneficiary_beneficiary
  FOREIGN KEY (beneficiary_vat_number)
  REFERENCES public.beneficiary(beneficiary_vat_number)
  ON DELETE CASCADE;

ALTER TABLE public.diavgeia_procurement
  ADD CONSTRAINT fk_diavgeia_procurement_diavgeia
  FOREIGN KEY (diavgeia_id)
  REFERENCES public.diavgeia(id)
  ON DELETE CASCADE;

ALTER TABLE public.diavgeia_procurement
  ADD CONSTRAINT fk_diavgeia_procurement_procurement
  FOREIGN KEY (procurement_id)
  REFERENCES public.procurement(id)
  ON DELETE CASCADE;

ALTER TABLE public.payment_beneficiary
  ADD CONSTRAINT fk_payment_beneficiary_payment
  FOREIGN KEY (payment_id)
  REFERENCES public.payment(id)
  ON DELETE CASCADE;

ALTER TABLE public.payment_beneficiary
  ADD CONSTRAINT fk_payment_beneficiary_beneficiary
  FOREIGN KEY (beneficiary_vat_number)
  REFERENCES public.beneficiary(beneficiary_vat_number)
  ON DELETE CASCADE;

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_region_org_key ON public.region (organization_key);
CREATE INDEX IF NOT EXISTS idx_region_key ON public.region (region_key);
CREATE INDEX IF NOT EXISTS idx_region_normalized_value ON public.region (region_normalized_value);
CREATE INDEX IF NOT EXISTS idx_region_normalized_name_id ON public.region (region_normalized_name_id);

CREATE INDEX IF NOT EXISTS idx_org_normalized_name_id ON public.organization (organization_normalized_name_id);
CREATE INDEX IF NOT EXISTS idx_org_diavgeia_id ON public.organization (diavgeia_id);
CREATE INDEX IF NOT EXISTS idx_organization_key ON public.organization (organization_key);
CREATE INDEX IF NOT EXISTS idx_org_normalized_value ON public.organization (organization_normalized_value);

CREATE INDEX IF NOT EXISTS idx_municipality_region_key ON public.municipality (region_key);
CREATE INDEX IF NOT EXISTS idx_municipality_org_key ON public.municipality (organization_key);
CREATE INDEX IF NOT EXISTS idx_municipality_key ON public.municipality (municipality_key);
CREATE INDEX IF NOT EXISTS idx_municipality_normalized_value ON public.municipality (municipality_normalized_value);
CREATE INDEX IF NOT EXISTS idx_municipality_normalized_name_id ON public.municipality (municipality_normalized_name_id);

CREATE INDEX IF NOT EXISTS idx_forest_fire_region_key ON public.forest_fire (region_key);
CREATE INDEX IF NOT EXISTS idx_forest_fire_municipality_key ON public.forest_fire (municipality_key);
CREATE INDEX IF NOT EXISTS idx_forest_fire_year ON public.forest_fire (year);

CREATE INDEX IF NOT EXISTS idx_fund_region_key ON public.fund (region_key);
CREATE INDEX IF NOT EXISTS idx_fund_org_key ON public.fund (organization_key);
CREATE INDEX IF NOT EXISTS idx_fund_municipality_key ON public.fund (municipality_key);
CREATE INDEX IF NOT EXISTS idx_fund_year ON public.fund (year);

CREATE INDEX IF NOT EXISTS idx_diavgeia_region_key ON public.diavgeia (region_key);
CREATE INDEX IF NOT EXISTS idx_diavgeia_org_key ON public.diavgeia (organization_key);
CREATE INDEX IF NOT EXISTS idx_diavgeia_municipality_key ON public.diavgeia (municipality_key);
CREATE INDEX IF NOT EXISTS idx_diavgeia_submission_ts ON public.diavgeia (submission_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_diavgeia_publish_ts ON public.diavgeia (publish_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_diavgeia_doc_type_uid ON public.diavgeia (diavgeia_document_type_decision_uid);

CREATE INDEX IF NOT EXISTS idx_payment_diavgeia_id ON public.payment (diavgeia_id);
CREATE INDEX IF NOT EXISTS idx_payment_fiscal_year ON public.payment (fiscal_year);
CREATE INDEX IF NOT EXISTS idx_payment_doc_type_uid ON public.payment (diavgeia_document_type_decision_uid);
CREATE INDEX IF NOT EXISTS idx_payment_procurement_id ON public.payment (procurement_id);
CREATE INDEX IF NOT EXISTS idx_payment_beneficiary_vat ON public.payment (beneficiary_vat_number);

CREATE INDEX IF NOT EXISTS idx_procurement_submission_at ON public.procurement (submission_at DESC);
CREATE INDEX IF NOT EXISTS idx_procurement_contract_signed_date ON public.procurement (contract_signed_date DESC);
CREATE INDEX IF NOT EXISTS idx_procurement_region_key ON public.procurement (region_key);
CREATE INDEX IF NOT EXISTS idx_procurement_org_key ON public.procurement (organization_key);
CREATE INDEX IF NOT EXISTS idx_procurement_municipality_key ON public.procurement (municipality_key);
CREATE INDEX IF NOT EXISTS idx_procurement_payment_id ON public.procurement (payment_id);
CREATE INDEX IF NOT EXISTS idx_procurement_diavgeia_ada ON public.procurement (diavgeia_ada);
CREATE INDEX IF NOT EXISTS idx_procurement_prev_reference_no ON public.procurement (prev_reference_no);

CREATE INDEX IF NOT EXISTS idx_cpv_procurement_id ON public.cpv (procurement_id);
CREATE INDEX IF NOT EXISTS idx_cpv_key ON public.cpv (cpv_key);

-- updated_at triggers
 DROP TRIGGER IF EXISTS trg_municipality_normalized_name_updated_at ON public.municipality_normalized_name;
CREATE TRIGGER trg_municipality_normalized_name_updated_at
BEFORE UPDATE ON public.municipality_normalized_name
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_region_normalized_name_updated_at ON public.region_normalized_name;
CREATE TRIGGER trg_region_normalized_name_updated_at
BEFORE UPDATE ON public.region_normalized_name
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_organization_normalized_name_updated_at ON public.organization_normalized_name;
CREATE TRIGGER trg_organization_normalized_name_updated_at
BEFORE UPDATE ON public.organization_normalized_name
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_diavgeia_document_type_updated_at ON public.diavgeia_document_type;
CREATE TRIGGER trg_diavgeia_document_type_updated_at
BEFORE UPDATE ON public.diavgeia_document_type
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_beneficiary_updated_at ON public.beneficiary;
CREATE TRIGGER trg_beneficiary_updated_at
BEFORE UPDATE ON public.beneficiary
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_region_updated_at ON public.region;
CREATE TRIGGER trg_region_updated_at
BEFORE UPDATE ON public.region
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_organization_updated_at ON public.organization;
CREATE TRIGGER trg_organization_updated_at
BEFORE UPDATE ON public.organization
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_municipality_updated_at ON public.municipality;
CREATE TRIGGER trg_municipality_updated_at
BEFORE UPDATE ON public.municipality
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_forest_fire_updated_at ON public.forest_fire;
CREATE TRIGGER trg_forest_fire_updated_at
BEFORE UPDATE ON public.forest_fire
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_fund_updated_at ON public.fund;
CREATE TRIGGER trg_fund_updated_at
BEFORE UPDATE ON public.fund
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_diavgeia_updated_at ON public.diavgeia;
CREATE TRIGGER trg_diavgeia_updated_at
BEFORE UPDATE ON public.diavgeia
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_payment_updated_at ON public.payment;
CREATE TRIGGER trg_payment_updated_at
BEFORE UPDATE ON public.payment
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_procurement_updated_at ON public.procurement;
CREATE TRIGGER trg_procurement_updated_at
BEFORE UPDATE ON public.procurement
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

 DROP TRIGGER IF EXISTS trg_cpv_updated_at ON public.cpv;
CREATE TRIGGER trg_cpv_updated_at
BEFORE UPDATE ON public.cpv
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- ---------------------------------------------------------------------------
-- API access grants (Supabase anon/authenticated roles)
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon, authenticated, service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;

COMMIT;
