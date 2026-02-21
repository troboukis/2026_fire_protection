BEGIN;

-- Keep everything in public for Supabase Data API compatibility.

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS public.organization (
  id BIGSERIAL PRIMARY KEY,
  org_type TEXT,
  org_name_clean TEXT,
  CONSTRAINT uq_organization UNIQUE (org_type, org_name_clean),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_organization_updated_at
BEFORE UPDATE ON public.organization
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.record (
  id BIGSERIAL PRIMARY KEY,
  ada TEXT NOT NULL UNIQUE,
  organization_id BIGINT NOT NULL REFERENCES public.organization(id) ON DELETE RESTRICT,
  "protocolNumber" TEXT,
  "issueDate" DATE,
  "documentUrl" TEXT,
  subject TEXT,
  "decisionType" TEXT,
  "thematicCategories" TEXT[],
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_record_updated_at
BEFORE UPDATE ON public.record
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_record_organization_id
ON public.record (organization_id);

CREATE INDEX IF NOT EXISTS idx_record_decision_type
ON public.record ("decisionType");

CREATE TABLE IF NOT EXISTS public.file (
  ada TEXT PRIMARY KEY REFERENCES public.record(ada) ON DELETE CASCADE,
  extracted_text TEXT,
  extracted_data JSONB,
  extraction_status TEXT NOT NULL DEFAULT 'PENDING',
  extraction_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_file_updated_at
BEFORE UPDATE ON public.file
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_file_status
ON public.file (extraction_status);

COMMIT;
