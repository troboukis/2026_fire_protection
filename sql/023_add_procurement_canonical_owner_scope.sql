BEGIN;

ALTER TABLE public.procurement
  ADD COLUMN IF NOT EXISTS canonical_owner_scope TEXT;

COMMIT;
