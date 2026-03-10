BEGIN;

ALTER TABLE public.procurement
  ADD COLUMN IF NOT EXISTS prev_reference_no TEXT,
  ADD COLUMN IF NOT EXISTS notice_reference_number TEXT,
  ADD COLUMN IF NOT EXISTS next_ref_no TEXT,
  ADD COLUMN IF NOT EXISTS next_extended BOOLEAN,
  ADD COLUMN IF NOT EXISTS next_modified BOOLEAN;

COMMIT;
