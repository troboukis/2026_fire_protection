BEGIN;

SET LOCAL lock_timeout = '10s';

-- Forest-fire document classification: 1 = prevention, 0 = suppression,
-- NULL = unknown. No default or backfill; classification is assigned later.
ALTER TABLE public.procurement
  ADD COLUMN IF NOT EXISTS prevention INTEGER;
ALTER TABLE public.diavgeia
  ADD COLUMN IF NOT EXISTS prevention INTEGER;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.procurement'::regclass
      AND conname = 'procurement_prevention_check'
  ) THEN
    ALTER TABLE public.procurement
      ADD CONSTRAINT procurement_prevention_check
      CHECK (prevention IN (0, 1));
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.diavgeia'::regclass
      AND conname = 'diavgeia_prevention_check'
  ) THEN
    ALTER TABLE public.diavgeia
      ADD CONSTRAINT diavgeia_prevention_check
      CHECK (prevention IN (0, 1));
  END IF;
END;
$$;

COMMENT ON COLUMN public.procurement.prevention IS
  'Forest-fire classification: 1 = prevention; 0 = suppression; NULL = unknown.';
COMMENT ON COLUMN public.diavgeia.prevention IS
  'Forest-fire classification: 1 = prevention; 0 = suppression; NULL = unknown.';

NOTIFY pgrst, 'reload schema';

COMMIT;
