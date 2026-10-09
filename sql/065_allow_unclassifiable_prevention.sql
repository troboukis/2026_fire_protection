BEGIN;

SET LOCAL lock_timeout = '10s';

-- NULL = not yet classified; 2 = reviewed but cannot be classified.
ALTER TABLE public.procurement
  DROP CONSTRAINT IF EXISTS procurement_prevention_check;
ALTER TABLE public.procurement
  ADD CONSTRAINT procurement_prevention_check CHECK (prevention IN (0, 1, 2));
ALTER TABLE public.diavgeia
  DROP CONSTRAINT IF EXISTS diavgeia_prevention_check;
ALTER TABLE public.diavgeia
  ADD CONSTRAINT diavgeia_prevention_check CHECK (prevention IN (0, 1, 2));

COMMENT ON COLUMN public.procurement.prevention IS
  'Forest-fire classification: 1 = prevention; 0 = suppression; 2 = reviewed but unclassifiable; NULL = not yet classified.';
COMMENT ON COLUMN public.diavgeia.prevention IS
  'Forest-fire classification: 1 = prevention; 0 = suppression; 2 = reviewed but unclassifiable; NULL = not yet classified.';

NOTIFY pgrst, 'reload schema';

COMMIT;
