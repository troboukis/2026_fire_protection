BEGIN;

DROP POLICY IF EXISTS public_read_beneficiary ON public.beneficiary;
CREATE POLICY public_read_beneficiary
ON public.beneficiary
FOR SELECT
TO anon, authenticated
USING (true);

COMMIT;
