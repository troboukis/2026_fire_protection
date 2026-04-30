BEGIN;

-- Enable RLS on all base tables in the public schema.
-- Views such as public.works_enriched are not affected by this block.
DO $$
DECLARE
  table_name text;
BEGIN
  FOR table_name IN
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
  LOOP
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', table_name);
  END LOOP;
END $$;

-- Preserve the current frontend behavior for direct browser reads.
-- These tables are already used via supabase.from(...) in the app.

DROP POLICY IF EXISTS public_read_procurement ON public.procurement;
CREATE POLICY public_read_procurement
ON public.procurement
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_payment ON public.payment;
CREATE POLICY public_read_payment
ON public.payment
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_beneficiary ON public.beneficiary;
CREATE POLICY public_read_beneficiary
ON public.beneficiary
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_cpv ON public.cpv;
CREATE POLICY public_read_cpv
ON public.cpv
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_organization ON public.organization;
CREATE POLICY public_read_organization
ON public.organization
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_region ON public.region;
CREATE POLICY public_read_region
ON public.region
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_municipality ON public.municipality;
CREATE POLICY public_read_municipality
ON public.municipality
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_municipality_normalized_name ON public.municipality_normalized_name;
CREATE POLICY public_read_municipality_normalized_name
ON public.municipality_normalized_name
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_municipality_fire_protection_data ON public.municipality_fire_protection_data;
CREATE POLICY public_read_municipality_fire_protection_data
ON public.municipality_fire_protection_data
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_forest_fire ON public.forest_fire;
CREATE POLICY public_read_forest_fire
ON public.forest_fire
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_fund ON public.fund;
CREATE POLICY public_read_fund
ON public.fund
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_copernicus ON public.copernicus;
CREATE POLICY public_read_copernicus
ON public.copernicus
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_works ON public.works;
CREATE POLICY public_read_works
ON public.works
FOR SELECT
TO anon, authenticated
USING (true);

COMMIT;
