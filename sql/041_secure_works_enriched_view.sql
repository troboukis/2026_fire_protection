BEGIN;

ALTER VIEW public.works_enriched
  SET (security_invoker = true);

COMMIT;
