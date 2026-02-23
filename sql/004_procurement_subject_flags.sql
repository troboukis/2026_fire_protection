-- 004_procurement_subject_flags.sql
-- Add subject-derived boolean flags to procurement_decisions.

BEGIN;

ALTER TABLE public.procurement_decisions
  ADD COLUMN IF NOT EXISTS subject_has_anatrop_or_anaklis BOOLEAN,
  ADD COLUMN IF NOT EXISTS subject_has_budget_balance_report_terms BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_procurement_subject_anatrop_anaklis
  ON public.procurement_decisions (subject_has_anatrop_or_anaklis);

CREATE INDEX IF NOT EXISTS idx_procurement_subject_budget_terms
  ON public.procurement_decisions (subject_has_budget_balance_report_terms);

COMMIT;

