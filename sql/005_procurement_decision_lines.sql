-- 005_procurement_decision_lines.sql
-- Add line-level procurement table (multiple amounts/counterparties per ADA).

BEGIN;

CREATE TABLE IF NOT EXISTS public.procurement_decision_lines (
  id                         BIGSERIAL PRIMARY KEY,
  ada                        TEXT NOT NULL REFERENCES public.procurement_decisions(ada) ON DELETE CASCADE,
  line_type                  TEXT NOT NULL,  -- spending_contractor / payment_beneficiary / commitment_kae_line / direct_assignment
  line_index                 INTEGER NOT NULL,
  source_field               TEXT NOT NULL,  -- source raw CSV field that generated the line
  counterparty_afm           TEXT,
  counterparty_name          TEXT,
  amount_raw                 TEXT,           -- raw string from Diavgeia (e.g. '8.755,24')
  amount_eur                 NUMERIC(14, 2), -- parsed numeric when possible
  currency                   TEXT,
  kae_ale_number             TEXT,
  remaining_kae_ale          TEXT,
  remaining_available_credit TEXT,
  raw_line_json              JSONB,          -- full raw line object for auditability
  created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_procurement_decision_lines UNIQUE (ada, line_type, line_index)
);

CREATE INDEX IF NOT EXISTS idx_proc_lines_ada
  ON public.procurement_decision_lines (ada);

CREATE INDEX IF NOT EXISTS idx_proc_lines_type
  ON public.procurement_decision_lines (line_type);

CREATE INDEX IF NOT EXISTS idx_proc_lines_counterparty_afm
  ON public.procurement_decision_lines (counterparty_afm);

CREATE INDEX IF NOT EXISTS idx_proc_lines_amount
  ON public.procurement_decision_lines (amount_eur);

ALTER TABLE public.procurement_decision_lines ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public_read" ON public.procurement_decision_lines
  FOR SELECT USING (true);

COMMIT;

