CREATE TABLE IF NOT EXISTS public.current_fires (
  incident_key TEXT PRIMARY KEY,
  first_seen_at TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT TRUE,
  scraped_at TIMESTAMPTZ NOT NULL,
  category TEXT NOT NULL,
  region TEXT,
  regional_unit TEXT,
  municipality_key TEXT,
  municipality_normalized_value TEXT,
  municipality_raw TEXT,
  fuel_type TEXT,
  start_date DATE,
  days_burning INTEGER,
  status_updated_at TIMESTAMPTZ,
  status TEXT,
  raw TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.current_fires ENABLE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS idx_current_fires_is_current
  ON public.current_fires (is_current);

CREATE INDEX IF NOT EXISTS idx_current_fires_municipality_key
  ON public.current_fires (municipality_key);

CREATE INDEX IF NOT EXISTS idx_current_fires_status
  ON public.current_fires (status);
