ALTER TABLE public.current_fires
  DROP COLUMN IF EXISTS is_112_notice,
  DROP COLUMN IF EXISTS citizen_instructions_112,
  DROP COLUMN IF EXISTS citizen_instructions_112_geocoded;

CREATE TABLE IF NOT EXISTS public."112_notice" (
  notice_id TEXT PRIMARY KEY,
  current_fire_incident_key TEXT REFERENCES public.current_fires (incident_key)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
  source TEXT NOT NULL DEFAULT 'x',
  account TEXT NOT NULL DEFAULT '112Greece',
  post_id TEXT NOT NULL UNIQUE,
  post_url TEXT NOT NULL,
  posted_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  notice_type TEXT,
  notice_text TEXT NOT NULL,
  instructions_geocoded JSONB NOT NULL,
  municipality_keys TEXT[],
  matched_municipality_key TEXT,
  raw JSONB NOT NULL
);

ALTER TABLE public."112_notice"
  ADD COLUMN IF NOT EXISTS municipality_keys TEXT[],
  ADD COLUMN IF NOT EXISTS matched_municipality_key TEXT,
  DROP COLUMN IF EXISTS match_score,
  DROP COLUMN IF EXISTS match_reasons,
  DROP COLUMN IF EXISTS match_distance_km;

ALTER TABLE public."112_notice" ENABLE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS idx_112_notice_current_fire_incident_key
  ON public."112_notice" (current_fire_incident_key);

CREATE INDEX IF NOT EXISTS idx_112_notice_matched_municipality_key
  ON public."112_notice" (matched_municipality_key);

CREATE INDEX IF NOT EXISTS idx_112_notice_posted_at
  ON public."112_notice" (posted_at DESC);

CREATE INDEX IF NOT EXISTS idx_112_notice_instructions_geocoded_gin
  ON public."112_notice"
  USING GIN (instructions_geocoded);

DROP POLICY IF EXISTS public_read_112_notice ON public."112_notice";
CREATE POLICY public_read_112_notice
ON public."112_notice"
FOR SELECT
TO anon, authenticated
USING (true);
