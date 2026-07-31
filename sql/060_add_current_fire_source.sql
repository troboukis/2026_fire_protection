ALTER TABLE public.current_fires
  ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'fireservice_live_page',
  ADD COLUMN IF NOT EXISTS source_account TEXT,
  ADD COLUMN IF NOT EXISTS source_post_id TEXT,
  ADD COLUMN IF NOT EXISTS source_url TEXT;

UPDATE public.current_fires
SET source = 'fireservice_live_page'
WHERE source IS NULL OR btrim(source) = '';

CREATE INDEX IF NOT EXISTS idx_current_fires_source
  ON public.current_fires (source);

