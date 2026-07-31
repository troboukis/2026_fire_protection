ALTER TABLE public.current_fires
  ADD COLUMN IF NOT EXISTS source_location TEXT;

UPDATE public.current_fires
SET source_location = NULLIF(
  raw::jsonb -> 'posts' -> -1 -> 'extracted' ->> 'location_name',
  ''
)
WHERE source = 'fireservice_x'
  AND source_location IS NULL
  AND raw IS NOT NULL
  AND raw ~ '^\s*\{';

