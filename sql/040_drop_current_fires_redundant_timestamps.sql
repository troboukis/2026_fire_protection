ALTER TABLE public.current_fires
  DROP COLUMN IF EXISTS scraped_at,
  DROP COLUMN IF EXISTS created_at,
  DROP COLUMN IF EXISTS updated_at;
