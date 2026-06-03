BEGIN;

ALTER TABLE public.firms_active_fire_detections
ADD COLUMN IF NOT EXISTS is_latest_pass BOOLEAN NOT NULL DEFAULT FALSE;

WITH latest_pass AS (
  SELECT source_product, MAX(acquired_at) AS max_acquired_at
  FROM public.firms_active_fire_detections
  GROUP BY source_product
)
UPDATE public.firms_active_fire_detections AS detection
SET
  is_latest_pass = detection.acquired_at = latest_pass.max_acquired_at
FROM latest_pass
WHERE detection.source_product = latest_pass.source_product;

CREATE INDEX IF NOT EXISTS idx_firms_active_fire_detections_latest_pass
  ON public.firms_active_fire_detections (source_product, is_latest_pass, acquired_at DESC);

COMMIT;
