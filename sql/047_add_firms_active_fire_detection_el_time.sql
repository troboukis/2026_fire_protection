BEGIN;

ALTER TABLE public.firms_active_fire_detections
ADD COLUMN IF NOT EXISTS acquired_at_el TIMESTAMP;

UPDATE public.firms_active_fire_detections
SET acquired_at_el = acquired_at AT TIME ZONE 'Europe/Athens'
WHERE acquired_at_el IS NULL;

ALTER TABLE public.firms_active_fire_detections
ALTER COLUMN acquired_at_el SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_firms_active_fire_detections_acquired_at_el
  ON public.firms_active_fire_detections (acquired_at_el DESC);

COMMIT;
