BEGIN;

ALTER TABLE public.firms_active_fire_detections
DROP COLUMN IF EXISTS latest_pass_acquired_at,
DROP COLUMN IF EXISTS latest_pass_acquired_at_el;

COMMIT;
