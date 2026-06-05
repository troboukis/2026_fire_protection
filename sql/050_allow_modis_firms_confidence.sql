BEGIN;

ALTER TABLE public.firms_active_fire_detections
  DROP CONSTRAINT IF EXISTS chk_firms_active_fire_detections_confidence;

ALTER TABLE public.firms_active_fire_detections
  ADD CONSTRAINT chk_firms_active_fire_detections_confidence
  CHECK (
    confidence IN ('l', 'n', 'h')
    OR (
      CASE
        WHEN confidence ~ '^[0-9]+(\.[0-9]+)?$'
        THEN confidence::NUMERIC >= 0 AND confidence::NUMERIC <= 100
        ELSE FALSE
      END
    )
  );

COMMIT;
