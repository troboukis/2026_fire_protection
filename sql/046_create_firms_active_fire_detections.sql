BEGIN;

CREATE TABLE IF NOT EXISTS public.firms_active_fire_detections (
  id                              BIGSERIAL PRIMARY KEY,
  source_product                  TEXT NOT NULL DEFAULT 'VIIRS_NOAA21_NRT',
  source_area                     TEXT NOT NULL DEFAULT 'world',
  detection_date                  DATE NOT NULL,
  acquisition_time_utc            SMALLINT NOT NULL,
  acquired_at                     TIMESTAMPTZ NOT NULL,
  acquired_at_el                  TIMESTAMP NOT NULL,
  is_latest_pass                  BOOLEAN NOT NULL DEFAULT FALSE,
  latitude                        NUMERIC(9, 6) NOT NULL,
  longitude                       NUMERIC(9, 6) NOT NULL,
  bright_ti4                      NUMERIC(8, 2),
  scan                            NUMERIC(8, 3),
  track                           NUMERIC(8, 3),
  satellite                       TEXT NOT NULL,
  instrument                      TEXT NOT NULL,
  confidence                      TEXT NOT NULL,
  version                         TEXT,
  bright_ti5                      NUMERIC(8, 2),
  frp                             NUMERIC(10, 2),
  daynight                        TEXT NOT NULL,
  municipality_key                TEXT,
  municipality_normalized_value   TEXT,
  municipality_match_method       TEXT,
  is_in_greece                    BOOLEAN NOT NULL DEFAULT TRUE,
  raw                             JSONB NOT NULL DEFAULT '{}'::jsonb,
  ingested_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_firms_active_fire_detection
    UNIQUE (
      source_product,
      satellite,
      instrument,
      detection_date,
      acquisition_time_utc,
      latitude,
      longitude
    ),
  CONSTRAINT fk_firms_active_fire_detections_municipality_key
    FOREIGN KEY (municipality_key)
    REFERENCES public.municipality_normalized_name(municipality_key)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
  CONSTRAINT chk_firms_active_fire_detections_latitude
    CHECK (latitude >= -90 AND latitude <= 90),
  CONSTRAINT chk_firms_active_fire_detections_longitude
    CHECK (longitude >= -180 AND longitude <= 180),
  CONSTRAINT chk_firms_active_fire_detections_acquisition_time
    CHECK (
      acquisition_time_utc >= 0
      AND acquisition_time_utc <= 2359
      AND acquisition_time_utc % 100 < 60
    ),
  CONSTRAINT chk_firms_active_fire_detections_confidence
    CHECK (
      confidence IN ('l', 'n', 'h')
      OR (
        CASE
          WHEN confidence ~ '^[0-9]+(\.[0-9]+)?$'
          THEN confidence::NUMERIC >= 0 AND confidence::NUMERIC <= 100
          ELSE FALSE
        END
      )
    ),
  CONSTRAINT chk_firms_active_fire_detections_daynight
    CHECK (daynight IN ('D', 'N')),
  CONSTRAINT chk_firms_active_fire_detections_frp
    CHECK (frp IS NULL OR frp >= 0),
  CONSTRAINT chk_firms_active_fire_detections_scan
    CHECK (scan IS NULL OR scan >= 0),
  CONSTRAINT chk_firms_active_fire_detections_track
    CHECK (track IS NULL OR track >= 0)
);

CREATE INDEX IF NOT EXISTS idx_firms_active_fire_detections_acquired_at
  ON public.firms_active_fire_detections (acquired_at DESC);

CREATE INDEX IF NOT EXISTS idx_firms_active_fire_detections_latest_pass
  ON public.firms_active_fire_detections (source_product, is_latest_pass, acquired_at DESC);

CREATE INDEX IF NOT EXISTS idx_firms_active_fire_detections_municipality_key
  ON public.firms_active_fire_detections (municipality_key);

CREATE INDEX IF NOT EXISTS idx_firms_active_fire_detections_confidence
  ON public.firms_active_fire_detections (confidence);

CREATE INDEX IF NOT EXISTS idx_firms_active_fire_detections_daynight
  ON public.firms_active_fire_detections (daynight);

CREATE INDEX IF NOT EXISTS idx_firms_active_fire_detections_source_date
  ON public.firms_active_fire_detections (source_product, detection_date DESC);

DROP TRIGGER IF EXISTS trg_firms_active_fire_detections_updated_at
  ON public.firms_active_fire_detections;

CREATE TRIGGER trg_firms_active_fire_detections_updated_at
BEFORE UPDATE ON public.firms_active_fire_detections
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.firms_active_fire_detections ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS public_read_firms_active_fire_detections
  ON public.firms_active_fire_detections;

CREATE POLICY public_read_firms_active_fire_detections
ON public.firms_active_fire_detections
FOR SELECT
TO anon, authenticated
USING (true);

COMMIT;
