CREATE OR REPLACE FUNCTION public.sync_current_fire_coordinates_from_firms()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_updated_count integer;
BEGIN
  WITH current_fire_counts AS (
    SELECT
      municipality_key,
      COUNT(*) AS current_fire_count
    FROM public.current_fires
    WHERE is_current IS TRUE
      AND municipality_key IS NOT NULL
      AND (status IS NULL OR status <> 'ΛΗΞΗ')
    GROUP BY municipality_key
  ),
  eligible_current_fires AS (
    SELECT
      current_fires.incident_key,
      current_fires.municipality_key
    FROM public.current_fires
    INNER JOIN current_fire_counts
      ON current_fire_counts.municipality_key = current_fires.municipality_key
    WHERE current_fires.is_current IS TRUE
      AND current_fire_counts.current_fire_count = 1
      AND (current_fires.status IS NULL OR current_fires.status <> 'ΛΗΞΗ')
  ),
  ranked_firms_detections AS (
    SELECT
      firms_active_fire_detections.municipality_key,
      firms_active_fire_detections.latitude,
      firms_active_fire_detections.longitude,
      ROW_NUMBER() OVER (
        PARTITION BY firms_active_fire_detections.municipality_key
        ORDER BY
          firms_active_fire_detections.acquired_at DESC,
          firms_active_fire_detections.frp DESC NULLS LAST,
          firms_active_fire_detections.id DESC
      ) AS detection_rank
    FROM public.firms_active_fire_detections
    INNER JOIN current_fire_counts
      ON current_fire_counts.municipality_key = firms_active_fire_detections.municipality_key
    WHERE firms_active_fire_detections.is_in_greece IS TRUE
      AND firms_active_fire_detections.municipality_key IS NOT NULL
      AND firms_active_fire_detections.acquired_at >= NOW() - INTERVAL '24 hours'
      AND current_fire_counts.current_fire_count = 1
  ),
  matched_coordinates AS (
    SELECT
      eligible_current_fires.incident_key,
      ranked_firms_detections.latitude,
      ranked_firms_detections.longitude
    FROM eligible_current_fires
    INNER JOIN ranked_firms_detections
      ON ranked_firms_detections.municipality_key = eligible_current_fires.municipality_key
    WHERE ranked_firms_detections.detection_rank = 1
  )
  UPDATE public.current_fires
  SET
    lat = matched_coordinates.latitude,
    lon = matched_coordinates.longitude
  FROM matched_coordinates
  WHERE current_fires.incident_key = matched_coordinates.incident_key
    AND (
      current_fires.lat IS DISTINCT FROM matched_coordinates.latitude
      OR current_fires.lon IS DISTINCT FROM matched_coordinates.longitude
    );

  GET DIAGNOSTICS v_updated_count = ROW_COUNT;
  RETURN v_updated_count;
END;
$$;

REVOKE ALL ON FUNCTION public.sync_current_fire_coordinates_from_firms()
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.sync_current_fire_coordinates_from_firms()
TO service_role;

DO $$
BEGIN
  IF to_regclass('public.current_fires') IS NOT NULL
     AND to_regclass('public.firms_active_fire_detections') IS NOT NULL THEN
    PERFORM public.sync_current_fire_coordinates_from_firms();
  END IF;
END $$;
