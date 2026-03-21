BEGIN;

-- Promote the user-facing canonical label for municipality_key 9065
-- while preserving existing alias rows used for matching.
UPDATE public.municipality
SET
  municipality_value = 'Άργους - Ορεστικού',
  municipality_normalized_value = 'ΑΡΓΟΥΣ - ΟΡΕΣΤΙΚΟΥ',
  updated_at = NOW()
WHERE municipality_key = '9065'
  AND source_system = 'geo'
  AND municipality_value IN ('Ορεστίδος', 'ΟΡΕΣΤΙΔΟΣ');

UPDATE public.municipality_normalized_name
SET
  municipality_value = 'Άργους - Ορεστικού',
  municipality_normalized_value = 'ΑΡΓΟΥΣ - ΟΡΕΣΤΙΚΟΥ',
  updated_at = NOW()
WHERE municipality_key = '9065';

COMMIT;
