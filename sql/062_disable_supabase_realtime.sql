BEGIN;

DO $disable_supabase_realtime$
DECLARE
  published_table record;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_publication
    WHERE pubname = 'supabase_realtime'
  ) THEN
    FOR published_table IN
      SELECT schemaname, tablename
      FROM pg_publication_tables
      WHERE pubname = 'supabase_realtime'
    LOOP
      EXECUTE format(
        'ALTER PUBLICATION supabase_realtime DROP TABLE %I.%I',
        published_table.schemaname,
        published_table.tablename
      );
    END LOOP;
  END IF;
END
$disable_supabase_realtime$;

COMMIT;
