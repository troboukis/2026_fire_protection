BEGIN;

SET LOCAL lock_timeout = '10s';

-- Last successful PDF reference extraction on the source document itself.
-- NULL = no successful scan; references = [] = successful scan with no findings.
-- A later failed attempt must not overwrite this result. No history or errors.
ALTER TABLE public.procurement
  ADD COLUMN IF NOT EXISTS pdf_link_scan JSONB;
ALTER TABLE public.diavgeia
  ADD COLUMN IF NOT EXISTS pdf_link_scan JSONB;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.procurement'::regclass
      AND conname = 'procurement_pdf_link_scan_check'
  ) THEN
    ALTER TABLE public.procurement
      ADD CONSTRAINT procurement_pdf_link_scan_check CHECK (
        pdf_link_scan IS NULL OR (
          jsonb_typeof(pdf_link_scan) = 'object'
          AND pdf_link_scan ?& ARRAY[
            'fingerprint', 'extractor_version', 'checked_at', 'pdf_sha256', 'references'
          ]
          AND pdf_link_scan - ARRAY[
            'fingerprint', 'extractor_version', 'checked_at', 'pdf_sha256', 'references'
          ] = '{}'::jsonb
          AND jsonb_typeof(pdf_link_scan->'fingerprint') = 'string'
          AND pdf_link_scan->>'fingerprint' ~ '^[0-9a-f]{64}$'
          AND jsonb_typeof(pdf_link_scan->'extractor_version') = 'number'
          AND pdf_link_scan->>'extractor_version' ~ '^[1-9][0-9]*$'
          AND jsonb_typeof(pdf_link_scan->'checked_at') = 'string'
          AND length(pdf_link_scan->>'checked_at') > 0
          AND jsonb_typeof(pdf_link_scan->'pdf_sha256') = 'string'
          AND pdf_link_scan->>'pdf_sha256' ~ '^[0-9a-f]{64}$'
          AND jsonb_typeof(pdf_link_scan->'references') = 'array'
        )
      );
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.diavgeia'::regclass
      AND conname = 'diavgeia_pdf_link_scan_check'
  ) THEN
    ALTER TABLE public.diavgeia
      ADD CONSTRAINT diavgeia_pdf_link_scan_check CHECK (
        pdf_link_scan IS NULL OR (
          jsonb_typeof(pdf_link_scan) = 'object'
          AND pdf_link_scan ?& ARRAY[
            'fingerprint', 'extractor_version', 'checked_at', 'pdf_sha256', 'references'
          ]
          AND pdf_link_scan - ARRAY[
            'fingerprint', 'extractor_version', 'checked_at', 'pdf_sha256', 'references'
          ] = '{}'::jsonb
          AND jsonb_typeof(pdf_link_scan->'fingerprint') = 'string'
          AND pdf_link_scan->>'fingerprint' ~ '^[0-9a-f]{64}$'
          AND jsonb_typeof(pdf_link_scan->'extractor_version') = 'number'
          AND pdf_link_scan->>'extractor_version' ~ '^[1-9][0-9]*$'
          AND jsonb_typeof(pdf_link_scan->'checked_at') = 'string'
          AND length(pdf_link_scan->>'checked_at') > 0
          AND jsonb_typeof(pdf_link_scan->'pdf_sha256') = 'string'
          AND pdf_link_scan->>'pdf_sha256' ~ '^[0-9a-f]{64}$'
          AND jsonb_typeof(pdf_link_scan->'references') = 'array'
        )
      );
  END IF;
END;
$$;

COMMENT ON COLUMN public.procurement.pdf_link_scan IS
  'Last successful PDF link scan: fingerprint, extractor_version, checked_at, pdf_sha256, references [{reference,page,evidence}]. NULL = no successful scan; empty references = scanned without findings. Failed attempts belong in logs.';
COMMENT ON COLUMN public.diavgeia.pdf_link_scan IS
  'Last successful PDF link scan: fingerprint, extractor_version, checked_at, pdf_sha256, references [{reference,page,evidence}]. NULL = no successful scan; empty references = scanned without findings. Failed attempts belong in logs.';

NOTIFY pgrst, 'reload schema';

COMMIT;
