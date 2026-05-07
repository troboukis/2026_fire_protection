-- Returns log-scale histogram bins for direct award contract values.
-- Matches the same direct-award universe as get_contract_analysis.
DROP FUNCTION IF EXISTS public.get_direct_award_distribution();

CREATE OR REPLACE FUNCTION public.get_direct_award_distribution()
RETURNS TABLE(bin_lo numeric, bin_hi numeric, cnt bigint, total_count bigint)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH first_payment AS (
  SELECT DISTINCT ON (py.procurement_id)
    py.procurement_id,
    COALESCE(py.amount_without_vat, 0) AS amount_without_vat
  FROM public.payment py
  ORDER BY py.procurement_id, py.id
),
base AS (
  SELECT
    COALESCE(fp.amount_without_vat, 0) AS amount,
    CASE
      WHEN LOWER(COALESCE(p.procedure_type_value, '')) LIKE '%απευθείας ανάθεση%' THEN 'Απευθείας Ανάθεση'
      WHEN LOWER(COALESCE(p.procedure_type_value, '')) LIKE '%ανοιχτή%' THEN 'Ανοιχτή Διαδικασία'
      WHEN LOWER(COALESCE(p.procedure_type_value, '')) LIKE '%διαπραγ%' THEN 'Διαπραγμάτευση'
      ELSE 'Άλλη'
    END AS procedure,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(p.reference_number), ''),
        NULLIF(BTRIM(p.diavgeia_ada), ''),
        NULLIF(BTRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  LEFT JOIN first_payment fp ON fp.procurement_id = p.id
  WHERE p.contract_signed_date >= '2024-01-01'
    AND p.contract_signed_date <= make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, 12, 31)
    AND COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = NULLIF(BTRIM(p.reference_number), '')
    )
),
direct_awards AS (
  SELECT amount
  FROM base
  WHERE rn = 1
    AND procedure = 'Απευθείας Ανάθεση'
),
totals AS (
  SELECT COUNT(*)::bigint AS total_count
  FROM direct_awards
),
valued AS (
  SELECT amount
  FROM direct_awards
  WHERE amount IS NOT NULL
    AND amount >= 100
),
binned AS (
  SELECT
    FLOOR(LOG(amount) * 10)::int AS bin_idx,
    COUNT(*)                      AS cnt
  FROM valued
  GROUP BY FLOOR(LOG(amount) * 10)::int
),
bin_range AS (
  SELECT MIN(bin_idx) AS lo, MAX(bin_idx) AS hi FROM binned
),
all_bins AS (
  SELECT generate_series(lo, hi) AS bin_idx FROM bin_range
)
SELECT
  ROUND(POW(10, ab.bin_idx::numeric / 10), 2)       AS bin_lo,
  ROUND(POW(10, (ab.bin_idx + 1)::numeric / 10), 2) AS bin_hi,
  COALESCE(b.cnt, 0)                                 AS cnt,
  totals.total_count
FROM all_bins ab
CROSS JOIN totals
LEFT JOIN binned b ON b.bin_idx = ab.bin_idx
ORDER BY ab.bin_idx;
$$;

GRANT EXECUTE ON FUNCTION public.get_direct_award_distribution() TO anon, authenticated;
