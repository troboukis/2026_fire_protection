DROP FUNCTION IF EXISTS public.get_news_ticker_articles();

CREATE OR REPLACE FUNCTION public.get_news_ticker_articles()
RETURNS TABLE (
  id bigint,
  article_title text,
  source text,
  article_url text,
  image_url text,
  published_at timestamptz,
  municipality_name text,
  area text,
  active_fire_incident_key text
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '10s'
AS $$
WITH today_rows AS (
  SELECT
    nf.id,
    nf.article_title,
    nf.source,
    nf.article_url,
    nf.image_url,
    nf.published_at,
    nf.scraped_at,
    nf.municipality_key,
    nf.municipality_name,
    nf.area
  FROM public.news_fires nf
  WHERE nf.published_at IS NOT NULL
    AND (nf.published_at AT TIME ZONE 'Europe/Athens')::date = (NOW() AT TIME ZONE 'Europe/Athens')::date
),
today_count AS (
  SELECT COUNT(*) AS article_count
  FROM today_rows
),
latest_rows AS (
  SELECT
    nf.id,
    nf.article_title,
    nf.source,
    nf.article_url,
    nf.image_url,
    nf.published_at,
    nf.scraped_at,
    nf.municipality_key,
    nf.municipality_name,
    nf.area
  FROM public.news_fires nf
  ORDER BY nf.published_at DESC NULLS LAST, nf.scraped_at DESC, nf.id DESC
  LIMIT 10
),
selected_rows AS (
  SELECT tr.*
  FROM today_rows tr
  WHERE (SELECT article_count FROM today_count) >= 5

  UNION ALL

  SELECT lr.*
  FROM latest_rows lr
  WHERE (SELECT article_count FROM today_count) < 5
),
active_fire_by_municipality AS (
  SELECT DISTINCT ON (cf.municipality_key)
    cf.municipality_key,
    cf.incident_key
  FROM public.current_fires cf
  WHERE cf.is_current IS TRUE
    AND cf.municipality_key IS NOT NULL
    AND (cf.status IS NULL OR cf.status <> 'ΛΗΞΗ')
  ORDER BY
    cf.municipality_key,
    cf.status_updated_at DESC NULLS LAST,
    cf.last_seen_at DESC NULLS LAST,
    cf.incident_key
)
SELECT
  sr.id,
  sr.article_title,
  sr.source,
  sr.article_url,
  sr.image_url,
  sr.published_at,
  sr.municipality_name,
  sr.area,
  af.incident_key AS active_fire_incident_key
FROM selected_rows sr
LEFT JOIN active_fire_by_municipality af
  ON af.municipality_key = sr.municipality_key
ORDER BY sr.published_at DESC NULLS LAST, sr.scraped_at DESC, sr.id DESC;
$$;

GRANT EXECUTE ON FUNCTION public.get_news_ticker_articles() TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
