BEGIN;

CREATE TABLE IF NOT EXISTS public.news_fires (
  id                  BIGSERIAL PRIMARY KEY,
  article_title       TEXT NOT NULL,
  source              TEXT NOT NULL,
  article_url         TEXT NOT NULL,
  image_url           TEXT,
  published_at        TIMESTAMPTZ,
  scraped_at          TIMESTAMPTZ NOT NULL,
  municipality_key    TEXT,
  municipality_name   TEXT,
  area                TEXT,
  geocode_query       TEXT,
  lat                 NUMERIC(10, 6),
  lon                 NUMERIC(10, 6),
  CONSTRAINT uq_news_fires_article_url UNIQUE (article_url),
  CONSTRAINT fk_news_fires_municipality_key
    FOREIGN KEY (municipality_key)
    REFERENCES public.municipality_normalized_name(municipality_key)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
  CONSTRAINT chk_news_fires_article_url
    CHECK (BTRIM(article_url) <> ''),
  CONSTRAINT chk_news_fires_source
    CHECK (BTRIM(source) <> ''),
  CONSTRAINT chk_news_fires_lat
    CHECK (lat IS NULL OR (lat >= -90 AND lat <= 90)),
  CONSTRAINT chk_news_fires_lon
    CHECK (lon IS NULL OR (lon >= -180 AND lon <= 180))
);

ALTER TABLE public.news_fires ENABLE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS idx_news_fires_published_at
  ON public.news_fires (published_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_news_fires_scraped_at
  ON public.news_fires (scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_fires_municipality_key
  ON public.news_fires (municipality_key);

CREATE INDEX IF NOT EXISTS idx_news_fires_lat_lon
  ON public.news_fires (lat, lon);

DROP POLICY IF EXISTS public_read_news_fires ON public.news_fires;
CREATE POLICY public_read_news_fires
ON public.news_fires
FOR SELECT
TO anon, authenticated
USING (true);

GRANT SELECT ON public.news_fires TO anon, authenticated, service_role;

COMMIT;
