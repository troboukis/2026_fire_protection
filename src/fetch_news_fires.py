from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import geopandas as gpd
import psycopg2
import requests
from bs4 import BeautifulSoup
from bs4.exceptions import FeatureNotFound
from dotenv import load_dotenv
from openai import OpenAI
from psycopg2.extras import execute_values
from shapely.geometry import Point

try:
    from src.map_copernicus_to_municipalities import (
        DEFAULT_GEOJSON,
        ROOT,
        WORKING_CRS,
        load_municipalities,
        resolve_database_url,
    )
except ModuleNotFoundError:
    from map_copernicus_to_municipalities import (
        DEFAULT_GEOJSON,
        ROOT,
        WORKING_CRS,
        load_municipalities,
        resolve_database_url,
    )


load_dotenv(ROOT / ".env")
load_dotenv(ROOT.parent / ".env")

ATHENS_TZ = ZoneInfo("Europe/Athens")
DEFAULT_STATE_PATH = ROOT / "logs" / "news_fires_state.json"
GOOGLE_GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"
REQUEST_TIMEOUT = 30
MAX_LISTING_PAGES = 20
MAX_AREA_CHARS = 120
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
        "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9,el;q=0.8",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Sec-CH-UA": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Priority": "u=0, i",
}


@dataclass(frozen=True)
class SourceConfig:
    key: str
    display_name: str
    first_url: str
    generated_page_url: str | None = None
    generated_page_offset: int = 1
    pagination_referer: str = "previous"


@dataclass
class ListingArticle:
    source_key: str
    source: str
    title: str
    url: str
    image_url: str | None = None
    published_at: datetime | None = None


@dataclass
class ArticleContent:
    body: str
    image_url: str | None = None
    published_at: datetime | None = None


@dataclass
class NewsFireRow:
    article_title: str
    source: str
    article_url: str
    image_url: str | None
    published_at: datetime | None
    scraped_at: datetime
    municipality_key: str | None
    municipality_name: str | None
    area: str | None
    geocode_query: str | None
    lat: float | None
    lon: float | None


@dataclass
class ListingCollection:
    articles: list[ListingArticle]
    state_articles: list[ListingArticle]


SOURCES = (
    SourceConfig(
        key="kathimerini",
        display_name="Καθημερινή",
        first_url="https://www.kathimerini.gr/epikairothta/",
        generated_page_url="https://www.kathimerini.gr/epikairothta/page/{page}/",
        generated_page_offset=2,
    ),
    SourceConfig(
        key="news247",
        display_name="News247",
        first_url="https://www.news247.gr/roi-eidiseon/page/1/",
        generated_page_url="https://www.news247.gr/roi-eidiseon/page/{page}/",
        generated_page_offset=2,
        pagination_referer="origin",
    ),
)


def progress(message: str) -> None:
    print(f"[news_fires] {message}", flush=True)


def short_text(value: Any, max_chars: int = 120) -> str:
    text = clean_text(value)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def make_soup(html: str) -> BeautifulSoup:
    for parser in ("lxml", "html.parser"):
        try:
            return BeautifulSoup(html, parser)
        except FeatureNotFound:
            continue
    return BeautifulSoup(html, "html.parser")


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_greek(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value or "")
    without_marks = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    upper = without_marks.upper().replace("Σ", "Σ")
    upper = upper.replace("Ϊ", "Ι").replace("Ϋ", "Υ")
    return re.sub(r"\s+", " ", upper).strip()


def contains_primary_title_term(title: str) -> bool:
    text = normalize_greek(title)
    return any(term in text for term in ("ΠΥΡΚΑΓΙ", "ΦΩΤΙ", "ΕΜΠΡΗΣΜ"))


def contains_secondary_body_term(body: str) -> bool:
    text = normalize_greek(body)
    compact = re.sub(r"\s+", " ", text)
    return any(
        term in compact
        for term in (
            "ΔΑΣΙΚ",
            "ΑΓΡΟΤΟΔΑΣΙΚ",
            "ΔΑΣΟΣ",
            "ΔΑΣΟΥΣ",
            "ΡΙΨΕΙΣ ΝΕΡΟΥ",
        )
    )


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"sources": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"sources": {}}
    if not isinstance(data, dict):
        return {"sources": {}}
    sources = data.get("sources")
    if not isinstance(sources, dict):
        sources = {}
    return {"sources": sources}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def article_identity(article: ListingArticle) -> dict[str, str]:
    return {
        "url": article.url,
        "title": article.title,
        "normalized_title": normalize_greek(article.title),
    }


def is_boundary(article: ListingArticle, boundary: dict[str, Any] | None) -> bool:
    if not boundary:
        return False
    boundary_url = clean_text(boundary.get("url"))
    if boundary_url and article.url == boundary_url:
        return True
    boundary_title = clean_text(boundary.get("normalized_title")) or normalize_greek(clean_text(boundary.get("title")))
    return bool(boundary_title and normalize_greek(article.title) == boundary_title)


def state_seen_urls(boundary: dict[str, Any] | None) -> set[str]:
    if not boundary:
        return set()
    raw_urls = boundary.get("seen_urls")
    if not isinstance(raw_urls, list):
        return set()
    return {clean_text(url) for url in raw_urls if clean_text(url)}


def scan_boundary(boundary: dict[str, Any] | None) -> dict[str, Any] | None:
    if not boundary:
        return None
    raw_urls = boundary.get("seen_urls")
    if isinstance(raw_urls, list):
        first_seen_url = next((clean_text(url) for url in raw_urls if clean_text(url)), "")
        if first_seen_url:
            return {
                **boundary,
                "url": first_seen_url,
            }
    return boundary


def append_unique_article(articles: list[ListingArticle], seen_urls: set[str], article: ListingArticle) -> bool:
    if article.url in seen_urls:
        return False
    articles.append(article)
    seen_urls.add(article.url)
    return True


def origin_referer(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}/"


def fetch_html(session: requests.Session, url: str, referer: str | None = None) -> tuple[str, str]:
    headers = {}
    effective_referer = referer or origin_referer(url)
    if effective_referer:
        headers["Referer"] = effective_referer
    response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    response.encoding = response.encoding or response.apparent_encoding or "utf-8"
    return response.text, response.url


def parse_datetime_value(value: str | None, *, base_date: datetime | None = None) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        return dt if dt.tzinfo else dt.replace(tzinfo=ATHENS_TZ)
    except ValueError:
        pass

    patterns = (
        r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})\s*,?\s*(\d{1,2}):(\d{2})",
        r"(\d{1,2}):(\d{2})",
    )
    first = re.search(patterns[0], text)
    if first:
        day, month, year, hour, minute = map(int, first.groups())
        return datetime(year, month, day, hour, minute, tzinfo=ATHENS_TZ)
    second = re.search(patterns[1], text)
    if second and base_date:
        hour, minute = map(int, second.groups())
        return base_date.astimezone(ATHENS_TZ).replace(hour=hour, minute=minute, second=0, microsecond=0)
    return None


def parse_srcset(value: str | None) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    first = text.split(",", 1)[0].strip().split(" ", 1)[0].strip()
    return first or None


def extract_image_url(container: Any, base_url: str) -> str | None:
    if not container:
        return None
    img = container.find("img") if hasattr(container, "find") else None
    if not img:
        return None
    raw = (
        img.get("src")
        or img.get("data-src")
        or img.get("data-lazy-src")
        or parse_srcset(img.get("srcset"))
        or parse_srcset(img.get("data-srcset"))
    )
    return urljoin(base_url, raw) if raw else None


def extract_meta_image(soup: BeautifulSoup, base_url: str) -> str | None:
    for selector in (
        {"property": "og:image"},
        {"name": "twitter:image"},
        {"property": "og:image:url"},
    ):
        tag = soup.find("meta", attrs=selector)
        raw = tag.get("content") if tag else None
        if raw:
            return urljoin(base_url, raw)
    return None


def extract_meta_datetime(soup: BeautifulSoup) -> datetime | None:
    for selector in (
        {"property": "article:published_time"},
        {"name": "article:published_time"},
        {"itemprop": "datePublished"},
        {"name": "date"},
    ):
        tag = soup.find("meta", attrs=selector)
        parsed = parse_datetime_value(tag.get("content") if tag else None)
        if parsed:
            return parsed
    time_tag = soup.find("time")
    if time_tag:
        return parse_datetime_value(time_tag.get("datetime") or time_tag.get_text(" ", strip=True))
    return None


def article_candidate_blocks(soup: BeautifulSoup) -> list[Any]:
    blocks = []
    seen_ids = set()
    for selector in ("article", "div.nx-article"):
        for block in soup.select(selector):
            marker = id(block)
            if marker not in seen_ids:
                blocks.append(block)
                seen_ids.add(marker)
    if blocks:
        return blocks
    headings = soup.find_all(["h2", "h3"], limit=80)
    return [heading.parent for heading in headings if heading and heading.find("a", href=True)]


def extract_listing_link(block: Any):
    for selector in ("a.mainlink[href]", "a[href]"):
        for candidate in block.select(selector):
            if clean_text(candidate.get_text(" ", strip=True)):
                return candidate
    for heading in block.find_all(["h1", "h2", "h3", "h4"]):
        candidate = heading.find("a", href=True)
        if candidate and clean_text(candidate.get_text(" ", strip=True)):
            return candidate
    return None


def extract_listing_title(link: Any) -> str:
    title_node = link.select_one(".card-title")
    if title_node:
        return clean_text(title_node.get_text(" ", strip=True))
    return clean_text(link.get_text(" ", strip=True))


def parse_listing_articles(
    source: SourceConfig,
    html: str,
    base_url: str,
) -> list[ListingArticle]:
    soup = make_soup(html)
    articles: list[ListingArticle] = []
    seen_urls: set[str] = set()
    now = datetime.now(ATHENS_TZ)

    for block in article_candidate_blocks(soup):
        link = extract_listing_link(block)
        if link is None:
            continue

        title = extract_listing_title(link)
        url = urljoin(base_url, link.get("href"))
        if not title or not url.startswith("http") or url in seen_urls:
            continue
        if urlparse(url).netloc and urlparse(source.first_url).netloc not in urlparse(url).netloc:
            continue

        published_at = None
        time_tag = block.find("time")
        if time_tag:
            published_at = parse_datetime_value(time_tag.get("datetime") or time_tag.get_text(" ", strip=True), base_date=now)
        if published_at is None:
            published_at = parse_datetime_value(block.get_text(" ", strip=True), base_date=now)

        articles.append(
            ListingArticle(
                source_key=source.key,
                source=source.display_name,
                title=title,
                url=url,
                image_url=extract_image_url(block, base_url),
                published_at=published_at,
            )
        )
        seen_urls.add(url)

    return articles


def find_next_listing_url(source: SourceConfig, soup: BeautifulSoup, current_url: str, page_index: int) -> str | None:
    _ = soup, current_url
    if source.generated_page_url:
        return source.generated_page_url.format(page=page_index + source.generated_page_offset)
    return None


def collect_new_listing_articles(
    source: SourceConfig,
    state: dict[str, Any],
    *,
    max_pages: int,
) -> ListingCollection:
    session = requests.Session()
    session.headers.update(HEADERS)
    boundary = state.get("sources", {}).get(source.key)
    boundary_for_scan = scan_boundary(boundary)
    seen_urls = state_seen_urls(boundary)
    new_articles: list[ListingArticle] = []
    state_articles: list[ListingArticle] = []
    url: str | None = source.first_url
    referer: str | None = None

    for page_index in range(max_pages):
        if not url:
            break
        progress(
            "fetch_listing "
            f"source={source.key} page_index={page_index} url={url} referer={referer or origin_referer(url)}"
        )
        html, final_url = fetch_html(session, url, referer=referer)
        soup = make_soup(html)
        page_articles = parse_listing_articles(source, html, final_url)
        progress(f"parsed_listing source={source.key} page_index={page_index} articles={len(page_articles)} final_url={final_url}")
        if page_index == 0:
            state_articles = page_articles

        found_boundary = False
        for article in page_articles:
            if is_boundary(article, boundary_for_scan):
                found_boundary = True
                progress(f"boundary_reached source={source.key} title={short_text(article.title)}")
                break
            if not append_unique_article(new_articles, seen_urls, article):
                progress(f"duplicate_listing_skip source={source.key} url={article.url}")

        if found_boundary:
            break
        referer = origin_referer(source.first_url) if source.pagination_referer == "origin" else final_url
        url = find_next_listing_url(source, soup, final_url, page_index)
        if url:
            progress(f"next_listing source={source.key} next_url={url}")

    return ListingCollection(articles=new_articles, state_articles=state_articles)


def extract_json_ld_article_body(soup: BeautifulSoup) -> str:
    bodies: list[str] = []
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            if isinstance(graph, list):
                items.extend(x for x in graph if isinstance(x, dict))
            article_body = item.get("articleBody") or item.get("description")
            if article_body:
                bodies.append(clean_text(article_body))
    return "\n".join(bodies)


def scrape_article_content(session: requests.Session, article: ListingArticle) -> ArticleContent:
    html, final_url = fetch_html(session, article.url, referer=origin_referer(article.url))
    soup = make_soup(html)

    for tag in soup.find_all(["script", "style", "noscript", "nav", "footer", "header", "aside"]):
        tag.decompose()

    body_container = soup.find("article") or soup.find("main") or soup.body or soup
    paragraphs = [
        clean_text(p.get_text(" ", strip=True))
        for p in body_container.find_all(["p", "li"])
        if len(clean_text(p.get_text(" ", strip=True))) > 30
    ]
    body = "\n".join(dict.fromkeys(paragraphs))
    if not body:
        body = extract_json_ld_article_body(soup)

    return ArticleContent(
        body=body,
        image_url=extract_meta_image(soup, final_url) or article.image_url,
        published_at=extract_meta_datetime(soup) or article.published_at,
    )


def resolve_openai_api_key() -> str:
    key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not key:
        raise ValueError("Missing OPENAI_API_KEY")
    return key


def resolve_google_api_key() -> str:
    key = (os.getenv("GOOGLE_GEOCODING_API_KEY") or "").strip()
    if not key:
        raise ValueError("Missing GOOGLE_GEOCODING_API_KEY")
    return key


def extract_fire_area(client: OpenAI, article: ListingArticle, body: str) -> str | None:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "area": {
                "type": ["string", "null"],
                "description": "One detailed Greek geographic area string for geocoding, or null if none exists.",
                "maxLength": MAX_AREA_CHARS,
            }
        },
        "required": ["area"],
    }
    prompt = f"""
Διάβασε το παρακάτω δημοσιογραφικό άρθρο για πιθανή πυρκαγιά.

Στόχος:
Εξήγαγε ΜΟΝΟ έναν γεωγραφικό προσδιορισμό της περιοχής της πυρκαγιάς, όσο πιο αναλυτικό γίνεται.

Κανόνες:
1. Μην κάνεις geocoding.
2. Μην επινοείς συντεταγμένες.
3. Μην επιστρέφεις δήλωση, περιγραφή συμβάντος ή πλήρη πρόταση.
4. Επέστρεψε null αν δεν υπάρχει σαφής γεωγραφική περιοχή.
5. Αν υπάρχουν πολλές περιοχές που αφορούν την ίδια πυρκαγιά, ένωσέ τες σε μία εμπλουτισμένη περιοχή.
6. Αν υπάρχουν πολλές περιοχές που αφορούν διαφορετικές πυρκαγιές, επέστρεψε την πρώτη περιοχή που εμφανίζεται στο άρθρο.
7. Επέστρεψε ΜΟΝΟ ένα σύντομο γεωγραφικό string κατάλληλο για Google Geocoding, π.χ. "Ηλεία, Βάρδα".
8. Μην επιστρέφεις λίστα, παρενθέσεις, άνω τελεία, bullets ή επεξηγήσεις.
9. Μέγιστο μήκος: {MAX_AREA_CHARS} χαρακτήρες.

Τίτλος:
{article.title}

Κείμενο:
{body[:20000]}
""".strip()
    response = client.responses.create(
        model="gpt-5-mini",
        input=prompt,
        text={
            "format": {
                "type": "json_schema",
                "name": "news_fire_area",
                "schema": schema,
                "strict": True,
            }
        },
    )
    parsed = json.loads(response.output_text)
    return normalize_extracted_area(parsed.get("area"))


def normalize_extracted_area(value: Any) -> str | None:
    area = clean_text(value)
    if not area:
        return None
    area = re.split(r"[;\n•]", area, maxsplit=1)[0].strip()
    area = re.sub(r"\s*\([^)]*\)", "", area).strip()
    area = clean_text(area)
    if not area:
        return None
    if len(area) > MAX_AREA_CHARS:
        area = area[:MAX_AREA_CHARS].rsplit(",", 1)[0].strip() or area[:MAX_AREA_CHARS].strip()
    return area or None


def geocode_area(google_api_key: str, geocode_query: str) -> tuple[float | None, float | None]:
    response = requests.get(
        GOOGLE_GEOCODING_URL,
        params={"address": geocode_query, "key": google_api_key, "region": "gr"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "OK" or not payload.get("results"):
        return None, None
    location = payload["results"][0]["geometry"]["location"]
    return float(location["lat"]), float(location["lng"])


class MunicipalityMatcher:
    def __init__(self, geojson_path: Path):
        municipalities = load_municipalities(geojson_path)
        self.municipalities = municipalities.to_crs(WORKING_CRS)
        self.sindex = self.municipalities.sindex

    def match(self, lat: float, lon: float) -> tuple[str, str] | None:
        point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(WORKING_CRS).iloc[0]
        candidate_idx = list(self.sindex.intersection(point.bounds))
        if not candidate_idx:
            return None
        candidates = self.municipalities.iloc[candidate_idx]
        containing = candidates[candidates.contains(point)]
        if containing.empty:
            containing = candidates[candidates.intersects(point)]
        if containing.empty:
            return None
        row = containing.iloc[0]
        return str(row["municipality_code"]).strip(), str(row["name"]).strip()


def build_news_fire_row(
    article: ListingArticle,
    content: ArticleContent,
    *,
    scraped_at: datetime,
    openai_client: OpenAI,
    google_api_key: str,
    matcher: MunicipalityMatcher,
) -> NewsFireRow:
    progress(f"llm_extract_start source={article.source_key} title={short_text(article.title)}")
    area = extract_fire_area(openai_client, article, content.body)
    progress(f"llm_extract_done source={article.source_key} area={area or ''}")
    geocode_query = f"{area}, Ελλάδα" if area else None
    lat = None
    lon = None
    municipality_key = None
    municipality_name = None

    if geocode_query:
        progress(f"geocode_start source={article.source_key} query={geocode_query}")
        lat, lon = geocode_area(google_api_key, geocode_query)
        progress(f"geocode_done source={article.source_key} lat={lat} lon={lon}")
        if lat is not None and lon is not None:
            progress(f"municipality_match_start source={article.source_key} lat={lat} lon={lon}")
            municipality = matcher.match(lat, lon)
            if municipality:
                municipality_key, municipality_name = municipality
                progress(
                    "municipality_match_done "
                    f"source={article.source_key} municipality_key={municipality_key} municipality_name={municipality_name}"
                )
            else:
                progress(f"municipality_match_miss source={article.source_key} lat={lat} lon={lon}")
                lat = None
                lon = None

    return NewsFireRow(
        article_title=article.title,
        source=article.source,
        article_url=article.url,
        image_url=content.image_url or article.image_url,
        published_at=content.published_at or article.published_at,
        scraped_at=scraped_at,
        municipality_key=municipality_key,
        municipality_name=municipality_name,
        area=area,
        geocode_query=geocode_query,
        lat=lat,
        lon=lon,
    )


def upsert_news_fires(db_url: str, rows: list[NewsFireRow]) -> int:
    if not rows:
        return 0
    rows = dedupe_news_fire_rows(rows)
    values = [
        (
            row.article_title,
            row.source,
            row.article_url,
            row.image_url,
            row.published_at,
            row.scraped_at,
            row.municipality_key,
            row.municipality_name,
            row.area,
            row.geocode_query,
            row.lat,
            row.lon,
        )
        for row in rows
    ]
    sql = """
        INSERT INTO public.news_fires (
          article_title,
          source,
          article_url,
          image_url,
          published_at,
          scraped_at,
          municipality_key,
          municipality_name,
          area,
          geocode_query,
          lat,
          lon
        )
        VALUES %s
        ON CONFLICT (article_url) DO UPDATE SET
          article_title = EXCLUDED.article_title,
          source = EXCLUDED.source,
          image_url = EXCLUDED.image_url,
          published_at = EXCLUDED.published_at,
          scraped_at = EXCLUDED.scraped_at,
          municipality_key = EXCLUDED.municipality_key,
          municipality_name = EXCLUDED.municipality_name,
          area = EXCLUDED.area,
          geocode_query = EXCLUDED.geocode_query,
          lat = EXCLUDED.lat,
          lon = EXCLUDED.lon
    """
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    execute_values(cur, sql, values)
    conn.commit()
    cur.close()
    conn.close()
    return len(rows)


def dedupe_news_fire_rows(rows: list[NewsFireRow]) -> list[NewsFireRow]:
    deduped: dict[str, NewsFireRow] = {}
    for row in rows:
        deduped[row.article_url] = row
    duplicate_count = len(rows) - len(deduped)
    if duplicate_count:
        progress(f"dedupe_rows skipped_duplicates={duplicate_count} unique_rows={len(deduped)}")
    return list(deduped.values())


def update_source_boundary(
    state: dict[str, Any],
    source_key: str,
    articles: list[ListingArticle],
    *,
    boundary_position: str,
) -> None:
    if not articles:
        return
    if boundary_position == "first":
        article = articles[0]
    elif boundary_position == "last":
        article = articles[-1]
    else:
        raise ValueError(f"Unsupported boundary_position: {boundary_position}")
    sources = state.setdefault("sources", {})
    sources[source_key] = {
        **article_identity(article),
        "seen_urls": [item.url for item in articles],
        "seen_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def run() -> int:
    state = load_state(DEFAULT_STATE_PATH)
    scraped_at = datetime.now(timezone.utc)
    progress(f"start state_file={DEFAULT_STATE_PATH} max_pages={MAX_LISTING_PAGES}")
    progress("resolve_database_url start")
    db_url = resolve_database_url(None)
    progress("resolve_database_url done")
    openai_client: OpenAI | None = None
    google_api_key: str | None = None
    matcher: MunicipalityMatcher | None = None
    completed_articles_by_source: dict[str, list[ListingArticle]] = {}
    successful_articles_by_source: dict[str, list[ListingArticle]] = {}
    source_errors: list[dict[str, str]] = []
    inserted = 0

    try:
        for source in SOURCES:
            progress(f"source_start source={source.key} first_url={source.first_url}")
            try:
                listing = collect_new_listing_articles(
                    source,
                    state,
                    max_pages=MAX_LISTING_PAGES,
                )
            except requests.RequestException as exc:
                source_errors.append({"source": source.key, "error": str(exc)})
                progress(f"source_listing_error source={source.key} error={exc}")
                continue
            new_articles = listing.articles
            progress(f"source_listing_done source={source.key} new_listing_articles={len(new_articles)}")
            if listing.state_articles:
                successful_articles_by_source[source.key] = listing.state_articles

            possible_articles = [article for article in new_articles if contains_primary_title_term(article.title)]
            article_session = requests.Session()
            article_session.headers.update(HEADERS)
            progress(
                "title_filter_done "
                f"source={source.key} checked={len(new_articles)} possible_articles={len(possible_articles)}"
            )

            for index, article in enumerate(possible_articles, start=1):
                progress(
                    "fetch_article_start "
                    f"source={source.key} index={index}/{len(possible_articles)} title={short_text(article.title)} url={article.url}"
                )
                try:
                    content = scrape_article_content(article_session, article)
                except requests.RequestException as exc:
                    progress(f"article_fetch_error source={source.key} url={article.url} error={exc}")
                    continue
                progress(f"fetch_article_done source={source.key} body_chars={len(content.body)}")
                if not contains_secondary_body_term(content.body):
                    progress(f"body_filter_skip source={source.key} title={short_text(article.title)}")
                    continue
                progress(f"body_filter_keep source={source.key} title={short_text(article.title)}")
                if openai_client is None:
                    progress("init_openai_client start")
                    openai_client = OpenAI(api_key=resolve_openai_api_key())
                    progress("init_openai_client done")
                if google_api_key is None:
                    progress("resolve_google_api_key start")
                    google_api_key = resolve_google_api_key()
                    progress("resolve_google_api_key done")
                if matcher is None:
                    progress(f"load_municipality_polygons start geojson={DEFAULT_GEOJSON}")
                    matcher = MunicipalityMatcher(DEFAULT_GEOJSON)
                    progress("load_municipality_polygons done")
                row = build_news_fire_row(
                    article,
                    content,
                    scraped_at=scraped_at,
                    openai_client=openai_client,
                    google_api_key=google_api_key,
                    matcher=matcher,
                )
                progress(f"db_upsert_article_start source={source.key} url={article.url}")
                inserted += upsert_news_fires(db_url, [row])
                progress(f"db_upsert_article_done source={source.key} total_rows={inserted}")
                completed_articles_by_source.setdefault(source.key, []).append(article)

        for source_key, articles in successful_articles_by_source.items():
            update_source_boundary(state, source_key, articles, boundary_position="first")
        progress(f"state_save_start path={DEFAULT_STATE_PATH}")
        save_state(DEFAULT_STATE_PATH, state)
        progress("state_save_done")

        print(
            json.dumps(
                {
                    "status": "partial_success" if source_errors else "success",
                    "rows": inserted,
                    "state_file": str(DEFAULT_STATE_PATH),
                    "source_errors": source_errors,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 0
    except KeyboardInterrupt:
        progress("interrupted no_db_write no_state_update")
        return 130
    except Exception as exc:
        progress(f"partial_error error={exc}")
        if completed_articles_by_source:
            for source_key, articles in completed_articles_by_source.items():
                update_source_boundary(state, source_key, articles, boundary_position="last")
            progress(f"partial_state_save_start path={DEFAULT_STATE_PATH}")
            save_state(DEFAULT_STATE_PATH, state)
            progress("partial_state_save_done")
            print(
                json.dumps(
                    {
                        "status": "partial_error",
                        "rows": inserted,
                        "state_file": str(DEFAULT_STATE_PATH),
                        "error": str(exc),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            return 1
        raise


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
