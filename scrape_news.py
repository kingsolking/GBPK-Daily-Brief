import os
import psycopg2
import feedparser
from datetime import datetime, timezone

DB_URL = os.getenv("DATABASE_URL")

FEEDS = [
    # consumer first
    "https://www.modernretail.co/feed/",
    "https://www.retaildive.com/feeds/news/",
    "https://www.fooddive.com/feeds/news/",
    "https://www.fastcompany.com/rss",
    "https://techcrunch.com/feed/",
    # bigger
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://feeds.bloomberg.com/markets/news.rss",
    "http://feeds.reuters.com/reuters/businessNews",
    "http://rss.cnn.com/rss/money_latest.rss",
]

KEYWORDS = [
    "consumer", "retail", "brand", "shopper",
    "food", "beverage", "cpg", "snack",
    "beauty", "skincare",
    "launch", "unveils", "introduces",
    "raised", "funding", "investment",
    "acquires", "acquisition",
    "amazon", "target", "walmart", "costco", "shein",
]

MAX_COLLECT = 40


def passes_filter(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    return any(k in text for k in KEYWORDS)


def ensure_table():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    # make company_id nullable
    try:
        cur.execute("alter table news_articles alter column company_id drop not null;")
    except Exception:
        pass
    # add unique on url
    cur.execute("""
        do $$
        begin
            if not exists (
                select 1 from pg_constraint where conname = 'unique_news_url'
            ) then
                alter table news_articles
                add constraint unique_news_url unique (url);
            end if;
        end$$;
    """)
    # add image_url if it doesn't exist
    cur.execute("""
        do $$
        begin
            if not exists (
                select 1
                from information_schema.columns
                where table_name = 'news_articles'
                  and column_name = 'image_url'
            ) then
                alter table news_articles add column image_url text;
            end if;
        end$$;
    """)
    conn.commit()
    cur.close()
    conn.close()


def extract_image(entry):
    # 1) media_content
    media = entry.get("media_content")
    if media and isinstance(media, list) and media[0].get("url"):
        return media[0]["url"]

    # 2) media_thumbnail
    thumb = entry.get("media_thumbnail")
    if thumb and isinstance(thumb, list) and thumb[0].get("url"):
        return thumb[0]["url"]

    # 3) some feeds put it at 'image'
    if entry.get("image") and entry["image"].get("href"):
        return entry["image"]["href"]

    # 4) fallback: none
    return None


def insert_article(title, source, url, image_url):
    published_at = datetime.now(timezone.utc)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        insert into news_articles (company_id, headline, source, url, published_at, image_url)
        values (null, %s, %s, %s, %s, %s)
        on conflict (url) do update
        set image_url = excluded.image_url;
    """, (title, source, url, published_at, image_url))
    conn.commit()
    cur.close()
    conn.close()


def main():
    ensure_table()

    seen = set()
    collected = []

    for feed_url in FEEDS:
        parsed = feedparser.parse(feed_url)
        for entry in parsed.entries:
            if len(collected) >= MAX_COLLECT:
                break

            title = entry.get("title") or ""
            summary = entry.get("summary", "")
            link = entry.get("link")

            if not title or not link:
                continue
            if link in seen:
                continue
            if not passes_filter(title, summary):
                continue

            # source
            if entry.get("source") and entry["source"].get("title"):
                src = entry["source"]["title"]
            else:
                src = feed_url

            image_url = extract_image(entry)

            collected.append((title, src, link, image_url))
            seen.add(link)

        if len(collected) >= MAX_COLLECT:
            break

    for title, src, link, image_url in collected:
        insert_article(title, src, link, image_url)


if __name__ == "__main__":
    main()
