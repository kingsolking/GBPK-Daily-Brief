import os
import psycopg2
import feedparser
from datetime import datetime, timezone

DB_URL = os.getenv("DATABASE_URL")

FEEDS = [
    "https://techcrunch.com/feed/",
    "https://www.modernretail.co/feed/",
    "https://www.fooddive.com/feeds/news/",
    "https://www.retaildive.com/feeds/news/",
    "https://www.fastcompany.com/rss",
    "https://www.prnewswire.com/rss/consumer-products-latest-news.rss",
]

KEYWORDS = [
    "consumer",
    "brand",
    "cpg",
    "food",
    "beverage",
    "beauty",
    "skincare",
    "launch",
    "raised",
    "funding",
    "retail",
    "ecommerce",
]


def passes_filter(title: str) -> bool:
    title_lower = title.lower()
    return any(k in title_lower for k in KEYWORDS)


def ensure_table():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    # make company_id nullable (in case it isn't)
    try:
        cur.execute("alter table news_articles alter column company_id drop not null;")
    except Exception:
        pass
    # add unique on url to avoid duplicates
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
    conn.commit()
    cur.close()
    conn.close()


def insert_article(title, source, url, published_at):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        insert into news_articles (company_id, headline, source, url, published_at)
        values (null, %s, %s, %s, %s)
        on conflict (url) do nothing;
    """, (title, source, url, published_at))
    conn.commit()
    cur.close()
    conn.close()


def main():
    ensure_table()
    for feed_url in FEEDS:
        parsed = feedparser.parse(feed_url)
        for entry in parsed.entries:
            title = entry.get("title") or ""
            if not title:
                continue
            if not passes_filter(title):
                continue

            link = entry.get("link")
            # some feeds report source differently — we'll fall back to feed url
            source = None
            if entry.get("source") and entry["source"].get("title"):
                source = entry["source"]["title"]
            else:
                source = feed_url

            published = entry.get("published_parsed")
            if published:
                published_at = datetime(*published[:6], tzinfo=timezone.utc)
            else:
                published_at = datetime.now(timezone.utc)

            insert_article(title, source, link, published_at)


if __name__ == "__main__":
    main()
