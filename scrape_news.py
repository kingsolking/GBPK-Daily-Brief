import os
import psycopg2
import feedparser
from datetime import datetime, timezone

DB_URL = os.getenv("DATABASE_URL")

FEEDS = [
    # consumer / retail / brand
    "https://www.modernretail.co/feed/",
    "https://www.retaildive.com/feeds/news/",
    "https://www.fooddive.com/feeds/news/",
    "https://techcrunch.com/feed/",
    "https://www.fastcompany.com/rss",
    # bigger business
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

# how many we want per day
MAX_STORIES = 15


def passes_filter(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    return any(k in text for k in KEYWORDS)


def ensure_table():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    try:
        cur.execute("alter table news_articles alter column company_id drop not null;")
    except Exception:
        pass
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


def insert_article(title, source, url):
    published_at = datetime.now(timezone.utc)
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

    seen_urls = set()
    collected = []

    # walk feeds in order and stop once we have MAX_STORIES
    for feed_url in FEEDS:
        parsed = feedparser.parse(feed_url)
        for entry in parsed.entries:
            if len(collected) >= MAX_STORIES:
                break

            title = entry.get("title") or ""
            summary = entry.get("summary", "")
            link = entry.get("link")

            if not title or not link:
                continue

            # de-dupe by link
            if link in seen_urls:
                continue

            if not passes_filter(title, summary):
                continue

            # get a decent source name
            if entry.get("source") and entry["source"].get("title"):
                src = entry["source"]["title"]
            else:
                src = feed_url

            collected.append((title, src, link))
            seen_urls.add(link)

        if len(collected) >= MAX_STORIES:
            break

    # write to DB
    for title, src, link in collected:
        insert_article(title, src, link)


if __name__ == "__main__":
    main()
