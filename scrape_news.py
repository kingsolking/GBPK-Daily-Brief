import os
import psycopg2
import feedparser
from datetime import datetime, timezone

DB_URL = os.getenv("DATABASE_URL")

# we keep your good consumer feeds AND add big business feeds
FEEDS = [
    # consumer / retail / brand
    "https://www.modernretail.co/feed/",
    "https://www.retaildive.com/feeds/news/",
    "https://www.fooddive.com/feeds/news/",
    "https://techcrunch.com/feed/",
    "https://www.fastcompany.com/rss",

    # bigger business / finance
    "http://feeds.reuters.com/reuters/businessNews",
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "http://rss.cnn.com/rss/money_latest.rss",
]

# broadened keywords so big feeds actually match
KEYWORDS = [
    "consumer", "retail", "brand", "shopper",
    "food", "beverage", "cpg", "snack",
    "beauty", "skincare",
    "launch", "unveils", "introduces",
    "raised", "funding", "investment",
    "acquires", "acquisition",
    "amazon", "target", "walmart", "costco", "shein",
]


def passes_filter(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    return any(k in text for k in KEYWORDS)


def ensure_table():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    # make company_id nullable if it isn't
    try:
        cur.execute("alter table news_articles alter column company_id drop not null;")
    except Exception:
        pass
    # make url unique so we don't insert the same story twice
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
    # force today's timestamp so your email (which filters on current_date)
    # actually sees this story
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

    for feed_url in FEEDS:
        parsed = feedparser.parse(feed_url)
        for entry in parsed.entries:
            title = entry.get("title") or ""
            summary = entry.get("summary", "")
            if not title:
                continue

            if not passes_filter(title, summary):
                continue

            link = entry.get("link")
            if not link:
                continue

            # try to get a nice source name, otherwise fall back to feed url
            src = None
            if entry.get("source") and entry["source"].get("title"):
                src = entry["source"]["title"]
            else:
                src = feed_url

            insert_article(title, src, link)


if __name__ == "__main__":
    main()
