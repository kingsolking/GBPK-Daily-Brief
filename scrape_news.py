import feedparser

# Define the sources you want to pull from
RSS_FEEDS = {
    "Reuters": "http://feeds.reuters.com/reuters/businessNews",
    "Bloomberg": "https://feeds.bloomberg.com/markets/news.rss",
    "NYTimes": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "TechCrunch": "https://techcrunch.com/feed/",
    "CNN Business": "http://rss.cnn.com/rss/money_latest.rss",
    "Food Business News": "https://www.foodbusinessnews.net/rss/topic/81-consumer-trends"
}

# Filter topics relevant to your audience
KEYWORDS = ["consumer", "retail", "brand", "food", "CPG", "snack", "startup", "AI"]

def get_news_items():
    articles = []

    for source, url in RSS_FEEDS.items():
        feed = feedparser.parse(url)
        for entry in feed.entries[:10]:
            title = entry.title
            link = entry.link
            summary = entry.get("summary", "")
            if any(word.lower() in title.lower() or word.lower() in summary.lower() for word in KEYWORDS):
                articles.append({
                    "source": source,
                    "title": title,
                    "link": link
                })
    return articles

if __name__ == "__main__":
    from pprint import pprint
    pprint(get_news_items())
