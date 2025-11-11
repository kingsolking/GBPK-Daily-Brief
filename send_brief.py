import os
import psycopg2
from datetime import date
from email.mime.text import MIMEText
import smtplib

DB_URL = os.getenv("DATABASE_URL")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

RECIPIENTS = [
    "solomon@gbpkcompany.com",
]


def fetch_today():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # events (from your events table)
    cur.execute("""
        select
            e.event_date::date as the_date,
            c.name as company_name,
            c.sector,
            e.event_type,
            e.title,
            e.amount::numeric,
            e.currency::text,
            e.source::text,
            e.source_url as url,
            coalesce(e.score, 0) as score
        from events e
        join companies c on c.id = e.company_id
        where e.event_date::date = current_date
        order by score desc, e.event_date desc, c.name asc
        limit 50;
    """)
    events = cur.fetchall()

    # news (now allowing null company_id)
    cur.execute("""
        select
            n.published_at::date as the_date,
            coalesce(c.name, 'General') as company_name,
            coalesce(c.sector, 'General') as sector,
            'news' as event_type,
            n.headline as title,
            null::numeric as amount,
            null::text as currency,
            n.source::text as source,
            n.url as url,
            0 as score
        from news_articles n
        left join companies c on c.id = n.company_id
        where n.published_at::date = current_date
        order by n.published_at desc
        limit 50;
    """)
    news = cur.fetchall()

    cur.close()
    conn.close()
    return events, news


def emoji_for_type(event_type: str) -> str:
    return {
        "funding": "💰",
        "launch": "🚀",
        "revenue_milestone": "📈",
        "news": "🗞️",
    }.get(event_type, "✨")


def build_sections(events, news):
    funding = []
    launches = []
    revenue = []
    news_lines = []

    # events
    for (
        _the_date,
        company,
        sector,
        event_type,
        title,
        amount,
        currency,
        source,
        url,
        _score,
    ) in events:

        if event_type == "funding":
            if amount:
                amt = f"${int(amount):,}"
                line = f"{emoji_for_type('funding')} <b>{company}</b> raised {amt} {title.replace(company, '').strip()} ({sector})"
            else:
                line = f"{emoji_for_type('funding')} <b>{company}</b> {title} ({sector})"
            if url:
                line += f" <a href='{url}' style='color:#0b5ed7;text-decoration:none;'>[{source}]</a>"
            elif source:
                line += f" [{source}]"
            funding.append((amount or 0, line))

        elif event_type == "launch":
            line = f"{emoji_for_type('launch')} <b>{company}</b> launched {title} ({sector})"
            if url:
                line += f" <a href='{url}' style='color:#0b5ed7;text-decoration:none;'>[{source}]</a>"
            elif source:
                line += f" [{source}]"
            launches.append(line)

        elif event_type == "revenue_milestone":
            line = f"{emoji_for_type('revenue_milestone')} <b>{company}</b> reported {title} ({sector})"
            if url:
                line += f" <a href='{url}' style='color:#0b5ed7;text-decoration:none;'>[{source}]</a>"
            elif source:
                line += f" [{source}]"
            revenue.append(line)

        else:
            line = f"{emoji_for_type(event_type)} <b>{company}</b> {title} ({sector})"
            if url:
                line += f" <a href='{url}' style='color:#0b5ed7;text-decoration:none;'>[{source}]</a>"
            elif source:
                line += f" [{source}]"
            launches.append(line)

    # news
    for (
        _the_date,
        company,
        sector,
        _event_type,
        title,
        _amount,
        _currency,
        source,
        url,
        _score,
    ) in news:
        line = f"{emoji_for_type('news')} <b>{company}</b> {title} ({sector})"
        if url:
            line += f" <a href='{url}' style='color:#0b5ed7;text-decoration:none;'>[{source}]</a>"
        elif source:
            line += f" [{source}]"
        news_lines.append(line)

    return funding, launches, revenue, news_lines


def summarize_top(funding, launches, revenue, news_lines):
    funding_count = len(funding)
    launch_count = len(launches)
    revenue_count = len(revenue)
    news_count = len(news_lines)

    total_disclosed = int(sum(f[0] for f in funding if f[0]))

    bits = [f"{funding_count} funding"]
    if launch_count:
        bits.append(f"{launch_count} launches")
    if revenue_count:
        bits.append(f"{revenue_count} rev updates")
    if news_count:
        bits.append(f"{news_count} news")
    if total_disclosed:
        bits.append(f"${total_disclosed:,} disclosed")

    return " · ".join(bits)


def format_html(funding, launches, revenue, news_lines):
    today_str = date.today().strftime("%b %d, %Y")
    summary = summarize_top(funding, launches, revenue, news_lines)

    html = [
        "<!doctype html>",
        "<html><body style='font-family:Helvetica,Arial,sans-serif;background:#f6f6f6;padding:20px;'>",
        "<table role='presentation' style='max-width:640px;margin:0 auto;background:#ffffff;border-radius:8px;padding:20px 24px;'>",
        f"<tr><td><h2 style='margin:0 0 6px 0;'>Daily Consumer People Brief — {today_str}</h2>",
        f"<p style='margin:0 0 16px 0;color:#555;font-size:13px;'>{summary}</p>",
        "<hr style='border:none;border-top:1px solid #eee;margin:16px 0;'>",
    ]

    if funding:
        html.append("<h3 style='margin:0 0 8px 0;font-size:15px;'>💰 Funding</h3>")
        for _amt, line in funding:
            html.append(f"<p style='margin:5px 0;'>{line}</p>")
        html.append("<hr style='border:none;border-top:1px solid #f0f0f0;margin:14px 0;'>")

    if launches:
        html.append("<h3 style='margin:0 0 8px 0;font-size:15px;'>🚀 Launches & Product</h3>")
        for line in launches:
            html.append(f"<p style='margin:5px 0;'>{line}</p>")
        html.append("<hr style='border:none;border-top:1px solid #f0f0f0;margin:14px 0;'>")

    if revenue:
        html.append("<h3 style='margin:0 0 8px 0;font-size:15px;'>📈 Revenue & Growth</h3>")
        for line in revenue:
            html.append(f"<p style='margin:5px 0;'>{line}</p>")
        html.append("<hr style='border:none;border-top:1px solid #f0f0f0;margin:14px 0;'>")

    if news_lines:
        html.append("<h3 style='margin:0 0 8px 0;font-size:15px;'>🗞️ In the News</h3>")
        for line in news_lines:
            html.append(f"<p style='margin:5px 0;'>{line}</p>")

    if not (funding or launches or revenue or news_lines):
        html.append("<p>No items for today.</p>")

    html.append("<hr style='border:none;border-top:1px solid #eee;margin:16px 0;'>")
    html.append("<p style='font-size:12px;color:#999;margin:0;'>Consumer People internal daily brief.</p>")
    html.append("</td></tr></table></body></html>")

    return "".join(html)


def send_email(html_body):
    today_str = date.today().strftime("%b %d, %Y")
    msg = MIMEText(html_body, "html")
    msg["Subject"] = f"Daily Consumer People Brief — {today_str}"
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(RECIPIENTS)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)


if __name__ == "__main__":
    events, news = fetch_today()
    funding, launches, revenue, news_lines = build_sections(events, news)
    html = format_html(funding, launches, revenue, news_lines)
    send_email(html)
