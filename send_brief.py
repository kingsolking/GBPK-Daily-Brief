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
    # add others here if you want
]


def fetch_today_news():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        select
            n.published_at::date as the_date,
            coalesce(c.name, 'General') as company_name,
            coalesce(c.sector, 'General') as sector,
            n.headline as title,
            n.source::text,
            n.url
        from news_articles n
        left join companies c on c.id = n.company_id
        where n.published_at::date = current_date
        order by n.published_at desc
        limit 50;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def emoji_for_news():
    return "🗞️"


def build_html(news_rows):
    today_str = date.today().strftime("%b %d, %Y")

    # take top 15 only (scraper should already do this, but we double-guard)
    news_rows = news_rows[:15]

    top = news_rows[:5]
    more = news_rows[5:]

    html_parts = [
        "<!doctype html>",
        "<html>",
        "<body style='background:#f6f6f6;margin:0;padding:20px 0;font-family:Helvetica,Arial,sans-serif;'>",
        "<table role='presentation' style='max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;overflow:hidden;'>",
        "<tr><td style='padding:22px 26px 12px 26px;'>",
        f"<h2 style='margin:0 0 4px 0;'>Daily Consumer People Brief — {today_str}</h2>",
        f"<p style='margin:0;color:#888;font-size:13px;'>Curated consumer, brand & retail items</p>",
        "</td></tr>",
    ]

    # TOP STORIES
    html_parts.append("<tr><td style='padding:10px 26px 0 26px;'>")
    html_parts.append("<h3 style='margin:10px 0 8px 0;font-size:15px;'>🟣 Top Stories (5)</h3>")

    if not news_rows:
        html_parts.append("<p style='margin:0 0 14px 0;'>No items today.</p>")
    else:
        for row in top:
            _date, company, sector, title, source, url = row
            html_parts.append(
                f"""
                <div style="margin-bottom:14px;">
                    <div style="font-size:14px;line-height:1.35;"><b>{emoji_for_news()} {title}</b></div>
                    <div style="font-size:12px;color:#777;">
                        {company} ({sector}) · <a href="{url}" style="color:#0b5ed7;text-decoration:none;">{source}</a>
                    </div>
                </div>
                """
            )

    html_parts.append("</td></tr>")

    # DIVIDER
    html_parts.append(
        "<tr><td style='padding:0 26px;'><hr style='border:none;border-top:1px solid #eee;margin:12px 0 10px 0;'></td></tr>"
    )

    # MORE HEADLINES
    if more:
        html_parts.append("<tr><td style='padding:0 26px 20px 26px;'>")
        html_parts.append(f"<h3 style='margin:6px 0 6px 0;font-size:15px;'>📋 More headlines ({len(more)})</h3>")
        html_parts.append("<ul style='margin:0;padding-left:16px;'>")
        for row in more:
            _date, company, sector, title, source, url = row
            html_parts.append(
                f"""
                <li style="margin-bottom:8px;">
                    <span style="font-size:13px;"><b>{title}</b></span>
                    <span style="font-size:12px;color:#777;"> — {company} ({sector})</span>
                    <a href="{url}" style="font-size:12px;color:#0b5ed7;text-decoration:none;">[{source}]</a>
                </li>
                """
            )
        html_parts.append("</ul>")
        html_parts.append("</td></tr>")

    # FOOTER
    html_parts.append(
        "<tr><td style='padding:12px 26px 20px 26px;'><p style='margin:0;font-size:11px;color:#aaa;'>Consumer People internal daily brief.</p></td></tr>"
    )

    html_parts.append("</table></body></html>")
    return "".join(html_parts)


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
    news = fetch_today_news()
    html = build_html(news)
    send_email(html)
