import os
import psycopg2
from datetime import date
from email.mime.text import MIMEText
import smtplib

# env vars from GitHub Actions
DB_URL = os.getenv("DATABASE_URL")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

# recipients
RECIPIENTS = [
    "solomon@gbpkcompany.com",
]


def get_todays_items():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        with todays_events as (
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
        ),
        todays_news as (
            select
                n.published_at::date as the_date,
                c.name as company_name,
                c.sector,
                'news' as event_type,
                n.headline as title,
                null::numeric as amount,
                null::text as currency,
                n.source::text as source,
                n.url as url,
                0 as score
            from news_articles n
            join companies c on c.id = n.company_id
            where n.published_at::date = current_date
        ),
        all_items as (
            select * from todays_events
            union all
            select * from todays_news
        ),
        ranked as (
            select
                *,
                row_number() over (
                    order by score desc, the_date desc, company_name asc
                ) as rn
            from all_items
        )
        select
            the_date, company_name, sector, event_type, title,
            amount, currency, source, url
        from ranked
        where rn <= 12;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def emoji_for_type(event_type: str) -> str:
    if event_type == "funding":
        return "💰"
    if event_type == "launch":
        return "🚀"
    if event_type == "revenue_milestone":
        return "📈"
    if event_type == "news":
        return "🗞️"
    return "✨"


def format_html(rows):
    today_str = date.today().strftime("%b %d, %Y")
    html_parts = [
        f"<h2 style='font-family:Helvetica;margin-bottom:10px;'>Daily GBPK Brief — {today_str}</h2>",
        "<div style='font-family:Helvetica;line-height:1.6;font-size:14px;'>"
    ]
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
    ) in rows:
        emoji = emoji_for_type(event_type)

        if event_type == "funding" and amount:
            amt = f"${int(amount):,}"
            text = f"{emoji} <b>{company}</b> raised {amt} {title.replace(company, '').strip()} ({sector})"
        elif event_type == "launch":
            text = f"{emoji} <b>{company}</b> launched {title} ({sector})"
        elif event_type == "revenue_milestone":
            text = f"{emoji} <b>{company}</b> reported {title} ({sector})"
        elif event_type == "news":
            text = f"{emoji} <b>{company}</b> {title} ({sector})"
        else:
            text = f"{emoji} <b>{company}</b> {title} ({sector})"

        if url:
            text += f" <a href='{url}' style='color:#0073e6;text-decoration:none;'>[{source}]</a>"
        elif source:
            text += f" [{source}]"

        html_parts.append(f"<p style='margin:6px 0;'>{text}</p>")

    html_parts.append("</div>")
    return "\n".join(html_parts)


def send_email(html_body):
    today_str = date.today().strftime("%b %d, %Y")
    msg = MIMEText(html_body, "html")
    msg["Subject"] = f"Daily GBPK Brief — {today_str}"
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(RECIPIENTS)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)


if __name__ == "__main__":
    rows = get_todays_items()
    if not rows:
        today_str = date.today().strftime("%b %d, %Y")
        html = f"<h2>Daily GBPK Brief — {today_str}</h2><p>No items today.</p>"
    else:
        html = format_html(rows)
    send_email(html)
