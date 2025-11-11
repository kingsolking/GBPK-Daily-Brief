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
    return {
        "funding": "💰",
        "launch": "🚀",
        "revenue_milestone": "📈",
        "news": "🗞️",
    }.get(event_type, "✨")


def format_html(rows):
    today_str = date.today().strftime("%b %d, %Y")
    html_parts = [
        "<!doctype html>",
        "<html><body style='font-family:Helvetica,Arial,sans-serif;background:#f6f6f6;padding:20px;'>",
        "<table role='presentation' style='max-width:640px;margin:0 auto;background:#ffffff;border-radius:8px;padding:20px 24px;'>",
        f"<tr><td><h2 style='margin:0 0 12px 0;'>Daily Consumer People Brief — {today_str}</h2>",
        "<p style='margin:0 0 16px 0;color:#666;'>Top activity from companies you care about.</p>",
        "<hr style='border:none;border-top:1px solid #eee;margin:16px 0;'>",
    ]

    if not rows:
        html_parts.append("<p>No items for today.</p>")
    else:
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
                line = f"{emoji} <b>{company}</b> raised {amt} {title.replace(company, '').strip()} ({sector})"
            elif event_type == "launch":
                line = f"{emoji} <b>{company}</b> launched {title} ({sector})"
            elif event_type == "revenue_milestone":
                line = f"{emoji} <b>{company}</b> reported {title} ({sector})"
            elif event_type == "news":
                line = f"{emoji} <b>{company}</b> {title} ({sector})"
            else:
                line = f"{emoji} <b>{company}</b> {title} ({sector})"

            if url:
                line += f" <a href='{url}' style='color:#0b5ed7;text-decoration:none;'>[{source}]</a>"
            elif source:
                line += f" [{source}]"

            html_parts.append(
                f"<p style='margin:6px 0 8px 0; line-height:1.5;'>{line}</p>"
            )

    html_parts.append("<hr style='border:none;border-top:1px solid #eee;margin:16px 0;'>")
    html_parts.append("<p style='font-size:12px;color:#999;margin:0;'>Consumer People internal daily brief.</p>")
    html_parts.append("</td></tr></table></body></html>")

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
    rows = get_todays_items()
    html = format_html(rows)
    send_email(html)
