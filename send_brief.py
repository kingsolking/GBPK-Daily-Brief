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
