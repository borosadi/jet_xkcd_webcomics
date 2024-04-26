with agg_comics as (
    select 
        fc.comic_id,
        fc.alt,
        fc.img,
        fc.link,
        fc.news,
        fc.safe_title,
        fc.title,
        fc.transcript,
        fc.upload_date,
        c.cost,
        v.views,
        r.reviews
    from {{ ref('fact_comics') }} as fc
    left join {{ ref('comics_cost') }} as c on c.comic_id = fc.comic_id
    left join {{ ref('comics_views') }} as v on v.comic_id = fc.comic_id
    left join {{ ref('comics_reviews') }} as r on r.comic_id = fc.comic_id
)

select *
from agg_comics
