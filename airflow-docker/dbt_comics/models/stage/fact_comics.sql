{{ config(materialized='incremental') }}

with fact_comics as (
    select 
        "num" as comic_id,
        make_date("year", "month", "day") as upload_date,
        title,
        safe_title,
        "link",
        news,
        transcript,
        alt,
        img
    from {{ source('raw_comics', 'comic') }}

    {% if is_incremental() %}
    where "num" > (select max(comic_id) from {{ this }})
    {% endif %}
)

select *
from fact_comics
