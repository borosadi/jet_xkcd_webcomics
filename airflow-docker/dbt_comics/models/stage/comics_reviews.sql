{{ config(materialized='incremental') }}

with review as (
    select 
        "num" as comic_id,
        (random()*10)::decimal(3,1) as reviews
    from {{ source('raw_comics', 'comic') }}

    {% if is_incremental() %}
    where "num" > (select max(comic_id) from {{ this }})
    {% endif %}
)

select *
from review
