{{ config(materialized='incremental') }}

with views as (
    select 
        "num" as comic_id,
        (random()*10000)::int as views
    from {{ source('raw_comics', 'comic') }}

    {% if is_incremental() %}
    where "num" > (select max(comic_id) from {{ this }})
    {% endif %}
)

select *
from views
