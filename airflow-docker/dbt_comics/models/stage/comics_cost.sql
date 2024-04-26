{{ config(materialized='incremental') }}

with cost as (
    select 
        "num" as comic_id,
        length(title)*5 as cost
    from {{ source('raw_comics', 'comic') }}

    {% if is_incremental() %}
    where "num" > (select max(comic_id) from {{ this }})
    {% endif %}
)

select *
from cost
