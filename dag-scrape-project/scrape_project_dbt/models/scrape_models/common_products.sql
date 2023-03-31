

{{ config(materialized='table') }}

with source_data as (

    select a.title as common_product_names
    from TestScrapeDataset.listings a 
    inner join 
    TestScrapeDataset.popup_links b 
    on a.title = b.title

)

select *
from source_data