

{{ config(materialized='table') }}

--DBT model which extracts the common product names
with source_data as (

    select a.title as common_product_names
    from TestScrapeDataset.listings a 
    inner join 
    TestScrapeDataset.popup_links b 
    on a.title = b.title

)

select *
from source_data
