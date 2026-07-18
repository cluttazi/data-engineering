-- The batch feed can deliver the same business rows twice under different
-- file names (resends); collapse to one row per branch, keeping the latest
-- ingest.
select
    branch_id,
    branch_name,
    region,
    cast(opened_date as date) as opened_date,
    cast(business_date as date) as business_date
from {{ source('silver', 'branches') }}
qualify row_number() over (partition by branch_id order by _ingest_ts desc) = 1
