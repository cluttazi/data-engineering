select
    base_currency,
    quote_currency,
    rate,
    cast(business_date as date) as business_date
from {{ source('silver', 'fx_rates') }}
qualify row_number() over (partition by quote_currency, business_date order by _ingest_ts desc) = 1
