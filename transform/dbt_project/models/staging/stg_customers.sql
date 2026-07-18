-- Current customer versions from the SCD2 history.
-- The full history stays queryable via the source; staging narrows to the
-- "as of now" view that marts consume.
select
    customer_id,
    full_name,
    email,
    phone,
    address,
    segment,
    risk_rating,
    created_at,
    updated_at,
    valid_from,
    business_key
from {{ source('silver', 'customers') }}
where is_current and not is_deleted
