select
    account_id,
    customer_id,
    account_type,
    currency,
    balance,
    status,
    opened_at,
    updated_at
from {{ source('silver', 'accounts') }}
where is_current and not is_deleted
