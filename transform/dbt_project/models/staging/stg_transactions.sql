select
    transaction_id,
    account_id,
    amount,
    currency,
    txn_type,
    counterparty,
    channel,
    status,
    booked_at,
    cast(booked_at as date) as booked_date
from {{ source('silver', 'transactions') }}
