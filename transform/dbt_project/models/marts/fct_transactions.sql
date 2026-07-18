-- Transaction fact at native grain, JPY-normalized via the FX batch feed.
select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.booked_date,
    t.booked_at,
    t.txn_type,
    t.channel,
    t.status,
    t.currency,
    t.amount,
    case
        when t.currency = 'JPY' then t.amount
        else round(t.amount / nullif(fx.rate, 0), 2)
    end as amount_jpy
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_accounts') }} a on a.account_id = t.account_id
left join {{ ref('stg_fx_rates') }} fx
    on fx.quote_currency = t.currency and fx.base_currency = 'JPY'
