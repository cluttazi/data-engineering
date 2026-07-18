-- Analyst-facing customer dimension: PII-minimized by design.
-- Direct identifiers (name, email, phone, address) never leave silver;
-- analysts get segmentation attributes plus a stable surrogate for joins.
-- This mirrors the Unity Catalog grant model where the analysts group has
-- no SELECT on silver (see governance/unity_catalog).
select
    c.customer_id,
    c.segment,
    c.risk_rating,
    cast(c.created_at as date) as customer_since,
    count(distinct a.account_id) as active_accounts,
    coalesce(sum(a.balance), 0) as total_balance
from {{ ref('stg_customers') }} c
left join {{ ref('stg_accounts') }} a
    on a.customer_id = c.customer_id and a.status = 'active'
group by all
