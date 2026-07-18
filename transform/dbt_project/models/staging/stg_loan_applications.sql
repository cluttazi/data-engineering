-- Current state of each loan application. Erased (deleted) applications drop
-- out here; their history remains in the silver source for audit.
select
    application_id,
    customer_id,
    product,
    amount,
    term_months,
    status,
    credit_score,
    submitted_at,
    updated_at
from {{ source('silver', 'loan_applications') }}
where is_current and not is_deleted
