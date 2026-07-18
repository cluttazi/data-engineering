-- Loan application funnel by product: where applications sit and how they
-- convert. Note: erased applications (data-minimization deletes) are absent
-- by design; the funnel reflects live state, the silver history retains audit.
with apps as (
    select
        product,
        status,
        amount,
        credit_score
    from {{ ref('stg_loan_applications') }}
)

select
    product,
    count(*) as applications,
    count(*) filter (status = 'submitted') as submitted,
    count(*) filter (status = 'under_review') as under_review,
    count(*) filter (status = 'approved') as approved,
    count(*) filter (status = 'rejected') as rejected,
    round(
        count(*) filter (status = 'approved')
        / nullif(count(*) filter (status in ('approved', 'rejected')), 0),
        3
    ) as approval_rate,
    round(avg(amount), 2) as avg_amount,
    round(avg(credit_score), 0) as avg_credit_score
from apps
group by product
order by applications desc
