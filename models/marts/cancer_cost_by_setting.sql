{{
    config(
        schema = 'oncology',
        materialized = 'table'
    )
}}

/*
    Mart: cancer cost breakdown by care setting
    Grain: one row per care setting
    Source: int_cancer_claims

    Answers: "Where is the money going for cancer patients?"
    Aggregates total paid amount, patient count, and % of total spend by care setting.
*/

with totals as (

    select sum(paid_amount) as grand_total
    from {{ ref('int_cancer_claims') }}

)

select
    c.care_setting,
    c.service_category_1,
    sum(c.paid_amount)                                          as total_paid_amount,
    sum(c.allowed_amount)                                       as total_allowed_amount,
    count(distinct c.person_id)                                 as patient_count,
    count(distinct c.claim_id)                                  as claim_count,
    round(
        100.0 * sum(c.paid_amount) / nullif(t.grand_total, 0),
        2
    )                                                           as pct_of_total_spend,
    round(
        sum(c.paid_amount) / nullif(count(distinct c.person_id), 0),
        2
    )                                                           as avg_paid_per_patient

from {{ ref('int_cancer_claims') }} c
cross join totals t
group by c.care_setting, c.service_category_1, t.grand_total
order by total_paid_amount desc
