{{
    config(
        schema = 'oncology',
        materialized = 'table'
    )
}}

/*
    Mart: cancer segments — cancer type × spend bucket cross-tab
    Grain: one row per primary_cancer_type × spend_bucket combination
    Source: dim_cancer_patients

    Answers: "Which cancer types drive the most cost, and how is spend distributed
              within each type?"

    The spend_bucket thresholds are rounded from actual population percentiles:
        p25 ~ $7,000  →  Low (bottom 25%)
        p75 ~ $25,000 →  Moderate (25th–75th percentile)
        p90 ~ $40,000 →  High (75th–90th percentile)
        >p90          →  Very High (top 10%)
*/

with population_total as (

    select count(*) as total_cancer_patients
    from {{ ref('dim_cancer_patients') }}

)

select
    d.primary_cancer_type,
    d.spend_bucket,
    count(*)                                                    as patient_count,
    sum(d.total_paid_amount)                                    as total_paid_amount,
    round(
        avg(d.total_paid_amount),
        2
    )                                                           as avg_paid_per_patient,
    round(
        100.0 * count(*) / nullif(p.total_cancer_patients, 0),
        2
    )                                                           as pct_of_cancer_population,
    round(
        100.0 * sum(d.total_paid_amount)
            / nullif(sum(sum(d.total_paid_amount)) over (), 0),
        2
    )                                                           as pct_of_total_cancer_spend

from {{ ref('dim_cancer_patients') }} d
cross join population_total p
group by d.primary_cancer_type, d.spend_bucket, p.total_cancer_patients
order by total_paid_amount desc
