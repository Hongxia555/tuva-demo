{{
    config(
        schema = 'oncology',
        materialized = 'table'
    )
}}

/*
    Mart: cancer claim fact table
    Grain: one row per claim line for cancer patients
    Source: int_cancer_claims

    This is the event-history fact table — it preserves full claim-line detail
    so downstream consumers can re-aggregate by any dimension (care setting,
    time period, cancer type, payer, etc.) without re-running the pipeline.

    This model is intentionally thin: it selects from int_cancer_claims and
    adds the spend_bucket from dim_cancer_patients for convenience.
*/

select
    c.medical_claim_id,
    c.claim_id,
    c.claim_line_number,
    c.person_id,
    c.primary_cancer_type,
    c.has_metastatic,
    c.claim_type,
    c.care_setting,
    c.service_category_1,
    c.service_category_2,
    c.claim_start_date,
    c.claim_end_date,
    c.paid_amount,
    c.allowed_amount,
    c.charge_amount,
    c.payer,
    c.plan,
    c.data_source,

    -- Enrich with patient-level attributes from the dimension
    d.age,
    d.age_group,
    d.sex,
    d.race,
    d.spend_bucket,
    d.treatment_status,
    d.cancer_type_count

from {{ ref('int_cancer_claims') }} c
left join {{ ref('dim_cancer_patients') }} d on c.person_id = d.person_id
