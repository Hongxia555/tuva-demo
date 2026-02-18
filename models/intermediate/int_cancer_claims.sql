{{
    config(
        schema = 'oncology',
        materialized = 'table'
    )
}}

/*
    Intermediate model: all medical claims for cancer patients
    Source: stg_medical_claims joined to int_cancer_patients
    Grain: one row per claim line (medical_claim_id) for cancer patients only

    This model captures the TOTAL cost of care for cancer patients — not just
    claims with cancer diagnosis codes. This reflects the full economic burden
    of caring for someone with cancer, which includes treatment, comorbidities,
    and other care needs.

    The primary_cancer_type is brought in from int_cancer_patients (the most
    frequently coded cancer type per patient). For patients with multiple cancer
    types, all claims are attributed to the primary type for cost aggregation.
*/

with cancer_patients as (

    -- Deduplicate to one row per patient: pick the cancer_type with the most claims
    select
        person_id,
        cancer_type   as primary_cancer_type,
        has_metastatic
    from (
        select
            person_id,
            cancer_type,
            has_metastatic,
            row_number() over (
                partition by person_id
                order by claim_count desc, cancer_type  -- tie-break alphabetically
            ) as rn
        from {{ ref('int_cancer_patients') }}
    ) ranked
    where rn = 1

)

select
    c.medical_claim_id,
    c.claim_id,
    c.claim_line_number,
    c.person_id,
    p.primary_cancer_type,
    p.has_metastatic,
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
    c.data_source

from {{ ref('stg_medical_claims') }} c
inner join cancer_patients p on c.person_id = p.person_id
