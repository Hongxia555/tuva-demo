{{
    config(
        schema = 'oncology',
        materialized = 'table'
    )
}}

/*
    Mart: cancer patient dimension
    Grain: one row per cancer patient
    Source: int_cancer_patients + core__patient + int_cancer_claims (cost roll-up)

    This is the central patient-level table for oncology analysis. It combines:
        - Demographics from core__patient (age, sex, race)
        - Cancer classification from int_cancer_patients (type, flags, metastatic status)
        - Cost summary from int_cancer_claims (total spend, claim count)

    Key design decisions:
        - primary_cancer_type: the cancer type with the highest claim_count
        - Boolean flags for all cancer types: enables multi-cancer patient analysis
        - spend_bucket: data-driven thresholds from actual population distribution
              p25 = $7,119  |  p75 = $25,535  |  p90 = $40,725
        - treatment_status: "Active" if patient had any cancer claim in the final
              6 months of the dataset period (approximating ongoing treatment)
*/

with patient_demographics as (

    select
        person_id,
        sex,
        race,
        birth_date,
        death_flag,
        age,
        age_group
    from {{ ref('core__patient') }}

),

-- Determine primary cancer type per patient (most claims, tie-break alphabetically)
primary_type as (

    select person_id, cancer_type as primary_cancer_type
    from (
        select
            person_id,
            cancer_type,
            row_number() over (
                partition by person_id
                order by claim_count desc, cancer_type
            ) as rn
        from {{ ref('int_cancer_patients') }}
    ) ranked
    where rn = 1

),

-- Pivot cancer types into boolean flags (one row per patient)
cancer_pivot as (

    select
        person_id,

        -- Cancer type boolean flags (supports multi-cancer patient analysis)
        max(case when cancer_type = 'Breast Cancer'      then 1 else 0 end) as has_breast_cancer,
        max(case when cancer_type = 'Lung Cancer'        then 1 else 0 end) as has_lung_cancer,
        max(case when cancer_type = 'Colorectal Cancer'  then 1 else 0 end) as has_colorectal_cancer,
        max(case when cancer_type = 'Prostate Cancer'    then 1 else 0 end) as has_prostate_cancer,
        max(case when cancer_type = 'Lymphoma'           then 1 else 0 end) as has_lymphoma,
        max(case when cancer_type = 'Leukemia'           then 1 else 0 end) as has_leukemia,
        max(case when cancer_type = 'Thyroid Cancer'     then 1 else 0 end) as has_thyroid_cancer,
        max(case when cancer_type = 'Pancreatic Cancer'  then 1 else 0 end) as has_pancreatic_cancer,
        max(case when cancer_type = 'Urologic Cancer'    then 1 else 0 end) as has_urologic_cancer,
        max(case when cancer_type = 'Gynecologic Cancer' then 1 else 0 end) as has_gynecologic_cancer,
        max(case when cancer_type = 'Other Cancer'       then 1 else 0 end) as has_other_cancer,

        -- Metastatic flag: 1 = patient has any C77-C79 secondary/metastatic code
        max(has_metastatic)                                             as has_metastatic,

        -- Count of distinct cancer types (comorbid cancer burden)
        count(distinct cancer_type)                                     as cancer_type_count,

        -- Earliest cancer diagnosis date across all cancer types
        min(first_diagnosis_date)                                       as first_diagnosis_date

    from {{ ref('int_cancer_patients') }}
    group by person_id

),

-- Roll up claims to patient-level cost summary
cost_summary as (

    select
        person_id,
        sum(paid_amount)                                            as total_paid_amount,
        sum(allowed_amount)                                         as total_allowed_amount,
        count(distinct claim_id)                                    as total_claims,
        max(claim_start_date)                                       as last_claim_date
    from {{ ref('int_cancer_claims') }}
    group by person_id

),

-- Get the latest claim date in the entire dataset to define "active" window
dataset_window as (
    select max(claim_start_date) as dataset_end_date
    from {{ ref('int_cancer_claims') }}
)

select
    d.person_id,
    d.sex,
    d.race,
    d.age,
    d.age_group,
    d.death_flag,

    -- Cancer classification
    pt.primary_cancer_type,
    c.cancer_type_count,
    c.has_metastatic,
    c.first_diagnosis_date,

    -- Cancer type boolean flags
    c.has_breast_cancer,
    c.has_lung_cancer,
    c.has_colorectal_cancer,
    c.has_prostate_cancer,
    c.has_lymphoma,
    c.has_leukemia,
    c.has_thyroid_cancer,
    c.has_pancreatic_cancer,
    c.has_urologic_cancer,
    c.has_gynecologic_cancer,
    c.has_other_cancer,

    -- Cost summary
    coalesce(cs.total_paid_amount, 0)   as total_paid_amount,
    coalesce(cs.total_allowed_amount, 0) as total_allowed_amount,
    coalesce(cs.total_claims, 0)         as total_claims,
    cs.last_claim_date,

    -- State: Active vs Historical (had a claim in the last 6 months of dataset)
    case
        when cs.last_claim_date >= (w.dataset_end_date - interval '6 months')
        then 'Active'
        else 'Historical'
    end as treatment_status,

    -- Spend bucket: thresholds rounded from data-driven percentiles for readability
    --   Raw percentiles: p25=$7,119 | p75=$25,535 | p90=$40,725
    --   Rounded cut lines: $7,000 | $25,000 | $40,000
    --   Rationale: rounding to clean numbers makes the buckets easier to communicate
    --   and explain to stakeholders without meaningfully changing the population distribution.
    case
        when coalesce(cs.total_paid_amount, 0) < 7000   then '1 - Low (bottom 25%)'
        when coalesce(cs.total_paid_amount, 0) < 25000  then '2 - Moderate (25th-75th pct)'
        when coalesce(cs.total_paid_amount, 0) < 40000  then '3 - High (75th-90th pct)'
        else '4 - Very High (top 10%)'
    end as spend_bucket

from cancer_pivot c
left join primary_type pt on c.person_id = pt.person_id
left join patient_demographics d on c.person_id = d.person_id
left join cost_summary cs on c.person_id = cs.person_id
cross join dataset_window w
