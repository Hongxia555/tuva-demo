{{
    config(
        schema = 'oncology',
        materialized = 'table'
    )
}}

/*
    Intermediate model: cancer patient cohort
    Source: stg_conditions
    Grain: one row per patient × cancer_type combination
           (a patient diagnosed with both Breast Cancer and Lymphoma has 2 rows)

    Cancer cohort definition:
        - ICD-10-CM codes C00–C96 (primary malignant neoplasms)
        - Any diagnosis position (condition_rank 1–N), not just primary
        - C77–C79 (secondary/metastatic codes) are NOT used to assign cancer types
          but ARE used to derive the has_metastatic flag at the patient level

    Cancer type classification:
        - Hardcoded CASE on ICD-10 3-character prefix
        - Classification logic lives here only; all downstream models inherit cancer_type

    Fields:
        - first_diagnosis_date: earliest appearance of this cancer type for the patient
        - last_diagnosis_date:  most recent appearance (helps distinguish resolved vs ongoing)
        - claim_count:          # of condition records with this cancer type (used to determine
                                primary_cancer_type in dim_cancer_patients)
        - primary_dx_claim_count: subset of claim_count where condition_rank = 1
                                  (cancer as primary reason for visit vs incidental mention)
        - has_metastatic:       1 if patient has any C77–C79 code (Stage 4 / disease spread)
*/

with all_cancer as (

    select
        person_id,
        normalized_code,
        condition_rank,
        recorded_date,

        -- Flag secondary/metastatic codes (C77–C79) separately
        case
            when normalized_code >= 'C77' and normalized_code < 'C80' then 1
            else 0
        end as is_metastatic_code,

        -- Classify cancer type from ICD-10 3-character prefix
        -- C77–C79 are intentionally set to null (handled via has_metastatic flag)
        case
            when normalized_code >= 'C77' and normalized_code < 'C80' then null
            when normalized_code like 'C50%' then 'Breast Cancer'
            when normalized_code like 'C34%' then 'Lung Cancer'
            when normalized_code like 'C18%'
              or normalized_code like 'C19%'
              or normalized_code like 'C20%' then 'Colorectal Cancer'
            when normalized_code like 'C61%' then 'Prostate Cancer'
            when normalized_code like 'C81%'
              or normalized_code like 'C82%'
              or normalized_code like 'C83%'
              or normalized_code like 'C84%'
              or normalized_code like 'C85%'
              or normalized_code like 'C86%'
              or normalized_code like 'C88%' then 'Lymphoma'
            when normalized_code like 'C91%'
              or normalized_code like 'C92%'
              or normalized_code like 'C93%'
              or normalized_code like 'C94%'
              or normalized_code like 'C95%' then 'Leukemia'
            when normalized_code like 'C73%'
              or normalized_code like 'C74%' then 'Thyroid Cancer'
            when normalized_code like 'C25%' then 'Pancreatic Cancer'
            when normalized_code like 'C64%'
              or normalized_code like 'C67%' then 'Urologic Cancer'
            when normalized_code >= 'C51'
             and normalized_code < 'C59' then 'Gynecologic Cancer'
            else 'Other Cancer'
        end as cancer_type

    from {{ ref('stg_conditions') }}
    where normalized_code >= 'C00' and normalized_code < 'C97'

),

-- Roll up to patient × cancer_type grain (exclude metastatic rows from type grouping)
typed as (

    select
        person_id,
        cancer_type,
        min(recorded_date)                                       as first_diagnosis_date,
        max(recorded_date)                                       as last_diagnosis_date,
        count(*)                                                 as claim_count,
        sum(case when condition_rank = 1 then 1 else 0 end)     as primary_dx_claim_count
    from all_cancer
    where is_metastatic_code = 0
      and cancer_type is not null
    group by person_id, cancer_type

),

-- Patient-level metastatic flag (any C77–C79 code)
metastatic as (

    select distinct person_id, 1 as has_metastatic
    from all_cancer
    where is_metastatic_code = 1

)

select
    t.person_id,
    t.cancer_type,
    t.first_diagnosis_date,
    t.last_diagnosis_date,
    t.claim_count,
    t.primary_dx_claim_count,
    coalesce(m.has_metastatic, 0) as has_metastatic

from typed t
left join metastatic m on t.person_id = m.person_id
