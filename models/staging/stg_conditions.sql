{{
    config(
        schema = 'oncology',
        materialized = 'view'
    )
}}

/*
    Staging model: conditions
    Source: Tuva core__condition (standardized ICD-10-CM diagnosis codes from claims)
    Grain: one row per condition record (person_id + claim_id + condition position)
    Purpose: thin wrapper selecting only the fields needed for oncology cohort analysis
*/

select
    person_id,
    claim_id,
    condition_rank,
    source_code_type,
    source_code,
    source_description,
    normalized_code_type,
    normalized_code,
    normalized_description,
    recorded_date,
    condition_type

from {{ ref('core__condition') }}

where normalized_code is not null
