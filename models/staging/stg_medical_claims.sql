{{
    config(
        schema = 'oncology',
        materialized = 'view'
    )
}}

/*
    Staging model: medical claims
    Source: Tuva core__medical_claim (claim-line level, already standardized)
    Grain: one row per claim line (medical_claim_id)
    Purpose: select relevant fields and use Tuva's service_category_1 as the care setting.
             service_category_1 is already derived by the Tuva package from bill_type_code
             and place_of_service_code — no need to re-derive.

    Care setting mapping (service_category_1 values):
        inpatient     → Inpatient
        outpatient    → Outpatient
        office-based  → Office Visit
        ancillary     → Ancillary
        other         → Other
*/

select
    medical_claim_id,
    claim_id,
    claim_line_number,
    person_id,
    claim_type,
    claim_start_date,
    claim_end_date,
    service_category_1,
    service_category_2,
    place_of_service_code,
    bill_type_code,
    paid_amount,
    allowed_amount,
    charge_amount,
    payer,
    plan,
    data_source,

    -- Standardize service_category_1 into a clean care_setting label
    case service_category_1
        when 'inpatient'    then 'Inpatient'
        when 'outpatient'   then 'Outpatient'
        when 'office-based' then 'Office Visit'
        when 'ancillary'    then 'Ancillary'
        else 'Other'
    end as care_setting

from {{ ref('core__medical_claim') }}
