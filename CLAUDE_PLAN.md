# Claude's Plan: Take-Home Assessment — Sr. Analytics Engineer @ Color

## Context

Build dbt models to characterize a cancer patient population using synthetic claims data (Tuva Project). The CEO wants to use billing data to find patients undergoing active cancer treatment and identify top cost drivers.

**Deliverables:**
- dbt models (Staging → Intermediate → Marts)
- Updated `README.md` with methodology, findings, and AI usage log

**Time budget:** 2–3 hours

---

## Step 0: Fork the Repo

Fork https://github.com/seamus-mckinsey/tuva-demo to your own GitHub account, then work from the fork. Submit by emailing the public fork URL.

> ⚠️ Local repo already at `/Users/hohoho/.../tuva-demo` is a clone — push to a forked repo before submitting.

---

## dbt Model Architecture (Staging → Intermediate → Marts)

We build ON TOP of Tuva's already-processed core models. Raw seed data → Tuva core → our layers.

### Architecture Decisions: Dimensional, SCD, and State-Separated Considerations

**Dimensional modeling:**
The marts follow a dimensional pattern — `dim_cancer_patients` (patient attributes) feeds `fct_cancer_claims` (grain-level facts) and aggregate summaries. We use `dim_` / `fct_` naming in marts to signal this intent. A reviewer immediately understands the grain and role of each table.

**Slowly Changing Dimensions (SCDs):**
- Eligibility is naturally SCD Type 2: patients may switch payer/plan across enrollment spans. We handle this in `dim_cancer_patients` by selecting the most recent active enrollment record to get current demographics.
- Cancer staging could also be SCD Type 2 (a patient may be diagnosed Stage 1, then rediagnosed Stage 3) — with claims data we can only infer this from diagnosis codes over time, not track it explicitly. We acknowledge this limitation in the README.
- For this dataset (single-year snapshot), full SCD materialization (`dbt snapshot`) is unnecessary. Worth noting for production.

**State-separated model:**
Separating "current state" from "event history" is valuable for cancer populations:
- **State:** Is the patient actively in treatment, in remission, or palliative? We approximate "active" vs "historical" treatment in `dim_cancer_patients` using a recency flag (did the patient have an oncology-related claim in the last 6 months of the dataset period?).
- **Events:** Individual claim records in `fct_cancer_claims` capture the event history.
- This separation makes it easy to query "who is currently active?" (state) vs "what was the cost trajectory?" (events).

```
models/
├── staging/
│   ├── stg_conditions.sql           # relevant fields from core__condition
│   └── stg_medical_claims.sql       # relevant fields from core__medical_claim + care_setting tag
├── intermediate/
│   ├── int_cancer_patients.sql      # patients with ICD-10 C00-C96 + cancer type (CASE statement)
│   └── int_cancer_claims.sql        # all claims for cancer patients + care setting tag
└── marts/
    ├── dim_cancer_patients.sql      # 1 row per patient: type, demographics, spend_bucket, active_flag
    ├── fct_cancer_claims.sql        # 1 row per claim: grain-level fact with all measures
    ├── cancer_cost_by_setting.sql   # aggregate: total spend % by care setting
    └── cancer_segments.sql          # aggregate: cancer_type × spend_bucket cross-tab
```

> Note: `cancer_cost_by_setting` and `cancer_segments` keep plain names (not `fct_`) because they are pre-aggregated summaries, not grain-level fact tables.

---

## Model Specs

### `staging/stg_conditions.sql`
- **Source:** `{{ ref('core__condition') }}` (Tuva core mart)
- **Purpose:** Thin wrapper — select only the columns needed downstream
- **Key columns:** `person_id`, `claim_id`, `condition_rank`, `normalized_code`, `normalized_description`, `recorded_date`, `condition_type`

### `staging/stg_medical_claims.sql`
- **Source:** `{{ ref('core__medical_claim') }}` (Tuva core mart)
- **Purpose:** Select claims fields + tag care setting
- **Key columns:** `claim_id`, `person_id`, `claim_start_date`, `paid_amount`, `allowed_amount`, `place_of_service_code`, `bill_type_code`, `claim_type`
- **Care setting tag:**

```sql
CASE
    WHEN claim_type = 'institutional' AND bill_type_code LIKE '11%' THEN 'Inpatient'
    WHEN claim_type = 'institutional' AND bill_type_code LIKE '13%' THEN 'Outpatient'
    WHEN place_of_service_code = '23' THEN 'Emergency Room'
    WHEN place_of_service_code = '11' THEN 'Office Visit'
    WHEN claim_type = 'pharmacy' THEN 'Pharmacy'
    ELSE 'Other'
END AS care_setting
```

### `intermediate/int_cancer_patients.sql`
- **Source:** `{{ ref('stg_conditions') }}`
- **Purpose:** Identify all cancer diagnoses per patient — **one row per patient × cancer_type** (no deduplication)
- **Cancer definition:** ICD-10 codes C00–C96 (primary malignant neoplasms)
- **Key output columns:** `person_id`, `cancer_type`, `first_diagnosis_date`, `last_diagnosis_date`, `claim_count`, `primary_dx_claim_count`, `has_metastatic`

**Cancer type logic — hardcoded CASE on ICD-10 prefix:**

```sql
WITH all_cancer AS (
    SELECT
        person_id,
        normalized_code,
        condition_rank,
        recorded_date,
        -- Flag secondary/metastatic codes separately (C77–C79) rather than excluding them
        CASE
            WHEN normalized_code >= 'C77' AND normalized_code < 'C80' THEN 1
            ELSE 0
        END AS is_metastatic_code,
        CASE
            WHEN normalized_code >= 'C77' AND normalized_code < 'C80' THEN NULL
            WHEN normalized_code LIKE 'C50%' THEN 'Breast Cancer'
            WHEN normalized_code LIKE 'C34%' THEN 'Lung Cancer'
            WHEN normalized_code LIKE 'C18%'
              OR normalized_code LIKE 'C19%'
              OR normalized_code LIKE 'C20%' THEN 'Colorectal Cancer'
            WHEN normalized_code LIKE 'C61%' THEN 'Prostate Cancer'
            WHEN normalized_code LIKE 'C81%' OR normalized_code LIKE 'C82%'
              OR normalized_code LIKE 'C83%' OR normalized_code LIKE 'C84%'
              OR normalized_code LIKE 'C85%' OR normalized_code LIKE 'C86%'
              OR normalized_code LIKE 'C88%' THEN 'Lymphoma'
            WHEN normalized_code LIKE 'C91%' OR normalized_code LIKE 'C92%'
              OR normalized_code LIKE 'C93%' OR normalized_code LIKE 'C94%'
              OR normalized_code LIKE 'C95%' THEN 'Leukemia'
            WHEN normalized_code LIKE 'C73%' OR normalized_code LIKE 'C74%' THEN 'Thyroid Cancer'
            WHEN normalized_code LIKE 'C25%' THEN 'Pancreatic Cancer'
            WHEN normalized_code LIKE 'C64%' OR normalized_code LIKE 'C67%' THEN 'Urologic Cancer'
            WHEN normalized_code >= 'C51' AND normalized_code < 'C59' THEN 'Gynecologic Cancer'
            ELSE 'Other Cancer'
        END AS cancer_type
    FROM {{ ref('stg_conditions') }}
    WHERE normalized_code >= 'C00' AND normalized_code < 'C97'
),
-- Roll up to patient-cancer_type grain (exclude metastatic rows from type grouping)
typed AS (
    SELECT
        person_id,
        cancer_type,
        MIN(recorded_date)                                          AS first_diagnosis_date,
        MAX(recorded_date)                                          AS last_diagnosis_date,
        COUNT(*)                                                    AS claim_count,
        SUM(CASE WHEN condition_rank = 1 THEN 1 ELSE 0 END)        AS primary_dx_claim_count
    FROM all_cancer
    WHERE is_metastatic_code = 0
      AND cancer_type IS NOT NULL
    GROUP BY person_id, cancer_type
),
-- Patient-level metastatic flag (any C77–C79 code found)
metastatic AS (
    SELECT DISTINCT person_id, 1 AS has_metastatic
    FROM all_cancer
    WHERE is_metastatic_code = 1
)
SELECT
    t.person_id,
    t.cancer_type,
    t.first_diagnosis_date,
    t.last_diagnosis_date,
    t.claim_count,
    t.primary_dx_claim_count,
    COALESCE(m.has_metastatic, 0) AS has_metastatic
FROM typed t
LEFT JOIN metastatic m ON t.person_id = m.person_id
-- Grain: one row per patient-cancer_type (a patient with Breast + Lymphoma has 2 rows)
-- has_metastatic is the same for all rows of the same patient (patient-level flag)
```

- **C77–C79 handling:** Not excluded — used to derive `has_metastatic` at the patient level. Identifies Stage 4/advanced patients without inflating cancer type classification.
- **`last_diagnosis_date`:** Enables identifying resolved vs ongoing diagnoses.
- **`primary_dx_claim_count`:** Distinguishes cancer as the primary reason for a visit (rank 1) vs incidental mention. If `0`, the cancer may be a comorbidity, not the main driver of utilization.

### `intermediate/int_cancer_claims.sql`
- **Source:** `{{ ref('int_cancer_patients') }}` + `{{ ref('stg_medical_claims') }}`
- **Purpose:** Filter all claims to cancer patients only, attach care setting
- **Key output columns:** `claim_id`, `person_id`, `cancer_type`, `care_setting`, `paid_amount`, `allowed_amount`, `claim_start_date`

### `marts/dim_cancer_patients.sql`
- **Source:** `{{ ref('int_cancer_patients') }}` + `core__patient` (demographics) + `int_cancer_claims` (cost roll-up)
- **Grain:** 1 row per cancer patient (patient dimension)
- **Key columns:** `person_id`, `age`, `sex`, `primary_cancer_type`, `total_paid_amount`, `total_claims`, `first_diagnosis_date`, `spend_bucket`, `treatment_status` + boolean cancer type flags

**Boolean flags for all cancer types** (tracks patients with multiple diagnoses — pivoted from `int_cancer_patients` which is one row per patient × cancer type):

```sql
MAX(CASE WHEN cancer_type = 'Breast Cancer'      THEN 1 ELSE 0 END) AS has_breast_cancer,
MAX(CASE WHEN cancer_type = 'Lung Cancer'        THEN 1 ELSE 0 END) AS has_lung_cancer,
MAX(CASE WHEN cancer_type = 'Colorectal Cancer'  THEN 1 ELSE 0 END) AS has_colorectal_cancer,
MAX(CASE WHEN cancer_type = 'Prostate Cancer'    THEN 1 ELSE 0 END) AS has_prostate_cancer,
MAX(CASE WHEN cancer_type = 'Lymphoma'           THEN 1 ELSE 0 END) AS has_lymphoma,
MAX(CASE WHEN cancer_type = 'Leukemia'           THEN 1 ELSE 0 END) AS has_leukemia,
MAX(CASE WHEN cancer_type = 'Thyroid Cancer'     THEN 1 ELSE 0 END) AS has_thyroid_cancer,
MAX(CASE WHEN cancer_type = 'Pancreatic Cancer'  THEN 1 ELSE 0 END) AS has_pancreatic_cancer,
MAX(CASE WHEN cancer_type = 'Urologic Cancer'    THEN 1 ELSE 0 END) AS has_urologic_cancer,
MAX(CASE WHEN cancer_type = 'Gynecologic Cancer' THEN 1 ELSE 0 END) AS has_gynecologic_cancer,
MAX(CASE WHEN cancer_type = 'Other Cancer'       THEN 1 ELSE 0 END) AS has_other_cancer,
MAX(has_metastatic)                                                  AS has_metastatic,
COUNT(DISTINCT cancer_type)                                          AS cancer_type_count
```

> **Design decision:** An early draft collapsed each patient to a single `primary_cancer_type`. This was revised to add boolean flags because 35% of patients have multiple cancer types — dropping that information would produce an incomplete picture of comorbid cancer burden. Both `primary_cancer_type` (for easy grouping) and the boolean flags (for multi-cancer queries) are preserved.

**`treatment_status` (state-separated flag):**

```sql
CASE
    WHEN cs.last_claim_date >= (w.dataset_end_date - INTERVAL '6 months')
    THEN 'Active'
    ELSE 'Historical'
END AS treatment_status
-- "Active" = had an oncology claim in the last 6 months of the dataset period
```

**Spend bucket — data-driven thresholds:**

Before hardcoding thresholds, run this exploratory query against `local.duckdb`:

```sql
WITH cancer_conditions AS (
    SELECT DISTINCT person_id
    FROM core.core__condition
    WHERE normalized_code >= 'C00' AND normalized_code < 'C97'
),
cancer_spend AS (
    SELECT
        c.person_id,
        SUM(m.paid_amount) AS total_paid
    FROM cancer_conditions c
    JOIN core.core__medical_claim m USING (person_id)
    GROUP BY c.person_id
)
SELECT
    MIN(total_paid)                                          AS min_spend,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_paid) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_paid) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_paid) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_paid) AS p90,
    MAX(total_paid)                                          AS max_spend,
    COUNT(*)                                                 AS cancer_patient_count
FROM cancer_spend;
```

Use p25, p75, and p90 as the bucket cut-points — actual quartiles of the population, not arbitrary dollar amounts. Round to clean numbers for stakeholder communication and document both values.

```sql
CASE
    WHEN total_paid_amount < 7000   THEN '1 - Low (bottom 25%)'
    WHEN total_paid_amount < 25000  THEN '2 - Moderate (25th–75th pct)'
    WHEN total_paid_amount < 40000  THEN '3 - High (75th–90th pct)'
    ELSE '4 - Very High (top 10%)'
END AS spend_bucket
-- Raw percentiles: p25=$7,119 | p75=$25,535 | p90=$40,725
-- Rounded to: $7,000 | $25,000 | $40,000
```

### `marts/fct_cancer_claims.sql`
- **Source:** `{{ ref('int_cancer_claims') }}`
- **Grain:** 1 row per claim (event history fact table)
- **Purpose:** Preserves full claim-level detail — downstream users can re-aggregate by any dimension without re-running the pipeline. Enables trend analysis and ad-hoc queries.

### `marts/cancer_cost_by_setting.sql`
- **Source:** `{{ ref('int_cancer_claims') }}`
- **Grain:** 1 row per care setting (aggregate)
- **Key columns:** `care_setting`, `total_paid_amount`, `pct_of_total_spend`, `patient_count`, `avg_paid_per_patient`
- **Purpose:** Answer "where is the money going?" — e.g., 34.8% Inpatient, 27.7% Outpatient

### `marts/cancer_segments.sql`
- **Source:** `{{ ref('dim_cancer_patients') }}`
- **Grain:** 1 row per `primary_cancer_type` × `spend_bucket` combination
- **Key columns:** `primary_cancer_type`, `spend_bucket`, `patient_count`, `total_paid_amount`, `avg_paid_per_patient`, `pct_of_cancer_population`, `pct_of_total_cancer_spend`
- **Purpose:** Show which cancer types drive the most cost and how spend is distributed within each type

---

## Data Testing + Documentation (schema.yml per layer)

### `models/staging/schema.yml`

```yaml
version: 2

models:
  - name: stg_conditions
    description: "Staging model selecting relevant fields from core__condition for cancer cohort analysis."
    columns:
      - name: person_id
        description: "Unique identifier for the patient."
        tests: [not_null]
      - name: claim_id
        description: "Claim associated with this condition."
      - name: condition_rank
        description: "Diagnosis position on the claim (1 = primary diagnosis)."
      - name: normalized_code_type
        description: "Coding system for the normalized diagnosis code (e.g. ICD-10-CM)."
        meta:
          terminology: https://thetuvaproject.com/terminology/code-type
      - name: normalized_code
        description: "Standardized ICD-10-CM diagnosis code."
        tests: [not_null]
        meta:
          terminology: https://thetuvaproject.com/terminology/icd-10-cm
      - name: normalized_description
        description: "Human-readable description of the diagnosis code."
      - name: recorded_date
        description: "Date the condition was recorded on the claim."
      - name: condition_type
        description: "Whether the condition is from a claim or a clinical source."

  - name: stg_medical_claims
    description: "Staging model selecting relevant fields from core__medical_claim and tagging each claim with a care setting."
    columns:
      - name: claim_id
        description: "Unique identifier for the claim."
        tests: [not_null, unique]
      - name: person_id
        description: "Unique identifier for the patient."
        tests: [not_null]
      - name: claim_start_date
        description: "Start date of the claim."
      - name: paid_amount
        description: "Total amount paid by the payer for this claim."
      - name: allowed_amount
        description: "Total allowed amount for this claim."
      - name: place_of_service_code
        description: "CMS place of service code indicating where care was delivered."
        meta:
          terminology: https://thetuvaproject.com/terminology/place-of-service
      - name: bill_type_code
        description: "Bill type code for institutional claims (e.g. 111 = inpatient)."
      - name: claim_type
        description: "Type of claim: professional, institutional, or pharmacy."
        meta:
          terminology: https://thetuvaproject.com/terminology/claim-type
      - name: care_setting
        description: "Derived care setting bucket based on service_category_1 from Tuva."
        tests:
          - accepted_values:
              values: ['Inpatient', 'Outpatient', 'Office Visit', 'Ancillary', 'Other']
```

### `models/intermediate/schema.yml`

```yaml
version: 2

models:
  - name: int_cancer_patients
    description: >
      One row per patient-cancer_type combination. A patient with Breast Cancer and Lymphoma
      will have 2 rows. ICD-10-CM codes C00–C96 (excluding C77–C79 secondary/metastatic).
      Used as input to dim_cancer_patients where boolean flags are pivoted out.
    columns:
      - name: person_id
        description: "Unique identifier for the cancer patient."
        tests: [not_null]
      - name: cancer_type
        description: "Grouped cancer type derived from ICD-10 prefix (e.g. Breast Cancer, Lung Cancer)."
        tests:
          - not_null
          - accepted_values:
              values: ['Breast Cancer', 'Lung Cancer', 'Colorectal Cancer', 'Prostate Cancer',
                       'Lymphoma', 'Leukemia', 'Thyroid Cancer', 'Pancreatic Cancer',
                       'Urologic Cancer', 'Gynecologic Cancer', 'Other Cancer']
      - name: first_diagnosis_date
        description: "Earliest date a cancer code of this type appeared on a claim for this patient."
      - name: last_diagnosis_date
        description: "Most recent date a cancer code of this type appeared. Helps identify resolved vs ongoing diagnoses."
      - name: claim_count
        description: "Number of claims with this cancer type for this patient. Used to determine primary_cancer_type in dim_cancer_patients."
      - name: primary_dx_claim_count
        description: "Claims where this cancer type was the primary diagnosis (condition_rank = 1). 0 suggests the cancer is incidental, not the main driver of utilization."
      - name: has_metastatic
        description: "1 if this patient has any C77-C79 secondary/metastatic code. Indicates documented disease spread (Stage 4)."

  - name: int_cancer_claims
    description: "All medical claims belonging to cancer patients, enriched with cancer_type and care_setting."
    columns:
      - name: claim_id
        description: "Unique identifier for the claim."
        tests: [not_null, unique]
      - name: person_id
        description: "Patient identifier — must exist in int_cancer_patients."
        tests:
          - not_null
          - relationships:
              to: ref('int_cancer_patients')
              field: person_id
      - name: cancer_type
        description: "Cancer type inherited from int_cancer_patients."
      - name: care_setting
        description: "Care setting inherited from stg_medical_claims."
      - name: paid_amount
        description: "Amount paid for this claim."
      - name: allowed_amount
        description: "Allowed amount for this claim."
      - name: claim_start_date
        description: "Start date of the claim."
```

### `models/marts/schema.yml`

```yaml
version: 2

models:
  - name: dim_cancer_patients
    description: >
      Patient dimension table. One row per cancer patient. Summarizes demographics,
      cancer type, total cost, spend bucket (data-driven percentile thresholds), and
      treatment_status (Active = had a claim in the last 6 months of the dataset period).
    columns:
      - name: person_id
        tests: [not_null, unique]
      - name: total_paid_amount
        tests: [not_null]
      - name: spend_bucket
        tests: [not_null]
      - name: treatment_status
        tests:
          - accepted_values:
              values: ['Active', 'Historical']
      - name: primary_cancer_type
        tests:
          - accepted_values:
              values: ['Breast Cancer', 'Lung Cancer', 'Colorectal Cancer', 'Prostate Cancer',
                       'Lymphoma', 'Leukemia', 'Thyroid Cancer', 'Pancreatic Cancer',
                       'Urologic Cancer', 'Gynecologic Cancer', 'Other Cancer']

  - name: fct_cancer_claims
    description: >
      Claim-grain fact table. One row per claim for cancer patients.
    columns:
      - name: medical_claim_id
        tests: [not_null, unique]
      - name: person_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_cancer_patients')
              field: person_id
      - name: care_setting
        tests:
          - accepted_values:
              values: ['Inpatient', 'Outpatient', 'Office Visit', 'Ancillary', 'Other']

  - name: cancer_cost_by_setting
    description: "Aggregate cost breakdown by care setting for the cancer population."
    columns:
      - name: care_setting
        tests: [not_null, unique]

  - name: cancer_segments
    description: "Cross-tabulation of cancer type and spend bucket for the cancer population."
    columns:
      - name: primary_cancer_type
        tests: [not_null]
      - name: spend_bucket
        tests: [not_null]
```

---

## Modularity, Reliability & Scalability

### Modularity
- **Cancer type CASE is in one place only:** The ICD-10 to cancer type mapping lives exclusively in `int_cancer_patients.sql`. All downstream models inherit `cancer_type` — no copy-pasting.
- **Single source of truth for cohort:** `int_cancer_patients` is the only place cancer membership is defined. All downstream models reference it — never re-derive the cohort.
- **Staging is reusable:** `stg_conditions` and `stg_medical_claims` are not cancer-specific — they could be used for any other population analysis (diabetes, readmissions, etc.). No cancer logic in the staging layer.

### Reliability
- `schema.yml` tests catch: nulls in key columns, duplicate grain, unexpected `care_setting` or `cancer_type` values, broken foreign keys between layers
- Run `dbt test` after each build step to catch issues early
- No `unique` test on `person_id` in `int_cancer_patients` (intentionally one row per patient × cancer type); `unique` is enforced at the mart grain

### Scalability
- **No row-level loops or cursors** — all SQL is set-based, scales to millions of patients
- **CASE statement evaluated inline** — no join overhead; cancer type tagging is cheap at any scale
- **Care setting logic is in staging** — tagged once at the claim level, not re-computed in every downstream model
- **Platform-agnostic** — uses standard SQL, no DuckDB-specific functions. The same models run on Snowflake/BigQuery/Databricks (Tuva supports all of them) without changes
- If the dataset grows: add `partition by year_month` or `cluster_by: person_id` in mart configs in `dbt_project.yml`

---

## Methodology Decisions

1. **Cancer definition:** ICD-10 C00–C96 (primary malignant neoplasms), any diagnosis position (not just primary)
2. **C77–C79 handling:** Not excluded — used to derive `has_metastatic` flag. Patient is already captured by their primary cancer code; using secondary codes only for the metastatic flag avoids inflation while preserving the Stage 4 signal.
3. **"Active" cancer:** Any patient with a C00–C96 code on any claim in the dataset. Limitation: a resolved cancer mentioned incidentally on one claim would be included.
4. **Multiple cancer types:** `primary_cancer_type` = type with most claim occurrences. Boolean flags preserve full multi-cancer history.
5. **Care setting:** Derived from Tuva's pre-computed `service_category_1` rather than re-deriving from `place_of_service_code` + `bill_type_code` — avoids duplicating Tuva's normalization logic.
6. **Spend buckets:** Thresholds from actual data (p25/p75/p90), rounded to clean numbers for communication. Raw percentiles: p25=$7,119 | p75=$25,535 | p90=$40,725. Rounded: $7,000 | $25,000 | $40,000.
7. **Building on Tuva core:** References `core__condition` and `core__medical_claim` (already standardized by Tuva) rather than raw seeds — avoids re-implementing normalization logic.

---

## Critical Files

| File | Action |
|---|---|
| `models/staging/stg_conditions.sql` | CREATE |
| `models/staging/stg_medical_claims.sql` | CREATE |
| `models/staging/schema.yml` | CREATE |
| `models/intermediate/int_cancer_patients.sql` | CREATE |
| `models/intermediate/int_cancer_claims.sql` | CREATE |
| `models/intermediate/schema.yml` | CREATE |
| `models/marts/dim_cancer_patients.sql` | CREATE |
| `models/marts/fct_cancer_claims.sql` | CREATE |
| `models/marts/cancer_cost_by_setting.sql` | CREATE |
| `models/marts/cancer_segments.sql` | CREATE |
| `models/marts/schema.yml` | CREATE |
| `dbt_project.yml` | UPDATE — add model configs for staging/intermediate/marts |
| `README.md` | UPDATE — append methodology, findings, AI usage log |

---

## Execution Steps

1. **Verify data exists** — query `local.duckdb` to confirm `core__condition` and `core__medical_claim` have rows
2. **Run spend distribution query** — execute the exploratory SQL above; record p25/p75/p90 for spend bucket thresholds
3. **Create staging models + schema.yml** — build `stg_conditions`, `stg_medical_claims`; run `dbt test --select staging`
4. **Create intermediate models + schema.yml** — build `int_cancer_patients`, `int_cancer_claims`; run `dbt test --select intermediate`
5. **Create marts + schema.yml** — build all mart models with data-driven spend thresholds; run `dbt test --select marts`
6. **Run full build + test** — `uv run dbt run && uv run dbt test`
7. **Query results** — pull final numbers for README
8. **Update README.md** — append methodology (with actual percentile thresholds), findings, and AI usage log
9. **Push to forked repo**

---

## Verification Checklist

- [ ] `dbt run` completes with 0 errors for all new models
- [ ] All `dbt test` assertions pass (42 tests)
- [ ] `dim_cancer_patients` row count matches expected cancer patient count
- [ ] `cancer_cost_by_setting` percentages sum to ~100%
- [ ] README is clear to a non-technical reader and includes methodology, findings, and AI usage log
