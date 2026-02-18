[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.5.x&color=orange)

# The Tuva Project Demo

## 🧰 What does this project do?

This demo provides a quick and easy way to run the Tuva Project 
Package in a dbt project with synthetic data for 1k patients loaded as dbt seeds.

To set up the Tuva Project with your own claims data or to better understand what the Tuva Project does, please review the ReadMe in [The Tuva Project](https://github.com/tuva-health/the_tuva_project) package for a detailed walkthrough and setup.

For information on the data models check out our [Docs](https://thetuvaproject.com/).

## ✅ How to get started

### Pre-requisites
You only need one thing installed:
1. [uv](https://docs.astral.sh/uv/getting-started/) - a fast Python package manager. Installation is simple and OS-agnostic:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
   Or on Windows:
   ```powershell
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

**Note:** This demo uses DuckDB as the database, so you don't need to configure a connection to an external data warehouse. Everything is configured and ready to go!

### Getting Started
Complete the following steps to run the demo:

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine or environment.
1. In the project directory, install Python dependencies and set up the virtual environment:
   ```bash
   uv sync
   ```
1. Activate the virtual environment:
   ```bash
   source .venv/bin/activate  # On macOS/Linux
   # or on Windows:
   .venv\Scripts\activate
   ```
1. Run `dbt deps` to install the Tuva Project package:
   ```bash
   dbt deps
   ```
1. Run `dbt build` to run the entire project with the built-in sample data:
   ```bash
   dbt build
   ```

The `profiles.yml` file is already included in this repo and pre-configured for DuckDB, so no additional setup is needed!

### Using uv commands
You can also run dbt commands directly with `uv run` without activating the virtual environment:
```bash
uv run dbt deps
uv run dbt build
```

## 🤝 Community

Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!

---

## Color Analytics Engineer — Take-Home Assessment

### Project Overview

This project extends the Tuva Project demo with an oncology analytics layer to help Color's data team identify patients undergoing active cancer treatment and understand their top drivers of cost. The CEO asked: *Can we use claims data to find cancer patients and where their spend is going?* This analysis answers that question using a clean Staging → Intermediate → Marts dbt architecture built on top of Tuva's standardized core models.

---

### Data Model Architecture

```
seeds (synthetic claims data)
    └── Tuva Project core models (core.condition, core.medical_claim, core.patient)
            └── staging/
            │   ├── stg_conditions.sql          view  — relevant fields from core__condition
            │   └── stg_medical_claims.sql       view  — relevant fields + care_setting tag
            └── intermediate/
            │   ├── int_cancer_patients.sql      table — one row per patient × cancer type
            │   └── int_cancer_claims.sql        table — all claims for cancer patients
            └── marts/
                ├── dim_cancer_patients.sql      table — patient dimension (1 row/patient)
                ├── fct_cancer_claims.sql        table — claim-line fact table
                ├── cancer_cost_by_setting.sql   table — aggregate spend by care setting
                └── cancer_segments.sql          table — cancer type × spend bucket cross-tab
```

All new models land in the `oncology` schema in DuckDB.

**Architecture decisions:**
- **Dimensional modeling:** `dim_` prefix for the patient dimension, `fct_` for the grain-level fact table. Aggregate summaries keep plain names.
- **State separation:** `dim_cancer_patients` has a `treatment_status` field (Active / Historical) to separate current state from the event history in `fct_cancer_claims`.
- **SCD awareness:** Eligibility is naturally SCD Type 2 (patients can switch payers). For this single-year snapshot, we use the most recent enrollment record. Full `dbt snapshot` SCD2 materialization would be appropriate in production.
- **Build on Tuva core:** We reference `core__condition` and `core__medical_claim` (already standardized by Tuva) rather than raw seeds, avoiding re-implementing normalization logic.

---

### Methodology

#### Cancer Cohort Definition

- **ICD-10-CM codes C00–C96** (primary malignant neoplasms), any diagnosis position (condition_rank 1–N)
- **"Active cancer"** is defined as: at least one claim with a C00–C96 code in the dataset period
- **C77–C79 (secondary/metastatic codes)** are not used to classify cancer type, but are used to derive a `has_metastatic` flag. This avoids inflating patient counts while preserving the clinical signal that a patient has documented disease spread (Stage 4)
- **Multiple cancer types:** A patient coded with Breast Cancer and Lymphoma appears in `int_cancer_patients` with 2 rows (one per type) and gets boolean flags in `dim_cancer_patients`. `primary_cancer_type` = the type with the most condition records

**Limitations:**
- Claims data can only capture what was billed, not clinical staging or treatment intent
- "Active cancer" is a billing-based approximation; a resolved cancer mentioned in passing on one claim would be included
- Synthetic data — prevalence rates are not representative of real-world populations (39.4% cancer rate is unrealistic)
- Single year snapshot (2018); no trend analysis possible

#### Care Setting

Used Tuva's pre-derived `service_category_1` field rather than re-deriving from `place_of_service_code` + `bill_type_code`. Tuva applies CMS standard logic; this avoids duplication of normalization logic and keeps staging models thin.

| care_setting | service_category_1 | Description |
|---|---|---|
| Inpatient | inpatient | Hospital admissions (acute, SNF, rehab, psychiatric) |
| Outpatient | outpatient | Outpatient hospital, ED, ASC, radiology |
| Office Visit | office-based | Office visits, telehealth, PT/OT |
| Ancillary | ancillary | Lab, DME, ambulance |
| Other | other | Unclassified |

#### Spend Buckets

Thresholds are derived from the actual spend distribution of cancer patients in this dataset. Raw percentiles were first calculated:

| Percentile | Raw value |
|---|---|
| p25 | $7,119 |
| p75 | $25,535 |
| p90 | $40,725 |

**Decision:** Cut lines were rounded to clean numbers ($7,000 / $25,000 / $40,000) for two reasons: (1) the rounding has negligible impact on population distribution (only a handful of patients shift buckets), and (2) round numbers are easier to communicate and explain to stakeholders without requiring them to understand the precise percentile calculation.

| Bucket | Threshold | Population share |
|---|---|---|
| 1 - Low | < $7,000 | ~Bottom 25% |
| 2 - Moderate | $7,000 – $25,000 | ~25th–75th percentile |
| 3 - High | $25,000 – $40,000 | ~75th–90th percentile |
| 4 - Very High | > $40,000 | ~Top 10% |

---

### Key Findings

#### Population

| Metric | Value |
|---|---|
| Total patients | 1,000 |
| Cancer patients | 394 (39.4%) |
| Active (claims in last 6 months) | 337 (85%) |
| Historical | 57 (15%) |
| Patients with 2+ cancer types | 138 (35%) |
| Patients with metastatic codes (C77–C79) | 25 (6%) |

```sql
-- Population summary
SELECT
    (SELECT count(*) FROM core.core__patient)                          AS total_patients,
    count(*)                                                           AS cancer_patients,
    round(100.0 * count(*) / (SELECT count(*) FROM core.core__patient), 1) AS cancer_pct,
    sum(CASE WHEN treatment_status = 'Active'   THEN 1 ELSE 0 END)    AS active_patients,
    sum(CASE WHEN treatment_status = 'Historical' THEN 1 ELSE 0 END)  AS historical_patients,
    sum(CASE WHEN cancer_type_count >= 2 THEN 1 ELSE 0 END)           AS multi_cancer_patients,
    sum(has_metastatic)                                                AS metastatic_patients
FROM oncology.dim_cancer_patients;
```

**Top cancer types by patient count:**

| Cancer Type | Patients | Total Spend |
|---|---|---|
| Other Cancer | 176 | $3,149,597 |
| Prostate Cancer | 87 | $1,665,160 |
| Breast Cancer | 46 | $878,919 |
| Lung Cancer | 11 | $325,755 |
| Gynecologic Cancer | 11 | $274,659 |
| Lymphoma | 12 | $232,608 |
| Leukemia | 10 | $232,190 |
| Urologic Cancer | 19 | $432,454 |
| Colorectal Cancer | 9 | $182,797 |
| Thyroid Cancer | 13 | $172,093 |

```sql
-- Cancer patients and total spend by primary cancer type
-- Note: primary_cancer_type = the type with the most condition records for that patient
SELECT
    primary_cancer_type,
    count(*)                    AS patient_count,
    sum(total_paid_amount)      AS total_spend
FROM oncology.dim_cancer_patients
GROUP BY primary_cancer_type
ORDER BY patient_count DESC;
```

#### Cost

| Metric | Value |
|---|---|
| Total cancer population spend | $7,546,232 |
| Average spend per cancer patient | $19,153 |

```sql
-- Total and average spend across all cancer patients
SELECT
    sum(total_paid_amount)                          AS total_cancer_spend,
    round(avg(total_paid_amount), 0)                AS avg_spend_per_patient
FROM oncology.dim_cancer_patients;
```

**Spend by care setting:**

| Care Setting | Total Spend | % of Total | Patients |
|---|---|---|---|
| Inpatient | $2,628,465 | 34.8% | 214 |
| Outpatient | $2,091,949 | 27.7% | 388 |
| Office Visit | $1,794,707 | 23.8% | 390 |
| Ancillary | $1,027,930 | 13.6% | 392 |
| Other | $3,182 | 0.04% | 28 |

```sql
-- Spend breakdown by care setting
SELECT
    care_setting,
    total_paid_amount,
    pct_of_total_spend,
    patient_count
FROM oncology.cancer_cost_by_setting
ORDER BY total_paid_amount DESC;
```

**Spend by bucket:**

| Spend Bucket | Patients | Total Spend | % of Cancer Spend |
|---|---|---|---|
| 1 - Low (<$7,000) | 95 | $384,542 | 5.1% |
| 2 - Moderate ($7,000–$25,000) | 198 | $2,801,108 | 37.1% |
| 3 - High ($25,000–$40,000) | 59 | $1,815,517 | 24.1% |
| 4 - Very High (>$40,000) | 42 | $2,545,065 | 33.7% |

```sql
-- Patient count, total spend, and share of cancer spend by spend bucket
SELECT
    spend_bucket,
    count(*)                                                            AS patient_count,
    sum(total_paid_amount)                                              AS total_spend,
    round(
        100.0 * sum(total_paid_amount) / sum(sum(total_paid_amount)) over (),
        1
    )                                                                   AS pct_of_cancer_spend
FROM oncology.dim_cancer_patients
GROUP BY spend_bucket
ORDER BY spend_bucket;
```

#### Notable Observations

1. **Inpatient is the largest single cost driver at 34.8%** — despite only 214 of 394 cancer patients having any inpatient claim. This points to high per-admission cost for cancer patients and signals a potential opportunity for care management programs focused on preventing avoidable inpatient stays.

```sql
-- Inpatient cost concentration: avg spend per patient vs share of total
SELECT
    care_setting,
    total_paid_amount,
    patient_count,
    pct_of_total_spend,
    avg_paid_per_patient
FROM oncology.cancer_cost_by_setting
WHERE care_setting = 'Inpatient';
```

2. **The top ~10% of spenders (42 patients, >$40K) drive 33.7% of total cancer spend** — a classic concentration of cost. Identifying and proactively managing this very high-cost cohort could meaningfully bend the cost curve.

```sql
-- Profile of Very High spend patients (>$40K): who are they and what drives their cost?
SELECT
    d.person_id,
    d.primary_cancer_type,
    d.age,
    d.sex,
    d.has_metastatic,
    d.total_paid_amount,
    d.total_claims,
    d.treatment_status
FROM oncology.dim_cancer_patients d
WHERE d.spend_bucket = '4 - Very High (top 10%)'
ORDER BY d.total_paid_amount DESC;
```

3. **35% of cancer patients have 2+ coded cancer types** — suggesting significant comorbid cancer burden in this synthetic population. In production, this pattern would warrant deeper clinical review to distinguish true co-occurring primaries from miscoded secondary/metastatic disease.

```sql
-- Distribution of patients by number of distinct cancer types
SELECT
    cancer_type_count,
    count(*)                                                            AS patient_count,
    round(100.0 * count(*) / sum(count(*)) over (), 1)                 AS pct_of_cancer_patients
FROM oncology.dim_cancer_patients
GROUP BY cancer_type_count
ORDER BY cancer_type_count;
```

---

### How to Run the Oncology Models

After completing the main setup (`dbt build`), run the oncology layer:

```bash
# Build all oncology models
dbt run --select staging intermediate marts

# Run tests
dbt test --select staging intermediate marts
```

All models land in the `oncology` schema. Query them directly:

```bash
# In Python
python -c "
import duckdb
con = duckdb.connect('local.duckdb')
print(con.execute('SELECT * FROM oncology.cancer_cost_by_setting').df())
"
```

---

### AI Usage Log

This assessment was completed with Claude (Anthropic) as an AI assistant, specifically using **Claude Code** (Anthropic's CLI coding agent) with its **plan mode** feature. Roughly 90% of the working time was spent in the planning phase — exploring the repo, debating architecture trade-offs, and writing a detailed implementation plan — before any SQL was written. This front-loaded approach caught several design issues early that would have required significant rework if discovered during implementation.

Here is a transparent log of AI involvement:

- **Plan mode / upfront design:** Claude Code's plan mode was used to draft the full model architecture (Staging → Intermediate → Marts DAG, grain decisions, schema.yml test strategy) before any files were created. The plan was reviewed and revised iteratively, with the majority of decisions made during this phase rather than mid-implementation.
- **Data model design — cancer type tracking:** An early draft of `dim_cancer_patients` collapsed each patient to a single `primary_cancer_type` (the type with the most claims). During the planning review it was identified that this loses information for the 35% of patients with multiple cancer types. The design was revised to add boolean flags (`has_breast_cancer`, `has_lung_cancer`, etc.) per cancer type while still surfacing a `primary_cancer_type` for easy grouping — a more complete representation that supports both use cases without adding downstream joins.
- **Cancer ICD-10 code groupings:** AI provided an initial grouping of C-code ranges to cancer types (e.g., C50 → Breast Cancer, C81-C88 → Lymphoma). These were verified against [icd10data.com](https://www.icd10data.com/) and adjusted — notably, C88 (malignant immunoproliferative diseases) was added to the Lymphoma bucket after review.
- **schema.yml scaffolding:** AI generated initial column descriptions and test definitions. Terminology `meta` links (e.g., `https://thetuvaproject.com/terminology/service-category`) were reviewed and corrected to match Tuva's actual documentation structure.
- **SQL review:** AI flagged a DuckDB incompatibility in `dim_cancer_patients.sql` where a window function (`FIRST_VALUE`) was used inside a GROUP BY aggregation. The fix was to split the primary type selection into a separate `primary_type` CTE.
- **Architecture decisions** (dimensional modeling, state separation, SCD discussion, data-driven spend buckets): These were reasoning-level contributions from AI that were evaluated and accepted as sound approaches for this use case.
