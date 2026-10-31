# Stage 2: Data Transformation Documentation

## Objective

The goal of Stage 2 is to transform the validated dataset handed over from the Extraction Stage into a cleaned, standardized, and business-ready dataset. The transformation process must be documented, traceable, and validated before the data proceeds to the Loading Stage.

This document describes the transformation process for the PrepAPig Batch Data Pipeline. The pipeline processes four farming-related datasets: `pig_batches`, `feed_records`, `vaccination_records`, and `expenses`. Authentication credentials, passwords, API keys, access tokens, and unrelated profile information are outside the transformation scope.


---

## 1. Transformation Input and Processing Specification

### 1.1 Input Source

Stage 2 receives the validated output from Extraction Stage. It must use the handover package and must not re-extract records from the PrepAPig source system.

| Item | Specification |
|---|---|
| Pipeline | PrepAPig Batch Data Pipeline |
| Input handover | Approved Extraction Stage extracted dataset, extraction metadata, and validation results |
| Input format | CSV files |
| Input files | `pig_batches.csv`, `feed_records.csv`, `vaccination_records.csv`, and `expenses.csv` |
| Extraction log | `extraction_log.csv` |
| Metadata received | Pipeline run ID, table name, extraction timestamps, extraction status, record counts, extraction window, validation result, error message, and extraction duration, where recorded |
| Expected handover status | `APPROVED`; only approved extraction output proceeds automatically |
| Extraction run ID | Read from the Extraction Stage metadata field `pipeline_run_id`; an example format is `PREPAPIG_20261004_230000` |
| Batch window | Use the `window_start` and `window_end` recorded in the extraction metadata for each table |
| Input location | The Extraction Stage output directory or configured handover location containing the CSV files and metadata. The exact deployed path is environment-specific and is not specified in the supplied documents. |

The extraction window follows the Extraction Stage incremental logic:

```text
window_start < created_at <= window_end
```

The Stage 2 process records the received extraction run ID and window in its transformation log so that transformed output can be traced back to the exact extraction handover.

### 1.2 Batch Processing Method

| Item | Specification |
|---|---|
| Transformation technology | Pure Python, consistent with the documented pipeline architecture |
| Scheduler | Node.js Cron triggers the Python ETL process |
| Frequency | Daily, at the predefined time configured in Node.js Cron |
| Batch size | All records in the four approved CSV files for the received extraction run and its recorded extraction window |
| Processing mode | Batch transformation |
| Failed batch handling | Log the failure, keep the input handover unchanged, and rerun the same extraction run after resolving the issue |
| Idempotency | The transformation should be designed to be idempotent: rerunning the same input run must not create duplicate transformed records. Use stable source IDs and replace or upsert the output for the same run rather than blindly appending duplicates. |
| Watermark handling | Stage 2 must not change the Extraction Stage extraction watermark. Extraction Stage updates that watermark only after its own successful extraction and handover. Stage 2 records its own success or failure independently. |

A batch should begin only after the input files and metadata are present and the Extraction Stage extraction is marked `APPROVED`. Before transforming, Stage 2 checks that all four expected files can be read, their expected columns are present, and their record counts agree with the handover metadata where those counts are available.

### 1.3 Transformation Scope

| Input table/file | Fields to clean or standardize | Derived fields or calculations | Output purpose |
|---|---|---|---|
| `pig_batches.csv` | `id`, `batch_code`, `breed`, `status`, `pig_count`, `start_weight`, `current_weight`, `date_acquired`, `created_at` | Optional calculated weight difference: `current_weight - start_weight`, only when both values are valid | Standardized pig batch master data |
| `feed_records.csv` | `id`, `batch_id`, `feed_type`, `quantity_kg`, `feeding_date`, `feeding_time`, `created_at` | Total feed quantity by batch for the processed batch window; calculate only after validating units and relationships | Clean feed records and batch-level feed summaries |
| `vaccination_records.csv` | `id`, `batch_id`, `vaccine_name`, `vaccination_date`, `next_due_date`, `administered_by`, `dosage`, `notes`, `status`, `created_at` | Optional due-status indicator based on `next_due_date` and the processing date, if approved as a business rule | Standardized vaccination history and schedule data |
| `expenses.csv` | `id`, `batch_id`, `expense_type`, `amount`, `expense_date`, `description`, `created_at` | Total expenses by batch and/or expense type for the processed data window | Clean expense records and expense summaries |
| All four files | Key fields, nulls, duplicates, whitespace, date and numeric formats, valid values, and foreign-key relationships | Record counts and rejected-record counts for audit | Validated transformed datasets and a transformation report |

The original identifiers (`id`, `batch_code`, and `batch_id`) must be preserved to maintain traceability and the relationships between pig batches and their related feed, vaccination, and expense records.

### 1.4 Dependencies and Assumptions

1. The Transformation Stage receives the four CSV files from an **approved** Extraction Stage handover; it does not query Supabase or re-extract source records.
2. `pig_batches` is processed first because it is the parent dataset for the other three tables.
3. `feed_records`, `vaccination_records`, and `expenses` are processed after the parent batch data is checked.
4. Child records must reference an existing `pig_batches.id` through `batch_id`. Orphan records are quarantined or held for review rather than silently joined to an unrelated batch.
5. The source schema and field names follow the Extraction Stage data contract. Unexpected missing columns or incompatible data types cause the batch to fail or be held for review.
6. The `created_at` field and Extraction Stage extraction window are used for lineage and handover checks. Business dates such as `feeding_date`, `vaccination_date`, and `expense_date` must not be substituted for the extraction watermark without an approved rule.
7. Late-arriving records created after the current extraction window are expected to appear in a later Extraction Stage batch. Stage 2 must retain the run and window metadata for each processed batch.
8. The supplied documents do not define a deployed input directory, exact daily clock time, actual run ID, or actual data-quality counts. These values must be taken from the run configuration and handover metadata rather than invented.
9. User credentials, passwords, API keys, access tokens, and unrelated user profile data remain excluded.

---

## 2. Data Profiling and Issue Identification

### 2.1 Profiling Procedure

Before applying cleaning rules, the Python transformation process profiles each input CSV and records:

- Total number of rows and number of columns.
- Missing or blank values by column.
- Duplicate values in primary keys and duplicate business identifiers such as `batch_code`.
- Duplicate full records, where applicable.
- Invalid or inconsistent numeric values.
- Invalid or mixed date/time formats.
- Leading/trailing whitespace and inconsistent text case.
- Unexpected category or status values.
- Invalid foreign-key references in `batch_id`.
- Record counts before transformation, after transformation, and in quarantine.
- Any schema mismatch or unreadable input file.

### 2.2 Data Quality Issue Register

The table below is a **profiling checklist**. The examples are illustrative values to help identify issues; they are not confirmed observations from the supplied files. Replace “To be measured” with the actual count and percentage after profiling.

| Table/file | Column affected | Issue type | Example | Affected records | Planned treatment |
|---|---|---|---|---|---|
| `pig_batches.csv` | `id` | Missing or duplicate primary key | Blank ID; repeated UUID | To be measured | Reject or quarantine affected records; see C-01 |
| `pig_batches.csv` | `batch_code` | Missing, duplicate, or extra whitespace | ` BATCH-001 `; repeated `BATCH-001` | To be measured | Trim whitespace; investigate duplicates; see C-02 and C-03 |
| `pig_batches.csv` | `pig_count` | Missing, non-integer, or invalid value | Blank; `five`; `-3` | To be measured | Validate as a non-negative integer; quarantine invalid records; see C-04 |
| `pig_batches.csv` | `start_weight`, `current_weight` | Invalid numeric value or implausible negative value | `abc`; `-12.50` | To be measured | Convert to decimal and quarantine invalid or negative weights; see C-04 |
| `pig_batches.csv` | `date_acquired`, `created_at` | Invalid or inconsistent date format | `10/04/2026`; malformed timestamp | To be measured | Parse and standardize valid values; quarantine unparseable required dates; see S-01 |
| `pig_batches.csv` | `breed`, `status` | Whitespace or inconsistent text case | ` landrace `; `active`, `Active` | To be measured | Trim text and normalize known category values; see S-02 |
| `feed_records.csv` | `id` | Missing or duplicate primary key | Blank ID; repeated UUID | To be measured | Reject or quarantine affected records; see C-01 |
| `feed_records.csv` | `batch_id` | Missing or orphan foreign key | Blank ID; UUID absent from `pig_batches.id` | To be measured | Quarantine record and report missing parent; see R-01 |
| `feed_records.csv` | `feed_type` | Missing value or inconsistent text | Blank; ` Starter `; `starter` | To be measured | Trim and normalize known labels; do not invent missing feed type; see C-05 and S-02 |
| `feed_records.csv` | `quantity_kg` | Invalid or negative quantity | `abc`; `-2.00` | To be measured | Validate decimal and non-negative quantity; quarantine invalid records; see C-04 |
| `feed_records.csv` | `feeding_date`, `feeding_time` | Invalid or inconsistent date/time | `04-10-2026`; `25:90` | To be measured | Standardize valid values and quarantine unparseable values; see S-01 |
| `vaccination_records.csv` | `id` | Missing or duplicate primary key | Blank ID; repeated UUID | To be measured | Reject or quarantine affected records; see C-01 |
| `vaccination_records.csv` | `batch_id` | Missing or orphan foreign key | Blank ID; UUID absent from `pig_batches.id` | To be measured | Quarantine record and report missing parent; see R-01 |
| `vaccination_records.csv` | `vaccine_name` | Missing value or inconsistent text | Blank; ` vaccine A ` | To be measured | Trim known text; quarantine if required vaccine name is missing; see C-05 and S-02 |
| `vaccination_records.csv` | `vaccination_date`, `next_due_date` | Invalid date or inconsistent format | `31/31/2026`; mixed date formats | To be measured | Standardize valid dates; quarantine invalid required dates; see S-01 |
| `vaccination_records.csv` | `status` | Inconsistent or unexpected status | `completed`; `DONE`; `unknown` | To be measured | Map only approved equivalent values; hold unknown values for review; see S-02 |
| `expenses.csv` | `id` | Missing or duplicate primary key | Blank ID; repeated UUID | To be measured | Reject or quarantine affected records; see C-01 |
| `expenses.csv` | `batch_id` | Missing or orphan foreign key | Blank ID; UUID absent from `pig_batches.id` | To be measured | Quarantine record and report missing parent; see R-01 |
| `expenses.csv` | `expense_type` | Missing value or inconsistent text | Blank; ` Feed `; `feed` | To be measured | Trim and normalize approved labels; see C-05 and S-02 |
| `expenses.csv` | `amount` | Invalid, negative, or non-numeric amount | `abc`; `-25.00` | To be measured | Validate decimal; quarantine negative or invalid amounts unless an approved rule permits refunds/adjustments; see C-04 |
| `expenses.csv` | `expense_date`, `created_at` | Invalid or inconsistent date format | `10/04/2026`; malformed timestamp | To be measured | Standardize valid values; quarantine unparseable required dates; see S-01 |
| All files | Entire record | Exact duplicate rows | Same record repeated in the file | To be measured | Remove exact duplicate copies only after logging them; preserve one record by stable ID; see C-03 |

### 2.3 Profiling Results Status

No actual CSV contents or profiling output were included in the supplied reference materials. Therefore, this document does not claim that any issue above has been found, and it does not invent affected-record counts or percentages. The transformation job must produce the actual profile report for each run before the cleaning results are approved.

---

## 3. Data Cleaning Rules

The following rules define how invalid or inconsistent data will be treated. The team should confirm business-specific thresholds and category mappings before production use.

| Rule ID | Target | Rule | Action |
|---|---|---|---|
| C-01 | `id` in all tables | Primary key must be present and unique within its table | Quarantine records with missing IDs. For duplicate IDs, stop or hold the affected dataset for investigation; do not arbitrarily choose a conflicting record. |
| C-02 | `batch_code` | Required business identifier must not be blank after trimming | Trim leading/trailing whitespace. Quarantine blank codes. |
| C-03 | All tables | Detect duplicate rows and repeated identifiers | Log exact duplicate rows and retain one copy only when the stable ID and all values match. Conflicting records with the same ID are held for review. |
| C-04 | `pig_count`, weights, `quantity_kg`, `amount` | Numeric fields must parse to their documented numeric types and satisfy basic validity checks | Convert valid values to integer/decimal. Quarantine non-numeric values, negative pig counts, negative weights, and negative feed quantities. Negative expenses are quarantined unless an approved business rule defines refunds or adjustments. |
| C-05 | Required fields | Required fields must not be null or blank | Do not fabricate missing values for `batch_code`, `feed_type`, `quantity_kg`, `feeding_date`, `vaccine_name`, `vaccination_date`, `expense_type`, or `amount`. Quarantine records missing required values. |
| C-06 | All tables | Preserve original primary and foreign keys | Do not generate replacement IDs for invalid source records. Keep source IDs for traceability. |
| C-07 | All tables | Every record must meet the minimum structural requirements of its table | Records missing required columns or with an incompatible schema cause the batch to fail or be held for review. |

**Quarantine:** A quarantined record is excluded from the business-ready output until it is corrected or explicitly approved. The transformation log should include the source file, source record ID if available, rule ID, reason, and processing run ID.

---

## 4. Standardization and Data Type Rules

| Rule ID | Field(s) | Standardization rule | Example |
|---|---|---|---|
| S-01 | `date_acquired`, `feeding_date`, `vaccination_date`, `next_due_date`, `expense_date`, `created_at` | Parse valid dates/timestamps and output dates as `YYYY-MM-DD`; output timestamps in a consistent ISO 8601 format. Preserve timezone meaning and do not guess ambiguous dates. | `2026-10-04` |
| S-02 | `breed`, `status`, `feed_type`, `vaccine_name`, `expense_type`, `administered_by` | Trim leading/trailing whitespace. Normalize case or category spelling only using an approved mapping; preserve meaningful names. | ` Active ` → `Active` |
| S-03 | `batch_code` | Trim surrounding whitespace while preserving the identifier's intended characters and uniqueness. | ` BATCH-001 ` → `BATCH-001` |
| S-04 | `pig_count` | Store as integer; retain valid whole-number values only. | `25` → `25` |
| S-05 | `start_weight`, `current_weight`, `quantity_kg`, `amount` | Store as decimal values with the documented two-decimal precision where appropriate. Do not silently round values beyond the approved precision. | `12.5` → `12.50` |
| S-06 | `feeding_time` | Standardize valid time values to `HH:MM` (or `HH:MM:SS` if seconds are provided consistently); quarantine values that cannot be parsed. | `7:30 AM` → `07:30`, if parsing is unambiguous |
| S-07 | All fields | Preserve snake_case column names and the expected table schemas from Extraction Stage. | `quantity_kg` |

Unknown status values or unrecognized category labels must be flagged for review rather than silently mapped to a guessed value. Optional fields such as `breed`, `notes`, `description`, and `administered_by` may remain null when the source allows them to be optional.

---

## 5. Business Transformation Rules

Derived values are created only after the source fields pass validation. The following are proposed transformations consistent with the pipeline's reporting and analytics purpose.

| Rule ID | Output / calculation | Input | Rule |
|---|---|---|---|
| D-01 | `weight_change` (optional) | `current_weight`, `start_weight` | Calculate `current_weight - start_weight` only when both weights are valid. Do not substitute missing weights with zero. |
| D-02 | Total feed quantity by pig batch | `feed_records.batch_id`, `quantity_kg` | Sum valid `quantity_kg` values by `batch_id` for the defined reporting window. Keep the summary's window and run ID. |
| D-03 | Total expenses by pig batch | `expenses.batch_id`, `amount` | Sum valid expense amounts by `batch_id` for the defined reporting window. Keep the summary's window and run ID. |
| D-04 | Expense total by category (optional) | `expense_type`, `amount` | Sum valid amounts by standardized `expense_type` for the defined reporting window. |
| D-05 | Vaccination due indicator (optional) | `next_due_date`, processing date | If approved by the team, derive a status such as `Overdue`, `Due`, or `Upcoming` using a documented date threshold. Do not overwrite the source `status`; store the derived indicator separately. |

Aggregations must be calculated from validated records only. The transformation output must distinguish detail-level records from summary-level outputs so that downstream users do not accidentally count both as the same type of data.

---

## 6. Relationship and Referential Integrity Rules

The documented parent-child structure is:

```text
pig_batches (1)
    ├── feed_records (many)
    ├── vaccination_records (many)
    └── expenses (many)
```

| Rule ID | Check | Required treatment |
|---|---|---|
| R-01 | Each child table's `batch_id` must match a valid `pig_batches.id` | Quarantine orphan records and log the missing parent reference. Do not assign a different batch ID automatically. |
| R-02 | `pig_batches.id` and all child-table `id` values must be unique within their respective tables | Hold the affected dataset if conflicting IDs are found. |
| R-03 | `pig_batches.batch_code` must be unique after trimming whitespace | If trimming creates a collision, hold the conflicting records for review. |
| R-04 | Parent batch data must be validated before child records are joined or aggregated | Process `pig_batches` first, then validate `feed_records`, `vaccination_records`, and `expenses`. |

---

## 7. Transformation Validation and Acceptance Criteria

Stage 2 validates the transformed output before handing it to the Loading Stage.

| Validation check | Acceptance criteria | Action if failed |
|---|---|---|
| Input handover status | Extraction Stage handover is `APPROVED` | Stop processing and request an approved handover |
| Input files | All four CSV files and required metadata are available and readable | Mark the batch failed; do not load partial outputs |
| Schema validation | Required columns match the Extraction Stage data contract | Stop or hold for schema review |
| Primary key validation | No missing or conflicting duplicate IDs in accepted output | Quarantine invalid records or fail the affected dataset |
| Business identifier validation | `batch_code` is present and unique after standardization | Hold conflicting records for review |
| Numeric validation | Accepted numeric fields are valid and meet the rules in Section 3 | Quarantine invalid records and record the reason |
| Date/time validation | Accepted dates and timestamps are parseable and standardized | Quarantine invalid required values |
| Referential integrity | Every accepted child record refers to an accepted `pig_batches.id` | Quarantine orphan records |
| Count reconciliation | Input count = accepted output count + quarantined/rejected count, after accounting for logged exact-duplicate handling | Fail validation if the reconciliation does not balance |
| Aggregation validation | Summary totals equal calculations from the corresponding accepted detail records | Recalculate and investigate mismatches |
| Idempotency check | Rerunning the same extraction run does not create duplicate output | Replace/upsert that run's output and verify unique source IDs |
| Audit/log validation | Run ID, input window, input/output counts, rule outcomes, and errors are recorded | Hold handover until traceability metadata is complete |

### Acceptance Status

- **PASS:** Required validations succeed and output is ready for handover.
- **WARNING / MANUAL REVIEW:** A non-critical issue requires review before the affected data is approved.
- **FAIL:** A critical schema, identity, completeness, or processing error prevents safe handover.

A dataset with quarantined records may proceed only if the project team approves partial acceptance, the rejected records are documented, and all accepted records pass the required validation checks. Otherwise, the batch remains on hold.

---

## 8. Transformation Metadata and Logging

Stage 2 must produce a transformation log for each run. The log links the input handover to the transformed output and records decisions made during processing.

### 8.1 Required Metadata Fields

| Metadata field | Purpose / example |
|---|---|
| `transformation_run_id` | Unique identifier for the transformation execution |
| `pipeline_run_id` | Extraction Stage extraction run ID, such as `PREPAPIG_20261004_230000` |
| `table_name` | `pig_batches`, `feed_records`, `vaccination_records`, or `expenses` |
| `window_start` | Extraction window start from Extraction Stage metadata |
| `window_end` | Extraction window end from Extraction Stage metadata |
| `transformation_start` | Date/time transformation begins |
| `transformation_end` | Date/time transformation finishes |
| `transformation_status` | `PASS`, `WARNING`, or `FAIL` |
| `input_records` | Number of rows read from the input file |
| `output_records` | Number of accepted transformed detail rows |
| `quarantined_records` | Number of rows excluded for data-quality reasons |
| `duplicates_removed` | Number of exact duplicate copies removed under the documented rule |
| `rules_applied` | Cleaning, standardization, relationship, and derivation rule IDs applied |
| `output_location` | Configured path or destination for the transformed output |
| `error_message` | Safe diagnostic message; must not contain credentials or secrets |
| `transformation_duration` | Elapsed processing time |

### 8.2 Logging and Traceability

1. Read the Extraction Stage `pipeline_run_id`, table names, extraction window, validation result, and record counts.
2. Assign a unique `transformation_run_id`.
3. Record input counts before processing.
4. Record each rule applied and the number of records affected.
5. Record accepted, quarantined, duplicate, and output counts.
6. Record the output location, status, timestamps, and safe error details.
7. Keep the Extraction Stage extraction metadata unchanged and link it to the Stage 2 log using `pipeline_run_id`.
8. Never write passwords, API keys, access tokens, or database connection secrets to transformation logs.

### 8.3 Suggested Output Naming

The exact deployed output directory is not specified in the reference documents. A suggested organization is:

```text
PrepAPig-ETL/
├── input/
│   └── extracted_data/
│       ├── pig_batches.csv
│       ├── feed_records.csv
│       ├── vaccination_records.csv
│       ├── expenses.csv
│       └── extraction_log.csv
├── output/
│   └── transformed_data/
│       ├── pig_batches_transformed.csv
│       ├── feed_records_transformed.csv
│       ├── vaccination_records_transformed.csv
│       ├── expenses_transformed.csv
│       ├── feed_summary.csv
│       └── expense_summary.csv
└── logs/
    └── transformation/
        ├── transformation_log.csv
        └── data_quality_report.csv
```

These names are proposed conventions and should be aligned with the actual Python ETL project configuration.

---

## 9. Failure Handling, Recovery, and Idempotency

1. **Missing or unapproved handover:** Stop the transformation and record the missing file or non-approved extraction status.
2. **Schema mismatch:** Do not guess column meanings or silently drop required columns. Record the mismatch and request review.
3. **Temporary processing failure:** Record the error and rerun the same input handover after the cause is corrected.
4. **Invalid records:** Quarantine affected records with their source ID, file name, rule ID, and reason.
5. **Conflicting duplicate IDs:** Hold the affected dataset for investigation rather than arbitrarily selecting a record.
6. **Partial output:** Write to a temporary output location first. Publish the transformed files only after validation succeeds, so an interrupted run is not mistaken for a complete output.
7. **Safe rerun:** Use the same `pipeline_run_id` and stable source IDs to identify records. Replace or upsert output associated with that run instead of appending another copy.
8. **Independent checkpoints:** Do not advance or modify the Extraction Stage extraction watermark from Stage 2. The transformation log records its own completion status.
9. **Recovery verification:** After rerunning, compare input, accepted, quarantined, and output counts and repeat the validation checks before marking the run as `PASS`.

The same approved input and rule configuration should produce the same transformed detail records and summary results. Any changes to transformation rules should be versioned or recorded in the log so that results can be reproduced.

---

## 10. Handover to the Loading Stage

The Transformation Stage hands over only validated, documented outputs to the Loading Stage.

| Handover item | Description |
|---|---|
| Transformed pig batches | Cleaned and standardized `pig_batches` records |
| Transformed feed records | Cleaned feed details with valid batch relationships |
| Transformed vaccination records | Cleaned vaccination details with standardized dates and values |
| Transformed expenses | Cleaned expense details with valid batch relationships |
| Summary outputs | Approved feed and expense summaries, if generated |
| Data-quality report | Profiling findings, counts, applied rules, and quarantined-record reasons |
| Transformation log | Run IDs, extraction window, counts, rules applied, status, output location, and errors |
| Acceptance status | `PASS`, or approved `WARNING / MANUAL REVIEW` according to project policy |

Before handover, confirm that the output schema is documented, all required validation checks have been completed, counts reconcile, and each output can be traced to the Extraction Stage `pipeline_run_id` and extraction window.

---


## 3. Data Cleaning Rules

### 3.1 Purpose

Data cleaning removes or isolates invalid, incomplete, duplicated, or inconsistent records before they are used for business calculations and reporting. The four datasets covered by this stage are `pig_batches`, `feed_records`, `vaccination_records`, and `expenses`.

Cleaning is performed only on the approved CSV handover received from the Extraction Stage. The original source identifiers are preserved. No record is silently deleted: every rejected or quarantined record is counted and recorded in the data-quality report and transformation log.

A **quarantined record** is excluded from the business-ready output until it has been corrected or approved for use. The quarantine entry records the source file, source record ID when available, rule ID, reason, and transformation run ID.

### 3.2 Cleaning Rule Register

| Rule ID and name | Target | Condition | Action | Justification |
|---|---|---|---|---|
| **C-01 — Missing Primary Key** | `id` in all four tables | The primary key is null, blank, or cannot be interpreted as a valid UUID. | Quarantine the affected record. Do not generate a replacement ID. | The source ID is needed to identify and trace each record reliably. |
| **C-02 — Duplicate Primary Key** | `id` in all four tables | The same ID occurs more than once in a table. | If the rows are exact duplicates (same ID and all field values), retain one copy and log the extra copies. If values conflict for the same ID, hold the affected dataset for investigation; do not choose a record arbitrarily. | Prevents duplicate or conflicting records from being treated as separate facts. |
| **C-03 — Duplicate Batch Code** | `pig_batches.batch_code` | A batch code is blank after trimming, or two records have the same batch code after trimming. | Trim surrounding whitespace. Quarantine blank values. Hold collisions for review rather than merging batches. | `batch_code` is a unique business identifier in the source schema. |
| **C-04 — Missing Required Values** | Required columns: `pig_batches.batch_code`, `pig_batches.pig_count`, `feed_records.batch_id`, `feed_records.feed_type`, `feed_records.quantity_kg`, `feed_records.feeding_date`, `vaccination_records.batch_id`, `vaccination_records.vaccine_name`, `vaccination_records.vaccination_date`, `expenses.batch_id`, `expenses.expense_type`, and `expenses.amount` / `expenses.expense_date` | A required value is null or blank. | Quarantine the affected record. Do not fill missing business values with guessed defaults. | Avoids inventing batch, feed, vaccination, or expense information. |
| **C-05 — Invalid Numeric Values** | `pig_batches.pig_count`, `pig_batches.start_weight`, `pig_batches.current_weight`, `feed_records.quantity_kg`, `expenses.amount` | A value cannot be converted to its expected numeric type, is outside the basic permitted range, or has more precision than the approved output precision. | Require `pig_count` to be a non-negative whole number. Require weights and feed quantity to be non-negative decimals. Quarantine non-numeric or negative values. For `expenses.amount`, quarantine negative values unless the project team approves a documented refund or adjustment rule. Do not silently round excess precision. | Prevents invalid counts, weights, quantities, and expense values from distorting reports. |
| **C-06 — Whitespace and Invalid Text** | Text fields in all four tables, including `batch_code`, `breed`, `status`, `feed_type`, `vaccine_name`, `administered_by`, `expense_type`, `notes`, and `description` | A text value contains leading or trailing whitespace or consists only of whitespace. | Trim leading and trailing whitespace. Treat whitespace-only required values as missing and quarantine the record under C-04. Preserve meaningful internal spaces and punctuation. | Prevents false mismatches while preserving the original meaning of names and descriptions. |
| **C-07 — Invalid Date or Time** | `date_acquired`, `feeding_date`, `feeding_time`, `vaccination_date`, `next_due_date`, `expense_date`, and `created_at` | A date or time cannot be parsed unambiguously, or a required date is missing. | Quarantine records with invalid required dates or times. An invalid optional date such as `next_due_date` may remain null only if the source contract permits it; record the issue. Do not guess between ambiguous date formats. | Keeps time-based reports accurate and prevents incorrect scheduling information. |
| **C-08 — Orphan Child Record** | `batch_id` in `feed_records`, `vaccination_records`, and `expenses` | `batch_id` is missing or does not match an accepted `pig_batches.id`. | Quarantine the child record and log the unmatched `batch_id`. Do not assign it to another batch. | Maintains the parent-child relationship and prevents unrelated records from being joined. |
| **C-09 — Unexpected Category or Status** | `pig_batches.status`, `vaccination_records.status`, `breed`, `feed_type`, `vaccine_name`, and `expense_type` | A value does not match an approved reference value or mapping. | Keep the original value for review or quarantine the affected record if the value prevents valid processing. Do not silently convert it to a guessed category. | Preserves source meaning and avoids misleading category-based analysis. |
| **C-10 — Schema or Required-Column Failure** | All four input files | A file is unreadable, a required column is absent, or the received structure is incompatible with the documented schema. | Stop or hold the batch. Record the file and schema error. Do not silently drop columns or continue with partial input. | Ensures that the transformation uses the agreed Extraction Stage data contract. |

### 3.3 Missing and Null Value Policy

The source schema identifies required fields through `NOT NULL` constraints. The transformation stage applies the following policy:

- **Required fields:** Missing or blank values are not replaced with invented values. The affected record is quarantined.
- **Optional fields:** Fields such as `breed`, `start_weight`, `current_weight`, `date_acquired`, `next_due_date`, `administered_by`, `dosage`, `notes`, and `description` may remain null where permitted by the source schema.
- **Default values:** Source defaults such as `pig_batches.status = 'Active'` and `vaccination_records.status = 'Completed'` are not re-applied to missing extracted values unless the Extraction Stage contract confirms that the source database supplied the default. Stage 2 must not assume a default that is not present in the handover.
- **No fabricated data:** Missing quantities, amounts, dates, IDs, and relationships must not be filled with zero, the current date, or a generated identifier.

### 3.4 Duplicate Detection and Record Retention

Duplicate checks are performed separately for each table:

1. Compare primary key `id` values within the same table.
2. If repeated IDs have identical values in every column, retain one copy and count the remaining copies as exact duplicates removed.
3. If repeated IDs have different values, hold the affected dataset for investigation. No “latest record” rule is applied because the documented schema does not define an `updated_at` column.
4. Check `pig_batches.batch_code` for uniqueness after whitespace trimming. If trimming creates a collision, hold the conflicting batches for review.
5. Record duplicate counts and the retention decision in the data-quality report.

### 3.5 Cleaning Output and Audit Requirements

For every input table, the cleaning process records:

- Input row count.
- Accepted row count.
- Quarantined row count.
- Exact duplicate copies removed.
- Rule IDs applied and number of affected records.
- Source record ID and rejection reason for each quarantined record, where an ID is available.

The reconciliation must account for every input row:

`input_records = accepted_records + quarantined_records + exact_duplicate_copies_removed`

If the counts do not reconcile, the batch must not be approved for loading.

---

## 4. Standardization and Data Type Rules

### 4.1 Purpose

Standardization makes values consistent across the four datasets while retaining the field names, identifiers, and meanings defined in the Lab 2 technical metadata. The output keeps the source table structure and uses the documented PostgreSQL-compatible data types.

### 4.2 Data Type and Format Rules

| Rule ID | Table and column | Source / expected type | Standard output type and format | Conversion and failure handling |
|---|---|---|---|---|
| **S-01** | `id` and `batch_id` in all applicable tables | UUID | UUID, preserving the original value | Validate UUID format without changing the identifier. Quarantine invalid or missing IDs; see C-01 and C-08. |
| **S-02** | `batch_code` | `VARCHAR(50)` | String, maximum 50 characters; trimmed | Preserve intended characters and case. Quarantine blank values and hold uniqueness collisions. |
| **S-03** | `pig_count` | `INTEGER` | Integer | Convert only valid whole-number values. Reject decimals that are not whole numbers, text values, and negative counts. |
| **S-04** | `start_weight`, `current_weight` | `DECIMAL(10,2)` | Decimal with two fractional digits | Convert valid numeric strings to decimal. Reject non-numeric and negative values. Do not silently round excess precision. |
| **S-05** | `quantity_kg` | `DECIMAL(10,2)` | Decimal with two fractional digits, in kilograms | Convert valid values to decimal. Reject non-numeric or negative quantities. Preserve the documented kilogram unit. |
| **S-06** | `amount` | `DECIMAL(10,2)` | Decimal with two fractional digits | Convert valid numeric values to decimal. The source documentation does not specify a currency, so Stage 2 must not assign or convert a currency. Quarantine invalid values and negative values unless an approved adjustment rule exists. |
| **S-07** | `date_acquired`, `feeding_date`, `vaccination_date`, `next_due_date`, `expense_date` | `DATE` | `YYYY-MM-DD` | Parse valid dates and write in ISO date format. Quarantine invalid required dates. Preserve null for optional dates when allowed. |
| **S-08** | `created_at` | `TIMESTAMP` | Consistent ISO 8601 timestamp representation | Parse valid timestamps and preserve their timezone meaning. Do not assume a timezone or convert local time to UTC unless the source metadata defines it. |
| **S-09** | `feeding_time` | `VARCHAR(20)` | Consistent `HH:MM` representation; use `HH:MM:SS` only when seconds are consistently supplied | Parse only unambiguous time values. Quarantine invalid values. |
| **S-10** | `breed`, `status`, `feed_type`, `vaccine_name`, `administered_by`, `expense_type` | `VARCHAR` fields as defined in the source schema | Trimmed text, preserving meaningful spelling and names | Apply case or spelling mappings only when an approved reference mapping exists. Flag unknown values for review. |
| **S-11** | `notes`, `description` | `TEXT` | Trimmed text; null allowed | Preserve the content and meaningful punctuation. Do not remove meaningful words or replace null with invented descriptions. |
| **S-12** | All columns | Source snake_case names | Retain the exact documented snake_case column names | Do not rename or silently remove source columns. Any schema change requires approval and documentation. |

### 4.3 Text and Reference Value Rules

Text standardization follows these principles:

- Remove leading and trailing whitespace from text fields.
- Preserve internal spaces, punctuation, and meaningful names.
- Normalize known status or category variants only through a mapping approved by the project team.
- Do not automatically convert all names to uppercase or title case because this may change meaningful names.
- Keep unrecognized values visible in the quality report for review.

The technical metadata documents default values of `Active` for `pig_batches.status` and `Completed` for `vaccination_records.status`. These are source defaults, not permission to replace missing values during transformation.

**Reference mapping status:** The supplied technical metadata does not provide an approved list of breed, feed type, vaccine, expense type, or status aliases. Therefore, no specific category mapping is assumed in this document. The team must approve a mapping table before such conversions are applied.

### 4.4 Numeric Precision, Units, and Currency

- `start_weight`, `current_weight`, `quantity_kg`, and `amount` use the documented `DECIMAL(10,2)` precision.
- `pig_count` remains an integer.
- Feed quantity remains measured in kilograms (`kg`).
- The source documentation does not identify the currency for `expenses.amount`; the transformation stage preserves the numeric amount without assigning a currency symbol or performing currency conversion.
- Values with excess decimal precision must be reviewed or rejected according to the approved precision policy. They must not be silently rounded.

### 4.5 Conversion Failure Handling

If a value cannot be converted to its target type:

1. Do not replace it with a default or guessed value.
2. Quarantine the affected record when the field is required or prevents reliable processing.
3. Record the table, column, source ID if available, rule ID, original value where safe, and conversion error.
4. If the problem affects the file schema or prevents reliable interpretation of the dataset, hold the batch for review.
5. Include the affected count in the data-quality report and transformation log.

---

## 5. Business Transformation Rules

### 5.1 Purpose

Business transformations convert the cleaned and standardized farming records into useful information for reporting and analytics. Calculations and summaries are performed only on accepted records after primary keys, numeric values, dates, and parent-child relationships have been validated.

The original source IDs are retained. Summary outputs are kept separate from detail-level records to prevent downstream users from counting the same information twice.

### 5.2 Business Rule Register

| Rule ID | Input columns | Logic or formula | Output column / dataset | Business reason |
|---|---|---|---|---|
| **B-01 — Batch Weight Change** | `pig_batches.start_weight`, `pig_batches.current_weight` | Calculate `current_weight - start_weight` only when both values are valid and non-null. Do not substitute missing weights with zero. | Optional `weight_change` in the transformed pig batch output | Shows the recorded difference between the batch's initial and current weight. |
| **B-02 — Total Feed per Batch** | `feed_records.batch_id`, `feed_records.quantity_kg` | Group accepted feed records by `batch_id` and sum `quantity_kg` within the defined extraction/reporting window. | `feed_summary.total_feed_kg`, grouped by `batch_id` | Helps measure the feed quantity recorded for each pig batch during the reporting window. |
| **B-03 — Total Expenses per Batch** | `expenses.batch_id`, `expenses.amount` | Group accepted expense records by `batch_id` and sum `amount` within the defined extraction/reporting window. | `expense_summary.total_expense`, grouped by `batch_id` | Provides a batch-level view of recorded expenses. |
| **B-04 — Expenses by Type** | `expenses.expense_type`, `expenses.amount` | Group accepted expense records by standardized `expense_type` and sum `amount` within the reporting window. | Optional `expense_type_summary.total_amount` | Supports comparison of spending across expense categories. |
| **B-05 — Vaccination Due Indicator** | `vaccination_records.next_due_date`, processing date | If approved by the project team, compare `next_due_date` with the processing date and assign a separate indicator such as `Overdue`, `Due`, or `Upcoming` using a documented threshold. | Optional `vaccination_due_status` | Helps identify vaccination records that may require attention. It must not overwrite the source `status`. |
| **B-06 — Batch Relationship Lookup** | `pig_batches.id`; `feed_records.batch_id`; `vaccination_records.batch_id`; `expenses.batch_id` | Match each child table's `batch_id` to `pig_batches.id`. Use the validated parent dataset as the lookup source. | Validated child records with preserved `batch_id`; optional reporting join | Connects feed, vaccination, and expense activity to the correct pig batch. |
| **B-07 — Reporting Window Attribution** | `created_at`, `window_start`, `window_end`, `pipeline_run_id` | Retain the extraction run ID and its recorded window with the transformed output and summaries. Do not use business dates as a substitute for the extraction watermark. | Run and window metadata associated with each output | Makes each result traceable to the exact extraction batch and supports reproducible reporting. |

### 5.3 Joins and Lookup Processing

The source schema defines one parent table and three child tables:

```text
pig_batches (1)
    ├── feed_records (many)
    ├── vaccination_records (many)
    └── expenses (many)
```

The transformation stage applies the following join and lookup rules:

| Parent table | Child table | Join key | Join approach | Handling unmatched records |
|---|---|---|---|---|
| `pig_batches` | `feed_records` | `pig_batches.id = feed_records.batch_id` | Validate the child foreign key against the accepted parent IDs before reporting joins. | Quarantine the feed record under R-01. |
| `pig_batches` | `vaccination_records` | `pig_batches.id = vaccination_records.batch_id` | Validate the child foreign key against the accepted parent IDs before reporting joins. | Quarantine the vaccination record under R-01. |
| `pig_batches` | `expenses` | `pig_batches.id = expenses.batch_id` | Validate the child foreign key against the accepted parent IDs before reporting joins. | Quarantine the expense record under R-01. |

`pig_batches` is processed first. Child records are checked against the accepted parent IDs before they are joined or aggregated. The transformation must not assign an unmatched child record to a different batch.

For detail-level outputs, retain the original child rows and their `batch_id`. If a combined reporting view is created, use the validated relationship and keep the detail and summary datasets clearly separated.

### 5.4 Aggregation Rules

| Summary dataset | Grouping level | Measure | Aggregation | Window |
|---|---|---|---|---|
| `feed_summary.csv` | `batch_id` | `quantity_kg` | Sum | Defined extraction/reporting window |
| `expense_summary.csv` | `batch_id` | `amount` | Sum | Defined extraction/reporting window |
| Optional expense-by-type summary | `expense_type` | `amount` | Sum | Defined extraction/reporting window |

Only accepted, validated detail records are included in the summaries. Each summary must retain or be associated with `pipeline_run_id`, `window_start`, and `window_end`. Summary totals must be checked against the accepted detail records before handover.

### 5.5 Filtering and Conditional Logic

The Extraction Stage applies the technical extraction window using the documented `created_at` boundary:

```text
window_start < created_at <= window_end
```

Stage 2 uses the received handover and does not re-extract records or change that watermark. Within Stage 2:

- Records failing cleaning or relationship rules are quarantined and counted.
- Exact duplicate copies are removed only under the documented duplicate rule and are counted.
- No business-status exclusion, such as excluding inactive pig batches or completed vaccination records, is applied unless the project team defines and approves that business rule.
- No cancellation or refund category is assumed because the supplied schema and technical metadata do not define such business statuses.
- Optional vaccination due indicators are generated only after the team approves the status definitions and date thresholds.

This approach ensures that business exclusions are explicit, reviewable, and reported rather than silently applied.

### 5.6 Dimensional Modeling

The supplied Lab 2 technical metadata describes operational farming tables and their relationships. It does not define a dimensional model, fact tables, dimension tables, surrogate keys, or Slowly Changing Dimension (SCD) requirements.

Therefore, Stage 2 will:

- Preserve the source UUID primary keys and foreign keys.
- Keep the four transformed detail datasets aligned with the documented source schema.
- Generate separate summary datasets only where required for reporting.
- Avoid creating surrogate keys or applying SCD Type 1 or Type 2 without an approved downstream data-model design.

### 5.7 Business Transformation Validation

Before the transformed datasets are handed to the Loading Stage, verify that:

1. `weight_change` is calculated only when both source weights are valid.
2. Feed and expense summaries include only accepted detail records.
3. Summary totals match the corresponding detail-level calculations for the same reporting window.
4. Each child record included in a join has a valid `batch_id` matching an accepted `pig_batches.id`.
5. The source IDs and relationships remain unchanged.
6. The extraction run ID and extraction window are retained for traceability.
7. Optional business rules, including vaccination due status or category mappings, have documented approval before being applied.

# 6. Transformed Data Contract and Naming Convention

## 6.1 Purpose

The transformed data contract defines the expected structure, data types, naming conventions, and source-to-target mapping of the business-ready PrepAPig outputs.

This contract is applied after data cleaning, standardization, and business transformation. It defines what the **Loading Stage** should receive from the Transformation Stage.

The contract is based on the documented PrepAPig source tables:

- `pig_batches`
- `feed_records`
- `vaccination_records`
- `expenses`

> **Scope:** This contract describes the transformed output. It is not a repetition of the Extraction Contract.

---

## 6.2 Required Output Schema

### 6.2.1 Transformed Pig Batch Dataset

| Target Column | Data Type | Nullable | Key / Constraint | Source / Derivation |
|---|---|---:|---|---|
| `id` | UUID | No | Primary Key | `pig_batches.id` |
| `batch_code` | VARCHAR(50) | No | Unique business identifier | `pig_batches.batch_code` |
| `pig_count` | INTEGER | No | Positive value | `pig_batches.pig_count` |
| `breed` | VARCHAR(100) | Yes | — | `pig_batches.breed` |
| `status` | VARCHAR(30) | Yes | — | `pig_batches.status` |
| `start_weight` | DECIMAL(10,2) | Yes | Non-negative | `pig_batches.start_weight` |
| `current_weight` | DECIMAL(10,2) | Yes | Non-negative | `pig_batches.current_weight` |
| `date_acquired` | DATE | Yes | Valid date | `pig_batches.date_acquired` |
| `created_at` | TIMESTAMP | No | Lineage timestamp | `pig_batches.created_at` |
| `weight_change` | DECIMAL(10,2) | Yes | Derived | `current_weight - start_weight` |

---

### 6.2.2 Transformed Feed Dataset

| Target Column | Data Type | Nullable | Key / Constraint | Source / Derivation |
|---|---|---:|---|---|
| `id` | UUID | No | Primary Key | `feed_records.id` |
| `batch_id` | UUID | No | Foreign Key → `pig_batches.id` | `feed_records.batch_id` |
| `feed_type` | VARCHAR(100) | No | Required text | `feed_records.feed_type` |
| `quantity_kg` | DECIMAL(10,2) | No | Positive value | `feed_records.quantity_kg` |
| `feeding_date` | DATE | No | Valid date | `feed_records.feeding_date` |
| `feeding_time` | TIME | Yes | Valid time | `feed_records.feeding_time` |
| `created_at` | TIMESTAMP | No | Lineage timestamp | `feed_records.created_at` |

---

### 6.2.3 Feed Summary Dataset

| Target Column | Data Type | Nullable | Key / Constraint | Source / Derivation |
|---|---|---:|---|---|
| `batch_id` | UUID | No | Foreign Key → `pig_batches.id` | `feed_records.batch_id` |
| `total_feed_kg` | DECIMAL(12,2) | No | Non-negative | `SUM(quantity_kg)` grouped by `batch_id` |
| `pipeline_run_id` | VARCHAR | No | Lineage identifier | Extraction handover metadata |
| `window_start` | TIMESTAMP | No | Extraction window | Extraction handover metadata |
| `window_end` | TIMESTAMP | No | Extraction window | Extraction handover metadata |

---

### 6.2.4 Transformed Vaccination Dataset

| Target Column | Data Type | Nullable | Key / Constraint | Source / Derivation |
|---|---|---:|---|---|
| `id` | UUID | No | Primary Key | `vaccination_records.id` |
| `batch_id` | UUID | No | Foreign Key → `pig_batches.id` | `vaccination_records.batch_id` |
| `vaccine_name` | VARCHAR(100) | No | Required text | `vaccination_records.vaccine_name` |
| `vaccination_date` | DATE | No | Valid date | `vaccination_records.vaccination_date` |
| `next_due_date` | DATE | Yes | Must not precede vaccination date | `vaccination_records.next_due_date` |
| `administered_by` | VARCHAR(100) | Yes | — | `vaccination_records.administered_by` |
| `dosage` | VARCHAR(100) | Yes | — | `vaccination_records.dosage` |
| `notes` | TEXT | Yes | — | `vaccination_records.notes` |
| `status` | VARCHAR(30) | Yes | — | `vaccination_records.status` |
| `created_at` | TIMESTAMP | No | Lineage timestamp | `vaccination_records.created_at` |
| `vaccination_due_status` | VARCHAR(20) | Yes | Derived, if approved | Derived from `next_due_date` and processing date |

---

### 6.2.5 Transformed Expense Dataset

| Target Column | Data Type | Nullable | Key / Constraint | Source / Derivation |
|---|---|---:|---|---|
| `id` | UUID | No | Primary Key | `expenses.id` |
| `batch_id` | UUID | No | Foreign Key → `pig_batches.id` | `expenses.batch_id` |
| `expense_type` | VARCHAR(100) | No | Required text | `expenses.expense_type` |
| `amount` | DECIMAL(12,2) | No | Non-negative | `expenses.amount` |
| `expense_date` | DATE | No | Valid date | `expenses.expense_date` |
| `description` | TEXT | Yes | — | `expenses.description` |
| `created_at` | TIMESTAMP | No | Lineage timestamp | `expenses.created_at` |

---

### 6.2.6 Expense Summary Dataset

| Target Column | Data Type | Nullable | Key / Constraint | Source / Derivation |
|---|---|---:|---|---|
| `batch_id` | UUID | No | Foreign Key → `pig_batches.id` | `expenses.batch_id` |
| `total_expense` | DECIMAL(12,2) | No | Non-negative | `SUM(amount)` grouped by `batch_id` |
| `pipeline_run_id` | VARCHAR | No | Lineage identifier | Extraction handover metadata |
| `window_start` | TIMESTAMP | No | Extraction window | Extraction handover metadata |
| `window_end` | TIMESTAMP | No | Extraction window | Extraction handover metadata |

---

# 6.3 Naming Convention

PrepAPig transformed datasets use **lowercase snake_case** naming.

### Table / Dataset Naming

```text
pig_batches
feed_records
feed_summary
vaccination_records
expenses
expense_summary
```

### Column Naming

```text
batch_id
batch_code
pig_count
start_weight
current_weight
weight_change
quantity_kg
feeding_date
vaccination_date
next_due_date
expense_type
expense_date
created_at
```

### Naming Rules

1. Use lowercase letters.
2. Use underscores to separate words.
3. Do not use spaces.
4. Do not use special characters.
5. Use descriptive business names.
6. Preserve established source identifiers such as `id` and `batch_id`.
7. Use unit suffixes where required, such as `_kg`.
8. Use `_date` for date-only fields.
9. Use `_time` for time-only fields.
10. Use `_at` for timestamps.

---

# 6.4 Key Naming Convention

### Primary Keys

The existing source UUID identifiers are preserved:

```text
id
```

### Foreign Keys

Foreign keys use the referenced entity name followed by `_id`:

```text
batch_id
```

Relationship:

```text
pig_batches.id
      ↓
feed_records.batch_id

pig_batches.id
      ↓
vaccination_records.batch_id

pig_batches.id
      ↓
expenses.batch_id
```

No surrogate keys are introduced because a dimensional model is not currently defined for the PrepAPig transformation.

---

# 6.5 Source-to-Target Mapping

The following mappings document how transformed columns are derived.

| Source Column | Target Column | Source Type | Target Type | Transformation Applied |
|---|---|---|---|---|
| `pig_batches.id` | `id` | UUID | UUID | Preserve source identifier |
| `pig_batches.batch_code` | `batch_code` | VARCHAR | VARCHAR(50) | Trim and standardize whitespace |
| `pig_batches.pig_count` | `pig_count` | INTEGER | INTEGER | Validate positive value |
| `pig_batches.start_weight` | `start_weight` | DECIMAL | DECIMAL(10,2) | Standardize numeric type |
| `pig_batches.current_weight` | `current_weight` | DECIMAL | DECIMAL(10,2) | Standardize numeric type |
| `pig_batches.start_weight`, `current_weight` | `weight_change` | DECIMAL | DECIMAL(10,2) | Derived: `current_weight - start_weight` |
| `feed_records.batch_id` | `batch_id` | UUID | UUID | Preserve FK relationship |
| `feed_records.feed_type` | `feed_type` | VARCHAR | VARCHAR | Trim and standardize whitespace |
| `feed_records.quantity_kg` | `quantity_kg` | DECIMAL | DECIMAL(10,2) | Validate positive quantity |
| `feed_records.feeding_date` | `feeding_date` | DATE | DATE | Standardize date |
| `vaccination_records.vaccine_name` | `vaccine_name` | VARCHAR | VARCHAR | Trim and standardize whitespace |
| `vaccination_records.vaccination_date` | `vaccination_date` | DATE | DATE | Standardize date |
| `vaccination_records.next_due_date` | `next_due_date` | DATE | DATE | Validate date relationship |
| `expenses.expense_type` | `expense_type` | VARCHAR | VARCHAR | Trim and standardize whitespace |
| `expenses.amount` | `amount` | DECIMAL | DECIMAL(12,2) | Validate non-negative amount |
| `expenses.expense_date` | `expense_date` | DATE | DATE | Standardize date |
| `feed_records.quantity_kg` | `total_feed_kg` | DECIMAL | DECIMAL(12,2) | Derived: `SUM(quantity_kg)` by `batch_id` |
| `expenses.amount` | `total_expense` | DECIMAL | DECIMAL(12,2) | Derived: `SUM(amount)` by `batch_id` |

---

# 6.6 Schema Consistency

The transformed output must conform to this contract.

### Expected Schema Change Handling

If an unexpected input column is:

- **Added:** ignore it unless an approved transformation rule requires it.
- **Removed:** fail or hold the affected transformation if the column is required.
- **Renamed:** treat it as a schema mismatch; do not guess the replacement.
- **Changed in data type:** hold the affected dataset until the mapping is reviewed.
- **Changed in meaning:** require an updated transformation rule and data contract.

### Required Response

```text
Unexpected schema change
        ↓
Detect
        ↓
Log
        ↓
Assess impact
        ↓
Fail / Hold affected output when required
        ↓
Update contract only after approval
```

The transformation process must not silently adapt to an unexpected schema change.

---

