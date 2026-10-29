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

## 3. Cleaning Rules

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

## 4. Standardization Rules

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

## 5. Derived Fields and Aggregation Rules

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


