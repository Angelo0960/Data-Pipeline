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

This section defines the data-cleaning rules applied to the extracted PrepAPig data before transformation and loading. Records that fail validation are handled according to the rules below to protect data quality and referential integrity.

| Rule ID | Cleaning Rule | Description | Action |
|---|---|---|---|
| C-01 | Missing Primary Key | A record has no valid primary key (`id`). | Generate a UUID when permitted by the source and target design; otherwise quarantine the record. |
| C-02 | Duplicate Primary Key | Multiple records contain the same primary key. | Retain the valid record and quarantine or reject conflicting duplicates. |
| C-03 | Duplicate Batch Code | Multiple pig batches share the same `batch_code`. | Check for duplicate batch records; retain the valid unique batch and flag conflicts for review. |
| C-04 | Required Fields | Required fields are missing or empty. | Reject or quarantine records missing required values; do not silently invent business data. |
| C-05 | Numeric Validity | Numeric fields contain invalid, non-numeric, or out-of-range values. | Validate and convert valid values; quarantine invalid values for correction. |
| C-06 | Whitespace and Invalid Text | Text contains leading/trailing spaces or invalid formatting. | Trim whitespace and normalize text where safe; flag values that cannot be safely corrected. |
| C-07 | Orphan Child Records | Child records reference a batch that does not exist. | Reject or quarantine the child record until its parent batch is available. |
| C-08 | Unexpected Categories or Status | Category/status values are outside the accepted values. | Preserve the original value and flag it for review; do not silently map it to another category. |
| C-09 | Schema Validation Failure | A record does not match the expected source/target structure. | Log the error and quarantine the record rather than loading malformed data. |

### 3.1 Missing-Value Policy

- Missing required values are not replaced with arbitrary defaults.
- Optional values may remain `NULL` when the target schema allows it.
- Invalid records are logged with the relevant table, record identifier, rule ID, and reason.
- Quarantined records are retained for review and possible reprocessing.

### 3.2 Duplicate and Referential-Integrity Handling

- Primary-key duplicates are checked before loading.
- `batch_code` uniqueness is validated for `pig_batches`.
- Child tables (`feed_records`, `vaccination_records`, and `expenses`) must reference an existing batch.
- Records that violate referential integrity are not loaded until corrected or their parent record is available.

## 4. Standardization and Data Type Rules

The following rules standardize field names, formats, and data types to match the target PostgreSQL schema and support consistent downstream processing.

| Rule ID | Field / Data | Standardization Rule |
|---|---|---|
| S-01 | Identifiers (`id`, `batch_id`) | Use UUID format consistently for primary and foreign keys. |
| S-02 | `batch_code` | Store as text (`VARCHAR(50)`); trim spaces and preserve the batch code's meaningful characters. |
| S-03 | Counts (`pig_count`) | Convert to integer; reject or quarantine non-integer values. |
| S-04 | Weights | Store `start_weight` and `current_weight` as `DECIMAL(10,2)`. |
| S-05 | Feed quantity | Store `quantity_kg` as `DECIMAL(10,2)`. |
| S-06 | Expenses | Store `amount` as `DECIMAL(10,2)` and validate numeric values. |
| S-07 | Dates and timestamps | Dates use `YYYY-MM-DD`; timestamps use ISO 8601 format with timezone information where available. |
| S-08 | Feeding time | Standardize `feeding_time` to `HH:MM` (24-hour format) when present. |
| S-09 | Text fields | Trim leading/trailing whitespace and apply consistent text formatting without changing the meaning of source values. |
| S-10 | Column names | Use `snake_case` consistently across tables and fields. |

### Type Conversion and Validation

- Convert source values only when the conversion is valid and does not change their meaning.
- Invalid date, time, numeric, or UUID values are recorded as validation errors.
- Preserve source values in the extraction/staging layer when possible to support troubleshooting.
- Do not round numeric values beyond the precision defined by the target field without documenting the conversion.

## 5. Business Transformation Rules

Business transformations prepare the cleaned and standardized records for reporting and analysis while preserving the meaning of the source data.

| Rule ID | Transformation | Description / Output |
|---|---|---|
| D-01 | Weight Change | If required by reporting, calculate `weight_change = current_weight - start_weight` for a batch with valid weights. |
| D-02 | Feed Summary | Aggregate feed records by batch, such as total `quantity_kg` per `batch_id`. |
| D-03 | Expense Summary by Batch | Aggregate expense `amount` by `batch_id` to calculate total expenses per batch. |
| D-04 | Expense Summary by Category | Aggregate expense amounts by `expense_type` for category-level reporting. |
| D-05 | Vaccination Due Indicator | If required by reporting, derive a due indicator from `next_due_date` relative to the reporting date. |
| D-06 | Batch and Child-Record Relationship | Join feed, vaccination, and expense records to their parent batch using `batch_id`; retain only valid relationships. |
| D-07 | Filtering and Reporting Controls | Exclude invalid/quarantined records from analytical outputs and document the applied filters. |

### 5.1 Transformation Controls

- Aggregations are calculated only from validated records.
- Batch-level summaries use `batch_id` as the grouping key.
- Date-based indicators use a documented reporting date/time.
- Derived fields are not treated as source fields; their formulas and assumptions are documented.
- Transformation errors are logged and do not silently alter source records.

### 5.2 Output and Traceability

- Each transformation should be reproducible from the cleaned source/staging data.
- The ETL process should retain enough metadata to identify the extraction run and transformation result.
- Records excluded from outputs should have a documented reason, such as validation failure or missing parent batch.

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

## 7. Transformation Validation and Data Quality Checks

The Transformation Stage validates the cleaned, standardized, and transformed PrepAPig datasets before they are handed over to the Loading Stage. Validation confirms that records are complete, accurate, consistent, and suitable for loading.

### 7.1 Validation Check Register

| Check Name | Target | Purpose | Validation Criteria | Severity |
|---|---|---|---|---|
| Input Handover Check | Four input CSV files and extraction metadata | Confirm that Transformation receives an approved and complete handover | Extraction status is `APPROVED`; all four files and required metadata are present and readable | Critical |
| Required Field Check | Required columns in all four datasets | Prevent incomplete records from entering the output | Required fields are not null or blank, based on the rules in Section 3 | Critical |
| Primary Key Uniqueness | `id` in `pig_batches`, `feed_records`, `vaccination_records`, and `expenses` | Prevent missing, duplicate, or conflicting record identifiers | Every accepted record has a valid UUID; no conflicting duplicate IDs remain | Critical |
| Batch Code Uniqueness | `pig_batches.batch_code` | Prevent two pig batch records from using the same business identifier | Batch codes are not blank and are unique after trimming whitespace | Critical |
| Data Type and Format Check | UUID, integer, decimal, date, time, and timestamp fields | Ensure that values follow the transformed data contract | Accepted values conform to the data types and formats defined in Section 4 and Section 6 | Critical |
| Numeric and Business Rule Check | `pig_count`, weights, `quantity_kg`, and `amount` | Prevent invalid values from affecting calculations | Counts and numeric values meet the approved non-negative or positive-value rules; invalid values are quarantined | Critical |
| Date Relationship Check | `vaccination_date` and `next_due_date` | Prevent invalid vaccination date relationships | When `next_due_date` is present, it must not be earlier than `vaccination_date` | Critical |
| Referential Integrity Check | Child-table `batch_id` fields | Prevent orphan records and incorrect batch relationships | Every accepted child record references an accepted `pig_batches.id` | Critical |
| Record Reconciliation | Each input table and its transformed detail output | Detect lost or unaccounted-for records | `input_records = accepted_records + quarantined_records + exact_duplicate_copies_removed` | Critical |
| Aggregation Check | `feed_summary` and `expense_summary` | Ensure summary values are correct | Summary totals equal the sums of the corresponding accepted detail records for the same reporting window | Critical |
| Join Cardinality Check | `pig_batches` joined with feed, vaccination, and expense records | Prevent unexpected row multiplication | Each child record matches no more than one parent batch; unmatched child records are quarantined | Critical |
| Idempotency Check | Output records for the same extraction run | Prevent duplicate output after a rerun | Reprocessing the same `pipeline_run_id` and source IDs does not create additional duplicate records | Critical |
| Audit and Metadata Check | Transformation log and data-quality report | Ensure that the run can be traced and reviewed | Run identifiers, window, counts, rules, validation results, status, and errors are recorded | Critical |

### 7.2 Validation and Reconciliation Rules

- **Row-level cleaning:** Account for every input row as accepted, quarantined, or an exact duplicate copy removed. Do not silently delete records.
- **Aggregation:** Recalculate feed and expense summaries from accepted detail records. Validate both the contributing records and the resulting totals.
- **Joins:** Validate the relationship between `pig_batches.id` and each child table's `batch_id`. Check for unmatched records and unexpected row multiplication.
- **Filtering:** Record the reason and count for every excluded record. Do not silently exclude records based on unapproved business conditions.
- **Modified records:** Track records changed by cleaning or standardization separately. Modified records remain part of the accepted output and are not an additional reconciliation category.

### 7.3 Validation Scope

Validation must cover required fields, primary-key uniqueness, batch-code uniqueness, referential integrity, data types, date relationships, numeric rules, aggregation accuracy, and record reconciliation. The Extraction Stage's source-connectivity and extraction checks are not repeated here. Transformation validates the quality and completeness of the received handover and the resulting transformed output.

---

## 8. Transformation Metadata and Log Specification

The Transformation Stage must create a log for each transformation run. The log connects the transformed output to its Extraction Stage handover and supports monitoring, troubleshooting, auditing, and recovery.

### 8.1 Required Metadata Fields

| Field | Purpose | Example or Format |
|---|---|---|
| `transformation_run_id` | Unique identifier for one transformation execution | `TRN_20261106_0200` |
| `pipeline_run_id` | Links the transformation to the Extraction Stage run | Read from extraction metadata, e.g. `PREPAPIG_20261004_230000` |
| `table_name` | Identifies the dataset being transformed | `pig_batches` |
| `window_start` | Records the beginning of the extraction window | Timestamp from extraction metadata |
| `window_end` | Records the end of the extraction window | Timestamp from extraction metadata |
| `transformation_start` | Records when transformation begins | ISO 8601 timestamp |
| `transformation_end` | Records when transformation finishes | ISO 8601 timestamp |
| `transformation_status` | Records the result of the run | `PASS`, `WARNING`, or `FAIL` |
| `input_records` | Number of rows read from the input file | Integer |
| `output_records` | Number of accepted transformed detail rows | Integer |
| `quarantined_records` | Number of records excluded for data-quality reasons | Integer |
| `duplicates_removed` | Number of exact duplicate copies removed | Integer |
| `records_modified` | Number of accepted records changed by cleaning or standardization | Integer; tracked separately |
| `rules_applied` | Cleaning, standardization, relationship, and business rule IDs applied, with affected counts | `C-06: 12; S-07: 35; B-02: 1` |
| `validation_result` | Records the outcome of required validation checks | `PASS` or `FAIL`, with warning details where applicable |
| `output_location` | Identifies where the transformed output was written | Configured output path |
| `error_message` | Provides a safe diagnostic message when an error occurs | Text; must not contain credentials or secrets |
| `transformation_duration` | Records the elapsed processing time | Seconds |
| `transformation_version` | Identifies the version of the transformation code or rules | Approved version identifier |

### 8.2 Logging Requirements

1. Read and retain the Extraction Stage `pipeline_run_id`, `window_start`, and `window_end`.
2. Generate a unique `transformation_run_id` for each execution.
3. Record the start and end timestamps, status, and duration.
4. Record input, accepted output, quarantined, duplicate, and modified record counts.
5. Record the rule IDs applied and the number of records affected by each rule.
6. Record validation results, output location, and safe error details.
7. Keep the Extraction Stage metadata unchanged; link to it using `pipeline_run_id`.
8. Never include passwords, API keys, access tokens, or database connection secrets in logs.

The transformation log and data-quality report must allow a reviewer to identify the input run, determine what happened to its records, and locate the resulting output.

---

## 9. Log Retention, Rejected Data, and Access

Transformation logs and quarantined records must be retained so that the team can investigate errors, verify processing decisions, and recover records when necessary.

### 9.1 Storage and Retention

- Store transformation logs and data-quality reports in the configured Transformation Stage log location.
- Store quarantined records separately from accepted transformed datasets.
- Retain the source table name, source record ID when available, `pipeline_run_id`, `transformation_run_id`, rule ID, rejection reason, and rejection timestamp for each quarantined record.
- Retain logs and quarantine records according to the approved project or institutional data-retention policy. The exact retention period and deployed storage location must be defined by the project team.
- Archive records that have reached the end of their active retention period when required by the approved policy.

### 9.2 Access Permissions

Access must be limited according to each user's responsibility.

| Role | Permitted Access |
|---|---|
| ETL Developer / Administrator | Read and write transformation outputs, logs, and quarantine records; perform approved recovery and maintenance |
| Project Analyst / Data Reviewer | Read transformation logs, validation results, and quarantine records for review |
| Authorized Project Supervisor | Review acceptance decisions and approve records or batches for handover |
| Unauthorized User | No access to transformation logs or quarantined data |

### 9.3 Archiving and Deletion

- Archive logs and rejected records when required by the approved retention policy.
- Delete records only after the retention period has ended and the responsible project authority has approved deletion.
- Do not delete logs or quarantined records while they are needed for troubleshooting, audit, correction, or recovery.
- Record approved archival or deletion actions for accountability.

---

## 10. Transformation Acceptance and Handover Rules

This section defines whether transformed data may proceed to the Loading Stage. Acceptance decisions are based on the validation criteria established in Section 7, not on assumptions made after processing.

### 10.1 Acceptance Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
|---|---|---|
| All required critical checks pass and no unresolved warning remains | `APPROVED` | Proceed to Loading Stage |
| Critical checks pass, but one or more non-critical warnings require review | `MANUAL REVIEW` | Hold the affected output until an authorized reviewer decides |
| One or more critical checks fail | `REJECTED` | Stop handover; correct, rerun, or quarantine the affected data |

### 10.2 Approval Conditions

**Automatic Approval**

A batch may be approved automatically only when:
- All required input files and metadata are present and approved.
- All critical validation checks pass.
- Record counts reconcile.
- Aggregation and relationship checks pass.
- Required transformation logs and data-quality reports are complete.
- No unresolved warning requires manual review.

**Manual Review**

A batch must be held for review when:
- A non-critical validation warning remains unresolved.
- An unexpected category or status value requires an approved mapping decision.
- A business rule or optional derived field requires confirmation.
- Partial acceptance is being considered because some records were quarantined.

The reviewer and approval decision must be recorded before the affected output proceeds.

**Rejection or Failure**

A batch or affected dataset must be rejected or stopped when:
- A required input file or column is missing or unreadable.
- A critical schema, primary-key, or referential-integrity check fails.
- Record reconciliation does not balance.
- Summary calculations are incorrect.
- Output cannot be traced to its extraction run.
- The transformation produces incomplete or unreliable output.

### 10.3 Handover Package

The Transformation Stage provides the following items to the Loading Stage:

| Handover Item | Description |
|---|---|
| `pig_batches_transformed.csv` | Cleaned and standardized pig batch records, including `weight_change` when valid |
| `feed_records_transformed.csv` | Accepted feed records with valid batch relationships |
| `feed_summary.csv` | Total accepted feed quantity by `batch_id` for the recorded window |
| `vaccination_records_transformed.csv` | Accepted vaccination records with standardized dates and values |
| `expenses_transformed.csv` | Accepted expense records with valid batch relationships |
| `expense_summary.csv` | Total accepted expenses by `batch_id` for the recorded window |
| Data-quality report | Profiling results, validation outcomes, record counts, applied rules, and quarantine reasons |
| Transformation log | Run identifiers, extraction window, timestamps, counts, status, output location, and errors |
| Acceptance result | `APPROVED`, `MANUAL REVIEW`, or `REJECTED`, with the decision reason |

Only approved outputs may proceed automatically to the Loading Stage. Outputs under manual review must remain on hold until an authorized decision is recorded. Rejected outputs must not be loaded.

---

## 11. Common Transformation Problems and Mitigation

The following problems are relevant to the PrepAPig batch transformation process. The listed causes and impacts describe possible pipeline risks, not confirmed problems found in the source data.

| Problem | Possible Cause | Impact | Mitigation |
|---|---|---|---|
| Duplicate records after a rerun | The same extraction batch is processed more than once and output is appended without checking source IDs | Duplicate batch, feed, vaccination, or expense records; incorrect summary totals | Make transformation idempotent. Use `pipeline_run_id` and stable source IDs; replace or upsert output for the same run rather than blindly appending |
| Late-arriving records | A source record is created after the current extraction window closes | The record is not included in the current batch and may appear in a later extraction run | Preserve extraction window metadata and process the later approved handover. Do not change the extraction watermark in Transformation |
| Unexpected schema change | A required source column is renamed, removed, or changes data type | Transformation may fail or produce incomplete or incorrect output | Compare input files with the documented data contract before processing. Log the mismatch and hold the affected dataset until the mapping is reviewed and approved |
| Invalid or incomplete records | Required values are missing, numeric values are invalid, or dates cannot be parsed | Records may be excluded or may distort calculations if not handled | Apply the cleaning and standardization rules in Sections 3 and 4. Quarantine affected records and record the rule ID and reason |
| Orphan child records | A feed, vaccination, or expense record references a `batch_id` that does not exist in the accepted `pig_batches` dataset | Records cannot be reliably associated with a pig batch | Validate child `batch_id` values against `pig_batches.id`. Quarantine unmatched records; do not assign another batch ID |
| Row multiplication after a join | A join uses an incorrect or non-unique lookup key | Detail rows and aggregate totals may be overstated | Validate join cardinality and use `pig_batches.id` as the parent key. Check that each child record matches no more than one parent |
| Batch processing takes too long | Input volume increases or transformation and aggregation operations become inefficient | Delayed output and downstream Loading Stage processing | Measure transformation duration, review slow processing steps, and optimize Python processing and joins without changing business rules |
| Summary totals do not match detail records | Invalid records are included, records are counted more than once, or aggregation logic is incorrect | Feed and expense reports become inaccurate | Recalculate summaries using accepted detail records only; compare totals by `batch_id` and reporting window before handover |
| Transformation output is incomplete after a failure | The process stops while output files are being written | The Loading Stage may receive partial or inconsistent files | Write outputs to a temporary location and publish them only after all required validations pass. Record the failure and rerun the same approved input |
| Missing or incomplete transformation logs | Logging fails or required metadata fields are not recorded | The team cannot trace, audit, or recover the affected batch reliably | Validate required log fields before handover. Hold output if run identifiers, counts, status, or validation results are missing |

### 11.1 Problem Monitoring

For each transformation run, review:
- Input, accepted, quarantined, and duplicate counts.
- Validation failures and warnings.
- Rule IDs applied and affected-record counts.
- Transformation duration and processing status.
- Summary reconciliation and join validation results.
- Output completeness and traceability to the Extraction Stage run.

The Transformation Stage is complete only when its outputs have passed the required validation checks, the run has been logged, and the acceptance decision has been recorded. Loading and warehouse performance tuning are outside the scope of this stage.
