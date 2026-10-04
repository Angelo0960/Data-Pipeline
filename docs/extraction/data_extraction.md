
# Data Extraction Documentation

The objective of Stage 1 is to extract the required farming data from
the PrepAPig Farm Management System source system and prepare a
documented, traceable, and validated raw dataset for the next stage of
the PrepAPig data pipeline.

The extraction covers the following operational data:

-   Pig batch records
-   Feed records
-   Vaccination records
-   Expense records


## 1. Data Source and Extraction Specification

The PrepAPig Farm Management System is a livestock monitoring and analytics system designed to assist the pig farm in tracking pig growth performance, managing vaccination schedules, monitoring feed consumption, and analyzing farm-related data.

The system serves as the primary source of operational farming data that will be extracted for the PrepAPig data pipeline.

### 1.2. Source Database or File
   The PrepAPig Farm Management System serves as the primary source of data for the extraction process. It uses Supabase as its platform and PostgreSQL as its database to store the farm's operational records.The following table presents the basic information about the source system and database used in the data extraction stage.

| Item | Details |
|---|---|
| **Source System** | PrepAPig Farm Management System |
| **Source Platform** | Supabase |
| **Supabase Project** | `it332-capstone-PrePApig` |



### 1.3. Extraction Method

#### Extraction Type

The pipeline uses **incremental batch extraction** after the initial load. The initial run extracts the available source records as a baseline. Later runs extract only records created within the new extraction window.

#### Extraction Window and Incremental Logic

The incremental watermark is the `created_at` timestamp available in each extraction table:

- `pig_batches.created_at`
- `feed_records.created_at`
- `vaccination_records.created_at`
- `expenses.created_at`

For each table, the extraction window is:

```text
last_successful_extraction_timestamp < created_at <= current_extraction_timestamp
```

The `last_successful_extraction_timestamp` is read from the previous successful pipeline run. The `current_extraction_timestamp` is captured when the extraction starts and remains fixed for that run. A record is extracted when its `created_at` falls inside this window. The upper-bound timestamp is included so records created during the run are not skipped, while the previous lower bound is excluded to prevent duplicate extraction.

If a table does not contain a usable `created_at` value, the extraction must flag the limitation and use the table's documented date field only if the team explicitly approves that mapping. Records with missing timestamps are reported as extraction exceptions rather than silently omitted.

#### Extraction Process

1. Node.js Cron starts the scheduled Python ETL script.
2. The Python script records the run start time as the current extraction timestamp.
3. It reads the previous successful extraction timestamp for each source table.
4. It connects to the PrepAPig operational database and queries each included table using the incremental window.
5. It extracts the related `pig_batches`, `feed_records`, `vaccination_records`, and `expenses` records while preserving primary and foreign keys.
6. It validates source accessibility, required tables, required columns, identifiers, and extracted record counts.
7. It records the extraction metadata and validation results.
8. Only after a successful extraction and validation does it save the new watermark for the next run and hand the extracted dataset to the Transformation stage.

The extraction stage does not perform business transformations or data cleaning beyond the checks required to confirm that the source data can be safely extracted.

#### Extraction Tool and Technology

- **Scheduler/orchestrator:** Node.js Cron
- **Extraction script:** Python ETL script
- **Extraction technology:** Python database connection and SQL queries against the PrepAPig operational database
- **Source system:** PrepAPig operational database
- **Output handover:** Validated extracted dataset plus run metadata, validation results, and the extraction window

#### Extraction Schedule

The pipeline runs **once per day** at the predefined time configured in Node.js Cron. The exact clock time is controlled by the deployment configuration. Each scheduled run creates a new pipeline run ID and extraction window. Failed runs do not advance the successful-extraction watermark; the next run retries the unprocessed window.


### 1.4. Extraction Scope

The extraction includes only the four farming-related source tables required by the PrepAPig pipeline.

Source Table Scope Purpose

`pig_batches` Pig batch records Provides the main batch information used to relate other farming records.

`feed_records` Feed records Provides feed consumption information associated with pig batches.

`vaccination_records` Vaccination records Provides vaccination history and scheduling information associated with pig batches.

`expenses_Expense` records Provides expense information associated with pig batches. Excluded Data
The following are outside the extraction scope:

User authentication credentials
- Passwords
- API keys or access tokens
- Unrelated user profile information
- Other application data not required by the ETL pipeline

### 1.5. Source Limitations and Assumptions
The extraction process assumes that:

1.  The project PrepAPig Farm Management System is accessible during
    extraction.
2.  The required source tables are available.
3.  The required columns and identifiers are present.
4.  The Python environment can establish the required database
    connection.
5.  Node.js Cron can successfully trigger the Python extraction process.
6.  The source schema remains compatible with the documented ETL
    structure.
7.  Source data required by the pipeline is available and readable.
8.  Any change to the source schema is detected before the data is
    handed over to the next stage.


## 2. Source Tables and Column Specifications

The extraction scope includes the farming data required by the PrepAPig batch pipeline. Authentication, password, user-profile, and other unrelated application data are excluded.

### 2.1 `pig_batches`

**Purpose:** Stores the primary record for each group of pigs being monitored.

| Column | Data type | Key or constraint | Description |
|---|---|---|---|
| `id` | UUID | Primary key, unique, not null | Unique identifier for the pig batch. |
| `batch_code` | VARCHAR(50) | Not null | Human-readable identifier for the batch. |
| `pig_count` | INTEGER | — | Number of pigs in the batch. |
| `breed` | VARCHAR(100) | — | Breed of the pigs. |
| `start_weight` | DECIMAL(10,2) | — | Initial weight of the batch. |
| `current_weight` | DECIMAL(10,2) | — | Current recorded weight of the batch. |
| `date_acquired` | DATE | — | Date the batch was acquired. |
| `status` | VARCHAR(20) | — | Current status of the batch. |
| `created_at` | TIMESTAMP | Default `NOW()` | Date and time the batch record was created. |

**Relevant relationships:** Parent table for `feed_records`, `vaccination_records`, and `expenses`. One batch can have many records in each child table through `batch_id`.

**Reason for inclusion:** Provides the batch identifiers and core attributes needed to associate and aggregate all farming records during extraction.

### 2.2 `feed_records`

**Purpose:** Records the feed supplied to each pig batch.

| Column | Data type | Key or constraint | Description |
|---|---|---|---|
| `id` | UUID | Primary key | Unique identifier for the feed record. |
| `batch_id` | UUID | Foreign key to `pig_batches(id)`, `ON DELETE CASCADE` | Identifies the related pig batch. |
| `feed_type` | VARCHAR(100) | Not null | Type of feed given. |
| `quantity_kg` | DECIMAL(10,2) | Not null | Amount of feed in kilograms. |
| `feeding_date` | DATE | Not null | Date of feeding. |
| `feeding_time` | VARCHAR(20) | Not null | Time of feeding. |
| `created_at` | TIMESTAMP | Default `NOW()` | Date and time the feed record was created. |

**Relevant relationships:** Many feed records can belong to one `pig_batches` record through `batch_id`.

**Reason for inclusion:** Supplies feed quantities and dates needed for batch-level reporting, aggregation, and analytics.

### 2.3 `vaccination_records`

**Purpose:** Records vaccinations administered to each pig batch.

| Column | Data type | Key or constraint | Description |
|---|---|---|---|
| `id` | UUID | Primary key | Unique identifier for the vaccination record. |
| `batch_id` | UUID | Foreign key to `pig_batches(id)` | Identifies the related pig batch. |
| `vaccine_name` | VARCHAR(100) | Not null | Name or type of vaccine administered. |
| `vaccination_date` | DATE | — | Date the vaccination was administered. |
| `next_due_date` | DATE | Not null | Scheduled date for the next vaccination. |
| `administered_by` | VARCHAR(100) | — | Person who administered the vaccination. |
| `dosage` | VARCHAR(50) | — | Vaccination dosage. |
| `notes` | TEXT | — | Additional vaccination details. |
| `status` | VARCHAR(20) | — | Vaccination record status. |
| `created_at` | TIMESTAMP | Default `NOW()` | Date and time the vaccination record was created. |

**Relevant relationships:** Many vaccination records can belong to one `pig_batches` record through `batch_id`.

**Reason for inclusion:** Provides vaccination history and scheduling data needed for batch monitoring and reporting.

### 2.4 `expenses`

**Purpose:** Records expenses associated with each pig batch.

| Column | Data type | Key or constraint | Description |
|---|---|---|---|
| `id` | UUID | Primary key | Unique identifier for the expense record. |
| `batch_id` | UUID | Foreign key to `pig_batches(id)` | Identifies the related pig batch. |
| `expense_type` | VARCHAR(100) | Not null | Type or category of the expense. |
| `amount` | DECIMAL(10,2) | Not null | Amount of the expense. |
| `expense_date` | DATE | — | Date when the expense occurred. |
| `description` | TEXT | — | Additional details about the expense. |
| `created_at` | TIMESTAMP | Default `NOW()` | Date and time the expense record was created. |

**Relevant relationships:** Many expense records can belong to one `pig_batches` record through `batch_id`.

**Reason for inclusion:** Provides cost data needed for expense aggregation and financial reporting related to pig batches.

### 2.5 Extraction Relationships

```text
pig_batches (1)
    ├── feed_records (many)
    ├── vaccination_records (many)
    └── expenses (many)
```

The `batch_id` foreign key in each child table links extracted records to their parent pig batch. These relationships must be preserved in the extracted dataset so downstream processing can join records accurately.




## 3. Extraction Validation and Data Quality Checks

The extraction validation process verifies that data retrieved from the Supabase project **PrepAPig Farm Management System** is complete, accessible, and structurally valid before it is handed over to the Transformation Stage.

Validation is performed after the Python extraction script retrieves records from the `pig_batches`, `feed_records`, `vaccination_records`, and `expenses` tables through the Supabase REST API.

### 3.1 Extraction Validation Checks

| Check Name                       | Target                                                              | Purpose                                                                                        | Validation Criteria                                                                                                                                                                   |
| -------------------------------- | ------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Source Connection Check**      | Supabase REST API                                                   | Verify that the Python extraction script can access the Supabase source.                       | **PASS:** API returns a successful HTTP response. **FAIL:** Connection error, timeout, authentication failure, or unsuccessful HTTP response prevents extraction.                     |
| **Required Table Check**         | `pig_batches`, `feed_records`, `vaccination_records`, `expenses`    | Verify that all required source tables can be accessed.                                        | **PASS:** All four required tables can be queried successfully. **FAIL:** One or more required tables cannot be accessed.                                                             |
| **Required Column Check**        | Extracted records from all four tables                              | Verify that fields required by the pipeline are present in the extracted dataset.              | **PASS:** All required columns documented for the table are present. **FAIL:** One or more required columns are missing.                                                              |
| **Primary Key Presence Check**   | `id` column of all four tables                                      | Ensure every extracted record has its required unique identifier.                              | **PASS:** No extracted record has a NULL or missing `id`. **FAIL:** One or more extracted records have a missing or NULL `id`.                                                        |
| **Primary Key Uniqueness Check** | `id` column of all four tables                                      | Detect duplicate identifiers within the extracted records.                                     | **PASS:** Number of unique `id` values equals the number of extracted records. **FAIL:** Duplicate `id` values are detected.                                                          |
| **Batch Code Uniqueness Check**  | `pig_batches.batch_code`                                            | Verify the documented UNIQUE constraint of pig batch codes.                                    | **PASS:** Every extracted `batch_code` is unique. **FAIL:** Duplicate batch codes are detected.                                                                                       |
| **Foreign Key Presence Check**   | `batch_id` in `feed_records`, `vaccination_records`, and `expenses` | Ensure child records contain the identifier required to associate them with a pig batch.       | **PASS:** Required `batch_id` values are present. **FAIL:** A required `batch_id` is missing or NULL.                                                                                 |
| **Extraction Window Check**      | `created_at`                                                        | Verify that incremental extraction returns records belonging to the defined extraction window. | **PASS:** Every extracted record satisfies `previous_successful_extraction < created_at <= current_extraction_time`. **FAIL:** A returned record falls outside the extraction window. |
| **Record Count Check**           | Supabase API result and generated CSV                               | Verify that all records retrieved during the extraction are written to the extraction output.  | **PASS:** Number of successfully extracted API records equals the number of data rows written to the CSV. **FAIL:** Counts do not match.                                              |
| **Extraction Error Check**       | Python extraction process                                           | Detect errors or interruptions during extraction.                                              | **PASS:** Extraction completes without a critical error. **FAIL:** API, network, authentication, script, or file-writing error prevents successful extraction.                        |

### 3.2 Required Column Validation

The following fields are expected from each source table based on the documented PrepAPig schema.

#### `pig_batches`

- `id`
- `batch_code`
- `pig_count`
- `breed`
- `start_weight`
- `current_weight`
- `date_acquired`
- `status`
- `created_at`

#### `feed_records`

- `id`
- `batch_id`
- `feed_type`
- `quantity_kg`
- `feeding_date`
- `feeding_time`
- `created_at`

#### `vaccination_records`

- `id`
- `batch_id`
- `vaccine_name`
- `vaccination_date`
- `next_due_date`
- `administered_by`
- `dosage`
- `notes`
- `status`
- `created_at`

#### `expenses`

- `id`
- `batch_id`
- `expense_type`
- `amount`
- `expense_date`
- `description`
- `created_at`

### 3.3 Incremental Extraction Window Validation

Because the pipeline uses incremental extraction, the `created_at` timestamp is checked against the current extraction window.

The expected condition is:

`previous_successful_extraction < created_at <= current_extraction_time`

For example:

- Previous successful extraction: `2026-10-03 23:00:00`
- Current extraction time: `2026-10-04 23:00:00`

A record with:

`created_at = 2026-10-04 14:30:00`

is considered valid because it falls within the extraction window.

A record with:

`created_at = 2026-10-03 20:00:00`

does not belong to the current incremental extraction window.

For the initial pipeline execution, when no previous successful extraction timestamp exists, the pipeline performs the defined initial extraction instead of applying the previous-run boundary.

### 3.4 Record Count Validation

For every table, the extraction script records:

- Number of records returned by the Supabase REST API.
- Number of records successfully extracted.
- Number of records written to the CSV file.
- Number of records rejected, if applicable.

The extraction passes the completeness check when:

`API records retrieved = successfully extracted records = CSV data rows`

If the counts do not match, the affected extraction is marked as **FAIL** and must not automatically proceed to the Transformation Stage.

### 3.5 Validation Results

Each validation check produces one of the following results:

| Result      | Meaning                                                                                          | Action                                                        |
| ----------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------- |
| **PASS**    | The extraction satisfies the required validation criteria.                                       | Continue processing.                                          |
| **WARNING** | A non-critical condition requires review but does not immediately indicate corrupted extraction. | Hold for manual review.                                       |
| **FAIL**    | A critical extraction requirement was not satisfied.                                             | Reject the extraction and stop or retry the affected process. |

### 3.6 Overall Extraction Validation

The overall extraction is classified according to the combined validation results:

- **APPROVED** — All critical validation checks return **PASS**.
- **MANUAL REVIEW** — No critical checks fail, but one or more non-critical checks return **WARNING**.
- **REJECTED** — One or more critical validation checks return **FAIL**.

Only an **APPROVED** extraction is automatically handed over to the Transformation Stage.

### 3.7 Scope Limitation

This stage validates the reliability and completeness of the **extraction process only**.

Data cleaning, standardization, business-rule validation, calculations, aggregation, and analytical transformations are not performed during Extraction. These operations are handled in the **Transformation Stage**.



## 4. Extraction Metadata and Log Specification

The extraction process produces one metadata and log record for every scheduled pipeline run. The run log connects the extraction window, source table, validation results, record counts, and final handover decision so that each extracted dataset can be traced from the PrepAPig operational database to the next pipeline stage.

### 4.1 Extraction Metadata Specification

| Metadata Field | Purpose | Example / Format |
|---|---|---|
| `pipeline_run_id` | Uniquely identifies each execution of the extraction pipeline. | `PREPAPIG_20261004_230000` |
| `table_name` | Identifies the Supabase table being extracted. | `feed_records` |
| `extraction_start` | Records the date and time when extraction of the table started. | `2026-10-04 23:00:00` |
| `extraction_end` | Records the date and time when extraction of the table finished. | `2026-10-04 23:00:03` |
| `extraction_status` | Indicates whether the extraction operation completed successfully. | `SUCCESS`, `WARNING`, or `FAILED` |
| `records_read` | Number of records returned from the Supabase REST API for the extraction window. | `25` |
| `records_extracted` | Number of records successfully included in the extraction output. | `25` |
| `records_rejected` | Number of records rejected because of critical extraction-level validation failures, if applicable. | `0` |
| `window_start` | Previous successful extraction timestamp used as the lower boundary of the incremental extraction. | `2026-10-03 23:00:00` |
| `window_end` | Current extraction timestamp used as the upper boundary of the incremental extraction. | `2026-10-04 23:00:00` |
| `validation_result` | Overall validation result for the extracted table. | `PASS`, `WARNING`, or `FAIL` |
| `error_message` | Records an error encountered during extraction. Remains empty when no error occurs. | `HTTP 401: Unauthorized` or empty |
| `extraction_duration` | Total time required to complete the extraction operation. | `3 seconds` |

### 4.2 How Metadata and Logs Connect to the Extraction Process

1. Node.js Cron starts the Python ETL script and the script creates a unique `pipeline_run_id`.
2. The script records `extraction_start` and uses it as the fixed `window_end` for the current extraction run.
3. For each Supabase table, the script reads the previous successful watermark into `window_start` and extracts records where `created_at` satisfies:

   ```text
   window_start < created_at <= window_end
   ```

4. The table-level log records `table_name`, `records_read`, `records_extracted`, and `records_rejected`.
5. The script records `validation_result` for source accessibility, required tables and columns, required identifiers, record counts, and extraction errors.
6. The script records `extraction_end` after extraction and validation finish, then calculates `extraction_duration` from `extraction_start` to `extraction_end`.
7. If all critical checks pass, `extraction_status` is `SUCCESS` and `validation_result` is `PASS`. The new successful timestamp is saved as the next `window_start` only after the extracted dataset is accepted.
8. If a critical check fails, `extraction_status` is `FAILED` and `validation_result` is `FAIL`. The `error_message` records the cause, the current window is not marked successful, and the next scheduled run retries the unprocessed window.
9. If only non-critical issues occur, `extraction_status` is `WARNING` and `validation_result` is `WARNING`. The affected records are counted in `records_rejected` and the handover is held for review according to the acceptance rules.

### 4.3 Monitoring, Troubleshooting, Auditing, and Recovery

- **Monitoring:** Operators use status, duration, record counts, validation results, and window boundaries to confirm that the daily extraction completed and that the volume is reasonable.
- **Troubleshooting:** The `pipeline_run_id`, `table_name`, timestamps, error message, and extraction window identify the affected run and table without requiring the entire source query to be repeated.
- **Auditing:** The log preserves what was extracted, when it was extracted, which Supabase table was used, which validation result was produced, and whether the output was accepted for the next stage.
- **Recovery:** A failed run does not advance the successful watermark. The next run reuses the last successful `window_start`, allowing the missed window to be retried without silently losing records.

### 4.4 Log Record Relationship

Each scheduled run has one run-level record and one table-level record for every source table processed:

```text
pipeline_run_id
    ├── pig_batches log
    ├── feed_records log
    ├── vaccination_records log
    └── expenses log
```

This structure keeps the logs connected to the incremental extraction method and provides a complete handover package: the extracted data, the extraction window, record counts, validation results, status, and any errors.