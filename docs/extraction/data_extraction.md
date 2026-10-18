
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


## 5. Log Retention and Access

The PrepAPig Batch Data Pipeline maintains extraction logs to monitor daily ETL execution, investigate extraction failures, verify data quality checks, support auditing, and recover unsuccessful extraction runs. Log management follows a defined retention policy to ensure that operational information and audit records remain available for the required period.

The logs contain the pipeline run ID, source table name, extraction timestamps, extraction status, record counts, validation results, extraction window, error messages, and execution duration, as defined in Section 4.

### 5.1 Log Retention Period and Justification

The pipeline uses separate retention periods for operational logs and audit records. The following periods are proposed for the project to support routine monitoring, troubleshooting, auditing, and recovery.

| Log Type | Retention Period | Justification |
|---|---|---|
| Operational extraction logs | 90 days | Provides sufficient history to investigate daily extraction failures, API connection problems, validation errors, and unexpected record counts. |
| Audit records and extraction metadata | 1 year | Preserves the history of extraction activities, validation results, extraction windows, and handover decisions for auditing and traceability. |
| Failed extraction logs | At least 90 days, or until the issue is resolved, whichever is later | Ensures that errors remain available for troubleshooting and that unresolved extraction failures can be investigated. |
| Recovery-related logs | Until recovery is completed and verified, subject to the applicable retention period | Preserves the information required to identify the failed extraction window and confirm successful reprocessing. |

The retention period begins from the date and time the log record is created. Logs must not be deleted before their applicable retention period expires. If a log is still needed for troubleshooting, auditing, investigation, or recovery, its retention must be extended until the need has ended.

### 5.2 Log Storage Location

Extraction logs are stored separately from the extracted farming datasets to make log management, access control, and retention easier to maintain.

The proposed storage arrangement is as follows:

| Storage Component | Purpose |
|---|---|
| Dedicated extraction log directory on the ETL host | Stores operational log files generated by the Python extraction script. |
| Restricted metadata and audit storage | Preserves run-level and table-level extraction metadata, validation results, record counts, statuses, and error information for long-term traceability. |
| Secured backup or archive storage | Holds retained copies of important audit records and logs required for recovery or historical reference. |

A suggested directory structure for the Python ETL project is:

```text
PrepAPig-ETL/
├── logs/
│   └── extraction/
│       ├── operational/
│       ├── audit/
│       └── archive/
├── output/
│   └── extracted_data/
└── extraction.py
```

The `operational` directory contains routine extraction logs, the `audit` directory contains records retained for audit and traceability, and the `archive` directory contains logs moved out of active storage after the applicable archival procedure.

If PostgreSQL is used for structured metadata and audit storage, the records should be maintained in a dedicated logging or metadata table, separate from the processed farming data.

The directory structure and storage arrangements described above are proposed configurations and must be implemented in the ETL environment. Log files must not be stored in publicly accessible locations or committed to a public source-code repository.

### 5.3 Access Permissions

Access to extraction logs must be restricted to authorized personnel and system processes according to their responsibilities.

| Role | Access Permission | Responsibility |
|---|---|---|
| Python ETL process account | Write access to the required log locations | Generates extraction logs and records run metadata, validation results, and errors. |
| Authorized developer or ETL operator | Read access to operational logs and relevant audit records | Monitors scheduled runs, investigates failures, and verifies extraction results. |
| Project administrator | Administrative access to log storage and retention settings | Manages permissions, storage capacity, archival procedures, and authorized deletion. |
| Unauthorized users | No access | Prevents unauthorized viewing, modification, or deletion of log records. |

Log access must follow the principle of least privilege. Only personnel who require access for pipeline operation, maintenance, or auditing should be granted the necessary permissions.

The Python ETL process should have only the permissions needed to create and update its designated logs. Routine users should not be allowed to modify or delete historical audit records. Database credentials, API keys, access tokens, passwords, and other secrets must not be written to log files.

### 5.4 Archiving Requirements

Logs that are no longer required for routine monitoring but must be retained for auditing or historical reference should be archived according to the proposed retention policy.

The archiving procedure follows these requirements:

1. Operational logs approaching the end of their active retention period are reviewed to determine whether they are still needed for troubleshooting, auditing, or recovery.
2. Logs required for longer-term traceability are transferred to the designated restricted archive or audit storage.
3. Archived logs must retain their original timestamps, pipeline run IDs, table names, extraction statuses, and relevant validation information.
4. Archived files must be protected against unauthorized modification and deletion.
5. Archive copies must be checked for completeness and readability after transfer.
6. Archive storage must be monitored to ensure that sufficient capacity remains available.
7. Logs associated with unresolved failures or ongoing investigations must remain accessible until the related issue is resolved.

Archiving does not mean that the original retention requirements are automatically satisfied or that the records can be deleted immediately. Archived logs remain subject to their applicable retention period and any outstanding troubleshooting, auditing, or recovery requirements.

### 5.5 Deletion Rules and Responsible Process

Log deletion must follow a controlled process to prevent the loss of information required for operational monitoring, auditing, or recovery.

The deletion procedure is as follows:

1. The project administrator or authorized ETL operator reviews logs that have reached the end of their applicable retention period.
2. The reviewer checks whether the logs are associated with unresolved extraction failures, pending investigations, audit requirements, or incomplete recovery activities.
3. Logs that remain necessary are retained, even if their normal retention period has expired.
4. Logs that have passed their retention period and are no longer required may be deleted through an authorized cleanup procedure.
5. The deletion process must not remove the current successful-extraction watermark, necessary recovery information, or records required to establish extraction history.
6. The responsible administrator records the deletion activity, including the deletion date, log period covered, and reason for deletion, where feasible.
7. The deletion procedure is reviewed periodically to confirm that it follows the approved retention policy.

A scheduled cleanup task may be configured to identify expired operational logs and remove eligible files. However, automated cleanup must enforce the retention period and check for applicable retention holds before deleting any records.

**Mandatory deletion rule:** Logs must not be deleted before the defined retention period has expired or while they are still needed for troubleshooting, auditing, investigation, or recovery. If any of these conditions apply, deletion must be postponed until the logs are no longer required.





## 6. Extraction Data Contract and Naming Convention

The extraction data contract defines the expected structure, identifiers, data types, naming rules, and schema consistency requirements for data extracted from the Supabase project `it332-capstone-PrePApig`.

The extracted data must maintain a consistent structure so that the CSV outputs can be reliably passed from the Extraction Stage to the Transformation Stage.

---

### 6.1 Required Schema

The extraction process is expected to retrieve the following four tables from the Supabase `it332-capstone-PrePApig` project:

1. `pig_batches`
2. `feed_records`
3. `vaccination_records`
4. `expenses`

#### Table: `pig_batches`

| Column | Data Type | Requirement / Constraint |
|---|---|---|
| `id` | UUID | Primary Key |
| `batch_code` | VARCHAR(50) | UNIQUE, NOT NULL |
| `pig_count` | INTEGER | NOT NULL |
| `breed` | VARCHAR(100) | — |
| `start_weight` | DECIMAL(10,2) | — |
| `current_weight` | DECIMAL(10,2) | — |
| `date_acquired` | DATE | — |
| `status` | VARCHAR(20) | DEFAULT 'Active' |
| `created_at` | TIMESTAMP | DEFAULT NOW() |

#### Table: `feed_records`

| Column | Data Type | Requirement / Constraint |
|---|---|---|
| `id` | UUID | Primary Key |
| `batch_id` | UUID | Foreign Key → `pig_batches.id` |
| `feed_type` | VARCHAR(100) | NOT NULL |
| `quantity_kg` | DECIMAL(10,2) | NOT NULL |
| `feeding_date` | DATE | NOT NULL |
| `feeding_time` | VARCHAR(20) | — |
| `created_at` | TIMESTAMP | DEFAULT NOW() |

#### Table: `vaccination_records`

| Column | Data Type | Requirement / Constraint |
|---|---|---|
| `id` | UUID | Primary Key |
| `batch_id` | UUID | Foreign Key → `pig_batches.id` |
| `vaccine_name` | VARCHAR(100) | NOT NULL |
| `vaccination_date` | DATE | NOT NULL |
| `next_due_date` | DATE | — |
| `administered_by` | VARCHAR(100) | — |
| `dosage` | VARCHAR(50) | — |
| `notes` | TEXT | — |
| `status` | VARCHAR(20) | DEFAULT 'Completed' |
| `created_at` | TIMESTAMP | DEFAULT NOW() |

#### Table: `expenses`

| Column | Data Type | Requirement / Constraint |
|---|---|---|
| `id` | UUID | Primary Key |
| `batch_id` | UUID | Foreign Key → `pig_batches.id` |
| `expense_type` | VARCHAR(100) | NOT NULL |
| `amount` | DECIMAL(10,2) | NOT NULL |
| `expense_date` | DATE | NOT NULL |
| `description` | TEXT | — |
| `created_at` | TIMESTAMP | DEFAULT NOW() |

### Required Identifiers and Relationships

The extraction must preserve the following identifiers:

- `pig_batches.id` — Primary Key
- `feed_records.id` — Primary Key
- `vaccination_records.id` — Primary Key
- `expenses.id` — Primary Key
- `feed_records.batch_id` → `pig_batches.id`
- `vaccination_records.batch_id` → `pig_batches.id`
- `expenses.batch_id` → `pig_batches.id`

The `batch_code` field in `pig_batches` must remain unique.

---

### 6.2 Naming Convention

The extracted data follows the existing naming convention defined by the PrepAPig source schema.

#### Column Naming Format

All column names use **snake_case**.

Examples:

```text
batch_code
pig_count
start_weight
current_weight
date_acquired
feed_type
quantity_kg
feeding_date
vaccine_name
vaccination_date
next_due_date
expense_type
expense_date
created_at


```
## 7. Extraction Acceptance and Handover Rules

The PrepAPig data pipeline establishes acceptance and handover rules to determine whether data extracted from the Supabase project `it332-capstone-PrePApig` is suitable for the Transformation Stage.

These rules apply to the `pig_batches`, `feed_records`, `vaccination_records`, and `expenses` tables.

The extraction process uses the validation results defined in Section 3 and the extraction metadata recorded in Section 4 to determine whether the extracted datasets should be automatically approved, held for manual review, or rejected.

### 7.1 Extraction Acceptance Decision Rules

| Validation Condition | Acceptance Criteria | Pipeline Action | Decision Type | Responsible Role |
|---|---|---|---|---|
| All critical validation checks pass | All four tables return PASS with no critical errors. | APPROVED — Proceed to Transformation Stage. | Automatic | Python extraction script |
| Non-critical validation warning | No critical failures, but an unexpected condition requires investigation. | MANUAL REVIEW — Hold extracted data for verification. | Manual | Data Engineer |
| Source connection failure | Supabase REST API cannot be accessed. | REJECTED — Stop extraction and retry after resolving the issue. | Automatic | Python extraction script |
| Required table or column missing | Expected source schema is incomplete. | REJECTED — Stop extraction and investigate schema changes. | Automatic | Python extraction script |
| Missing or duplicate primary key | One or more records contain missing or duplicate identifiers. | REJECTED — Hold the affected dataset and investigate. | Automatic | Python extraction script |
| Record count mismatch | Source record count differs from the extracted output count. | REJECTED — Retry or investigate incomplete extraction. | Automatic | Python extraction script |
| Extraction window mismatch | One or more extracted records fall outside the defined incremental window. | REJECTED — Stop handover and review the extraction query. | Automatic | Python extraction script |
| Extraction error or interruption | Extraction cannot complete successfully. | REJECTED — Stop, record the error, and retry when appropriate. | Automatic | Python extraction script |

### 7.2 Automatic Approval

The extracted datasets are automatically approved when:

| No. | Automatic Approval Condition |
|---|---|
| 1 | All four required Supabase tables are successfully accessed. |
| 2 | All required columns and identifiers are present. |
| 3 | Primary keys are unique within their respective datasets. |
| 4 | Extracted records fall within the defined incremental extraction window. |
| 5 | Source and output record counts match. |
| 6 | No critical extraction errors occur. |
| 7 | All critical validation checks return PASS. |

When these conditions are satisfied, the Python extraction script marks the overall extraction as **_APPROVED_** and allows the validated CSV files to proceed to the Transformation Stage.

### 7.3 Manual Review

Manual review is required when extraction completes without critical errors but produces a non-critical warning.

| No. | Example of Non-Critical Warning |
|---|---|
| 1 | An unexpected decrease or increase in the number of extracted records. |
| 2 | An extraction window containing no new records despite an expectation of new activity. |
| 3 | A non-critical issue requiring verification of extraction metadata. |

During manual review, the Data Engineer examines the extracted CSV files, validation results, and `extraction_log.csv`.

The dataset remains on hold until the Data Engineer approves or rejects the extraction.

### 7.4 Rejection or Failure

An extraction is rejected when one or more critical validation checks fail.

| No. | Pipeline Requirement |
|---|---|
| 1 | Stop the affected dataset from proceeding to the Transformation Stage. |
| 2 | Record the failure in `logs/extraction_log.csv`. |
| 3 | Preserve the extraction error and validation results. |
| 4 | Retry recoverable errors, such as temporary network failures. |
| 5 | Hold invalid datasets for investigation when necessary. |
| 6 | Maintain the previous successful extraction checkpoint. |

The checkpoint must not be updated when the overall extraction is rejected.

This allows the next extraction attempt to retrieve records from the last successful extraction window.

### 7.5 Extraction Handover

Once the extraction is approved, the following outputs are prepared for the Transformation Stage.

| Handover Component | Output | Purpose |
|---|---|---|
| Pig Batch Data | `pig_batches.csv` | Provides extracted pig batch records. |
| Feed Records | `feed_records.csv` | Provides extracted feed consumption records. |
| Vaccination Records | `vaccination_records.csv` | Provides extracted vaccination records. |
| Expense Records | `expenses.csv` | Provides extracted farm expense records. |
| Extraction Metadata | `extraction_log.csv` | Provides extraction timestamps, record counts, status, and extraction windows. |
| Validation Results | Recorded validation results | Confirms whether extraction requirements were satisfied. |
| Extraction Checkpoint | Last successful extraction timestamp | Supports the next incremental extraction run. |

The CSV files contain raw extracted data. Data cleaning, standardization, aggregation, and business calculations are performed in the Transformation Stage.

### 7.6 Validation Summary Example

| Source Table | Validation Result |
|---|---|
| `pig_batches` | PASS |
| `feed_records` | PASS |
| `vaccination_records` | PASS |
| `expenses` | PASS |
| **_Overall Extraction_** | **_APPROVED_** |

> **This is an illustrative validation summary, not an actual execution result.**

When all required tables pass validation, the extracted datasets may proceed to the Transformation Stage.

---



## 8. Common Extraction Problems and Mitigation

During the extraction stage of the PrepAPig Farm Management System, several problems may occur when retrieving and packaging data from the Supabase PostgreSQL database. Since the extraction process uses incremental extraction based on the `created_at` field and is executed through a Python ETL script scheduled by Node.js Cron, extraction problems must be detected and handled before the data is handed over to the Transformation stage.

The following table identifies realistic extraction problems, their possible causes, their impact on the extraction process, and the proposed mitigation.

| Extraction Problem | Possible Cause | Impact on Extraction Process | Proposed Solution / Mitigation |
|---|---|---|---|
| **Supabase database or API is unavailable** | Temporary service interruption, database downtime, network problem, or Supabase service issue. | The Python ETL script cannot retrieve records, resulting in a failed or incomplete extraction. | Retry the extraction after a short interval. If the problem persists, mark the extraction run as **FAILED**, retain the previous successful watermark, and retry the same extraction window during the next scheduled run. |
| **Authentication or database connection failure** | Invalid credentials, expired connection configuration, incorrect database/API settings, or unavailable connection. | The extraction script cannot access the required PrepAPig tables. No valid dataset can be produced. | Verify the configured connection and access permissions. Credentials must be stored securely and must not be written to logs. Mark the run as **FAILED** until a valid connection is restored. |
| **Required table is missing or inaccessible** | Source schema changed, table was renamed/deleted, or the ETL account no longer has sufficient permissions. | The affected table cannot be extracted, causing the extracted dataset to be incomplete. | Check that `pig_batches`, `feed_records`, `vaccination_records`, and `expenses` exist and are accessible. Stop or reject the extraction if a required table cannot be retrieved. Investigate the source schema before rerunning. |
| **Required column is missing or changed** | Source database schema was modified without updating the extraction process. | The Python ETL script may fail or produce incomplete records because required fields cannot be retrieved. | Validate required columns before extraction. If a required column is missing, mark the run as **FAILED** and update the extraction script only after confirming the approved source schema. |
| **Invalid or missing `created_at` values** | Source records contain null, malformed, or unexpected timestamp values. | Incremental extraction may skip records or extract records outside the intended window, resulting in missing or duplicated data across runs. | Validate the `created_at` field before applying the incremental filter. Records that cannot be reliably evaluated should not be silently included or excluded. Flag the issue for review and retain the previous watermark when the extraction cannot be trusted. |
| **Incorrect incremental extraction window** | Incorrect watermark, incorrect comparison operators, timestamp conversion problems, or failure to use the fixed extraction timestamp for the current run. | Records may be duplicated, skipped, or extracted more than once across consecutive runs. | Use the defined window: `last_successful_extraction_timestamp < created_at <= current_extraction_timestamp`. Capture `current_extraction_timestamp` once at the beginning of the run and only update the watermark after successful validation and handover. |
| **Duplicate primary keys detected** | Duplicate source records, source integrity problems, or an extraction/query error. | Duplicate records may cause the extracted dataset to violate the expected source structure and may affect downstream relationships. | Validate primary key uniqueness after extraction. Mark the validation as **FAIL** for critical duplicate-key violations and do not approve the dataset for Transformation until the issue is investigated. |
| **Missing primary key or `batch_id` values** | Incomplete source records or source data integrity problems. | Records cannot be reliably identified or associated with their corresponding pig batch. This can affect downstream relationships and validation. | Check required identifiers during extraction validation. Flag invalid records and prevent approval when the missing identifier affects a critical relationship. |
| **Invalid foreign-key relationship** | A `feed_records`, `vaccination_records`, or `expenses` record references a `batch_id` that does not exist in `pig_batches`. | The extracted data may contain orphan records that cannot be reliably related to a pig batch during Transformation. | Validate foreign-key relationships before handover. Record the affected rows in the validation results and classify the extraction as **WARNING**, **MANUAL REVIEW**, or **REJECTED** depending on the severity and number of affected records. |
| **Incomplete results due to API pagination or query limits** | The Supabase API or extraction query returns only part of the requested records because of pagination or result-size limits. | The extracted dataset contains fewer records than the source for the specified extraction window, causing incomplete data handover. | Implement pagination or appropriate query handling in the Python ETL script. Compare extracted record counts with the expected query results and do not approve the extraction when completeness cannot be confirmed. |
| **Network interruption during extraction** | Unstable internet connection, temporary network failure, or connection timeout. | The extraction may stop partway through, producing incomplete output or an unsuccessful run. | Use controlled retry handling for temporary connection failures. If the extraction cannot be completed, mark the run as **FAILED**, discard incomplete output, retain the previous watermark, and retry the same window on the next run. |
| **Python ETL script terminates unexpectedly** | Programming error, unhandled exception, insufficient system resources, or unexpected source data. | The extraction may stop before all required tables are processed, resulting in incomplete output. | Capture exceptions and write an error message to the extraction log without exposing credentials or sensitive connection information. Mark the run as **FAILED** and investigate the error before approving the output. |
| **Extracted CSV file cannot be created or written** | Incorrect output path, insufficient permissions, unavailable storage, or file-system error. | Data may be successfully retrieved from Supabase but cannot be packaged for handover to Transformation. | Validate the output directory before extraction and verify that the CSV file is successfully written. If the output cannot be created, mark the run as **FAILED** and do not advance the watermark. |
| **CSV output is incomplete or truncated** | ETL interruption during file writing, unexpected termination, or storage problem. | The output file may contain fewer records than were successfully retrieved, resulting in incomplete handover. | Verify the output file after writing, including record counts and file existence. Incomplete files must not be handed over or used for Transformation. Re-run the extraction using the unchanged watermark. |
| **Node.js Cron does not trigger the extraction script** | Scheduler configuration error, server restart, incorrect script path, permission problem, or Cron service failure. | The daily extraction does not occur, causing the source data to remain unextracted for the expected period. | Monitor scheduled runs using extraction logs and run metadata. Check the Cron configuration and Python script path. A missed run should be executed using the appropriate previous successful watermark so that no extraction window is skipped. |
| **Multiple extraction runs execute at the same time** | Cron starts a new run before the previous run has completed, or the extraction script is manually executed while a scheduled run is active. | Concurrent runs may produce duplicate output, conflicting metadata, or inconsistent watermark updates. | Use a run-control mechanism such as a lock file or process check to allow only one extraction run at a time. A new run should wait or terminate safely when another extraction is already in progress. |
| **Timezone mismatch in timestamps** | Database timestamps, server time, or ETL environment use different timezone settings. | Records may be incorrectly included or excluded from the incremental extraction window. | Use a consistent timestamp standard for the extraction process and ensure that `created_at`, the watermark, and the current extraction timestamp are compared using the same timezone convention. |
| **Unexpected increase or decrease in extracted record count** | Source system activity changed, failed query, incomplete extraction, duplicate records, or unexpected source data changes. | A significantly abnormal record count may indicate an incomplete or incorrect extraction. | Compare the current record count with previous extraction runs. Generate a **WARNING** when the count is unusual and require manual review when the difference suggests possible extraction failure. |
| **Empty extraction result** | No new records were created during the extraction window, incorrect extraction window, or source/query problem. | An empty result may be valid, but it may also indicate that the incremental filter is incorrect. | Treat an empty result as valid only when the extraction query and source connection are successfully validated. Record the run metadata and extraction window so the result can be distinguished from a failed extraction. |
| **Source schema changes without notification** | New columns, renamed columns, removed fields, or changed data types in the PrepAPig database. | The extraction script may fail or produce data that no longer matches the expected extraction structure. | Perform schema validation before extraction. Any incompatible schema change should cause the affected extraction to enter **FAIL** or **MANUAL REVIEW** status until the extraction process and data contract are updated. |
| **Secrets or credentials appear in extraction logs** | Debug logging, exception messages, or connection strings are written directly to operational logs. | Sensitive information may be exposed and create a security risk. | Never write passwords, API keys, access tokens, or complete connection strings to logs. Log only the error type, affected process, timestamp, and safe diagnostic information. |
| **Watermark is updated after an unsuccessful extraction** | Watermark is saved before validation or before the extracted dataset is successfully packaged. | The next extraction may start after records that were never successfully handed over, causing permanent data loss. | Update the watermark only after extraction, validation, output packaging, and successful handover have completed. Failed runs must retain the previous successful watermark. |

### 8.1 Extraction Failure Handling

The extraction process follows a controlled failure-handling approach. When an extraction problem occurs, the ETL process records the problem in the extraction metadata and logs. The extraction status is then classified according to the validation results.

- **PASS** – The extraction completed successfully and the extracted dataset passed the required validation checks.
- **WARNING** – The extraction completed, but a non-critical issue requires monitoring or manual review.
- **FAIL** – A critical problem prevents the extracted dataset from being considered reliable.
- **MANUAL REVIEW** – The extraction does not contain a critical failure but requires an authorized team member to verify the result before handover.

For critical extraction failures, the previous successful watermark is retained. This allows the next scheduled run to retry the same extraction window instead of skipping records that were not successfully extracted.

### 8.2 Retry and Recovery

Temporary problems such as network interruptions, temporary Supabase unavailability, or connection timeouts may be retried. However, repeated failures should not automatically be treated as successful extraction.

The recovery process is:

1. Start the scheduled extraction.
2. Capture the current extraction timestamp.
3. Retrieve records using the previous successful watermark.
4. Validate the extracted records and output.
5. If validation passes, package the dataset and update the watermark.
6. If a critical failure occurs, mark the run as **FAILED** and retain the previous watermark.
7. Retry the same extraction window during the next scheduled run after the underlying problem has been resolved.

This approach prevents records from being skipped when an extraction run fails.

### 8.3 Monitoring and Maintenance

The extraction logs and run metadata should be monitored for repeated problems such as connection failures, abnormal record counts, missed Cron executions, schema changes, and validation failures. Repeated failures should be investigated rather than continuously retried without identifying the underlying cause.

The following information should be available for troubleshooting:

- `pipeline_run_id`
- `table_name`
- `extraction_start`
- `extraction_end`
- `extraction_status`
- `records_read`
- `records_extracted`
- `records_rejected`
- `window_start`
- `window_end`
- `validation_result`
- `error_message`
- `extraction_duration`

These records allow team members to determine what happened during an extraction run, identify whether data was successfully extracted, and determine whether the dataset is safe to hand over to Transformation.

### 8.4 Scope of Mitigation

The mitigation activities described in this section are limited to problems affecting the **extraction and packaging of source data**. They do not include data cleaning, standardization, business-rule calculations, aggregation, or other transformations.

Once the extracted dataset has successfully passed the required extraction validations and has been packaged with its corresponding metadata, it can be handed over to the **Transformation stage**. Any cleaning, standardization, or business transformation required by the project should be performed during that subsequent stage.
