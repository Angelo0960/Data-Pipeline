
## Stage 3: Data Load Documentation

## Objective

The objective of this stage is to load the validated and transformed dataset into the target PostgreSQL database and ensure that the loaded data is complete, accurate, traceable, and ready for reporting, dashboards, and analysis.

The pipeline uses batch processing, where data is loaded in scheduled and bounded batches. Streaming, micro-batch, and real-time loading are outside the scope of this stage.

### 1. Load Source and Target Specification

#### 1.1 Load Input

The load stage receives the approved, transformed output produced by the
data transformation process. It uses the transformed files only. It does
not retrieve new records from the operational database or repeat
data-cleaning and transformation operations.

The expected input files are:

  ---------------------------------------------------------------------------
  Transformed file                        Description
  --------------------------------------- -----------------------------------
  `pig_batches_transformed.csv`           Validated pig batch records

  `feed_records_transformed.csv`          Validated feed records

  `vaccination_records_transformed.csv`   Validated vaccination records

  `expenses_transformed.csv`              Validated expense records

  `feed_summary.csv`                      Feed-consumption totals grouped by
                                          batch

  `expense_summary.csv`                   Expense totals grouped by batch

  `expense_type_summary.csv`              Expense totals grouped by expense
                                          type
  ---------------------------------------------------------------------------

Each load must be associated with its transformation run ID, pipeline
run ID, batch ID, and reporting window. These identifiers are recorded
so that the loaded records can be traced to the correct transformed
output.

The transformed files are expected to be stored in the run-specific
output directory:

``` text
pipeline/data/transformed/<pipeline_run_id>/
```

Only a complete and approved handover is accepted for loading. If a
required file is missing, unreadable, or has an incompatible structure,
the load is stopped and the issue is recorded.

#### 1.2 Target System

  -----------------------------------------------------------------------
  Item                                Specification
  ----------------------------------- -----------------------------------
  Target system                       PrepAPig reporting database

  Database management system          PostgreSQL

  Target database                     Configured PostgreSQL database

  Target schema                       `prepapig_etl`

  Purpose                             Store validated detail records and
                                      summary data for reporting and
                                      analysis
  -----------------------------------------------------------------------

The load process uses a database account with only the permissions
required to write to the target schema and read the information needed
for validation. Credentials and connection secrets are stored in
environment variables and are not included in this document or in log
files.

#### 1.3 Load Method

The load is performed by the Python loading module (`pipeline.load`). It
reads the approved transformed files and writes the records to the
PostgreSQL target.

The load is batch-based. A batch is started after the transformed output
is ready and approved. The process does not use streaming, micro-batch,
or real-time loading.

The proposed write method is a bulk or batch insert with upsert behavior
for records that already exist. This reduces repeated row-by-row
database operations and allows a batch to be safely rerun.

The load schedule is dependent on the completion and approval of the
transformation output. A fixed execution time should only be configured
when the project schedule and operating environment have been finalized.

#### 1.4 Load Scope and Order

The load covers the four detail tables and the summary outputs listed
below.

  ---------------------------------------------------------------------------
                    Load order Target table or output   Purpose
  ---------------------------- ------------------------ ---------------------
                             1 `pig_batches`            Parent records for
                                                        pig batches

                             2 `feed_records`           Feed records
                                                        associated with pig
                                                        batches

                             3 `vaccination_records`    Vaccination records
                                                        associated with pig
                                                        batches

                             4 `expenses`               Expense records,
                                                        including records
                                                        that may not be
                                                        assigned to a
                                                        specific batch

                             5 `feed_summary`           Feed-consumption
                                                        totals grouped by
                                                        batch and reporting
                                                        window

                             6 `expense_summary`        Expense totals
                                                        grouped by batch and
                                                        reporting window

                             7 `expense_type_summary`   Expense totals
                                                        grouped by expense
                                                        type and reporting
                                                        window
  ---------------------------------------------------------------------------

The parent table is loaded before its related detail tables. Summary
tables are loaded after their contributing detail records. The expected
record count for each table is taken from the transformation handover
metadata rather than estimated in the load stage.

The load must complete within the batch processing window established by
the project. The actual expected duration and record volume should be
monitored and adjusted using recorded load results.

#### 1.5 Limitations and Assumptions

-   The load uses only the approved transformed handover.
-   The target PostgreSQL database and `prepapig_etl` schema must exist
    before loading.
-   The loading account must have the required insert, update, and
    select permissions.
-   The load depends on valid database connectivity and sufficient
    target storage.
-   A load must not begin while another load for the same batch is still
    running.
-   The load does not perform new extraction, cleaning, or business
    calculations.
-   The load schedule and exact batch time depend on the configured
    execution environment.

### 2. Target Tables and Column Specification

The target schema stores detail-level farming records and separately
generated summaries. Original UUID identifiers are preserved. The
`batch_id` field links child records to their parent pig batch.

#### 2.1 Detail Tables

  -------------------------------------------------------------------------------------------------------------------------
  Target table            Type            Primary key Important columns and data types  Foreign keys and    Purpose
                                                                                        constraints         
  ----------------------- --------------- ----------- --------------------------------- ------------------- ---------------
  `pig_batches`           Parent/detail   `id UUID`   `batch_code VARCHAR(50)`,         `id` is primary     Stores pig
                                                      `pig_count INTEGER`,              key; `batch_code`   batch
                                                      `start_weight DECIMAL(10,2)`,     is unique; required information
                                                      `current_weight DECIMAL(10,2)`,   identifiers and     
                                                      `date_acquired DATE`,             required values     
                                                      `created_at TIMESTAMP`            must not be null    

  `feed_records`          Detail/child    `id UUID`   `batch_id UUID`,                  `batch_id`          Stores feed
                                                      `feed_type VARCHAR`,              references          records
                                                      `quantity_kg DECIMAL(10,2)`,      `pig_batches.id`;   associated with
                                                      `feeding_date DATE`,              quantity must be    a batch
                                                      `feeding_time VARCHAR(20)`,       non-negative        
                                                      `created_at TIMESTAMP`                                

  `vaccination_records`   Detail/child    `id UUID`   `batch_id UUID`,                  `batch_id`          Stores
                                                      `vaccine_name VARCHAR`,           references          vaccination
                                                      `vaccination_date DATE`,          `pig_batches.id`;   history and
                                                      `next_due_date DATE`,             optional fields may due-date
                                                      `dosage VARCHAR`,                 remain null when    information
                                                      `administered_by VARCHAR`,        permitted           
                                                      `created_at TIMESTAMP`                                

  `expenses`              Detail/child    `id UUID`   `batch_id UUID`,                  `batch_id`          Stores
                                                      `expense_type VARCHAR`,           references          farm-level and
                                                      `amount DECIMAL(10,2)`,           `pig_batches.id`    batch-related
                                                      `expense_date DATE`,              when supplied;      expenses
                                                      `description VARCHAR`,            `batch_id` may be   
                                                      `created_at TIMESTAMP`            null for farm-level 
                                                                                        expenses; amount    
                                                                                        must follow the     
                                                                                        approved numeric    
                                                                                        rules               
  -------------------------------------------------------------------------------------------------------------------------

The load uses each table's source UUID `id` as its match key. The
original UUID values are preserved; new identifiers are not generated
during loading.

The `batch_id` relationship is checked before child records are written.
A child record with a non-null `batch_id` that does not match a row in
`pig_batches.id` must not be loaded.

#### 2.2 Summary Tables

  ------------------------------------------------------------------------------------------
  Target table             Type           Key fields        Important columns Purpose
  ------------------------ -------------- ----------------- ----------------- --------------
  `feed_summary`           Aggregate      `batch_id`,       `total_feed_kg`   Stores total
                                          `window_start`,                     feed quantity
                                          `window_end`,                       for each batch
                                          `run_id`                            and reporting
                                                                              window

  `expense_summary`        Aggregate      `batch_id`,       `total_expense`   Stores total
                                          `window_start`,                     expenses for
                                          `window_end`,                       each batch and
                                          `run_id`                            reporting
                                                                              window

  `expense_type_summary`   Aggregate      `expense_type`,   `total_amount`    Stores expense
                                          `window_start`,                     totals grouped
                                          `window_end`,                       by expense
                                          `run_id`                            type
  ------------------------------------------------------------------------------------------

Summary records must retain the reporting window and run ID. These
fields identify the data included in each aggregate and help prevent
records from different processing windows from being mixed.

#### 2.3 Constraints and Indexes

The following constraints are required for reliable loading:

-   Primary key constraints prevent duplicate record identifiers.
-   Unique constraints apply to business identifiers that must be
    unique, such as `pig_batches.batch_code`.
-   Foreign key constraints maintain the relationship between
    `pig_batches` and its child tables.
-   Required columns must satisfy their `NOT NULL` constraints.
-   Numeric fields such as `pig_count`, `quantity_kg`, weights, and
    `amount` must satisfy the applicable data rules.
-   Indexes should be maintained on primary keys and frequently used
    relationship or matching fields, especially `batch_id`.
-   Summary tables should have a unique key covering the grouping fields
    and reporting window so that a repeated batch does not create
    duplicate summaries.

Only constraints and indexes required by the target schema should be
created. Any schema change must be applied and checked before the load
begins.

### 3. Load Strategy and Rules

#### 3.1 Load Type and Write Logic

The load uses incremental processing. It writes new records and updates
existing records when the same source identifier is received again.

  ---------------------------------------------------------------------------------------
  Target table             Load type      Write logic    Match key         Handling on
                                                                           failure
  ------------------------ -------------- -------------- ----------------- --------------
  `pig_batches`            Incremental    Upsert         `id`              Roll back the
                                                                           batch and stop

  `feed_records`           Incremental    Upsert         `id`              Roll back the
                                                                           batch and stop

  `vaccination_records`    Incremental    Upsert         `id`              Roll back the
                                                                           batch and stop

  `expenses`               Incremental    Upsert         `id`              Roll back the
                                                                           batch and stop

  `feed_summary`           Incremental by Upsert         `batch_id`,       Roll back the
                           run/window                    `window_start`,   batch and stop
                                                         `window_end`,     
                                                         `run_id`          

  `expense_summary`        Incremental by Upsert         `batch_id`,       Roll back the
                           run/window                    `window_start`,   batch and stop
                                                         `window_end`,     
                                                         `run_id`          

  `expense_type_summary`   Incremental by Upsert         `expense_type`,   Roll back the
                           run/window                    `window_start`,   batch and stop
                                                         `window_end`,     
                                                         `run_id`          
  ---------------------------------------------------------------------------------------

The upsert operation inserts a row when its match key does not exist and
updates the existing row when the same key is already present. This
prevents duplicate detail records when a batch is rerun.

#### 3.2 Handling of Changing, Deleted, and Late-Arriving Data

The detail tables use overwrite behavior for an existing record with the
same UUID. This is equivalent to SCD Type 1 behavior: the latest
approved values replace the existing values. Historical versions are not
maintained in this design.

Deleted or inactive source records are not automatically removed from
the target. A deletion or inactive-record rule must be explicitly
defined before the load process changes or removes existing target
records.

Late-arriving records are accepted when they are included in a valid,
approved transformed handover and pass the required key and relationship
checks. Records that fail those checks are rejected and recorded for
review.

If the transformed handover has a missing or incompatible column,
unexpected data type, or schema version that does not match the target,
the batch is held or rejected. The load does not guess missing values or
change the transformed data to make it fit.

#### 3.3 Load Order and Dependencies

The load order follows the parent-child relationships:

1.  Load `pig_batches`.
2.  Load `feed_records`.
3.  Load `vaccination_records`.
4.  Load `expenses`.
5.  Load `feed_summary`.
6.  Load `expense_summary`.
7.  Load `expense_type_summary`.

The parent table must be available before child records that reference
it are loaded. Child records with an unmatched non-null `batch_id` are
rejected or quarantined for investigation. The load must not assign an
unrelated batch ID or create a substitute parent record.

Summary tables are loaded after the detail records so that their values
can be reconciled against the corresponding detail data.

#### 3.4 Transaction and Rollback Handling

A complete batch should be loaded within a database transaction. The
process performs the required writes and validation checks before
committing the transaction.

If a critical error occurs during the load, the transaction is rolled
back. This prevents a partially loaded batch from being treated as
complete. The failure reason and affected table are recorded in the load
log.

A batch is marked successful only after the writes and required
post-load checks have completed. If the database or process fails before
the commit, the batch must be safely rerun using the same batch and run
identifiers.

#### 3.5 Rerun and Idempotency

The load must be idempotent. Running the same approved batch more than
once must not create duplicate detail rows or inflate summary totals.

Idempotency is maintained by:

-   Checking the batch ID and load status before starting.
-   Using source UUIDs as match keys for detail tables.
-   Using the defined grouping fields, reporting window, and run ID as
    summary match keys.
-   Using upsert logic rather than unconditional inserts.
-   Recording each batch's load status and record counts.
-   Rolling back incomplete transactions before a retry.

A rerun must use the same approved transformed handover unless a
corrected transformation run has been produced and approved. The load
stage must not silently replace the input with newly extracted or newly
transformed data.

## 4. Pre-Load Validation Checks

Pre-load validation is performed before the Python loading module (`pipeline.load`) writes data to the PostgreSQL `prepapig_etl` schema. The checks confirm that the database is ready and that the incoming transformed files are complete, approved, and compatible with the target tables.

For each check, the table identifies the **Check Name**, **Target**, **Purpose**, **Validation Criteria**, and **Severity**. A **Critical** failure stops the load. A **Non-critical** warning is recorded and reviewed.

### 4.1 Pre-Load Validation Checklist

| Check Name                              | Target                                              | Purpose                                                                                  | Validation Criteria                                                                                                                                                                                | Severity |
|:----------------------------------------|:----------------------------------------------------|:-----------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------|
| Target Connection and Write Permissions | PostgreSQL database and `prepapig_etl` schema       | Confirm that the loading module can connect and perform the required database operations | Connection succeeds; the loading account has the required `INSERT`, `UPDATE`, and `SELECT` permissions                                                                                             | Critical |
| Target Schema Match                     | Seven target tables in `prepapig_etl`               | Prevent missing-table, missing-column, and data-type errors                              | All seven target tables and required columns exist; column names and data types match the approved target data contract                                                                            | Critical |
| Transformation Handover Status          | Transformation handover metadata                    | Ensure that only approved transformed data is loaded                                     | Handover status is `APPROVED`; transformation run ID, pipeline run ID, batch ID, and reporting window are present                                                                                  | Critical |
| Input File Completeness                 | Four detail files and three summary files           | Prevent an incomplete batch from being loaded                                            | All seven required transformed files exist in the run-specific output directory and are readable                                                                                                   | Critical |
| Input Record Count                      | Transformed files and handover metadata             | Confirm that the received files match the approved handover                              | Actual record count for each file equals the expected count recorded in the handover metadata                                                                                                      | Critical |
| Parent and Foreign-Key Check            | `pig_batches.id` and child-table `batch_id` columns | Prevent child records from referencing a missing pig batch                               | Every non-null `batch_id` in `feed_records`, `vaccination_records`, and `expenses` matches an existing or incoming `pig_batches.id`; a null `expenses.batch_id` is allowed for farm-level expenses | Critical |
| Duplicate Batch Check                   | Batch/load history using `batch_id`                 | Avoid processing a batch that has already completed successfully                         | The batch is not already recorded with final status `SUCCESS`, and no load for the same batch is currently running                                                                                 | Critical |
| Target Storage Availability             | PostgreSQL target database                          | Ensure that the database can accommodate the incoming batch                              | Sufficient target storage is available for the expected load                                                                                                                                       | Critical |

### 4.2 Required Transformed Input Files

The load receives the following seven files from the transformation stage. They are expected in the run-specific directory:

`pipeline/data/transformed/<pipeline_run_id>/`

| Load Order | Transformed Input File                | Target Table           | Description                                                    |
|-----------:|:--------------------------------------|:-----------------------|:---------------------------------------------------------------|
|          1 | `pig_batches_transformed.csv`         | `pig_batches`          | Validated pig batch records; parent records for related tables |
|          2 | `feed_records_transformed.csv`        | `feed_records`         | Validated feed records associated with pig batches             |
|          3 | `vaccination_records_transformed.csv` | `vaccination_records`  | Validated vaccination and due-date records                     |
|          4 | `expenses_transformed.csv`            | `expenses`             | Validated farm-level and batch-related expense records         |
|          5 | `feed_summary.csv`                    | `feed_summary`         | Feed-consumption totals grouped by batch and reporting window  |
|          6 | `expense_summary.csv`                 | `expense_summary`      | Expense totals grouped by batch and reporting window           |
|          7 | `expense_type_summary.csv`            | `expense_type_summary` | Expense totals grouped by expense type and reporting window    |

All seven files must belong to the same approved handover. The expected count for each file must come from the transformation handover metadata, not from an estimate made during loading.

### 4.3 Validation Procedure

1.  Connect to the configured PostgreSQL database and confirm access to the `prepapig_etl` schema.
2.  Confirm that the loading account has the required `INSERT`, `UPDATE`, and `SELECT` permissions.
3.  Confirm that the seven target tables and their required columns exist and match the target data contract.
4.  Verify that the transformation handover is marked `APPROVED` and includes the pipeline run ID, transformation run ID, batch ID, and reporting window.
5.  Confirm that all seven transformed files exist in the run-specific output directory and can be read.
6.  Compare each file’s actual record count with its expected count in the handover metadata.
7.  Check that each non-null child `batch_id` refers to a valid `pig_batches.id`. Do not reject a null `expenses.batch_id` when it represents an allowed farm-level expense.
8.  Check batch history to ensure the batch has not already completed successfully and is not currently being loaded.
9.  Confirm that sufficient PostgreSQL storage is available.

### 4.4 Severity and Decision

| Result    | Meaning                                                                              | Required Action                                                                                         |
|:----------|:-------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------|
| `PASS`    | The validation condition is satisfied                                                | Continue to the next check                                                                              |
| `WARNING` | A non-critical issue is detected and does not violate a critical loading requirement | Record the warning and request review                                                                   |
| `FAIL`    | A critical requirement is not satisfied                                              | Stop the load, record the failed check and reason, and notify the responsible developer or project lead |

The batch may proceed only when all critical pre-load checks pass. A failed batch must not write records to the target. Validation results must be traceable through the pipeline run ID, transformation run ID, and batch ID.

------------------------------------------------------------------------

