# Data-Pipeline

## Group Members

- De Lunas, Maris N.
- Domingo, Yhuan Miguelle D.
- Elola, Isabella Grace M.
- Patal, Mark Angelo G.

## Pipeline Overview

The **PrepAPig Batch Data Pipeline** processes accumulated pig-farming data in batches for reporting and analytics. It extracts records from the PrepAPig operational database, validates and transforms them using Python, and loads the processed results into a separate PostgreSQL database.

The pipeline covers these farming-related datasets:

- Pig batches
- Feed records
- Vaccination records
- Expenses

User credentials, authentication tokens, and unrelated profile data are outside the ETL scope.

## Pipeline Architecture

Node.js Cron schedules and triggers the Python ETL script. The Python process performs the following steps:

1. Extract accumulated farming records from the PrepAPig operational database.
2. Validate and clean the extracted data.
3. Calculate, transform, and aggregate the records.
4. Load the processed data into a separate PostgreSQL database.
5. Make the processed data available for reports and analytics.

### Technologies

| Technology | Role |
|---|---|
| PrepAPig operational database | Source of accumulated farming data |
| Python | ETL processing, validation, cleaning, transformation, calculations, aggregation, and loading |
| Node.js Cron | Scheduling and orchestration of batch ETL execution |
| PostgreSQL | Destination database for processed ETL results and analytics |

## Data Lineage

```text
PrepAPig operational database
            │
            ▼
   Python batch extraction
            │
            ▼
 Validation, cleaning, transformation,
       calculations, aggregation
            │
            ▼
 Separate PostgreSQL database
            │
            ▼
     Reports and analytics
```

The pipeline runs on a predefined batch schedule and depends on the PrepAPig operational database, the Python environment, Node.js Cron, the PostgreSQL database, configuration settings, and valid application data.
