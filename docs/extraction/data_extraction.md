
# Data Extraction Documentation


## 1. Data Source and Extraction Specifiation



### 1.2. Source Database or File



### 1.3. Extraction Method


### 1.4. Extraction Scope

### 1.5. Source Limitations and Assumptions



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

