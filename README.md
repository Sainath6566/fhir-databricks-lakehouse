# fhir-databricks-lakehouse

# FHIR API Data Ingestion & Analytics using Databricks

## 1. Project Overview

This project implements an end-to-end FHIR API data ingestion and analytics pipeline using Databricks and PySpark.

The pipeline extracts healthcare data from a HAPI FHIR R4 API, stores the raw API responses, processes the data through a Medallion architecture, maintains historical versions using SCD Type 2, and creates Gold-layer tables for reporting and analytics.

### FHIR Resources

The pipeline processes the following FHIR resources:

* Patient
* Encounter
* Observation
* Condition

---

## 2. Architecture

```text
                    HAPI FHIR R4 API
                           |
                           v
                 01_raw_ingestion
                           |
                           v
                    Raw JSON Layer
                           |
                           v
                      02_bronze
                           |
                           v
                    Bronze Delta
                           |
                           v
                       03_silver
                           |
                +----------+----------+
                |                     |
          Cleansing              Deduplication
                |                     |
                +----------+----------+
                           |
                           v
                    SCD Type 2
                           |
                           v
                       04_gold
                           |
                           v
                 Reporting / Analytics
```

---

## 3. Technologies Used

* Databricks
* PySpark
* Delta Lake
* Unity Catalog
* Python
* REST API
* FHIR R4
* GitHub
* Databricks Workflows

---

## 4. Source API

The project uses the HAPI FHIR R4 server.

Base API:

```text
https://hapi.fhir.org/baseR4
```

FHIR version:

```text
R4 - 4.0.1
```

The pipeline uses FHIR search parameters to retrieve records incrementally based on the `_lastUpdated` field.

---

## 5. Incremental Data Ingestion

The Raw ingestion notebook retrieves data for a rolling 3-day period.

The extraction window is calculated dynamically:

```python
END_DATE = datetime.now(timezone.utc).date()
START_DATE = END_DATE - timedelta(days=3)
```

The API request uses the `_lastUpdated` parameter:

```text
_lastUpdated >= START_DATE
_lastUpdated < END_DATE
```

This avoids hardcoding extraction dates and allows the pipeline to be reused for future runs.

---

## 6. Pagination

FHIR API responses are paginated.

The ingestion process checks the Bundle response for the `next` link and continues requesting pages until there is no next page.

```text
API Request
    |
    v
Page 1
    |
    v
next link
    |
    v
Page 2
    |
    v
next link
    |
    v
Page N
    |
    v
Complete
```

Each page is stored separately in the Raw layer.

---

## 7. Raw Layer

The Raw layer preserves the API response in its original JSON format.

Storage structure:

```text
/Volumes/workspace/fhir/raw_data/

├── Patient/
│   └── extraction_date=YYYY-MM-DD/
│       ├── page_001.json
│       ├── page_002.json
│       └── ...
│
├── Encounter/
│   └── extraction_date=YYYY-MM-DD/
│
├── Observation/
│   └── extraction_date=YYYY-MM-DD/
│
├── Condition/
│   └── extraction_date=YYYY-MM-DD/
│
└── _metadata/
    └── api_calls/
```

The raw layer allows the original API response to be retained for traceability and reprocessing.

---

## 8. Metadata and Audit Logging

The ingestion process captures metadata for each API call, including:

* Resource type
* Page number
* Extraction timestamp
* API URL and parameters
* HTTP status
* Record count
* Raw file path
* Raw save timestamp

This provides basic ingestion auditing and traceability.

---

## 9. Bronze Layer

The Bronze layer converts the raw FHIR Bundle JSON into Delta tables.

Bronze tables:

```text
workspace.fhir.bronze_patient
workspace.fhir.bronze_encounter
workspace.fhir.bronze_observation
workspace.fhir.bronze_condition
```

The Bronze layer retains the complete FHIR resource as `raw_json`.

Additional metadata includes:

* `full_url`
* `source_file`
* `resource_type`
* `resource_id`
* `ingestion_timestamp`

The source file path is obtained using the Unity Catalog-compatible `_metadata.file_path`.

---

## 10. Silver Layer

The Silver layer performs data cleansing, validation, deduplication, and historical versioning.

Silver tables:

```text
workspace.fhir.silver_patient
workspace.fhir.silver_encounter
workspace.fhir.silver_observation
workspace.fhir.silver_condition
```

### Transformations

The Silver layer performs:

* Column extraction from FHIR JSON
* Data cleansing
* String trimming
* Case standardization
* Null validation
* Data quality status
* Deduplication
* Record hashing
* Historical version tracking

---

## 11. Data Quality

A basic data quality status is generated during Silver processing.

Example:

```text
VALID
INVALID
```

For example, a Patient record without a valid `patient_id` is marked as `INVALID`.

The approach can be extended with additional business validation rules.

---

## 12. Deduplication

Duplicate records are handled using Spark window functions.

For each FHIR resource, the resource ID is used as the business key and the latest ingestion timestamp is retained.

Example:

```python
Window \
    .partitionBy("patient_id") \
    .orderBy(col("ingestion_timestamp").desc())
```

Only the latest version is retained in the current Silver dataset.

---

## 13. SCD Type 2

SCD Type 2 is used to maintain historical changes to records.

The following columns are used:

```text
record_hash
effective_start_date
effective_end_date
is_current
```

A hash is generated from the important business attributes.

When an existing record changes:

```text
Old version
    |
    +-- effective_end_date = change date
    +-- is_current = false

New version
    |
    +-- effective_start_date = change date
    +-- effective_end_date = null
    +-- is_current = true
```

This allows historical versions of a record to be preserved.

---

## 14. Gold Layer

The Gold layer contains analytics-ready Delta tables.

### Gold Patient

```text
workspace.fhir.gold_patient
```

Contains patient-level information such as:

* Patient ID
* First name
* Last name
* Gender
* Birth date
* City
* State
* Postal code

### Gold Patient Encounters

```text
workspace.fhir.gold_patient_encounters
```

Combines Patient and Encounter information for analytics.

### Gold Patient Activity

```text
workspace.fhir.gold_patient_activity
```

Provides patient-level activity metrics including:

* Total encounters
* Total observations
* Total conditions

---

## 15. Reporting View

A reporting view is created:

```text
workspace.fhir.vw_patient_activity
```

This view provides an analytics-friendly representation of patient activity.

Example:

```sql
SELECT *
FROM workspace.fhir.vw_patient_activity;
```

---

## 16. Databricks Workflow

The pipeline is designed to run in the following order:

```text
01_raw_ingestion
        |
        v
02_bronze
        |
        v
03_silver
        |
        v
04_gold
```

Each stage depends on the successful completion of the previous stage.

The Raw ingestion notebook processes the FHIR resources in the following order:

```text
Patient
   ↓
Encounter
   ↓
Observation
   ↓
Condition
```

---

## 17. Project Structure

```text
fhir-databricks-lakehouse/
│
├── notebooks/
│   ├── 01_raw_ingestion
│   ├── 02_bronze
│   ├── 03_silver
│   └── 04_gold
│
└── README.md
```

---

## 18. How to Run

### Step 1 — Run Raw Ingestion

Run:

```text
01_raw_ingestion
```

This retrieves the FHIR resources using incremental filtering and pagination and stores the API responses in the Raw layer.

### Step 2 — Run Bronze

Run:

```text
02_bronze
```

This converts the Raw JSON Bundles into Bronze Delta tables.

### Step 3 — Run Silver

Run:

```text
03_silver
```

This performs cleansing, validation, deduplication, hashing, and historical version processing.

### Step 4 — Run Gold

Run:

```text
04_gold
```

This creates analytics-ready Gold tables and the reporting view.

### Step 5 — Run Workflow

The complete pipeline can be executed using the Databricks Workflow:

```text
Raw → Bronze → Silver → Gold
```

---

## 19. Key Features

The project demonstrates:

* REST API ingestion
* FHIR R4 data processing
* Incremental ingestion
* API pagination
* Raw data preservation
* Metadata and audit logging
* Medallion architecture
* Delta Lake
* Unity Catalog
* Data cleansing
* Deduplication
* Record hashing
* SCD Type 2
* Analytics-ready Gold tables
* Databricks Workflow orchestration
* GitHub-based project version control

