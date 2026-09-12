# FHIR API Data Ingestion & Analytics using Databricks

## 1. Project Overview

This project is developed as part of the FHIR API Data Ingestion & Analytics assignment.

The main objective is to extract healthcare data from the public HAPI FHIR R4 API and process it using a Medallion architecture in Databricks.

The project works with the following FHIR resources:

* Patient
* Encounter
* Observation
* Condition

The data is first stored in the Raw layer as the original API response. It is then converted into Delta format in the Bronze layer, cleaned and deduplicated in Silver, and finally prepared for reporting in the Gold layer.

## 2. Tools Used

* Databricks Free Edition
* PySpark
* Python
* Delta Lake
* Unity Catalog
* GitHub
* HAPI FHIR R4 API
* Databricks Workflows

## 3. Source API

The project uses the HAPI FHIR R4 public API.

Base URL:

`https://hapi.fhir.org/baseR4`

FHIR resources used:

```text
Patient
Encounter
Observation
Condition
```

The API is queried using the `_lastUpdated` parameter so that the ingestion can be performed for a specific date range instead of reading all available records.

## 4. Overall Architecture

The project follows the below flow:

```text
HAPI FHIR API
       |
       v
01_raw_ingestion
       |
       v
Raw JSON
       |
       v
02_bronze
       |
       v
Bronze Delta Tables
       |
       v
03_silver
       |
       v
Cleaned and Deduplicated Data
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

## 5. Raw Layer

The Raw layer stores the API responses without changing the original FHIR Bundle structure.

The API is called incrementally for a 2–3 day period and pagination is handled using the `next` link returned by the FHIR API.

The Raw folder structure is date based:

```text
/Volumes/workspace/fhir/raw_data/

├── Patient/
│   └── extraction_date=YYYY-MM-DD/
│       ├── page_001.json
│       ├── page_002.json
│       └── ...

├── Encounter/
├── Observation/
├── Condition/

└── _metadata/
    └── api_calls/
```

Each JSON file contains the complete FHIR Bundle returned by the API.

This helps keep the original API response available if the data needs to be reprocessed later.

## 6. API Metadata

Along with the Raw files, API call information is recorded.

The metadata includes:

* `resource_type`
* `page_number`
* `extraction_timestamp`
* `api_url_or_params`
* `http_status`
* `record_count`
* `raw_path`
* `raw_save_timestamp`

`extraction_timestamp` represents when the API response was obtained.

`raw_save_timestamp` represents when the response was saved into the Raw layer.

This metadata is stored separately as Delta data under the Raw metadata location.

## 7. Bronze Layer

The Bronze layer reads the Raw FHIR Bundle JSON files.

The `entry` array from each FHIR Bundle is exploded so that every FHIR resource becomes a separate row.

The Bronze tables are:

```text
workspace.fhir.bronze_patient
workspace.fhir.bronze_encounter
workspace.fhir.bronze_observation
workspace.fhir.bronze_condition
```

The Bronze data contains fields such as:

```text
full_url
resource_type
resource_id
source_file
ingestion_timestamp
raw_json
```

The complete FHIR resource is retained in the `raw_json` column.

## 8. Silver Layer

The Silver layer is used to prepare the data for further analysis.

The main operations are:

* Extract required fields from the Bronze data
* Data type conversion
* Null and data-quality checks
* Deduplication
* Validation of important identifiers
* Identification of the latest version of a record

The Silver tables are created separately for Patient, Encounter, Observation and Condition.

## 9. Data Versioning / SCD Type 2

The assignment requires historical tracking when a FHIR record changes between daily loads.

For this, SCD Type 2 logic is used.

The Silver tables maintain columns such as:

```text
record_hash
effective_start_date
effective_end_date
is_current
```

A record is compared with the existing version using the resource ID and record hash.

If the record has not changed, the existing current record remains active.

If the record has changed:

1. The previous version is closed.
2. `effective_end_date` is updated.
3. `is_current` becomes `false`.
4. A new version is inserted.
5. The new record gets a new `effective_start_date`.
6. The new record is marked as current.

Example:

```text
patient_id | city       | start_date | end_date   | is_current
-----------|------------|------------|------------|-----------
101        | Hyderabad  | 2026-09-10 | 2026-09-12 | false
101        | Bangalore  | 2026-09-12 | 9999-12-31 | true
```

This allows previous versions to be retained instead of simply overwriting the data.

## 10. Gold Layer

The Gold layer contains data prepared for reporting and analytics.

The Gold layer combines information from the cleaned Silver tables where required.

Example analytical output:

```text
Patient
   |
   +---- Encounter
   |
   +---- Observation
   |
   +---- Condition
```

A patient-level summary can be created with information such as:

* Patient ID
* Gender
* Birth date
* Number of encounters
* Number of observations
* Number of conditions
* Latest encounter date

The exact Gold tables can be extended depending on the reporting requirement.

## 11. Orchestration

A Databricks Workflow is used to run the notebooks in sequence.

The main workflow is:

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

Inside the Raw ingestion process, the FHIR resources are processed in the required order:

```text
Patient
   ↓
Encounter
   ↓
Observation
   ↓
Condition
```

The workflow is intended to handle:

* API ingestion
* Data transformation
* Metadata logging
* Delta table loading

## 12. Reusable Code

The ingestion logic is written using reusable Python functions instead of creating separate hardcoded logic for every resource.

For example, the same ingestion function can be used for:

```text
Patient
Encounter
Observation
Condition
```

The resource type, date range and page size are passed as parameters.

This makes it easier to reuse the code for another extraction date or another FHIR resource.

## 13. Notebook Structure

The repository contains the following notebooks:

```text
notebooks/
│
├── 01_raw_ingestion
├── 02_bronze
├── 03_silver
└── 04_gold
```

### 01_raw_ingestion

Responsible for:

* Calling the FHIR API
* Incremental extraction
* Pagination
* Saving Raw JSON
* API metadata logging

### 02_bronze

Responsible for:

* Reading Raw JSON
* Exploding FHIR Bundle entries
* Creating Bronze Delta tables

### 03_silver

Responsible for:

* Data cleaning
* Validation
* Deduplication
* Data versioning
* SCD Type 2

### 04_gold

Responsible for:

* Creating reporting/analytics datasets
* Joining required Silver data
* Preparing final output for reporting

## 14. How the Output Was Validated

The output is validated at each layer instead of only checking the final table.

### Raw validation

The following are checked:

* HTTP status should be successful
* Raw files should exist
* FHIR response should contain `Bundle`
* Pagination should create multiple page files when applicable
* API audit metadata should contain the corresponding API calls

### Bronze validation

The following are checked:

* Bronze tables exist
* Record counts are greater than zero where the API returned data
* `resource_id` is populated
* `resource_type` is correct
* `raw_json` contains the original FHIR resource
* Bronze counts are consistent with the records extracted from Raw

### Silver validation

The following are checked:

* Duplicate records are removed
* Required IDs are not null
* Data types are correct
* Current records have `is_current = true`
* Historical records have `is_current = false`
* Changed records create a new SCD Type 2 version

### Gold validation

The Gold output is checked against the Silver tables.

For example, if a Gold patient summary contains an encounter count, the count can be compared with the corresponding Encounter records in Silver.

Basic SQL count and join checks are also used to make sure records are not unexpectedly lost during transformations.

## 15. Repository Structure

```text
fhir-databricks-lakehouse/
│
├── notebooks/
│   ├── 01_raw_ingestion
│   ├── 02_bronze
│   ├── 03_silver
│   └── 04_gold
│
├── README.md
│
└── docs/
    └── architecture / supporting documentation
```

## 16. Final Result

The completed solution provides:

* Incremental FHIR API ingestion
* Pagination handling
* Raw JSON storage
* Bronze Delta tables
* Silver cleaned and deduplicated data
* SCD Type 2 historical tracking
* Gold reporting datasets
* API and ingestion metadata
* Databricks Workflow orchestration
* Reusable PySpark/Python code
* GitHub version control




