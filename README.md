# 🏥 ApexCare — Real-Time Healthcare Data Engineering & Analytics Platform

[![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-blue?logo=microsoftazure)](https://azure.microsoft.com/products/data-factory/)
[![Azure Databricks](https://img.shields.io/badge/Azure-Databricks-red?logo=databricks)](https://azure.microsoft.com/products/databricks/)
[![Delta Lake](https://img.shields.io/badge/Delta-Lake-0052CC?logo=delta)](https://delta.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)](https://kafka.apache.org/)
[![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-0078D4?logo=microsoftazure)](https://azure.microsoft.com/products/synapse-analytics/)
[![Power BI](https://img.shields.io/badge/Power-BI-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)

---

## 📌 Project Overview

**ApexCare** is an end-to-end healthcare data engineering and analytics platform built on Microsoft Azure.

The project demonstrates how batch healthcare data and real-time patient telemetry can be ingested, processed, governed, modeled, and served through a modern **Medallion Lakehouse Architecture (Bronze → Silver → Gold)**.

The platform combines:

- Metadata-driven batch ingestion using **Azure Data Factory**
- Real-time bedside telemetry using **Azure Event Hubs (Kafka protocol)**
- Streaming and batch transformations using **Azure Databricks & PySpark**
- ACID-compliant storage and SCD Type 2 processing using **Delta Lake**
- Metadata and pipeline audit control using **Azure SQL Database**
- Secret management using **Azure Key Vault**
- Data governance using **Databricks Unity Catalog**
- Analytical serving using **Azure Synapse Serverless SQL**
- Executive, clinical, and financial analytics using **Power BI**
- Source control and automated validation using **GitHub Actions**

The project uses **synthetically generated healthcare data** and is designed as a portfolio/reference implementation of a production-style Azure data engineering platform.

---

## 🏗️ End-to-End Architecture

```text
                         ┌──────────────────────────┐
                         │ Healthcare Batch Sources │
                         │ CSV / JSON               │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                              ADLS Gen2 — Raw
                                      │
                                      ▼
                         Azure Data Factory (ADF)
                       Metadata-Driven Ingestion
                                      │
                                      ▼
                              Bronze Delta Layer
                                      │
                                      │
┌───────────────────────┐             │
│ Bedside Vitals        │             │
│ Telemetry Generator   │             │
└───────────┬───────────┘             │
            │                         │
            ▼                         │
   Azure Event Hubs                   │
    Kafka Endpoint                    │
            │                         │
            ▼                         │
 Databricks Structured                │
      Streaming                       │
            │                         │
            └──────────────┬──────────┘
                           ▼
                    Silver Delta Layer
               Cleaning / DQ / SCD Type 2
                           │
                           ▼
                     Gold Delta Layer
                       Star Schema
                           │
                           ▼
                 ADLS Gen2 Gold Storage
                           │
                           ▼
                Synapse Serverless SQL
                    ApexCareGold
                           │
                           ▼
                       Power BI
             Executive / Clinical / Financial
                       Analytics
```

Supporting the pipeline:

```text
Azure SQL Database  → Metadata Control + Audit Tables
Azure Key Vault     → Secret Management
Unity Catalog       → Data Governance
GitHub Actions      → CI Validation + Deployment Simulation
```

---

## 🛠️ Technology Stack

| Layer | Azure Service / Technology | Implementation |
|---|---|---|
| **Storage** | Azure Data Lake Storage Gen2 | Raw, Bronze, Silver, Gold and streaming checkpoint storage |
| **Control Plane** | Azure SQL Database (Serverless) | Metadata-driven ingestion configuration and audit tables |
| **Batch Ingestion** | Azure Data Factory | Parameterized metadata-driven ingestion pipeline |
| **Streaming Ingestion** | Azure Event Hubs | Kafka-compatible real-time bedside telemetry ingestion |
| **Processing** | Azure Databricks | PySpark batch and Structured Streaming workloads |
| **Lakehouse Storage** | Delta Lake | Bronze/Silver/Gold architecture, ACID transactions and MERGE |
| **Data Modeling** | PySpark / Delta | SCD Type 2 and dimensional/star-schema transformations |
| **Governance** | Databricks Unity Catalog | Catalog/schema governance and controlled data access |
| **Security** | Azure Key Vault | Centralized secret management |
| **Serving** | Azure Synapse Serverless SQL | SQL views over Gold-layer Parquet data |
| **Analytics** | Power BI | Executive, clinical-risk and payer/revenue analytics |
| **DevOps** | GitHub + GitHub Actions | Source control, linting, JSON validation and deployment simulation |

---

# 🔄 Data Engineering Pipeline

## 1️⃣ Batch Ingestion — Azure Data Factory

Batch healthcare datasets are landed in the ADLS Gen2 Raw layer and processed through the metadata-driven ADF pipeline:

```text
PL_Ingest_Source_To_Bronze
```

ADF uses configuration stored in:

```text
meta.IngestionControl
```

to determine source systems, entities, file formats, source paths, target locations, ingestion strategies, and watermark configuration.

The repository contains the actual ADF artifacts used by the project, including:

```text
adf/
├── dataset/
│   ├── ds_adls_bronze_dynamic.json
│   ├── ds_adls_raw_dynamic.json
│   └── ds_sqldb_lookup.json
│
├── linkedService/
│   ├── ls_adls_gen2.json
│   └── ls_sqldb_controldb.json
│
└── pipeline/
    └── PL_Ingest_Source_To_Bronze.json
```

The SQL linked service uses secret-based configuration rather than storing plaintext credentials in the repository.

---

## 2️⃣ Real-Time Streaming — Event Hubs + Databricks

A Python telemetry producer simulates bedside patient-vitals events and sends them to an **Azure Event Hubs Kafka-compatible endpoint**.

Example telemetry attributes include:

```text
Heart Rate
Blood Pressure
Oxygen Saturation
Body Temperature
Device ID
Encounter ID
Event Timestamp
```

Streaming flow:

```text
Python Telemetry Producer
        ↓
Azure Event Hubs
   Kafka Endpoint
        ↓
Databricks Structured Streaming
        ↓
Bronze Delta
        ↓
Silver / Gold
```

Databricks Structured Streaming uses checkpointing to maintain streaming progress and support recoverable processing.

Sensitive Event Hubs credentials are retrieved at runtime through Databricks secret management rather than stored directly in source code.

---

# 💎 Medallion Lakehouse Architecture

## 🥉 Bronze Layer

The Bronze layer preserves ingested healthcare data with minimal transformation.

Responsibilities include:

- Batch source ingestion
- Real-time telemetry ingestion
- Source traceability
- Ingestion metadata
- Delta-based storage
- Structured Streaming checkpoints

---

## 🥈 Silver Layer

The Silver layer performs data cleansing, standardization and business-rule processing.

Implemented transformations include:

- Data type standardization
- Timestamp validation
- Duplicate handling
- Healthcare data-quality rules
- Encounter transformations
- Length-of-stay calculation
- Patient dimension processing
- SCD Type 2 history tracking

### SCD Type 2

Patient demographic history is maintained using Delta Lake `MERGE` logic.

Historical versions are maintained using attributes such as:

```text
PatientSK
IsCurrent
EffectiveStartDate
EffectiveEndDate
```

This preserves historical patient-dimension changes while maintaining a current record.

---

## 🥇 Gold Layer

The Gold layer provides analytics-ready dimensional datasets.

The implemented star schema contains:

### Dimensions

```text
dim_patient
dim_provider
dim_department
```

### Facts

```text
fact_patient_encounters
fact_vitals_telemetry
```

Gold Delta datasets are optimized using Delta Lake techniques including:

```text
OPTIMIZE
ZORDER
```

The Gold layer is persisted in ADLS Gen2 and exposed to downstream analytics through **Synapse Serverless SQL**.

> A Dedicated SQL Pool DDL example is retained in the repository as an optional/reference serving architecture. The implemented ApexCare reporting architecture uses **Synapse Serverless SQL**.

---

# 🗄️ Metadata Control & Auditing

Azure SQL Database provides the pipeline control plane.

## Metadata Configuration

```text
meta.IngestionControl
```

controls ingestion behavior across healthcare source entities.

Configured entities include:

```text
Patients
Providers
Departments
Encounters
LabResults
BillingClaims
PatientVitals
```

## Audit Tables

```text
audit.PipelineExecutionLog
audit.DataQualityLog
```

provide structures for pipeline execution and data-quality auditing.

The control database also includes:

```text
meta.usp_UpdateWatermark
```

for updating ingestion watermark state.

---

# 🔐 Security & Governance

## Azure Key Vault

Secrets and connection information are externalized from source code and managed through Azure Key Vault / secret references where implemented.

Examples include:

```text
Event Hubs connection information
Azure SQL credentials
Storage credentials / fallback access
```

No production secrets or plaintext passwords are intentionally stored in the repository.

## Databricks Secret Management

Databricks workloads retrieve sensitive values at runtime using secret scopes rather than hardcoded credentials.

Example pattern:

```python
dbutils.secrets.get(scope="<secret-scope>", key="<secret-name>")
```

## Unity Catalog

Unity Catalog is used as the governance layer for the Databricks environment, organizing data across:

```text
bronze
silver
gold
```

and providing centralized governance over lakehouse data access.

---

# ⚡ Synapse Serverless SQL Serving Layer

The implemented analytics-serving architecture uses:

```text
Azure Synapse Serverless SQL
Database: ApexCareGold
Endpoint: Built-in
```

Serverless SQL views query Gold-layer Parquet data stored in ADLS Gen2.

### Base Analytical Views

```text
gold.vw_Dim_Patient
gold.vw_Dim_Provider
gold.vw_Dim_Department
gold.vw_Fact_PatientEncounters
gold.vw_Fact_VitalsTelemetry
```

### Executive Analytical Views

```text
gold.vw_exec_encounter_summary
gold.vw_exec_critical_vitals
gold.vw_exec_revenue_by_payer
```

These views provide the SQL serving layer consumed by Power BI.

The actual Serverless SQL DDL is available at:

```text
synapse/DDL_Serverless_Gold_Views.sql
```

An optional Dedicated SQL Pool schema is retained separately for architectural comparison:

```text
synapse/DDL_Dedicated_SQL_Pool_Optional.sql
```

---

# 📊 Power BI Healthcare Analytics

The final analytics layer contains **three interactive Power BI report pages** covering executive, clinical and financial perspectives.

## 1. Executive Healthcare Overview

Provides operational and hospital-performance analytics including:

- Patient volume
- Encounter volume
- Revenue KPIs
- Average length of stay
- Admission trends
- Department performance
- Encounter-type analysis

![Executive Healthcare Overview](powerbi/screenshots/01_executive_healthcare_overview.png)

---

## 2. Clinical Vitals & Patient Risk Analytics

Analyzes bedside telemetry generated through the Event Hubs → Databricks streaming pipeline.

Includes:

- Critical vitals alerts
- Critical alert rate
- Heart-rate monitoring
- Oxygen-saturation monitoring
- Device-level alert analysis
- Patient risk filtering

![Clinical Vitals & Patient Risk Analytics](powerbi/screenshots/02_clinical_vitals_risk_analytics.png)

---

## 3. Financial & Payer Analytics

Provides financial analytics across insurance payers, encounter types and geographic regions.

Includes:

- Total revenue
- Revenue per encounter
- Payer distribution
- Revenue by insurance payer
- Revenue by state
- Encounter analysis

![Financial & Payer Analytics](powerbi/screenshots/03_financial_payer_analytics.png)

The complete Power BI report is included at:

```text
powerbi/ApexCare_Healthcare_Analytics.pbix
```

Additional dashboard documentation is available in:

```text
powerbi/README.md
```

> The dashboard screenshots provide browser-friendly portfolio previews. The PBIX file is included for local exploration in Power BI Desktop.

---

# 💰 Cost-Conscious Azure Architecture

ApexCare was developed with Azure cost controls in mind.

Cost-management decisions included:

- Single-node Databricks development compute
- Automatic Databricks cluster termination
- Azure SQL Database serverless compute with auto-pause
- Synapse Serverless SQL for the implemented reporting workload
- Event Hubs configured for the project streaming workload
- Dedicated SQL Pool kept outside the active reporting path
- Compute resources terminated or paused when not required

Synapse Serverless follows a **pay-per-data-processed** model, making it suitable for portfolio-scale analytical workloads without maintaining dedicated warehouse compute.

---

# 🔄 GitHub CI & Deployment Simulation

The project uses **GitHub Actions** for automated repository validation.

Workflow:

```text
.github/workflows/deploy_apexcare.yml
```

The CI workflow performs:

```text
Repository Checkout
        ↓
Python Environment Setup
        ↓
Python / PySpark Linting
        ↓
ADF JSON Validation
        ↓
Deployment Simulation
```

The workflow validates repository artifacts and simulates downstream ADF and Databricks deployment stages.

> Azure infrastructure deployment is intentionally represented as a **deployment simulation** in the current portfolio implementation. The workflow does not claim automated production deployment to Azure.

This structure demonstrates how validation and deployment stages can be incorporated into a production CI/CD strategy while keeping the portfolio environment safe and cost-controlled.

---

# 📂 Repository Structure

```text
Healthcare-RealTime-Azure-Data-Platform/
│
├── .github/
│   └── workflows/
│       └── deploy_apexcare.yml
│
├── adf/
│   ├── dataset/
│   │   ├── ds_adls_bronze_dynamic.json
│   │   ├── ds_adls_raw_dynamic.json
│   │   └── ds_sqldb_lookup.json
│   ├── linkedService/
│   │   ├── ls_adls_gen2.json
│   │   └── ls_sqldb_controldb.json
│   └── pipeline/
│       └── PL_Ingest_Source_To_Bronze.json
│
├── architecture/
│   └── data_flow_and_security_matrix.md
│
├── databricks/
│   ├── bronze/
│   │   └── 01_ingest_streaming_vitals_to_bronze.py
│   ├── silver/
│   │   ├── 01_transform_encounters_silver.py
│   │   └── 04_transform_patients_scd2_silver.py
│   ├── gold/
│   │   ├── 01_build_gold_star_schema.py
│   │   └── 02_delta_lake_optimizations.py
│   └── utilities/
│       └── data_quality_engine.py
│
├── datasets/
│   ├── generator/
│   │   └── generate_batch_healthcare_data.py
│   └── raw_batch/
│       ├── BillingClaims.csv
│       ├── Departments.csv
│       ├── Encounters.csv
│       ├── LabResults.json
│       ├── Patients.csv
│       └── Providers.csv
│
├── docs/
│   └── data_dictionary_and_erd.md
│
├── eventhub/
│   └── streaming_event_producer.py
│
├── powerbi/
│   ├── screenshots/
│   │   ├── 01_executive_healthcare_overview.png
│   │   ├── 02_clinical_vitals_risk_analytics.png
│   │   └── 03_financial_payer_analytics.png
│   ├── ApexCare_Healthcare_Analytics.pbix
│   └── README.md
│
├── sql/
│   ├── metadata_control_db.sql
│   └── seed_metadata_control_tables.sql
│
├── synapse/
│   ├── DDL_Serverless_Gold_Views.sql
│   └── DDL_Dedicated_SQL_Pool_Optional.sql
│
├── .gitignore
└── README.md
```

---

# 🧠 Key Engineering Concepts Demonstrated

ApexCare demonstrates practical implementation of:

- Metadata-driven data ingestion
- Batch and real-time data processing
- Kafka-compatible event streaming
- Spark Structured Streaming
- Delta Lake Medallion Architecture
- SCD Type 2 dimensional processing
- Star-schema data modeling
- Data-quality processing
- Streaming checkpoint management
- SQL-based metadata control
- Pipeline auditing structures
- Azure Key Vault secret management
- Databricks secret scopes
- Unity Catalog governance
- Synapse Serverless SQL
- Power BI healthcare analytics
- Git-based source control
- CI validation with GitHub Actions
- Cost-conscious Azure architecture

---

# 📜 Project Scope

ApexCare is a **portfolio/reference healthcare data engineering implementation** built using synthetic data.

It demonstrates how Azure services can be combined to support:

```text
Source Systems
      ↓
Batch + Streaming Ingestion
      ↓
Lakehouse Processing
      ↓
Data Quality & Historical Tracking
      ↓
Dimensional Modeling
      ↓
Serverless SQL Serving
      ↓
Business Intelligence
```

The architecture is designed to demonstrate production-oriented engineering patterns while remaining practical for a portfolio-scale Azure environment.

---

## 📌 Disclaimer

All healthcare data used in this repository is **synthetically generated for educational and portfolio purposes**.

No real patient information, Protected Health Information (PHI), or production healthcare data is included.
