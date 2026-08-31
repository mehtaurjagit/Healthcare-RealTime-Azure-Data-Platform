# 🏥 APEXCARE — Enterprise Healthcare Data Engineering & Analytics Platform

[![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-blue?logo=microsoftazure)](https://azure.microsoft.com/en-us/products/data-factory/)
[![Azure Databricks](https://img.shields.io/badge/Azure-Databricks-red?logo=databricks)](https://azure.microsoft.com/en-us/products/databricks/)
[![Delta Lake](https://img.shields.io/badge/Delta-Lake-0052CC?logo=delta)](https://delta.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)](https://kafka.apache.org/)
[![Unity Catalog](https://img.shields.io/badge/Databricks-Unity%20Catalog-FF3621)](https://www.databricks.com/product/unity-catalog)
[![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-0078D4?logo=microsoftazure)](https://azure.microsoft.com/en-us/products/synapse-analytics/)
[![Power BI](https://img.shields.io/badge/Power-BI-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)

---

## 📌 Executive Summary

**ApexCare** is an end-to-end, enterprise-grade Healthcare Data Engineering and Real-Time Analytics Platform built on Microsoft Azure. It unifies clinical Electronic Health Records (EHR), hospital admission billing, lab diagnostic JSON feeds, and high-frequency ICU bedside telemetry streaming into a unified **Lakehouse Medallion Architecture** (Bronze → Silver → Gold).

The architecture leverages **Azure Data Factory** for dynamic metadata-driven batch ingestion, **Azure Event Hubs (Kafka protocol)** and **Databricks Structured Streaming** for real-time patient vitals, **Delta Lake** for ACID transactions and **SCD Type 2** patient history tracking, **Databricks Unity Catalog** for centralized data governance, **Azure Key Vault** for secret management, **Synapse Serverless SQL** for high-performance serving, and **Power BI** for executive clinical dashboards.

---

## 🏗️ Architecture & Data Flow

```
[ EHR CSVs / Lab JSONs ] ────► ADLS Gen2 (Raw) ──► ADF Metadata Engine ──► Bronze Delta
                                                                               │
[ ICU Bedside Vitals ] ──► Event Hubs (Kafka) ──► Databricks Streaming ────────┤
                                                                               ▼
                                                                     Silver Delta (Clean / SCD2)
                                                                               │
                                                                               ▼
                                                                     Gold Delta (Star Schema)
                                                                               │
                                                                               ▼
Power BI Executive Dashboard ◄── Synapse Serverless SQL ◄── Unity Catalog Governance
```

---

## 🛠️ Technology Stack

| Layer | Service / Technology | Implementation Details |
| :--- | :--- | :--- |
| **Storage / Data Lake** | **Azure ADLS Gen2** | Multi-container setup (`raw`, `bronze`, `silver`, `gold`, `synapse`, `system-checkpoints`) |
| **Control Plane** | **Azure SQL Serverless** | Metadata-driven ingestion engine (`meta.IngestionControl`) and audit logs (`audit.PipelineExecutionLog`, `audit.DataQualityLog`) |
| **Batch Ingestion** | **Azure Data Factory** | Parameterized pipeline `PL_Ingest_Source_To_Bronze` with Lookup & ForEach copy activities |
| **Streaming Ingestion** | **Azure Event Hubs & PySpark** | Kafka-compatible endpoint (`vitals-telemetry-hub`) streaming bedside vitals over SASL PLAIN |
| **Data Processing & Delta** | **Azure Databricks (PySpark)** | Medallion architecture, SCD Type 2 MERGE engine, schema enforcement, `OPTIMIZE` & `ZORDER` indexing |
| **Governance & Security** | **Unity Catalog & Key Vault** | Access Connector managed identity (`sc_apexcare_adls`), Key Vault Vault Access Policies, Secret Scope (`dbsecrets-apexcare`) |
| **Serving Layer** | **Synapse Serverless SQL** | Database `ApexCareGold` on `Built-in` endpoint querying Gold Delta/Parquet files directly |
| **Analytics & Reporting** | **Power BI Desktop** | Executive healthcare overview, clinical vitals risk alerts, and hospital revenue dashboards |
| **CI/CD & DevOps** | **GitHub Actions** | Automated validation, PySpark linting, ADF JSON verification, and deployment workflow |

---

## 💎 Medallion Lakehouse Architecture

### 1. Bronze Layer (Raw Ingestion)
- **Batch Data**: Landed from raw drop files into Delta format preserving original schema and adding metadata headers (`_ingested_at`, `_source_file`).
- **Streaming Telemetry**: Structured Streaming consuming bedside vitals (`HeartRate`, `BloodPressureSystolic`, `BloodPressureDiastolic`, `OxygenSaturation`, `BodyTemperature`) into `bronze.vitals_streaming`.

### 2. Silver Layer (Harmonization & SCD Type 2)
- **Data Quality & Quarantine**: Filters invalid timestamps, impossible vital ranges, and null keys, writing exceptions to `silver/_quarantine/` and logging metrics to `audit.DataQualityLog`.
- **Encounters & Admissions**: De-duplicates records using PySpark window function `ROW_NUMBER() OVER (PARTITION BY EncounterID ORDER BY AdmitTimestamp DESC)` and calculates `LengthOfStayHours`.
- **Patients Master (SCD Type 2)**: Tracks patient demographic history using Delta `MERGE INTO` statement with surrogate key `PatientSK = MD5(concat(PatientID, UpdatedTimestamp))` and metadata flags (`IsCurrent`, `EffectiveStartDate`, `EffectiveEndDate`).

### 3. Gold Layer (Star Schema & Optimizations)
- **Dimensional Modeling**:
  - `gold.dim_patient` (Replicated dimension)
  - `gold.dim_provider` (Replicated dimension)
  - `gold.dim_department` (Replicated dimension)
  - `gold.fact_patient_encounters` (Hash-distributed on `PatientSK`)
  - `gold.fact_vitals_telemetry` (Hash-distributed on `PatientSK`)
- **Delta Lake Indexing**: Executed `OPTIMIZE gold.fact_patient_encounters ZORDER BY (AdmitTimestamp, PatientSK)` for sub-second query performance.

---

## 🏛️ Unity Catalog & Governance Matrix

* **Metastore ID**: `azure:centralus:c8ef8bd4-7721-4d7a-92ef-9ad875efbc5f`
* **Catalog**: `dbw_apexcare_prod_eastus2`
* **Schemas**: `bronze`, `silver`, `gold`
* **Storage Credential**: `sc_apexcare_adls` backed by Azure Databricks Access Connector System-Assigned Managed Identity.
* **External Locations**: Pointing to `abfss://bronze@...`, `abfss://silver@...`, `abfss://gold@...` on `stapexcareprodeastus` storage account with **zero data copy**.

---

## 📊 Power BI Analytics Layer

Power BI connects directly to **Synapse Serverless SQL** (`synapseapexcare2026-ondemand.sql.azuresynapse.net`, database `ApexCareGold`) using DirectQuery/Import mode over 3 specialized views:

1. **`gold.vw_exec_encounter_summary`**: Departmental admission volumes, average length of stay, and billed revenue.
2. **`gold.vw_exec_critical_vitals`**: ICU bedside device telemetry readings, critical alert counts, and average SpO2/Heart Rate.
3. **`gold.vw_exec_revenue_by_payer`**: Financial breakdown by primary insurance payer (Medicare, Medicaid, Private) and state.

---

## 💰 Cost Optimization & Free Trial Architecture

Built under strict Azure Free Trial constraints (~$200 budget):
- **Single-Node Databricks Clusters**: Configured with 15-minute auto-termination.
- **Azure SQL Serverless**: Configured with 0.5 vCores minimum and 1-hour auto-pause.
- **Synapse Dedicated SQL Pool**: `sqldw_apexcare` (DW100c) is **PAUSED permanently** ($0.00/hr compute).
- **Synapse Serverless SQL**: Used for reporting (pay-per-query scanned = ~$0 cost).

---

## 📂 Repository Structure

```
.
├── .github/
│   └── workflows/
│       └── deploy_apexcare.yml         # GitHub Actions CI/CD pipeline
├── adf/
│   ├── linkedService/                  # ADF Linked Services JSON
│   └── pipeline/
│       └── PL_Ingest_Source_To_Bronze.json # Metadata-driven copy pipeline
├── architecture/
│   └── data_flow_and_security_matrix.md # Security & Secret Scope specification
├── databricks/
│   ├── bronze/                         # PySpark Streaming ingestion notebooks
│   ├── silver/                         # Silver transformations & SCD2 MERGE notebooks
│   ├── gold/                           # Gold Star Schema & Delta Z-Ordering notebooks
│   └── utilities/                      # Data Quality Engine & Quarantine module
├── datasets/                           # Synthetic healthcare batch data generators
├── docs/
│   ├── data_dictionary_and_erd.md      # Healthcare data dictionary & ERD model
│   └── interview_defense_guide.md      # Senior Data Engineer Interview Q&A
├── eventhub/
│   └── streaming_event_producer.py     # Python Kafka ICU bedside vitals producer
├── sql/
│   ├── metadata_control_db.sql         # Azure SQL control plane & audit DDL
│   └── seed_metadata_control_tables.sql# Metadata seed configuration scripts
└── synapse/
    └── DDL_Gold_Star_Schema.sql        # Synapse SQL DDL & analytical views
```

---

## 📜 License & Portfolio Usage

This project is created for professional portfolio demonstration and Azure Data Engineering technical interview defense.
