# 🎯 APEXCARE — Senior Azure Data Engineer Interview Defense Guide

This document equips you with battle-tested interview talking points, architectural trade-off rationales, and deep-dive technical answers for presenting the **ApexCare Healthcare Data Platform** in senior Azure Data Engineering interviews.

---

## 1. 🚀 30-Second Elevator Pitch

> *"In my recent project, **ApexCare**, I architected an enterprise real-time healthcare data platform on Azure handling both legacy EHR batch ingestion and live bedside ICU telemetry streaming. I designed a metadata-driven ADF ingestion pipeline control plane in Azure SQL Serverless, streamed high-frequency vitals over Event Hubs (Kafka protocol) into Databricks PySpark Structured Streaming, built a Bronze-Silver-Gold Medallion Lakehouse with SCD Type 2 patient tracking, applied Delta Lake Z-Ordering for sub-second queries, and governed the entire dataset using Databricks Unity Catalog without moving raw files. Finally, I exposed Gold views via Synapse Serverless SQL for executive Power BI dashboards, achieving zero-compute standby costs."*

---

## 2. 💡 Deep-Dive Architectural Talking Points

### Q1: Why did you choose a hybrid Medallion Lakehouse + Synapse Serverless design?
* **Answer**: *"Databricks with Delta Lake handles high-throughput ETL, streaming ingestion, schema enforcement, and complex PySpark transformations (like SCD Type 2 MERGE). However, enterprise BI tools like Power BI and business analysts require a SQL interface. Instead of provisioning an expensive dedicated Data Warehouse (which incurs 24/7 compute costs), I created external Star Schema views in **Synapse Serverless SQL** over the Gold Parquet/Delta files. This provides sub-second ANSI-SQL query performance for Power BI while keeping compute costs strictly pay-per-query (~$5 per TB scanned)."*

### Q2: How did you implement Slowly Changing Dimension Type 2 (SCD2) in PySpark?
* **Answer**: *"For `dim_patient_scd2`, patient demographic updates from EHR batch files must preserve historical records for clinical auditing. I generated a deterministic surrogate key `PatientSK = MD5(concat(PatientID, UpdatedTimestamp))`. In PySpark, I used Delta Lake's native `MERGE INTO` statement with conditions:
  - When matched on `PatientID` and source attributes changed AND `target.IsCurrent = true`, I update `EffectiveEndDate = source.UpdatedTimestamp` and set `IsCurrent = false`.
  - I then insert the new record with `IsCurrent = true` and `EffectiveStartDate = source.UpdatedTimestamp`.
  Using Delta Lake's ACID transaction log ensures zero data corruption during concurrent writes."*

### Q3: How does your real-time bedside telemetry ingestion work?
* **Answer**: *"ICU bedside monitors emit high-frequency vitals (HeartRate, BloodPressure, SpO2) into **Azure Event Hubs** using its Kafka-compatible endpoint (`evh-apexcare-prod-eastus.servicebus.windows.net:9093`). In Databricks, I configured PySpark Structured Streaming with `readStream.format("kafka")` using SASL PLAIN authentication with secrets fetched dynamically from Azure Key Vault. Data streams into Bronze Delta storage with 10-second trigger intervals, utilizing `option("checkpointLocation", ...)` to guarantee exactly-once fault-tolerant ingestion."*

### Q4: How did you handle Data Governance and Security?
* **Answer**: *"I implemented a zero-trust security model:
  1. **Unity Catalog**: Governs ADLS Gen2 data using an Azure Access Connector and Managed Identity (`sc_apexcare_adls`). External tables in `dbw_apexcare_prod_eastus2` catalog (`bronze`, `silver`, `gold` schemas) expose data without copying bytes.
  2. **Azure Key Vault**: Stored SQL passwords and Kafka connection strings in `kv-apexcare-prod-eastus` using Vault Access Policies. Databricks secret scope `dbsecrets-apexcare` and ADF Key Vault Linked Services fetch credentials at runtime, ensuring zero hard-coded secrets in code."*

### Q5: How did you ensure data quality and operational observability?
* **Answer**: *"I built a lightweight operational audit and data quality framework backed by Azure SQL Serverless:
  - **ADF Pipelines**: Execute `Script` activities logging `STARTED`, `SUCCESS`, `FAILED` status, run IDs, and `filesWritten` into `audit.PipelineExecutionLog`.
  - **Databricks Silver Layer**: Evaluates healthcare rules (e.g. non-null keys, valid heart rate ranges 30–220, valid length of stay). Valid records proceed to Silver Delta tables; failing records route to `_quarantine/` while execution metrics are written directly to `audit.DataQualityLog` via Spark JDBC."*

---

## 3. 🛠️ Key Technology Decision Matrix

| Technology | Why Used Over Alternatives? | Enterprise Benefit |
| :--- | :--- | :--- |
| **Azure Event Hubs (Kafka Protocol)** | Native Azure PaaS vs self-managed Apache Kafka EC2/VMs | Zero cluster management, instant scalability, native Kafka API support. |
| **Azure SQL Serverless Control DB** | Dynamic metadata-driven ADF copy vs hard-coded static pipelines | Auto-pauses when idle ($0 cost), allows adding new source tables by inserting 1 SQL row. |
| **Delta Lake (ACID Lakehouse)** | Standard Parquet vs Delta Parquet | ACID transactions, time travel, schema enforcement, `MERGE` for SCD2, `Z-ORDER` indexing. |
| **Unity Catalog** | Hive Metastore vs Unity Catalog | Centralized access control, lineage, schema-level security, cloud-agnostic governance. |
| **Synapse Serverless SQL** | Dedicated SQL Pool DW100c vs Serverless SQL | Eliminates $1,100+/mo dedicated DW compute; queries Parquet directly on-demand. |

---

## 4. 📊 Project Metrics to Quote in Interviews

* **Batch Ingestion**: Metadata engine ingests 6 core healthcare domains dynamically.
* **Streaming Throughput**: Real-time Kafka telemetry processing with sub-second latency and 100% checkpoint resilience.
* **Data Scale**: Simulated scale over 2,000 patient master records, 5,000 hospital encounters, 300 telemetry streams, 300 providers, and 25 clinical departments.
* **Cost Efficiency**: **> 95% cost reduction** achieved via single-node auto-terminating Databricks clusters, serverless Azure SQL, paused DW pools, and serverless Synapse queries.
