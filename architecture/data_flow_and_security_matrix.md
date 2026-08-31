# ApexCare Platform: Security Architecture & Access Control Matrix

This document details the security model, credential management, encryption standards, and Role-Based Access Control (RBAC) matrix for the ApexCare Real-Time Healthcare Data Platform.

---

## 🔒 Security Architecture Principles

1. **Zero Trust & Least Privilege Access**: Every service connection relies on system-assigned Azure Managed Identities with strictly scoped Role-Based Access Control (RBAC). No shared service keys or account passwords are hardcoded in notebooks or pipelines.
2. **Key Vault Integration**: External database connection strings, SAS tokens, Event Hub connection strings, and service principal secrets are stored in **Azure Key Vault** (`kv-apexcare-prod-01`) and mounted in Azure Databricks via **Key Vault-Backed Secret Scopes**.
3. **Encryption at Rest & in Transit**:
   - **In Transit**: All stream ingestion (Kafka protocol to Azure Event Hubs) and storage requests enforce TLS 1.2+ encryption.
   - **At Rest**: ADLS Gen2 storage accounts and Azure Synapse SQL Dedicated Pools utilize Azure Storage Service Encryption (SSE) with Microsoft-Managed Keys (or Customer-Managed Keys via Key Vault).
4. **Data Isolation (HIPAA Compliance)**: Protected Health Information (PHI) columns (e.g., `FirstName`, `LastName`, `AddressLine`) are quarantined during Silver layer transformation and masked via dynamic column masking in Gold serving views.

---

## 🔑 Key Vault Secret Scope Mapping

| Key Vault Secret Name | Secret Purpose | Accessing Service | Authorization Method |
| :--- | :--- | :--- | :--- |
| `evh-vitals-connection-string` | Event Hubs Kafka connection string | Azure Databricks | Databricks Secret Scope (`dbsecrets-apexcare`) |
| `sqldb-control-connection-string` | Azure SQL Metadata Control DB connection | Azure Data Factory | Managed Identity (`adf-apexcare-prod`) |
| `adls-gen2-access-key` | ADLS Gen2 Storage key fallback | Azure Databricks | Secret Scope (`dbsecrets-apexcare`) |
| `synapse-sql-admin-password` | Synapse SQL DW Admin credentials | Azure Data Factory | Key Vault Secret Linked Service |

---

## 🛡️ Azure Role-Based Access Control (RBAC) Matrix

```
+--------------------------+----------------------------+-------------------------------------+
| Identity / Principal     | Target Azure Resource      | Assigned Azure RBAC Role            |
+--------------------------+----------------------------+-------------------------------------+
| ADF Managed Identity     | ADLS Gen2 (Storage)        | Storage Blob Data Contributor       |
| ADF Managed Identity     | Key Vault                  | Key Vault Secrets User              |
| ADF Managed Identity     | Azure SQL Control DB       | db_datareader, db_datawriter        |
| ADF Managed Identity     | Azure Databricks           | Databricks Worker Host / Contributor|
| Databricks Managed ID    | ADLS Gen2 (Storage)        | Storage Blob Data Contributor       |
| Databricks Managed ID    | Key Vault                  | Key Vault Secrets User              |
| Synapse Managed ID       | ADLS Gen2 (Storage)        | Storage Blob Data Reader            |
| Power BI Service Account | Azure Synapse SQL DW       | db_datareader (Gold Schema Only)    |
+--------------------------+----------------------------+-------------------------------------+
```

---

## 🌐 Network Topology & Data Path Security

```
                                 [ PUBLIC INTERNET / BEDSIDE DEVICES ]
                                                  |
                                                  v (TLS 1.2 Encrypted Kafka Connection)
+---------------------------------------------------------------------------------------------------+
| AZURE SUBSCRIPTION (VNET: vnet-apexcare-prod-eastus)                                              |
|                                                                                                   |
|   +------------------------------------+          +-------------------------------------------+   |
|   | Subnet: snet-ingestion             |          | Subnet: snet-databricks                   |   |
|   | - Azure Event Hubs (Private Endpoint)|        | - Azure Databricks Control / Worker Nodes |   |
|   +------------------------------------+          +-------------------------------------------+   |
|                     |                                                   |                         |
|                     v (Private Link)                                    v (Private Endpoint)      |
|   +-------------------------------------------------------------------------------------------+   |
|   | Subnet: snet-storage                                                                      |   |
|   | - ADLS Gen2 Storage Account (Raw / Bronze / Silver / Gold)                                |   |
|   | - Access restricted via IP Firewall & VNet Service Endpoints                              |   |
|   +-------------------------------------------------------------------------------------------+   |
|                                                                                                   |
+---------------------------------------------------------------------------------------------------+
```
