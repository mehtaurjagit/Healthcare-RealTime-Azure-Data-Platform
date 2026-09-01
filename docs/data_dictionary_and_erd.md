# ApexCare Platform: Data Dictionary & ERD Specification

This document defines the schema contracts, primary/foreign key relationships, entity grains, and datatype mappings for the analytics datasets implemented across the ApexCare Real-Time Healthcare Data Platform.

---

## 📐 Entity Relationship Diagram (ERD)

```text
                           +------------------------+
                           |     Dim_Department     |
                           +------------------------+
                           | PK: DepartmentSK       |
                           |     DepartmentID (NK)  |
                           |     DepartmentName     |
                           |     FacilityName       |
                           +------------------------+
                                      ^
                                      |
                                    (1:N)
                                      |
+--------------------+     +--------------------------+     +--------------------+
|    Dim_Patient     |     | Fact_PatientEncounters  |     |    Dim_Provider    |
+--------------------+     +--------------------------+     +--------------------+
| PK: PatientSK      |<----| FK: PatientSK            |---->| PK: ProviderSK     |
|     PatientID (NK) | N:1 | FK: ProviderSK           | N:1 |     ProviderID (NK)|
|     MRN            |     | FK: DepartmentSK         |     |     NPI_Number     |
|     FirstName      |     | PK: EncounterID          |     |     ProviderName   |
|     LastName       |     |     AdmitTimestamp       |     |     Specialty      |
|     SCD_Current    |     |     DischargeTimestamp   |     |     DepartmentID   |
|     SCD_StartDate  |     |     EncounterType        |     +--------------------+
|     SCD_EndDate    |     |     DischargeStatus      |
+--------------------+     |     TotalBilledAmount    |
          ^                +--------------------------+
          |
        (1:N)
          |
+--------------------------+
| Fact_VitalsTelemetry     |
+--------------------------+
| PK: TelemetryID          |
| FK: PatientSK            |
| FK: EncounterID          |
|     DeviceID             |
|     HeartRate            |
|     BloodPressureSystolic|
|     OxygenSaturation     |
|     EventTimestamp       |
+--------------------------+
```

### Relationship Summary

- `Dim_Patient` → `Fact_PatientEncounters`: **1:N**
- `Dim_Provider` → `Fact_PatientEncounters`: **1:N**
- `Dim_Department` → `Fact_PatientEncounters`: **1:N**
- `Dim_Patient` → `Fact_VitalsTelemetry`: **1:N**
- `Fact_PatientEncounters` → `Fact_VitalsTelemetry`: **1:N** through `EncounterID`

The implemented Gold analytics model therefore consists of **three dimensions and two fact tables**.

---

## 📖 Data Dictionary

### 1. `Dim_Patient` (Slowly Changing Dimension Type 2)

- **Description**: Master patient demographic profile. Tracks changes over time in address, phone number, and insurance coverage using SCD Type 2.
- **Grain**: One record per patient per version state (`PatientSK`).

| Column Name | Datatype | Nullable | Key Type | Business Rules & Notes |
|---|---|---|---|---|
| `PatientSK` | BIGINT | No | PK (Surrogate) | Hash surrogate key (`MD5(PatientID + EffectiveStartDate)`) |
| `PatientID` | VARCHAR(50) | No | Business Key | Natural identifier from EHR system |
| `MedicalRecordNumber` | VARCHAR(50) | No | Natural Key | Enterprise MRN |
| `FirstName` | VARCHAR(100) | No | None | Synthetic patient demographic attribute |
| `LastName` | VARCHAR(100) | No | None | Synthetic patient demographic attribute |
| `DateOfBirth` | DATE | No | None | Used for age calculation |
| `Gender` | VARCHAR(20) | No | None | Standardized gender value |
| `PrimaryLanguage` | VARCHAR(50) | Yes | None | Patient's primary language |
| `AddressLine` | VARCHAR(255) | Yes | None | Tracked for SCD Type 2 changes |
| `City` | VARCHAR(100) | Yes | None | Patient city |
| `State` | VARCHAR(2) | Yes | None | State code |
| `ZipCode` | VARCHAR(10) | Yes | None | Postal code |
| `PrimaryInsurancePayer` | VARCHAR(100) | Yes | None | Tracked for SCD Type 2 changes |
| `IsCurrent` | BOOLEAN | No | Metadata | `TRUE` if active version; `FALSE` if historical |
| `EffectiveStartDate` | TIMESTAMP | No | Metadata | Timestamp when row state became active |
| `EffectiveEndDate` | TIMESTAMP | Yes | Metadata | Timestamp when row state expired |

---

### 2. `Dim_Provider` (Dimension)

- **Description**: Attending physician, surgeon, or nurse practitioner profile.
- **Grain**: One record per medical provider (`ProviderSK`).

| Column Name | Datatype | Nullable | Key Type | Business Rules & Notes |
|---|---|---|---|---|
| `ProviderSK` | BIGINT | No | PK (Surrogate) | Hash surrogate key derived from `ProviderID` |
| `ProviderID` | VARCHAR(50) | No | Business Key | Internal EHR provider identifier |
| `NPI_Number` | VARCHAR(10) | No | Natural Key | National Provider Identifier |
| `ProviderName` | VARCHAR(150) | No | None | Provider name |
| `Specialty` | VARCHAR(100) | No | None | Clinical specialty |
| `DepartmentID` | VARCHAR(50) | No | FK / Business Reference | Links provider to department source identifier |

---

### 3. `Dim_Department` (Dimension)

- **Description**: Represents hospital departments and their associated healthcare facilities. Supports departmental analysis of encounters, utilization, length of stay, and revenue.
- **Grain**: One record per hospital department (`DepartmentSK`).

| Column Name | Datatype | Nullable | Key Type | Business Rules & Notes |
|---|---|---|---|---|
| `DepartmentSK` | BIGINT | No | PK (Surrogate) | Surrogate key for the department dimension |
| `DepartmentID` | VARCHAR(50) | No | Business Key | Natural department identifier from the source system |
| `DepartmentName` | VARCHAR(100) | No | None | Name of the clinical or operational department |
| `FacilityName` | VARCHAR(150) | No | None | Healthcare facility associated with the department |

---

### 4. `Fact_PatientEncounters` (Fact Table)

- **Description**: Tracks hospital admissions, Emergency Department visits, outpatient appointments, transfers, and discharges.
- **Grain**: One record per patient encounter (`EncounterID`).

| Column Name | Datatype | Nullable | Key Type | Business Rules & Notes |
|---|---|---|---|---|
| `EncounterID` | VARCHAR(50) | No | PK | Unique encounter reference identifier |
| `PatientSK` | BIGINT | No | FK | Points to the appropriate `Dim_Patient` record |
| `ProviderSK` | BIGINT | No | FK | Points to `Dim_Provider` |
| `DepartmentSK` | BIGINT | No | FK | Points to `Dim_Department` |
| `EncounterType` | VARCHAR(50) | No | None | Encounter classification |
| `AdmitTimestamp` | TIMESTAMP | No | Degenerate | Admission/check-in timestamp |
| `DischargeTimestamp` | TIMESTAMP | Yes | Degenerate | Discharge timestamp |
| `LengthOfStayHours` | DECIMAL(10,2) | Yes | Metric | Calculated encounter duration |
| `AdmitReason` | VARCHAR(255) | Yes | None | Admission reason / clinical description |
| `DischargeStatus` | VARCHAR(50) | Yes | None | Final encounter disposition |
| `TotalBilledAmount` | DECIMAL(12,2) | No | Metric | Total billed amount associated with the encounter |

---

### 5. `Fact_VitalsTelemetry` (Real-Time Streaming Fact Table)

- **Description**: Bedside patient-monitoring telemetry emitted from simulated ICU/clinical monitors and ingested in real time through Azure Event Hubs.
- **Grain**: One telemetry event per device reading (`TelemetryID`).

| Column Name | Datatype | Nullable | Key Type | Business Rules & Notes |
|---|---|---|---|---|
| `TelemetryID` | VARCHAR(100) | No | PK | Unique telemetry-event identifier |
| `PatientSK` | BIGINT | No | FK | Resolved patient surrogate key |
| `EncounterID` | VARCHAR(50) | No | FK | Associated patient encounter |
| `DeviceID` | VARCHAR(50) | No | Degenerate | Bedside monitoring-device identifier |
| `HeartRate` | INT | No | Metric | Heart rate in beats per minute |
| `BloodPressureSystolic` | INT | No | Metric | Systolic blood pressure |
| `BloodPressureDiastolic` | INT | No | Metric | Diastolic blood pressure |
| `OxygenSaturation` | DECIMAL(5,2) | No | Metric | SpO2 percentage |
| `BodyTemperature` | DECIMAL(4,1) | No | Metric | Patient body temperature |
| `IsCriticalAlert` | BOOLEAN | No | Flag | Derived critical-vitals indicator |
| `EventTimestamp` | TIMESTAMP | No | Event Time | Timestamp generated for the telemetry event |
| `IngestionTimestamp` | TIMESTAMP | No | System | Timestamp when the event is consumed by the processing layer |

---

## ⭐ Gold Star Schema

The implemented ApexCare Gold analytical model contains:

```text
Dimensions
├── Dim_Patient
├── Dim_Provider
└── Dim_Department

Facts
├── Fact_PatientEncounters
└── Fact_VitalsTelemetry
```

### Primary Analytical Relationships

```text
Dim_Patient
     │
     ├──────────────► Fact_PatientEncounters
     │                         ▲
     │                         │
     │              Dim_Provider
     │
     │              Dim_Department
     │                         │
     │                         ▼
     └──────────────► Fact_VitalsTelemetry
```

`Fact_PatientEncounters` provides the core hospital encounter and financial measures, while `Fact_VitalsTelemetry` provides the real-time clinical telemetry fact stream.

---

## 📊 Downstream Serving

The Gold model is exposed through Azure Synapse Serverless SQL using the following base views:

```text
gold.vw_Dim_Patient
gold.vw_Dim_Provider
gold.vw_Dim_Department
gold.vw_Fact_PatientEncounters
gold.vw_Fact_VitalsTelemetry
```

Additional executive analytical views include:

```text
gold.vw_exec_encounter_summary
gold.vw_exec_critical_vitals
gold.vw_exec_revenue_by_payer
```

These views provide the serving layer used by the Power BI analytics dashboards.

---

## 📌 Data Scope

All healthcare data represented in this specification is **synthetically generated for educational and portfolio purposes**.

No real patient information, Protected Health Information (PHI), or production healthcare data is included.
