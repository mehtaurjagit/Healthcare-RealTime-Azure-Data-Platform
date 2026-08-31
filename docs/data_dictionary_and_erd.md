# ApexCare Platform: Data Dictionary & ERD Specification

This document defines the schema contracts, primary/foreign key relationships, entity grains, and datatype mappings for all datasets processed across the ApexCare Real-Time Healthcare Data Platform.

---

## 📐 Entity Relationship Diagram (ERD)

```
                           +------------------------+
                           |    Dim_Department      |
                           +------------------------+
                           | PK: DepartmentSK       |
                           |     DepartmentID (NK)  |
                           |     DepartmentName     |
                           |     FacilityName       |
                           +------------------------+
                                       ^
                                       | (1:N)
+-------------------+      +------------------------+      +--------------------+
|    Dim_Patient    |      | Fact_PatientEncounters |      |    Dim_Provider    |
+-------------------+      +------------------------+      +--------------------+
| PK: PatientSK     |      | PK: EncounterID        |      | PK: ProviderSK     |
|     PatientID (NK)|<---- | FK: PatientSK          | ---->|     ProviderID (NK)|
|     MRN           | (N:1)| FK: ProviderSK         | (N:1)|     NPI_Number     |
|     FirstName     |      | FK: DepartmentSK       |      |     ProviderName   |
|     LastName      |      |     AdmitTimestamp     |      |     Specialty      |
|     SCD_Current   |      |     DischargeTimestamp |      +--------------------+
|     SCD_StartDate |      |     EncounterType      |
|     SCD_EndDate   |      |     DischargeStatus    |
+-------------------+      |     TotalBilledAmount  |
          ^                +------------------------+
          | (1:N)                      ^
          |                            | (1:N)
+------------------------+ +------------------------+
| Fact_VitalsTelemetry   | |   Fact_BillingClaims   |
+------------------------+ +------------------------+
| PK: TelemetryID        | | PK: ClaimID            |
| FK: PatientSK          | | FK: EncounterID        |
|     DeviceID           | | FK: PatientSK          |
|     HeartRate          | |     ClaimAmount        |
|     BloodPressureSystolic|     InsurancePayer     |
|     OxygenSaturation   | |     ClaimStatus        |
|     EventTimestamp     | +------------------------+
+------------------------+
```

---

## 📖 Data Dictionary

### 1. `Dim_Patient` (Slowly Changing Dimension Type 2)
* **Description**: Master patient demographic profile. Tracks changes over time in address, phone number, and insurance coverage using SCD Type 2.
* **Grain**: One record per patient per version state (`PatientSK`).

| Column Name | Datatype | Nullable | Key Type | Business Rules & Notes |
| :--- | :--- | :--- | :--- | :--- |
| `PatientSK` | BIGINT | No | PK (Surrogate) | Hash surrogate key (`MD5(PatientID + EffectiveStartDate)`) |
| `PatientID` | VARCHAR(50) | No | Business Key | Natural identifier from EHR system |
| `MedicalRecordNumber` | VARCHAR(50) | No | Natural Key | Enterprise MRN |
| `FirstName` | VARCHAR(100) | No | None | PII (Masked in non-prod environments) |
| `LastName` | VARCHAR(100) | No | None | PII (Masked in non-prod environments) |
| `DateOfBirth` | DATE | No | None | Used for age calculation |
| `Gender` | VARCHAR(20) | No | None | Standardized ('M', 'F', 'OTHER', 'UNKNOWN') |
| `PrimaryLanguage` | VARCHAR(50) | Yes | None | |
| `AddressLine` | VARCHAR(255) | Yes | None | Tracked for SCD2 changes |
| `City` | VARCHAR(100) | Yes | None | |
| `State` | VARCHAR(2) | Yes | None | 2-character ISO code |
| `ZipCode` | VARCHAR(10) | Yes | None | |
| `PrimaryInsurancePayer`| VARCHAR(100)| Yes | None | Tracked for SCD2 changes |
| `IsCurrent` | BOOLEAN | No | Metadata | `TRUE` if active version record; `FALSE` if historical |
| `EffectiveStartDate` | TIMESTAMP | No | Metadata | Timestamp when row state became active |
| `EffectiveEndDate` | TIMESTAMP | Yes | Metadata | Timestamp when row state expired (`9999-12-31 23:59:59` if current) |

---

### 2. `Dim_Provider` (Dimension)
* **Description**: Attending physician, surgeon, or nurse practitioner profile.
* **Grain**: One record per medical provider (`ProviderSK`).

| Column Name | Datatype | Nullable | Key Type | Business Rules & Notes |
| :--- | :--- | :--- | :--- | :--- |
| `ProviderSK` | BIGINT | No | PK (Surrogate) | Hash surrogate key (`MD5(ProviderID)`) |
| `ProviderID` | VARCHAR(50) | No | Business Key | Internal EHR Provider Identifier |
| `NPI_Number` | VARCHAR(10) | No | Natural Key | National Provider Identifier (10-digit standard) |
| `ProviderName` | VARCHAR(150) | No | None | Full legal provider name |
| `Specialty` | VARCHAR(100) | No | None | Clinical specialty (e.g., 'Cardiology', 'ICU Critical Care') |
| `DepartmentID` | VARCHAR(50) | No | FK | Links to `Dim_Department` |

---

### 3. `Fact_PatientEncounters` (Fact Table)
* **Description**: Tracks hospital admissions, Emergency Department visits, outpatient appointments, bed transfers, and discharges.
* **Grain**: One record per patient encounter (`EncounterID`).

| Column Name | Datatype | Nullable | Key Type | Business Rules & Notes |
| :--- | :--- | :--- | :--- | :--- |
| `EncounterID` | VARCHAR(50) | No | PK | Unique encounter reference identifier |
| `PatientSK` | BIGINT | No | FK | Points to active `Dim_Patient` version at admit time |
| `ProviderSK` | BIGINT | No | FK | Points to `Dim_Provider` attending physician |
| `DepartmentSK` | BIGINT | No | FK | Points to `Dim_Department` admitting ward |
| `EncounterType` | VARCHAR(50) | No | None | 'EMERGENCY', 'INPATIENT', 'OUTPATIENT', 'ICU' |
| `AdmitTimestamp` | TIMESTAMP | No | Degenerate | Timestamp of admission/check-in |
| `DischargeTimestamp`| TIMESTAMP | Yes | Degenerate | Null if patient currently admitted |
| `LengthOfStayHours` | DECIMAL(10,2)| Yes | Metric | Computed duration (`DischargeTimestamp - AdmitTimestamp`) |
| `AdmitReason` | VARCHAR(255) | Yes | None | Chief complaint / clinical code |
| `DischargeStatus` | VARCHAR(50) | Yes | None | 'HOME', 'TRANSFERRED', 'DECEASED', 'AMA' |
| `TotalBilledAmount` | DECIMAL(12,2)| No | Metric | Total facility charge |

---

### 4. `Fact_VitalsTelemetry` (Real-Time Streaming Fact Table)
* **Description**: Bedside patient monitoring telemetry emitted from ICU and ED monitors in real time via Azure Event Hubs.
* **Grain**: One event per device reading per second (`TelemetryID`).

| Column Name | Datatype | Nullable | Key Type | Business Rules & Notes |
| :--- | :--- | :--- | :--- | :--- |
| `TelemetryID` | VARCHAR(100) | No | PK | UUID emitted by device streaming producer |
| `PatientSK` | BIGINT | No | FK | Resolved surrogate key for target patient |
| `EncounterID` | VARCHAR(50) | No | FK | Associated active hospital encounter ID |
| `DeviceID` | VARCHAR(50) | No | Degenerate | Bedside monitor hardware serial number |
| `HeartRate` | INT | No | Metric | Beats per minute (Normal range: 60 - 100) |
| `BloodPressureSystolic`| INT | No | Metric | mmHg (Normal range: 90 - 120) |
| `BloodPressureDiastolic`| INT | No | Metric | mmHg (Normal range: 60 - 80) |
| `OxygenSaturation` | DECIMAL(5,2) | No | Metric | SpO2 percentage (Alert if < 92%) |
| `BodyTemperature` | DECIMAL(4,1) | No | Metric | Fahrenheit |
| `IsCriticalAlert` | BOOLEAN | No | Flag | Derived flag: `TRUE` if vitals exceed critical thresholds |
| `EventTimestamp` | TIMESTAMP | No | Event Time | Timestamp generated at monitor device |
| `IngestionTimestamp`| TIMESTAMP | No | System | Timestamp when consumed by Databricks stream |
