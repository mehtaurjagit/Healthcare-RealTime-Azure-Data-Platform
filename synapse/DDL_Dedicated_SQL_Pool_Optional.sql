-- ================================================================================
-- ApexCare Real-Time Healthcare Data Platform
-- Azure Synapse Dedicated SQL Pool (Data Warehouse) DDL
-- Target Engine: Azure Synapse Dedicated SQL Pool (DW100c)
-- ================================================================================

CREATE SCHEMA [gold];
GO

-- --------------------------------------------------------------------------------
-- 1. DIMENSION TABLE: Dim_Patient (REPLICATE Distribution)
-- Replicated across all compute nodes to eliminate join shuffling
-- --------------------------------------------------------------------------------
IF OBJECT_ID('gold.Dim_Patient', 'U') IS NOT NULL
    DROP TABLE gold.Dim_Patient;

CREATE TABLE gold.Dim_Patient (
    PatientSK VARCHAR(32) NOT NULL,
    PatientID VARCHAR(50) NOT NULL,
    MedicalRecordNumber VARCHAR(50) NOT NULL,
    FirstName VARCHAR(100) NOT NULL,
    LastName VARCHAR(100) NOT NULL,
    DateOfBirth DATE NOT NULL,
    Gender VARCHAR(20) NOT NULL,
    PrimaryLanguage VARCHAR(50) NULL,
    AddressLine VARCHAR(255) NULL,
    City VARCHAR(100) NULL,
    State VARCHAR(2) NULL,
    ZipCode VARCHAR(10) NULL,
    PrimaryInsurancePayer VARCHAR(100) NULL,
    EffectiveStartDate DATETIME2 NOT NULL
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- --------------------------------------------------------------------------------
-- 2. DIMENSION TABLE: Dim_Provider (REPLICATE Distribution)
-- --------------------------------------------------------------------------------
IF OBJECT_ID('gold.Dim_Provider', 'U') IS NOT NULL
    DROP TABLE gold.Dim_Provider;

CREATE TABLE gold.Dim_Provider (
    ProviderSK VARCHAR(32) NOT NULL,
    ProviderID VARCHAR(50) NOT NULL,
    NPI_Number VARCHAR(10) NOT NULL,
    ProviderName VARCHAR(150) NOT NULL,
    Specialty VARCHAR(100) NOT NULL,
    DepartmentID VARCHAR(50) NOT NULL
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- --------------------------------------------------------------------------------
-- 3. DIMENSION TABLE: Dim_Department (REPLICATE Distribution)
-- --------------------------------------------------------------------------------
IF OBJECT_ID('gold.Dim_Department', 'U') IS NOT NULL
    DROP TABLE gold.Dim_Department;

CREATE TABLE gold.Dim_Department (
    DepartmentSK VARCHAR(32) NOT NULL,
    DepartmentID VARCHAR(50) NOT NULL,
    DepartmentName VARCHAR(100) NOT NULL,
    FacilityName VARCHAR(150) NOT NULL
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- --------------------------------------------------------------------------------
-- 4. FACT TABLE: Fact_PatientEncounters (HASH Distribution on PatientSK)
-- Hash-distributed across 60 compute distributions for parallel MPP queries
-- --------------------------------------------------------------------------------
IF OBJECT_ID('gold.Fact_PatientEncounters', 'U') IS NOT NULL
    DROP TABLE gold.Fact_PatientEncounters;

CREATE TABLE gold.Fact_PatientEncounters (
    EncounterID VARCHAR(50) NOT NULL,
    PatientSK VARCHAR(32) NULL,
    ProviderSK VARCHAR(32) NULL,
    DepartmentSK VARCHAR(32) NULL,
    EncounterType VARCHAR(50) NOT NULL,
    AdmitTimestamp DATETIME2 NOT NULL,
    DischargeTimestamp DATETIME2 NULL,
    LengthOfStayHours DECIMAL(10,2) NULL,
    AdmitReason VARCHAR(255) NULL,
    DischargeStatus VARCHAR(50) NULL,
    TotalBilledAmount DECIMAL(12,2) NOT NULL,
    _created_at DATETIME2 NOT NULL
)
WITH (
    DISTRIBUTION = HASH(PatientSK),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- --------------------------------------------------------------------------------
-- 5. FACT TABLE: Fact_VitalsTelemetry (HASH Distribution on PatientSK)
-- Real-time bedside telemetry fact table
-- --------------------------------------------------------------------------------
IF OBJECT_ID('gold.Fact_VitalsTelemetry', 'U') IS NOT NULL
    DROP TABLE gold.Fact_VitalsTelemetry;

CREATE TABLE gold.Fact_VitalsTelemetry (
    TelemetryID VARCHAR(100) NOT NULL,
    PatientSK VARCHAR(32) NULL,
    EncounterID VARCHAR(50) NOT NULL,
    DeviceID VARCHAR(50) NOT NULL,
    HeartRate INT NOT NULL,
    BloodPressureSystolic INT NOT NULL,
    BloodPressureDiastolic INT NOT NULL,
    OxygenSaturation DECIMAL(5,2) NOT NULL,
    BodyTemperature DECIMAL(4,1) NOT NULL,
    IsCriticalAlert BIT NOT NULL,
    EventTimestamp DATETIME2 NOT NULL
)
WITH (
    DISTRIBUTION = HASH(PatientSK),
    CLUSTERED COLUMNSTORE INDEX
);
GO
