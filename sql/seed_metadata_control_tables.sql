-- ================================================================================
-- ApexCare Real-Time Healthcare Data Platform
-- Seed Data Script for Ingestion Control Table
-- Target Table: meta.IngestionControl
-- ================================================================================

USE [ApexCare_ControlDB];
GO

TRUNCATE TABLE meta.IngestionControl;
GO

INSERT INTO meta.IngestionControl (
    SourceSystem,
    EntityName,
    SourceFormat,
    SourceFolderPath,
    TargetBronzePath,
    TargetSilverTable,
    IngestionType,
    WatermarkColumn,
    WatermarkValue,
    IsActive
)
VALUES
(
    'EPIC_EHR',
    'Patients',
    'CSV',
    'landing/ehr/patients/',
    'bronze/ehr/patients/',
    'silver.dim_patient_scd2',
    'INCREMENTAL_WATERMARK',
    'UpdatedTimestamp',
    '1970-01-01 00:00:00',
    1
),
(
    'EPIC_EHR',
    'Providers',
    'CSV',
    'landing/ehr/providers/',
    'bronze/ehr/providers/',
    'silver.dim_provider',
    'FULL_RELOAD',
    NULL,
    NULL,
    1
),
(
    'FACILITY_PM',
    'Departments',
    'CSV',
    'landing/pm/departments/',
    'bronze/pm/departments/',
    'silver.dim_department',
    'FULL_RELOAD',
    NULL,
    NULL,
    1
),
(
    'EPIC_EHR',
    'Encounters',
    'CSV',
    'landing/ehr/encounters/',
    'bronze/ehr/encounters/',
    'silver.fact_encounters',
    'INCREMENTAL_WATERMARK',
    'AdmitTimestamp',
    '1970-01-01 00:00:00',
    1
),
(
    'CERNER_LABS',
    'LabResults',
    'JSON',
    'landing/labs/lab_results/',
    'bronze/labs/lab_results/',
    'silver.fact_lab_results',
    'INCREMENTAL_WATERMARK',
    'ResultTimestamp',
    '1970-01-01 00:00:00',
    1
),
(
    'CLAIMS_ENGINE',
    'BillingClaims',
    'CSV',
    'landing/claims/billing_claims/',
    'bronze/claims/billing_claims/',
    'silver.fact_billing_claims',
    'INCREMENTAL_WATERMARK',
    'ClaimDate',
    '1970-01-01 00:00:00',
    1
),
(
    'BEDSIDE_IOT',
    'PatientVitals',
    'STREAMING',
    'eventhub/vitals-telemetry-hub',
    'bronze/telemetry/vitals_streaming/',
    'silver.fact_vitals_telemetry',
    'STREAMING',
    'event_timestamp',
    NULL,
    1
);
GO

SELECT * FROM meta.IngestionControl;
GO
