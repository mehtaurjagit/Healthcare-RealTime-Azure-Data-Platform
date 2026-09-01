-- ============================================================
-- ApexCare Real-Time Healthcare Data Platform
-- Azure Synapse Serverless SQL - Gold Serving Views
-- Database: ApexCareGold
-- Source: ADLS Gen2 Gold Parquet files
-- ============================================================

CREATE SCHEMA gold;
GO

-- ============================================================
-- Dimension Views
-- ============================================================

CREATE VIEW gold.vw_Dim_Department AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_department/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) AS r;
GO

CREATE VIEW gold.vw_Dim_Patient AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_patient/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) AS r;
GO

CREATE VIEW gold.vw_Dim_Provider AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_provider/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) AS r;
GO

-- ============================================================
-- Fact Views
-- ============================================================

CREATE VIEW gold.vw_Fact_PatientEncounters AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_patient_encounters/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) AS r;
GO

CREATE VIEW gold.vw_Fact_VitalsTelemetry AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_vitals_telemetry/*.parquet',
    DATA_SOURCE = 'gold_adls',
    FORMAT = 'PARQUET'
) AS r;
GO

-- ============================================================
-- Executive Analytical Views
-- ============================================================

CREATE VIEW gold.vw_exec_critical_vitals AS
SELECT
    vt.DeviceID,
    vt.EncounterID,
    COUNT(*) AS TotalReadings,
    SUM(CAST(vt.IsCriticalAlert AS INT)) AS CriticalAlerts,
    AVG(CAST(vt.HeartRate AS FLOAT)) AS AvgHeartRate,
    AVG(CAST(vt.OxygenSaturation AS FLOAT)) AS AvgOxygenSaturation,
    MIN(vt.EventTimestamp) AS FirstReading,
    MAX(vt.EventTimestamp) AS LastReading
FROM gold.vw_Fact_VitalsTelemetry vt
GROUP BY
    vt.DeviceID,
    vt.EncounterID;
GO

CREATE VIEW gold.vw_exec_encounter_summary AS
SELECT
    dd.DepartmentName,
    dd.FacilityName,
    fe.EncounterType,
    COUNT(fe.EncounterID) AS TotalEncounters,
    AVG(fe.LengthOfStayHours) AS AvgLengthOfStayHours,
    SUM(fe.TotalBilledAmount) AS TotalRevenue,
    AVG(fe.TotalBilledAmount) AS AvgBilledAmount
FROM gold.vw_Fact_PatientEncounters fe
LEFT JOIN gold.vw_Dim_Department dd
    ON fe.DepartmentSK = dd.DepartmentSK
GROUP BY
    dd.DepartmentName,
    dd.FacilityName,
    fe.EncounterType;
GO

CREATE VIEW gold.vw_exec_revenue_by_payer AS
SELECT
    dp.PrimaryInsurancePayer,
    dp.State,
    fe.EncounterType,
    COUNT(fe.EncounterID) AS TotalEncounters,
    SUM(fe.TotalBilledAmount) AS TotalRevenue,
    AVG(fe.LengthOfStayHours) AS AvgLOS
FROM gold.vw_Fact_PatientEncounters fe
LEFT JOIN gold.vw_Dim_Patient dp
    ON fe.PatientSK = dp.PatientSK
GROUP BY
    dp.PrimaryInsurancePayer,
    dp.State,
    fe.EncounterType;
GO