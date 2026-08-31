-- ================================================================================
-- ApexCare Real-Time Healthcare Data Platform
-- Metadata Control & Orchestration Database DDL
-- Database Target: Azure SQL Database / Synapse Control DB
-- ================================================================================

CREATE SCHEMA [meta];
GO

CREATE SCHEMA [audit];
GO

-- --------------------------------------------------------------------------------
-- 1. CONTROL TABLE: Source Entity Ingestion Configuration
-- Controls dynamic metadata-driven ADF copy pipelines and Databricks ingestion
-- --------------------------------------------------------------------------------
IF OBJECT_ID('meta.IngestionControl', 'U') IS NOT NULL
    DROP TABLE meta.IngestionControl;

CREATE TABLE meta.IngestionControl (
    SourceID INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystem VARCHAR(50) NOT NULL,          -- e.g., 'EPIC_EHR', 'CERNER_LABS', 'FACILITY_PM'
    EntityName VARCHAR(100) NOT NULL,          -- e.g., 'Patients', 'Encounters', 'Claims'
    SourceFormat VARCHAR(20) NOT NULL,          -- e.g., 'CSV', 'JSON', 'PARQUET', 'SQL'
    SourceFolderPath VARCHAR(255) NOT NULL,     -- e.g., 'landing/ehr/patients/'
    TargetBronzePath VARCHAR(255) NOT NULL,     -- e.g., 'bronze/ehr/patients/'
    TargetSilverTable VARCHAR(100) NOT NULL,    -- e.g., 'silver.dim_patient_scd2'
    IngestionType VARCHAR(20) NOT NULL,         -- 'FULL_RELOAD', 'INCREMENTAL_WATERMARK', 'STREAMING'
    WatermarkColumn VARCHAR(50) NULL,           -- e.g., 'UpdatedTimestamp'
    WatermarkValue DATETIME2 NULL,              -- Last ingested timestamp for incremental loads
    IsActive BIT DEFAULT 1 NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETUTCDATE() NOT NULL,
    UpdatedDate DATETIME2 DEFAULT GETUTCDATE() NOT NULL
);
GO

-- --------------------------------------------------------------------------------
-- 2. AUDIT TABLE: Pipeline Execution & Batch Telemetry Tracker
-- Tracks ADF Pipeline and Databricks job run metrics
-- --------------------------------------------------------------------------------
IF OBJECT_ID('audit.PipelineExecutionLog', 'U') IS NOT NULL
    DROP TABLE audit.PipelineExecutionLog;

CREATE TABLE audit.PipelineExecutionLog (
    ExecutionID UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
    PipelineName VARCHAR(100) NOT NULL,
    RunID VARCHAR(100) NOT NULL,               -- ADF RunID or Databricks Job RunID
    SourceEntity VARCHAR(100) NOT NULL,
    Layer VARCHAR(20) NOT NULL,                -- 'RAW', 'BRONZE', 'SILVER', 'GOLD'
    Status VARCHAR(20) NOT NULL,               -- 'STARTED', 'SUCCESS', 'FAILED', 'QUARANTINED'
    RecordsIngested BIGINT DEFAULT 0 NOT NULL,
    RecordsQuarantined BIGINT DEFAULT 0 NOT NULL,
    ExecutionStartTime DATETIME2 NOT NULL,
    ExecutionEndTime DATETIME2 NULL,
    ErrorMessage VARCHAR(MAX) NULL,
    ExecutedBy VARCHAR(100) DEFAULT SUSER_SNAME() NOT NULL
);
GO

-- --------------------------------------------------------------------------------
-- 3. AUDIT TABLE: Data Quality Rule Execution & Quarantine Metrics
-- Records validation failures, schema drift, and quarantine counts
-- --------------------------------------------------------------------------------
IF OBJECT_ID('audit.DataQualityLog', 'U') IS NOT NULL
    DROP TABLE audit.DataQualityLog;

CREATE TABLE audit.DataQualityLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    ExecutionID UNIQUEIDENTIFIER NOT NULL,
    TableName VARCHAR(100) NOT NULL,
    RuleName VARCHAR(100) NOT NULL,            -- e.g., 'NOT_NULL_PATIENT_ID', 'VALID_HEART_RATE_RANGE'
    RuleType VARCHAR(50) NOT NULL,             -- 'SCHEMA_VALIDATION', 'BUSINESS_RULE', 'REFERENTIAL_INTEGRITY'
    TotalRecordsChecked BIGINT NOT NULL,
    FailedRecordCount BIGINT NOT NULL,
    QuarantineLocation VARCHAR(255) NULL,       -- Path to quarantined Delta table/directory
    LogTimestamp DATETIME2 DEFAULT GETUTCDATE() NOT NULL,
    CONSTRAINT FK_DataQualityLog_Execution FOREIGN KEY (ExecutionID) REFERENCES audit.PipelineExecutionLog(ExecutionID)
);
GO

-- --------------------------------------------------------------------------------
-- 4. STORED PROCEDURE: Update Watermark Timestamp for Incremental Copy
-- Called by ADF upon successful batch ingestion completion
-- --------------------------------------------------------------------------------
IF OBJECT_ID('meta.usp_UpdateWatermark', 'P') IS NOT NULL
    DROP PROCEDURE meta.usp_UpdateWatermark;
GO

CREATE PROCEDURE meta.usp_UpdateWatermark
    @SourceSystem VARCHAR(50),
    @EntityName VARCHAR(100),
    @NewWatermarkValue DATETIME2
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE meta.IngestionControl
    SET WatermarkValue = @NewWatermarkValue,
        UpdatedDate = GETUTCDATE()
    WHERE SourceSystem = @SourceSystem
      AND EntityName = @EntityName;

    SELECT @@ROWCOUNT AS RowsUpdated;
END;
GO
