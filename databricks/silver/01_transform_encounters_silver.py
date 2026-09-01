# Databricks notebook source
# COMMAND ----------
# 1. PARAMETERS & STORAGE PATHS
STORAGE_ACCOUNT = "stapexcareprodeastus"

BRONZE_ENCOUNTERS_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/ehr/encounters/Encounters.csv"
SILVER_ENCOUNTERS_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/fact_encounters/"
QUARANTINE_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_quarantine/"

# Replace with your actual Storage Account Key 1
STORAGE_KEY = "<YOUR_STORAGE_ACCOUNT_KEY>"
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)



# COMMAND ----------

# COMMAND ----------
# 2. READ RAW ENCOUNTERS FROM BRONZE
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit, row_number
from pyspark.sql.window import Window

raw_encounters_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(BRONZE_ENCOUNTERS_PATH)
)

print(f"Read {raw_encounters_df.count()} raw encounter records from Bronze.")

# COMMAND ----------

# COMMAND ----------
# 3. DATA TYPE CLEANING & DERIVED COLUMNS
cleaned_df = (
    raw_encounters_df
    .withColumn("AdmitTimestamp", to_timestamp(col("AdmitTimestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("DischargeTimestamp", to_timestamp(col("DischargeTimestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("LengthOfStayHours", col("LengthOfStayHours").cast("decimal(10,2)"))
    .withColumn("TotalBilledAmount", col("TotalBilledAmount").cast("decimal(12,2)"))
    .withColumn("_processed_at", current_timestamp())
    .withColumn("_source_system", lit("EPIC_EHR"))
)


# COMMAND ----------

# COMMAND ----------
# 4. DE-DUPLICATION USING WINDOW FUNCTION (ROW_NUMBER)
window_spec = Window.partitionBy("EncounterID").orderBy(col("AdmitTimestamp").desc())

dedup_df = (
    cleaned_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print(f"De-duplicated count: {dedup_df.count()} records.")

# COMMAND ----------

# COMMAND ----------
# 5. DATA QUALITY CHECK & QUARANTINE ROUTING

from pyspark.sql.functions import col

# Rule 1: Critical keys must exist
rule_not_null_keys = (
    col("EncounterID").isNotNull() &
    col("PatientID").isNotNull()
)

# Rule 2: Length of stay must be realistic
rule_valid_los = (
    (col("LengthOfStayHours") >= 0) &
    (col("LengthOfStayHours") <= 8760)
)

# Rule 3: Billed amount cannot be negative
rule_valid_billed_amount = (
    col("TotalBilledAmount") >= 0
)

# Final valid / invalid routing
valid_df = dedup_df.filter(
    rule_not_null_keys &
    rule_valid_los &
    rule_valid_billed_amount
)

invalid_df = dedup_df.filter(
    ~(
        rule_not_null_keys &
        rule_valid_los &
        rule_valid_billed_amount
    )
)

invalid_count = invalid_df.count()

if invalid_count > 0:
    print(f"⚠️ Quarantining {invalid_count} invalid encounter records...")
    invalid_df.write.format("delta").mode("append").save(
        f"{QUARANTINE_PATH}/fact_encounters_exceptions/"
    )
else:
    print("✅ No invalid encounter records found.")

# COMMAND ----------

# COMMAND ----------
# 6. WRITE CLEAN SILVER FACT ENCOUNTERS DELTA TABLE
(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_ENCOUNTERS_PATH)
)

print(f"✅ Successfully wrote clean Silver Delta table: {SILVER_ENCOUNTERS_PATH}")

# COMMAND ----------

# COMMAND ----------
# 7. LOG PIPELINE EXECUTION & DATA QUALITY RESULTS TO AZURE SQL

import uuid
from pyspark.sql.functions import current_timestamp

execution_id = str(uuid.uuid4())

SQL_SERVER = "sql-apexcare-prod-eastus.database.windows.net"
SQL_DB = "sqldb-apexcare-control"
SQL_USER = "sqladmin"

# Secure password retrieval from Azure Key Vault-backed Databricks scope
SQL_PWD = dbutils.secrets.get(
    scope="dbsecrets-apexcare",
    key="sqldb-control-password"
)

jdbc_url = (
    f"jdbc:sqlserver://{SQL_SERVER}:1433;"
    f"database={SQL_DB};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
)

# Reuse counts already produced by the DQ logic
total_records = dedup_df.count()
failed_count = invalid_df.count()
passed_count = total_records - failed_count

# ---------------------------------------
# 7A. Insert parent execution audit record
# ---------------------------------------

parent_audit_data = [(
    execution_id,
    "01_transform_encounters_silver",
    "DBX_" + execution_id[:8],
    "Encounters",
    "SILVER",
    "SUCCESS" if failed_count == 0 else "QUARANTINED",
    passed_count,
    failed_count
)]

parent_df = spark.createDataFrame(
    parent_audit_data,
    [
        "ExecutionID",
        "PipelineName",
        "RunID",
        "SourceEntity",
        "Layer",
        "Status",
        "RecordsIngested",
        "RecordsQuarantined"
    ]
)

parent_df = (
    parent_df
    .withColumn("ExecutionStartTime", current_timestamp())
    .withColumn("ExecutionEndTime", current_timestamp())
)

parent_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "audit.PipelineExecutionLog") \
    .option("user", SQL_USER) \
    .option("password", SQL_PWD) \
    .mode("append") \
    .save()

print("✅ Parent pipeline audit record written.")

# ---------------------------------------
# 7B. Calculate failures for each DQ rule
# ---------------------------------------

rule_01_failed = dedup_df.filter(~rule_not_null_keys).count()
rule_02_failed = dedup_df.filter(~rule_valid_los).count()
rule_03_failed = dedup_df.filter(~rule_valid_billed_amount).count()

dq_log_data = [
    (
        execution_id,
        "silver.fact_encounters",
        "RULE_01_NOT_NULL_KEYS",
        "NOT_NULL",
        total_records,
        rule_01_failed,
        f"{QUARANTINE_PATH}/fact_encounters_exceptions/"
    ),
    (
        execution_id,
        "silver.fact_encounters",
        "RULE_02_VALID_LENGTH_OF_STAY",
        "RANGE_CHECK",
        total_records,
        rule_02_failed,
        f"{QUARANTINE_PATH}/fact_encounters_exceptions/"
    ),
    (
        execution_id,
        "silver.fact_encounters",
        "RULE_03_VALID_BILLED_AMOUNT",
        "RANGE_CHECK",
        total_records,
        rule_03_failed,
        f"{QUARANTINE_PATH}/fact_encounters_exceptions/"
    )
]

dq_df = spark.createDataFrame(
    dq_log_data,
    [
        "ExecutionID",
        "TableName",
        "RuleName",
        "RuleType",
        "TotalRecordsChecked",
        "FailedRecordCount",
        "QuarantineLocation"
    ]
)

dq_df = dq_df.withColumn("LogTimestamp", current_timestamp())

dq_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "audit.DataQualityLog") \
    .option("user", SQL_USER) \
    .option("password", SQL_PWD) \
    .mode("append") \
    .save()

print("✅ Data quality audit records written successfully.")