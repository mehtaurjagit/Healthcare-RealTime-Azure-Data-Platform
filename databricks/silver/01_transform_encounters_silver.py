# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 ApexCare Real-Time Healthcare Platform
# MAGIC ## Notebook 02: Silver Layer Transformation — Encounters & Hospital Admissions
# MAGIC 
# MAGIC **Business Purpose**: Reads raw encounter drop files from Bronze (`bronze/ehr/encounters/`), cleans data types, de-duplicates records using window functions (`ROW_NUMBER()`), calculates `LengthOfStayHours`, enforces data quality checks (not null `EncounterID`), and writes clean Delta table to `silver.fact_encounters`.

# COMMAND ----------

# 1. PARAMETERS & STORAGE PATHS
STORAGE_ACCOUNT = "stapexcareprodeastus"

BRONZE_ENCOUNTERS_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/ehr/encounters/"
SILVER_ENCOUNTERS_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/fact_encounters/"
QUARANTINE_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_quarantine/"

STORAGE_KEY = dbutils.widgets.get("storage_account_key") if "storage_account_key" in [w.name for w in dbutils.widgets.getExtra()] else "<YOUR_STORAGE_ACCOUNT_KEY>"
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

# COMMAND ----------

# 2. READ RAW ENCOUNTERS FROM BRONZE
from pyspark.sql.functions import col, to_timestamp, round, current_timestamp, lit, row_number
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

# 3. TYPE CASTING & DERIVED METRICS
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

# 4. DE-DUPLICATION USING WINDOW FUNCTION (ROW_NUMBER)
# Deduplicate by EncounterID keeping the latest AdmitTimestamp
window_spec = Window.partitionBy("EncounterID").orderBy(col("AdmitTimestamp").desc())

dedup_df = (
    cleaned_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print(f"De-duplicated count: {dedup_df.count()} records.")

# COMMAND ----------

# 5. DATA QUALITY CHECK & QUARANTINE (NOT NULL ENCOUNTER_ID)
valid_df = dedup_df.filter(col("EncounterID").isNotNull() & col("PatientID").isNotNull())
invalid_df = dedup_df.filter(col("EncounterID").isNull() | col("PatientID").isNull())

invalid_count = invalid_df.count()
if invalid_count > 0:
    print(f"⚠️ Quarantining {invalid_count} invalid encounter records...")
    invalid_df.write.format("delta").mode("append").save(f"{QUARANTINE_PATH}/fact_encounters_null_keys/")

# COMMAND ----------

# 6. WRITE CLEAN SILVER FACT ENCOUNTERS DELTA TABLE
(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_ENCOUNTERS_PATH)
)

print(f"✅ Successfully wrote clean Silver table: {SILVER_ENCOUNTERS_PATH}")
