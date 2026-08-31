# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 ApexCare Real-Time Healthcare Platform
# MAGIC ## Notebook 03: Silver Layer — Patients Slowly Changing Dimension (SCD) Type 2 Engine
# MAGIC 
# MAGIC **Business Purpose**: Tracks historical changes over time in patient addresses, phone numbers, and insurance coverage using **SCD Type 2**. Adds surrogate keys (`PatientSK`), `IsCurrent` status flags, `EffectiveStartDate`, and `EffectiveEndDate`.

# COMMAND ----------

# 1. PARAMETERS & STORAGE PATHS
STORAGE_ACCOUNT = "stapexcareprodeastus"

BRONZE_PATIENTS_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/ehr/patients/"
SILVER_PATIENTS_SCD2_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/dim_patient_scd2/"

STORAGE_KEY = dbutils.widgets.get("storage_account_key") if "storage_account_key" in [w.name for w in dbutils.widgets.getExtra()] else "<YOUR_STORAGE_ACCOUNT_KEY>"
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

# COMMAND ----------

# 2. READ BRONZE PATIENTS DROP FILE
from pyspark.sql.functions import col, md5, concat_ws, to_timestamp, lit, current_timestamp
from delta.tables import DeltaTable

raw_patients_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(BRONZE_PATIENTS_PATH)
)

print(f"Loaded {raw_patients_df.count()} raw patient records from Bronze.")

# COMMAND ----------

# 3. CONSTRUCT STAGING SURROGATE KEYS & METADATA
staged_patients_df = (
    raw_patients_df
    .withColumn("UpdatedTimestamp", to_timestamp(col("UpdatedTimestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("PatientSK", md5(concat_ws("||", col("PatientID"), col("UpdatedTimestamp"))))
    .withColumn("IsCurrent", lit(True))
    .withColumn("EffectiveStartDate", col("UpdatedTimestamp"))
    .withColumn("EffectiveEndDate", to_timestamp(lit("9999-12-31 23:59:59"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("_processed_at", current_timestamp())
)

# COMMAND ----------

# 4. INITIALIZE SILVER DELTA TABLE IF NOT EXISTS
if not DeltaTable.isDeltaTable(spark, SILVER_PATIENTS_SCD2_PATH):
    print("Initializing new Silver dim_patient_scd2 Delta table...")
    (
        staged_patients_df.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATIENTS_SCD2_PATH)
    )
    print("✅ Successfully initialized dim_patient_scd2 Delta table!")
else:
    # 5. EXECUTE DELTA MERGE FOR SCD TYPE 2
    print("Executing Delta MERGE for SCD Type 2 updates...")
    target_table = DeltaTable.forPath(spark, SILVER_PATIENTS_SCD2_PATH)

    # Step A: Update existing current records where attributes changed (set IsCurrent = False, EffectiveEndDate = Source.EffectiveStartDate)
    merge_condition = "target.PatientID = source.PatientID AND target.IsCurrent = True"

    # Merge stage 1: Upsert changed records
    (
        target_table.alias("target")
        .merge(
            staged_patients_df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdate(
            condition = """
                target.AddressLine != source.AddressLine OR
                target.City != source.City OR
                target.PrimaryInsurancePayer != source.PrimaryInsurancePayer
            """,
            set = {
                "IsCurrent": "false",
                "EffectiveEndDate": "source.EffectiveStartDate"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("✅ SCD Type 2 MERGE complete!")

# COMMAND ----------

# 6. VERIFY SILVER SCD TYPE 2 DIMENSION RECORD COUNTS
silver_df = spark.read.format("delta").load(SILVER_PATIENTS_SCD2_PATH)
current_count = silver_df.filter(col("IsCurrent") == True).count()
total_versions = silver_df.count()

print(f"📊 Total Patient Versions: {total_versions} | Active Current Patients: {current_count}")
