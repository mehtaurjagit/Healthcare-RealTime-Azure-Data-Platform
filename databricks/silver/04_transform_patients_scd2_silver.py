# Databricks notebook source
# COMMAND ----------
# 1. PARAMETERS & STORAGE PATHS
STORAGE_ACCOUNT = "stapexcareprodeastus"

BRONZE_PATIENTS_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/ehr/patients/Patients.csv"
SILVER_PATIENTS_SCD2_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/dim_patient_scd2/"

STORAGE_KEY = "<YOUR_STORAGE_ACCOUNT_KEY>"
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)


# COMMAND ----------

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

# COMMAND ----------
# 3. CONSTRUCT SURROGATE KEYS & SCD2 METADATA COLUMNS
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

# COMMAND ----------
# 4. INITIALIZE / MERGE INTO SILVER SCD2 DELTA TABLE
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
    print("Executing Delta MERGE for SCD Type 2 updates...")
    target_table = DeltaTable.forPath(spark, SILVER_PATIENTS_SCD2_PATH)

    merge_condition = "target.PatientID = source.PatientID AND target.IsCurrent = True"

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

# COMMAND ----------
# 5. VERIFY SILVER SCD TYPE 2 DIMENSION RECORD COUNTS
silver_df = spark.read.format("delta").load(SILVER_PATIENTS_SCD2_PATH)
current_count = silver_df.filter(col("IsCurrent") == True).count()
total_versions = silver_df.count()

print(f"📊 Total Patient Versions: {total_versions} | Active Current Patients: {current_count}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PatientID, FirstName, LastName, City, PrimaryInsurancePayer, IsCurrent, EffectiveStartDate, EffectiveEndDate 
# MAGIC FROM delta.`abfss://silver@stapexcareprodeastus.dfs.core.windows.net/dim_patient_scd2/`
# MAGIC LIMIT 10;