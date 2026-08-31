# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 ApexCare Real-Time Healthcare Platform
# MAGIC ## Notebook 05: Delta Lake Performance Optimization (`OPTIMIZE` & `ZORDER`)
# MAGIC 
# MAGIC **Business Purpose**: Executes Delta Lake compaction and **Z-Order Indexing** on Gold tables. Z-Ordering co-locates related clinical attributes into physical Parquet files in ADLS Gen2, enabling **data skipping** (pruning 90%+ of files during analytical SQL queries in Synapse and Power BI).

# COMMAND ----------

# 1. PARAMETERS & STORAGE PATHS
STORAGE_ACCOUNT = "stapexcareprodeastus"
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

STORAGE_KEY = dbutils.widgets.get("storage_account_key") if "storage_account_key" in [w.name for w in dbutils.widgets.getExtra()] else "<YOUR_STORAGE_ACCOUNT_KEY>"
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

# COMMAND ----------

# 2. EXECUTE OPTIMIZE AND ZORDER ON FACT_PATIENT_ENCOUNTERS
print("🚀 Optimizing Gold Table: fact_patient_encounters with ZORDER BY (EncounterType, AdmitTimestamp)...")

spark.sql(f"""
    OPTIMIZE delta.`{GOLD_PATH}/fact_patient_encounters/`
    ZORDER BY (EncounterType, AdmitTimestamp)
""")

print("✅ OPTIMIZE & ZORDER complete for fact_patient_encounters!")

# COMMAND ----------

# 3. EXECUTE OPTIMIZE AND ZORDER ON FACT_VITALS_TELEMETRY
print("🚀 Optimizing Gold Streaming Table: fact_vitals_telemetry with ZORDER BY (IsCriticalAlert, EventTimestamp)...")

spark.sql(f"""
    OPTIMIZE delta.`{GOLD_PATH}/fact_vitals_telemetry/`
    ZORDER BY (IsCriticalAlert, EventTimestamp)
""")

print("✅ OPTIMIZE & ZORDER complete for fact_vitals_telemetry!")
