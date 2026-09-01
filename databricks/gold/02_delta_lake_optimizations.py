# Databricks notebook source
# COMMAND ----------
# 1. PARAMETERS & STORAGE PATHS
STORAGE_ACCOUNT = "stapexcareprodeastus"
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

STORAGE_KEY = "<YOUR_STORAGE_ACCOUNT_KEY>"
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)


# COMMAND ----------

# COMMAND ----------
# 2. EXECUTE OPTIMIZE AND ZORDER ON FACT_PATIENT_ENCOUNTERS
print("🚀 Optimizing Gold Table: fact_patient_encounters with ZORDER BY (EncounterType, AdmitTimestamp)...")

spark.sql(f"""
    OPTIMIZE delta.`{GOLD_PATH}/fact_patient_encounters/`
    ZORDER BY (EncounterType, AdmitTimestamp)
""")

print("✅ OPTIMIZE & ZORDER complete for fact_patient_encounters!")

# COMMAND ----------

# COMMAND ----------
# 3. EXECUTE OPTIMIZE AND ZORDER ON FACT_VITALS_TELEMETRY
print("🚀 Optimizing Gold Streaming Table: fact_vitals_telemetry with ZORDER BY (IsCriticalAlert, EventTimestamp)...")

spark.sql(f"""
    OPTIMIZE delta.`{GOLD_PATH}/fact_vitals_telemetry/`
    ZORDER BY (IsCriticalAlert, EventTimestamp)
""")

print("✅ OPTIMIZE & ZORDER complete for fact_vitals_telemetry!")

# COMMAND ----------

# MAGIC %sql
SELECT 
 d.FacilityName,
 e.EncounterType,
 COUNT(e.EncounterID) AS TotalEncounters,
 ROUND(AVG(e.LengthOfStayHours), 1) AS AvgLengthOfStayHours,
 ROUND(SUM(e.TotalBilledAmount), 2) AS TotalBilledAmount
 FROM delta.`abfss://gold@stapexcareprodeastus.dfs.core.windows.net/fact_patient_encounters/` e
 LEFT JOIN delta.`abfss://gold@stapexcareprodeastus.dfs.core.windows.net/dim_department/` d ON e.DepartmentSK = d.DepartmentSK
 GROUP BY d.FacilityName, e.EncounterType
 ORDER BY TotalBilledAmount DESC;