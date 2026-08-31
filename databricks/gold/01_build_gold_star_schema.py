# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 ApexCare Real-Time Healthcare Platform
# MAGIC ## Notebook 04: Gold Layer — Star Schema Dimensional Model Construction
# MAGIC 
# MAGIC **Business Purpose**: Reads clean Silver Delta tables (`dim_patient_scd2`, `fact_encounters`, `fact_vitals_telemetry`, `Providers`, `Departments`) and constructs a production **Star Schema** in `gold/` container for executive reporting in Synapse & Power BI.

# COMMAND ----------

# 1. PARAMETERS & STORAGE PATHS
STORAGE_ACCOUNT = "stapexcareprodeastus"

SILVER_PATIENTS_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/dim_patient_scd2/"
SILVER_ENCOUNTERS_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/fact_encounters/"
BRONZE_PROVIDERS_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/ehr/providers/Providers.csv"
BRONZE_DEPARTMENTS_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/pm/departments/Departments.csv"
BRONZE_VITALS_STREAMING_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/telemetry/vitals_streaming/"

GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

STORAGE_KEY = dbutils.widgets.get("storage_account_key") if "storage_account_key" in [w.name for w in dbutils.widgets.getExtra()] else "<YOUR_STORAGE_ACCOUNT_KEY>"
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

# COMMAND ----------

# 2. BUILD GOLD DIM_PATIENT (ACTIVE CURRENT RECORDS)
from pyspark.sql.functions import col, md5, concat_ws, current_timestamp

silver_patients_df = spark.read.format("delta").load(SILVER_PATIENTS_PATH)

gold_dim_patient = (
    silver_patients_df
    .filter(col("IsCurrent") == True)
    .select(
        "PatientSK", "PatientID", "MedicalRecordNumber", "FirstName", "LastName",
        "DateOfBirth", "Gender", "PrimaryLanguage", "AddressLine", "City", "State",
        "ZipCode", "PrimaryInsurancePayer", "EffectiveStartDate"
    )
)

gold_dim_patient.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_patient/")
print(f"✅ Created Gold Table: dim_patient ({gold_dim_patient.count()} active records)")

# COMMAND ----------

# 3. BUILD GOLD DIM_PROVIDER & DIM_DEPARTMENT
raw_providers_df = spark.read.option("header", "true").csv(BRONZE_PROVIDERS_PATH)
gold_dim_provider = (
    raw_providers_df
    .withColumn("ProviderSK", md5(concat_ws("||", col("ProviderID"))))
    .select("ProviderSK", "ProviderID", "NPI_Number", "ProviderName", "Specialty", "DepartmentID")
)
gold_dim_provider.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_provider/")

raw_dept_df = spark.read.option("header", "true").csv(BRONZE_DEPARTMENTS_PATH)
gold_dim_department = (
    raw_dept_df
    .withColumn("DepartmentSK", md5(concat_ws("||", col("DepartmentID"))))
    .select("DepartmentSK", "DepartmentID", "DepartmentName", "FacilityName")
)
gold_dim_department.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_department/")

print("✅ Created Gold Tables: dim_provider & dim_department")

# COMMAND ----------

# 4. BUILD GOLD FACT_PATIENT_ENCOUNTERS
silver_encounters_df = spark.read.format("delta").load(SILVER_ENCOUNTERS_PATH)

gold_fact_encounters = (
    silver_encounters_df.alias("e")
    .join(gold_dim_patient.alias("p"), col("e.PatientID") == col("p.PatientID"), "left")
    .join(gold_dim_provider.alias("pr"), col("e.ProviderID") == col("pr.ProviderID"), "left")
    .join(gold_dim_department.alias("d"), col("e.DepartmentID") == col("d.DepartmentID"), "left")
    .select(
        col("e.EncounterID"),
        col("p.PatientSK"),
        col("pr.ProviderSK"),
        col("d.DepartmentSK"),
        col("e.EncounterType"),
        col("e.AdmitTimestamp"),
        col("e.DischargeTimestamp"),
        col("e.LengthOfStayHours"),
        col("e.AdmitReason"),
        col("e.DischargeStatus"),
        col("e.TotalBilledAmount"),
        current_timestamp().alias("_created_at")
    )
)

gold_fact_encounters.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/fact_patient_encounters/")
print(f"✅ Created Gold Table: fact_patient_encounters ({gold_fact_encounters.count()} records)")

# COMMAND ----------

# 5. BUILD GOLD FACT_VITALS_TELEMETRY (REAL-TIME STREAMING FACT)
streaming_vitals_df = spark.read.format("delta").load(BRONZE_VITALS_STREAMING_PATH)

gold_fact_vitals = (
    streaming_vitals_df.alias("v")
    .join(gold_dim_patient.alias("p"), col("v.PatientID") == col("p.PatientID"), "left")
    .select(
        col("v.TelemetryID"),
        col("p.PatientSK"),
        col("v.EncounterID"),
        col("v.DeviceID"),
        col("v.HeartRate"),
        col("v.BloodPressureSystolic"),
        col("v.BloodPressureDiastolic"),
        col("v.OxygenSaturation"),
        col("v.BodyTemperature"),
        col("v.IsCriticalAlert"),
        col("v.EventTimestamp")
    )
)

gold_fact_vitals.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/fact_vitals_telemetry/")
print(f"✅ Created Gold Table: fact_vitals_telemetry ({gold_fact_vitals.count()} streamed records)")
