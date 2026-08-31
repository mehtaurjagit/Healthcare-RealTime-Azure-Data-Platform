# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 ApexCare Real-Time Healthcare Platform
# MAGIC ## Notebook 01: Real-Time Bedside ICU Telemetry Ingestion (Event Hubs Kafka → Bronze Delta)
# MAGIC 
# MAGIC **Business Purpose**: Consumes continuous streaming telemetry (Heart rate, Blood Pressure, SpO2, Emergency Alerts) from bedside ICU monitors via Azure Event Hubs (Kafka Protocol), enforces JSON schema contract, adds audit metadata, and writes to Bronze Delta Lake using Structured Streaming.

# COMMAND ----------

# 1. DEFINE WIDGET PARAMETERS & STORAGE PATHS
dbutils.widgets.text("storage_account_name", "stapexcareprodeastus")
dbutils.widgets.text("event_hubs_namespace", "evh-apexcare-prod-eastus")
dbutils.widgets.text("event_hub_topic", "vitals-telemetry-hub")

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account_name")
EVENT_HUBS_NAMESPACE = dbutils.widgets.get("event_hubs_namespace")
TOPIC_NAME = dbutils.widgets.get("event_hub_topic")

# Storage Container Target Paths (ABFSS protocol)
BRONZE_TARGET_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/telemetry/vitals_streaming/"
CHECKPOINT_PATH = f"abfss://system-checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/vitals_streaming/"

print(f"Target Bronze Path: {BRONZE_TARGET_PATH}")
print(f"Checkpoint Path: {CHECKPOINT_PATH}")

# COMMAND ----------

# 2. CONFIGURE STORAGE ACCOUNT ACCESS KEY SECURELY
# (Note: In Phase 2 Enhancements, we replace raw key config with Key Vault Secret Scope dbsecrets-apexcare)
STORAGE_KEY = dbutils.widgets.get("storage_account_key") if "storage_account_key" in [w.name for w in dbutils.widgets.getExtra()] else "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

# COMMAND ----------

# 3. IMPORT REQUIRED PYSPARK LIBRARIES & DEFINE TELEMETRY JSON SCHEMA
from pyspark.sql.functions import from_json, col, current_timestamp, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

# Schema contract for bedside ICU device telemetry events
telemetry_schema = StructType([
    StructField("TelemetryID", StringType(), False),
    StructField("PatientID", StringType(), False),
    StructField("EncounterID", StringType(), False),
    StructField("DeviceID", StringType(), True),
    StructField("HeartRate", IntegerType(), True),
    StructField("BloodPressureSystolic", IntegerType(), True),
    StructField("BloodPressureDiastolic", IntegerType(), True),
    StructField("OxygenSaturation", DoubleType(), True),
    StructField("BodyTemperature", DoubleType(), True),
    StructField("IsCriticalAlert", BooleanType(), True),
    StructField("EventTimestamp", StringType(), True)
])

# COMMAND ----------

# 4. CONFIGURE EVENT HUBS KAFKA CONNECTION PARAMETERS
EVENT_HUBS_SAS_KEY = "<YOUR_EVENT_HUBS_PRIMARY_CONNECTION_STRING>"
KAFKA_BROKER = f"{EVENT_HUBS_NAMESPACE}.servicebus.windows.net:9093"

# SASL JAAS authentication string for Azure Event Hubs Kafka Surface
sasl_jaas_config = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" '
    f'password="{EVENT_HUBS_SAS_KEY}";'
)

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BROKER,
    "subscribe": TOPIC_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": sasl_jaas_config,
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

# COMMAND ----------

# 5. READ STRUCTURED STREAM FROM EVENT HUBS KAFKA TOPIC
kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# COMMAND ----------

# 6. PARSE JSON PAYLOAD & ADD AUDIT METADATA COLUMNS
# Event Hubs payload is stored as binary in 'value' column
parsed_stream_df = (
    kafka_stream_df
    .selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_enqueued_time", "partition", "offset")
    .select(
        from_json(col("json_payload"), telemetry_schema).alias("data"),
        col("kafka_enqueued_time"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset")
    )
    .select("data.*", "_kafka_partition", "_kafka_offset", "kafka_enqueued_time")
    .withColumn("EventTimestamp", col("EventTimestamp").cast(TimestampType()))
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_system", lit("BEDSIDE_ICU_EVENT_HUBS"))
)

# COMMAND ----------

# 7. WRITE STRUCTURED STREAM TO BRONZE DELTA LAKE TABLE WITH CHECKPOINTING
query = (
    parsed_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")
    .start(BRONZE_TARGET_PATH)
)

print(f"🚀 Streaming query started! Status: {query.status}")
