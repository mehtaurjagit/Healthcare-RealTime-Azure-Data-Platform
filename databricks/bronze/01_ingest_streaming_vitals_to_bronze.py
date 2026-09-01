# Databricks notebook source
# COMMAND ----------
# 1. DEFINE STORAGE & EVENT HUBS PARAMETERS
STORAGE_ACCOUNT = "stapexcareprodeastus"
EVENT_HUBS_NAMESPACE = "evh-apexcare-prod-eastus"
TOPIC_NAME = "vitals-telemetry-hub"

# Paste your Keys below - .strip() will automatically clean any accidental quotes
STORAGE_KEY = "<YOUR_STORAGE_ACCOUNT_KEY>".strip("'\" ")
EVENT_HUBS_SAS_KEY = dbutils.secrets.get(
    scope="dbsecrets-apexcare",
    key="evh-vitals-connection-string")

BRONZE_TARGET_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/telemetry/vitals_streaming/"
CHECKPOINT_PATH = f"abfss://system-checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/vitals_streaming/"

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

# COMMAND ----------

# COMMAND ----------
# 2. IMPORT LIBRARIES & DEFINE TELEMETRY JSON SCHEMA CONTRACT
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

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

# COMMAND ----------
# 3. CONFIGURE KAFKA STREAM READER FROM AZURE EVENT HUBS
KAFKA_BROKER = f"{EVENT_HUBS_NAMESPACE}.servicebus.windows.net:9093"

# Escape characters safely before inserting secret into JAAS config
eventhub_secret_escaped = (
    EVENT_HUBS_SAS_KEY
    .replace("\\", "\\\\")
    .replace('"', '\\"')
)

sasl_jaas_config = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="$ConnectionString" '
    f'password="{eventhub_secret_escaped}";'
)

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BROKER,
    "subscribe": TOPIC_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": sasl_jaas_config,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

print("✅ Kafka options configured with kafkashaded module!")

# COMMAND ----------

# COMMAND ----------
# 4. READ STRUCTURED STREAM & PARSE JSON PAYLOAD WITH AUDIT COLUMNS
kafka_stream_df = spark.readStream.format("kafka").options(**kafka_options).load()

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

# COMMAND ----------
# 5. WRITE STREAM TO BRONZE DELTA TABLE WITH CHECKPOINTING
query = (
    parsed_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")
    .start(BRONZE_TARGET_PATH)
)

print(f"🚀 Streaming query started successfully! Stream status: {query.status}")

# COMMAND ----------

import socket

host = "evh-apexcare-prod-eastus.servicebus.windows.net"
port = 9093

try:
    s = socket.create_connection((host, port), timeout=10)
    print(f"✅ SUCCESS: Port {port} is open and reachable on {host}")
    s.close()
except Exception as e:
    print(f"❌ CONNECTION FAILED: {e}")