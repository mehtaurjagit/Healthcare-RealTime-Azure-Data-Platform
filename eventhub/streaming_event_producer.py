"""
================================================================================
ApexCare Real-Time Healthcare Data Platform
Bedside ICU Telemetry Event Producer (Azure Event Hubs Kafka Surface)
================================================================================
Simulates continuous real-time ICU bedside monitoring telemetry:
  - HeartRate (BPM)
  - BloodPressureSystolic (mmHg)
  - BloodPressureDiastolic (mmHg)
  - OxygenSaturation (SpO2 %)
  - BodyTemperature (°F)
  - IsCriticalAlert (Derived threshold flag)
Sends JSON payload stream to Azure Event Hubs using confluent-kafka / kafka-python
================================================================================
"""

import json
import os
import random
import sys
import time
from datetime import datetime, timezone
import uuid

# Check for kafka library
try:
    from kafka import KafkaProducer
except ImportError:
    print("❌ 'kafka-python' library missing. Install via: pip install kafka-python")
    sys.exit(1)

# CONFIGURATION PARAMETERS
# Replace with your actual Event Hubs Connection Details from Step 2
EVENT_HUBS_NAMESPACE = "evh-apexcare-prod-eastus"
EVENT_HUB_TOPIC = "vitals-telemetry-hub"
SHARED_ACCESS_KEY_NAME = "RootManageSharedAccessKey"
# Paste your primary connection string copied in Step 2 here or set environment variable
SHARED_ACCESS_KEY = os.getenv("EVENT_HUBS_SAS_KEY", "<YOUR_EVENT_HUBS_PRIMARY_CONNECTION_STRING>")

KAFKA_BROKER = f"{EVENT_HUBS_NAMESPACE}.servicebus.windows.net:9093"

if "<YOUR_EVENT_HUBS_PRIMARY_CONNECTION_STRING>" in SHARED_ACCESS_KEY:
    print("⚠️ WARNING: Please update SHARED_ACCESS_KEY with your actual Event Hubs connection string from Step 2!")

# Construct SASL connection string for Kafka protocol over TLS
SASL_JAAS_CONFIG = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" '
    f'password="{SHARED_ACCESS_KEY}";'
)

print(f"📡 Initializing Kafka Producer to Azure Event Hubs: {KAFKA_BROKER}")

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
        sasl_plain_password=SHARED_ACCESS_KEY,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("✅ Successfully connected to Azure Event Hubs Kafka Endpoint!")
except Exception as e:
    print(f"❌ Failed to connect to Event Hubs Kafka Broker: {e}")
    sys.exit(1)

# Generate simulated patient pool
PATIENT_IDS = [f"PAT_{i:06d}" for i in range(1, 101)]
ENCOUNTER_IDS = [f"ENC_{i:07d}" for i in range(1, 101)]
DEVICE_IDS = [f"ICU_MONITOR_{i:03d}" for i in range(1, 20)]

def generate_telemetry_event():
    pat_idx = random.randint(0, len(PATIENT_IDS) - 1)
    hr = random.randint(50, 140)
    bp_sys = random.randint(85, 175)
    bp_dia = random.randint(55, 110)
    spo2 = round(random.uniform(88.0, 100.0), 1)
    temp = round(random.uniform(97.0, 103.5), 1)

    # Critical alert logic
    is_critical = hr > 120 or hr < 55 or spo2 < 90.0 or bp_sys > 160

    event = {
        "TelemetryID": str(uuid.uuid4()),
        "PatientID": PATIENT_IDS[pat_idx],
        "EncounterID": ENCOUNTER_IDS[pat_idx],
        "DeviceID": random.choice(DEVICE_IDS),
        "HeartRate": hr,
        "BloodPressureSystolic": bp_sys,
        "BloodPressureDiastolic": bp_dia,
        "OxygenSaturation": spo2,
        "BodyTemperature": temp,
        "IsCriticalAlert": is_critical,
        "EventTimestamp": datetime.now(timezone.utc).isoformat()
    }
    return event

def start_streaming(events_per_second=5, duration_seconds=60):
    print(f"🚀 Starting Real-Time Telemetry Stream (~{events_per_second} events/sec for {duration_seconds} seconds)...")
    sent_count = 0
    start_time = time.time()

    try:
        while (time.time() - start_time) < duration_seconds:
            event = generate_telemetry_event()
            producer.send(EVENT_HUB_TOPIC, value=event)
            sent_count += 1
            if is_critical := event["IsCriticalAlert"]:
                print(f"🚨 [CRITICAL ALERT] Patient: {event['PatientID']} | HR: {event['HeartRate']} | SpO2: {event['OxygenSaturation']}%")
            else:
                print(f"📊 [NORMAL VITALS] Patient: {event['PatientID']} | HR: {event['HeartRate']} | SpO2: {event['OxygenSaturation']}%")
            
            time.sleep(1.0 / events_per_second)

        producer.flush()
        print(f"\n✅ Stream complete! Total events sent: {sent_count}")
    except KeyboardInterrupt:
        print("\n⏹️ Stream stopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    # Test run: 5 events/sec for 30 seconds
    start_streaming(events_per_second=5, duration_seconds=30)
