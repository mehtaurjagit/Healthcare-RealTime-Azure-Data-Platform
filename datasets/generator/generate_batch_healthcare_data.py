"""
================================================================================
ApexCare Real-Time Healthcare Data Platform
Synthetic Enterprise Healthcare Batch Data Generator
================================================================================
Generates realistic, schema-validated healthcare datasets:
  - Patients.csv (SCD Type 2 source with demographic changes)
  - Providers.csv
  - Departments.csv
  - Encounters.csv (ED visits, Inpatient, ICU admissions)
  - BillingClaims.csv
  - LabResults.json
================================================================================
"""

import csv
import json
import os
import random
from datetime import datetime, timedelta

# Define output directory relative to project root
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw_batch")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Seed for reproducible realistic data
random.seed(42)

# Global configuration constants
NUM_PATIENTS = 2000
NUM_PROVIDERS = 300
NUM_DEPARTMENTS = 25
NUM_ENCOUNTERS = 5000
NUM_CLAIMS = 5000
NUM_LAB_RESULTS = 3000

FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Rahul", "Priya", "Amit", "Ananya", "Carlos", "Maria"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Mehta", "Patel", "Sharma"]
CITIES = ["New York", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose", "Austin"]
STATES = ["NY", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA", "TX"]
SPECIALTIES = ["Cardiology", "Emergency Medicine", "ICU Critical Care", "Internal Medicine", "Orthopedics", "Neurology", "Pediatrics", "General Surgery", "Oncology"]
INSURANCE_PAYERS = ["BlueCross BlueShield", "UnitedHealth", "Aetna", "Cigna", "Medicare", "Medicaid", "Kaiser Permanente"]
DEPT_NAMES = ["Emergency Department", "ICU Ward A", "ICU Ward B", "Cardiology Clinic", "Outpatient Surgery", "General Pediatrics", "Neurology Suite", "Orthopedics Unit"]

START_DATE = datetime(2025, 1, 1)

print(f"[+] Generating ApexCare Synthetic Data in: {OUTPUT_DIR}")

# ------------------------------------------------------------------------------
# 1. GENERATE DEPARTMENTS.CSV
# ------------------------------------------------------------------------------
departments = []
for i in range(1, NUM_DEPARTMENTS + 1):
    dept_id = f"DEPT_{i:03d}"
    dept_name = random.choice(DEPT_NAMES) + f" #{random.randint(1, 5)}"
    facility = f"ApexCare Regional Hospital {random.choice(['North', 'South', 'Central', 'East', 'West'])}"
    departments.append({"DepartmentID": dept_id, "DepartmentName": dept_name, "FacilityName": facility})

dept_file = os.path.join(OUTPUT_DIR, "Departments.csv")
with open(dept_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["DepartmentID", "DepartmentName", "FacilityName"])
    writer.writeheader()
    writer.writerows(departments)
print(f"[SUCCESS] Created Departments.csv ({len(departments)} records)")

# ------------------------------------------------------------------------------
# 2. GENERATE PROVIDERS.CSV
# ------------------------------------------------------------------------------
providers = []
for i in range(1, NUM_PROVIDERS + 1):
    prov_id = f"PROV_{i:04d}"
    npi = str(random.randint(1000000000, 9999999999))
    name = f"Dr. {random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    specialty = random.choice(SPECIALTIES)
    dept_id = random.choice(departments)["DepartmentID"]
    providers.append({
        "ProviderID": prov_id,
        "NPI_Number": npi,
        "ProviderName": name,
        "Specialty": specialty,
        "DepartmentID": dept_id
    })

prov_file = os.path.join(OUTPUT_DIR, "Providers.csv")
with open(prov_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["ProviderID", "NPI_Number", "ProviderName", "Specialty", "DepartmentID"])
    writer.writeheader()
    writer.writerows(providers)
print(f"[SUCCESS] Created Providers.csv ({len(providers)} records)")

# ------------------------------------------------------------------------------
# 3. GENERATE PATIENTS.CSV (SCD TYPE 2 SOURCE)
# ------------------------------------------------------------------------------
patients = []
for i in range(1, NUM_PATIENTS + 1):
    pat_id = f"PAT_{i:06d}"
    mrn = f"MRN_{random.randint(100000, 999999)}"
    fname = random.choice(FIRST_NAMES)
    lname = random.choice(LAST_NAMES)
    dob = (START_DATE - timedelta(days=random.randint(6500, 28000))).strftime("%Y-%m-%d")
    gender = random.choice(["M", "F"])
    lang = random.choice(["English", "Spanish", "Hindi", "Mandarin"])
    city_idx = random.randint(0, len(CITIES) - 1)
    addr = f"{random.randint(100, 9999)} Main St, Apt {random.randint(1, 50)}"
    city = CITIES[city_idx]
    state = STATES[city_idx]
    zip_code = f"{random.randint(10000, 99999)}"
    payer = random.choice(INSURANCE_PAYERS)
    updated_ts = (START_DATE + timedelta(days=random.randint(0, 180))).strftime("%Y-%m-%d %H:%M:%S")

    patients.append({
        "PatientID": pat_id,
        "MedicalRecordNumber": mrn,
        "FirstName": fname,
        "LastName": lname,
        "DateOfBirth": dob,
        "Gender": gender,
        "PrimaryLanguage": lang,
        "AddressLine": addr,
        "City": city,
        "State": state,
        "ZipCode": zip_code,
        "PrimaryInsurancePayer": payer,
        "UpdatedTimestamp": updated_ts
    })

pat_file = os.path.join(OUTPUT_DIR, "Patients.csv")
with open(pat_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "PatientID", "MedicalRecordNumber", "FirstName", "LastName", "DateOfBirth",
        "Gender", "PrimaryLanguage", "AddressLine", "City", "State", "ZipCode",
        "PrimaryInsurancePayer", "UpdatedTimestamp"
    ])
    writer.writeheader()
    writer.writerows(patients)
print(f"[SUCCESS] Created Patients.csv ({len(patients)} records)")

# ------------------------------------------------------------------------------
# 4. GENERATE ENCOUNTERS.CSV
# ------------------------------------------------------------------------------
encounters = []
for i in range(1, NUM_ENCOUNTERS + 1):
    enc_id = f"ENC_{i:07d}"
    pat = random.choice(patients)
    prov = random.choice(providers)
    dept_id = prov["DepartmentID"]
    enc_type = random.choice(["EMERGENCY", "INPATIENT", "OUTPATIENT", "ICU"])
    
    admit_dt = START_DATE + timedelta(days=random.randint(0, 300), hours=random.randint(0, 23))
    stay_hours = round(random.uniform(2.0, 120.0), 2)
    discharge_dt = (admit_dt + timedelta(hours=stay_hours)).strftime("%Y-%m-%d %H:%M:%S")
    admit_str = admit_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    admit_reason = random.choice(["Chest Pain", "Shortness of Breath", "Acute Abdominal Pain", "High Fever", "Fracture", "Sepsis Alert", "Cardiac Arrhythmia"])
    status = random.choice(["HOME", "TRANSFERRED", "DECEASED", "AMA"]) if random.random() > 0.1 else "ADMITTED"
    billed_amt = round(random.uniform(500.0, 45000.0), 2)

    encounters.append({
        "EncounterID": enc_id,
        "PatientID": pat["PatientID"],
        "ProviderID": prov["ProviderID"],
        "DepartmentID": dept_id,
        "EncounterType": enc_type,
        "AdmitTimestamp": admit_str,
        "DischargeTimestamp": discharge_dt if status != "ADMITTED" else "",
        "LengthOfStayHours": stay_hours if status != "ADMITTED" else "",
        "AdmitReason": admit_reason,
        "DischargeStatus": status,
        "TotalBilledAmount": billed_amt
    })

enc_file = os.path.join(OUTPUT_DIR, "Encounters.csv")
with open(enc_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "EncounterID", "PatientID", "ProviderID", "DepartmentID", "EncounterType",
        "AdmitTimestamp", "DischargeTimestamp", "LengthOfStayHours", "AdmitReason",
        "DischargeStatus", "TotalBilledAmount"
    ])
    writer.writeheader()
    writer.writerows(encounters)
print(f"[SUCCESS] Created Encounters.csv ({len(encounters)} records)")

# ------------------------------------------------------------------------------
# 5. GENERATE BILLING_CLAIMS.CSV
# ------------------------------------------------------------------------------
claims = []
for i in range(1, NUM_CLAIMS + 1):
    enc = random.choice(encounters)
    claim_id = f"CLM_{i:07d}"
    claim_amt = round(enc["TotalBilledAmount"] * random.uniform(0.8, 1.2), 2)
    payer = random.choice(INSURANCE_PAYERS)
    status = random.choice(["APPROVED", "DENIED", "PENDING", "REJECTED"])
    claim_date = (datetime.strptime(enc["AdmitTimestamp"], "%Y-%m-%d %H:%M:%S") + timedelta(days=random.randint(1, 15))).strftime("%Y-%m-%d")

    claims.append({
        "ClaimID": claim_id,
        "EncounterID": enc["EncounterID"],
        "PatientID": enc["PatientID"],
        "ClaimAmount": claim_amt,
        "InsurancePayer": payer,
        "ClaimStatus": status,
        "ClaimDate": claim_date
    })

claims_file = os.path.join(OUTPUT_DIR, "BillingClaims.csv")
with open(claims_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["ClaimID", "EncounterID", "PatientID", "ClaimAmount", "InsurancePayer", "ClaimStatus", "ClaimDate"])
    writer.writeheader()
    writer.writerows(claims)
print(f"[SUCCESS] Created BillingClaims.csv ({len(claims)} records)")

# ------------------------------------------------------------------------------
# 6. GENERATE LAB_RESULTS.JSON
# ------------------------------------------------------------------------------
labs = []
TEST_TYPES = [
    {"code": "LOINC_8480-6", "name": "Systolic Blood Pressure", "unit": "mmHg", "min": 90, "max": 140},
    {"code": "LOINC_15074-8", "name": "Glucose", "unit": "mg/dL", "min": 70, "max": 180},
    {"code": "LOINC_2823-3", "name": "Potassium", "unit": "mmol/L", "min": 3.5, "max": 5.2},
    {"code": "LOINC_5792-7", "name": "Troponin I", "unit": "ng/mL", "min": 0.0, "max": 0.04}
]

for i in range(1, NUM_LAB_RESULTS + 1):
    enc = random.choice(encounters)
    test = random.choice(TEST_TYPES)
    val = round(random.uniform(test["min"] * 0.8, test["max"] * 1.3), 2)
    is_abnormal = val < test["min"] or val > test["max"]

    labs.append({
        "LabResultID": f"LAB_{i:07d}",
        "EncounterID": enc["EncounterID"],
        "PatientID": enc["PatientID"],
        "TestCode": test["code"],
        "TestName": test["name"],
        "TestValue": val,
        "Unit": test["unit"],
        "IsAbnormal": is_abnormal,
        "ResultTimestamp": (datetime.strptime(enc["AdmitTimestamp"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=random.randint(1, 24))).strftime("%Y-%m-%d %H:%M:%S")
    })

lab_file = os.path.join(OUTPUT_DIR, "LabResults.json")
with open(lab_file, "w", encoding="utf-8") as f:
    json.dump(labs, f, indent=2)
print(f"[SUCCESS] Created LabResults.json ({len(labs)} records)")

print("\n[SUCCESS] All synthetic healthcare batch datasets generated successfully!")
