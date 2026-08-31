"""
================================================================================
ApexCare Real-Time Healthcare Data Platform
Data Quality Engine & Quarantine Framework
================================================================================
Enforces business rules, schema contracts, null checks, and numeric bounds.
Valid records pass to Silver Delta tables; failed records are isolated in
the Silver Quarantine directory for auditing.
================================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when

class DataQualityEngine:
    def __init__(self, spark_session):
        self.spark = spark_session

    def validate_and_quarantine(self, df: DataFrame, rule_name: str, condition_expr, table_name: str, quarantine_path: str):
        """
        Splits incoming DataFrame into valid and invalid (quarantined) records based on condition_expr.
        
        Args:
            df: Input PySpark DataFrame
            rule_name: Descriptive rule identifier (e.g., 'NOT_NULL_PATIENT_ID')
            condition_expr: PySpark Column boolean expression representing valid records
            table_name: Target Silver table name
            quarantine_path: ABFSS storage path for quarantined Delta records
            
        Returns:
            valid_df: PySpark DataFrame containing records that passed the rule
        """
        # Add evaluation tag
        evaluated_df = df.withColumn("_dq_rule_failed", ~condition_expr)
        
        valid_df = evaluated_df.filter(col("_dq_rule_failed") == False).drop("_dq_rule_failed")
        invalid_df = evaluated_df.filter(col("_dq_rule_failed") == True)
        
        invalid_count = invalid_df.count()
        total_count = df.count()
        
        print(f"📊 [Data Quality Check: {rule_name}] Total: {total_count} | Valid: {valid_df.count()} | Quarantined: {invalid_count}")
        
        if invalid_count > 0:
            quarantine_target = f"{quarantine_path}/{table_name}_{rule_name}/"
            print(f"⚠️ [QUARANTINE ALERT] Writing {invalid_count} failed records to: {quarantine_target}")
            
            (invalid_df
             .withColumn("_quarantine_reason", lit(rule_name))
             .withColumn("_quarantined_at", current_timestamp())
             .write
             .format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .save(quarantine_target))
             
        return valid_df
