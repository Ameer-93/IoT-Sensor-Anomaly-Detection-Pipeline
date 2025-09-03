# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, countDistinct
import unittest

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Load Gold layer table
try:
    gold_df = spark.table("iot_catalog.analytics.device_stats")
except Exception as e:
    print("❌ Could not load Gold Layer table:", e)
    gold_df = None

expected_columns = [
    "time", "temperature", "humidity", "air_quality", "light", "loudness", "event_time",
    "avg_temp_5min", "avg_humidity_5min", "avg_air_quality_5min", "avg_light_5min", "avg_loudness_5min",
    "temp_anomaly", "humidity_anomaly", "overall_anomaly", "window",
    "avg_temp", "avg_humidity", "avg_air_quality", "avg_light", "avg_loudness", "total_anomalies"
]

class IoTGoldExtendedTests(unittest.TestCase):

    # 1️⃣ Row Count Test
    def test_row_count(self):
        if gold_df is None: self.skipTest("Gold layer table missing")
        self.assertGreater(gold_df.count(), 0, "Gold table is empty ❌")

    # 2️⃣ Schema Validation
    def test_schema_columns(self):
        if gold_df is None: self.skipTest("Gold layer table missing")
        actual_cols = [f.name for f in gold_df.schema.fields]
        missing = [c for c in expected_columns if c not in actual_cols]
        self.assertFalse(missing, f"Missing schema columns: {missing} ❌")

    # 3️⃣ Anomaly Flag Validation
    def test_anomaly_flags(self):
        if gold_df is None: self.skipTest("Gold layer table missing")
        bad_flags = gold_df.filter(
            (~col("temp_anomaly").isin([0,1])) |
            (~col("humidity_anomaly").isin([0,1])) |
            (~col("overall_anomaly").isin([0,1]))
        ).count()
        self.assertEqual(bad_flags, 0, "Invalid anomaly flag values ❌")

    # 4️⃣ Unique Time Window Test
    def test_unique_window(self):
        if gold_df is None: self.skipTest("Gold layer table missing")
        duplicate_windows = gold_df.groupBy("window").count().filter(col("count") > 1).count()
        self.assertEqual(duplicate_windows, 0, "Duplicate time windows exist ❌")

    # 5️⃣ Value Range Test (Physical Sensor Limits)
    def test_value_ranges(self):
        if gold_df is None: self.skipTest("Gold layer table missing")
        temp_out_of_range = gold_df.filter((col("temperature") < -50) | (col("temperature") > 100)).count()
        humidity_out_of_range = gold_df.filter((col("humidity") < 0) | (col("humidity") > 100)).count()
        self.assertEqual(temp_out_of_range, 0, "Temperature values out of range ❌")
        self.assertEqual(humidity_out_of_range, 0, "Humidity values out of range ❌")

    # 6️⃣ Duplicate Rows Test
    def test_duplicate_rows(self):
        if gold_df is None: self.skipTest("Gold layer table missing")
        total_count = gold_df.count()
        distinct_count = gold_df.distinct().count()
        self.assertEqual(total_count, distinct_count, "Duplicate rows exist in Gold table ❌")

# ---------------- Run Tests ----------------
suite = unittest.TestLoader().loadTestsFromTestCase(IoTGoldExtendedTests)
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)

# ---------------- Summary ----------------
print("\n===== EXTENDED TEST SUMMARY =====")
print(f"Ran {result.testsRun} tests")
print(f"✅ Passed: {result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped)}")
print(f"❌ Failed: {len(result.failures)}")
print(f"⚠️ Errors: {len(result.errors)}")
print(f"⏭️ Skipped: {len(result.skipped)}")
