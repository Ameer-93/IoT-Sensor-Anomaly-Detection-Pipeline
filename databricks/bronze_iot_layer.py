# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.utils import AnalysisException
import traceback

# ----------------------------
# 1. Spark Session
# ----------------------------
spark = SparkSession.builder.appName("IoT_Bronze").getOrCreate()

# ----------------------------
# 2. Define Bronze Schema (Raw only)
# ----------------------------
bronze_schema = StructType([
    StructField("Time", StringType(), True),
    StructField("Temperature", StringType(), True),
    StructField("Humidity", StringType(), True),
    StructField("Air_Quality", StringType(), True),
    StructField("Light", StringType(), True),
    StructField("Loudness", StringType(), True)
])

# ----------------------------
# 3. Safe Streaming Read from S3
# ----------------------------
bronze_path = "s3://iotdatabucket5/2025/08/21/17/"

try:
    bronze_stream_df = (
        spark.readStream
        .schema(bronze_schema)
        .option("badRecordsPath", "s3://iotdatabucket52/error/bronze_bad_records/")  # catches corrupt JSON
        .json(bronze_path)
    )
except AnalysisException as e:
    print("❌ Error while reading from S3:", e)
    traceback.print_exc()
    raise
except Exception as e:
    print("❌ Unexpected error during stream read:", e)
    traceback.print_exc()
    raise

# ----------------------------
# 4. Clean Column Names
# ----------------------------
for c in bronze_stream_df.columns:
    try:
        new_name = (
            c.strip()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("{", "")
            .replace("}", "")
            .replace(";", "")
            .replace("=", "")
            .replace("\n", "")
            .replace("\t", "")
        )
        if new_name != c:
            bronze_stream_df = bronze_stream_df.withColumnRenamed(c, new_name)
    except Exception as e:
        print(f"⚠️ Skipping column rename for {c}: {e}")

# ----------------------------
# 5. Write Bronze Delta Table (RAW only) with checkpointing
# ----------------------------
bronze_checkpoint_path = "s3://iotdatabucket5/checkpoints/bronze/"
bronze_table_name = "iot_catalog.raw.sensor_readings"

try:
    bronze_stream_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", bronze_checkpoint_path) \
        .trigger(processingTime="2 seconds") \
        .toTable(bronze_table_name)
except Exception as e:
    print("❌ Error while writing to Delta table:", e)
    traceback.print_exc()
    raise

# ----------------------------
# 6. Optional: Display stream (for Databricks notebooks)
# ----------------------------
try:
    display(bronze_stream_df)
except Exception as e:
    print("⚠️ Display failed:", e)
