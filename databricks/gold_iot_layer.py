# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, when, abs, lit, sum, count, window, round
from pyspark.sql.types import DoubleType, StringType, TimestampType
import boto3
import datetime
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Add aws-services folder to Python path if needed
sys.path.append(os.path.abspath("../aws-services"))
import aws_credentials  # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

# ----------------------------
# 1. Spark Session
# ----------------------------
spark = SparkSession.builder.appName("IoT_Gold").getOrCreate()

# ----------------------------
# 2. Read from Silver Layer
# ----------------------------
try:
    silver_df = spark.read.format("delta").table("iot_catalog.processed.valid_readings")
    print("✅ Silver table read successful")
except Exception as e:
    print(f"❌ Failed to read Silver table: {e}")
    sys.exit(1)

# ----------------------------
# 3. Compute Mean & Stddev for anomaly detection
# ----------------------------
stats = silver_df.agg(
    avg("temperature").alias("mean_temp"),
    stddev("temperature").alias("std_temp"),
    avg("humidity").alias("mean_humidity"),
    stddev("humidity").alias("std_humidity")
).collect()[0]

mean_temp = stats["mean_temp"] or 0
std_temp = stats["std_temp"] or 0.0001
mean_humidity = stats["mean_humidity"] or 0
std_humidity = stats["std_humidity"] or 0.0001

# ----------------------------
# 4. Add Anomaly Flags
# ----------------------------
gold_df = silver_df.withColumn(
    "temp_anomaly",
    when(abs(col("temperature") - lit(mean_temp)) > 3 * lit(std_temp), 1).otherwise(0)
).withColumn(
    "humidity_anomaly",
    when(abs(col("humidity") - lit(mean_humidity)) > 3 * lit(std_humidity), 1).otherwise(0)
).withColumn(
    "overall_anomaly",
    when((col("temp_anomaly") == 1) | (col("humidity_anomaly") == 1), 1).otherwise(0)
)

# ----------------------------
# 5. Aggregate Metrics (hourly)
# ----------------------------
device_stats_df = gold_df.groupBy(
    window(col("event_time"), "1 hour")
).agg(
    round(avg("temperature"), 4).alias("avg_temp"),
    round(avg("humidity"), 4).alias("avg_humidity"),
    round(avg("air_quality"), 4).alias("avg_air_quality"),
    round(avg("light"), 4).alias("avg_light"),
    round(avg("loudness"), 4).alias("avg_loudness"),
    sum("overall_anomaly").alias("total_anomalies")
).orderBy("window")

# ----------------------------
# 6. Save Gold Analytics Table
# ----------------------------
device_stats_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("iot_catalog.analytics.device_stats")

# ----------------------------
# 7. Log Anomalies
# ----------------------------
anomaly_log_df = gold_df.filter(col("overall_anomaly") == 1).select(
    round(col("temperature"), 4).cast(DoubleType()).alias("temp_value"),
    round(col("humidity"), 4).cast(DoubleType()).alias("humidity_value"),
    col("event_time").cast(TimestampType()).alias("detected_at")
).withColumn(
    "details", lit("Anomaly detected").cast(StringType())
)

anomaly_log_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("iot_catalog.logs.anomaly_log")

# ----------------------------
# 8. Persist Anomaly Rates (10-min window)
# ----------------------------
anomaly_rate_df = gold_df.filter(col("overall_anomaly") == 1).groupBy(
    window(col("event_time"), "10 minutes")
).agg(count("*").alias("anomaly_count"))

anomaly_rate_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("iot_catalog.analytics.anomaly_rate")

# ----------------------------
# 9. Push Metrics to AWS CloudWatch
# ----------------------------
cloudwatch = boto3.client(
    "cloudwatch",
    region_name=aws_credentials.AWS_REGION,
    aws_access_key_id=aws_credentials.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=aws_credentials.AWS_SECRET_ACCESS_KEY
)

def safe_timestamp(ts):
    now = datetime.datetime.now(datetime.timezone.utc)
    two_weeks_ago = now - datetime.timedelta(days=14)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=datetime.timezone.utc)
    if ts < two_weeks_ago:
        return two_weeks_ago + datetime.timedelta(seconds=1)
    elif ts > now:
        return now
    return ts

def push_metric(namespace, metric_name, value, timestamp=None, unit="Count", dimensions=None):
    if dimensions is None:
        dimensions = []
    ts = safe_timestamp(timestamp or datetime.datetime.now(datetime.timezone.utc))
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[{
            "MetricName": metric_name,
            "Dimensions": dimensions,
            "Timestamp": ts,
            "Value": float(value),
            "Unit": unit
        }]
    )

for row in device_stats_df.collect():
    push_metric("IoT/GoldLayer", "TotalAnomalies", row["total_anomalies"], row["window"]["start"])

for row in anomaly_rate_df.collect():
    push_metric("IoT/GoldLayer", "AnomalyRate_10min", row["anomaly_count"], row["window"]["start"])

# ----------------------------
# 10. Display & Visualizations
# ----------------------------

# Display Gold Metrics
display(
    spark.read.table("iot_catalog.analytics.device_stats")
         .select("window", "avg_temp", "avg_humidity", "total_anomalies")
         .orderBy("window")
         .limit(20)
)

# Display Anomaly Log
display(
    spark.read.table("iot_catalog.logs.anomaly_log")
         .select("temp_value", "humidity_value", "detected_at", "details")
         .orderBy(col("detected_at").desc())
         .limit(20)
)

# Display Anomaly Rate
display(
    spark.read.table("iot_catalog.analytics.anomaly_rate")
         .select("window", "anomaly_count")
         .orderBy("window")
         .limit(20)
)

# ----------------------------
# 11. Matplotlib / Seaborn Visualizations (optional)
# ----------------------------
# Convert to Pandas for plotting and extract window start
stats_pdf = device_stats_df.select(
    col("window.start").alias("window_start"),
    "avg_temp",
    "avg_humidity",
    "total_anomalies"
).toPandas()

anomaly_pdf = anomaly_rate_df.select(
    col("window.start").alias("window_start"),
    "anomaly_count"
).toPandas()

# Ensure datetime format
stats_pdf["window_start"] = pd.to_datetime(stats_pdf["window_start"])
anomaly_pdf["window_start"] = pd.to_datetime(anomaly_pdf["window_start"])

# Plot Hourly Avg Temperature & Humidity
plt.figure(figsize=(14,5))
sns.lineplot(data=stats_pdf, x="window_start", y="avg_temp", label="Avg Temperature")
sns.lineplot(data=stats_pdf, x="window_start", y="avg_humidity", label="Avg Humidity")
plt.xticks(rotation=45)
plt.title("Hourly Avg Temperature & Humidity")
plt.legend()
plt.show()

# Plot Total Anomalies per Hour
plt.figure(figsize=(14,5))
sns.barplot(data=stats_pdf, x="window_start", y="total_anomalies", color="red")
plt.xticks(rotation=45)
plt.title("Total Anomalies (Hourly)")
plt.show()

# Plot Anomaly Rate (10-min Window)
plt.figure(figsize=(14,5))
sns.lineplot(data=anomaly_pdf, x="window_start", y="anomaly_count", marker="o", color="orange")
plt.xticks(rotation=45)
plt.title("Anomaly Count (10-min Buckets)")
plt.show()
