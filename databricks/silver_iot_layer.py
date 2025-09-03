# Databricks notebook source


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg, window
import traceback

# ==================================================
# 1. Spark session
# ==================================================
try:
    spark = SparkSession.builder.appName("IoT_Silver").getOrCreate()
    print("✅ SparkSession created successfully")
except Exception as e:
    print("❌ Error creating SparkSession:", e)
    traceback.print_exc()
    
    raise

# ==================================================
# 2. Read from Bronze Delta
# ==================================================
try:
    bronze_df = spark.read.format("delta").table("iot_catalog.raw.sensor_readings")
    print(f"✅ Bronze table loaded, records: {bronze_df.count()}")
except Exception as e:
    print("❌ Error reading Bronze table:", e)
    traceback.print_exc()
    raise

# ==================================================
# 3. Select + Standardize column names
# ==================================================
try:
    silver_df = bronze_df.select(
        col("Time").alias("time"),
        col("Temperature").cast("double").alias("temperature"),
        col("Humidity").cast("double").alias("humidity"),
        col("Air_Quality").cast("double").alias("air_quality"),
        col("Light").cast("double").alias("light"),
        col("Loudness").cast("double").alias("loudness")
    )
    print("✅ Columns standardized successfully")
except Exception as e:
    print("❌ Error selecting/casting columns:", e)
    traceback.print_exc()
    raise

# ==================================================
# 4. Filter invalid records
# ==================================================
try:
    silver_df = silver_df.filter(
        (col("temperature").isNotNull()) & (col("temperature") >= 0) &
        (col("humidity").isNotNull()) & (col("humidity") >= 0) &
        (col("air_quality").isNotNull()) & (col("air_quality") >= 0) &
        (col("light").isNotNull()) & (col("light") >= 0) &
        (col("loudness").isNotNull()) & (col("loudness") >= 0)
    )
    print(f"✅ Invalid records filtered, remaining: {silver_df.count()}")
except Exception as e:
    print("❌ Error filtering invalid records:", e)
    traceback.print_exc()
    raise

# ==================================================
# 5. Convert epoch time to proper timestamp
# ==================================================
try:
    silver_df = silver_df.withColumn("event_time", to_timestamp(col("time").cast("long")))
    print("✅ Event time converted successfully")
except Exception as e:
    print("❌ Error converting timestamp:", e)
    traceback.print_exc()
    raise

# ==================================================
# 6. Compute rolling metrics (5-min averages)
# ==================================================
try:
    rolling_df = silver_df.groupBy(
        window(col("event_time"), "5 minutes")
    ).agg(
        avg("temperature").alias("avg_temp_5min"),
        avg("humidity").alias("avg_humidity_5min"),
        avg("air_quality").alias("avg_air_quality_5min"),
        avg("light").alias("avg_light_5min"),
        avg("loudness").alias("avg_loudness_5min")
    )
    print("✅ Rolling averages calculated")
except Exception as e:
    print("❌ Error computing rolling averages:", e)
    traceback.print_exc()
    raise

# ==================================================
# 7. Join rolling metrics back
# ==================================================
try:
    silver_df = silver_df.join(
        rolling_df,
        (silver_df.event_time >= rolling_df.window.start) &
        (silver_df.event_time <= rolling_df.window.end),
        how="left"
    ).drop("window")
    print("✅ Rolling metrics joined with Silver dataframe")
except Exception as e:
    print("❌ Error joining rolling metrics:", e)
    traceback.print_exc()
    raise

# ==================================================
# 8. Write Silver table
# ==================================================
try:
    silver_df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("iot_catalog.processed.valid_readings")
    print("✅ Silver table written successfully")
except Exception as e:
    print("❌ Error writing Silver table:", e)
    traceback.print_exc()
    raise

# ==================================================
# 9. Visualizations (Databricks display)
# ==================================================
try:
    print("📊 Displaying cleaned Silver dataframe")
    display(silver_df)

    print("📊 Average temperature trend (5-min window)")
    display(
        silver_df.groupBy("event_time").agg(avg("temperature").alias("avg_temp"))
    )

    print("📊 Correlation matrix (temperature, humidity, air_quality, light, loudness)")
    numeric_cols = ["temperature", "humidity", "air_quality", "light", "loudness"]
    corr_data = {col1: [silver_df.corr(col1, col2) for col2 in numeric_cols] for col1 in numeric_cols}
    import pandas as pd
    corr_df = pd.DataFrame(corr_data, index=numeric_cols)
    display(corr_df)
except Exception as e:
    print("❌ Error in visualization:", e)
    traceback.print_exc()


# COMMAND ----------

