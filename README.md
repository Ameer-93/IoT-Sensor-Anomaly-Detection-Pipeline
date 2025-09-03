# IoT-Sensor-Anomaly-Detection-Pipeline


## 📌 Project Overview  
This project implements a **real-time IoT anomaly detection pipeline** using **Amazon Kinesis, AWS Lambda, PySpark, Databricks, and Delta Lake**.  
It streams IoT sensor data (temperature, humidity, air quality, etc.), applies **z-score–based anomaly detection**, and stores curated results in Bronze, Silver, and Gold layers for **real-time monitoring and predictive maintenance**.  

The pipeline is fault-tolerant, scalable, and integrates **alerts (Slack, CloudWatch)** along with **data quality checks, error handling, and Git-based version control**.  

---

## 🎯 Objective  
- Build a **streaming data pipeline** to detect anomalies in IoT sensor readings.  
- Provide **predictive maintenance insights** by monitoring abnormal device behavior.  
- Ensure **data quality, error handling, and governance** via Delta Lake and Unity Catalog.  

---

## 🏗️ Architecture  

**Data Flow:**  
1. **Producer (IoT dataset from Kaggle – AnoML-IoT)** → JSON records with temperature, humidity, etc.  
2. **AWS Lambda** → pushes data into **Amazon Kinesis stream**.  
3. **Bronze Layer** → Raw ingestion into Delta Lake (`iot_catalog.raw.sensor_readings`).  
4. **Silver Layer** → Cleaned, validated data + 5-min rolling aggregates (`iot_catalog.processed.valid_readings`).  
5. **Gold Layer** → Anomaly detection (z-score), aggregated stats, and anomaly logs (`iot_catalog.analytics.device_stats`).  
6. **Alerts** → Slack + CloudWatch metrics for anomaly spikes.  
7. **Orchestration** → Managed with **Databricks Workflows**.  

---

## 🔄 Pipeline Layers  

### 🟤 Bronze Layer (`bronze_iot_layer.py`)  
- Streams raw JSON events from **Amazon Kinesis → S3**.  
- Applies schema validation & catches bad records in an error path.  
- Stores raw data in Delta tables.  

### ⚪ Silver Layer (`silver_iot_layer.py`)  
- Cleans and standardizes data (type casting, filtering invalid values).  
- Adds **rolling 5-minute averages**.  
- Writes processed data into curated Silver Delta tables.  

### 🟡 Gold Layer (`gold_iot_layer.py`)  
- Computes mean/std for anomaly detection.  
- Flags anomalies using **3-sigma (z-score) rule**.  
- Aggregates hourly metrics (temperature, humidity, etc.).  
- Stores results in analytics tables + anomaly logs.  
- Pushes metrics to **AWS CloudWatch** and triggers alerts.  

---

## ⚙️ Tools & Technologies  
- **Databricks** (Workflows, Delta Lake, Unity Catalog)  
- **PySpark** (streaming, transformations, anomaly detection)  
- **Amazon Kinesis** (real-time ingestion)  
- **AWS Lambda** (data ingestion to Firehose)  
- **Slack / CloudWatch** (alerting & monitoring)  
- **GitHub** (version control, CI/CD)  

---

## ✅ Key Features & Outcomes  
- **Real-Time Streaming** → 10-second micro-batches for IoT data.  
- **Data Quality Checks** → Filtering invalid/corrupt records, schema validation.  
- **Error Handling** → Logging anomalies & ingestion errors.  
- **Scalability** → Autoscaling Databricks cluster (8–32 workers, m5.xlarge).  
- **Testing** → Unit tests (`test.py`) for schema validation, anomaly flags, and ranges.  
- **Business Impact** → Early anomaly detection reduces downtime & maintenance costs.  

---

## 📂 Repository Structure  

**iot_sensor_anomaly_detection_pipeline/
│── bronze_iot_layer.py # Bronze layer ingestion
│── silver_iot_layer.py # Silver layer cleansing + rolling metrics
│── gold_iot_layer.py # Gold layer anomaly detection + analytics
│── lambda_function.py # Lambda function for ingesting data to Kinesis
│── kinesis.txt # Kinesis pipeline documentation
│── test.py # Unit tests for validation
│── iaddataset.csv # Sample IoT dataset (AnoML-IoT)
│── IoT_Anomly_Detection_Project.pptx # Project presentation
│── .gitignore # Ignore unnecessary files
│── README.md # Project documentation**



---

## 🧪 Testing  
Run tests to validate the Gold layer schema, anomaly flags, and data quality:  
```bash
python test.py


**🚀 How to Run**

**1. Clone the Repo**

git clone https://github.com/Ameer-93/iot_sensor_anomaly_detection_pipeline.git
cd iot_sensor_anomaly_detection_pipeline

**2. Setup Dependencies**

Install Python libraries (for local testing/validation):

pip install pyspark delta-spark boto3 pandas matplotlib seaborn

**3. Import into Databricks**

Upload notebooks (bronze, silver, gold) into Databricks.

Configure cluster with required libraries (delta-spark, aws-kinesis-spark).

Set up Amazon Kinesis stream and update paths.

Run the pipeline with Databricks Workflows.

**4. Monitor**

Use Slack notifications for anomaly alerts.

Use CloudWatch dashboards for anomaly metrics.

📊 **Business Value**

Predictive Maintenance: Detect anomalies in real-time to reduce equipment failures.

Operational Efficiency: Automated monitoring and alerts streamline maintenance.

Data Reliability: Ensures clean, validated, and governed data across Bronze–Silver–Gold layers.

🎥 Demo & Presentation

📌 See IoT_Anomly_Detection_Project.pptx in this repo for architecture diagrams, flow, and outcomes.

**👨‍💻 Author**

Syed Ameer – Aspiring Data Engineer

Skills: Python, SQL, PySpark, Databricks, AWS, BigQuery, ETL Pipelines

GitHub: Ameer-93 
