# 🇦🇪 Dubai Real Estate Data Lake | Enterprise Medallion Architecture

![Databricks](https://img.shields.io/badge/Compute-Databricks_Serverless-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/Processing-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Storage-Delta_Lake-00AAD2?style=for-the-badge)
![Python](https://img.shields.io/badge/Language-Python_3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 📌 Architecture Overview

A production-grade, highly scalable data engineering pipeline engineered for the fast-moving Dubai real estate market. This system automatically processes, sanitizes, and models high-volume property transaction streams (10M+ records) into a highly structured data lakehouse. 

It utilizes a strict **three-tier Medallion Architecture (Bronze ➔ Silver ➔ Gold)** optimized for distributed big data processing, schema governance, and real-time business intelligence readiness.

---

## 🏗️ System Design

> **Note:** Architecture Diagram goes here. (Upload an image to your repo and link it!)

### Pipeline Stages:
1. **🟤 Bronze Layer (Raw Ingestion):** Ingests streaming property transactions (10.5M records) distributed natively across Databricks nodes. Persisted as append-only Delta tables for historical preservation.
2. **⚪ Silver Layer (Sanitization & QA):** Programmatically normalizes complex currency strings (AED/M) into strict numeric floats using Regex. Deploys PySpark window functions (`row_number()`) to prune late-arriving duplicate events.
3. **🟡 Gold Layer (Analytical Optimization):** Transforms sanitized events into a high-performance Star Schema. Generates aggregated business metrics (Avg Price/SqFt, Regional Volume) across Downtown, Marina, JVC, and Palm Jumeirah.

---

## ⚙️ Core Technical Implementation

### Regular Expression Data Standardization
```python
# Normalizing complex textual shorthand (e.g., '3.5M', '3500000 AED') into uniform float currencies
cleaned_price_df = raw_bronze_df.withColumn(
    "clean_price_aed",
    when(col("raw_price").contains("M"), 
         regexp_replace(col("raw_price"), "M", "").cast("double") * 1000000)
    .otherwise(regexp_replace(col("raw_price"), " AED", "").cast("double"))
)
