# E-Commerce Order Analytics Pipeline (PySpark, ETL)

## 📌 Project Overview
This project implements an **ETL pipeline using PySpark** to process and analyze large-scale e-commerce order data. The goal is to generate insights into **order volume by customer state**, enabling data-driven decisions for demand forecasting and regional strategy.

---

## ⚙️ Features
- **Extract** raw order and customer datasets from CSV/Parquet sources.  
- **Transform** data using PySpark:
  - Clean and standardize input records.
  - Join order and customer datasets.  
  - Aggregate orders using `groupBy`, `count`, and `countDistinct`.  
- **Load** transformed results into a DataFrame / warehouse-ready table.  
- **Analytics**:
  - Compute **total order volume by customer state**.  
  - Identify **top 5 states** with highest order demand.  

---

## 🛠️ Tech Stack
- **Python**  
- **PySpark** (Spark SQL, DataFrame API)  
- **Jupyter Notebook / VS Code** (for development & testing)  
- **AWS S3 (optional)** for data storage  
- **Amazon EMR (optional)** for distributed execution  

---

## 📂 Project Structure
