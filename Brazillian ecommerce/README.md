# Azure End-to-End ETL Pipeline using Medallion Architecture

## 📌 Project Overview
This project implements a complete ETL (Extract, Transform, Load) pipeline using Microsoft Azure services and the Medallion Architecture (Bronze → Silver → Gold).
The pipeline ingests data from GitHub and Azure SQL, performs transformations and enrichment in Azure Databricks using Cosmos DB, and finally loads curated data into Azure Synapse Analytics for analysis.

---

## Architecture Flow
![ETL Architecture Flow](https://github.com/bhavi40/DataEngineering/blob/main/Brazillian%20ecommerce/Architecture%20Diagram.png)

## Workflow Steps
- Created the following Azure resources within a Resource Group:
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
