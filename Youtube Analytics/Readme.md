# 📊 YouTube Trending Analysis (AWS)
## Understanding what makes a YouTube video go viral

## 📌 Project Overview
This project analyzes YouTube trending video data collected using an end-to-end AWS ETL pipeline.
The goal is to identify key factors that influence the popularity of YouTube videos and present insights through a Tableau dashboard.

---

## Architecture Flow
![ETL Architecture Flow](https://github.com/bhavi40/DataEngineering/blob/main/Youtube%20Analytics/AWS%20Data%20Pipeline%20%20Architecture.png)

## 1. Workflow Steps
Created the following Azure resources within a Resource Group:
  - **Azure Data Lake Storage Gen2** - for implementing Medallion layers (Bronze, Silver, Gold)
  - **Azure Data Factory** - for pipeline orchestration
  - **Azure Databricks** - for data transformation using PySpark
  - **Azure Cosmos DB** - for data enrichment
  - **Azure Synapse Analytics** - for gold-layer reporting
---

## 2. Data Ingestion (Bronze Layer)
- Data sourced from:
   - GitHub – eCommerce dataset (CSV files)
   - Azure SQL – product and sales data
- Used Azure Data Factory (ADF) pipelines to ingest data from both sources into ADLS Gen2 (Bronze layer).
---

## 3. Data Transformation (Silver Layer)
- Transformation done in Azure Databricks using PySpark:
   - Data cleaning, deduplication, and formatting
   - Joins and enrichment with data from Azure Cosmos DB
   - Creation of derived columns
- Transformed data stored back in ADLS Gen2 → Silver layer
---

## Data Serving (Gold Layer)
- Connected Azure Synapse Analytics to the ADLS Gen2 Silver layer.
- Created views and external tables in Synapse for the Gold layer.
- Final curated data is ready for dashboards and analytical queries.
---

## 📁 Project Structure
```text
📦 Azure-ETL-Pipeline
│
├── CodeForDataIngestion/
│   ├── [Scripts to send local data to Azure SQL & Cosmos DB]
│   └── (Acts as data source for ADF pipelines)
│
├── Data/
│   ├── [GitHub-based source data files, e.g., ecommerce CSVs]
│   └── (Raw data for ingestion to ADLS Gen2 Bronze layer)
│
├── Databricks_ecommerce.ipynb
│   ├── PySpark transformation logic:
│   │   - Cleans and enriches data
│   │   - Joins with Cosmos DB
│   │   - Writes to Silver layer in ADLS Gen2
│
├── ForEachInput.json
│   ├── ADF helper file for lookup activity:
│   │   - Used to iterate through multiple input files dynamically
│   │   - Enables sequential ingestion without manual uploads
│
├── ForEachInputScript.ipynb
│   ├── Python/Notebook used to generate the `ForEachInput.json` dynamically
│   │   - Simplifies automation of the pipeline’s lookup step
│
└── README.md



