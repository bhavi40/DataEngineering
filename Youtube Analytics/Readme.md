# 🚀 YouTube Trending Data ETL Pipeline (AWS)
Understanding what makes a YouTube video go viral

## 📌 Project Overview
This project analyzes YouTube trending video data collected using an end-to-end AWS ETL pipeline.
The goal is to identify key factors that influence the popularity of YouTube videos and present insights through a Tableau dashboard.

---

## Architecture Flow
![ETL Architecture Flow](https://github.com/bhavi40/DataEngineering/blob/main/Youtube%20Analytics/Architecture.png)

## Workflow Steps
Used the following AWS services:
  - **AWS CLI** - To push the raw data to S3
  - **Amazon S3** - Raw & Processed data storage
  - **AWS Step Functions** - Workflow orchestration
  - **Amazon Lambda** - clean the Json and convert into parquet
  - **AWS Glue ETL Job** - Transform raw csv files into optimized parquet
  - **AWS Glue Crawler** - Auto-detect files in s3 → catalog tables
  - **AWS Glue Catalog** - Metadata storage for Athena
  - **Amazon Athena** - SQL queries directly on S3
  - **AWS Glue ETL Job** - To Join the cleaned csv and Json 
---

## 2. DataSet
- Data is  sourced from kaggle - ![Kaggle Data](https://www.kaggle.com/datasets/datasnaek/youtube-new)
---


## 📁 Project Structure
```text
📦 AWS-ETL-Pipeline
│
├── S3_cli_command.sh
│   ├── [Scripts to send local data to s3]
│
├── Data/
│   └── [Kaggle Link]
│
├── Lambda_function_cleanJson.py
│   ├── python code to clean JSON and convert into optimized parquet files and store them in s3
│
├── ETL JOB-cleansed-csv-to-parquet.py
│   ├── python code that imports data from data catalog and converts into optimized parquet file
│
├── ETL JOB- joining-csv-json.py
│   ├── python code that imports csv and json tables from data catalog,  joins them and converts to optimized parquet file
│
└── README.md
```
---

## Installations

AWS CLI - ![Guide to Install AWS CLI](https://aws.amazon.com/cli/)

