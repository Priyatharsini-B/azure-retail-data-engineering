# Azure Retail Data Engineering Project
---
## Project Overview
- This project demonstrates an end-to-end Azure Data Engineering solution for a retail business.
The goal is to ingest data from multiple sources, transform it using cloud-native tools, and build analytical dashboards.
---
## Architecture

**Source → Ingestion → Storage → Transformation → Analytics**

- Source Systems
      - Azure SQL Database (Products, Stores, Transactions)
      - REST API (Customer data)
- Ingestion
      - Azure Data Factory (Copy Activity)
- Storage
      - Azure Data Lake Storage Gen2
      - Medallion Architecture (Bronze, Silver, Gold)
- Transformation
      - Azure Databricks (PySpark, Delta Lake)
- Serving Layer
      - Azure SQL Database (Star Schema)
- Visualization
      - Power BI (DAX, Interactive Dashboards)
---
## Medallion Architecture

- Bronze Layer – Raw ingested data from source systems
- Silver Layer – Cleaned and standardized data
- Gold Layer – Aggregated and business-ready data
---
## Data Flow

1. Data ingested from Azure SQL and REST API using ADF pipelines
2. Raw data stored in ADLS Bronze layer
3. Transformations applied in Databricks
4. Processed data written to Silver and Gold layers
5. Gold data loaded into Azure SQL
6. Power BI dashboards built on curated data
---
## Tech Stack

- Azure Data Factory
- Azure Databricks (PySpark, Delta Lake)
- Azure Data Lake Storage Gen2
- Azure SQL Database
- Power BI
- GitHub
---
## Key Features Implemented

- End-to-end ETL pipeline using Azure services
- Medallion architecture (Bronze → Silver → Gold)
- Data transformations using PySpark
- Star schema modeling in Azure SQL
- Power BI dashboards with DAX measures
---
## Planned Enhancements (Phase-2)

- Parameterized ADF pipelines
- Ingestion date–based partitioning
- Incremental load using watermark logic
- Error handling & logging framework
- Performance optimization using Delta Lake
---
## Sample Dashboard
<img width="1418" height="798" alt="Screenshot 2026-01-30 155536" src="https://github.com/user-attachments/assets/a0da140c-8896-45ce-8dd5-6578fa3b1f71" />





