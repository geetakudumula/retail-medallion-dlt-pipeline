# Retail Medallion DLT Pipeline (Databricks)

## Overview
This project demonstrates a Delta Live Tables (DLT) pipeline implemented using the Medallion Architecture (Bronze → Silver → Gold) in Databricks.

## Architecture
- **Bronze Layer**: Raw data ingestion from Delta table
- **Silver Layer**: Data cleaning and enrichment
- **Gold Layer**: Business-level aggregations

## Pipeline Flow
bronze_sales → silver_sales → 
- gold_sales_by_product
- gold_sales_by_day
- gold_top_customers

## Technologies Used
- Databricks Delta Live Tables (DLT)
- PySpark
- Delta Lake
- GitHub (Version Control)

## Key Features
- Declarative pipeline using @dp.materialized_view
- Automatic dependency management
- Batch processing pipeline
- Modular medallion design

## How to Run
1. Create DLT pipeline in Databricks
2. Attach transformations folder
3. Run pipeline
4. View lineage graph

## Author
Geeta Kudumula
