# 🚴 Citi Bike Data Engineering Platform (Databricks + Delta Lake)

## 📌 Overview

This project builds an end-to-end data engineering pipeline using Citi Bike trip data. The goal is to simulate a real-world data platform using modern data engineering tools and practices.

## 🧱 Architecture

* Data Source: Public S3 dataset (Citi Bike trip data)
* Processing: Databricks (PySpark + SQL)
* Storage: Delta Lake tables (Bronze, Silver, Gold)
* Governance: Unity Catalog

## Data Transformation Layers

- **Bronze**: Raw ingested data from CitiBike API
- **Silver**: Cleaned and enriched datasets (handled via SQL scripts in `/sql/silver`)
- **Gold**: Aggregated datasets for analytics and reporting

## 🔄 Pipeline Design (Medallion Architecture)

### Bronze Layer

* Raw data ingestion from S3
* Minimal transformations
* Append-only storage

### Silver Layer

* Data cleaning and validation
* Schema standardization
* Deduplication

### Gold Layer

* Business-ready tables
* Aggregations for analytics

## ⚙️ Tech Stack

* Python (PySpark)
* SQL
* Delta Lake
* Databricks

## 🚧 Challenges & Learnings

* Handling ZIP files during ingestion
* Working with restricted storage (no DBFS root)
* Managing schema inconsistencies across datasets
* Designing incremental ingestion pipelines
* Deriving KPIs with a business impact

## 📊 Future Improvements

* Add orchestration (Airflow / Databricks Workflows)
* Implement data quality checks
* Add dashboard (Power BI)
* Introduce streaming ingestion

## 📁 Project Structure

(Explain folders here briefly)

## 🚀 How to Run

(To be added)

## ✅ Current Progress
- Implemented ingestion pipeline from public S3 dataset
- Handled ZIP file extraction in-memory due to environment restrictions
- Loaded data into Bronze Delta table
- Wrote silver transformations in sql - cleaned data and added derived columns
- Gold aggregation for Bike Angels program in sql
- Orchestrated the sql queries using Python notebook
