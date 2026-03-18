# 🏥 Healthcare Staffing & Analytics Orchestration Engine

**An automated data engineering solution that synchronizes Payroll-Based Journal (PBJ) staffing records with facility metadata to generate actionable labor and compliance intelligence.**

## 🎯 Project Purpose: Architectural Comparison
This project demonstrates two distinct methods for handling healthcare data ETL and analytics. By providing both **Google Colab** and **Databricks** versions, it compares how data workflows scale from individual research environments to enterprise-grade data lakehouses.

| Feature | **Method 1: Google Colab** | **Method 2: Azure Databricks** |
| :--- | :--- | :--- |
| **Engine** | Local Python + In-Memory SQLite | Distributed Apache Spark (PySpark) |
| **Data Handling** | Single-node RAM (Pandas) | Multi-node Cluster (Spark DataFrames) |
| **Storage** | Temporary VM Storage | Delta Lake / DBFS |
| **Ideal Use Case** | Rapid Prototyping & Small Datasets | Big Data Production & ETL Pipelines |

---

## 🚀 Project Overview
The core objective is to process **Payroll-Based Journal (PBJ)** staffing hours and align them with facility records. It features a hybrid approach of using **Python** for complex data cleaning and **Relational Queries** for structured analytical reporting.

### Key Capabilities Demonstrated:
- **Automated Data Retrieval:** Programmatic ingestion of source CSVs from remote cloud storage.
- **Advanced ETL Workflows:** Standardizing unique identifiers (CMS IDs) with zero-padding to ensure 100% join accuracy.
- **Data Unpivoting:** Transforming wide-format staffing records into long-format time-series data.
- **Admin Contact Discovery:** A fuzzy-matching logic to pair high-volume facilities with their respective administrative leadership for outreach.

---

## 💡 Method 1: Google Colab (Lightweight Analytics)
The Colab version is built for accessibility and speed.
* **Run in the Cloud:** No local installation of Python or SQL is required.
* **SQLite Orchestration:** Uses an in-memory SQL engine to perform relational joins across datasets without needing a dedicated server.
* **Interactive Documentation:** Weaves together code, SQL results, and data dictionaries into a single "live" document.

## 🧱 Method 2: Databricks (Enterprise Orchestration)
The Databricks version is built for scalability and data integrity.
* **Spark SQL Optimization:** Replaces SQLite with Spark SQL Global Views, allowing for massive parallel processing of millions of records.
* **Schema Enforcement:** Uses Spark DataFrames to ensure data types (Integers, Doubles, Strings) remain consistent across the pipeline.
* **Production Ready:** Designed to be converted into a "Databricks Job" for automated weekly reporting.

---

## 📊 The Data Stack
The project utilizes several key datasets related to healthcare operations:
* **Facilities Table:** Metadata for skilled nursing facilities (ratings, bed counts, locations).
* **PBJ Hours:** Staffing activity for RN, LPN, and CNA roles.
* **Admin Details:** Licensure data including administrator names and contact emails.

---

## 🛠️ Installation & Usage

### For Colab:
1. Upload the `.ipynb` file to Google Colab.
2. Ensure you have the required Google Drive file IDs.
3. Select `Runtime > Run all`.

### For Databricks:
1. Import the notebook into your Databricks Workspace.
2. Attach the notebook to a running Cluster (Runtime 10.4 LTS or higher recommended).
3. Ensure your **Git Integration** is configured to sync with this repository.
4. Run the cells sequentially to initialize the Spark Views.

---

## 🧪 Analytical Examples Included

### 1. Chain-Level Staffing Analysis
Aggregates statewide staffing hours to identify the top 10 healthcare chains by volume.
- **Calculates:** Total facilities, active reporting units, and percentage of statewide market share.

### 2. Admin Contact Matching
A targeted script to identify the top 100 facilities by labor volume and retrieve their administrator's contact details for business development.

---
*Note: This project was developed as a technical assessment to demonstrate proficiency in Decision Intelligence and Data Engineering across both research and enterprise environments.*
