# 🏥 Healthcare Staffing & Analytics Orchestration Engine

**An automated data engineering solution that triggered and scheduled via GitHub Actions.**

## 🎯 Project Purpose: Architectural Comparison
This project demonstrates three distinct methods for handling healthcare data ETL and analytics, showcasing how workflows transition from local prototyping to enterprise-scale environments.

| Feature | **Method 1: Google Colab** | **Method 2: Databricks (Local)** | **Method 3: Databricks (Spark)** |
| :--- | :--- | :--- | :--- |
| **Engine** | Python + In-Memory SQLite | **Local Python (Pandas)** | Apache Spark (PySpark) |
| **Execution** | Cloud VM (Single Node) | **Databricks Driver Node** | Distributed Cluster |
| **Data Handling** | In-memory SQL | **Vectorized Pandas DataFrames** | Spark Partitioned RDDs |
| **Scaling** | Small Datasets (<500MB) | **Medium Datasets (<2GB)** | Big Data (Multi-GB/TB) |

---

## 🚀 Project Overview
The core objective is to process **Payroll-Based Journal (PBJ)** staffing hours and align them with facility records. It features a hybrid approach of using **Python** for complex data cleaning and **Relational Logic** for structured analytical reporting.

### Key Capabilities Demonstrated:
- **Automated Data Retrieval:** Programmatic ingestion of source CSVs from remote cloud storage using `requests` and `io`.
- **Robust ETL Pipelines:** Dynamic column detection and regex-based header cleaning to prevent pipeline breaks.
- **Data Standardization:** Automated zero-padding of CMS IDs and fuzzy-matching logic for administrator contact discovery.
- **Multimodal Orchestration:** Identical business logic implemented across SQLite, Pandas, and PySpark.

---

## 💡 Method 1: Google Colab (Lightweight Analytics)
Ideal for rapid prototyping and interactive documentation.
* **Zero Setup:** Runs in any browser with no local installation required.
* **SQLite Orchestration:** Uses an in-memory SQL engine to perform relational joins within a single "live" document.

## 🐍 Method 2: Databricks Local Python (Optimized Performance)
Newly added **Pandas-based** implementation for Databricks.
* **Low Overhead:** Executes entirely on the cluster's Driver node, bypassing Spark's distribution overhead for faster performance on medium-sized datasets.
* **Dynamic Cleaning:** Features advanced regex cleaning to handle inconsistent government data headers automatically.
* **Decision Intelligence:** Optimized for rapid iteration and high-speed data manipulation.

## 🧱 Method 3: Databricks Spark (Enterprise Scaling)
The enterprise-grade version built for massive datasets and production reliability.
* **Spark SQL Optimization:** Uses Global Temp Views for parallelized analytical queries.
* **Production Ready:** Designed for scheduled Databricks Jobs and integration with Delta Lake architectures.

---

## 📊 The Data Stack
The project utilizes several key datasets related to healthcare operations:
* **Facilities Table:** Metadata for skilled nursing facilities (ratings, bed counts, locations).
* **PBJ Hours:** Staffing activity for RN, LPN, and CNA roles.
* **Admin Details:** Licensure data including administrator names and contact emails.

---

## 🛠️ Installation & Usage

### For Databricks (Local Python or Spark):
1. Import the `.ipynb` files into your Databricks Workspace.
2. Attach the notebook to a running Cluster (Runtime 10.4 LTS or higher).
3. **For Local Python:** Run the cell labeled `FINAL STABLE LOCAL PANDAS PORT`.
4. **For Spark:** Run the cell labeled `CELL 1: Setup and Data Ingestion`.

---

## 🧪 Analytical Examples Included

### 1. Chain-Level Staffing Analysis
Aggregates statewide staffing hours to identify the top 10 healthcare chains by volume and market share.

### 2. Admin Contact Matching
A targeted algorithm to identify the top 100 facilities by labor volume and retrieve their administrator's contact details for business development outreach.

---
*Note: This project was developed as a technical assessment to demonstrate proficiency in Decision Intelligence and Data Engineering across research, mid-market, and enterprise environments.*
