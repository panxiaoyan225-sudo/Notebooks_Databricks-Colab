# 🏥 Healthcare Staffing & Analytics Pipeline

This repository contains a Google Colab-based data engineering and analytics project focused on **Skilled Nursing Facility (SNF)** staffing data. The project automates the extraction, transformation, and analysis of large-scale healthcare datasets using Python, SQL, and Pandas.

## 🚀 Project Overview

The core objective of this project is to process **Payroll-Based Journal (PBJ)** staffing hours and align them with facility metadata to generate actionable business intelligence. It features a hybrid approach of using **Python** for complex data cleaning and **SQLite** for structured analytical querying.

### Key Capabilities Demonstrated:
- **Automated Data Retrieval:** Uses `gdown` to programmatically pull source CSVs from remote cloud storage.
- **Complex ETL Pipelines:** Transforms "melted" staffing data, standardizes unique identifiers (CMS IDs) with zero-padding, and handles mixed-type data ingestion.
- **SQL Orchestration:** Builds an in-memory SQLite database to perform relational joins across multiple datasets.
- **Matched Contact Discovery:** Implements a Python-based matching algorithm to pair high-volume facilities with their respective administrative contact information.

## 📊 The Data Stack

The project utilizes several key datasets related to healthcare operations:
* **Facilities Table:** Metadata for California/Illinois skilled nursing facilities (ratings, bed counts, locations).
* **PBJ Hours:** Staffing activity for RN, LPN, and CNA roles.
* **Admin Details:** Licensure data including administrator names and contact emails.
* **Shifts & Deals:** Synthetic operational and financial data used for revenue and commission modeling.

## 🛠️ Installation & Usage

1. **Open in Colab:** Upload the `.ipynb` file to your Google Colab environment.
2. **Environment Setup:** The notebook automatically installs necessary dependencies like `gdown`.
3. **Execution:** Click `Runtime > Run all`.
    - The script will download approximately 25MB of source data.
    - It initializes an in-memory SQLite database.
    - It executes the analytical assessment questions.

## 🧪 Analytical Examples Included

### 1. Chain-Level Staffing Analysis (SQL)
Aggregates statewide staffing hours to identify the top 10 healthcare chains by volume. 
- **Calculates:** Total facilities, facilities with active reporting, and percentage of statewide hours.

### 2. Admin Contact Matching (Python/Pandas)
A targeted script to identify the top 100 facilities by labor volume and retrieve their administrator's contact details for business development outreach.

## 📈 Sample Output

The pipeline generates structured insights such as:
- **Market Share:** "PACS GROUP" represents ~14.98% of statewide staffing hours.
- **Data Integrity:** Standardized matching achieved across 78+ high-priority facilities despite naming inconsistencies in raw licensure files.

---
*Note: This project was developed as a technical assessment to demonstrate proficiency in Decision Intelligence and Data Engineering.*
