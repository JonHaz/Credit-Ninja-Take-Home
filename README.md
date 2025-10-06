# Data Engineering Case Study: Lending Data Warehouse  

## 🧭 Project Overview

This dbt project models data from a transactional lending system into a dimensional warehouse optimized for analytics and reporting.  
The goal is to enable robust insights into **loan performance**, **repayment behavior**, and **customer segmentation** using clean, governed data models in Snowflake.

## 🧱 Design Choices

### **1. Layered Medallion Architecture**

The project follows a **Bronze → Silver → Gold** pattern for clarity and scalability:

| Layer | Schema | Purpose |
|--------|---------|----------|
| **Bronze** | `credit_ninja.snowflake` (sources) | Raw transactional tables ingested directly from the operational database. |
| **Silver** | `staging` models | Cleans, standardizes, and type-casts raw data to create trusted intermediate datasets. |
| **Gold** | `marts` models | Implements a **dimensional model** for analytics using star-schema principles. |

This layered design supports modularity, reproducibility, and easier debugging while aligning with industry best practices for data lakehouse and warehouse modeling.

### **2. Dimensional Modeling**

The Gold layer adopts a **Star Schema**, centering on measurable business processes (facts) and descriptive attributes (dimensions):

| Table | Type | Description |
|--------|------|-------------|
| **fct_loans** | Fact | One record per approved loan, including disbursement dates, term, and status. |
| **fct_payments** | Fact | One record per payment, capturing amount, type, and late-payment indicators. |
| **dim_customers** | Dimension | Master reference of customer attributes for segmentation and joins. |
| **dim_dates** | Dimension | Calendar lookup to support time-series analysis and aggregations. |

This schema enables flexible queries such as:
- **Loan performance:** total disbursed, default rates, repayment trends  
- **Customer segmentation:** loan size, repayment behavior, regional patterns  
- **Time-based analytics:** loans and payments by month, quarter, or year

### **3. Governance and Data Quality**

- **Data Tests:**  
  Each model includes `unique`, `not_null`, and `relationships` tests to enforce referential integrity and ensure data accuracy.  
- **Incremental Models:**  
  `fct_loans`, `fct_payments`, and `dim_customers` use `materialized='incremental'` with `incremental_strategy='merge'`, ensuring only new or updated records (based on `record_loaded_at`) are processed.  
- **Documentation:**  
  YAML metadata is used to generate dbt Docs, providing model lineage and column-level descriptions for transparency.  
- **Audit Columns:**  
  All models track `record_loaded_at` and `record_updated_at` timestamps for data lineage, traceability, and compliance.  

## 🧩 dbt Project Structure
```
models/
├── sources/
│   └── snowflake.yml             # Defines raw source tables in Snowflake
├── staging/
│   ├── stg_customers.sql
│   ├── stg_loans.sql
│   ├── stg_loan_applications.sql
│   └── stg_payments.sql
└── marts/
├── dim_customers.sql
├── dim_dates.sql
├── fct_loans.sql
├── fct_payments.sql
└── schema.yml
```

## ⚙️ Pipeline Approach

### **1. Ingestion**

Instead of relying on an external ELT tool, this project uses a custom **Python script — `data_generator.py` — executed in Databricks** to simulate raw transactional data.  
The script programmatically generates realistic records for the following tables:  
- `customers`  
- `loan_applications`  
- `loans`  
- `payments`  

Each table is written directly into the **Bronze layer** of the Databricks Lakehouse (or Snowflake-compatible schema) using Delta tables.  
This approach enables rapid prototyping, reproducibility, and consistent data volumes for testing the dbt transformation pipeline.

Example workflow inside Databricks:
1. Run `data_generator.py` to create or refresh synthetic source data.  
2. Verify that generated tables exist in the Bronze schema (`hazeley_consulting.credit_ninja`).  
3. Trigger dbt Cloud or local runs (`dbt build`) to transform this data into the Silver and Gold layers.

### **2. Transformation**

dbt Cloud orchestrates transformations:

1. **`dbt run` / `dbt build`** executes models in dependency order  
2. Staging models clean and cast data  
3. Marts models aggregate and enrich data into facts and dimensions  

**Performance Optimization:**  
Incremental filters based on `record_loaded_at` minimize compute cost by processing only new or updated records.

### **3. Testing and Documentation**

- Automated **dbt tests** validate schema, data integrity, and relationships.  
- **dbt Docs** provide a browsable catalog and lineage graph.  
- Jobs in dbt Cloud are scheduled for **daily incremental runs**, with full-refresh capability for data resets.

