# Data Engineering Case Study: Lending Data Warehouse  

## Objective  
As a candidate for the **(Sr.) Staff Data Engineer** position, you are tasked with designing a data modeling solution to transform a lending concept from a transactional database into a **data warehouse optimized for analytics and reporting**.  

The exercise evaluates your ability to:  
- Architect scalable data models  
- Implement transformations using **Snowflake** and **dbt**  
- Ensure data quality and governance  

Your solution should demonstrate **technical expertise, strategic thinking, and alignment with enterprise best practices**.  

## Background  
You work for a **fintech company** that provides personal loans to customers.  

- The **transactional database** captures loan applications, approvals, disbursements, and repayments in real-time.  
- The business requires a **data warehouse** to support analytics and reporting, such as:  
  - Loan performance  
  - Customer segmentation  
  - Risk analysis  

Your task: **Model the lending concept in a data warehouse using dbt for transformations** to enable efficient querying and reporting.  

## Transactional Database Schema (Simplified)  

### `customers`
- `customer_id` (PK): Unique identifier for a customer  
- `first_name`: Customer’s first name  
- `last_name`: Customer’s last name  
- `email`: Customer’s email address  
- `created_at`: Timestamp when the customer record was created  

### `loan_applications`
- `application_id` (PK): Unique identifier for a loan application  
- `customer_id` (FK): References `customers.customer_id`  
- `application_date`: Date the application was submitted  
- `loan_amount_requested`: Requested loan amount (decimal)  
- `status`: Application status (e.g., “Pending”, “Approved”, “Rejected”)  
- `updated_at`: Timestamp of the last update  

### `loans`
- `loan_id` (PK): Unique identifier for an approved loan  
- `application_id` (FK): References `loan_applications.application_id`  
- `customer_id` (FK): References `customers.customer_id`  
- `loan_amount`: Approved loan amount (decimal)  
- `interest_rate`: Annual interest rate (decimal)  
- `start_date`: Loan disbursement date  
- `end_date`: Loan maturity date  
- `status`: Loan status (e.g., “Active”, “Paid”, “Defaulted”)  

### `payments`
- `payment_id` (PK): Unique identifier for a payment  
- `loan_id` (FK): References `loans.loan_id`  
- `customer_id` (FK): References `customers.customer_id`  
- `payment_amount`: Amount paid (decimal)  
- `payment_date`: Date of payment  
- `payment_type`: Type of payment (e.g., “Scheduled”, “Prepayment”)  

## Requirements  

### 1. Data Warehouse Schema Design  
- Propose a **dimensional model** optimized for analytics.  
- Define **fact** and **dimension** tables, including:  
  - Columns  
  - Data types  
  - Relationships  
- Ensure schema supports queries for:  
  - Loan performance (loans disbursed, repayment rates, default rates)  
  - Customer segmentation (loan amount, repayment behavior)  
  - Time-based analysis (loans disbursed by month, payment trends)  
- Incorporate **data governance** considerations: data quality, auditability.  

### 2. dbt Transformations  
- Outline a **dbt project structure** with models, tests, and documentation.  
- Provide **SQL code** for at least **two dbt models**:  
  - One **fact table**  
  - One **dimension table**  
- Explain how your transformations ensure **scalability** and **modularity**.  

### 3. Data Pipeline Considerations  
- Describe how data will be **ingested** from the transactional database into **Snowflake**.  

## Deliverables  
- **Data warehouse schema design**  
- **dbt model SQL code** (one fact table, one dimension table, including tests)  
- **Explanation** of:  
  - Your design choices  
  - dbt project structure  
  - Pipeline approach

---
# 🧭 Project Overview

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

- **Tests:** Each model defines `unique`, `not_null`, and `relationships` tests to enforce referential integrity and ensure data reliability.  
- **Documentation:** Column-level metadata (YAML) feeds dbt Docs for lineage and discoverability.  
- **Audit Columns:** Models include `record_loaded_at` and `record_updated_at` timestamps to support data lineage and change tracking.  
- **Incremental Models:** Large tables (`fct_loans`, `fct_payments`, `dim_customers`) use dbt’s `incremental` materialization with merge logic for scalable processing.

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

Raw transactional tables (`customers`, `loan_applications`, `loans`, `payments`) are ingested into Snowflake’s **Bronze schema** via an ELT tool (e.g., Fivetran, Airbyte, or Snowflake Streams).

### **2. Transformation**

dbt Cloud orchestrates transformations:

1. **`dbt run` / `dbt build`** executes models in dependency order  
2. Staging models clean and cast data  
3. Marts models aggregate and enrich data into facts and dimensions  

Incremental logic ensures only new or changed records are processed, minimizing compute cost and runtime.
