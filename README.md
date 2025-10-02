# Data Engineering Case Study: Lending Data Warehouse  

## Objective  
As a candidate for the **(Sr.) Staff Data Engineer** position, you are tasked with designing a data modeling solution to transform a lending concept from a transactional database into a **data warehouse optimized for analytics and reporting**.  

The exercise evaluates your ability to:  
- Architect scalable data models  
- Implement transformations using **Snowflake** and **dbt**  
- Ensure data quality and governance  

Your solution should demonstrate **technical expertise, strategic thinking, and alignment with enterprise best practices**.  

---

## Background  
You work for a **fintech company** that provides personal loans to customers.  

- The **transactional database** captures loan applications, approvals, disbursements, and repayments in real-time.  
- The business requires a **data warehouse** to support analytics and reporting, such as:  
  - Loan performance  
  - Customer segmentation  
  - Risk analysis  

Your task: **Model the lending concept in a data warehouse using dbt for transformations** to enable efficient querying and reporting.  

---

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

---

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

---

### 2. dbt Transformations  
- Outline a **dbt project structure** with models, tests, and documentation.  
- Provide **SQL code** for at least **two dbt models**:  
  - One **fact table**  
  - One **dimension table**  
- Explain how your transformations ensure **scalability** and **modularity**.  

---

### 3. Data Pipeline Considerations  
- Describe how data will be **ingested** from the transactional database into **Snowflake**.  

---

## Deliverables  
- **Data warehouse schema design**  
- **dbt model SQL code** (one fact table, one dimension table, including tests)  
- **Explanation** of:  
  - Your design choices  
  - dbt project structure  
  - Pipeline approach  
