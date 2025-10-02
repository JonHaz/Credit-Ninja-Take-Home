!pip install databricks-sql-connector faker

from faker import Faker
import random
from datetime import datetime, timedelta
import pandas as pd

fake = Faker()

# -------------------------
# Parameters
# -------------------------
NUM_CUSTOMERS = 10
NUM_APPLICATIONS = 20
NUM_PAYMENTS = 50

# -------------------------
# Generate Customers
# -------------------------
customers = []
for cid in range(1, NUM_CUSTOMERS + 1):
    customers.append({
        "customer_id": cid,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "created_at": datetime.now()
    })

df_customers = spark.createDataFrame(pd.DataFrame(customers))
df_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("hazeley_consulting.credit_ninja.customers")

# -------------------------
# Generate Loan Applications
# -------------------------
applications = []
app_id = 100
for _ in range(NUM_APPLICATIONS):
    cust_id = random.randint(1, NUM_CUSTOMERS)
    status = random.choice(["Pending", "Approved", "Rejected"])
    applications.append({
        "application_id": app_id,
        "customer_id": cust_id,
        "application_date": fake.date_between(start_date="-2y", end_date="today"),
        "loan_amount_requested": round(random.uniform(1000, 20000), 2),
        "status": status,
        "updated_at": datetime.now()
    })
    app_id += 1

df_applications = spark.createDataFrame(pd.DataFrame(applications))
df_applications.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("hazeley_consulting.credit_ninja.loan_applications")

# -------------------------
# Generate Loans (only for approved applications)
# -------------------------
apps_approved = [a for a in applications if a["status"] == "Approved"]
loans = []
loan_id = 1000
for app in apps_approved:
    start_date = fake.date_between(start_date=app["application_date"], end_date="today")
    end_date = start_date + timedelta(days=365 * random.randint(1, 5))
    loans.append({
        "loan_id": loan_id,
        "application_id": app["application_id"],
        "customer_id": app["customer_id"],
        "loan_amount": app["loan_amount_requested"],
        "interest_rate": round(random.uniform(3.5, 12.0), 2),
        "start_date": start_date,
        "end_date": end_date,
        "status": random.choice(["Active", "Paid", "Defaulted"])
    })
    loan_id += 1

df_loans = spark.createDataFrame(pd.DataFrame(loans))
df_loans.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("hazeley_consulting.credit_ninja.loans")

# -------------------------
# Generate Payments
# -------------------------
payments = []
pay_id = 9000
for _ in range(NUM_PAYMENTS):
    loan = random.choice(loans)
    payments.append({
        "payment_id": pay_id,
        "loan_id": loan["loan_id"],
        "customer_id": loan["customer_id"],
        "payment_amount": round(random.uniform(100, 1000), 2),
        "payment_date": fake.date_between(start_date=loan["start_date"], end_date="today"),
        "payment_type": random.choice(["Scheduled", "Prepayment"])
    })
    pay_id += 1

df_payments = spark.createDataFrame(pd.DataFrame(payments))
df_payments.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("hazeley_consulting.credit_ninja.payments")

print("✅ Dummy data loaded into hazeley_consulting.credit_ninja tables!")