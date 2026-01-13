import pandas as pd
from sqlalchemy import create_engine
import json
from datetime import datetime

DB_URL = "postgresql+psycopg2://etl_user:strong_password@localhost:5432/medallion_db"
engine = create_engine(DB_URL)

def reject_rows(df, condition, table, reason):
    rejected = df[condition].copy()
    if not rejected.empty:
        audit_df = pd.DataFrame({
            "source_table": table,
            "reason": reason,
            "record_data": rejected.apply(lambda r: json.dumps(r.to_dict()), axis=1),
            "rejected_at": datetime.now()
        })
        audit_df.to_sql(
            "rejected_rows",
            engine,
            schema="audit",
            if_exists="append",
            index=False
        )
    return df[~condition]

# ---------------- customers ----------------
customers = pd.read_sql("select * from bronze.customers_raw", engine)

customers = reject_rows(
    customers,
    (customers.age < 18) | (customers.age > 100),
    "customers",
    "invalid_age"
)

customers = customers.drop_duplicates(subset=["customer_id"])

customers.to_sql(
    "customers",
    engine,
    schema="silver",
    if_exists="replace",
    index=False
)

# ---------------- accounts ----------------
accounts = pd.read_sql("select * from bronze.accounts_raw", engine)
valid_customers = set(customers.customer_id)

accounts = reject_rows(
    accounts,
    ~accounts.customer_id.isin(valid_customers),
    "accounts",
    "invalid_customer_id"
)

accounts = accounts.drop_duplicates(subset=["account_id"])

accounts.to_sql(
    "accounts",
    engine,
    schema="silver",
    if_exists="replace",
    index=False
)

# ---------------- transactions ----------------
transactions = pd.read_sql("select * from bronze.transactions_raw", engine)

transactions = reject_rows(
    transactions,
    transactions.amount == 0,
    "transactions",
    "zero_amount"
)

transactions = transactions.drop_duplicates(subset=["txn_id"])

transactions.to_sql(
    "transactions",
    engine,
    schema="silver",
    if_exists="replace",
    index=False
)

# ---------------- loans ----------------
loans = pd.read_sql("select * from bronze.loans_raw", engine)

loans = reject_rows(
    loans,
    (loans.interest_rate < 0) | (loans.interest_rate > 100),
    "loans",
    "invalid_interest_rate"
)

loans = loans.drop_duplicates(subset=["loan_id"])

loans.to_sql(
    "loans",
    engine,
    schema="silver",
    if_exists="replace",
    index=False
)

# ---------------- credit cards ----------------
cards = pd.read_sql("select * from bronze.credit_cards_raw", engine)

# standardize column name from bronze → silver
cards = cards.rename(columns={"limit": "credit_limit"})

cards = reject_rows(
    cards,
    cards.usage_last_month > cards.credit_limit,
    "credit_cards",
    "usage_exceeds_limit"
)

cards = cards.drop_duplicates(subset=["cc_id"])

cards.to_sql(
    "credit_cards",
    engine,
    schema="silver",
    if_exists="replace",
    index=False
)


print("Silver layer built successfully")


