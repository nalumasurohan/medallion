import pandas as pd
import hashlib
from sqlalchemy import create_engine
from datetime import datetime
import os

DB_URL = "postgresql+psycopg2://etl_user:strong_password@localhost:5432/medallion_db"

engine = create_engine(DB_URL)

BRONZE_DIR = "bronze_inputs"

TABLE_MAP = {
    "customers.csv": "customers_raw",
    "accounts.csv": "accounts_raw",
    "transactions.csv": "transactions_raw",
    "loans.csv": "loans_raw",
    "credit_cards.csv": "credit_cards_raw"
}

def calculate_checksum(file_path):
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

for file_name, table_name in TABLE_MAP.items():
    file_path = os.path.join(BRONZE_DIR, file_name)
    df = pd.read_csv(file_path)

    row_count = len(df)
    checksum = calculate_checksum(file_path)

    df.to_sql(
        table_name,
        engine,
        schema="bronze",
        if_exists="replace",
        index=False
    )

    audit_df = pd.DataFrame([{
        "table_name": f"bronze.{table_name}",
        "file_name": file_name,
        "row_count": row_count,
        "checksum": checksum,
        "status": "success",
        "load_timestamp": datetime.now()
    }])

    audit_df.to_sql(
        "bronze_load_log",
        engine,
        schema="audit",
        if_exists="append",
        index=False
    )

    print(f"Loaded {file_name} → bronze.{table_name} ({row_count} rows)")
