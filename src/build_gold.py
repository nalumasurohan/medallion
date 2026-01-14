import sys
import os

# Add project root to PYTHONPATH
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from sqlalchemy import create_engine, text
from config.db import DB_URL


def build_gold():
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        sql_path = os.path.join(PROJECT_ROOT, "sql", "gold_ddl.sql")
        with open(sql_path, "r") as f:
            statements = f.read().split(";")

        for stmt in statements:
            if stmt.strip():
                conn.execute(text(stmt))

    print("Gold layer built successfully")


if __name__ == "__main__":
    build_gold()
