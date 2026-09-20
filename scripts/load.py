"""
load.py
--------
Load layer of the ETL pipeline.

Reads the cleaned Parquet output produced by transform.py and loads
it into the analytics warehouse (PostgreSQL in this demo — the same
pattern applies to Snowflake via SnowSQL/COPY INTO, or to Delta Lake
tables on Databricks).
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

WAREHOUSE_TABLE = os.environ.get("WAREHOUSE_TABLE", "fact_orders")


def get_warehouse_engine():
    user = os.environ["WAREHOUSE_DB_USER"]
    password = os.environ["WAREHOUSE_DB_PASSWORD"]
    host = os.environ["WAREHOUSE_DB_HOST"]
    port = os.environ.get("WAREHOUSE_DB_PORT", "5432")
    db = os.environ["WAREHOUSE_DB_NAME"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def load_orders(processed_path: str) -> int:
    """
    Loads all Parquet files under processed_path into the warehouse
    fact table using an append-only, idempotent-per-partition write.

    Args:
        processed_path: directory containing partitioned Parquet output.

    Returns:
        int: number of rows loaded.
    """
    df = pd.read_parquet(processed_path)

    engine = get_warehouse_engine()
    with engine.begin() as conn:
        # Idempotency: remove any existing rows for this batch of
        # order_ids before inserting, so re-runs don't duplicate data.
        order_ids = tuple(df["order_id"].tolist())
        if order_ids:
            conn.exec_driver_sql(
                f"DELETE FROM {WAREHOUSE_TABLE} WHERE order_id IN %(ids)s",
                {"ids": order_ids},
            )
        df.to_sql(WAREHOUSE_TABLE, conn, if_exists="append", index=False)

    logger.info("Loaded %s rows into %s", len(df), WAREHOUSE_TABLE)
    return len(df)


if __name__ == "__main__":
    import sys
    load_orders(sys.argv[1])
