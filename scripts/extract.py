"""
extract.py
-----------
Extraction layer of the ETL pipeline.

Responsible for pulling raw order data from the source system.
In this demo, the "source" is a CSV file (simulating an upstream
export from an OLTP database or partner API). In production this
function would instead call a REST API, read from a message queue,
or query a source database via JDBC.
"""

import logging
import os
import shutil
from datetime import datetime

logger = logging.getLogger(__name__)

RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", "/opt/airflow/data/raw/orders_sample.csv")
LANDING_ZONE = os.environ.get("LANDING_ZONE", "/opt/airflow/data/landing")


def extract_orders(execution_date: str = None) -> str:
    """
    Copies the raw source file into a date-partitioned landing zone.

    Args:
        execution_date: Airflow execution date (YYYY-MM-DD). Used to
                         partition the landing zone the way a real
                         ingestion job would.

    Returns:
        str: path to the extracted file in the landing zone.
    """
    execution_date = execution_date or datetime.utcnow().strftime("%Y-%m-%d")
    partition_dir = os.path.join(LANDING_ZONE, f"dt={execution_date}")
    os.makedirs(partition_dir, exist_ok=True)

    if not os.path.exists(RAW_DATA_PATH):
        raise FileNotFoundError(f"Source file not found at {RAW_DATA_PATH}")

    dest_path = os.path.join(partition_dir, "orders_raw.csv")
    shutil.copyfile(RAW_DATA_PATH, dest_path)

    logger.info("Extracted source file to %s", dest_path)
    return dest_path


if __name__ == "__main__":
    print(extract_orders())
