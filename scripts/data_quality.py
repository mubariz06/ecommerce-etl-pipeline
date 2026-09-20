"""
data_quality.py
----------------
Lightweight data-quality gate run after the load step.

Checks:
  - Row count is above a minimum threshold
  - No nulls in critical columns
  - No negative revenue values

Raises an AssertionError (which fails the Airflow task) if any
check fails, so bad data never silently reaches downstream consumers.
"""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

MIN_EXPECTED_ROWS = int(os.environ.get("MIN_EXPECTED_ROWS", 1))


def run_quality_checks(processed_path: str) -> bool:
    df = pd.read_parquet(processed_path)

    row_count = len(df)
    logger.info("Row count for quality check: %s", row_count)
    assert row_count >= MIN_EXPECTED_ROWS, (
        f"Row count {row_count} is below minimum threshold {MIN_EXPECTED_ROWS}"
    )

    critical_cols = ["order_id", "order_date", "revenue"]
    for col in critical_cols:
        null_count = df[col].isnull().sum()
        assert null_count == 0, f"Found {null_count} nulls in critical column '{col}'"

    negative_revenue = (df["revenue"] < 0).sum()
    assert negative_revenue == 0, f"Found {negative_revenue} rows with negative revenue"

    logger.info("All data quality checks passed.")
    return True


if __name__ == "__main__":
    import sys
    run_quality_checks(sys.argv[1])
