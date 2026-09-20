"""
test_transform.py
-------------------
Unit tests for the PySpark transform logic. Run with:

    pytest tests/test_transform.py

Uses a local SparkSession — no Airflow or Docker required.
"""

import os
import sys
import tempfile

import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))
from transform import transform_orders  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-transform").master("local[1]").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def sample_csv(tmp_path):
    content = (
        "order_id,customer_id,order_date,product_category,product_name,"
        "quantity,unit_price,country,status\n"
        "1,C1,2024-01-01,Electronics,Mouse,2,10.0,India,Completed\n"
        "2,,2024-01-01,Books,Novel,1,15.0,USA,Completed\n"          # missing customer_id
        "3,C3,2024-01-02,Clothing,Shirt,1,,India,Completed\n"       # missing price -> dropped
        "4,C4,2024-01-02,Electronics,Mouse,1,10.0,India,Cancelled\n"  # cancelled -> dropped
        "1,C1,2024-01-01,Electronics,Mouse,2,10.0,India,Completed\n"  # duplicate order_id
    )
    file_path = tmp_path / "orders_raw.csv"
    file_path.write_text(content)
    return str(file_path)


def test_transform_orders_cleans_and_enriches(spark, sample_csv, tmp_path, monkeypatch):
    output_dir = tmp_path / "processed"
    monkeypatch.setenv("PROCESSED_DATA_PATH", str(output_dir))

    import transform as transform_module
    monkeypatch.setattr(transform_module, "PROCESSED_DATA_PATH", str(output_dir))

    output_path = transform_orders(sample_csv, "2024-01-01")
    result_df = spark.read.parquet(output_path).toPandas()

    # Only 2 rows survive: order 1 (deduped) and order 2 (guest checkout)
    assert len(result_df) == 2
    assert set(result_df["order_id"]) == {1, 2}

    # Guest checkout fill
    guest_row = result_df[result_df["order_id"] == 2].iloc[0]
    assert guest_row["customer_id"] == "GUEST"

    # Revenue computed correctly
    order_1 = result_df[result_df["order_id"] == 1].iloc[0]
    assert order_1["revenue"] == 20.0

    # No cancelled orders present
    assert 4 not in set(result_df["order_id"])
