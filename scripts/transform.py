"""
transform.py
-------------
Transformation layer of the ETL pipeline, built on PySpark.

Handles:
  - Schema enforcement
  - Null / missing-value handling
  - De-duplication
  - Derived columns (revenue, order date parts)
  - Filtering out invalid / cancelled records for the analytics table
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

logger = logging.getLogger(__name__)

PROCESSED_DATA_PATH = os.environ.get("PROCESSED_DATA_PATH", "/opt/airflow/data/processed")

ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), False),
    StructField("product_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
])


def get_spark_session(app_name: str = "ecommerce-etl-transform") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )


def transform_orders(input_path: str, execution_date: str) -> str:
    """
    Reads raw extracted CSV, cleans and enriches it, and writes the
    result to Parquet in the processed zone.

    Args:
        input_path: path to the raw CSV produced by extract.py
        execution_date: date partition key (YYYY-MM-DD)

    Returns:
        str: path to the processed Parquet output.
    """
    spark = get_spark_session()

    df = (
        spark.read
        .option("header", True)
        .schema(ORDER_SCHEMA)
        .csv(input_path)
    )

    logger.info("Raw record count: %s", df.count())

    cleaned = (
        df
        # Drop rows missing the natural key
        .filter(F.col("order_id").isNotNull())
        # Fill missing customer_id with a placeholder for guest checkouts
        .withColumn("customer_id", F.coalesce(F.col("customer_id"), F.lit("GUEST")))
        # Drop rows where price is missing — cannot compute revenue reliably
        .filter(F.col("unit_price").isNotNull())
        .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(1)))
        # De-duplicate on order_id, keeping the first occurrence
        .dropDuplicates(["order_id"])
        # Derived columns
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
        # Exclude cancelled/returned orders from the revenue-facing table
        .filter(F.col("status") == "Completed")
    )

    logger.info("Cleaned record count: %s", cleaned.count())

    output_path = os.path.join(PROCESSED_DATA_PATH, f"dt={execution_date}")
    (
        cleaned.write
        .mode("overwrite")
        .parquet(output_path)
    )

    logger.info("Wrote transformed data to %s", output_path)
    # Note: we intentionally do not call spark.stop() here. Each Airflow
    # task runs in its own subprocess (LocalExecutor), so the JVM is torn
    # down when the process exits. Explicitly stopping the session is
    # also unsafe when a SparkSession is shared/reused, e.g. in tests.
    return output_path


if __name__ == "__main__":
    import sys
    transform_orders(sys.argv[1], sys.argv[2])
