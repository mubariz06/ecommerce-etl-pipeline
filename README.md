End-to-End E-Commerce ETL Pipeline

A production-style end-to-end ETL pipeline for e-commerce order data using Apache Airflow, PySpark, PostgreSQL, Docker, and Python.

The pipeline extracts raw order data, transforms and validates it with PySpark, applies data-quality checks, and loads the cleaned data into a PostgreSQL analytics warehouse.

The project is designed to demonstrate production-oriented ETL practices such as idempotent processing, data-quality gates, modular design, orchestration, and containerized deployment.

Architecture
                    ┌─────────────────────┐
                    │   Raw Order CSV     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Extract        │
                    │  Landing Zone       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   PySpark Transform │
                    │                     │
                    │ • Schema validation │
                    │ • Null handling     │
                    │ • Deduplication     │
                    │ • Revenue derivation│
                    │ • Order filtering   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Parquet Output    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Data Quality Gate  │
                    │                     │
                    │ • Row count         │
                    │ • Null checks       │
                    │ • Revenue checks    │
                    └──────────┬──────────┘
                               │
                         PASS  │
                               ▼
                    ┌─────────────────────┐
                    │      PostgreSQL     │
                    │     fact_orders     │
                    └─────────────────────┘

              Apache Airflow orchestrates the pipeline

Pipeline Flow

The Airflow DAG orchestrates the following stages:

Extract
Copies the raw CSV into a date-partitioned landing zone.

Transform
PySpark reads the landing data, applies the expected schema, handles null values, removes duplicate order_id records, derives revenue and date attributes, and filters invalid order statuses.

Data Quality
Validates the transformed Parquet data before loading it into the warehouse.

Load
Loads validated records into the PostgreSQL fact_orders table.

Idempotency
Re-running the same batch does not create duplicate warehouse records.

Key Features

Apache Airflow orchestration

PySpark distributed data processing

PostgreSQL analytics warehouse

Dockerized development environment

Data-quality validation before loading

Idempotent batch loading

Date-partitioned landing data

CSV → Parquet transformation

Modular Extract / Transform / Quality / Load architecture

Unit tests for transformation logic

Environment-based configuration

Designed with future Snowflake migration in mind

Technologies
Component	Technology
Orchestration	Apache Airflow 2.9
Execution	Airflow LocalExecutor
Data Processing	PySpark 3.5
Programming Language	Python 3.10
Data Warehouse	PostgreSQL 15
Containerization	Docker / Docker Compose
Source Format	CSV
Processed Format	Parquet
Testing	Pytest
Repository Structure
ecommerce-etl-pipeline/
│
├── dags/
│   └── ecommerce_etl_dag.py
│
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── data_quality.py
│   └── load.py
│
├── data/
│   └── raw/
│       └── orders_sample.csv
│
├── sql/
│   └── init_warehouse.sql
│
├── tests/
│   └── test_transform.py
│
├── diagrams/
│   ├── architecture.drawio
│   └── architecture.svg
│
├── screenshots/
│   └── ...
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md

Prerequisites

Install the following before running the project:

Docker

Docker Compose

Git

You do not need to install Airflow, PySpark, or PostgreSQL directly when using the Docker setup.

Run Locally
1. Clone the repository
git clone https://github.com/mubariz06/ecommerce-etl-pipeline.git
cd ecommerce-etl-pipeline

2. Create the environment file

Copy the example environment configuration:

cp .env.example .env


Open .env and configure the required database and Airflow credentials.

Important: Never commit .env to Git. Keep credentials and passwords local.

3. Build and start the services
docker compose up --build


The first startup may take several minutes because the Airflow image and Python/Java dependencies need to be built.

4. Open Airflow

Open:

http://localhost:8080


Log in using the Airflow credentials configured in .env.

5. Run the ETL pipeline

In the Airflow UI:

Find the ecommerce_etl_pipeline DAG.

Enable the DAG if necessary.

Trigger it manually.

Open the DAG run and inspect each task.

The expected flow is:

extract
   ↓
transform
   ↓
data_quality
   ↓
load


The load step should only run after the data-quality checks pass.

6. Verify the PostgreSQL warehouse

You can query the fact_orders table using the PostgreSQL container:

docker compose exec warehouse-db \
  psql -U <WAREHOUSE_DB_USER> \
  -d <WAREHOUSE_DB_NAME> \
  -c "SELECT * FROM fact_orders LIMIT 10;"


Replace the placeholders with the values configured in .env.

7. Stop the environment

To stop the containers:

docker compose down


To stop the containers and remove their volumes:

docker compose down -v


Use docker compose down -v only when you intentionally want to remove the local PostgreSQL data.

Running the Transformation Without Airflow

The transformation code can also be tested independently.

Install the Python dependencies:

pip install -r requirements.txt


Run extraction:

python scripts/extract.py


Run the PySpark transformation:

python scripts/transform.py \
  data/landing/dt=2024-01-19/orders_raw.csv \
  2024-01-19


This is useful for debugging the transformation layer without running the complete Airflow environment.

Data Quality Checks

The data-quality gate is implemented in:

scripts/data_quality.py


The pipeline validates the transformed data before loading it into PostgreSQL.

Current checks include:

Minimum row-count validation

Null checks for required fields

Validation of order_id

Validation of order_date

Validation of revenue

Negative-revenue detection

If a quality check fails, the pipeline blocks the load step rather than inserting invalid data into the warehouse.

Idempotent Loading

The load process is designed to be idempotent.

For a given batch, existing records for the affected order_id values are removed before the cleaned records are inserted.

Conceptually:

Batch 1
  ↓
Delete existing order_ids
  ↓
Insert cleaned records


Running the same batch again therefore does not continuously create duplicate records.

Testing

Run the unit tests with:

pytest


The tests focus on transformation behaviour and help verify that changes to the ETL logic do not introduce regressions.

Future Snowflake Migration

The warehouse layer is isolated in scripts/load.py, allowing the processing and extraction layers to remain unchanged if the warehouse is migrated.

A future Snowflake implementation could use:

snowflake-connector-python

snowflake-sqlalchemy

Snowflake MERGE statements for idempotent upserts

The overall architecture would remain:

Airflow
   ↓
Extract
   ↓
PySpark
   ↓
Data Quality
   ↓
Snowflake

Production Considerations

This project is designed as a portfolio and learning implementation of production-style ETL patterns.

For a larger production deployment, additional capabilities could include:

Cloud object storage

Secret management

CI/CD

Automated integration tests

Data lineage

Monitoring and alerting

Retry and backoff policies

Centralized logging

Schema evolution handling

Incremental processing from a production source system

Snowflake or another cloud data warehouse

Screenshots

Pipeline execution screenshots and architecture diagrams are available in:

screenshots/
diagrams/

Author

Mohammed Mubariz

Big Data Engineer

GitHub: https://github.com/mubariz06

Email: gulammohammedmubarizuddin@gmail.com
