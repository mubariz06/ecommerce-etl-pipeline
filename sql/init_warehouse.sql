-- Fact table for cleaned, revenue-bearing e-commerce orders.
-- Loaded by scripts/load.py after the PySpark transform step.

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id         INTEGER PRIMARY KEY,
    customer_id      VARCHAR(20) NOT NULL,
    order_date       DATE NOT NULL,
    product_category VARCHAR(50),
    product_name     VARCHAR(100),
    quantity         INTEGER NOT NULL,
    unit_price       NUMERIC(10, 2) NOT NULL,
    country          VARCHAR(50),
    status           VARCHAR(20),
    order_year       INTEGER,
    order_month      INTEGER,
    revenue          NUMERIC(10, 2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON fact_orders (order_date);
CREATE INDEX IF NOT EXISTS idx_fact_orders_category ON fact_orders (product_category);
