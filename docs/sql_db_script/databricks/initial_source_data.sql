-- Project 1 baseline source data for Databricks Free Edition.
-- Run this entire script in the Databricks SQL Editor.
--
-- WARNING: CREATE OR REPLACE resets these five source tables and their data.
-- The script does not modify any dbt-owned staging, parking, or mart tables.

CREATE SCHEMA IF NOT EXISTS workspace.dim;

CREATE OR REPLACE TABLE workspace.dim.dim_region_source (
    region_id BIGINT NOT NULL,
    region_code STRING NOT NULL,
    region_name STRING NOT NULL,
    country_name STRING NOT NULL,
    source_updated_at TIMESTAMP NOT NULL
)
USING DELTA;

INSERT INTO workspace.dim.dim_region_source (
    region_id,
    region_code,
    region_name,
    country_name,
    source_updated_at
)
VALUES
    (10, 'CA-EAST', 'Canada East', 'Canada', TIMESTAMP '2026-07-01 08:00:00'),
    (20, 'CA-WEST', 'Canada West', 'Canada', TIMESTAMP '2026-07-01 08:01:00');

CREATE OR REPLACE TABLE workspace.dim.dim_store_source (
    store_id BIGINT NOT NULL,
    store_code STRING NOT NULL,
    store_name STRING NOT NULL,
    region_id BIGINT NOT NULL,
    is_active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP NOT NULL
)
USING DELTA;

INSERT INTO workspace.dim.dim_store_source (
    store_id,
    store_code,
    store_name,
    region_id,
    is_active,
    source_updated_at
)
VALUES
    (101, 'STORE-101', 'Toronto Test Store', 10, true, TIMESTAMP '2026-07-01 08:10:00'),
    (102, 'STORE-102', 'Vancouver Test Store', 20, true, TIMESTAMP '2026-07-01 08:11:00');

CREATE OR REPLACE TABLE workspace.dim.dim_customer_source (
    customer_id BIGINT NOT NULL,
    customer_code STRING NOT NULL,
    first_name STRING NOT NULL,
    last_name STRING NOT NULL,
    email STRING NOT NULL,
    region_id BIGINT NOT NULL,
    is_active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP NOT NULL
)
USING DELTA;

INSERT INTO workspace.dim.dim_customer_source (
    customer_id,
    customer_code,
    first_name,
    last_name,
    email,
    region_id,
    is_active,
    source_updated_at
)
VALUES
    (1001, 'CUST-1001', 'Alex', 'Morgan', 'alex@example.test', 10, true, TIMESTAMP '2026-07-01 08:20:00'),
    (1002, 'CUST-1002', 'Blair', 'Singh', 'blair@example.test', 20, true, TIMESTAMP '2026-07-01 08:21:00'),
    (1003, 'CUST-1003', 'Casey', 'Chen', 'casey@example.test', 10, false, TIMESTAMP '2026-07-01 08:22:00');

-- Customer 1999 is intentionally absent. Sale 5003 should be parked on the
-- first dbt run and resolved after customer 1999 is added in a later snapshot.

CREATE OR REPLACE TABLE workspace.dim.dim_product_source (
    product_id BIGINT NOT NULL,
    product_code STRING NOT NULL,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    is_active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP NOT NULL
)
USING DELTA;

INSERT INTO workspace.dim.dim_product_source (
    product_id,
    product_code,
    product_name,
    category,
    unit_price,
    is_active,
    source_updated_at
)
VALUES
    (2001, 'PROD-2001', 'Training Ball', 'Equipment', 25.00, true, TIMESTAMP '2026-07-01 08:30:00'),
    (2002, 'PROD-2002', 'Practice Jersey', 'Apparel', 80.00, true, TIMESTAMP '2026-07-01 08:31:00');

-- Product 2999 is intentionally absent. Sale 5004 should remain in customer
-- sales but be excluded from product sales by the product inner join.

CREATE OR REPLACE TABLE workspace.dim.sales_source (
    sales_id BIGINT NOT NULL,
    order_number STRING NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    store_id BIGINT NOT NULL,
    sales_date DATE NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_amount DECIMAL(10, 2) NOT NULL,
    source_updated_at TIMESTAMP NOT NULL
)
USING DELTA;

INSERT INTO workspace.dim.sales_source (
    sales_id,
    order_number,
    customer_id,
    product_id,
    store_id,
    sales_date,
    quantity,
    unit_price,
    discount_amount,
    source_updated_at
)
VALUES
    (5001, 'ORD-5001', 1001, 2001, 101, DATE '2026-07-01', 2, 25.00, 5.00, TIMESTAMP '2026-07-01 09:00:00'),
    (5002, 'ORD-5002', 1002, 2002, 102, DATE '2026-07-02', 1, 80.00, 0.00, TIMESTAMP '2026-07-02 09:00:00'),
    (5003, 'ORD-5003', 1999, 2001, 101, DATE '2026-07-03', 3, 25.00, 0.00, TIMESTAMP '2026-07-03 09:00:00'),
    (5004, 'ORD-5004', 1001, 2999, 102, DATE '2026-07-04', 1, 40.00, 0.00, TIMESTAMP '2026-07-04 09:00:00'),
    (5005, 'ORD-5005', 1003, 2002, 101, DATE '2026-07-05', 2, 80.00, 10.00, TIMESTAMP '2026-07-05 09:00:00'),
    (5006, 'ORD-5006', 1002, 2001, 102, DATE '2026-07-06', 4, 25.00, 25.00, TIMESTAMP '2026-07-06 09:00:00');

-- Verification: expected row counts are 2, 2, 3, 2, and 6 respectively.
SELECT 'dim_region_source' AS table_name, count(*) AS row_count
FROM workspace.dim.dim_region_source
UNION ALL
SELECT 'dim_store_source', count(*)
FROM workspace.dim.dim_store_source
UNION ALL
SELECT 'dim_customer_source', count(*)
FROM workspace.dim.dim_customer_source
UNION ALL
SELECT 'dim_product_source', count(*)
FROM workspace.dim.dim_product_source
UNION ALL
SELECT 'sales_source', count(*)
FROM workspace.dim.sales_source
ORDER BY table_name;

-- Verification: calculated amounts should be 45, 80, 75, 40, 150, and 75.
SELECT
    sales_id,
    order_number,
    CAST((quantity * unit_price) - discount_amount AS DECIMAL(12, 2)) AS expected_sales_amount
FROM workspace.dim.sales_source
ORDER BY sales_id;
