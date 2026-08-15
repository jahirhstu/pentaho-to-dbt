-- Project 1 second source snapshot: recovery and incremental update.
--
-- Prerequisite: run initial_source_data.sql, followed by the initial dbt build,
-- before running this script. The initial build creates the unresolved parking
-- record for sale 5003 that this snapshot is intended to resolve.
--
-- This script is rerunnable. MERGE prevents duplicate customer, product, and
-- sales rows.

-- Recover the customer that was intentionally absent from the initial snapshot.
MERGE INTO workspace.dim.dim_customer_source AS target
USING (
    SELECT
        CAST(1999 AS BIGINT) AS customer_id,
        'CUST-1999' AS customer_code,
        'Drew' AS first_name,
        'Recovery' AS last_name,
        'drew@example.test' AS email,
        CAST(10 AS BIGINT) AS region_id,
        true AS is_active,
        TIMESTAMP '2026-07-07 08:00:00' AS source_updated_at
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    target.customer_code = source.customer_code,
    target.first_name = source.first_name,
    target.last_name = source.last_name,
    target.email = source.email,
    target.region_id = source.region_id,
    target.is_active = source.is_active,
    target.source_updated_at = source.source_updated_at
WHEN NOT MATCHED THEN INSERT (
    customer_id,
    customer_code,
    first_name,
    last_name,
    email,
    region_id,
    is_active,
    source_updated_at
)
VALUES (
    source.customer_id,
    source.customer_code,
    source.first_name,
    source.last_name,
    source.email,
    source.region_id,
    source.is_active,
    source.source_updated_at
);

-- Recover the product that was intentionally absent from the initial snapshot.
-- This allows existing sale 5004 to enter stg_product_sales on the next dbt run.
MERGE INTO workspace.dim.dim_product_source AS target
USING (
    SELECT
        CAST(2999 AS BIGINT) AS product_id,
        'PROD-2999' AS product_code,
        'Recovered Training Product' AS product_name,
        'Equipment' AS category,
        CAST(40.00 AS DECIMAL(10, 2)) AS unit_price,
        true AS is_active,
        TIMESTAMP '2026-07-07 08:30:00' AS source_updated_at
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    target.product_code = source.product_code,
    target.product_name = source.product_name,
    target.category = source.category,
    target.unit_price = source.unit_price,
    target.is_active = source.is_active,
    target.source_updated_at = source.source_updated_at
WHEN NOT MATCHED THEN INSERT (
    product_id,
    product_code,
    product_name,
    category,
    unit_price,
    is_active,
    source_updated_at
)
VALUES (
    source.product_id,
    source.product_code,
    source.product_name,
    source.category,
    source.unit_price,
    source.is_active,
    source.source_updated_at
);

-- Update sale 5001 and insert sale 5007 in one idempotent operation.
-- Expected amounts after dbt calculation:
--   sale 5001: (3 * 25.00) - 10.00 = 65.00
--   sale 5007: (3 * 80.00) - 20.00 = 220.00
MERGE INTO workspace.dim.sales_source AS target
USING (
    SELECT
        CAST(5001 AS BIGINT) AS sales_id,
        'ORD-5001' AS order_number,
        CAST(1001 AS BIGINT) AS customer_id,
        CAST(2001 AS BIGINT) AS product_id,
        CAST(101 AS BIGINT) AS store_id,
        DATE '2026-07-01' AS sales_date,
        CAST(3 AS INT) AS quantity,
        CAST(25.00 AS DECIMAL(10, 2)) AS unit_price,
        CAST(10.00 AS DECIMAL(10, 2)) AS discount_amount,
        TIMESTAMP '2026-07-07 09:00:00' AS source_updated_at

    UNION ALL

    SELECT
        CAST(5007 AS BIGINT) AS sales_id,
        'ORD-5007' AS order_number,
        CAST(1002 AS BIGINT) AS customer_id,
        CAST(2002 AS BIGINT) AS product_id,
        CAST(102 AS BIGINT) AS store_id,
        DATE '2026-07-07' AS sales_date,
        CAST(3 AS INT) AS quantity,
        CAST(80.00 AS DECIMAL(10, 2)) AS unit_price,
        CAST(20.00 AS DECIMAL(10, 2)) AS discount_amount,
        TIMESTAMP '2026-07-07 09:10:00' AS source_updated_at
) AS source
ON target.sales_id = source.sales_id
WHEN MATCHED THEN UPDATE SET
    target.order_number = source.order_number,
    target.customer_id = source.customer_id,
    target.product_id = source.product_id,
    target.store_id = source.store_id,
    target.sales_date = source.sales_date,
    target.quantity = source.quantity,
    target.unit_price = source.unit_price,
    target.discount_amount = source.discount_amount,
    target.source_updated_at = source.source_updated_at
WHEN NOT MATCHED THEN INSERT (
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
VALUES (
    source.sales_id,
    source.order_number,
    source.customer_id,
    source.product_id,
    source.store_id,
    source.sales_date,
    source.quantity,
    source.unit_price,
    source.discount_amount,
    source.source_updated_at
);

-- Source verification: expected counts are 4 customers, 3 products, and 7 sales.
SELECT 'dim_customer_source' AS table_name, count(*) AS row_count
FROM workspace.dim.dim_customer_source
UNION ALL
SELECT 'dim_product_source', count(*)
FROM workspace.dim.dim_product_source
UNION ALL
SELECT 'sales_source', count(*)
FROM workspace.dim.sales_source
ORDER BY table_name;

-- Confirm the recovered customer.
SELECT *
FROM workspace.dim.dim_customer_source
WHERE customer_id = 1999;

-- Confirm the recovered product.
SELECT *
FROM workspace.dim.dim_product_source
WHERE product_id = 2999;

-- Confirm the changed sale and new sale, including their expected amounts.
SELECT
    sales_id,
    order_number,
    customer_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    CAST((quantity * unit_price) - discount_amount AS DECIMAL(12, 2)) AS expected_sales_amount,
    source_updated_at
FROM workspace.dim.sales_source
WHERE sales_id IN (5001, 5007)
ORDER BY sales_id;

-- After the verification queries succeed, preserve the parking history by using
-- the normal incremental command. Do not use --full-refresh for this snapshot:
--
-- dbt build --select pentaho_project_1 --vars '{
--   "sales_watermark_start": "2026-07-06 09:00:00",
--   "sales_watermark_end": "2026-07-07 09:10:00",
--   "customer_watermark_start": "2026-07-01 08:22:00",
--   "customer_watermark_end": "2026-07-07 08:00:00",
--   "product_watermark_start": "2026-07-01 08:31:00",
--   "product_watermark_end": "2026-07-07 08:30:00"
-- }'
