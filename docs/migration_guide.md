# Pentaho-to-dbt migration guide

## Transformation procedure inventory

`etl.usp_run_incremental_sales_etl` is the first procedure suitable for conversion
into dbt models. Pentaho contributes orchestration only; the procedure contains the
business logic.

Inputs read by the procedure:

| SQL Server object | Purpose | Current dbt status |
|---|---|---|
| `dim.sales_source` | Sales records and incremental timestamps | Declared source |
| `dim.dim_customer_source` | Customer attributes and customer validation | Declared source |
| `dim.dim_product_source` | Product name and category enrichment | Declared source |
| `parking.park_sales_missing_customer` | Unresolved sales rejected for missing customers | Replaced by the dbt model `park_sales_missing_customer` |
| `etl.etl_run_control` | Last successful watermark and execution audit | Will not be declared as a source; replace with dbt state/artifacts and Databricks workflow metadata |

Tables written by the procedure:

| SQL Server object | Procedure behavior | Likely dbt design |
|---|---|---|
| `parking.park_sales_missing_customer` | Insert/update rejected sales and mark resolved rows | Implemented as incremental model `park_sales_missing_customer`, keyed by `sales_id` |
| `staging.stg_customer_sales` | Merge valid sales enriched with customers | Implemented as incremental model `stg_customer_sales`, keyed by `sales_id` |
| `staging.stg_product_sales` | Merge valid sales enriched with products | Implemented as incremental model `stg_product_sales`, keyed by `sales_id` |
| `reporting.rpt_customer_sales_summary` | Recalculate summaries for affected customers | Implemented as table model `rpt_customer_sales_summary` |
| `reporting.rpt_product_sales_summary` | Recalculate summaries for affected products | Implemented as table model `rpt_product_sales_summary` |
| `etl.etl_run_control` | Record status, watermark, counts, and error text | Replaced by dbt run artifacts and Databricks workflow monitoring; it is not a dbt source or model in this learning project |

The procedure uses `source_updated_at` and the last successful run time as its
incremental watermark. The initial learning conversion favors clarity and result
correctness: it selects the current valid input and lets dbt merge by `sales_id`.
This avoids recreating the procedural run-control table. A later optimization can
reduce the source scan after the baseline output has been validated.

### Step 7 architecture decisions

Only tables supplied independently of dbt are permanent dbt sources. For this
pipeline those are `dim.sales_source`, `dim.dim_customer_source`, and
`dim.dim_product_source`.

`parking.park_sales_missing_customer` is different: the original procedure reads
and writes the same table. A dbt resource should not be treated simultaneously as
an external source and as a dbt model. Step 8 removed its transitional source
declaration and replaced it with a dbt-owned incremental model. The model retains
exceptions and marks them resolved after the corresponding customer becomes
available.

`etl.etl_run_control` mixes three operational concerns:

- an incremental watermark;
- job success or failure status;
- inserted, updated, and rejected row counts.

The dbt conversion does not reproduce this table. Incremental models use their
existing target state for merge operations; dbt artifacts describe model and test
results; Databricks workflow metadata records execution status and errors. This
avoids rebuilding procedural job-control logic inside transformation models.

## Step 8 implemented dbt model graph

Step 8 converts one large procedure into nine models. A model is a saved SQL
`select` statement. `source()` points to a table supplied to dbt, while `ref()`
points to another dbt model and creates an ordered dependency.

```text
source: sales_source ──> stg_sales_source ──┐
                                            ├─> int_sales_customer_status
source: customer ──────> stg_customer_source┘          │
                                                       ├─> park_sales_missing_customer
                                                       ├─> stg_customer_sales
                                                       │      └─> rpt_customer_sales_summary
source: product ───────> stg_product_source ───────────┴─> stg_product_sales
                                                              └─> rpt_product_sales_summary
```

### Source-boundary staging views

- `stg_sales_source` standardizes the sales data types.
- `stg_customer_source` standardizes customer names, email, flags, and timestamps.
- `stg_product_source` standardizes product attributes and timestamps.

These are views because they are lightweight one-to-one cleanup steps. They contain
no procedural state and no business aggregation.

### Customer validation

`int_sales_customer_status` left joins every sale to the customer source. A left
join keeps the sale even when no customer exists. The model adds
`is_missing_customer`, replacing the procedure's repeated missing-customer join
logic with one reusable definition.

### Parking exceptions

`park_sales_missing_customer` is incremental and uses `sales_id` as its unique key.
On its first run it selects sales with missing customers. On later runs it also
reads its own prior table using `{{ this }}`. This lets it preserve `parked_at` and
set `is_resolved` and `resolved_at` when the customer becomes available.

The dbt version deliberately removes the SQL Server identity `park_id` and
`etl_run_id`. `sales_id` is the merge key, and execution metadata belongs to dbt
and the Databricks workflow.

### Enriched sales

`stg_customer_sales` reproduces the customer enrichment and calculates:

```text
sales_amount = (quantity * unit_price) - discount_amount
```

`stg_product_sales` reproduces the product enrichment. Both are incremental Delta
models using dbt's `merge` strategy and `sales_id` as the unique key. Full-refresh
runs rebuild the complete valid result. Incremental runs use workflow-supplied,
source-specific watermark windows to identify sales changed directly, sales affected
by customer or product changes, and parked sales recovered by late-arriving lookups.
Only those affected sales enter the merge, so unchanged rows retain `loaded_at`.

The incremental window is bounded as `start < source_updated_at <= end` separately
for sales, customers, and products. The orchestrator must persist each end watermark
only after the complete dbt build succeeds, then use it as the next start watermark.
This design intentionally does not compare calculated rows with the target and does
not use row hashes. Consequently, any source row whose timestamp moves inside the
window is treated as affected even when its selected business values are unchanged.

### Reporting marts

`rpt_customer_sales_summary` groups the customer sales model by `customer_id`.
`rpt_product_sales_summary` groups the product sales model by `product_id`. They
recalculate complete current summaries as tables. This is simpler than maintaining
temporary lists of affected keys and produces the same business aggregates.

The reporting models omit `etl_run_id` and `created_at`. They retain `updated_at`.
This is an intentional metadata difference that must be considered when comparing
the dbt and Pentaho outputs.

## Step 9 model tests

Project 1 now tests primary model keys for uniqueness and required columns for
nulls. Relationship tests validate customer, product, store, region, and sales
references. Accepted-value tests constrain boolean status columns and the parking
rejection reason.

Singular business-rule tests reject invalid sales amounts, enforce consistent
parking resolution timestamps, and compare both reporting marts with fresh
aggregations of their staging models. These tests can be parsed locally; executing
them requires the Databricks connection created in later steps.

## Inventory one pipeline

Collect the `.kjb`, every referenced `.ktr`, stored-procedure definitions, table/view
DDL, parameter values, a small anonymized input, and expected output. Record:

1. Execution order and success/failure branches from the KJB.
2. Inputs, outputs, mappings, and procedure calls from each KTR.
3. Tables read and written by every stored procedure.
4. Temporary tables, updates, deletes, merges, transactions, and audit logging.

## Map Pentaho concepts to dbt

| Pentaho or SQL Server | dbt equivalent |
|---|---|
| KJB orchestration | Databricks Workflow and dbt selectors |
| KTR transformation | One or more dbt models |
| Source table | `source()` |
| Transformation dependency | `ref()` |
| Temporary table | CTE or intermediate model |
| INSERT/UPDATE/DELETE procedure | Declarative model or incremental merge |
| Procedure parameter | dbt variable or workflow parameter |
| Validation step | dbt data test |

## Convert the stored procedure

Do not translate a procedure line by line. Split it into logical datasets:

- `staging`: rename fields, standardize types, and perform basic cleanup.
- `intermediate`: joins, business rules, deduplication, and reusable calculations.
- `marts`: final facts, dimensions, and reporting tables.

Replace physical dependencies with `source()` and `ref()` so dbt constructs the DAG.
Use incremental models only when a reliable unique key and change column exist.

## Validate

For identical anonymized inputs, compare Pentaho and dbt outputs using row counts,
key uniqueness, null counts, aggregates, and row-level differences. Document any
intentional behavior change.
