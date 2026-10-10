# Step 19.1 — Streaming scope and event contracts

Prepared: 2026-10-10  
Status: analysis and proposed development contract; ingestion is not implemented.

## 1. Objective and boundaries

Define exactly what the Python producer sends to Kafka, how the consumer validates it, and how valid events become the source rows already consumed by dbt.

The development flow is:

```text
Python synthetic-data producer
  → Kafka sales/customer/product topics
  → Python consumer: validation and normalization
  → durable, repeatable Delta source-table upserts
  → prepare_watermarks → dbt_build → advance_watermarks
  → customer/product marts
```

Step 19.1 delivers a contract and implementation guide. Broker setup belongs to 19.2; producer, consumer, writes, integration, and recovery testing belong to 19.3–19.7. No broker, deployment, producer, consumer, or live database change is performed by this document.

Start with full-row insert/update events for sales, customers, and products. Keep region and store as seeded reference data. Exclude hard deletes, partial updates, schema migration automation, and continuous dbt execution from the first version. The source tables represent current state, not event history. Retain event history separately if replay or audit is required.

## 2. Existing repository behavior

These are facts from the checked-in code, rather than assumptions about the live workspace:

| Evidence | Consequence for ingestion |
|---|---|
| `docs/sql_db_script/databricks/initial_source_data.sql` | Defines the source columns, Delta types, and reference fixtures. Its `CREATE OR REPLACE` statements reset source tables; use only for a controlled baseline. |
| `models/sources/sql_server_sources.yml` | Declares all five sources, with non-null columns and unique source IDs tested by dbt. |
| `models/pentaho_project_1/staging/*.sql` | Casts IDs, decimals, dates, booleans, and timestamps; trims names/category and lowercases customer email. |
| `models/pentaho_project_1/intermediate/stg_customer_sales.sql` and `stg_product_sales.sql` | Select changed records with `source_updated_at > start AND source_updated_at <= end`; changed dimensions also cause affected sales to be processed. |
| `databricks/notebooks/prepare_watermarks.py` | Reads separate sales/customer/product control rows and captures each source maximum in UTC. |
| `databricks/notebooks/advance_watermarks.py` | Advances the captured ends after successful processing; it cannot discover events that arrive behind those ends. |
| Parking models and `tests/` | Support missing customer/product recovery and test sales values, parking state, and summary reconciliation. |

The source YAML still describes SQL Server origin. Kafka replaces the ongoing population mechanism; it does not require changing dbt's source names or moving business transformations into the consumer.

## 3. Step-by-step contract definition

### Step 1 — Map each entity to one topic and source table

Use these proposed development names consistently in the producer and consumer:

| Entity | Topic | Kafka key | Delta destination | Upsert key |
|---|---|---|---|---|
| Sales | `pentaho.project1.sales.v1` | Decimal string of `sales_id` | `workspace.dim.sales_source` | `sales_id` |
| Customer | `pentaho.project1.customer.v1` | Decimal string of `customer_id` | `workspace.dim.dim_customer_source` | `customer_id` |
| Product | `pentaho.project1.product.v1` | Decimal string of `product_id` | `workspace.dim.dim_product_source` | `product_id` |

A sales ID identifies one sales row, not an order. Multiple rows may share an order number. Customer/product codes must stay unique within their entity and stable for a given ID.

Use UTF-8 JSON values and UTF-8 string keys. Require the key to match the payload ID. Keep a given entity ID owned by one producer at a time. Multiple producer instances must use allocated, nonoverlapping ID ranges or a shared ID allocator; Faker randomness alone is not an ID allocation strategy.

Kafka partition order does not establish order across topics or coordinate independent producers. Choose partition count and retention in Step 19.2. See [Apache Kafka design](https://kafka.apache.org/design/) for ordering and delivery semantics.

**Output:** an entity/topic/key/table mapping shared by the two applications.

### Step 2 — Define one common event envelope

Every event uses the following required envelope:

| Field | JSON type | Contract |
|---|---|---|
| `schema_version` | integer | Exactly `1` for this version. Unknown versions go to quarantine. |
| `event_id` | string | UUID identifying the logical event. Preserve it on retry/replay. |
| `entity_type` | string | `sales`, `customer`, or `product`; must agree with the topic. |
| `operation` | string | `upsert` only. Delete/tombstone and patch messages are unsupported. |
| `producer_id` | string | Stable producer instance or allocation identity. |
| `produced_at` | string | Timezone-aware RFC 3339 timestamp, normalized to UTC. |
| `data` | object | Complete entity row with all fields below. |

Keep envelope fields out of the existing source tables. Store them with Kafka topic, partition, offset, validation result, and ingestion batch ID in a separate ingestion audit/quarantine store designed in Steps 19.4–19.5. The exact store remains to be selected.

For v1, reject unexpected envelope or data fields rather than silently ignoring misspellings. A new schema version must have explicit consumer support before producers emit it. This document defines the contract in tables and examples; machine-readable validation schemas are a later implementation deliverable.

**Output:** one envelope definition, reused by all three entity validators.

### Step 3 — Define exact payload columns

All listed fields are required and non-null. No consumer defaults should silently fill missing business values.

#### Sales payload

| Column | JSON representation | Delta type | Validation |
|---|---|---|---|
| `sales_id` | integer | BIGINT | Positive, stable, unique source row ID. |
| `order_number` | string | STRING | Nonblank order reference. |
| `customer_id` | integer | BIGINT | Positive customer ID; temporary absence is allowed for parking scenarios. |
| `product_id` | integer | BIGINT | Positive product ID; temporary absence is allowed for parking scenarios. |
| `store_id` | integer | BIGINT | Must reference an existing seeded store. |
| `sales_date` | string `YYYY-MM-DD` | DATE | Valid calendar date; independent of arrival time. |
| `quantity` | integer | INT | Greater than zero, within signed 32-bit range. |
| `unit_price` | decimal string | DECIMAL(10,2) | Nonnegative, exactly two decimal places, within precision. |
| `discount_amount` | decimal string | DECIMAL(10,2) | Nonnegative and no greater than `quantity × unit_price`. |
| `source_updated_at` | UTC timestamp string | TIMESTAMP | Source mutation/version timestamp; see Section 4. |

Validate that the derived amount `(quantity × unit_price) − discount_amount` fits dbt's `DECIMAL(12,2)` output as well. Do not send `sales_amount`; dbt calculates it. The sale's price is its transaction price and may differ from the product's current price.

#### Customer payload

| Column | JSON representation | Delta type | Validation |
|---|---|---|---|
| `customer_id` | integer | BIGINT | Positive stable ID. |
| `customer_code` | string | STRING | Nonblank and unique across customers. |
| `first_name` | string | STRING | Nonblank after trimming. |
| `last_name` | string | STRING | Nonblank after trimming. |
| `email` | string | STRING | Nonblank; generate synthetic addresses under `example.test`; trim and lowercase. |
| `region_id` | integer | BIGINT | Must reference an existing seeded region. |
| `is_active` | boolean | BOOLEAN | JSON `true` or `false`, not strings or numeric flags. |
| `source_updated_at` | UTC timestamp string | TIMESTAMP | Source mutation/version timestamp. |

#### Product payload

| Column | JSON representation | Delta type | Validation |
|---|---|---|---|
| `product_id` | integer | BIGINT | Positive stable ID. |
| `product_code` | string | STRING | Nonblank and unique across products. |
| `product_name` | string | STRING | Nonblank after trimming. |
| `category` | string | STRING | Nonblank after trimming. |
| `unit_price` | decimal string | DECIMAL(10,2) | Nonnegative, exactly two decimal places, within precision. |
| `is_active` | boolean | BOOLEAN | Actual JSON boolean. |
| `source_updated_at` | UTC timestamp string | TIMESTAMP | Source mutation/version timestamp. |

Parse monetary strings with decimal arithmetic, not binary floating point. Reject excess fractional digits instead of rounding silently. Reject booleans where an integer is expected, fractions for IDs, and values outside signed BIGINT range. Timestamp precision is at most microseconds; reject unsupported precision rather than collapsing distinct versions.

The stronger nonblank and positive-ID rules above are proposed ingestion rules; SQL source DDL itself only declares types and non-null constraints. `is_active=false` is a valid source state, not a deletion command. Do not filter inactive rows out in ingestion.

**Output:** three complete payload validators and normalization rules to implement later.

### Step 4 — Keep region/store references explicit

The baseline fixture contains:

| Reference | ID | Details |
|---|---|---|
| Region | `10` | Canada East |
| Region | `20` | Canada West |
| Store | `101` | Toronto Test Store, region `10` |
| Store | `102` | Vancouver Test Store, region `20` |

Verify the actual development tables before using these IDs:

```sql
SELECT region_id, region_code, region_name
FROM workspace.dim.dim_region_source ORDER BY region_id;

SELECT store_id, store_code, region_id, is_active
FROM workspace.dim.dim_store_source ORDER BY store_id;
```

Configure the producer's allowed references from the verified development fixture. The consumer validates these references and quarantines unknown stores/regions. Updating the fixture is a controlled operation; there are no region/store streaming topics in v1.

Do not require the sale's store region to equal the customer's home region: the baseline includes sales across regions. Missing customer/product IDs are allowed in explicit recovery scenarios because dbt parks those sales. Missing store/region references are outside that recovery mechanism.

**Output:** a verified reference manifest for development.

### Step 5 — Define update, duplicate, and conflict behavior

For each ID, compare the incoming `source_updated_at` with the stored version:

| Incoming state | Required action |
|---|---|
| New ID | Insert once, subject to the late-arrival policy in Section 4. |
| Timestamp newer than stored row | Replace the complete source payload. |
| Same timestamp and same normalized payload | Duplicate; no source mutation. |
| Same timestamp and different payload | Conflict; quarantine and investigate. Do not use arrival order to choose a winner. |
| Timestamp older than stored row | Stale event; retain audit evidence and leave current state unchanged. |
| Null payload, delete operation, unknown schema, invalid types | Quarantine; do not mutate source state. |

Deduplicate within each incoming batch before merging. Detect conflicting equal versions before selecting the newest valid version per ID. `event_id` helps trace retries; business ID plus version and payload comparison determines source-state idempotency.

A later correction creates a new event ID and a strictly later mutation timestamp. A retry preserves the original event ID, timestamp, and payload. Never refresh timestamps merely to make a replay look new.

Acknowledge only the contiguous successfully handled prefix of each Kafka partition after a durable source write or a durable quarantine decision. A failed write must remain retryable. A crash after the Delta write but before offset commit can cause redelivery, so repeated writes must be safe. Do not claim end-to-end exactly-once behavior from Kafka producer settings alone. See [Apache Kafka consumer API](https://kafka.apache.org/35/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).

**Output:** a deterministic decision table for Step 19.5 and its recovery tests.

## 4. Critical analysis: timestamps and watermark safety

### Preserve the meaning of `source_updated_at`

It identifies the latest mutation of a source record. It is not `sales_date`, Kafka offset, producer send time, or consumer processing time. Require timezone-aware timestamps and normalize to UTC with up to six fractional digits. Retries keep the same value; actual updates advance it strictly for that ID. Reject future timestamps beyond an explicitly configured tolerance; for the first controlled synthetic scenario, generate no future mutation timestamps.

The three source streams have independent watermarks. An event can be newer than its stored row and still be older than its stream's already committed watermark.

### Failure example: a delayed event

1. Sales watermark is `10:00`.
2. Another sale with mutation timestamp `10:10` is ingested and dbt succeeds through `10:10`.
3. A previously unseen sale with timestamp `10:05` arrives afterward.
4. Inserting it into the source does not make it eligible for the next `(10:10, end]` window.

This record can be missed indefinitely by the current incremental models. A timestamp equality at the start is also excluded. A newer-version-only source upsert does not fix this problem. Do not overwrite the event timestamp with arrival time to hide it.

### Failure example: changing a source during dbt

If preparation captures end `10:10` and ingestion overwrites an eligible source row with version `10:11` before dbt reads it, the older version is no longer in the current-state table. The new version may be processed next run, but the captured window is not a frozen source snapshot. Separate model reads can also see different dimension states. Fixed watermark values alone do not provide snapshot isolation across the workflow.

### Proposed first-development policy

Use controlled ingestion batches and serialize publication with the complete dbt workflow:

1. Stop competing source writers and schedules for the exercise.
2. Read the three persisted watermark starts and source maxima without changing them.
3. Generate a bounded batch whose new accepted mutations are later than both the relevant committed watermark and existing source maximum.
4. Consume the complete expected batch, validate it, and finish all source writes. Track a scenario manifest of expected event IDs/counts; elapsed idle time is not proof of completeness.
5. Quarantine any new or newer mutation at or behind the committed watermark. A duplicate or an already superseded version may be handled as a no-op with audit evidence. Do not silently discard a late mutation requiring recovery.
6. Pause source-table publication while `prepare_watermarks → dbt_build → advance_watermarks` runs, including failure repair. Producers may continue buffering in Kafka within retention limits.
7. Verify dbt results and exact watermark advancement before publishing the next batch. Preserve the original captured window for repair.

A shared orchestration gate must cover all source writers and both manual and scheduled dbt jobs. A concurrency limit on one job does not enforce this across different jobs or ingestion processes. Checking the watermark before a write without coordinating with advancement also leaves a race.

This policy supports a controlled learning exercise, not unrestricted continuous ingestion. Intentionally late customer/product recovery uses a new lookup mutation timestamp in the next accepted batch, even when the referenced sale date is old.

### Decision required before continuous operation

Step 19.6 must implement and test a robust alternative: for example, durable ingestion batches with a stable snapshot/change set and its own processing cursor, or a redesigned change-history/CDC integration. An overlap window may help bounded delays, but needs an explicit lateness bound, reprocessing logic, and recovery for events outside that bound; it is not a complete solution by itself.

If changing dbt to use an ingestion cursor, keep the original mutation timestamp separately and change the models, control contract, and tests explicitly. The present notebooks and models do not implement these alternatives.

## 5. Example events

These are illustrative fixture events, not commands to publish. IDs are outside the supplied baseline; check for collisions and allocate ranges before use. Timestamps must be regenerated for a real scenario according to Section 4.

### Customer — Kafka key `100001`

```json
{
  "schema_version": 1,
  "event_id": "f1a3a800-0000-4000-8000-000000000001",
  "entity_type": "customer",
  "operation": "upsert",
  "producer_id": "dev-producer-01",
  "produced_at": "2026-10-10T14:00:01.000000Z",
  "data": {
    "customer_id": 100001,
    "customer_code": "CUST-100001",
    "first_name": "Alex",
    "last_name": "Morgan",
    "email": "alex.100001@example.test",
    "region_id": 10,
    "is_active": true,
    "source_updated_at": "2026-10-10T14:00:00.000000Z"
  }
}
```

### Product — Kafka key `200001`

```json
{
  "schema_version": 1,
  "event_id": "f1a3a800-0000-4000-8000-000000000002",
  "entity_type": "product",
  "operation": "upsert",
  "producer_id": "dev-producer-01",
  "produced_at": "2026-10-10T14:00:02.000000Z",
  "data": {
    "product_id": 200001,
    "product_code": "PROD-200001",
    "product_name": "Training Ball",
    "category": "Equipment",
    "unit_price": "25.00",
    "is_active": true,
    "source_updated_at": "2026-10-10T14:00:00.000000Z"
  }
}
```

### Sale — Kafka key `500001`

```json
{
  "schema_version": 1,
  "event_id": "f1a3a800-0000-4000-8000-000000000003",
  "entity_type": "sales",
  "operation": "upsert",
  "producer_id": "dev-producer-01",
  "produced_at": "2026-10-10T14:00:03.000000Z",
  "data": {
    "sales_id": 500001,
    "order_number": "ORD-500001",
    "customer_id": 100001,
    "product_id": 200001,
    "store_id": 101,
    "sales_date": "2026-10-10",
    "quantity": 2,
    "unit_price": "25.00",
    "discount_amount": "5.00",
    "source_updated_at": "2026-10-10T14:00:00.000000Z"
  }
}
```

After all three source rows are durable, the sale contributes `45.00` to both enriched outputs and the corresponding customer/product summaries. Publication across different topics must not rely on a global Kafka order.

## 6. Walkthrough for the next implementation steps

1. **Inspect development state.** Run the read-only reference queries above, inspect source schemas, and capture the three control watermarks. Do not rerun the reset script against an active workflow.
2. **Freeze the v1 contract.** Use the mapping, envelope, payloads, and duplicate/conflict rules above. Record any change here before implementing it in either application.
3. **Allocate synthetic IDs.** Reserve nonoverlapping ranges per producer and preserve a scenario manifest across restarts. Generate stable codes from IDs.
4. **Set up Kafka in 19.2.** Select a broker and configure topics, partitions, retention, authentication, and connectivity. Keep credentials outside Git. Confirm the chosen ingestion deployment can actually reach both Kafka and Databricks; the repository does not prove that connectivity.
5. **Build the producer in 19.3.** Emit full-row JSON, stable keys, UTC source versions, exact decimal strings, and reproducible scenarios. Publish normal customer/product events before their sales, while retaining explicit missing-lookup scenarios.
6. **Build validation in 19.4.** Parse strictly, normalize consistently, check references, and durably quarantine invalid/conflicting records with reasons and Kafka positions.
7. **Implement durable writes in 19.5.** Select the Databricks write mechanism, deduplicate each batch, enforce version comparisons, coordinate concurrent writers, and commit Kafka progress only after durable handling.
8. **Integrate under the publication gate in 19.6.** Complete a bounded ingestion batch, run the existing three-task dbt workflow, inspect artifacts and watermark results, then release the next batch. Resolve continuous-ingestion design before enabling independent schedules.
9. **Exercise the acceptance scenarios below in 19.7.** Record expected and actual source rows, parking states, mart totals, offsets, and workflow IDs.

## 7. Acceptance scenarios to carry forward

| Scenario | Expected result |
|---|---|
| Customer + product + sale examples | One source row each; sale amount `45.00`; correct enriched rows and mart totals. |
| Exact event replay | No duplicate source IDs or changed source version; no double counting. |
| Newer update to sale quantity | Same sales ID updated once; both affected reports reflect the revised amount. |
| Newer customer/product update | Affected sales are reprocessed through the appropriate dimension window. |
| Older version after newer version | Stored newest row preserved; stale delivery auditable. |
| Equal version with different payload | Conflict quarantined; source row preserved. |
| New ID with timestamp at/below committed watermark | Held in quarantine for explicit recovery, not silently accepted and missed by dbt. |
| Missing customer, then later customer event | Sale initially parked; recovery batch resolves customer parking and updates eligible enriched outputs. |
| Missing product, then later product event | Customer output can contain the valid-customer sale; product output waits for lookup recovery. |
| Unknown store/region or invalid numeric/date value | Durable quarantine with reason; no invalid source mutation. |
| Consumer crash after Delta write, before offset commit | Redelivery is harmless; durable source result retained. |
| Invalid record followed by valid records in same partition | Offset never advances past an unhandled record; durable quarantine allows controlled progress. |
| Two producers with allocated IDs | No collisions or code duplication; same-ID updates have explicit ownership. |
| dbt failure followed by repair | Watermarks remain unchanged on failure; publication stays gated; repair reuses original bounds. |
| Attempted source publication while dbt runs | Gate blocks publication; source reads see the controlled batch. |

## 8. Completion and open decisions

Step 19.1 analysis is documented when the topic mapping, schemas, source-column mappings, region/store policy, and timestamp/replay rules can be reviewed together. Implementation acceptance remains for later steps.

Before implementing, confirm these proposed defaults: JSON full-row upserts; three entity topics; static region/store fixtures; single-owner entity IDs; timestamp-based version comparison; durable quarantine; and serialized development ingestion/dbt batches.

Still to select in the appropriate later steps: broker/platform, partition and retention settings, Python Kafka client, machine-readable schema tooling, Databricks write path, audit/quarantine storage, publication-gate implementation, and the continuous-ingestion recovery strategy. No platform availability, deployment, or runtime test result is claimed here.

Related documents: [implementation tracker](steps.txt), [migration guide](migration_guide.md), and [version 2 notebook workflow guide](notebook_runner_v2.md).
