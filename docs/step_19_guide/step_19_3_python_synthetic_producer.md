# Step 19.3 — Create the Python synthetic-data producer project

Status: implementation guide; the producer described below is not implemented by
this document. Creating this guide does not complete Step 19.3.

## 1. Objective and boundaries

Build a separate Python application that uses Faker to generate coherent
customers, products, and sales, then publishes full-row JSON events to the local
Kafka broker installed in [Step 19.2](step_19_2_local_kafka_ubuntu.md).

```text
Faker + scenario configuration
  → validate and save immutable event files + manifest
  → Python Kafka producer
  → local Kafka topics
```

The application must support stable IDs, repeatable scenarios, updates, retries,
and multiple producer instances. It must not write to Databricks, run dbt, or
advance watermarks. Consumer validation, source-table writes, and workflow
coordination belong to Steps 19.4–19.6.

Use [Step 19.1](../step_19_1_streaming_scope_and_contracts.md) as the authoritative
event contract. Do not change its field names or add scenario metadata to the
event envelope. Keep scenario metadata in local manifests.

## 2. Check prerequisites on Ubuntu

Run from the repository root on the same Ubuntu host as Kafka:

```bash
python3 --version
sudo systemctl status kafka --no-pager
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Use Python 3.11 or newer as the proposed application baseline. If the virtual
environment module is missing, install the package matching Ubuntu's Python:

```bash
sudo apt update
sudo apt install -y python3-venv
```

Confirm these topics already exist with the Step 19.2 configuration:

| Entity | Topic | UTF-8 Kafka key |
|---|---|---|
| Customer | `pentaho.project1.customer.v1` | Decimal string of `customer_id` |
| Product | `pentaho.project1.product.v1` | Decimal string of `product_id` |
| Sales | `pentaho.project1.sales.v1` | Decimal string of `sales_id` |

Each currently has one partition, replication factor one, and seven-day
retention. The broker is loopback-only: running this application on another
machine or inside a container will not make that machine's `localhost` point to
the Ubuntu broker.

## 3. Create an independent Python project

Proposed location inside this repository:

```text
streaming/producer/
├── pyproject.toml
├── requirements.lock
├── requirements-dev.lock
├── README.md
├── config/
│   ├── references.example.json
│   ├── allocations.example.json
│   └── scenario.example.json
├── src/synthetic_producer/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py
│   ├── config.py
│   ├── contracts.py
│   ├── generators.py
│   ├── scenarios.py
│   ├── manifest.py
│   └── publisher.py
└── tests/
    ├── test_contracts.py
    ├── test_scenarios.py
    └── test_publisher.py
```

Use `Faker` for synthetic descriptions/names and `confluent-kafka` for Kafka
publishing; use `pytest` for development checks. Standard-library `decimal`,
`datetime`, `uuid`, and `json` cover money, UTC timestamps, IDs, and serialization.
No Confluent Cloud subscription or Schema Registry is required for these plain
JSON events sent to Apache Kafka.

Declare the application and supported Python version in `pyproject.toml`, with
package discovery under `src/`. Resolve and commit exact dependency versions,
including transitive dependencies, in the two lock files after checking Ubuntu
installation compatibility. The development lock must use the same runtime
versions. Pin Faker to its patch version because seeded output can change
between versions. Do not add these dependencies to the root dbt requirements.

Once the project and lock files exist, install it from the repository root:

```bash
cd streaming/producer
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.lock
python -m pip install --no-deps -e .
python -m synthetic_producer --help
```

Add producer-specific ignore rules for generated `runs/`, `*.egg-info/`,
`__pycache__/`, and `.pytest_cache/`. The existing root ignore rules already
exclude `.venv/` and `.env`. Keep small deliberate test fixtures in Git; keep
generated run artifacts out of Git. Preserve them locally for replay.

## 4. Define configuration and ownership before generating data

Separate three inputs:

1. **Reference manifest:** verified region/store IDs from development.
2. **Allocation manifest:** reserved entity-ID ranges and their producer owner.
3. **Scenario configuration:** seed, locale, counts, timestamps, and scenario type.

Verify reference data using the read-only SQL in Step 19.1. The baseline fixture
has regions `10`, `20` and stores `101`, `102`, but those values must match the
actual development tables. Export or manually record the verified values; the
producer itself does not need Databricks credentials or connectivity.

Example allocation proposal, requiring collision checks before use:

| Producer | Customer IDs | Product IDs | Sales IDs |
|---|---|---|---|
| `dev-producer-01` | 100001–109999 | 200001–209999 | 500001–509999 |
| `dev-producer-02` | 110001–119999 | 210001–219999 | 510001–519999 |

Check ranges against existing source IDs, other reservations, and prior retained
scenario manifests. Fail if a requested count exceeds a range or if configured
ranges overlap within an entity. A local file cannot enforce exclusive ownership
across machines: reserve ranges centrally through the development allocation
record and permit only one active writer per allocation. A shared allocator is
future work if manual reservations stop being sufficient.

Derive stable codes from IDs, for example `CUST-100001` and `PROD-200001`.
Never use random Faker values as primary IDs. A fresh scenario must reserve new
IDs or explicitly update existing owned IDs; it must not silently reuse a range
as if all rows were new.

Validate configuration before creating output or contacting Kafka. Reject
unknown scenario names, invalid counts, empty producer identities, unallocated
IDs, unknown references, and invalid timestamps. Use a bounded event-rate option
for publication rather than an unbounded generation loop.

## 5. Implement the event contract and coherent generation

Implement strict validators in `contracts.py` directly from Step 19.1. Reuse them
when generating and when reading event files for publication. Reject extra or
missing fields; reject booleans as integers; enforce BIGINT/INT and decimal
bounds. Consumers will independently validate the same contract in Step 19.4.

Every event contains exactly:

```text
schema_version, event_id, entity_type, operation,
producer_id, produced_at, data
```

Use `schema_version=1` and `operation="upsert"`. The `data` object must contain
all columns for its entity, including `source_updated_at`. Do not emit partial
updates, deletes, derived sales amounts, or extra envelope fields.

Generation rules:

- Create customers/products first and keep their complete rows in an ID-indexed
  registry. Choose normal sales references from those registries.
- Use Faker for names and product descriptions; construct customer email as a
  synthetic unique value such as `alex.100001@example.test`.
- Select customer regions and sales stores from the verified manifest. A
  customer's region need not match the sale's store region.
- Generate real JSON booleans for `is_active`. Retain inactive rows as valid
  states; they are not deletions.
- Generate monetary values as integer cents, calculate with `Decimal`, and
  serialize as strings with exactly two decimal places. Bound discount by the
  gross amount and the net amount by `DECIMAL(12,2)`.
- Use positive integer quantities. Use valid ISO sales dates on or before the
  configured mutation date for the initial realistic scenarios. A transaction
  price may differ from the product's current price.
- An update retains its business ID/code and emits a complete replacement row,
  a new event ID, and a strictly later source mutation timestamp.

Use an instance-local Faker seed and an independent seeded random generator.
Fix locale, seed, ID allocations, scenario configuration, and timestamp inputs.
Generate deterministic UUIDs with UUIDv5 from a fixed application namespace plus
producer ID, scenario ID, entity, business ID, and logical mutation number.
Persist those values so changing implementation details cannot change a replay.

## 6. Make timestamps safe for the existing watermark workflow

`source_updated_at` is the source mutation version; `produced_at` is event
creation time; `sales_date` is the business date. They serve different purposes.
Use timezone-aware UTC timestamps with at most microsecond precision.

For a new normal development batch, an operator must capture, per entity, both
the persisted watermark and current source maximum using read-only queries.
Inspect [prepare_watermarks.py](../../databricks/notebooks/prepare_watermarks.py)
for the control-table structure and inspect the deployed job's parameters for
the actual pipeline identity before querying. Do not invent a pipeline name or
reset watermarks.

Record those three pairs of bounds in the scenario input. Every new accepted
mutation must be strictly later than its entity's two bounds and any preceding
version of the same ID. Recheck the bounds before later ingestion: an exported
snapshot is not a lock against concurrent writers.

Choose a fixed, recent UTC base time above the captured bounds. Allocate
microsecond increments to mutations and verify the final timestamp is not in
the future at generation time. Fail rather than silently shifting timestamps
or generating future versions when insufficient time is available. Different
records can share a timestamp; versions of one record must increase strictly.

The initial generation captures `produced_at` once and saves it. Regenerating a
fixture can supply that captured time explicitly; replay never refreshes either
timestamp. Exact repeatability requires these fixed inputs, not only a seed.

Step 19.3 only publishes to Kafka. Before Step 19.5 allows source writes, enforce
the serialized ingestion/dbt policy from Step 19.1, including its shared gate.
Kafka acknowledgements do not establish watermark safety or source-write
completion. Arbitrary continuous ingestion remains a Step 19.6 design decision.

## 7. Save immutable scenarios before publishing

Implement two separate CLI operations: `generate` and `publish`. The command
names and options below are the proposed interface to implement, not existing
repository commands.

```bash
python -m synthetic_producer generate \
  --config config/scenario.local.json \
  --output runs/baseline-001

python -m synthetic_producer publish \
  --run-dir runs/baseline-001 \
  --bootstrap-servers localhost:9092 \
  --max-events-per-second 10
```

The local scenario configuration must include its scenario/producer identities,
seed, locale, reference/allocation files, per-entity counts and captured bounds,
fixed mutation base, and any fixed event-creation time needed for regeneration.

Generation must not contact Kafka. Save the following before any send:

| Artifact | Required contents |
|---|---|
| `events.jsonl` | Ordered records of `{topic, key, value}`; `value` is the exact v1 event. |
| `manifest.json` | Scenario ID, producer allocation, dependency versions, seed/locale, timestamps, reference snapshot, expected event IDs, per-topic counts, distinct IDs, intended final versions, expected business totals, and event-file SHA-256. |
| `delivery-attempt-<id>.jsonl` | Created during each publish attempt: event ID, topic, key, success/error, partition/offset when acknowledged. |
| `delivery-summary-<id>.json` | Attempt totals, unresolved events, exit status, and completion time. |

Write generation artifacts through temporary files and atomically finalize them
before publication; refuse to overwrite an existing run directory. Each publish
attempt verifies the file hash, keys, topic/entity agreement, and contracts.
Never modify the saved events or manifest during a send attempt.

For replay, run `publish` against the same directory. This deliberately may
publish duplicates; preserve event IDs, payloads, producer identity, and both
timestamps. If a crash makes delivery status ambiguous, replay the immutable
batch rather than inventing new logical events. Later consumers must tolerate
these duplicates. Retain artifacts beyond Kafka retention when longer replay
is required; old events still need the late-arrival policy before source writes.

## 8. Implement reliable Kafka publication

Proposed `confluent_kafka.Producer` configuration:

```python
{
    "bootstrap.servers": "localhost:9092",
    "client.id": producer_id,
    "security.protocol": "PLAINTEXT",
    "enable.idempotence": True,
    "acks": "all",
    "allow.auto.create.topics": False,
    "delivery.timeout.ms": 120000,
}
```

Use `json.dumps(..., allow_nan=False)` and UTF-8 encoding for values; encode
the decimal ID key as UTF-8. Verify the three required topics exist before
sending. Do not silently create a misspelled topic.

Implement these behaviors in `publisher.py`:

1. Enqueue events with a delivery callback that records each outcome and sets
   an application failure flag on errors. Enqueuing is not acknowledgement.
2. Call `poll()` regularly to serve callbacks. If the local queue is full,
   poll and retry enqueuing the same event within a bounded deadline; do not
   create a new event or busy-loop indefinitely.
3. Publish the normal customer/product phase and confirm its delivery before
   sending sales. This helps inspection but does not guarantee cross-topic
   consumer processing order.
4. Flush on completion and controlled shutdown. Use a timeout longer than the
   configured delivery deadline, and inspect the returned undelivered count.
5. Exit nonzero on delivery errors, unresolved records, artifact-write failures,
   or interruption. Save the best available attempt report. A hard kill may
   leave uncertainty; the immutable batch remains the replay source.
6. Declare success only when every intended send has an acknowledgement and
   there are no failures or pending records. Compare manifest counts with
   callback counts, including deliberate duplicate sends.

The client dispatches delivery callbacks through `poll()` or `flush()`; see the
[official Python client guide](https://docs.confluent.io/kafka-clients/python/current/overview.html).
Idempotence reduces duplicates from internal producer retries. It does not
deduplicate separate replay invocations or make Databricks writes exactly once.
With one broker, `acks=all` still has no redundant broker copy.

## 9. Implement bounded, repeatable scenarios

| Scenario | Events and expected producer evidence |
|---|---|
| Baseline | Two customers, two products, five sales; all sales reference generated lookups; exact IDs/counts/net total in manifest. |
| Exact replay | Publish the same saved baseline again with unchanged event IDs and bytes. |
| Sales update | Change one owned sale's quantity; preserve ID, emit a full row with a new event ID/newer version, record revised expected total. |
| Lookup update | Change an owned customer or product with a full-row newer version; existing sales IDs remain stable. |
| Missing customer | Publish a sale referencing a reserved, not-yet-published customer; save the later lookup as a separate recovery batch. |
| Missing product | Same pattern for a reserved product ID. |
| Stale version | Emit a saved older version after a newer one, with explicit expected stale classification. |
| Equal-version conflict | Deliberately change payload at the same version, with expected conflict classification. |
| Invalid contract | Explicit negative fixture such as an unknown store or invalid quantity; expected quarantine reason in manifest. |
| Two producers | Generate and publish two independently owned allocations without ID/code collisions. |

For missing-lookup recovery, choose the recovery mutation timestamp above the
bounds captured for that later batch; do not publish a previously prepared old
timestamp as if it were a new accepted mutation. Preserve the original sale.

Normal generation/publication must reject malformed events. Permit negative
fixtures only through an explicit test-scenario option that checks the intended
violation, records it in the manifest, and requires an explicit publish flag.
Keep these fixtures out of normal development batches. Source mutation, parking,
quarantine, and mart outcomes are verified in later steps, not asserted from a
producer delivery callback.

## 10. Verify offline, then verify the local broker

Implement meaningful offline tests and run them from `streaming/producer`:

```bash
python -m pytest tests -q
```

Cover fixed-input reproducibility; contract/type/decimal boundaries; valid
references; range exhaustion and overlap; stable ownership/codes; strictly newer
update versions; no future timestamps; unchanged replay bytes; manifest hash
verification; and delivery errors, queue backpressure, and incomplete flush
through a fake Kafka client. Test negative-scenario opt-in separately.

For live verification, first capture each topic's current end offset with a
temporary inspection consumer, then publish a small baseline. Read from those
captured positions using a dedicated temporary verification group, without
changing the future ingestion consumer group's offsets. Implement this as a
test utility or use an equivalent Kafka inspection tool.

Collect records until the expected manifest identities and multiplicities are
observed, with a bounded timeout. Filter out unrelated scenario traffic and
fail on missing or unexpected scenario records. Match topic, key, exact event
value, partition, and delivery offsets to the attempt report. A quiet terminal
or a successful `produce()` call is not a completeness check.

Repeat with the immutable replay and two concurrently publishing, disjoint
producer allocations. The current single partition per topic permits multiple
producers, but limits consumer-group parallelism to one active consumer per
topic partition; increasing partitions is outside this producer setup.

Finally, stop the broker before a small publish attempt and verify bounded
failure, a nonzero exit, and preserved replay artifacts. Restart Kafka and replay
the same run directory successfully. Do not delete shared topics or reset source
tables to clean up this test.

## 11. Troubleshooting

| Symptom | Check |
|---|---|
| Cannot connect to `localhost:9092` | Broker service, loopback listener, and whether the producer is actually running on the Ubuntu host. |
| Missing topic | Compare names with Step 19.2; leave automatic creation disabled. |
| Queue full | Ensure regular polling, a bounded retry deadline, and an appropriate event-rate limit. |
| Program reports success but messages are missing | Inspect callback errors, flush pending count, expected counts, and exit status. |
| Same seed gives different data | Compare pinned Faker version, locale, call order, random generator, IDs, and fixed timestamps. |
| ID/code collision | Correct allocation ownership; do not generate a replacement random ID during replay. |
| Proposed timestamps are behind source state | Capture current bounds and generate a distinct new batch; retain old replay artifacts unchanged. |
| Kafka acknowledgements but no Databricks rows | Expected until the consumer and durable write path exist in Steps 19.4–19.5. |

## 12. Completion checklist and handoff

Only mark Step 19.3 **Done** in [steps.txt](../steps.txt) after:

- The independent producer package, dependency locks, configuration examples,
  CLI, and README exist and install successfully on Ubuntu.
- Generated events obey the exact Step 19.1 schemas and reference rules.
- Stable allocated IDs, full-row updates, timestamp bounds, and deterministic
  generation/replay are verified.
- Immutable event files, manifests, and delivery attempt reports are retained.
- Baseline, update, replay, recovery, and explicit negative fixtures exist.
- Offline tests pass, and local Kafka inspection confirms expected events.
- Two producer instances publish without ID/code collisions.
- Broker failure produces a visible bounded failure; replay succeeds afterward.
- The producer contains no Databricks writer or credential requirement.

Record the Git revision, Python/dependency versions, producer allocations,
scenario IDs, test results, Kafka delivery evidence, and unresolved issues.
Do not claim downstream ingestion or dbt verification at this stage.

Hand Step 19.4 the immutable event fixtures, manifest format, v1 validators,
reference snapshot, and expected classifications. Hand Step 19.5 the version
and replay cases, and Step 19.6 the timestamp bounds and publication-gate
requirements from Step 19.1.

## References

- [Step 19.1 — scope and contracts](../step_19_1_streaming_scope_and_contracts.md)
- [Step 19.2 — local Kafka](step_19_2_local_kafka_ubuntu.md)
- [Implementation tracker](../steps.txt)
- [Faker documentation: seeding and reproducibility](https://faker.readthedocs.io/en/stable/)
- [Confluent Python client guide](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Confluent Python client API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
