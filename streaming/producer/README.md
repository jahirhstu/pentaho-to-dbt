# Synthetic Kafka producer

Independent Python 3.11+ application for producing v1 customer, product, and sales events to the local Apache Kafka broker.

## Setup

```bash
cd streaming/producer
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.lock
python -m pip install --no-deps -e .
python -m synthetic_producer --help
```

Before generating, verify the reference IDs and captured watermarks for the active development environment. Replace the example files with verified local values.

## Generate and publish

```bash
python -m synthetic_producer generate \
  --config config/scenario.example.json \
  --output runs/baseline-001

python -m synthetic_producer publish \
  --run-dir runs/baseline-001 \
  --bootstrap-servers localhost:9092 \
  --max-events-per-second 10
```

Generation is offline. It refuses existing run directories and writes an immutable `events.jsonl`, manifest, and later delivery reports. Publishing verifies the event file hash and all v1 contracts before enqueueing.

Use the same run directory for replay. Replaying can duplicate events; event IDs and payload bytes remain unchanged.

## Tests

```bash
python -m pytest tests -q
```

Generated `runs/` are local replay artifacts and are excluded by Git. This producer has no Databricks dependencies or credentials.
