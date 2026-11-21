import hashlib
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_text(path, text):
    path = Path(path)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
        handle.flush()
    temporary.replace(path)


def write_generation(run_dir, events, config, references, allocations, dependency_versions):
    run_dir = Path(run_dir)
    if run_dir.exists():
        raise FileExistsError(f"run directory already exists: {run_dir}")
    run_dir.mkdir(parents=True)

    events_path = run_dir / "events.jsonl"
    with events_path.open("x", encoding="utf-8", newline="\n") as handle:
        for event in events:
            line = canonical_json({
                "topic": "pentaho.project1." + event["entity_type"] + ".v1" if event["entity_type"] != "sales" else "pentaho.project1.sales.v1",
                "key": str(event["data"][f'{event["entity_type"]}_id']),
                "value": event,
            })
            handle.write(line + "\n")

    event_hash = sha256_file(events_path)
    counts = {}
    distinct_ids = {}
    totals = {}
    for entity in ("customer", "product", "sales"):
        entity_events = [event for event in events if event["entity_type"] == entity]
        counts[entity] = len(entity_events)
        id_field = f"{entity}_id"
        ids = {event["data"][id_field] for event in entity_events}
        distinct_ids[entity] = len(ids)
        if entity == "sales":
            from decimal import Decimal
            totals["expected_sales_net_cents"] = sum(
                int(Decimal(event["data"]["unit_price"]) * 100) * event["data"]["quantity"]
                - int(Decimal(event["data"]["discount_amount"]) * 100)
                for event in entity_events
            )

    intended_versions = {}
    for entity in ("customer", "product", "sales"):
        intended_versions[entity] = {
            str(event["data"][f"{entity}_id"]): event["data"]["source_updated_at"]
            for event in events if event["entity_type"] == entity
        }

    manifest = {
        "schema_version": 1,
        "scenario_id": config["scenario_id"],
        "scenario_type": config["scenario_type"],
        "producer_id": allocations["producer_id"],
        "producer_allocation": allocations["ranges"],
        "seed": config["seed"],
        "locale": config["locale"],
        "dependency_versions": dependency_versions,
        "runtime": {"python": sys.version.split()[0], "platform": platform.platform()},
        "timestamps": {
            "mutation_base": config["mutation_base"],
            "produced_at": config["produced_at"],
            "mutation_date": config["mutation_date"],
            "sales_date": config["sales_date"],
        },
        "bounds": config["bounds"],
        "reference_snapshot": references,
        "expected_event_ids": [event["event_id"] for event in events],
        "per_topic_counts": counts,
        "distinct_ids": distinct_ids,
        "intended_final_versions": intended_versions,
        "expected_business_totals": totals,
        "events_sha256": event_hash,
    }
    atomic_write_text(run_dir / "manifest.json", canonical_json(manifest) + "\n")
    return manifest


def read_run(run_dir):
    run_dir = Path(run_dir)
    manifest_path = run_dir / "manifest.json"
    events_path = run_dir / "events.jsonl"
    if not manifest_path.is_file() or not events_path.is_file():
        raise FileNotFoundError("run directory requires manifest.json and events.jsonl")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if sha256_file(events_path) != manifest["events_sha256"]:
        raise ValueError("events.jsonl SHA-256 does not match manifest")

    records = []
    with events_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            record = json.loads(line)
            if set(record) != {"topic", "key", "value"}:
                raise ValueError(f"invalid event file record at line {line_number}")
            records.append(record)
    return manifest, records
