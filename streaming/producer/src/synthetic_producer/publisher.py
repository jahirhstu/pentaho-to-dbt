import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .contracts import TOPICS, validate_event
from .manifest import atomic_write_text


class PublishingError(RuntimeError):
    pass


def verify_records(records, manifest, references):
    if len(records) != sum(manifest["per_topic_counts"].values()):
        raise ValueError("event count differs from manifest")
    seen = set()
    for record in records:
        event = record["value"]
        key = validate_event(event, references)
        if record["key"] != key:
            raise ValueError("Kafka key does not match payload ID")
        if record["topic"] != TOPICS[event["entity_type"]]:
            raise ValueError("topic does not match entity_type")
        seen.add(event["event_id"])
    if seen != set(manifest["expected_event_ids"]):
        raise ValueError("event IDs differ from manifest")


def publish_records(records, manifest, references, producer, max_events_per_second=10, queue_timeout=30.0):
    verify_records(records, manifest, references)
    attempt_id = uuid.uuid4().hex
    run_dir = Path(manifest["_run_dir"])
    attempt_path = run_dir / f"delivery-attempt-{attempt_id}.jsonl"
    summary_path = run_dir / f"delivery-summary-{attempt_id}.json"

    outcomes = []
    application_failed = False
    attempt_handle = attempt_path.open("x", encoding="utf-8", newline="\n")
    event_ids_by_key = {record["key"]: record["value"]["event_id"] for record in records}

    def delivery_callback(error, message):
        nonlocal application_failed
        raw_key_getter = getattr(message, "key", None)
        raw_key = raw_key_getter() if callable(raw_key_getter) else raw_key_getter
        key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else (raw_key or "")
        outcome = {
            "event_id": event_ids_by_key.get(key),
            "topic": message.topic() if hasattr(message, "topic") else None,
            "key": key,
            "success": error is None,
            "error": str(error) if error else None,
            "partition": message.partition() if hasattr(message, "partition") else None,
            "offset": message.offset() if hasattr(message, "offset") else None,
        }
        outcomes.append(outcome)
        attempt_handle.write(json.dumps(outcome, ensure_ascii=False, allow_nan=False, sort_keys=True) + "\n")
        attempt_handle.flush()
        if error:
            application_failed = True

    interval = 1.0 / max_events_per_second if max_events_per_second > 0 else 0
    last_send = 0.0
    dispatched = 0

    try:
        for record in records:
            if interval:
                wait = interval - (time.monotonic() - last_send)
                if wait > 0:
                    time.sleep(wait)
            deadline = time.monotonic() + queue_timeout
            while True:
                try:
                    value = json.dumps(record["value"], ensure_ascii=False, allow_nan=False).encode("utf-8")
                    producer.produce(record["topic"], key=record["key"].encode("utf-8"), value=value, on_delivery=delivery_callback)
                    dispatched += 1
                    last_send = time.monotonic()
                    break
                except BufferError:
                    producer.poll(0.1)
                    if time.monotonic() >= deadline:
                        application_failed = True
                        raise PublishingError("Kafka producer queue remained full")
                except Exception:
                    application_failed = True
                    raise
            producer.poll(0)

        producer.flush(120.0)
        pending = getattr(producer, "_last_pending", 0)
        if hasattr(producer, "flush"):
            try:
                pending = producer.flush(120.0)
            except TypeError:
                pending = 0

        callback_count = len(outcomes)
        unresolved = dispatched - callback_count
        success = not application_failed and unresolved == 0 and callback_count == len(records)
        status = "success" if success else "failed"
        summary = {
            "attempt_id": attempt_id,
            "attempted_events": dispatched,
            "acknowledged_events": callback_count,
            "unresolved_events": unresolved,
            "failures": 1 if application_failed else 0,
            "exit_status": status,
            "completed_at": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        }
        atomic_write_text(summary_path, json.dumps(summary, ensure_ascii=False, allow_nan=False, sort_keys=True) + "\n")
        if not success:
            raise PublishingError("not every event was acknowledged")
        return summary
    finally:
        attempt_handle.close()
