import json

import pytest

from synthetic_producer.publisher import PublishingError, publish_records


class FakeProducer:
    def __init__(self, fail=False, queue_full=False):
        self.fail = fail
        self.queue_full = queue_full
        self.sent = []

    def produce(self, topic, key, value, on_delivery):
        if self.queue_full:
            raise BufferError("queue full")
        self.sent.append((topic, key, value))
        event_id = json.loads(value)["event_id"]
        on_delivery(RuntimeError("delivery failed") if self.fail else None, FakeMessage(topic, key))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


class FakeMessage:
    def __init__(self, topic, key):
        self._topic = topic
        self._key = key

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def partition(self):
        return 0

    def offset(self):
        return 123


def test_delivery_error_is_visible(tmp_path):
    event = {
        "topic": "pentaho.project1.customer.v1",
        "key": "100001",
        "value": {
            "schema_version": 1,
            "event_id": "e1",
            "entity_type": "customer",
            "operation": "upsert",
            "producer_id": "dev-producer-01",
            "produced_at": "2026-11-20T12:30:00.000000Z",
            "data": {
                "customer_id": 100001,
                "customer_code": "CUST-100001",
                "first_name": "Alex",
                "last_name": "Morgan",
                "email": "alex.100001@example.test",
                "region_id": 10,
                "is_active": True,
                "source_updated_at": "2026-11-20T12:00:00.000000Z",
            },
        },
    }
    manifest = {
        "per_topic_counts": {"customer": 1, "product": 0, "sales": 0},
        "expected_event_ids": ["e1"],
        "_run_dir": str(tmp_path),
    }
    with pytest.raises(PublishingError):
        publish_records([event], manifest, {"region_ids": [10, 20], "store_ids": [101, 102]}, FakeProducer(fail=True), max_events_per_second=0)
