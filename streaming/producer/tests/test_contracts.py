import pytest

from synthetic_producer.contracts import validate_event


REFERENCES = {"region_ids": [10, 20], "store_ids": [101, 102]}


def customer_event():
    return {
        "schema_version": 1,
        "event_id": "f1a3a800-0000-4000-8000-000000000001",
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
    }


def test_contract_accepts_complete_customer():
    assert validate_event(customer_event(), REFERENCES) == "100001"


def test_contract_rejects_boolean_id():
    event = customer_event()
    event["data"]["customer_id"] = True
    with pytest.raises(ValueError):
        validate_event(event, REFERENCES)


def test_contract_rejects_extra_field():
    event = customer_event()
    event["data"]["extra"] = "x"
    with pytest.raises(ValueError):
        validate_event(event, REFERENCES)


def test_contract_rejects_unknown_region():
    event = customer_event()
    event["data"]["region_id"] = 99
    with pytest.raises(ValueError):
        validate_event(event, REFERENCES)
