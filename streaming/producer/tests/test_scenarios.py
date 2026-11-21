import json

from synthetic_producer.config import validate_allocations, validate_reference_manifest
from synthetic_producer.generators import generate_scenario


def base_config(tmp_path):
    references = validate_reference_manifest({
        "regions": [{"region_id": 10}, {"region_id": 20}],
        "stores": [{"store_id": 101}, {"store_id": 102}],
    })
    producer_id, ranges = validate_allocations({
        "producer_id": "dev-producer-01",
        "customer": {"start": 100001, "end": 109999},
        "product": {"start": 200001, "end": 209999},
        "sales": {"start": 500001, "end": 509999},
    })
    config = {
        "scenario_id": "test-001",
        "scenario_type": "baseline",
        "producer_id": producer_id,
        "seed": 1907,
        "locale": "en_US",
        "counts": {"customer": 2, "product": 2, "sales": 5},
        "bounds": {},
        "mutation_base": "2026-11-20T12:00:00.000000Z",
        "produced_at": "2026-11-20T12:30:00.000000Z",
        "mutation_date": "2026-11-20T12:30:00.000000Z",
        "sales_date": "2026-11-20",
    }
    return config, references, producer_id, ranges


def test_fixed_input_reproducibility():
    config, references, producer_id, ranges = base_config(None)
    first = generate_scenario(config, references, producer_id, ranges)
    second = generate_scenario(config, references, producer_id, ranges)
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_allocation_range_exhaustion():
    config, references, producer_id, ranges = base_config(None)
    config["counts"] = {"customer": 3, "product": 2, "sales": 1}
    ranges = dict(ranges)
    ranges["customer"] = (100001, 100002)
    try:
        generate_scenario(config, references, producer_id, ranges)
        raise AssertionError("expected range exhaustion")
    except ValueError:
        pass
