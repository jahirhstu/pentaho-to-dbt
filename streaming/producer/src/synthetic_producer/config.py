import json
from pathlib import Path


def load_json(path):
    path = Path(path)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_reference_manifest(config):
    regions = config.get("regions")
    stores = config.get("stores")
    if not isinstance(regions, list) or not isinstance(stores, list):
        raise ValueError("reference manifest requires regions and stores lists")
    if not regions or not stores:
        raise ValueError("reference manifest cannot be empty")
    region_ids = {int(row["region_id"]) for row in regions}
    store_ids = {int(row["store_id"]) for row in stores}
    if region_ids & set():
        raise ValueError("invalid region data")
    return {"region_ids": sorted(region_ids), "store_ids": sorted(store_ids)}


def validate_allocations(config):
    required = {"producer_id", "customer", "product", "sales"}
    if not isinstance(config, dict) or set(config) & required != required:
        raise ValueError("allocation manifest is incomplete")
    producer_id = str(config["producer_id"])
    if not producer_id.strip():
        raise ValueError("producer_id cannot be empty")

    ranges = {}
    for entity in ("customer", "product", "sales"):
        row = config[entity]
        if not isinstance(row, dict) or set(row) != {"start", "end"}:
            raise ValueError(f"invalid {entity} allocation")
        start = int(row["start"])
        end = int(row["end"])
        if start <= 0 or end < start:
            raise ValueError(f"invalid {entity} allocation range")
        ranges[entity] = (start, end)

    for entity in ("customer", "product", "sales"):
        for other in ("customer", "product", "sales"):
            if entity != other and ranges[entity][0] == ranges[other][0]:
                raise ValueError("entity ranges must not share a start")
    return producer_id, ranges
