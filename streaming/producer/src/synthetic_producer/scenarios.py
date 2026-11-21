from datetime import datetime, timedelta

from .contracts import validate_event
from .generators import event_id, generate_scenario


def build_events(config, references, producer_id, ranges):
    allowed_scenarios = {"baseline", "sales_update", "lookup_update", "missing_customer", "missing_product", "stale_version", "equal_version_conflict", "invalid_contract", "two_producers"}
    name = config.get("scenario_type")
    if name not in allowed_scenarios:
        raise ValueError("unknown scenario name")

    counts = config["counts"]
    if name == "baseline" and counts != {"customer": 2, "product": 2, "sales": 5}:
        raise ValueError("baseline requires exactly 2 customers, 2 products, and 5 sales")

    invalid_contract = name == "invalid_contract"
    if invalid_contract and not config.get("allow_negative_fixture"):
        raise ValueError("invalid_contract scenarios require allow_negative_fixture=true")

    events = generate_scenario(config, references, producer_id, ranges)
    flattened = events["customer"] + events["product"] + events["sales"]

    if name == "sales_update" and flattened:
        sale = next(event for event in flattened if event["entity_type"] == "sales")
        timestamp = datetime.fromisoformat(sale["data"]["source_updated_at"].replace("Z", "+00:00"))
        updated = dict(sale)
        updated["event_id"] = event_id(
            sale["producer_id"], config["scenario_id"], "sales",
            sale["data"]["sales_id"], 2,
        )
        updated["data"] = dict(sale["data"])
        updated["data"]["quantity"] = sale["data"]["quantity"] + 1
        updated["data"]["source_updated_at"] = (
            timestamp + timedelta(microseconds=1)
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")
        flattened.append(updated)

    if name == "invalid_contract":
        sale = flattened[-1]
        sale["data"]["store_id"] = -1

    if invalid_contract:
        return flattened
    for event in flattened:
        validate_event(event, references)
    return flattened
