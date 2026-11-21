from datetime import datetime, timedelta
from decimal import Decimal
import random
from uuid import NAMESPACE_URL, uuid5

from faker import Faker

from .contracts import format_utc_timestamp

APP_NAMESPACE = uuid5(NAMESPACE_URL, "pentaho-to-dbt:synthetic-producer:v1")


def event_id(producer_id, scenario_id, entity_type, business_id, mutation_number):
    return str(uuid5(APP_NAMESPACE, "|".join((producer_id, scenario_id, entity_type, str(business_id), str(mutation_number)))))


def mutation_timestamp(base, offsets, entity_type, business_id, mutation_number=1):
    # Offsets are microseconds from the fixed scenario base.
    key = f"{entity_type}:{business_id}:{mutation_number}"
    return base + timedelta(microseconds=offsets.get(key, mutation_number))


def cents_as_decimal(cents):
    return Decimal(cents) / Decimal(100)


def format_money(cents):
    value = cents_as_decimal(cents)
    return format(value, ".2f")


def generate_scenario(config, references, producer_id, ranges, fixed_event_time=None):
    seed = int(config["seed"])
    locale = str(config["locale"])
    faker = Faker(locale)
    faker.seed_instance(seed)
    rng = random.Random(seed)

    scenario_id = str(config["scenario_id"])
    counts = config["counts"]
    base = datetime.fromisoformat(config["mutation_base"].replace("Z", "+00:00"))
    produced_at = fixed_event_time or datetime.fromisoformat(config["produced_at"].replace("Z", "+00:00"))

    if datetime.now(produced_at.tzinfo) < produced_at:
        raise ValueError("fixed produced_at cannot be in the future")

    result = {"customer": [], "product": [], "sales": []}

    customer_count = int(counts["customer"])
    product_count = int(counts["product"])
    sales_count = int(counts["sales"])
    for entity, count in (("customer", customer_count), ("product", product_count), ("sales", sales_count)):
        start, end = ranges[entity]
        if count > end - start + 1:
            raise ValueError(f"{entity} count exceeds allocation range")

    customer_ids = list(range(ranges["customer"][0], ranges["customer"][0] + customer_count))
    product_ids = list(range(ranges["product"][0], ranges["product"][0] + product_count))
    sales_ids = list(range(ranges["sales"][0], ranges["sales"][0] + sales_count))

    mutation_offset = 0
    for customer_id in customer_ids:
        mutation_offset += 1
        region_id = rng.choice(sorted(references["region_ids"]))
        first_name = faker.first_name()
        last_name = faker.last_name()
        result["customer"].append({
            "schema_version": 1,
            "event_id": event_id(producer_id, scenario_id, "customer", customer_id, 1),
            "entity_type": "customer",
            "operation": "upsert",
            "producer_id": producer_id,
            "produced_at": format_utc_timestamp(produced_at),
            "data": {
                "customer_id": customer_id,
                "customer_code": f"CUST-{customer_id}",
                "first_name": first_name,
                "last_name": last_name,
                "email": f"{first_name.lower()}.{customer_id}@example.test",
                "region_id": region_id,
                "is_active": True,
                "source_updated_at": format_utc_timestamp(base + timedelta(microseconds=mutation_offset)),
            },
        })

    for product_id in product_ids:
        mutation_offset += 1
        cents = rng.randint(500, 50000)
        result["product"].append({
            "schema_version": 1,
            "event_id": event_id(producer_id, scenario_id, "product", product_id, 1),
            "entity_type": "product",
            "operation": "upsert",
            "producer_id": producer_id,
            "produced_at": format_utc_timestamp(produced_at),
            "data": {
                "product_id": product_id,
                "product_code": f"PROD-{product_id}",
                "product_name": faker.catch_phrase(),
                "category": rng.choice(["Equipment", "Apparel", "Accessories"]),
                "unit_price": format_money(cents),
                "is_active": True,
                "source_updated_at": format_utc_timestamp(base + timedelta(microseconds=mutation_offset)),
            },
        })

    sales_date = datetime.strptime(str(config["sales_date"]), "%Y-%m-%d").date()
    mutation_date = datetime.fromisoformat(config["mutation_date"].replace("Z", "+00:00")).date()
    if sales_date > mutation_date:
        raise ValueError("sales_date cannot be after mutation_date")

    for sales_id in sales_ids:
        mutation_offset += 1
        customer_id = rng.choice(customer_ids)
        product_id = rng.choice(product_ids)
        store_id = rng.choice(sorted(references["store_ids"]))
        quantity = rng.randint(1, 5)
        unit_cents = rng.randint(500, 50000)
        gross = unit_cents * quantity
        discount = rng.randint(0, min(gross, gross // 4))
        result["sales"].append({
            "schema_version": 1,
            "event_id": event_id(producer_id, scenario_id, "sales", sales_id, 1),
            "entity_type": "sales",
            "operation": "upsert",
            "producer_id": producer_id,
            "produced_at": format_utc_timestamp(produced_at),
            "data": {
                "sales_id": sales_id,
                "order_number": f"ORD-{sales_id}",
                "customer_id": customer_id,
                "product_id": product_id,
                "store_id": store_id,
                "sales_date": sales_date.isoformat(),
                "quantity": quantity,
                "unit_price": format_money(unit_cents),
                "discount_amount": format_money(discount),
                "source_updated_at": format_utc_timestamp(base + timedelta(microseconds=mutation_offset)),
            },
        })

    if base > datetime.now(base.tzinfo):
        raise ValueError("mutation_base cannot be in the future")

    bounds = config.get("bounds", {})
    for entity in ("customer", "product", "sales"):
        entity_bounds = bounds.get(entity)
        if not entity_bounds:
            continue
        lower_bounds = []
        for field in ("committed_watermark", "source_maximum"):
            if entity_bounds.get(field):
                lower_bounds.append(datetime.fromisoformat(entity_bounds[field].replace("Z", "+00:00")))
        if lower_bounds:
            minimum = max(lower_bounds)
            entity_events = result[entity]
            if entity_events:
                first_mutation = datetime.fromisoformat(entity_events[0]["data"]["source_updated_at"].replace("Z", "+00:00"))
                if first_mutation <= minimum:
                    raise ValueError(f"{entity} mutations must be later than captured bounds")
    return result
