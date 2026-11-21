from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re

TOPICS = {
    "customer": "pentaho.project1.customer.v1",
    "product": "pentaho.project1.product.v1",
    "sales": "pentaho.project1.sales.v1",
}

BIGINT_MIN = -(2**63)
BIGINT_MAX = 2**63 - 1
INT_MIN = -(2**31)
INT_MAX = 2**31 - 1

PAYLOAD_FIELDS = {
    "customer": {"customer_id", "customer_code", "first_name", "last_name", "email", "region_id", "is_active", "source_updated_at"},
    "product": {"product_id", "product_code", "product_name", "category", "unit_price", "is_active", "source_updated_at"},
    "sales": {"sales_id", "order_number", "customer_id", "product_id", "store_id", "sales_date", "quantity", "unit_price", "discount_amount", "source_updated_at"},
}
ENVELOPE_FIELDS = {"schema_version", "event_id", "entity_type", "operation", "producer_id", "produced_at", "data"}


def require_int(value, name, minimum=None, maximum=None):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} is below {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} exceeds {maximum}")
    return value


def require_bigint(value, name, positive=False):
    result = require_int(value, name, BIGINT_MIN, BIGINT_MAX)
    if positive and result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def require_nonblank(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonblank string")
    return value.strip()


def parse_utc_timestamp(value, name):
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    if not re.search(r"(?:Z|[+-]\d{2}:\d{2})$", value):
        raise ValueError(f"{name} must include a timezone")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    if parsed.microsecond and len(value.rsplit(".", 1)[-1].rstrip("Z+")) > 6:
        raise ValueError(f"{name} exceeds microsecond precision")
    return parsed.astimezone(timezone.utc)


def format_utc_timestamp(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_money(value, name, max_integral_digits=10):
    if not isinstance(value, str) or not re.fullmatch(r"\d+\.\d{2}", value):
        raise ValueError(f"{name} must be a decimal string with exactly two places")
    try:
        amount = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} is invalid") from exc
    if amount < 0:
        raise ValueError(f"{name} must be nonnegative")
    integral_digits = len(value.split(".", 1)[0].lstrip("0")) or 1
    if integral_digits > max_integral_digits:
        raise ValueError(f"{name} exceeds DECIMAL(10,2) bounds")
    if amount.as_tuple().exponent != -2:
        raise ValueError(f"{name} must have exactly two fractional digits")
    return amount


def validate_payload(entity_type, data, references=None):
    if not isinstance(data, dict):
        raise ValueError("data must be an object")
    if set(data) != PAYLOAD_FIELDS[entity_type]:
        raise ValueError(f"{entity_type} data fields must exactly match the v1 contract")

    if entity_type == "customer":
        require_bigint(data["customer_id"], "customer_id", positive=True)
        require_nonblank(data["customer_code"], "customer_code")
        require_nonblank(data["first_name"], "first_name")
        require_nonblank(data["last_name"], "last_name")
        email = require_nonblank(data["email"], "email").lower()
        if not email.endswith("@example.test"):
            raise ValueError("customer email must use example.test")
        require_bigint(data["region_id"], "region_id", positive=True)
        if not isinstance(data["is_active"], bool):
            raise ValueError("is_active must be a boolean")
        parse_utc_timestamp(data["source_updated_at"], "source_updated_at")
        if references and data["region_id"] not in references["region_ids"]:
            raise ValueError("unknown region_id")

    elif entity_type == "product":
        require_bigint(data["product_id"], "product_id", positive=True)
        require_nonblank(data["product_code"], "product_code")
        require_nonblank(data["product_name"], "product_name")
        require_nonblank(data["category"], "category")
        parse_money(data["unit_price"], "unit_price")
        if not isinstance(data["is_active"], bool):
            raise ValueError("is_active must be a boolean")
        parse_utc_timestamp(data["source_updated_at"], "source_updated_at")

    else:
        require_bigint(data["sales_id"], "sales_id", positive=True)
        require_nonblank(data["order_number"], "order_number")
        require_bigint(data["customer_id"], "customer_id", positive=True)
        require_bigint(data["product_id"], "product_id", positive=True)
        require_bigint(data["store_id"], "store_id", positive=True)
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", data["sales_date"]):
            raise ValueError("sales_date must use YYYY-MM-DD")
        datetime.strptime(data["sales_date"], "%Y-%m-%d")
        quantity = require_int(data["quantity"], "quantity", 1, INT_MAX)
        unit_price = parse_money(data["unit_price"], "unit_price")
        discount = parse_money(data["discount_amount"], "discount_amount")
        gross = unit_price * quantity
        net = gross - discount
        if discount > gross:
            raise ValueError("discount_amount exceeds gross amount")
        if net < 0:
            raise ValueError("invalid net amount")
        if net != net.quantize(Decimal("0.01")):
            raise ValueError("net amount must have two decimal places")
        if len(format(net, "f").partition(".")[0]) > 12:
            raise ValueError("net amount exceeds DECIMAL(12,2)")
        parse_utc_timestamp(data["source_updated_at"], "source_updated_at")
        if references and data["store_id"] not in references["store_ids"]:
            raise ValueError("unknown store_id")


def validate_event(event, references=None):
    if not isinstance(event, dict) or set(event) != ENVELOPE_FIELDS:
        raise ValueError("event envelope fields must exactly match the v1 contract")
    if event["schema_version"] != 1 or isinstance(event["schema_version"], bool):
        raise ValueError("schema_version must be integer 1")
    require_nonblank(event["event_id"], "event_id")
    require_nonblank(event["producer_id"], "producer_id")
    if event["entity_type"] not in TOPICS:
        raise ValueError("unknown entity_type")
    if event["operation"] != "upsert":
        raise ValueError("operation must be upsert")
    parse_utc_timestamp(event["produced_at"], "produced_at")
    validate_payload(event["entity_type"], event["data"], references)
    data = event["data"]
    key_id = data[f'{event["entity_type"]}_id']
    return str(key_id)
