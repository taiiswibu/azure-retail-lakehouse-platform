from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _string_schema(*columns: str) -> StructType:
    return StructType([StructField(column, StringType(), True) for column in columns])


BATCH_SCHEMAS: dict[str, StructType] = {
    "customers": _string_schema(
        "customer_id",
        "full_name",
        "email",
        "phone",
        "city",
        "country",
        "signup_date",
        "loyalty_tier",
        "updated_at",
    ),
    "products": _string_schema(
        "product_id",
        "sku",
        "product_name",
        "category",
        "brand",
        "unit_price",
        "cost_price",
        "active",
        "updated_at",
    ),
    "orders": _string_schema(
        "order_id",
        "customer_id",
        "order_ts",
        "status",
        "channel",
        "payment_method",
        "shipping_city",
        "subtotal",
        "discount_amount",
        "shipping_fee",
        "total_amount",
        "updated_at",
    ),
    "order_items": _string_schema(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "line_amount",
    ),
    "customer_profiles": _string_schema(
        "customer_id",
        "acquisition_source",
        "marketing_opt_in",
        "preferred_category",
        "last_contact_at",
        "updated_at",
    ),
    "support_tickets": _string_schema(
        "ticket_id",
        "customer_id",
        "created_at",
        "resolved_at",
        "status",
        "priority",
        "topic",
        "satisfaction_score",
    ),
}


CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("session_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("anonymous_id", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("page", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("quantity", IntegerType(), True),
    ]
)


IOT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("device_id", StringType(), False),
        StructField("warehouse_id", StringType(), False),
        StructField("sensor_type", StringType(), False),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity_pct", DoubleType(), True),
        StructField("battery_pct", DoubleType(), True),
        StructField("is_door_open", BooleanType(), True),
    ]
)


def batch_schema(name: str) -> StructType:
    try:
        return BATCH_SCHEMAS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown batch contract: {name}") from exc

