from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

EMAIL_PATTERN = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
ORDER_STATUSES = ("pending", "paid", "shipped", "completed", "cancelled", "refunded")
CHANNELS = ("web", "mobile", "marketplace", "store")


def deduplicate_latest(df: DataFrame, keys: Iterable[str], order_column: str) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(F.col(order_column).desc_nulls_last())
    return df.withColumn("_row_number", F.row_number().over(window)).where(
        F.col("_row_number") == 1
    ).drop("_row_number")


def with_validation(df: DataFrame, rules: list[tuple[Column, str]]) -> DataFrame:
    reason = F.lit(None).cast("string")
    valid = F.lit(True)
    for condition, message in rules:
        valid = valid & condition
        reason = F.when(~condition & reason.isNull(), F.lit(message)).otherwise(reason)
    return df.withColumn("_dq_is_valid", valid).withColumn("_dq_reason", reason)


def clean_customers(df: DataFrame) -> DataFrame:
    clean = (
        df.select(
            F.trim("customer_id").alias("customer_id"),
            F.trim("full_name").alias("full_name"),
            F.lower(F.trim("email")).alias("email"),
            F.regexp_replace(F.trim("phone"), r"[^0-9+]", "").alias("phone"),
            F.initcap(F.trim("city")).alias("city"),
            F.upper(F.trim("country")).alias("country"),
            F.to_date("signup_date").alias("signup_date"),
            F.lower(F.trim("loyalty_tier")).alias("loyalty_tier"),
            F.to_timestamp("updated_at").alias("updated_at"),
            "_ingested_at",
            "_source_file",
        )
        .transform(lambda value: deduplicate_latest(value, ["customer_id"], "updated_at"))
    )
    return with_validation(
        clean,
        [
            (F.col("customer_id").isNotNull() & (F.length("customer_id") > 0), "missing_customer_id"),
            (F.col("email").rlike(EMAIL_PATTERN), "invalid_email"),
            (F.col("signup_date").isNotNull(), "invalid_signup_date"),
            (F.col("loyalty_tier").isin("bronze", "silver", "gold", "platinum"), "invalid_loyalty_tier"),
            (F.col("updated_at").isNotNull(), "invalid_updated_at"),
        ],
    )


def clean_products(df: DataFrame) -> DataFrame:
    clean = (
        df.select(
            F.trim("product_id").alias("product_id"),
            F.upper(F.trim("sku")).alias("sku"),
            F.trim("product_name").alias("product_name"),
            F.initcap(F.trim("category")).alias("category"),
            F.initcap(F.trim("brand")).alias("brand"),
            F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
            F.col("cost_price").cast("decimal(18,2)").alias("cost_price"),
            F.lower(F.trim("active")).isin("true", "1", "yes").alias("active"),
            F.to_timestamp("updated_at").alias("updated_at"),
            "_ingested_at",
            "_source_file",
        )
        .transform(lambda value: deduplicate_latest(value, ["product_id"], "updated_at"))
    )
    return with_validation(
        clean,
        [
            (F.col("product_id").isNotNull() & (F.length("product_id") > 0), "missing_product_id"),
            (F.col("sku").isNotNull() & (F.length("sku") > 0), "missing_sku"),
            (F.col("unit_price") > 0, "invalid_unit_price"),
            (F.col("cost_price") >= 0, "invalid_cost_price"),
            (F.col("unit_price") >= F.col("cost_price"), "price_below_cost"),
            (F.col("updated_at").isNotNull(), "invalid_updated_at"),
        ],
    )


def clean_orders(df: DataFrame) -> DataFrame:
    clean = (
        df.select(
            F.trim("order_id").alias("order_id"),
            F.trim("customer_id").alias("customer_id"),
            F.to_timestamp("order_ts").alias("order_ts"),
            F.lower(F.trim("status")).alias("status"),
            F.lower(F.trim("channel")).alias("channel"),
            F.lower(F.trim("payment_method")).alias("payment_method"),
            F.initcap(F.trim("shipping_city")).alias("shipping_city"),
            F.col("subtotal").cast("decimal(18,2)").alias("subtotal"),
            F.col("discount_amount").cast("decimal(18,2)").alias("discount_amount"),
            F.col("shipping_fee").cast("decimal(18,2)").alias("shipping_fee"),
            F.col("total_amount").cast("decimal(18,2)").alias("total_amount"),
            F.to_timestamp("updated_at").alias("updated_at"),
            "_ingested_at",
            "_source_file",
        )
        .transform(lambda value: deduplicate_latest(value, ["order_id"], "updated_at"))
    )
    amount_matches = (
        F.abs(
            F.col("total_amount")
            - (F.col("subtotal") - F.col("discount_amount") + F.col("shipping_fee"))
        )
        <= F.lit(0.02)
    )
    return with_validation(
        clean,
        [
            (F.col("order_id").isNotNull() & (F.length("order_id") > 0), "missing_order_id"),
            (F.col("customer_id").isNotNull(), "missing_customer_id"),
            (F.col("order_ts").isNotNull(), "invalid_order_ts"),
            (F.col("status").isin(*ORDER_STATUSES), "invalid_order_status"),
            (F.col("channel").isin(*CHANNELS), "invalid_channel"),
            (F.col("subtotal") >= 0, "invalid_subtotal"),
            (F.col("discount_amount") >= 0, "invalid_discount"),
            (F.col("shipping_fee") >= 0, "invalid_shipping_fee"),
            (F.col("total_amount") >= 0, "invalid_total_amount"),
            (amount_matches, "order_total_mismatch"),
        ],
    )


def clean_order_items(df: DataFrame) -> DataFrame:
    clean = df.select(
        F.trim("order_item_id").alias("order_item_id"),
        F.trim("order_id").alias("order_id"),
        F.trim("product_id").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
        F.col("discount_amount").cast("decimal(18,2)").alias("discount_amount"),
        F.col("line_amount").cast("decimal(18,2)").alias("line_amount"),
        "_ingested_at",
        "_source_file",
    ).dropDuplicates(["order_item_id"])
    line_matches = (
        F.abs(
            F.col("line_amount")
            - (F.col("quantity") * F.col("unit_price") - F.col("discount_amount"))
        )
        <= F.lit(0.02)
    )
    return with_validation(
        clean,
        [
            (F.col("order_item_id").isNotNull(), "missing_order_item_id"),
            (F.col("order_id").isNotNull(), "missing_order_id"),
            (F.col("product_id").isNotNull(), "missing_product_id"),
            (F.col("quantity") > 0, "invalid_quantity"),
            (F.col("unit_price") >= 0, "invalid_unit_price"),
            (F.col("discount_amount") >= 0, "invalid_discount"),
            (line_matches, "line_amount_mismatch"),
        ],
    )


def clean_customer_profiles(df: DataFrame) -> DataFrame:
    clean = (
        df.select(
            F.trim("customer_id").alias("customer_id"),
            F.lower(F.trim("acquisition_source")).alias("acquisition_source"),
            F.lower(F.trim("marketing_opt_in")).isin("true", "1", "yes").alias("marketing_opt_in"),
            F.initcap(F.trim("preferred_category")).alias("preferred_category"),
            F.to_timestamp("last_contact_at").alias("last_contact_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
            "_ingested_at",
            "_source_file",
        )
        .transform(lambda value: deduplicate_latest(value, ["customer_id"], "updated_at"))
    )
    return with_validation(
        clean,
        [
            (F.col("customer_id").isNotNull(), "missing_customer_id"),
            (F.col("updated_at").isNotNull(), "invalid_updated_at"),
        ],
    )


def clean_support_tickets(df: DataFrame) -> DataFrame:
    clean = df.select(
        F.trim("ticket_id").alias("ticket_id"),
        F.trim("customer_id").alias("customer_id"),
        F.to_timestamp("created_at").alias("created_at"),
        F.to_timestamp("resolved_at").alias("resolved_at"),
        F.lower(F.trim("status")).alias("status"),
        F.lower(F.trim("priority")).alias("priority"),
        F.lower(F.trim("topic")).alias("topic"),
        F.col("satisfaction_score").cast("int").alias("satisfaction_score"),
        "_ingested_at",
        "_source_file",
    ).dropDuplicates(["ticket_id"])
    resolved_consistent = (
        (F.col("status") == "resolved") & F.col("resolved_at").isNotNull()
    ) | ((F.col("status") != "resolved") & F.col("resolved_at").isNull())
    return with_validation(
        clean,
        [
            (F.col("ticket_id").isNotNull(), "missing_ticket_id"),
            (F.col("customer_id").isNotNull(), "missing_customer_id"),
            (F.col("created_at").isNotNull(), "invalid_created_at"),
            (F.col("status").isin("open", "in_progress", "resolved"), "invalid_ticket_status"),
            (F.col("priority").isin("low", "medium", "high", "urgent"), "invalid_priority"),
            (resolved_consistent, "resolved_timestamp_mismatch"),
            (
                F.col("satisfaction_score").isNull()
                | F.col("satisfaction_score").between(1, 5),
                "invalid_satisfaction_score",
            ),
        ],
    )


def parse_stream_payload(df: DataFrame, schema: StructType) -> DataFrame:
    parsed = df.withColumn("_payload", F.from_json("raw_payload", schema))
    payload_columns = [F.col(f"_payload.{field.name}").alias(field.name) for field in schema.fields]
    return parsed.select(
        *payload_columns,
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
        "_ingested_at",
        "raw_payload",
    )


def validate_clickstream(df: DataFrame) -> DataFrame:
    return with_validation(
        df.dropDuplicates(["event_id"]),
        [
            (F.col("event_id").isNotNull(), "missing_event_id"),
            (F.col("event_ts").isNotNull(), "invalid_event_ts"),
            (F.col("session_id").isNotNull(), "missing_session_id"),
            (
                F.col("event_type").isin(
                    "page_view", "product_view", "add_to_cart", "checkout", "purchase"
                ),
                "invalid_event_type",
            ),
        ],
    )


def validate_iot(df: DataFrame) -> DataFrame:
    return with_validation(
        df.dropDuplicates(["event_id"]),
        [
            (F.col("event_id").isNotNull(), "missing_event_id"),
            (F.col("event_ts").isNotNull(), "invalid_event_ts"),
            (F.col("device_id").isNotNull(), "missing_device_id"),
            (F.col("warehouse_id").isNotNull(), "missing_warehouse_id"),
            (F.col("temperature_c").between(-30, 70), "temperature_out_of_range"),
            (F.col("humidity_pct").between(0, 100), "humidity_out_of_range"),
            (F.col("battery_pct").between(0, 100), "battery_out_of_range"),
        ],
    )

