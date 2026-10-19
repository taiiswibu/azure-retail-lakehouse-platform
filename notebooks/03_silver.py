# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver
# MAGIC Casts types, deduplicates, validates business rules, quarantines invalid records,
# MAGIC and applies idempotent Delta `MERGE` operations.

# COMMAND ----------

import uuid
from datetime import UTC, datetime

from pyspark.sql import functions as F

from retail_lakehouse.config import LakehouseConfig
from retail_lakehouse.delta_utils import merge_scd1
from retail_lakehouse.schemas import CLICKSTREAM_SCHEMA, IOT_SCHEMA
from retail_lakehouse.transforms import (
    clean_customer_profiles,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
    clean_support_tickets,
    parse_stream_payload,
    validate_clickstream,
    validate_iot,
)

dbutils.widgets.text("catalog", "retail_dev")
config = LakehouseConfig(catalog=dbutils.widgets.get("catalog"))
run_id = str(uuid.uuid4())


def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


def quarantine_and_merge(source_name, transform, keys, update_condition=None):
    bronze_table = config.table("bronze", source_name)
    if not table_exists(bronze_table):
        print(f"Skipping {source_name}: bronze table does not exist")
        return

    transformed = transform(spark.table(bronze_table))
    invalid = transformed.where(~F.col("_dq_is_valid"))
    if invalid.limit(1).count() > 0:
        payload_columns = [
            column for column in invalid.columns if column not in {"_dq_is_valid", "_dq_reason"}
        ]
        quarantine = invalid.select(
            F.lit(run_id).alias("run_id"),
            F.lit(source_name).alias("source_table"),
            F.col("_dq_reason").alias("reason"),
            F.to_json(F.struct(*[F.col(column) for column in payload_columns])).alias("raw_record"),
            F.current_timestamp().alias("quarantined_at"),
        )
        quarantine.write.format("delta").mode("append").saveAsTable(
            config.table("ops", "quarantine_records")
        )

    valid = transformed.where(F.col("_dq_is_valid")).drop("_dq_is_valid", "_dq_reason")
    merge_scd1(
        spark,
        valid,
        config.table("silver", source_name),
        keys,
        update_condition=update_condition,
    )
    print(f"Processed {source_name}")


# COMMAND ----------

quarantine_and_merge("customers", clean_customers, ["customer_id"], "source.updated_at >= target.updated_at")
quarantine_and_merge("products", clean_products, ["product_id"], "source.updated_at >= target.updated_at")
quarantine_and_merge("orders", clean_orders, ["order_id"], "source.updated_at >= target.updated_at")
quarantine_and_merge("order_items", clean_order_items, ["order_item_id"])
quarantine_and_merge(
    "customer_profiles",
    clean_customer_profiles,
    ["customer_id"],
    "source.updated_at >= target.updated_at",
)
quarantine_and_merge("support_tickets", clean_support_tickets, ["ticket_id"])

# COMMAND ----------

if table_exists(config.table("bronze", "clickstream_events")):
    quarantine_and_merge(
        "clickstream_events",
        lambda df: validate_clickstream(parse_stream_payload(df, CLICKSTREAM_SCHEMA)),
        ["event_id"],
    )

if table_exists(config.table("bronze", "iot_telemetry")):
    quarantine_and_merge(
        "iot_telemetry",
        lambda df: validate_iot(parse_stream_payload(df, IOT_SCHEMA)),
        ["event_id"],
    )

# COMMAND ----------

customers_table = config.table("silver", "customers")
profiles_table = config.table("silver", "customer_profiles")
if table_exists(customers_table) and table_exists(profiles_table):
    enriched = (
        spark.table(customers_table)
        .alias("customers")
        .join(spark.table(profiles_table).alias("profiles"), "customer_id", "left")
        .select(
            "customer_id",
            "full_name",
            "email",
            "phone",
            "city",
            "country",
            "signup_date",
            "loyalty_tier",
            "acquisition_source",
            "marketing_opt_in",
            "preferred_category",
            "last_contact_at",
            F.greatest("customers.updated_at", "profiles.updated_at").alias("updated_at"),
            F.current_timestamp().alias("_refreshed_at"),
        )
    )
    merge_scd1(
        spark,
        enriched,
        config.table("silver", "customers_enriched"),
        ["customer_id"],
        "source.updated_at >= target.updated_at",
    )

print(f"Silver run completed: {run_id} at {datetime.now(UTC).isoformat()}")

