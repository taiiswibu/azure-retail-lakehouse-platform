# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold
# MAGIC Builds conformed dimensions, facts, and business data marts for BI.

# COMMAND ----------

from pyspark.sql import functions as F

from retail_lakehouse.config import LakehouseConfig
from retail_lakehouse.delta_utils import apply_scd2, overwrite_table

dbutils.widgets.text("catalog", "retail_dev")
config = LakehouseConfig(catalog=dbutils.widgets.get("catalog"))


def exists(layer: str, name: str) -> bool:
    return spark.catalog.tableExists(config.table(layer, name))


required = ["customers_enriched", "products", "orders", "order_items"]
missing = [name for name in required if not exists("silver", name)]
if missing:
    raise RuntimeError(f"Missing required Silver tables: {', '.join(missing)}")

# COMMAND ----------

customer_source = spark.table(config.table("silver", "customers_enriched")).select(
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
    "updated_at",
)
apply_scd2(
    spark,
    customer_source,
    config.table("gold", "dim_customer"),
    natural_key="customer_id",
    tracked_columns=[
        "full_name",
        "email",
        "phone",
        "city",
        "loyalty_tier",
        "acquisition_source",
        "marketing_opt_in",
        "preferred_category",
    ],
    effective_column="updated_at",
)

products = spark.table(config.table("silver", "products")).select(
    F.sha2("product_id", 256).alias("product_sk"),
    "product_id",
    "sku",
    "product_name",
    "category",
    "brand",
    "unit_price",
    "cost_price",
    "active",
    "updated_at",
)
overwrite_table(products, config.table("gold", "dim_product"))

# COMMAND ----------

orders = spark.table(config.table("silver", "orders"))
date_bounds = orders.agg(
    F.coalesce(F.min(F.to_date("order_ts")), F.date_sub(F.current_date(), 365)).alias("start_date"),
    F.coalesce(F.max(F.to_date("order_ts")), F.current_date()).alias("end_date"),
).first()
dates = spark.sql(
    f"SELECT explode(sequence(date('{date_bounds.start_date}'), date('{date_bounds.end_date}'), interval 1 day)) AS date"
).select(
    F.date_format("date", "yyyyMMdd").cast("int").alias("date_sk"),
    "date",
    F.year("date").alias("year"),
    F.quarter("date").alias("quarter"),
    F.month("date").alias("month"),
    F.date_format("date", "MMMM").alias("month_name"),
    F.weekofyear("date").alias("week_of_year"),
    F.dayofweek("date").alias("day_of_week"),
    F.date_format("date", "EEEE").alias("day_name"),
    F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
)
overwrite_table(dates, config.table("gold", "dim_date"))

# COMMAND ----------

items = spark.table(config.table("silver", "order_items"))
customers = spark.table(config.table("gold", "dim_customer"))
dim_products = spark.table(config.table("gold", "dim_product"))

fact_sales = (
    orders.alias("orders")
    .join(items.alias("items"), "order_id")
    .join(dim_products.alias("products"), "product_id", "left")
    .join(
        customers.alias("customers"),
        (F.col("orders.customer_id") == F.col("customers.customer_id"))
        & (F.col("orders.order_ts") >= F.col("customers.valid_from"))
        & (
            F.col("customers.valid_to").isNull()
            | (F.col("orders.order_ts") < F.col("customers.valid_to"))
        ),
        "left",
    )
    .select(
        F.sha2("order_item_id", 256).alias("sales_sk"),
        "order_item_id",
        "order_id",
        F.col("customers.customer_sk"),
        F.col("products.product_sk"),
        F.date_format("order_ts", "yyyyMMdd").cast("int").alias("date_sk"),
        "order_ts",
        "status",
        "channel",
        "payment_method",
        "shipping_city",
        "quantity",
        F.col("items.unit_price").alias("unit_price"),
        (F.col("items.quantity") * F.col("items.unit_price")).cast("decimal(18,2)").alias("gross_amount"),
        F.col("items.discount_amount").alias("discount_amount"),
        F.col("items.line_amount").alias("net_amount"),
        F.when(F.col("status").isin("paid", "shipped", "completed"), F.col("items.line_amount"))
        .otherwise(F.lit(0))
        .cast("decimal(18,2)")
        .alias("recognized_revenue"),
        F.current_timestamp().alias("_refreshed_at"),
    )
)
overwrite_table(fact_sales, config.table("gold", "fact_sales"), ["date_sk"])

# COMMAND ----------

sales_daily = fact_sales.groupBy("date_sk", "channel").agg(
    F.countDistinct("order_id").alias("orders"),
    F.sum("quantity").alias("units"),
    F.sum("gross_amount").alias("gross_revenue"),
    F.sum("discount_amount").alias("discount_amount"),
    F.sum("recognized_revenue").alias("net_revenue"),
    (F.sum("recognized_revenue") / F.countDistinct("order_id")).alias("average_order_value"),
)
overwrite_table(sales_daily, config.table("gold", "mart_sales_daily"), ["date_sk"])

customer_360 = (
    fact_sales.groupBy("customer_sk")
    .agg(
        F.countDistinct("order_id").alias("lifetime_orders"),
        F.sum("recognized_revenue").alias("lifetime_value"),
        F.max("order_ts").alias("last_order_at"),
        F.avg("net_amount").alias("average_item_value"),
    )
    .join(
        customers.where("is_current = true").select(
            "customer_sk",
            "customer_id",
            "full_name",
            "city",
            "loyalty_tier",
            "acquisition_source",
            "marketing_opt_in",
            "preferred_category",
        ),
        "customer_sk",
        "left",
    )
)
overwrite_table(customer_360, config.table("gold", "mart_customer_360"))

# COMMAND ----------

if exists("silver", "clickstream_events"):
    clickstream = spark.table(config.table("silver", "clickstream_events"))
    funnel = (
        clickstream.groupBy(F.to_date("event_ts").alias("event_date"), "device_type")
        .pivot("event_type", ["page_view", "product_view", "add_to_cart", "checkout", "purchase"])
        .count()
        .na.fill(0)
        .withColumn("view_to_cart_rate", F.col("add_to_cart") / F.greatest(F.col("product_view"), F.lit(1)))
        .withColumn("checkout_conversion_rate", F.col("purchase") / F.greatest(F.col("checkout"), F.lit(1)))
    )
    overwrite_table(funnel, config.table("gold", "mart_digital_funnel"), ["event_date"])

if exists("silver", "iot_telemetry"):
    telemetry = spark.table(config.table("silver", "iot_telemetry"))
    health = (
        telemetry.groupBy(
            F.window("event_ts", "1 hour").alias("time_window"), "warehouse_id", "device_id"
        )
        .agg(
            F.avg("temperature_c").alias("avg_temperature_c"),
            F.max("temperature_c").alias("max_temperature_c"),
            F.avg("humidity_pct").alias("avg_humidity_pct"),
            F.min("battery_pct").alias("min_battery_pct"),
            F.sum(F.when(F.col("is_door_open"), 1).otherwise(0)).alias("door_open_events"),
            F.count("*").alias("telemetry_events"),
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "warehouse_id",
            "device_id",
            "avg_temperature_c",
            "max_temperature_c",
            "avg_humidity_pct",
            "min_battery_pct",
            "door_open_events",
            "telemetry_events",
        )
        .dropDuplicates(["window_start", "warehouse_id", "device_id"])
        .withColumn("event_date", F.to_date("window_start"))
        .withColumn(
            "health_status",
            F.when((F.col("max_temperature_c") > 32) | (F.col("min_battery_pct") < 15), "alert")
            .when((F.col("max_temperature_c") > 29) | (F.col("min_battery_pct") < 30), "warning")
            .otherwise("healthy"),
        )
    )
    overwrite_table(health, config.table("gold", "mart_inventory_health"), ["event_date"])

print("Gold dimensional model and marts refreshed")

