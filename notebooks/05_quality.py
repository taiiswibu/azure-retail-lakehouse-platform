# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality gates
# MAGIC Persists test results and fails the job when a critical contract is violated.

# COMMAND ----------

import uuid

from retail_lakehouse.config import LakehouseConfig
from retail_lakehouse.quality import (
    null_rate_check,
    orphan_rate_check,
    row_count_check,
    uniqueness_check,
    write_results,
)

dbutils.widgets.text("catalog", "retail_dev")
config = LakehouseConfig(catalog=dbutils.widgets.get("catalog"))
run_id = str(uuid.uuid4())

customers = spark.table(config.table("silver", "customers"))
products = spark.table(config.table("silver", "products"))
orders = spark.table(config.table("silver", "orders"))
items = spark.table(config.table("silver", "order_items"))
fact_sales = spark.table(config.table("gold", "fact_sales"))

results = [
    row_count_check(customers, "silver.customers", 1),
    row_count_check(products, "silver.products", 1),
    row_count_check(orders, "silver.orders", 1),
    row_count_check(items, "silver.order_items", 1),
    row_count_check(fact_sales, "gold.fact_sales", 1),
    null_rate_check(customers, "silver.customers", "customer_id"),
    null_rate_check(products, "silver.products", "product_id"),
    null_rate_check(orders, "silver.orders", "order_id"),
    uniqueness_check(customers, "silver.customers", ["customer_id"]),
    uniqueness_check(products, "silver.products", ["product_id"]),
    uniqueness_check(orders, "silver.orders", ["order_id"]),
    uniqueness_check(items, "silver.order_items", ["order_item_id"]),
    orphan_rate_check(orders, customers, "silver.orders", "customer_id", "customer_id"),
    orphan_rate_check(items, orders, "silver.order_items", "order_id", "order_id"),
    orphan_rate_check(items, products, "silver.order_items", "product_id", "product_id"),
]

write_results(spark, results, config.table("ops", "data_quality_results"), run_id)

failed = [result for result in results if result.status == "FAIL"]
for result in results:
    print(f"{result.status:4} | {result.table_name:25} | {result.check_name:35} | {result.observed_value}")
if failed:
    raise RuntimeError(f"{len(failed)} critical data-quality checks failed; run_id={run_id}")

