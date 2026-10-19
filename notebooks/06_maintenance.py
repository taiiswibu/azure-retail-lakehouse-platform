# Databricks notebook source
# MAGIC %md
# MAGIC # Delta maintenance
# MAGIC Run manually after several demo loads. VACUUM keeps the safe seven-day retention.

# COMMAND ----------

from retail_lakehouse.config import LakehouseConfig

dbutils.widgets.text("catalog", "retail_dev")
config = LakehouseConfig(catalog=dbutils.widgets.get("catalog"))

tables = [
    ("silver", "customers"),
    ("silver", "products"),
    ("silver", "orders"),
    ("silver", "order_items"),
    ("gold", "fact_sales"),
    ("gold", "mart_sales_daily"),
]

for layer, name in tables:
    table = config.table(layer, name)
    if spark.catalog.tableExists(table):
        spark.sql(f"OPTIMIZE {table}")
        spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
        print(f"Maintained {table}")

