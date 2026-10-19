# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap Unity Catalog objects
# MAGIC Run once as a metastore administrator after the Azure infrastructure deployment.

# COMMAND ----------

from retail_lakehouse.config import (
    require_identifier,
    require_storage_account,
)

dbutils.widgets.text("catalog", "retail_dev")
dbutils.widgets.text("storage_account_name", "")

catalog = require_identifier(dbutils.widgets.get("catalog"), "catalog")
storage_account = require_storage_account(dbutils.widgets.get("storage_account_name"))

credential = f"{catalog}_storage_credential"
external_location = f"{catalog}_lakehouse_location"
root_url = f"abfss://lakehouse@{storage_account}.dfs.core.windows.net"

# COMMAND ----------
spark.sql(
    f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS `{external_location}`
    URL '{root_url}'
    WITH (STORAGE CREDENTIAL `{credential}`)
    COMMENT 'ADLS Gen2 root for the retail lakehouse'
    """
)

spark.sql(
    f"""
    CREATE CATALOG IF NOT EXISTS `{catalog}`
    MANAGED LOCATION '{root_url}/managed'
    COMMENT 'E-commerce lakehouse portfolio catalog'
    """
)

for schema_name in ("landing", "checkpoints", "bronze", "silver", "gold", "ops"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema_name}`")

spark.sql(
    f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS `{catalog}`.`landing`.`files`
    LOCATION '{root_url}/landing'
    COMMENT 'Inbound batch source files'
    """
)
spark.sql(
    f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS `{catalog}`.`checkpoints`.`state`
    LOCATION '{root_url}/checkpoints'
    COMMENT 'Auto Loader schemas and Structured Streaming checkpoints'
    """
)

print(f"Unity Catalog bootstrap completed for {catalog}")

