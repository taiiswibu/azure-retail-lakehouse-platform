# Databricks notebook source
# MAGIC %md
# MAGIC # Landing to Bronze
# MAGIC Incremental file ingestion with Auto Loader. `availableNow` drains new files and then
# MAGIC stops the job cluster, which is appropriate for an on-demand portfolio demo.

# COMMAND ----------

from dataclasses import dataclass

from pyspark.sql import functions as F

from retail_lakehouse.config import LakehouseConfig
from retail_lakehouse.schemas import batch_schema

dbutils.widgets.text("catalog", "retail_dev")
config = LakehouseConfig(catalog=dbutils.widgets.get("catalog"))


@dataclass(frozen=True)
class LandingDataset:
    name: str
    relative_path: str
    file_format: str


datasets = [
    LandingDataset("customers", "erp/customers", "csv"),
    LandingDataset("products", "erp/products", "csv"),
    LandingDataset("orders", "erp/orders", "csv"),
    LandingDataset("order_items", "erp/order_items", "csv"),
    LandingDataset("customer_profiles", "crm/customer_profiles", "json"),
    LandingDataset("support_tickets", "crm/support_tickets", "json"),
]

# COMMAND ----------

for dataset in datasets:
    source_path = f"{config.landing_volume}/{dataset.relative_path}"
    checkpoint = f"{config.state_volume}/autoloader/{dataset.name}/checkpoint"
    schema_path = f"{config.state_volume}/autoloader/{dataset.name}/schema"

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", dataset.file_format)
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(batch_schema(dataset.name))
    )
    if dataset.file_format == "csv":
        reader = reader.option("header", "true").option("mode", "PERMISSIVE")
    else:
        reader = reader.option("multiLine", "false").option("mode", "PERMISSIVE")

    bronze = (
        reader.load(source_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_modified_at", F.col("_metadata.file_modification_time"))
    )
    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(config.table("bronze", dataset.name))
    )
    query.awaitTermination()
    print(f"Ingested new files for {dataset.name}")

