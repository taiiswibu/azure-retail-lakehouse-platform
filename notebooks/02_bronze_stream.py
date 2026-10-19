# Databricks notebook source
# MAGIC %md
# MAGIC # Event Hubs to Bronze
# MAGIC Reads Azure Event Hubs through its Kafka-compatible endpoint and retains the raw JSON.

# COMMAND ----------

from pyspark.sql import functions as F

from retail_lakehouse.config import LakehouseConfig

dbutils.widgets.text("catalog", "retail_dev")
dbutils.widgets.text("eventhubs_namespace", "")
dbutils.widgets.text("secret_scope", "retail-secrets")
dbutils.widgets.text("secret_key", "eventhubs-connection-string")
dbutils.widgets.text("clickstream_hub", "clickstream")
dbutils.widgets.text("iot_hub", "iot-telemetry")

config = LakehouseConfig(
    catalog=dbutils.widgets.get("catalog"),
    secret_scope=dbutils.widgets.get("secret_scope"),
    eventhubs_secret_key=dbutils.widgets.get("secret_key"),
)
namespace = dbutils.widgets.get("eventhubs_namespace").strip()
if not namespace:
    raise ValueError("eventhubs_namespace is required")

connection_string = dbutils.secrets.get(
    scope=config.secret_scope, key=config.eventhubs_secret_key
)
if '"' in connection_string or "\n" in connection_string:
    raise ValueError("Event Hubs connection string contains unsupported characters")

common_options = {
    "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="$ConnectionString" '
        f'password="{connection_string}";'
    ),
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
}

# COMMAND ----------

for hub_widget, target_name in (
    ("clickstream_hub", "clickstream_events"),
    ("iot_hub", "iot_telemetry"),
):
    hub_name = dbutils.widgets.get(hub_widget)
    stream = spark.readStream.format("kafka")
    for key, value in common_options.items():
        stream = stream.option(key, value)
    raw = (
        stream.option("subscribe", hub_name)
        .load()
        .select(
            F.col("value").cast("string").alias("raw_payload"),
            F.col("key").cast("string").alias("message_key"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
    checkpoint = f"{config.state_volume}/eventhubs/{hub_name}/checkpoint"
    query = (
        raw.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(config.table("bronze", target_name))
    )
    query.awaitTermination()
    print(f"Drained available events from {hub_name}")

