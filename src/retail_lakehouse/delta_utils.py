from __future__ import annotations

from collections.abc import Iterable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def merge_scd1(
    spark: SparkSession,
    source: DataFrame,
    target_table: str,
    keys: Iterable[str],
    update_condition: str | None = None,
) -> None:
    keys = list(keys)
    if not spark.catalog.tableExists(target_table):
        source.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    join_condition = " AND ".join([f"target.{key} = source.{key}" for key in keys])
    builder = DeltaTable.forName(spark, target_table).alias("target").merge(
        source.alias("source"), join_condition
    )
    if update_condition:
        builder = builder.whenMatchedUpdateAll(condition=update_condition)
    else:
        builder = builder.whenMatchedUpdateAll()
    builder.whenNotMatchedInsertAll().execute()


def apply_scd2(
    spark: SparkSession,
    source: DataFrame,
    target_table: str,
    natural_key: str,
    tracked_columns: list[str],
    effective_column: str,
) -> None:
    source_with_hash = (
        source.withColumn(
            "attribute_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in tracked_columns],
                ),
                256,
            ),
        )
        .withColumn("valid_from", F.coalesce(F.col(effective_column), F.current_timestamp()))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "customer_sk",
            F.sha2(F.concat_ws("||", F.col(natural_key), F.col("valid_from").cast("string")), 256),
        )
    )

    if not spark.catalog.tableExists(target_table):
        source_with_hash.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    current = spark.table(target_table).where("is_current = true").select(
        F.col(natural_key), F.col("attribute_hash").alias("current_hash")
    )
    changes = (
        source_with_hash.alias("source")
        .join(current.alias("current"), natural_key, "left")
        .where(F.col("current_hash").isNull() | (F.col("attribute_hash") != F.col("current_hash")))
        .drop("current_hash")
    )

    if changes.limit(1).count() == 0:
        return

    expiration = changes.select(
        natural_key, F.col("valid_from").alias("new_valid_from")
    )
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(
            expiration.alias("source"),
            f"target.{natural_key} = source.{natural_key} AND target.is_current = true",
        )
        .whenMatchedUpdate(
            set={"is_current": "false", "valid_to": "source.new_valid_from"}
        )
        .execute()
    )
    changes.write.format("delta").mode("append").saveAsTable(target_table)


def overwrite_table(df: DataFrame, target_table: str, partition_columns: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.saveAsTable(target_table)

