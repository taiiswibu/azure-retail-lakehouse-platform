from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class QualityResult:
    table_name: str
    check_name: str
    status: Literal["PASS", "FAIL"]
    observed_value: float
    threshold: float
    details: str


def row_count_check(df: DataFrame, table: str, minimum: int = 1) -> QualityResult:
    observed = df.count()
    return QualityResult(
        table,
        "row_count",
        "PASS" if observed >= minimum else "FAIL",
        float(observed),
        float(minimum),
        f"Expected at least {minimum} rows",
    )


def null_rate_check(
    df: DataFrame, table: str, column: str, maximum_rate: float = 0.0
) -> QualityResult:
    total = df.count()
    nulls = df.where(F.col(column).isNull()).count()
    rate = nulls / total if total else 1.0
    return QualityResult(
        table,
        f"null_rate__{column}",
        "PASS" if rate <= maximum_rate else "FAIL",
        float(rate),
        float(maximum_rate),
        f"{nulls} nulls across {total} rows",
    )


def uniqueness_check(df: DataFrame, table: str, columns: list[str]) -> QualityResult:
    total = df.count()
    distinct = df.select(*columns).distinct().count()
    duplicate_rate = (total - distinct) / total if total else 0.0
    label = "__".join(columns)
    return QualityResult(
        table,
        f"duplicate_rate__{label}",
        "PASS" if duplicate_rate == 0 else "FAIL",
        float(duplicate_rate),
        0.0,
        f"{total - distinct} duplicate rows across {total} rows",
    )


def orphan_rate_check(
    child: DataFrame,
    parent: DataFrame,
    table: str,
    child_key: str,
    parent_key: str,
    maximum_rate: float = 0.0,
) -> QualityResult:
    total = child.count()
    orphan_count = child.join(
        parent.select(F.col(parent_key).alias(child_key)).distinct(), child_key, "left_anti"
    ).count()
    rate = orphan_count / total if total else 0.0
    return QualityResult(
        table,
        f"orphan_rate__{child_key}",
        "PASS" if rate <= maximum_rate else "FAIL",
        float(rate),
        float(maximum_rate),
        f"{orphan_count} orphan rows across {total} rows",
    )


def write_results(
    spark: SparkSession,
    results: list[QualityResult],
    target_table: str,
    run_id: str,
) -> None:
    checked_at = datetime.now(timezone.utc)
    rows = [
        (
            run_id,
            result.table_name,
            result.check_name,
            result.status,
            result.observed_value,
            result.threshold,
            result.details,
            checked_at,
        )
        for result in results
    ]
    schema = (
        "run_id string, table_name string, check_name string, status string, "
        "observed_value double, threshold double, details string, checked_at timestamp"
    )
    spark.createDataFrame(rows, schema=schema).write.format("delta").mode("append").saveAsTable(
        target_table
    )

