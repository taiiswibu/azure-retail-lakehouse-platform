from __future__ import annotations

from datetime import datetime

import pytest

from retail_lakehouse.transforms import clean_customers, clean_orders


@pytest.mark.spark
def test_customer_cleaning_deduplicates_and_quarantines_bad_email(spark) -> None:
    rows = [
        ("c1", "Old Name", "old@example.com", "+841", "can tho", "vn", "2026-01-01", "bronze", "2026-01-01T00:00:00Z", datetime(2026, 1, 1), "old.csv"),
        ("c1", "New Name", "new@example.com", "+841", "can tho", "vn", "2026-01-01", "silver", "2026-02-01T00:00:00Z", datetime(2026, 2, 1), "new.csv"),
        ("c2", "Bad Email", "invalid", "+842", "vinh long", "vn", "2026-01-02", "gold", "2026-02-01T00:00:00Z", datetime(2026, 2, 1), "bad.csv"),
    ]
    columns = [
        "customer_id", "full_name", "email", "phone", "city", "country",
        "signup_date", "loyalty_tier", "updated_at", "_ingested_at", "_source_file",
    ]
    result = clean_customers(spark.createDataFrame(rows, columns))
    assert result.count() == 2
    assert result.where("customer_id = 'c1'").first().full_name == "New Name"
    bad = result.where("customer_id = 'c2'").first()
    assert bad._dq_is_valid is False
    assert bad._dq_reason == "invalid_email"


@pytest.mark.spark
def test_order_total_mismatch_is_rejected(spark) -> None:
    rows = [
        ("o1", "c1", "2026-01-01T00:00:00Z", "paid", "web", "cod", "Can Tho", "100.00", "10.00", "5.00", "999.00", "2026-01-01T01:00:00Z", datetime(2026, 1, 1), "orders.csv"),
    ]
    columns = [
        "order_id", "customer_id", "order_ts", "status", "channel", "payment_method",
        "shipping_city", "subtotal", "discount_amount", "shipping_fee", "total_amount",
        "updated_at", "_ingested_at", "_source_file",
    ]
    result = clean_orders(spark.createDataFrame(rows, columns)).first()
    assert result._dq_is_valid is False
    assert result._dq_reason == "order_total_mismatch"

