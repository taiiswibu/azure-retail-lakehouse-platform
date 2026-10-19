from __future__ import annotations

import argparse
import csv
import json
import random
from pathlib import Path

from generators.generate_batch import generate
from generators.send_events import emit_dry_run


def test_batch_generator_creates_relational_datasets(tmp_path: Path) -> None:
    args = argparse.Namespace(
        output=tmp_path,
        customers=25,
        products=12,
        orders=40,
        days=10,
        seed=42,
        run_id="pytest",
        inject_bad_records=False,
    )
    manifest = generate(args)
    assert sum(manifest["datasets"].values()) > 25 + 12 + 40

    customer_path = next((tmp_path / "batch" / "erp" / "customers").rglob("customers.csv"))
    order_path = next((tmp_path / "batch" / "erp" / "orders").rglob("orders.csv"))
    with customer_path.open(encoding="utf-8") as handle:
        customer_ids = {row["customer_id"] for row in csv.DictReader(handle)}
    with order_path.open(encoding="utf-8") as handle:
        orders = list(csv.DictReader(handle))
    assert len(customer_ids) == 25
    assert len(orders) == 40
    assert {order["customer_id"] for order in orders}.issubset(customer_ids)


def test_stream_dry_run_produces_valid_jsonl(tmp_path: Path) -> None:
    emit_dry_run(tmp_path, 5, random.Random(42))
    for path in (tmp_path / "clickstream.jsonl", tmp_path / "iot_telemetry.jsonl"):
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        assert len(rows) == 5
        assert all("event_id" in row and "event_ts" in row for row in rows)


def test_injected_bad_dimensions_are_not_referenced_by_facts(tmp_path: Path) -> None:
    args = argparse.Namespace(
        output=tmp_path,
        customers=100,
        products=100,
        orders=250,
        days=30,
        seed=42,
        run_id="quality-safe",
        inject_bad_records=True,
    )
    generate(args)
    batch = tmp_path / "batch"

    customer_path = next((batch / "erp" / "customers").rglob("customers.csv"))
    product_path = next((batch / "erp" / "products").rglob("products.csv"))
    order_path = next((batch / "erp" / "orders").rglob("orders.csv"))
    item_path = next((batch / "erp" / "order_items").rglob("order_items.csv"))

    with customer_path.open(encoding="utf-8") as handle:
        valid_customers = {
            row["customer_id"] for row in csv.DictReader(handle) if "@" in row["email"]
        }
    with product_path.open(encoding="utf-8") as handle:
        valid_products = {
            row["product_id"] for row in csv.DictReader(handle) if float(row["unit_price"]) > 0
        }
    with order_path.open(encoding="utf-8") as handle:
        orders = list(csv.DictReader(handle))
    with item_path.open(encoding="utf-8") as handle:
        items = list(csv.DictReader(handle))

    assert {row["customer_id"] for row in orders}.issubset(valid_customers)
    assert {row["product_id"] for row in items}.issubset(valid_products)

