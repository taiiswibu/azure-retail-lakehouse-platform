#!/usr/bin/env python3
"""Generate deterministic, relational e-commerce source data without external packages."""

from __future__ import annotations

import argparse
import csv
import json
import random
import uuid
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

FIRST_NAMES = ["An", "Anh", "Binh", "Chau", "Dung", "Giang", "Hanh", "Khanh", "Lan", "Minh", "Nam", "Phuong", "Tai", "Thao", "Trang"]
LAST_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Huynh", "Vo", "Dang", "Bui", "Do", "Ho"]
CITIES = ["Can Tho", "Ho Chi Minh", "Ha Noi", "Da Nang", "An Giang", "Vinh Long", "Dong Thap", "Kien Giang"]
CATEGORIES = ["Electronics", "Accessories", "Home", "Beauty", "Sports", "Books"]
BRANDS = ["Nova", "Orbit", "Mekong", "Zen", "VietTech", "Aurora"]
CHANNELS = ["web", "mobile", "marketplace", "store"]
PAYMENTS = ["cod", "bank_transfer", "e_wallet", "credit_card"]
ORDER_STATUSES = ["paid", "shipped", "completed", "cancelled", "refunded"]
ACQUISITION_SOURCES = ["organic", "paid_search", "social", "referral", "marketplace"]
TICKET_TOPICS = ["delivery", "return", "payment", "product", "warranty"]


def stable_id(namespace: str, value: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"azure-retail-lakehouse/{namespace}/{value}"))


def money(value: float | Decimal) -> str:
    return str(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError(f"Refusing to write an empty dataset: {path}")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def generate(args: argparse.Namespace) -> dict[str, object]:
    rng = random.Random(args.seed)
    now = datetime.now(UTC).replace(microsecond=0)
    start = now - timedelta(days=args.days)
    run_id = args.run_id or now.strftime("%Y%m%dT%H%M%SZ")
    load_date = now.date().isoformat()
    root = args.output.resolve() / "batch"
    partition = Path(f"load_date={load_date}") / f"run_id={run_id}"

    customers: list[dict[str, object]] = []
    profiles: list[dict[str, object]] = []
    for index in range(args.customers):
        customer_id = stable_id("customer", index)
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        signup = start + timedelta(seconds=rng.randint(0, max(1, int((now - start).total_seconds()))))
        updated = signup + timedelta(days=rng.randint(0, max(1, (now - signup).days)))
        email = f"{first}.{last}.{index}@example.com".lower()
        if args.inject_bad_records and args.customers > 1 and index % 97 == 0:
            email = "not-an-email"
        customers.append(
            {
                "customer_id": customer_id,
                "full_name": f"{last} {first}",
                "email": email,
                "phone": f"+84{rng.randint(300000000, 999999999)}",
                "city": rng.choice(CITIES),
                "country": "VN",
                "signup_date": signup.date().isoformat(),
                "loyalty_tier": rng.choices(
                    ["bronze", "silver", "gold", "platinum"], weights=[50, 30, 15, 5]
                )[0],
                "updated_at": iso(updated),
            }
        )
        profiles.append(
            {
                "customer_id": customer_id,
                "acquisition_source": rng.choice(ACQUISITION_SOURCES),
                "marketing_opt_in": rng.random() < 0.72,
                "preferred_category": rng.choice(CATEGORIES),
                "last_contact_at": iso(updated),
                "updated_at": iso(updated),
            }
        )

    products: list[dict[str, object]] = []
    for index in range(args.products):
        category = CATEGORIES[index % len(CATEGORIES)]
        price = Decimal(rng.randint(50_000, 15_000_000))
        cost = price * Decimal(str(rng.uniform(0.45, 0.82)))
        if args.inject_bad_records and args.products > 1 and index % 89 == 0:
            price = Decimal("-1")
        products.append(
            {
                "product_id": stable_id("product", index),
                "sku": f"{category[:3].upper()}-{index:05d}",
                "product_name": f"{rng.choice(BRANDS)} {category} {index + 1}",
                "category": category,
                "brand": rng.choice(BRANDS),
                "unit_price": money(price),
                "cost_price": money(cost),
                "active": rng.random() > 0.05,
                "updated_at": iso(now - timedelta(days=rng.randint(0, 20))),
            }
        )

    orderable_customers = [customer for customer in customers if "@" in str(customer["email"])]
    orderable_products = [product for product in products if Decimal(str(product["unit_price"])) > 0]
    orders: list[dict[str, object]] = []
    order_items: list[dict[str, object]] = []
    for order_index in range(args.orders):
        order_id = stable_id("order", order_index)
        order_ts = start + timedelta(seconds=rng.randint(0, max(1, int((now - start).total_seconds()))))
        chosen_products = rng.sample(
            orderable_products, k=rng.randint(1, min(4, len(orderable_products)))
        )
        subtotal = Decimal("0")
        order_discount = Decimal("0")
        for item_index, product in enumerate(chosen_products):
            quantity = rng.randint(1, 4)
            unit_price = max(Decimal("1"), Decimal(str(product["unit_price"])))
            discount = (unit_price * quantity * Decimal(str(rng.choice([0, 0, 0.05, 0.1])))).quantize(Decimal("0.01"))
            line_amount = unit_price * quantity - discount
            subtotal += unit_price * quantity
            order_discount += discount
            order_items.append(
                {
                    "order_item_id": stable_id("order-item", order_index * 10 + item_index),
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": money(unit_price),
                    "discount_amount": money(discount),
                    "line_amount": money(line_amount),
                }
            )
        shipping_fee = Decimal(rng.choice([0, 15_000, 25_000, 35_000]))
        total = subtotal - order_discount + shipping_fee
        orders.append(
            {
                "order_id": order_id,
                "customer_id": rng.choice(orderable_customers)["customer_id"],
                "order_ts": iso(order_ts),
                "status": rng.choices(ORDER_STATUSES, weights=[12, 18, 60, 7, 3])[0],
                "channel": rng.choice(CHANNELS),
                "payment_method": rng.choice(PAYMENTS),
                "shipping_city": rng.choice(CITIES),
                "subtotal": money(subtotal),
                "discount_amount": money(order_discount),
                "shipping_fee": money(shipping_fee),
                "total_amount": money(total),
                "updated_at": iso(min(now, order_ts + timedelta(days=rng.randint(0, 5)))),
            }
        )

    tickets: list[dict[str, object]] = []
    for index in range(max(1, args.orders // 12)):
        created = start + timedelta(seconds=rng.randint(0, max(1, int((now - start).total_seconds()))))
        status = rng.choices(["open", "in_progress", "resolved"], weights=[20, 15, 65])[0]
        resolved = created + timedelta(hours=rng.randint(1, 96)) if status == "resolved" else None
        tickets.append(
            {
                "ticket_id": stable_id("ticket", index),
                "customer_id": rng.choice(orderable_customers)["customer_id"],
                "created_at": iso(created),
                "resolved_at": iso(resolved) if resolved else None,
                "status": status,
                "priority": rng.choices(["low", "medium", "high", "urgent"], [35, 45, 17, 3])[0],
                "topic": rng.choice(TICKET_TOPICS),
                "satisfaction_score": rng.randint(1, 5) if status == "resolved" else None,
            }
        )

    outputs = {
        Path("erp/customers") / partition / "customers.csv": customers,
        Path("erp/products") / partition / "products.csv": products,
        Path("erp/orders") / partition / "orders.csv": orders,
        Path("erp/order_items") / partition / "order_items.csv": order_items,
        Path("crm/customer_profiles") / partition / "customer_profiles.jsonl": profiles,
        Path("crm/support_tickets") / partition / "support_tickets.jsonl": tickets,
    }
    for relative_path, rows in outputs.items():
        destination = root / relative_path
        if destination.suffix == ".csv":
            write_csv(destination, rows)
        else:
            write_jsonl(destination, rows)

    manifest = {
        "run_id": run_id,
        "generated_at": iso(now),
        "seed": args.seed,
        "load_date": load_date,
        "inject_bad_records": args.inject_bad_records,
        "root": str(root),
        "datasets": {str(path): len(rows) for path, rows in outputs.items()},
    }
    manifest_path = root / "manifests" / partition / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/generated"))
    parser.add_argument("--customers", type=int, default=1_000)
    parser.add_argument("--products", type=int, default=120)
    parser.add_argument("--orders", type=int, default=5_000)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-id")
    parser.add_argument("--inject-bad-records", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()
    for field in ("customers", "products", "orders", "days"):
        if getattr(args, field) <= 0:
            parser.error(f"--{field.replace('_', '-')} must be greater than zero")
    return args


if __name__ == "__main__":
    print(json.dumps(generate(parse_args()), ensure_ascii=False, indent=2))

