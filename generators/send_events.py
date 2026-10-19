#!/usr/bin/env python3
"""Send realistic clickstream and warehouse telemetry to Azure Event Hubs."""

from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path


def clickstream_event(rng: random.Random, index: int) -> dict[str, object]:
    event_type = rng.choices(
        ["page_view", "product_view", "add_to_cart", "checkout", "purchase"],
        weights=[40, 30, 17, 8, 5],
    )[0]
    return {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"clickstream/{index}")),
        "event_ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "session_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"session/{index // 8}")),
        "customer_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"azure-retail-lakehouse/customer/{index % 1000}")) if rng.random() < 0.7 else None,
        "anonymous_id": f"anon-{index % 400:05d}",
        "event_type": event_type,
        "product_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"azure-retail-lakehouse/product/{index % 120}")) if event_type != "page_view" else None,
        "page": rng.choice(["home", "search", "product", "cart", "checkout"]),
        "device_type": rng.choice(["mobile", "desktop", "tablet"]),
        "traffic_source": rng.choice(["organic", "social", "paid", "direct"]),
        "quantity": rng.randint(1, 3) if event_type in {"add_to_cart", "purchase"} else None,
    }


def iot_event(rng: random.Random, index: int) -> dict[str, object]:
    door_open = rng.random() < 0.08
    temperature = rng.normalvariate(25.5, 2.2) + (5.0 if door_open else 0.0)
    return {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"iot/{index}")),
        "event_ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "device_id": f"sensor-{index % 24:03d}",
        "warehouse_id": f"WH-{index % 3 + 1:02d}",
        "sensor_type": "environment",
        "temperature_c": round(temperature, 2),
        "humidity_pct": round(rng.uniform(45, 82), 2),
        "battery_pct": round(max(0, 100 - (index % 500) / 5), 2),
        "is_door_open": door_open,
    }


def emit_dry_run(output: Path, events: int, rng: random.Random) -> None:
    output.mkdir(parents=True, exist_ok=True)
    files = {
        "clickstream": (output / "clickstream.jsonl").open("w", encoding="utf-8"),
        "iot": (output / "iot_telemetry.jsonl").open("w", encoding="utf-8"),
    }
    try:
        for index in range(events):
            files["clickstream"].write(json.dumps(clickstream_event(rng, index)) + "\n")
            files["iot"].write(json.dumps(iot_event(rng, index)) + "\n")
    finally:
        for handle in files.values():
            handle.close()


def send(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    if args.dry_run:
        emit_dry_run(args.output, args.events, rng)
        print(f"Wrote {args.events * 2} sample events to {args.output.resolve()}")
        return

    connection_string = os.environ.get("EVENTHUBS_CONNECTION_STRING")
    if not connection_string:
        raise SystemExit("EVENTHUBS_CONNECTION_STRING is required unless --dry-run is used")

    try:
        from azure.eventhub import EventData, EventHubProducerClient
    except ImportError as exc:
        raise SystemExit("Install producer dependencies: pip install -e .[producer]") from exc

    producers = {
        "clickstream": EventHubProducerClient.from_connection_string(
            connection_string, eventhub_name=args.clickstream_hub
        ),
        "iot": EventHubProducerClient.from_connection_string(
            connection_string, eventhub_name=args.iot_hub
        ),
    }
    interval = 1.0 / args.rate if args.rate > 0 else 0
    try:
        for index in range(args.events):
            for name, payload in (
                ("clickstream", clickstream_event(rng, index)),
                ("iot", iot_event(rng, index)),
            ):
                batch = producers[name].create_batch()
                batch.add(EventData(json.dumps(payload, separators=(",", ":"))))
                producers[name].send_batch(batch)
            if interval:
                time.sleep(interval)
    finally:
        for producer in producers.values():
            producer.close()
    print(f"Sent {args.events} events to each Event Hub")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=500)
    parser.add_argument("--rate", type=float, default=20.0, help="Event pairs per second")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--clickstream-hub", default=os.getenv("EVENTHUB_CLICKSTREAM", "clickstream"))
    parser.add_argument("--iot-hub", default=os.getenv("EVENTHUB_IOT", "iot-telemetry"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("data/generated/stream_samples"))
    args = parser.parse_args()
    if args.events <= 0:
        parser.error("--events must be greater than zero")
    if args.rate < 0:
        parser.error("--rate cannot be negative")
    return args


if __name__ == "__main__":
    send(parse_args())

