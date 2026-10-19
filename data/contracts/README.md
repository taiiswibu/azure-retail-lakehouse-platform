# Data contracts

These contracts define the producer-facing schema boundary. Bronze preserves the original
payload and ingestion metadata. Silver applies the stricter business rules implemented in
`src/retail_lakehouse/transforms.py` and sends rejected records to
`<catalog>.ops.quarantine_records`.

Compatibility policy:

- Adding an optional field is backward compatible.
- Renaming or changing the type of an existing field requires a new contract version.
- Producers must supply a globally unique `event_id` for streamed events.
- Timestamps are UTC ISO-8601 values.

