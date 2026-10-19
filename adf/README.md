# Optional Azure Data Factory path

Set `deployDataFactory=true` in `infra/main.dev.bicepparam` when you want to
demonstrate Azure Data Factory. The deployment creates:

- `LS_ADLS_GEN2`, authenticated with the Data Factory managed identity;
- parameterized binary datasets for source and sink;
- `PL_METADATA_BATCH_INGEST`, a parallel metadata-driven copy pipeline.

The pipeline copies files from `lakehouse/source-drop/...` into
`lakehouse/landing/...`. Use `pipeline-parameters.example.json` as the shape of
the `items` parameter. The Databricks Auto Loader job detects only newly landed
files and is safe to rerun because its checkpoints are kept separately.

For the lowest-cost demo, leave Data Factory disabled and use
`tools/upload_to_volume.py`. Both paths produce the same landing contract.

