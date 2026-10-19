# Validation report

Validation date: 2026-10-12 UTC  
Scope: cloud-free checks in the delivery workspace. No Azure subscription or
Databricks workspace was modified.

## Results

| Check | Result | Evidence |
|---|---|---|
| Python syntax | PASS | `compileall` completed for source, generators, tools and notebooks |
| Ruff lint | PASS | No lint findings in source, generators, tools or tests |
| Unit tests | PASS | 13 tests passed, including two local PySpark transformation tests |
| Batch generator | PASS | Six datasets plus manifest generated with relational keys |
| Streaming generator | PASS | Clickstream and IoT JSONL dry run parsed successfully |
| Project structure | PASS | Required files present; JSON/YAML parse and secret-pattern scan passed |
| Python wheel | PASS | `azure_retail_lakehouse-1.0.0-py3-none-any.whl` built successfully |
| Bicep compile | PASS | Bicep CLI 0.48.1 compiled `infra/main.bicep` with no errors or warnings |
| Databricks bundle parse | PASS/PARTIAL | CLI 1.20.0 loaded bundle/resources; remote validation stopped at expected missing authentication |

## Explicitly not claimed

- Azure resources were not deployed because this environment has no user subscription authority.
- Unity Catalog permissions and regional Azure SKU availability require the user's account.
- Event Hubs and Databricks integration must be verified after secrets and workspace identity exist.
- Power BI pages cannot be packaged with a live authenticated connection here; the SQL, DAX,
  theme, relationships and page specification are included for deterministic assembly.

## Acceptance test after deployment

The cloud deployment is accepted only when:

1. Bicep deployment succeeds and outputs the expected resource names.
2. `create_storage_credential.ps1` creates the managed-identity credential; `retail_bootstrap` creates the catalog, schemas and volumes.
3. `retail_batch_pipeline` completes all four tasks.
4. Re-running batch without new files leaves business row counts stable.
5. `retail_stream_pipeline` creates clickstream and IoT Silver tables.
6. All critical checks in the latest `ops.data_quality_results` run are `PASS`.
7. Power BI refreshes the Gold model.
8. The dedicated resource group is deleted after evidence capture.

