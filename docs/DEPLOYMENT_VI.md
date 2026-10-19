# Hướng dẫn triển khai đầy đủ trên Azure

Hướng dẫn này dành cho Windows PowerShell và tài khoản Azure for Students. Toàn
bộ tài nguyên nằm trong **một resource group riêng** để có thể xóa ngay sau khi demo.

## 1. Điều kiện ban đầu

Cài đặt:

```powershell
winget install -e --id Microsoft.AzureCLI
winget install -e --id Databricks.DatabricksCLI
winget install -e --id Git.Git
```

Dùng Python 3.10 hoặc 3.11. Không cần cài Spark local nếu bạn chỉ chạy trên Azure.

Kiểm tra:

```powershell
az version
databricks version
git --version
py --version
```

## 2. Kiểm soát chi phí trước khi triển khai

Trong Azure Portal:

1. Mở **Cost Management + Billing**.
2. Chọn subscription sinh viên.
3. Tạo budget nhỏ và cảnh báo tại 50%, 80%, 100%.
4. Không triển khai chung với resource group của dự án khác.

Các tài nguyên tính phí đáng chú ý là Databricks job compute, Event Hubs Standard,
Log Analytics ingestion và SQL Warehouse. Pipeline sử dụng job cluster tự kết
thúc; Event Hubs và resource group phải được xóa sau khi chụp xong minh chứng.

## 3. Đăng nhập và chuẩn bị subscription

Mở PowerShell tại thư mục gốc repository:

```powershell
az login
az account list --output table
$subscriptionId = "THAY_SUBSCRIPTION_ID"
az account set --subscription $subscriptionId

az provider register --namespace Microsoft.Databricks
az provider register --namespace Microsoft.Storage
az provider register --namespace Microsoft.EventHub
az provider register --namespace Microsoft.KeyVault
az provider register --namespace Microsoft.OperationalInsights
az provider register --namespace Microsoft.DataFactory
```

Đợi trạng thái cần thiết chuyển thành `Registered`:

```powershell
az provider show --namespace Microsoft.Databricks --query registrationState -o tsv
az provider show --namespace Microsoft.EventHub --query registrationState -o tsv
```

Lấy Object ID của tài khoản để có quyền đọc hai secret demo:

```powershell
$deployerObjectId = az ad signed-in-user show --query id -o tsv
```

Nếu lệnh trên bị chặn bởi quyền Entra ID, để trống giá trị và cấp quyền
**Key Vault Secrets Officer** thủ công trong Portal sau khi deploy.

## 4. Deploy hạ tầng Bicep

Chọn tên resource group chỉ dùng cho dự án này:

```powershell
$resourceGroup = "rg-retail-lakehouse-demo"
$location = "southeastasia"

.\scripts\deploy_azure.ps1 `
  -SubscriptionId $subscriptionId `
  -ResourceGroup $resourceGroup `
  -Location $location `
  -ProjectName "retail" `
  -Environment "dev" `
  -DeployerObjectId $deployerObjectId
```

Mặc định ADF không được tạo. Khi cần minh họa ADF, thêm `-DeployDataFactory` vào
lệnh gọi `deploy_azure.ps1`.

Lấy outputs:

```powershell
$deploymentName = "retail-lakehouse-dev"
$outputs = az deployment group show `
  --resource-group $resourceGroup `
  --name $deploymentName `
  --query properties.outputs `
  --output json | ConvertFrom-Json

$databricksHost = $outputs.databricksWorkspaceUrl.value
$storageAccount = $outputs.storageAccountName.value
$accessConnectorId = $outputs.accessConnectorId.value
$eventHubsNamespace = $outputs.eventHubsNamespaceName.value
$keyVaultName = $outputs.keyVaultName.value

$databricksHost
$storageAccount
$eventHubsNamespace
```

Không in giá trị Key Vault secret ra màn hình hoặc đưa vào ảnh chụp.

## 5. Kết nối Databricks CLI

```powershell
databricks auth login --host $databricksHost --profile retail-demo
databricks current-user me --profile retail-demo
```

Nếu Databricks workspace mới mở lần đầu, đăng nhập workspace bằng Portal trước rồi
chạy lại lệnh `auth login`.

## 6. Đưa Event Hubs consumer secret vào Databricks

Role assignment có thể cần vài phút để lan truyền.

```powershell
databricks secrets create-scope retail-secrets --profile retail-demo

$consumerSecret = az keyvault secret show `
  --vault-name $keyVaultName `
  --name eventhubs-consumer-connection `
  --query value `
  --output tsv

databricks secrets put-secret retail-secrets eventhubs-connection-string `
  --string-value $consumerSecret `
  --profile retail-demo

$consumerSecret = $null
Remove-Variable consumerSecret
```

Nếu scope đã tồn tại, bỏ qua lỗi của `create-scope`; không xóa scope đang dùng bởi
dự án khác.

## 7. Validate và deploy Databricks bundle

Bundle nhận outputs thông qua biến môi trường, không yêu cầu sửa source:

```powershell
$env:DATABRICKS_CONFIG_PROFILE = "retail-demo"
$env:BUNDLE_VAR_storage_account_name = $storageAccount
$env:BUNDLE_VAR_eventhubs_namespace = $eventHubsNamespace
$env:BUNDLE_VAR_catalog = "retail_dev"

databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Nếu `Standard_DS3_v2` không có quota trong region, tìm node type khả dụng trong
Compute rồi đặt thêm:

```powershell
$env:BUNDLE_VAR_node_type_id = "NODE_TYPE_CO_SAN"
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## 8. Khởi tạo Unity Catalog

Tài khoản chạy các lệnh sau cần quyền tạo storage credential, external location và
catalog. Storage credential được tạo qua Databricks CLI/API; notebook bootstrap chỉ
tạo external location, catalog, schemas và volumes.

```powershell
.\scripts\create_storage_credential.ps1 `
  -AccessConnectorId $accessConnectorId `
  -Catalog "retail_dev" `
  -Profile "retail-demo"

databricks bundle run retail_bootstrap -t dev
```

Kiểm tra trong Catalog Explorer:

- catalog `retail_dev`;
- schemas `landing`, `checkpoints`, `bronze`, `silver`, `gold`, `ops`;
- external volumes `retail_dev.landing.files` và `retail_dev.checkpoints.state`.

Nếu workspace không cấp quyền metastore admin cho tài khoản của bạn, vào phần
Admin/Catalog để cấp quyền hoặc chạy notebook `00_setup_catalog.py` bằng tài khoản admin.

## 9. Tạo và upload dữ liệu ERP/CRM

Tạo môi trường Python:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[producer]"
```

Tạo dữ liệu. Các lỗi có chủ ý giúp chứng minh quarantine hoạt động:

```powershell
python generators/generate_batch.py `
  --customers 1000 `
  --products 120 `
  --orders 5000 `
  --seed 42
```

Upload vào external volume:

```powershell
python tools/upload_to_volume.py data/generated/batch `
  --catalog retail_dev `
  --profile retail-demo
```

Chạy batch pipeline:

```powershell
databricks bundle run retail_batch_pipeline -t dev
```

Kết quả mong đợi:

- sáu bảng Bronze;
- các bảng Silver hợp lệ;
- một số row trong `ops.quarantine_records`;
- star schema và hai mart batch;
- tất cả critical quality checks đạt `PASS`.

## 10. Gửi dữ liệu clickstream và IoT

Lấy producer-only secret vào biến môi trường, chạy producer rồi xóa biến:

```powershell
$env:EVENTHUBS_CONNECTION_STRING = az keyvault secret show `
  --vault-name $keyVaultName `
  --name eventhubs-producer-connection `
  --query value `
  --output tsv

python generators/send_events.py --events 500 --rate 20 --seed 42

Remove-Item Env:EVENTHUBS_CONNECTION_STRING
```

Sau đó drain backlog và cập nhật Silver/Gold:

```powershell
databricks bundle run retail_stream_pipeline -t dev
```

Kiểm tra:

```sql
SELECT COUNT(*) FROM retail_dev.silver.clickstream_events;
SELECT COUNT(*) FROM retail_dev.silver.iot_telemetry;
SELECT * FROM retail_dev.gold.mart_digital_funnel ORDER BY event_date DESC;
SELECT * FROM retail_dev.gold.mart_inventory_health ORDER BY window_start DESC;
```

## 11. Tạo BI views

1. Mở Databricks SQL Editor.
2. Tạo hoặc bật SQL Warehouse kích thước nhỏ nhất, auto-stop 10 phút.
3. Chạy toàn bộ `sql/10_bi_views.sql`.
4. Làm theo `docs/POWER_BI_VI.md`.
5. Stop SQL Warehouse ngay khi hoàn tất.

## 12. Kiểm tra idempotency

Không tạo file mới, chạy lại batch pipeline:

```powershell
databricks bundle run retail_batch_pipeline -t dev
```

Auto Loader không đọc lại các file cũ. Row count của các business key không tăng
bất thường. Sau đó tạo một run mới và chạy lại để chứng minh incremental ingestion.

## 13. Module ADF tùy chọn

Khi `deployDataFactory=true`, upload file vào `lakehouse/source-drop/...`, mở ADF
Studio và chạy `PL_METADATA_BATCH_INGEST` với shape trong
`adf/pipeline-parameters.example.json`. Pipeline dùng managed identity để copy
vào `lakehouse/landing/...`; Auto Loader xử lý phần còn lại.

## 14. Tắt và xóa tài nguyên

Trước khi xóa, chụp đủ ảnh theo `SCREENSHOT_CHECKLIST_VI.md`. Sau đó chạy:

```powershell
.\scripts\destroy_azure.ps1 `
  -ResourceGroup $resourceGroup `
  -ConfirmName $resourceGroup
```

Lệnh này xóa **toàn bộ** resource group đã chỉ định. Script bắt buộc nhập lại đúng
tên để tránh nhầm. Kiểm tra Portal đến khi resource group biến mất và kiểm tra lại
Cost Management vào ngày hôm sau.

## 15. Lỗi thường gặp

| Lỗi | Nguyên nhân chính | Cách xử lý |
|---|---|---|
| `AuthorizationFailed` khi deploy | Thiếu Contributor/Owner | Dùng đúng subscription hoặc xin role |
| Không tạo được VM node | Region thiếu quota | Đổi node type hoặc region rồi deploy lại |
| Tạo credential/bootstrap bị `PERMISSION_DENIED` | Thiếu quyền metastore hoặc Azure Contributor trên Access Connector | Cấp quyền rồi chạy lại script/bootstrap |
| `403` khi tạo external location | RBAC chưa lan truyền | Đợi 5–10 phút rồi chạy lại bootstrap |
| Không đọc được Event Hubs | Secret/namespace sai | Kiểm tra scope, secret key và bundle variable |
| Stream có 0 row | Producer chưa gửi hoặc retention hết | Gửi event lại rồi chạy stream job |
| Quality gate fail | Orphan hoặc dữ liệu lỗi vượt gate | Xem `ops.data_quality_results` và quarantine |
| Power BI không kết nối | SQL Warehouse tắt hoặc hostname/path sai | Bật warehouse, copy lại Server Hostname và HTTP Path |

