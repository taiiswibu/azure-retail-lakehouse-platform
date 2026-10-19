param(
    [Parameter(Mandatory = $true)][string]$AccessConnectorId,
    [string]$Catalog = "retail_dev",
    [string]$Profile = "retail-demo"
)

$ErrorActionPreference = "Continue"

if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    throw "Databricks CLI is required."
}

if ($Catalog -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') {
    throw "Catalog must be a valid Unity Catalog identifier."
}

if ($AccessConnectorId -notmatch '^/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\.Databricks/accessConnectors/[^/]+$') {
    throw "AccessConnectorId is not a valid Azure Databricks Access Connector resource ID."
}

$credentialName = "${Catalog}_storage_credential"

databricks storage-credentials get $credentialName --profile $Profile 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "Storage credential '$credentialName' already exists; leaving it unchanged."
    exit 0
}

$payload = @{
    name = $credentialName
    comment = "Managed identity credential for the retail lakehouse"
    azure_managed_identity = @{
        access_connector_id = $AccessConnectorId
    }
} | ConvertTo-Json -Depth 4 -Compress

$payload | databricks storage-credentials create --json "@-" --profile $Profile | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Could not create storage credential '$credentialName'."
}

Write-Host "Created storage credential '$credentialName'."
