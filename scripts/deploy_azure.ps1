param(
    [Parameter(Mandatory = $true)][string]$SubscriptionId,
    [Parameter(Mandatory = $true)][string]$ResourceGroup,
    [string]$Location = "southeastasia",
    [string]$ProjectName = "retail",
    [string]$Environment = "dev",
    [string]$DeployerObjectId = "",
    [switch]$DeployDataFactory
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI (az) is required."
}

az account set --subscription $SubscriptionId
az group create --name $ResourceGroup --location $Location --tags `
    project=azure-retail-lakehouse environment=$Environment lifecycle=on-demand | Out-Null

$parameters = @(
    "projectName=$ProjectName",
    "environment=$Environment",
    "location=$Location",
    "deployDataFactory=$($DeployDataFactory.IsPresent.ToString().ToLowerInvariant())"
)
if ($DeployerObjectId) {
    $parameters += "deployerObjectId=$DeployerObjectId"
}

az deployment group create `
    --resource-group $ResourceGroup `
    --template-file infra/main.bicep `
    --parameters $parameters `
    --name "retail-lakehouse-$Environment"

az deployment group show `
    --resource-group $ResourceGroup `
    --name "retail-lakehouse-$Environment" `
    --query properties.outputs `
    --output table

Write-Host "Azure resources deployed. Continue with docs/DEPLOYMENT_VI.md."

