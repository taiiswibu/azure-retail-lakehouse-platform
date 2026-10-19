param(
    [Parameter(Mandatory = $true)][string]$ResourceGroup,
    [Parameter(Mandatory = $true)][string]$ConfirmName
)

$ErrorActionPreference = "Stop"

if ($ResourceGroup -ne $ConfirmName) {
    throw "Safety check failed: -ConfirmName must exactly equal -ResourceGroup."
}
if ($ResourceGroup -notmatch '^rg-[a-z0-9-]+$') {
    throw "Refusing to delete a resource group that does not follow the expected rg-* convention."
}

Write-Host "Deleting resource group $ResourceGroup and every Azure resource inside it."
az group delete --name $ResourceGroup --yes --no-wait
Write-Host "Deletion was submitted. Verify completion in Azure Portal."

