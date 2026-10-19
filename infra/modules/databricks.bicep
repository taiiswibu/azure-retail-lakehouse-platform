param location string
param workspaceName string
param accessConnectorName string
param managedResourceGroupName string
param storageAccountName string
param tags object

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: workspaceName
  location: location
  tags: tags
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId(
      'Microsoft.Resources/resourceGroups',
      managedResourceGroupName
    )
    publicNetworkAccess: 'Enabled'
    requiredNsgRules: 'AllRules'
    parameters: {
      enableNoPublicIp: {
        value: true
      }
    }
  }
}

resource accessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: accessConnectorName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
}

var storageBlobDataContributorRole = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)

resource accessConnectorStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, accessConnector.id, storageBlobDataContributorRole)
  scope: storage
  properties: {
    principalId: accessConnector.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: storageBlobDataContributorRole
  }
}

output workspaceName string = workspace.name
output workspaceUrl string = 'https://${workspace.properties.workspaceUrl}'
output accessConnectorId string = accessConnector.id
output accessConnectorPrincipalId string = accessConnector.identity.principalId

