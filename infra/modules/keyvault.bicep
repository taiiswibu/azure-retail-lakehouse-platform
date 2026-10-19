param location string
param vaultName string
param deployerObjectId string = ''
@secure()
param eventHubsConsumerConnectionString string
@secure()
param eventHubsProducerConnectionString string
param tags object

resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: vaultName
  location: location
  tags: tags
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    enableRbacAuthorization: true
    // enablePurgeProtection: false
    enableSoftDelete: true
    publicNetworkAccess: 'Enabled'
    softDeleteRetentionInDays: 7
  }
}

var keyVaultSecretsOfficerRole = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'b86a8fe4-44ce-4948-aee5-eccb2c155cd7'
)

resource deployerSecretsRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(deployerObjectId)) {
  name: guid(vault.id, deployerObjectId, keyVaultSecretsOfficerRole)
  scope: vault
  properties: {
    principalId: deployerObjectId
    principalType: 'User'
    roleDefinitionId: keyVaultSecretsOfficerRole
  }
}

resource consumerSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: vault
  name: 'eventhubs-consumer-connection'
  properties: {
    value: eventHubsConsumerConnectionString
  }
}

resource producerSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: vault
  name: 'eventhubs-producer-connection'
  properties: {
    value: eventHubsProducerConnectionString
  }
}

output vaultName string = vault.name
output vaultId string = vault.id

