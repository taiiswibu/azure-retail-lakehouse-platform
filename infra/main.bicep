targetScope = 'resourceGroup'

@minLength(3)
@maxLength(12)
@description('Short lowercase project name used in Azure resource names.')
param projectName string = 'retail'

@allowed([
  'dev'
  'demo'
])
param environment string = 'dev'

param location string = resourceGroup().location

@description('Optional Microsoft Entra object ID that receives Key Vault Secrets Officer.')
param deployerObjectId string = ''

@description('Deploy optional Azure Data Factory resources.')
param deployDataFactory bool = false

var suffix = take(uniqueString(subscription().subscriptionId, resourceGroup().id, projectName, environment), 6)
var baseName = '${toLower(projectName)}-${environment}-${suffix}'
var storageName = take(replace('st${toLower(projectName)}${environment}${suffix}', '-', ''), 24)
var eventHubsNamespaceName = 'evhns-${baseName}'
var tags = {
  project: 'azure-retail-lakehouse'
  environment: environment
  lifecycle: 'on-demand'
  owner: 'portfolio'
}

module monitoring 'modules/monitoring.bicep' = {
  name: 'monitoring-${suffix}'
  params: {
    location: location
    workspaceName: 'log-${baseName}'
    tags: tags
  }
}

module storage 'modules/storage.bicep' = {
  name: 'storage-${suffix}'
  params: {
    location: location
    storageAccountName: storageName
    tags: tags
  }
}

module databricks 'modules/databricks.bicep' = {
  name: 'databricks-${suffix}'
  params: {
    location: location
    workspaceName: 'dbw-${baseName}'
    accessConnectorName: 'dac-${baseName}'
    managedResourceGroupName: 'mrg-dbw-${baseName}'
    storageAccountName: storage.outputs.storageAccountName
    tags: tags
  }
}

module eventHubs 'modules/eventhubs.bicep' = {
  name: 'eventhubs-${suffix}'
  params: {
    location: location
    namespaceName: eventHubsNamespaceName
    tags: tags
  }
}

var consumerRuleResourceId = resourceId(
  'Microsoft.EventHub/namespaces/authorizationRules',
  eventHubsNamespaceName,
  'databricks-consumer'
)
var producerRuleResourceId = resourceId(
  'Microsoft.EventHub/namespaces/authorizationRules',
  eventHubsNamespaceName,
  'demo-producer'
)
var consumerConnectionString = listKeys(consumerRuleResourceId, '2024-01-01').primaryConnectionString
var producerConnectionString = listKeys(producerRuleResourceId, '2024-01-01').primaryConnectionString

module keyVault 'modules/keyvault.bicep' = {
  name: 'keyvault-${suffix}'
  params: {
    location: location
    vaultName: take('kv-${baseName}', 24)
    deployerObjectId: deployerObjectId
    eventHubsConsumerConnectionString: consumerConnectionString
    eventHubsProducerConnectionString: producerConnectionString
    tags: tags
  }
  dependsOn: [eventHubs]
}

module dataFactory 'modules/data_factory.bicep' = if (deployDataFactory) {
  name: 'datafactory-${suffix}'
  params: {
    location: location
    factoryName: 'adf-${baseName}'
    storageAccountName: storage.outputs.storageAccountName
    tags: tags
  }
}

output storageAccountName string = storage.outputs.storageAccountName
output lakehouseContainerUrl string = storage.outputs.lakehouseContainerUrl
output databricksWorkspaceName string = databricks.outputs.workspaceName
output databricksWorkspaceUrl string = databricks.outputs.workspaceUrl
output accessConnectorId string = databricks.outputs.accessConnectorId
output eventHubsNamespaceName string = eventHubs.outputs.namespaceName
output clickstreamEventHub string = eventHubs.outputs.clickstreamHubName
output iotEventHub string = eventHubs.outputs.iotHubName
output keyVaultName string = keyVault.outputs.vaultName
output logAnalyticsWorkspaceName string = monitoring.outputs.workspaceName
output dataFactoryName string = deployDataFactory ? 'adf-${baseName}' : 'not-deployed'

