param location string
param namespaceName string
param tags object

resource namespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: namespaceName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    isAutoInflateEnabled: false
    maximumThroughputUnits: 0
    publicNetworkAccess: 'Enabled'
    disableLocalAuth: false
    minimumTlsVersion: '1.2'
  }
}

resource clickstream 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: namespace
  name: 'clickstream'
  properties: {
    partitionCount: 4
    messageRetentionInDays: 1
  }
}

resource iot 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: namespace
  name: 'iot-telemetry'
  properties: {
    partitionCount: 4
    messageRetentionInDays: 1
  }
}

resource consumerRule 'Microsoft.EventHub/namespaces/authorizationRules@2024-01-01' = {
  parent: namespace
  name: 'databricks-consumer'
  properties: {
    rights: ['Listen']
  }
}

resource producerRule 'Microsoft.EventHub/namespaces/authorizationRules@2024-01-01' = {
  parent: namespace
  name: 'demo-producer'
  properties: {
    rights: ['Send']
  }
}

output namespaceName string = namespace.name
output clickstreamHubName string = last(split(clickstream.name, '/'))
output iotHubName string = last(split(iot.name, '/'))
output consumerRuleId string = consumerRule.id
output producerRuleId string = producerRule.id

