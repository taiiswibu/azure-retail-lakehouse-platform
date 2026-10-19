param location string
param factoryName string
param storageAccountName string
param tags object

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

resource factory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: factoryName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

var storageBlobDataContributorRole = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)

resource factoryStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, factory.id, storageBlobDataContributorRole)
  scope: storage
  properties: {
    principalId: factory.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: storageBlobDataContributorRole
  }
}

resource adlsLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: factory
  name: 'LS_ADLS_GEN2'
  properties: {
    type: 'AzureBlobFS'
    typeProperties: {
      url: 'https://${storage.name}.dfs.${environment().suffixes.storage}'
    }
    annotations: ['Managed identity; no storage key']
  }
}

resource sourceDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: factory
  name: 'DS_BINARY_SOURCE'
  properties: {
    linkedServiceName: {
      referenceName: 'LS_ADLS_GEN2'
      type: 'LinkedServiceReference'
    }
    parameters: {
      folderPath: { type: 'String' }
      fileName: { type: 'String' }
    }
    type: 'Binary'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileSystem: 'lakehouse'
        folderPath: {
          value: '@dataset().folderPath'
          type: 'Expression'
        }
        fileName: {
          value: '@dataset().fileName'
          type: 'Expression'
        }
      }
    }
  }
}

resource sinkDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: factory
  name: 'DS_BINARY_SINK'
  properties: {
    linkedServiceName: {
      referenceName: 'LS_ADLS_GEN2'
      type: 'LinkedServiceReference'
    }
    parameters: {
      folderPath: { type: 'String' }
      fileName: { type: 'String' }
    }
    type: 'Binary'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileSystem: 'lakehouse'
        folderPath: {
          value: '@dataset().folderPath'
          type: 'Expression'
        }
        fileName: {
          value: '@dataset().fileName'
          type: 'Expression'
        }
      }
    }
  }
}

resource metadataPipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  parent: factory
  name: 'PL_METADATA_BATCH_INGEST'
  properties: {
    description: 'Metadata-driven binary copy from source-drop to the governed landing path.'
    parameters: {
      items: {
        type: 'Array'
        defaultValue: []
      }
    }
    activities: [
      {
        name: 'ForEachDataset'
        type: 'ForEach'
        typeProperties: {
          isSequential: false
          batchCount: 4
          items: {
            value: '@pipeline().parameters.items'
            type: 'Expression'
          }
          activities: [
            {
              name: 'CopyToLanding'
              type: 'Copy'
              policy: {
                timeout: '0.02:00:00'
                retry: 2
                retryIntervalInSeconds: 30
                secureInput: false
                secureOutput: false
              }
              inputs: [
                {
                  referenceName: 'DS_BINARY_SOURCE'
                  type: 'DatasetReference'
                  parameters: {
                    folderPath: {
                      value: '@item().sourceFolder'
                      type: 'Expression'
                    }
                    fileName: {
                      value: '@item().fileName'
                      type: 'Expression'
                    }
                  }
                }
              ]
              outputs: [
                {
                  referenceName: 'DS_BINARY_SINK'
                  type: 'DatasetReference'
                  parameters: {
                    folderPath: {
                      value: '@item().sinkFolder'
                      type: 'Expression'
                    }
                    fileName: {
                      value: '@item().fileName'
                      type: 'Expression'
                    }
                  }
                }
              ]
              typeProperties: {
                source: {
                  type: 'BinarySource'
                  storeSettings: {
                    type: 'AzureBlobFSReadSettings'
                    recursive: false
                  }
                }
                sink: {
                  type: 'BinarySink'
                  storeSettings: {
                    type: 'AzureBlobFSWriteSettings'
                  }
                }
                enableStaging: false
              }
            }
          ]
        }
      }
    ]
  }
  dependsOn: [sourceDataset, sinkDataset]
}

output factoryName string = factory.name
output factoryId string = factory.id

