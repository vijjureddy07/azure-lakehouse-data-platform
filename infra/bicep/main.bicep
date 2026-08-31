@description('Primary Azure deployment location')
param location string = resourceGroup().location

@description('Deployment environment tier')
@allowed([
  'dev'
  'test'
  'prod'
])
param environment string = 'dev'

@description('Name of ADLS Gen2 storage account (must be globally unique, lowercase alphanumeric)')
param storageAccountName string = 'stlakehouse${environment}'

@description('Name of Azure Data Factory resource')
param dataFactoryName string = 'adf-lakehouse-${environment}'

@description('Name of the primary ADLS Gen2 filesystem container')
param containerName string = 'lakehouse'

// 1. ADLS Gen2 Storage Account with Hierarchical Namespace (HNS)
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
  }
  tags: {
    Environment: environment
    Project: 'AzureLakehouseDataPlatform'
    Module: 'Module2_CloudIngestion'
  }
}

// 2. Primary Blob Service & Lakehouse Container
resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

resource lakehouseContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobServices
  name: containerName
  properties: {
    publicAccess: 'None'
  }
}

// 3. Azure Data Factory with System-Assigned Managed Identity
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  tags: {
    Environment: environment
    Project: 'AzureLakehouseDataPlatform'
    Module: 'Module2_CloudIngestion'
  }
}

// 4. Role Assignment: Storage Blob Data Contributor for ADF Managed Identity
// Role Definition ID for "Storage Blob Data Contributor": ba92f5b4-2d11-453d-a403-e96b0029c9fe
var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource roleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, dataFactory.id, storageBlobDataContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output storageAccountId string = storageAccount.id
output storageAccountNameOut string = storageAccount.name
output dataFactoryId string = dataFactory.id
output dataFactoryNameOut string = dataFactory.name
output dataFactoryPrincipalId string = dataFactory.identity.principalId
output containerNameOut string = lakehouseContainer.name
