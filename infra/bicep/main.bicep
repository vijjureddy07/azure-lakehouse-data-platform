@description('Primary Azure deployment location')
param location string = resourceGroup().location

@description('Deployment environment tier')
@allowed([
  'dev'
  'test'
  'prod'
])
param environment string = 'dev'

@description('Name of ADLS Gen2 storage account (must be lowercase alphanumeric, <= 24 chars). If empty, generates unique name.')
param storageAccountName string = ''

@description('Name of Azure Data Factory resource')
param dataFactoryName string = 'adf-lakehouse-${environment}'

@description('Name of the primary ADLS Gen2 filesystem container')
param containerName string = 'lakehouse'

var uniqueStorageSuffix = uniqueString(resourceGroup().id)
var resolvedStorageAccountName = !empty(storageAccountName) ? toLower(storageAccountName) : take('stlakehouse${environment}${uniqueStorageSuffix}', 24)

// 1. ADLS Gen2 Storage Account with Hierarchical Namespace (HNS)
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: resolvedStorageAccountName
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

// 5. Azure Databricks Workspace (Premium tier for Unity Catalog support)
@description('Name of Azure Databricks workspace')
param databricksWorkspaceName string = 'dbw-lakehouse-${environment}'

resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: databricksWorkspaceName
  location: location
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', 'rg-dbw-managed-${environment}-${uniqueStorageSuffix}')
  }
  tags: {
    Environment: environment
    Project: 'AzureLakehouseDataPlatform'
    Module: 'Module3_Databricks_Delta_Medallion'
  }
}

// 6. Azure Databricks Access Connector (Managed Identity for Unity Catalog storage credentials)
@description('Name of Databricks Access Connector')
param accessConnectorName string = 'dbx-access-connector-${environment}'

resource databricksAccessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: accessConnectorName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  tags: {
    Environment: environment
    Project: 'AzureLakehouseDataPlatform'
    Module: 'Module3_Databricks_Delta_Medallion'
  }
}

// 7. Role Assignment: Storage Blob Data Contributor for Databricks Access Connector Managed Identity
resource dbxAccessConnectorRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, databricksAccessConnector.id, storageBlobDataContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalId: databricksAccessConnector.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output storageAccountId string = storageAccount.id
output storageAccountNameOut string = storageAccount.name
output dataFactoryId string = dataFactory.id
output dataFactoryNameOut string = dataFactory.name
output dataFactoryPrincipalId string = dataFactory.identity.principalId
output containerNameOut string = lakehouseContainer.name
output databricksWorkspaceId string = databricksWorkspace.id
output databricksWorkspaceNameOut string = databricksWorkspace.name
output databricksAccessConnectorId string = databricksAccessConnector.id
output databricksAccessConnectorPrincipalId string = databricksAccessConnector.identity.principalId

