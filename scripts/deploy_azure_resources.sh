#!/usr/bin/env bash
# ==============================================================================
# Azure Cloud Ingestion Platform — Provisioning & ADF Deployment Script (Module 2)
# ==============================================================================
# Automates:
# 1. Azure Resource Group creation
# 2. ADLS Gen2 Storage Account provisioning with Hierarchical Namespace (HNS)
# 3. Primary 'lakehouse' container creation
# 4. Azure Data Factory provisioning with System-Assigned Managed Identity
# 5. Azure RBAC 'Storage Blob Data Contributor' assignment to ADF identity
# 6. Publishing ADF Linked Services, Datasets, and Parameterized Pipelines
# 7. Triggering initial Master Ingestion Pipeline execution (pl_master_retail_ingestion)
# ==============================================================================

set -euo pipefail

# Configurable Parameters (Override with environment variables if desired)
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-rg-lakehouse-dev-eastus}"
LOCATION="${AZURE_LOCATION:-eastus}"
ENVIRONMENT="${AZURE_ENV:-dev}"
STORAGE_ACCOUNT_NAME="${AZURE_STORAGE_ACCOUNT:-stlakehouse${ENVIRONMENT}}"
DATA_FACTORY_NAME="${AZURE_DATA_FACTORY:-adf-lakehouse-${ENVIRONMENT}}"
CONTAINER_NAME="${AZURE_CONTAINER:-lakehouse}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=============================================================================="
echo "AZURE LAKEHOUSE DATA PLATFORM — PROVISIONING & DEPLOYMENT"
echo "Resource Group:       ${RESOURCE_GROUP}"
echo "Location:             ${LOCATION}"
echo "Storage Account:      ${STORAGE_ACCOUNT_NAME} (ADLS Gen2 HNS Enabled)"
echo "Data Factory:         ${DATA_FACTORY_NAME}"
echo "Container:            ${CONTAINER_NAME}"
echo "=============================================================================="

# 1. Verify Azure CLI Authentication
if ! command -v az &> /dev/null; then
    echo "ERROR: Azure CLI ('az') is not installed. Please install it to execute cloud deployments."
    exit 1
fi

echo "Checking Azure CLI authentication status..."
if ! az account show &> /dev/null; then
    echo "ERROR: You are not logged into Azure. Please run 'az login' before running this script."
    exit 1
fi

SUBSCRIPTION_ID=$(az account show --query id -o tsv)
SUBSCRIPTION_NAME=$(az account show --query name -o tsv)
echo "Active Subscription: ${SUBSCRIPTION_NAME} (${SUBSCRIPTION_ID})"

# 2. Create Resource Group
echo "--> Creating Resource Group: ${RESOURCE_GROUP} in ${LOCATION}..."
az group create \
    --name "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --tags Environment="${ENVIRONMENT}" Project="AzureLakehouseDataPlatform" Module="Module2" \
    --output table

# 3. Create ADLS Gen2 Storage Account (Hierarchical Namespace Enabled)
echo "--> Provisioning ADLS Gen2 Storage Account: ${STORAGE_ACCOUNT_NAME}..."
az storage account create \
    --name "${STORAGE_ACCOUNT_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --enable-hierarchical-namespace true \
    --https-only true \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false \
    --tags Environment="${ENVIRONMENT}" Project="AzureLakehouseDataPlatform" \
    --output table

# 4. Create Lakehouse Filesystem / Container
echo "--> Creating ADLS Gen2 Container: ${CONTAINER_NAME}..."
az storage fs create \
    --name "${CONTAINER_NAME}" \
    --account-name "${STORAGE_ACCOUNT_NAME}" \
    --auth-mode login \
    --output table

# 5. Provision Azure Data Factory with System-Assigned Managed Identity
echo "--> Provisioning Azure Data Factory: ${DATA_FACTORY_NAME}..."
az datafactory create \
    --name "${DATA_FACTORY_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --tags Environment="${ENVIRONMENT}" Project="AzureLakehouseDataPlatform" \
    --output table

# Retrieve ADF System-Assigned Identity Principal ID
echo "--> Retrieving ADF Managed Identity Principal ID..."
ADF_PRINCIPAL_ID=$(az datafactory show \
    --name "${DATA_FACTORY_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --query "identity.principalId" \
    --output tsv)

echo "ADF Principal ID: ${ADF_PRINCIPAL_ID}"

# 6. Assign Azure RBAC: Storage Blob Data Contributor to ADF
echo "--> Assigning 'Storage Blob Data Contributor' RBAC role to ADF Managed Identity..."
STORAGE_ACCOUNT_ID=$(az storage account show \
    --name "${STORAGE_ACCOUNT_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --query id \
    --output tsv)

az role assignment create \
    --assignee-object-id "${ADF_PRINCIPAL_ID}" \
    --assignee-principal-type ServicePrincipal \
    --role "Storage Blob Data Contributor" \
    --scope "${STORAGE_ACCOUNT_ID}" \
    --output table

# 7. Deploy ADF Linked Services
echo "--> Deploying ADF Linked Services..."
az datafactory linked-service create \
    --factory-name "${DATA_FACTORY_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --name "ls_http_source" \
    --properties @"${REPO_ROOT}/adf/linkedService/ls_http_source.json" \
    --output table

az datafactory linked-service create \
    --factory-name "${DATA_FACTORY_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --name "ls_adls_gen2" \
    --properties @"${REPO_ROOT}/adf/linkedService/ls_adls_gen2.json" \
    --output table

# 8. Deploy ADF Datasets
echo "--> Deploying ADF Datasets..."
az datafactory dataset create \
    --factory-name "${DATA_FACTORY_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --name "ds_http_raw_file" \
    --properties @"${REPO_ROOT}/adf/dataset/ds_http_raw_file.json" \
    --output table

az datafactory dataset create \
    --factory-name "${DATA_FACTORY_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --name "ds_adls_landing_file" \
    --properties @"${REPO_ROOT}/adf/dataset/ds_adls_landing_file.json" \
    --output table

# 9. Deploy ADF Pipelines
echo "--> Deploying ADF Pipelines..."
az datafactory pipeline create \
    --factory-name "${DATA_FACTORY_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --name "pl_ingest_single_file" \
    --pipeline @"${REPO_ROOT}/adf/pipeline/pl_ingest_single_file.json" \
    --output table

az datafactory pipeline create \
    --factory-name "${DATA_FACTORY_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --name "pl_master_retail_ingestion" \
    --pipeline @"${REPO_ROOT}/adf/pipeline/pl_master_retail_ingestion.json" \
    --output table

# 10. Trigger Pipeline Run
echo "=============================================================================="
echo "DEPLOYMENT COMPLETE! Triggering Master Ingestion Pipeline Run..."
echo "=============================================================================="

RUN_ID=$(az datafactory pipeline create-run \
    --factory-name "${DATA_FACTORY_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --name "pl_master_retail_ingestion" \
    --parameters storage_account_name="${STORAGE_ACCOUNT_NAME}" destination_container="${CONTAINER_NAME}" \
    --query runId \
    --output tsv)

echo "Master Pipeline triggered successfully!"
echo "Pipeline Run ID: ${RUN_ID}"
echo "To monitor status:"
echo "  az datafactory pipeline-run show --factory-name ${DATA_FACTORY_NAME} --resource-group ${RESOURCE_GROUP} --run-id ${RUN_ID} --output table"
echo "To verify landed files:"
echo "  python scripts/verify_azure_deployment.py --storage-account ${STORAGE_ACCOUNT_NAME} --container ${CONTAINER_NAME}"
