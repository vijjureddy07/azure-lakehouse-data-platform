#!/usr/bin/env bash
# ==============================================================================
# Azure Cloud Ingestion Platform — Canonical Bicep Deployment Script (Module 2)
# ==============================================================================
# Canonical Workflow:
# 1. Verify Azure CLI Authentication
# 2. Create Azure Resource Group
# 3. Deploy infra/bicep/main.bicep (ADLS Gen2 HNS + ADF Managed Identity + RBAC)
# 4. Extract dynamic deployment outputs (Storage Account, Data Factory, Container)
# 5. Deploy ADF Linked Services, Datasets, and Pipelines using extracted properties payloads
# 6. Trigger Master Pipeline (pl_master_retail_ingestion)
# 7. Poll until terminal state and verify 'Succeeded'
# 8. Run live verification with exact successful RUN_ID
# ==============================================================================

set -euo pipefail

RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-rg-lakehouse-dev-eastus}"
LOCATION="${AZURE_LOCATION:-eastus}"
ENVIRONMENT="${AZURE_ENV:-dev}"
STORAGE_ACCOUNT_NAME_OVERRIDE="${AZURE_STORAGE_ACCOUNT:-}"
DATA_FACTORY_NAME="${AZURE_DATA_FACTORY:-adf-lakehouse-${ENVIRONMENT}}"
CONTAINER_NAME="${AZURE_CONTAINER:-lakehouse}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=============================================================================="
echo "AZURE LAKEHOUSE PLATFORM — MODULE 2 CLOUD INGESTION DEPLOYMENT"
echo "Resource Group:       ${RESOURCE_GROUP}"
echo "Location:             ${LOCATION}"
echo "Environment:          ${ENVIRONMENT}"
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
echo "Active Azure Subscription: ${SUBSCRIPTION_NAME} (${SUBSCRIPTION_ID})"

# 2. Create Resource Group if not exists
echo "--> 1/5 Ensuring Resource Group '${RESOURCE_GROUP}' in '${LOCATION}'..."
az group create \
    --name "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --tags Environment="${ENVIRONMENT}" Project="AzureLakehouseDataPlatform" Module="Module2" \
    --output table

# 3. Deploy Canonical Infrastructure via Bicep
echo "--> 2/5 Deploying Canonical Infrastructure via Bicep (infra/bicep/main.bicep)..."

BICEP_PARAMS=("location=${LOCATION}" "environment=${ENVIRONMENT}" "dataFactoryName=${DATA_FACTORY_NAME}" "containerName=${CONTAINER_NAME}")
if [[ -n "${STORAGE_ACCOUNT_NAME_OVERRIDE}" ]]; then
    BICEP_PARAMS+=("storageAccountName=${STORAGE_ACCOUNT_NAME_OVERRIDE}")
fi

DEPLOYMENT_OUTPUT_JSON=$(mktemp)
trap 'rm -f "${DEPLOYMENT_OUTPUT_JSON}"' EXIT

az deployment group create \
    --resource-group "${RESOURCE_GROUP}" \
    --template-file "${REPO_ROOT}/infra/bicep/main.bicep" \
    --parameters "${BICEP_PARAMS[@]}" \
    --output json > "${DEPLOYMENT_OUTPUT_JSON}"

# 4. Extract Dynamic Deployment Outputs
STORAGE_ACCOUNT_NAME=$(python3 -c "import json; print(json.load(open('${DEPLOYMENT_OUTPUT_JSON}'))['properties']['outputs']['storageAccountNameOut']['value'])")
DEPLOYED_DATA_FACTORY=$(python3 -c "import json; print(json.load(open('${DEPLOYMENT_OUTPUT_JSON}'))['properties']['outputs']['dataFactoryNameOut']['value'])")
DEPLOYED_CONTAINER=$(python3 -c "import json; print(json.load(open('${DEPLOYMENT_OUTPUT_JSON}'))['properties']['outputs']['containerNameOut']['value'])")
ADF_PRINCIPAL_ID=$(python3 -c "import json; print(json.load(open('${DEPLOYMENT_OUTPUT_JSON}'))['properties']['outputs']['dataFactoryPrincipalId']['value'])")

echo "--> Infrastructure Deployed Successfully:"
echo "    Storage Account:  ${STORAGE_ACCOUNT_NAME} (ADLS Gen2 HNS Enabled)"
echo "    Data Factory:     ${DEPLOYED_DATA_FACTORY}"
echo "    Container:        ${DEPLOYED_CONTAINER}"
echo "    ADF Principal ID: ${ADF_PRINCIPAL_ID}"

# 5. Deploy ADF Linked Services, Datasets, and Pipelines using Extracted Properties Payloads
echo "--> 3/5 Deploying ADF Artifacts using extracted properties payloads..."
python3 "${REPO_ROOT}/scripts/deploy_adf_artifacts.py" \
    --resource-group "${RESOURCE_GROUP}" \
    --factory-name "${DEPLOYED_DATA_FACTORY}" \
    --adf-dir "${REPO_ROOT}/adf"

# 6. Trigger Master Pipeline Run
echo "--> 4/5 Triggering Master Ingestion Pipeline (pl_master_retail_ingestion)..."
RUN_ID=$(az datafactory pipeline create-run \
    --factory-name "${DEPLOYED_DATA_FACTORY}" \
    --resource-group "${RESOURCE_GROUP}" \
    --name "pl_master_retail_ingestion" \
    --parameters storage_account_name="${STORAGE_ACCOUNT_NAME}" destination_container="${DEPLOYED_CONTAINER}" \
    --query runId \
    --output tsv)

echo "Master Pipeline triggered! Run ID: ${RUN_ID}"
echo "Polling pipeline run status until completion..."

# 7. Poll until Terminal State
while true; do
    STATUS=$(az datafactory pipeline-run show \
        --factory-name "${DEPLOYED_DATA_FACTORY}" \
        --resource-group "${RESOURCE_GROUP}" \
        --run-id "${RUN_ID}" \
        --query status \
        --output tsv)

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Pipeline Run: ${RUN_ID} | Status: ${STATUS}"

    if [[ "${STATUS}" == "Succeeded" ]]; then
        echo "Master Pipeline Run ${RUN_ID} Succeeded!"
        break
    elif [[ "${STATUS}" == "Failed" || "${STATUS}" == "Cancelled" ]]; then
        echo "ERROR: Pipeline Run ${RUN_ID} finished with failure state: ${STATUS}"
        exit 1
    fi
    sleep 10
done

# 8. Run Live Cloud Verification with Exact Run ID
echo "--> 5/5 Running Live Cloud Verification on Run ID: ${RUN_ID}..."
python3 "${REPO_ROOT}/scripts/verify_azure_deployment.py" \
    --resource-group "${RESOURCE_GROUP}" \
    --storage-account "${STORAGE_ACCOUNT_NAME}" \
    --data-factory "${DEPLOYED_DATA_FACTORY}" \
    --container "${DEPLOYED_CONTAINER}" \
    --run-id "${RUN_ID}"

echo "=============================================================================="
echo "DEPLOYMENT & LIVE CLOUD VERIFICATION COMPLETED SUCCESSFULLY!"
echo "=============================================================================="
