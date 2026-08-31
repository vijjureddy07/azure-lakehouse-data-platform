# 05. Azure Data Factory & ADLS Gen2 Cloud Ingestion (Module 2 Study Guide)

> **Status:** Build Complete & Deployment-Ready | **Learning Status:** ⏳ NOT STUDIED / PENDING

---

## 1. Azure Data Lake Storage Gen2 (ADLS Gen2) & Hierarchical Namespace (HNS)

### WHAT IT IS
Azure Data Lake Storage Gen2 is Microsoft's dedicated data lake solution built on top of Azure Blob Storage. It converges the massive scalability and cost-efficiency of object storage with a true **Hierarchical Namespace (HNS)**.

### WHY HNS MATTERS (OBJECT STORE VS HIERARCHICAL NAMESPACE)
- **Standard Object Storage (Flat Blob):** Directories are virtual prefixes in a key-value store. Renaming or moving a "directory" containing 10,000 files requires 10,000 individual copy-and-delete API operations, resulting in massive I/O overhead and non-atomic transactions.
- **ADLS Gen2 with HNS:** Enables genuine filesystem-level directory hierarchies. Renaming a directory is an **atomic metadata pointer update** $O(1)$, exactly like a Linux filesystem.
- **Performance Impact:** Essential for big data engines (Apache Spark, Azure Databricks, Azure Synapse) when executing atomic directory commits during partition overwrites, job commits, and Delta Lake ACID transactions.

### STORAGE STRUCTURE IN THIS PROJECT
```
Storage Account: stlakehousedev (Kind: StorageV2, isHnsEnabled: true)
  └── Container (Filesystem): lakehouse
      └── landing/retail/<dataset_name>/ingestion_date=<yyyy-MM-dd>/run_id=<run_id>/<file_name>
```

### EXACT ARTIFACT REFERENCES
- [main.bicep](../infra/bicep/main.bicep#L18-L37) (`isHnsEnabled: true`)
- [arm_template.json](../infra/arm_template.json)
- [ls_adls_gen2.json](../adf/linkedService/ls_adls_gen2.json)

---

## 2. Azure Data Factory (ADF) Architecture

### CORE COMPONENTS
1. **Pipeline:** A logical grouping of activities that together perform a unit of work (e.g., orchestrating an entire batch of source files).
2. **Activity:** An individual processing step inside a pipeline (e.g., `Copy`, `ForEach`, `ExecutePipeline`, `Lookup`, `Web`).
3. **Linked Service:** The connection definition (connection string, endpoint URL, authentication mechanism) to an external data store or compute service. Analogy: A database connection string.
4. **Dataset:** A named view of data that points to or references data within a Linked Service (e.g., a specific folder, file format, or table). Analogy: A table or file pointer.
5. **Integration Runtime (IR):** The compute infrastructure that ADF uses to execute activities, perform data movements (Copy Activity), and dispatch external pipeline steps.
   - *Azure IR:* Fully managed, serverless, auto-scaling cloud compute for cloud-to-cloud data movement.
   - *Self-Hosted IR (SHIR):* Installed on on-premises VMs or private VNet infrastructure to copy data behind corporate firewalls.

---

## 3. Metadata-Driven Parameterized Ingestion Pattern

### THE ANTI-PATTERN: HARD-CODED PIPELINES
Creating 8 separate pipelines or 8 hard-coded Copy Activities in a canvas creates high maintenance debt. Adding a 9th dataset requires modifying visual pipeline code, re-testing, and deploying code changes.

### THE MODERN PATTERN: MASTER-CHILD ORCHESTRATION
Module 2 demonstrates an enterprise metadata-driven pattern using two pipelines:

```
[ Master Pipeline: pl_master_retail_ingestion ]
       │
       ├── Parameters: datasets_config (Array of 8 Dataset Metadata Objects)
       │
       └── ForEach Activity (Parallel batch execution across array items)
              │
              ▼
       [ Child Pipeline: pl_ingest_single_file ]
              │
              ├── Parameters: dataset_name, source_relative_url, destination_folder_path...
              │
              └── Copy Activity (HTTP Binary Source ──► ADLS Gen2 Binary Sink)
```

### DYNAMIC EXPRESSIONS IMPLEMENTATION
- Dynamic UTC Date String: `@formatDateTime(utcnow(), 'yyyy-MM-dd')`
- Unique Auditable Execution ID: `@pipeline().RunId`
- Dynamic Landing Directory:
  ```json
  "@concat('landing/retail/', item().dataset_name, '/ingestion_date=', formatDateTime(utcnow(), 'yyyy-MM-dd'), '/run_id=', pipeline().RunId)"
  ```

### EXACT ARTIFACT REFERENCES
- [pl_master_retail_ingestion.json](../adf/pipeline/pl_master_retail_ingestion.json)
- [pl_ingest_single_file.json](../adf/pipeline/pl_ingest_single_file.json)
- [ds_http_raw_file.json](../adf/dataset/ds_http_raw_file.json)
- [ds_adls_landing_file.json](../adf/dataset/ds_adls_landing_file.json)

---

## 4. Authentication, Managed Identity & Azure RBAC

### ZERO-KEY SECURITY PHILOSOPHY
Never commit storage account access keys, SAS tokens, passwords, or connection strings to Git. Access keys grant root-level superuser bypass over the entire storage account, creating severe security vulnerabilities.

### MANAGED IDENTITY (MICROSOFT ENTRA ID)
- Azure Data Factory is provisioned with a **System-Assigned Managed Identity** (`identity: { type: 'SystemAssigned' }`).
- Azure automatically manages identity lifecycle, token issuance, and authentication behind the scenes without requiring secret storage.

### AZURE RBAC VS POSIX ACLS
| Dimension | Azure RBAC (Role-Based Access Control) | ADLS Gen2 POSIX Access Control Lists (ACLs) |
| :--- | :--- | :--- |
| **Scope** | Coarse-grained (Subscription, Resource Group, Storage Account, Container) | Fine-grained (Specific folders and individual files) |
| **Role Applied** | `Storage Blob Data Contributor` (`ba92f5b4-2d11-453d-a403-e96b0029c9fe`) | Read (`r-x`), Write (`rwx`) POSIX masks on directory nodes |
| **Project Usage** | Used for ADF service-to-service access to the `lakehouse` container. | Used when granular user/group-level directory permissions are needed inside Databricks. |

### EXACT ARTIFACT REFERENCES
- [main.bicep](../infra/bicep/main.bicep#L51-L69) (`Microsoft.Authorization/roleAssignments`)
- [ls_adls_gen2.json](../adf/linkedService/ls_adls_gen2.json) (`type: "AzureBlobFS"`, Managed Identity)

---

## 5. Raw Data Preservation & Landing Zone Architecture

### PRINCIPLES OF RAW CLOUD INGESTION
1. **Fidelity Preservation:** Raw files must land **unchanged** in their original format (CSV stays CSV, JSON stays JSON Lines). No parsing, data type casting, or cleansing occurs during landing.
2. **Immutable Audit Trail:** Placing `@pipeline().RunId` in the storage path ensures every pipeline run creates an isolated, auditable folder snapshot:
   ```
   landing/retail/<dataset>/ingestion_date=<yyyy-MM-dd>/run_id=<RUN_ID>/<file>
   ```
3. **Byte-for-Byte SHA-256 Verification:** The live verifier (`scripts/verify_azure_deployment.py`) downloads landed files from the specific run and verifies that `source_bytes == landed_bytes` via SHA-256 checksums.
4. **Decoupling Landing from Lakehouse (Module 2 vs Module 3):**
   - *Module 2 (Landing):* Acquires external files and stores raw byte streams in ADLS Gen2.
   - *Module 3 (Bronze / Silver / Gold):* Reads landed files into Apache Spark / Delta Lake to apply schema enforcement, deduplication, and ACID transactions.

---

## 6. Infrastructure as Code & Automated Deployment

### CANONICAL BICEP PROVISIONING & DETERMINISTIC STORAGE NAMING
- Azure Storage Account names must be globally unique across all Azure tenants.
- [main.bicep](../infra/bicep/main.bicep) generates a deterministic valid name if omitted: `take('stlakehouse${environment}${uniqueString(resourceGroup().id)}', 24)`.
- The single canonical provisioning script [deploy_azure_resources.sh](../scripts/deploy_azure_resources.sh) deploys Bicep and dynamically extracts output values.

### CLI ARTIFACT DEPLOYMENT & PAYLOAD EXTRACTION
- ADF JSON files in `adf/` are kept in readable ARM resource wrapper formats for GitHub review.
- [deploy_adf_artifacts.py](../scripts/deploy_adf_artifacts.py) extracts the inner `.properties` dictionary to temporary files before passing to Azure CLI (`az datafactory linked-service create --properties`, `dataset create --properties`, `pipeline create --pipeline`), preventing ARM wrapper deployment errors.

---

## 7. Interview Questions & Expected Answers

### INTERVIEW QUESTION 1
> "What is the architectural advantage of enabling Hierarchical Namespace (HNS) in ADLS Gen2 compared to standard Azure Blob storage for data engineering pipelines?"

### EXPECTED ANSWER
> "Standard Blob Storage uses a flat object namespace where folders are merely virtual key prefixes. Operations like directory renames or deletions require O(N) copy-and-delete API calls across every individual blob. ADLS Gen2 with Hierarchical Namespace (HNS) implements true filesystem directory nodes, making directory renames and atomic moves O(1) metadata updates. This is critical for big data compute engines like Spark and Delta Lake, which rely on atomic directory rename operations for job commits, idempotent partition overwrites, and transactional integrity."

---

### INTERVIEW QUESTION 2
> "How do you build a metadata-driven ingestion framework in Azure Data Factory instead of maintaining dozens of individual copy pipelines?"

### EXPECTED ANSWER
> "In ADF, we implement a Master-Child pipeline pattern. The Master pipeline defines or retrieves an array of metadata objects (specifying dataset names, source endpoints, file formats, and sink paths). A ForEach activity iterates over this configuration array in parallel and calls a single, reusable parameterized Child pipeline via the ExecutePipeline activity. The Child pipeline accepts parameters for source URL, destination container, dynamic folder paths (e.g. formatted with UTC date and pipeline RunId), and executes a single generic Copy activity. When onboarding a new dataset, we simply append a new metadata configuration object without touching pipeline code."

---

### INTERVIEW QUESTION 3
> "Why is Managed Identity with Azure RBAC preferred over Storage Account Keys or SAS Tokens in enterprise Data Factory deployments?"

### EXPECTED ANSWER
> "Storage Account Keys provide unrestricted root access to the entire storage account and cannot be scoped or audited by identity. SAS tokens expire and require manual rotation and secure secret storage (e.g. in Azure Key Vault). System-Assigned Managed Identity leverages Microsoft Entra ID to authenticate ADF directly to ADLS Gen2 with zero stored credentials. We grant ADF the least-privilege role 'Storage Blob Data Contributor' over the storage account scope. Microsoft handles token acquisition, rotation, and lifecycle management automatically, eliminating credential leakage risk."

