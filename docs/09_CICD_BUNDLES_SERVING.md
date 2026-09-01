# Module 6: Production CI/CD, Declarative Automation Bundles & Governed SQL Serving

## 1. Executive Summary & Architectural Overview

Module 6 operationalizes the omnichannel retail data platform with enterprise-grade release automation, zero-secret continuous deployment, and a governed analytical serving layer.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        MODULE 6 CAPABILITIES                           │
│                                                                        │
│  1. Python Release Packaging: Wheel (.whl) distribution via setuptools │
│  2. Declarative Automation Bundles: Multi-environment Databricks IaC   │
│  3. Automated CI Pipeline: GitHub Actions running Ruff, Pytest & Wheel │
│  4. Zero-Secret CD: Workload Identity Federation (GitHub OIDC)         │
│  5. Governed SQL Serving: Serverless Databricks SQL Warehouse & Views  │
└────────────────────────────────────────────────────────────────────────┘
```

```mermaid
graph TD
    subgraph CI_CD["Continuous Integration & Deployment (GitHub Actions)"]
        PR["Pull Request / Push to main"] --> CI["CI Workflow (.github/workflows/ci.yml)"]
        CI --> Lint["1. Ruff Static Analysis"]
        CI --> Test["2. Full Pytest Suite (Modules 1–6)"]
        CI --> Wheel["3. Build & Smoke Test Python Wheel (.whl)"]
        CI --> BundleCheck["4. Bundle & Serving Contract Validation"]
        
        ManualTrigger["Operator Dispatch (workflow_dispatch)"] --> CD["CD Workflow (.github/workflows/deploy_databricks.yml)"]
        CD --> CLIPin["Databricks CLI Setup (Pinned to 1.10.0)"]
        CLIPin --> OIDC["GitHub OIDC Token Exchange (No Stored Secrets)"]
        OIDC --> SP["Databricks Service Principal (BUNDLE_VAR_deployment_sp_id)"]
        SP --> VersionCheck["databricks version"]
        VersionCheck --> Validate["databricks bundle validate --target prod"]
        Validate --> Deploy["databricks bundle deploy --target prod"]
    end

    subgraph Databricks_Platform["Azure Databricks Workspace"]
        Deploy --> LakeflowJob["Lakeflow Job: Retail Lakehouse Batch Pipeline"]
        Deploy --> SQLWH["Serverless SQL Warehouse (PRO, 2X-Small)"]
        Deploy --> ServingJob["Serving Setup Job (04_serving_views.sql)"]
        
        ServingJob --> ServingSchema["Unity Catalog: <catalog>.serving"]
        ServingSchema --> Views["8 Governed Serving Views"]
        Views --> Consumers["SQL Analysts / BI Dashboards / Future Power BI"]
    end

    style CI_CD fill:#f5f5f5,stroke:#333,stroke-width:2px;
    style Databricks_Platform fill:#e3f2fd,stroke:#1565c0,stroke-width:2px;
    style OIDC fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
    style SQLWH fill:#fff3e0,stroke:#e65100,stroke-width:2px;
```

---

## 2. Python Release Packaging

The platform logic across Modules 1–5 is packaged as a standard Python release wheel (`retail_lakehouse_data_platform-0.1.0-py3-none-any.whl`) via `pyproject.toml` and `build`:

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "retail-lakehouse-data-platform"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "pyspark>=3.5.0,<3.6.0",
    "delta-spark>=3.2.0,<3.3.0",
    "pyarrow>=14.0.0",
    "Faker>=28.0.0",
    "pyyaml>=6.0.0",
]
```

### Packaging & Orchestration Boundary
- **Wheel (`.whl`):** Release artifact containing the reusable library code (`src/medallion`, `src/modeling`, `src/orchestration`, `src/quality`, `src/schemas`).
- **Lakeflow Tasks (`databricks/tasks/`):** Thin task wrapper notebooks that import from the installed wheel or workspace files, retrieve Lakeflow widgets, execute with in-process retries, and set Lakeflow task values.

---

## 3. Declarative Automation Bundle Architecture

Databricks **Declarative Automation Bundles** (historically called *Databricks Asset Bundles*) manage all lakehouse resources as code.

### Root Bundle Specification (`databricks.yml`)
```yaml
bundle:
  name: "retail-lakehouse-data-platform"

include:
  - "databricks/jobs/*.yml"
  - "databricks/resources/*.yml"

artifacts:
  retail_lakehouse_wheel:
    type: whl
    path: .
    build: python -m build --wheel

targets:
  dev:
    mode: development
    default: true
    workspace:
      root_path: "/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}"
    variables:
      environment: "dev"
      catalog_name: "retail_lakehouse"
      storage_account_name: "stlakehousedev"
      container_name: "lakehouse"
      serving_warehouse_name: "retail_lakehouse_serving_wh_dev"

  prod:
    mode: production
    workspace:
      root_path: "/Shared/.bundle/${bundle.name}/${bundle.target}"
    run_as:
      service_principal_name: "${var.deployment_sp_id}"
    variables:
      environment: "prod"
      catalog_name: "retail_lakehouse_prod"
      storage_account_name: "stlakehouseprod"
      container_name: "lakehouse"
      serving_warehouse_name: "retail_lakehouse_serving_wh_prod"
```

### Target Isolation & Parameterization
- **`dev` Target:** Uses `mode: development`, deploys to the developer's user workspace path, prefixes resource names, and uses development storage accounts.
- **`prod` Target:** Uses `mode: production`, runs under a governed Service Principal identity (`run_as: { service_principal_name: "${var.deployment_sp_id}" }`), and targets production storage accounts and catalogs.
- **Job Parameter Compatibility:** `retail_lakehouse_job.yml` defines default values referencing Bundle variables (`${var.environment}`, `${var.storage_account_name}`, `${var.catalog_name}`). Redundant task-level `base_parameters` have been eliminated to comply with Databricks Bundle validation rules, relying on native job parameter pushdown and `dbutils.jobs.taskValues` for runtime task communication.

---

## 4. GitHub Actions CI/CD & Zero-Secret OIDC Authentication

### Continuous Integration (`.github/workflows/ci.yml`)
Runs automatically on every Pull Request and push to `main`:
1. Checks out repository.
2. Configures Python 3.11 and Java 17 (Temurin).
3. Executes Ruff static analysis (`ruff check .`).
4. Executes complete Pytest suite (102 tests across Modules 1–6).
5. Builds the Python wheel (`python -m build --wheel`).
6. Installs wheel in an isolated clean virtualenv and verifies smoke imports.
7. Validates bundle configuration and serving view SQL syntax.

### Continuous Deployment (`.github/workflows/deploy_databricks.yml`)
- **Safe by Default:** Triggered manually via `workflow_dispatch` (not automated on merge).
- **Concurrency Control:** `concurrency: { group: databricks-deployment-${{ inputs.target }}, cancel-in-progress: false }` ensures zero overlapping deployments.
- **Databricks CLI Pin:** Explicitly pinned to stable Databricks CLI `1.10.0` (`uses: databricks/setup-cli@main` with `version: "1.10.0"`).
- **Version Verification:** Executes `databricks version` prior to validation.
- **Workload Identity Federation (GitHub OIDC):**
  - Requests token: `permissions: { id-token: write, contents: read }`.
  - Configures `DATABRICKS_AUTH_TYPE: "github-oidc"`.
  - Exposes `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, and passes `BUNDLE_VAR_deployment_sp_id: ${{ vars.DATABRICKS_CLIENT_ID }}`.
  - **Zero Stored Secrets:** No Personal Access Tokens (PATs) or long-lived client secrets are stored in GitHub Secrets.

---

## 5. Governed SQL Serving Architecture

The serving layer exposes governed analytical SQL views over the Gold KPI aggregate tables and the Kimball dimensional warehouse without copying underlying data.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        SERVING ARCHITECTURE                            │
│                                                                        │
│  COMPUTE: Serverless Databricks SQL Warehouse (PRO SKU, 2X-Small)      │
│  SCHEMA:  <catalog>.serving (parameterized via :catalog_name)          │
│  OBJECTS: 8 Governed SQL Views                                         │
└────────────────────────────────────────────────────────────────────────┘
```

### Serverless SQL Warehouse Resource (`databricks/resources/sql_serving.yml`)
```yaml
resources:
  sql_warehouses:
    retail_lakehouse_serving_warehouse:
      name: "${var.serving_warehouse_name}"
      cluster_size: "2X-Small"
      min_num_clusters: 1
      max_num_clusters: 1
      auto_stop_mins: 10
      warehouse_type: "PRO"
      enable_serverless_compute: true
```

### Catalog Parameterization (`databricks/sql/04_serving_views.sql`)
```sql
USE CATALOG IDENTIFIER(:catalog_name);

CREATE SCHEMA IF NOT EXISTS serving
COMMENT 'Governed analytical serving layer for SQL analysts, BI tools, and reporting dashboards';

USE SCHEMA serving;
```

### Governed Serving Views Specification

| Serving View Name | Source Layer | Underlying Tables | Key Columns & Frozen Schema Alignment |
| :--- | :--- | :--- | :--- |
| `daily_sales_performance` | Gold | `gold.gold_daily_sales_performance` | `order_date`, `total_orders`, `total_units_sold`, `gross_revenue`, `total_discounts`, `net_sales`, `total_cogs`, `gross_profit`, `returns_count`, `total_refunded_amount` |
| `monthly_revenue` | Gold | `gold.gold_monthly_revenue` | `year`, `month`, `total_orders`, `total_units_sold`, `total_net_revenue`, `total_refunded_amount`, `net_retained_revenue` |
| `store_region_revenue` | Gold | `gold.gold_revenue_by_store_region` | `store_id`, `store_name`, `store_type`, `region`, `state`, `country`, `total_orders`, `total_net_revenue`, `avg_order_value` *(City excluded per frozen Gold schema)* |
| `category_revenue_performance` | Gold | `gold.gold_category_revenue_performance` | `category`, `subcategory`, `units_sold`, `gross_revenue`, `total_discounts`, `net_revenue`, `units_returned`, `total_refunded_amount`, `return_rate_pct` *(Uses `subcategory`)* |
| `customer_spending_summary` | Gold | `gold.gold_customer_spending_summary` | `customer_id`, `first_name`, `last_name`, `email`, `loyalty_tier`, `total_orders`, `lifetime_spend`, `first_order_date`, `latest_order_date`, `avg_order_value` |
| `return_refund_performance` | Gold | `gold.gold_return_refund_performance` | `return_reason`, `return_count`, `total_refund_amount`, `avg_refund_amount` |
| `sales_detail` | Warehouse | `warehouse.fact_sales` + Dimensions | Fact columns (`order_item_id`, `order_id`, `order_timestamp`, `order_status`, `channel`, `quantity`, `unit_price`, `gross_amount`, `discount_amount`, `net_amount`, `cost_amount`, `profit_amount`) + Temporal date keys + SCD2 customer (joined on `customer_key`) + SCD1 product (`subcategory`, `product_current_unit_price`) + Store (`region`, `state`, `country`). **Zero dim_employee join to preserve fact grain**. |
| `returns_detail` | Warehouse | `warehouse.fact_returns` + Dimensions | `return_id`, `order_item_id`, `order_id`, `return_timestamp`, `return_reason`, `return_status`, `refund_amount`, `return_date_key` + Customer (surrogate inherited from original sale) + Product + Store. |

---

## 6. Point-in-Time SCD2 Join Correctness in `sales_detail`

In the `sales_detail` serving view, joining `fact_sales` to `dim_customer` is performed strictly on `customer_key`:

```sql
SELECT
    s.order_item_id,
    s.order_id,
    s.order_timestamp,
    s.net_amount,
    s.profit_amount,
    c.loyalty_tier AS customer_historical_loyalty_tier,
    c.city AS customer_historical_city
FROM warehouse.fact_sales s
JOIN warehouse.dim_customer c ON s.customer_key = c.customer_key
...
```

### Why This Is Architecturally Critical:
- `fact_sales.customer_key` was already point-in-time resolved during the Module 4 warehouse load (`order_timestamp >= effective_from AND (order_timestamp < effective_to OR effective_to IS NULL)`).
- Joining on `customer_key` in the serving layer guarantees that analysts see the **exact customer loyalty tier and address that was valid when the purchase occurred**, without needing expensive or error-prone range joins in BI tools.
- In `returns_detail`, `customer_key` is inherited from the associated original sale record.

---

## 7. Verification Status & Cloud Honesty

- **Local Packaging & Build:** 🟢 `VERIFIED` (`python -m build --wheel` builds clean wheel and smoke tests pass).
- **Static Resource Contract Tests:** 🟢 `VERIFIED` (104/104 unit & integration tests pass across Modules 1–6).
- **GitHub Actions CI Workflow:** 🟢 `IMPLEMENTED / LOCAL TESTED` (Runs clean locally; remote CI execution pending repository push).
- **Authenticated Databricks Bundle Validation:** ⏳ `PENDING` (Requires live Databricks CLI authentication and workspace connection).
- **Databricks Cloud Deployment:** ⏳ `PENDING`
- **Serverless SQL Warehouse Cloud Creation:** ⏳ `PENDING`
- **Serving Views Cloud Creation:** ⏳ `PENDING`
- **Learning Status:** ⏳ `NOT STUDIED / PENDING`
