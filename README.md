# Databricks Medallion Pipeline — Complete Reference

Production-grade data pipeline on Databricks using the **Medallion (Bronze/Silver/Gold) architecture**, deployed with **Databricks Asset Bundles (DAB)** across four environments.

---

## 📐 Architecture Overview

```
Landing Volume (Unity Catalog)
        │
        ▼  [Auto Loader / CloudFiles]
  ┌─────────────┐
  │   BRONZE    │  Raw ingestion + metadata + schema validation
  │  (DLT)      │  Tables: bronze_orders_raw, bronze_customers_raw,
  └──────┬──────┘         bronze_products_raw, *_quarantine
         │
         ▼  [Type casting, PII masking, dedup, SCD-1]
  ┌─────────────┐
  │   SILVER    │  Cleansed & conformed
  │  (DLT)      │  Tables: silver_orders, silver_customers,
  └──────┬──────┘         silver_products, silver_customers_scd1
         │
         ▼  [Business aggregations, metrics, ranking]
  ┌─────────────┐
  │    GOLD     │  Business-ready
  │  (DLT)      │  Tables: gold_daily_sales,
  └─────────────┘         gold_customer_lifetime_value,
                          gold_product_performance
```

---

## 🗂️ Repository Structure

```
databricks-medallion-pipeline/
├── databricks.yml                  ← DAB root config (all environments declared here)
├── setup.py                        ← Python wheel packaging
├── pyproject.toml                  ← Tool configuration (ruff, black, pytest)
├── requirements-dev.txt            ← Dev/test dependencies
│
├── src/
│   ├── dlt/
│   │   ├── bronze_layer.py         ← DLT: Auto Loader ingestion + quarantine tables
│   │   ├── silver_layer.py         ← DLT: Cleansing, PII masking, SCD-1 upserts
│   │   └── gold_layer.py           ← DLT: Business aggregations (daily sales, CLV, products)
│   ├── jobs/
│   │   ├── setup_catalog.py        ← Job: Create catalog/schemas/volumes/grants
│   │   ├── validate_source.py      ← Job: Pre-flight source data validation
│   │   ├── run_quality_checks.py   ← Job: Post-DLT gold layer quality gates
│   │   └── notify_pipeline_status.py
│   └── utils/
│       ├── config.py               ← PipelineConfig dataclass (env-aware)
│       ├── logger.py               ← Structured logging
│       └── helpers.py              ← Shared transforms, write helpers, DQ utilities
│
├── resources/
│   ├── pipelines/
│   │   └── medallion_dlt.yml       ← DLT pipeline definition (parameterized)
│   ├── jobs/
│   │   ├── setup_job.yml           ← Catalog setup job
│   │   └── orchestration_job.yml   ← Full pipeline orchestration job (scheduled)
│   └── schemas/
│       └── unity_catalog.yml       ← Unity Catalog schema resources (DAB managed)
│
├── tests/
│   ├── conftest.py                 ← Shared fixtures (local SparkSession, sample data)
│   └── unit/
│       ├── test_config.py          ← PipelineConfig unit tests
│       ├── test_helpers.py         ← Helper function unit tests
│       ├── test_silver_transforms.py
│       ├── test_gold_aggregations.py
│       └── test_quality_checks.py
│
└── .github/workflows/
    ├── ci.yml                      ← PR: lint + unit tests + bundle validate
    ├── deploy-dev.yml              ← push to develop → auto-deploy to DEV
    ├── deploy-test.yml             ← push to main → auto-deploy to TEST
    ├── deploy-int.yml              ← manual dispatch → INT (with approval gate)
    └── deploy-prod.yml             ← GitHub Release → PROD (with approval gate)
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Java 11 (required by local PySpark)
- Databricks CLI v0.220+

### 1. Clone & Install

```bash
git clone https://github.com/your-org/databricks-medallion-pipeline.git
cd databricks-medallion-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pip install -e .
```

### 2. Configure Local Environment

```bash
cp .env.example .env
# Edit .env with your workspace hosts and tokens
```

### 3. Run Unit Tests

```bash
pytest tests/unit/ -v --cov=src --cov-report=html
open htmlcov/index.html
```

### 4. Authenticate with Databricks CLI

```bash
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...

# Or configure per environment:
databricks configure --profile dev
databricks configure --profile prod
```

### 5. Validate the Bundle

```bash
databricks bundle validate --target dev
```

### 6. Deploy to Dev

```bash
# Deploy all resources
databricks bundle deploy --target dev

# Run catalog setup
databricks bundle run catalog_setup_job --target dev

# Trigger the pipeline manually
databricks bundle run medallion_orchestration_job --target dev \
  --python-named-params "run_date=2024-01-15"
```

---

## 🌍 Environment Strategy

| Target | Trigger | Approval | DLT Channel | Schedule |
|--------|---------|----------|-------------|----------|
| **dev** | Push to `develop` | Auto | PREVIEW | Paused |
| **test** | Push to `main` | Auto | CURRENT | Paused |
| **int** | Manual dispatch | Manual | CURRENT | Paused |
| **prod** | GitHub Release | Manual + Protected | CURRENT | **Active** 06:00 UTC |

---

## 🔑 GitHub Secrets Required

Configure these in **Settings → Secrets and variables → Actions**:

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST_DEV` | DEV workspace URL |
| `DATABRICKS_HOST_TEST` | TEST workspace URL |
| `DATABRICKS_HOST_INT` | INT workspace URL |
| `DATABRICKS_HOST_PROD` | PROD workspace URL |
| `DATABRICKS_TOKEN_DEV` | PAT for DEV |
| `DATABRICKS_TOKEN_TEST` | PAT for TEST |
| `DATABRICKS_TOKEN_INT` | PAT for INT |
| `DATABRICKS_TOKEN_PROD` | PAT for PROD |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook |

---

## 🧱 Unity Catalog Structure

```
{env}_catalog/
├── medallion_bronze/
│   ├── bronze_orders_raw
│   ├── bronze_orders_quarantine
│   ├── bronze_customers_raw
│   ├── bronze_products_raw
│   └── [Volume] raw_landing/
│       ├── orders/          ← JSON files drop here
│       ├── customers/
│       ├── products/
│       └── _checkpoints/
├── medallion_silver/
│   ├── silver_orders
│   ├── silver_customers
│   ├── silver_customers_scd1
│   └── silver_products
└── medallion_gold/
    ├── gold_daily_sales          (partitioned by order_date)
    ├── gold_customer_lifetime_value
    └── gold_product_performance
```

---

## 🏷️ DLT Data Quality Expectations

| Layer | Table | Expectation | Action |
|-------|-------|-------------|--------|
| Bronze | orders_raw | order_id NOT NULL | Drop → quarantine |
| Bronze | orders_raw | quantity > 0 | Drop → quarantine |
| Silver | orders | status IN (...) | Drop |
| Silver | orders | total_amount >= 0 | Drop |
| Gold | daily_sales | order_count > 0 | Drop |
| Gold | clv | lifetime_value >= 0 | Drop |

---

## 🧪 Testing

```bash
# All unit tests
pytest tests/unit/ -v

# With coverage (must stay ≥ 80%)
pytest tests/unit/ --cov=src --cov-fail-under=80

# Specific module
pytest tests/unit/test_gold_aggregations.py -v
```

---

## 📦 Deploying a New Release to PROD

1. Merge feature branch → `develop` (auto-deploys to DEV)
2. Merge `develop` → `main` (auto-deploys to TEST)
3. Manually trigger INT deploy via GitHub Actions → get approval
4. Create a GitHub Release with a semver tag (e.g. `v1.2.0`) → triggers PROD deploy (requires approval)

---

## ⚙️ Parameterization Reference

All pipeline behaviour is parameterized via DAB variables (overridable per target):

| Variable | Default | Description |
|----------|---------|-------------|
| `env` | dev | Environment name |
| `catalog_name` | dev_catalog | Unity Catalog name |
| `schema_prefix` | medallion | Schema name prefix |
| `source_system` | ecommerce | Source system tag |
| `pipeline_channel` | CURRENT | DLT runtime channel |
| `min_workers` / `max_workers` | 1 / 4 | Autoscale bounds |
| `notification_email` | — | Alert recipient |
| `volume_name` | raw_landing | Landing volume name |
| `log_level` | INFO | Python log level |
