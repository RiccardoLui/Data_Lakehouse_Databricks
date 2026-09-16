# Security, Governance and CI/CD extension

This folder is an additive extension for the existing `Data_Lakehouse_Databricks` repository.

## What is added

- `Governance/governance_setup.py`
  - creates a governance schema
  - documents Bronze/Silver/Gold
  - creates table and column inventory views
  - tags the customer dimension and PII columns
  - creates `gold.gold_dim_customers_secure`, which masks names and generalizes date of birth for non-admin users
- `Governance/rbac_enterprise_template.sql`
  - least-privilege role/group template for a full Databricks account
- `Governance/governance_checks.sql`
  - verification queries for metadata and permissions
- `databricks.yml` + `resources/medallion_jobs.yml`
  - Declarative Automation Bundle definitions for the batch and Auto Loader workflows
  - both workflows finish with a governance refresh
- `.github/workflows/ci.yml`
  - validates Python, notebooks, YAML, bundle paths, and obvious leaked credentials
  - runs `databricks bundle validate` when workspace credentials are configured
- `.github/workflows/deploy.yml`
  - deploys the bundle after a push to `main` or a manual dispatch
- `ci/templates/deploy_oidc_enterprise.yml`
  - ready-to-adapt secretless GitHub OIDC deployment for a full Databricks account
- `SECURITY.md`
  - repository security rules

## Expected repository layout

Copy these files into the root of your existing repository, preserving paths:

```text
Data_Lakehouse_Databricks/
├── Bronze/
├── Silver/
├── Gold/
├── Setup/
├── Governance/                 # new
├── resources/                  # new
├── ci/                         # new
├── .github/workflows/          # new
├── databricks.yml              # new
├── requirements-dev.txt        # new
└── SECURITY.md                 # new
```

## Pipeline behavior

### Batch job

```text
Bronze/Bronze.ipynb
    -> Silver/modular/notebooks/Silver_orchestration.py
    -> Gold/Gold_orchestration.ipynb
    -> Governance/governance_setup.py
```

### Streaming simulation job

```text
Setup/Data_ingestion.ipynb
    -> Setup/Auto_loader.ipynb
    -> Silver/modular/notebooks/Silver_orchestration.py
    -> Gold/Gold_orchestration.ipynb
    -> Governance/governance_setup.py
```



## Local / Databricks bundle usage

From a terminal with the current Databricks CLI:

```bash
databricks auth login --host https://<your-workspace-host>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run medallion_batch_pipeline -t dev
databricks bundle run -t dev --params batch_id=1 medallion_streaming_pipeline
```

On Databricks Free Edition you can also clone the GitHub repository into a Databricks Git Folder and deploy the bundle from the workspace UI.

## Existing Databricks jobs

The bundle definitions create bundle-managed jobs by default. Because this project already has manually created batch and streaming jobs, validate the new definitions before deciding which set to keep. To adopt an existing job instead of creating a duplicate, Databricks supports `databricks bundle generate job --existing-job-id <JOB_ID> --bind` or `databricks bundle deployment bind <resource_key> <JOB_ID>`. Review the generated/diffed configuration before the next deploy because the bundle becomes the source of truth for the bound job.

Do not bind both the `dev` and `prod` targets to the same remote job in the single Free Edition workspace. Pick the target that you want to own that job.
