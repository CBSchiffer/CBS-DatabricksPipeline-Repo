# M1 — Azure Databricks setup and synthetic ERP data

**Target:** November 21–22, 2026  
**Dependency:** Azure pay-as-you-go subscription  
**Outcome:** A functioning Azure Databricks workspace, consistent Unity Catalog paths, and deterministic multi-region test files.  
**Time priority:** Configuration and a one-file smoke test matter more than polished generators.

## Why this milestone exists

The objective is to become comfortable with the Azure → Databricks → Unity Catalog workflow *before* tackling Spark transformations. Confirm access rights, storage conventions, and compute behavior; avoid losing M2 to subscription or permissions issues.

## Decisions to lock in

- **Region:** Choose an Azure region where Databricks and your selected compute are available; keep services together when possible.
- **Resource group:** `rg-global-erp-dbx-poc` (example; use any consistent name).
- **Workspace:** `dbw-global-erp-poc` (example; workspace names must be unique as Azure requires).
- **Catalog:** `global_erp_dev`, falling back to a writable workspace/default catalog if needed.
- **Schemas:** `bronze`, `silver`, `gold`, `reference`, `ops`.
- **Volumes:** `ops.landing` (source files), `ops.pipeline_state` (Auto Loader schema/checkpoints).
- **GitHub:** `global-erp-databricks`; main branch for stable progress.

Store the chosen values in a non-secret configuration file or notebook parameters; do not hard-code subscription IDs or credentials into Git.

## Task checklist

### A. Azure setup and spending controls

- [ ] Create the dedicated resource group.
- [ ] In Azure Cost Management, create a budget at a **personally agreed spending ceiling** and set warnings (e.g. 50%, 80%, 100%). A budget alerts; it does not stop charges.
- [ ] Check eligibility for the Databricks **Trial (Premium – 14-day DBUs)** tier. Trial DBUs do not necessarily cover VM/storage/network charges; verify pricing in the portal.
- [ ] Create a Databricks workspace and open it successfully.
- [ ] Prefer serverless compute if available and suitable. Otherwise use the smallest supported classic compute, with a short inactivity auto-termination (e.g. 10–15 minutes).
- [ ] Stop unused clusters/warehouses after each session; verify resource costs periodically.
- [ ] If you cannot access an Azure feature without extra infrastructure, prefer the **managed-volume version** rather than building networking complexity.

### B. Unity Catalog and storage

- [ ] Verify Unity Catalog is enabled and a writable catalog exists.
- [ ] Create (or use a permitted existing) catalog and the five schemas.
- [ ] Create the managed volumes `ops.landing` and `ops.pipeline_state`.
- [ ] Under the landing volume create directories `jp/`, `us/`, `eu/`.
- [ ] Under the state volume plan independent `schemas/jp`, `schemas/us`, `schemas/eu`, and `checkpoints/jp`, `checkpoints/us`, `checkpoints/eu`.
- [ ] Verify a notebook can list the landing paths and read an uploaded file.

Conceptual SQL setup (adapt permissions and catalog naming):

```sql
CREATE CATALOG IF NOT EXISTS global_erp_dev;
CREATE SCHEMA IF NOT EXISTS global_erp_dev.bronze;
CREATE SCHEMA IF NOT EXISTS global_erp_dev.silver;
CREATE SCHEMA IF NOT EXISTS global_erp_dev.gold;
CREATE SCHEMA IF NOT EXISTS global_erp_dev.reference;
CREATE SCHEMA IF NOT EXISTS global_erp_dev.ops;
CREATE VOLUME IF NOT EXISTS global_erp_dev.ops.landing;
CREATE VOLUME IF NOT EXISTS global_erp_dev.ops.pipeline_state;
```

If `CREATE CATALOG` is forbidden, use an accessible catalog and substitute its name throughout subsequent SQL, paths, and docs. Avoid wasting a day requesting unnecessary permissions in a personal PoC.

### C. Repository and development workflow

- [ ] Create GitHub repository with this README and milestone docs.
- [ ] Clone through Databricks **Git folders** or edit locally and push through your normal Git workflow.
- [ ] Choose code organization: `src/generate_erp_data.py`, `notebooks/01_bronze.py`, `02_silver.py`, `03_gold.sql` or `.py`, `04_validate.sql` or `.py`.
- [ ] Record the actual catalog, runtime/compute mode, and paths in a setup note (without secret values).
- [ ] Test that a trivial PySpark DataFrame can be created and displayed using your selected compute.

### D. Generate source data

Consult [DATA_CONTRACT.md](../DATA_CONTRACT.md) before writing the generator.

- [ ] Implement a seeded Python generator producing **one file per region per batch**.
- [ ] Japan: UTF-8 CSV; Japanese headers; `yyyy/MM/dd`; JPY.
- [ ] US: JSON Lines with `product` nested object; `MM-dd-yyyy`; USD.
- [ ] EU: pipe-delimited text/CSV; `dd.MM.yyyy`; EUR.
- [ ] Use the same canonical product catalog (`P-001`, `P-002`, etc.) across regions.
- [ ] Make date coverage deterministic, e.g. November 1–2, 2026, to simplify historical FX joins.
- [ ] Include deliberately invalid entries (missing ID, negative quantity, unknown SKU) in the generator, with a manifest of expected reject counts.
- [ ] Keep Batch B **separate**: a future correction with the same key but later `source_updated_at`; do not upload it until M3 tests.
- [ ] Upload Batch A files to each regional landing directory.

**Generator reproducibility:** fixed seed, stable files, no call to live FX APIs. Start with ~20 valid lines per region plus a few rejects, not millions of rows. Avoid external package dependencies where the Python standard library is enough.

## Smoke test

From Databricks:

```python
print(dbutils.fs.ls('/Volumes/global_erp_dev/ops/landing/jp/'))
sample = spark.read.option('header', True).csv(
    '/Volumes/global_erp_dev/ops/landing/jp/'
)
display(sample.limit(5))
```

Expected: files are listed; headers/characters appear correctly; Spark can read a handful of rows. A standard `spark.read` smoke test is intentional here; Auto Loader comes in M2.

## Deliverables

- Usable Azure Databricks workspace and selected compute.
- Unity Catalog catalog/schemas/volumes, with known writable paths.
- GitHub repository and initial notebook.
- Three region-specific Batch A source files, plus a local data generator.
- A short documented note on cost settings and any setup limitations.

## Acceptance criteria (exit gate)

- [ ] `SELECT current_catalog()` succeeds and shows expected context, or the chosen catalog is explicitly qualified.
- [ ] All source paths are visible through a notebook.
- [ ] At least one JP row has been read successfully with Spark.
- [ ] Synthetic files are deterministic and contain **no real enterprise/customer data**.
- [ ] Compute has been terminated or is verified to have no active chargeable session after work.

## Troubleshooting / fallbacks

| Issue | Action |
|---|---|
| `CREATE CATALOG` denied | Reuse a writable catalog, create schemas there |
| Insufficient VM quota | Try supported serverless option or another permitted smaller compute/region |
| Unexpected spend | Stop compute, check Azure Cost Analysis and Databricks usage, reduce configuration |
| JP characters are corrupted | Verify file is UTF-8; postpone Shift-JIS variant |
| Git folder auth fails | Push via local Git; notebook source-control integration is not an MVP blocker |

## Interview evidence

One screenshot of workspace Catalog Explorer with schemas/volumes, and one notebook showing JP data readable in Spark. Spend no more than a few minutes on screenshots at this stage.

## Official references

- https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial
- https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-volumes
- https://learn.microsoft.com/en-us/azure/databricks/repos/git-operations-with-repos
- https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-acm-create-budgets
