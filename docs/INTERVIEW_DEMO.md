# Interview demonstration runbook — December 1, 2026

**Goal:** Show implementation competence with Databricks, not claim experience with a production deployment of this stack.

## Recommended order (3–5 minutes)

| Time | Show | Message |
|---|---|---|
| 0:00–0:30 | README problem statement | Three regional ERPs have inconsistent source files |
| 0:30–1:15 | Architecture Mermaid diagram | Auto Loader → Bronze → Silver → Gold; Unity Catalog and Jobs |
| 1:15–2:45 | Silver PySpark + Delta MERGE | Explicit mapping, quality checks, dated FX joins, latest-version logic |
| 2:45–3:30 | Job DAG screenshot | One orchestrated, validated execution |
| 3:30–4:15 | Gold query / rerun proof | Regional monthly revenue; no duplicates; corrected-order delta |
| 4:15–5:00 | Limitations and next steps | Synthetic data; real ERP connectors and Fabric/BI are extensions |

## Evidence to collect as you build

- [ ] Screenshot: populated Bronze tables with metadata fields.
- [ ] Screenshot: short PySpark code section standardizing data from three formats.
- [ ] Screenshot: latest-order-line record updated after Batch B.
- [ ] Screenshot: SQL check returning zero duplicate business keys.
- [ ] Screenshot: Gold monthly revenue by region.
- [ ] Screenshot: successful job DAG (with date/time and tasks visible if possible).
- [ ] Optional screenshot: Unity Catalog table lineage actually observed.

## Honest claims

**Can say (once proven):** "I implemented and tested Auto Loader, PySpark standardization, Delta Lake MERGE, and a multi-task Job for synthetic ERP data in Azure Databricks."

**Should not say:** "I have shipped Databricks to production," "we integrated with real regional ERPs," "Fabric Direct Lake works with this setup," or "column-level lineage and masking were fully implemented" unless those specific claims become true.

## One-sentence project pitch

> A compact Azure Databricks lakehouse that ingests synthetic ERP sales files from Japan, the US, and Europe, standardizes their formats and currencies, updates corrected transactions without duplicates, and publishes governed analytics tables through an orchestrated pipeline.

## Questions and tradeoffs

| Likely question | Answer direction |
|---|---|
| Why medallion architecture? | Raw auditability vs trusted transforms vs consumption-friendly modeling |
| Why Auto Loader? | Stateful incremental discovery, compatible with batch-like jobs using `availableNow` |
| How handle duplicates? | Business-key dedup + timestamp-ranked source + conditional Delta MERGE |
| How avoid bad FX math? | Dated lookup, DECIMAL, synthetic deterministic rates, validation |
| What changes for production? | Real connectors/CDC, credential isolation, security, monitoring, scaling, recovery, CI/CD |

## Before the interview

- Verify links and screenshots open without access to your personal Azure environment.
- Keep a 2-minute fallback: architecture + one screenshot of a successful run + a single code excerpt.
- Close any actively charging compute resources after you capture the evidence.
