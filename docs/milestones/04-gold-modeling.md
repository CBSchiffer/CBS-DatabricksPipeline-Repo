# M4 — Gold: dimensional modeling and analytics queries

**Target:** November 28, 2026  
**Depends on:** M3  
**Outcome:** A small but coherent star schema and regional/monthly sales reporting derived solely from validated Silver data.

## Why this matters

Konica Minolta's project is not just about moving data between systems; the implementation must make data **useful** for consumers. A deliberate Gold model is a concrete, interview-friendly explanation of data modeling, not just Spark transformations.

## Scope and modeling decisions

Use only **sales** for the initial PoC. A valid sales record is an order line and its `sales_amount_usd` is already calculated in Silver.

**Grain:** one row in `gold.fact_sales` = **one latest valid order line** from an ERP system. Keep the composite source key to prevent collisions across regions.

### Output tables

| Table | Example columns | Notes |
|---|---|---|
| `gold.dim_product` | `product_id`, `product_name`, `product_category` | From governed product reference data |
| `gold.dim_region` | `region`, `region_name`, `reporting_currency` | Three rows: JP, US, EU |
| `gold.fact_sales` | `source_system`, `order_id`, `order_line_id`, `order_date`, `product_id`, `region`, `quantity`, `sales_amount_usd` | Fact at order-line grain |
| `gold.v_monthly_regional_sales` | `sales_month`, `region`, `revenue_usd`, `units_sold`, `order_line_count` | Monthly reporting view |

The project does **not** need integer surrogate keys. For a small demo, stable product and region natural keys are simpler and sufficient. Call out that production models may use surrogate dimensions and slowly changing dimensions (SCDs).

## Implementation checklist

### A. Dimensions

- [ ] Build `dim_product` from `reference.products` with unique non-null `product_id`.
- [ ] Build `dim_region` from a fixed mapping (`JP→Japan`, `US→United States`, `EU→Europe`).
- [ ] Ensure both dimensions are small and easily understandable.
- [ ] Add table comments describing field definitions if time allows.

### B. Fact table

- [ ] Select only current valid rows from `silver.global_sales_unified`.
- [ ] Retain the full business key and transaction date.
- [ ] Include numeric quantity and `DECIMAL(20,2)` USD revenue.
- [ ] Validate each fact's `product_id` and `region` matches a dimension row.
- [ ] Rebuild Gold deterministically from Silver for the MVP (`CREATE OR REPLACE TABLE AS SELECT` or controlled overwrite). For this small dataset, rebuilding is simpler than introducing a second merge pipeline.

### C. Monthly summary

- [ ] Create `gold.v_monthly_regional_sales` grouping by `date_trunc('month', order_date)` and `region`.
- [ ] Aggregate `SUM(sales_amount_usd)`, `SUM(quantity)`, `COUNT(*)`.
- [ ] Prefer a view on `fact_sales` so that a corrected source record is reflected after the Gold refresh.
- [ ] Run a query sorted by month and region and prepare to show results.

Illustrative SQL:

```sql
CREATE OR REPLACE VIEW global_erp_dev.gold.v_monthly_regional_sales AS
SELECT
  CAST(date_trunc('MONTH', order_date) AS DATE) AS sales_month,
  region,
  CAST(SUM(sales_amount_usd) AS DECIMAL(22,2)) AS revenue_usd,
  SUM(quantity) AS units_sold,
  COUNT(*) AS order_line_count
FROM global_erp_dev.gold.fact_sales
GROUP BY CAST(date_trunc('MONTH', order_date) AS DATE), region;
```

### D. Quality and reconciliation

- [ ] Verify no null keys in Gold facts.
- [ ] Verify duplicate fact keys count is zero.
- [ ] Verify every product and region key maps to a dimension.
- [ ] Verify `SUM(fact_sales.sales_amount_usd) = SUM(silver.global_sales_unified.sales_amount_usd)`.
- [ ] Verify aggregation matches fact totals per region and month.
- [ ] After processing correction Batch B, refresh Gold and verify revenue changes by the **expected delta**, not the entire old record being added again.

Example orphan check:

```sql
SELECT COUNT(*) AS orphan_products
FROM global_erp_dev.gold.fact_sales f
LEFT ANTI JOIN global_erp_dev.gold.dim_product p
ON f.product_id = p.product_id;
-- Expected: 0
```

## What to discuss in the interview

- **Grain:** We chose an order-line fact because regional ERPs can reuse order IDs and one order can contain multiple products.
- **Separation:** Silver is source-independent and validated; Gold is optimized for reporting consumers.
- **Consistency:** Rebuilding Gold from current Silver avoids double-counting after a corrected order line.
- **Constraints:** The PoC simplifies taxes, refunds, cancellation states, FX close rates, dimension history, and source master-data governance.
- **Extensibility:** Actual Power BI or Fabric integration would consume these Gold tables through an explicitly designed supported connector/sharing architecture; **Direct Lake is not presumed to read arbitrary Databricks Delta tables without additional configuration**.

## Deliverables

- Gold fact, two dimension tables, and a monthly aggregate view.
- SQL scripts/notebooks for repeatable creation and validation.
- Screenshot of a regional revenue query result.
- Test results showing Gold/Silver reconciliation and corrected revenue.

## Acceptance criteria

- [ ] Gold fact is one row per latest valid order line.
- [ ] All fact foreign keys resolve to dimensions.
- [ ] Regional/monthly query produces expected values.
- [ ] Re-running Gold does not change counts or totals without source changes.
- [ ] Processing a corrected Silver row updates Gold totals by the expected difference.

## Stretch goals (only if spare time)

A simple Databricks SQL visualization, an additional top-products view, or a BI screenshot. Avoid building inventory turnover: that would need inventory levels, timestamps, and a separate design.
