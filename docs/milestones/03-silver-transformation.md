# M3 — Silver: normalization, quality validation, and idempotent MERGE

**Target:** November 25–27, 2026  
**Depends on:** M2  
**Outcome:** A unified, validated sales table, quarantine records, and an explicit proof that corrected source records update cleanly without duplicates.

## Priority

**This is the most technically important milestone for the interview.** Prior professional NiFi/Kafka/Flink experience explains why the project exists; the actual *Databricks proof* is the PySpark/Delta implementation here. Protect time for testing.

## Inputs and outputs

Read all three source-shaped Bronze tables and reference data. Write:

1. `global_erp_dev.silver.global_sales_unified` — exactly one **latest valid record** per composite business key.
2. `global_erp_dev.silver.sales_quarantine` — invalid records with reason and provenance.
3. `global_erp_dev.reference.fx_rates` — historical fixture rates used in conversion (created once and joined during transform).

The canonical target types and the source mappings are defined in [DATA_CONTRACT.md](../DATA_CONTRACT.md). Do not invent inconsistent field names per notebook.

## Step-by-step plan

### A. Create the reference datasets

- [ ] Populate `reference.products` with a handful of canonical product IDs.
- [ ] Populate `reference.fx_rates` with one rate for each `(order_date, currency_code)` used by the fixtures, including USD=1.
- [ ] Use the convention `usd_per_unit`: 1 JPY → X USD, 1 EUR → Y USD.
- [ ] Store rates as `DECIMAL(18,8)`, not binary floats; declare whether the quoted conversion is a synthetic test fixture.
- [ ] Validate uniqueness of each FX rate key, positivity, and full date coverage.

Do not fetch live exchange rates: that makes reproducibility and expected totals harder without improving the Databricks interview signal.

### B. Map each source to the canonical field names

- [ ] JP: explicitly select Japanese header names and alias to English column names.
- [ ] US: flatten `product.sku` and `product.unitPrice` from the JSON struct.
- [ ] EU: trim strings and turn sentinel nulls (`NULL`, `N/A`, whitespace) into null.
- [ ] Add `source_system`, `region`, and `currency_code` from controlled source mappings.
- [ ] Parse dates **with explicit source-specific formats**. For invalid dates, prefer tolerant parse logic / validation rather than aborting the entire transform.
- [ ] Parse numeric fields to integer/decimal; use explicit casts and validate resulting nulls.
- [ ] Convert timestamps to a consistent UTC representation.
- [ ] Union the three normalized DataFrames with `unionByName`, selecting a consistent set of target columns.

An illustrative mapping table:

| Canonical field | JP | US | EU |
|---|---|---|---|
| `order_id` | `受注番号` | `orderId` | `ORDER_NO` |
| `order_line_id` | `明細番号` | `lineId` | `LINE_NO` |
| `product_id` | `商品コード` | `product.sku` | `SKU` |
| `order_date` | `受注日` | `orderDate` | `ORDER_DT` |
| `quantity` | `数量` | `quantity` | `QTY` |
| `unit_price_local` | `単価` | `product.unitPrice` | `UNIT_PRICE` |
| `source_updated_at` | `更新日時` | `modifiedAt` | `UPDATED_AT` |

### C. Validate records and retain failure reasons

- [ ] Reject missing `source_system`, `order_id`, `order_line_id`.
- [ ] Reject missing/unknown `product_id`, unparseable `order_date`, quantity <= 0, price < 0, or invalid update timestamp.
- [ ] Join FX rates on `currency_code` **and** `order_date`; reject missing rates.
- [ ] Compute the USD amount: `ROUND(quantity * unit_price_local * fx_rate_to_usd, 2)`; cast to `DECIMAL(20,2)`.
- [ ] Write invalid rows to `silver.sales_quarantine` with `reject_reason`, original source representation (or unmodified source fields), `source_file` and ingestion metadata.
- [ ] Track **distinct rejected events**, not every time you happen to rescan the same Bronze row.
- [ ] Keep clean records separate for merging.

**Recommended simplification for this small PoC:** on each execution, read *all* Bronze rows and derive the **latest valid record per business key** with a deterministic `row_number()` window. Then `MERGE` into Silver. Also overwrite/rebuild the derived quarantine **snapshot** from the full Bronze event history, keyed/deduplicated by source event identity. This is easy to reason about and makes reruns deterministic. Document that it is **not a scalable incremental Silver strategy**; later improvements could use Bronze change data feed or watermarking.

### D. Implement version-aware MERGE

- [ ] Within the validated DataFrame, partition by `source_system`, `order_id`, `order_line_id`.
- [ ] Order by `source_updated_at DESC`, then a deterministic tie-break (e.g. `source_file DESC`).
- [ ] Select only the first row per key for the merge source.
- [ ] Use Delta Lake `MERGE INTO` on the **three-part key**.
- [ ] For a matched key, update the target **only when the incoming source version is newer** (or is deterministically preferred on ties); do not replace newer data with old source versions.
- [ ] For an unmatched key, insert.

Example logic shape (table/dataframe aliases and full conditions to be implemented in code):

```sql
MERGE INTO global_erp_dev.silver.global_sales_unified AS target
USING latest_valid_source AS src
ON target.source_system = src.source_system
 AND target.order_id = src.order_id
 AND target.order_line_id = src.order_line_id
WHEN MATCHED AND src.source_updated_at > target.source_updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**Important:** `UPDATE SET *` assumes the source and target columns are aligned. In the actual implementation, explicit mappings are clearer and safer. Ensure exactly **one source row per target key** before `MERGE`: Databricks warns that multiple source matches can cause ambiguous or failed merges. When update timestamps tie, specify a deterministic policy (e.g. do not update; or persist and compare an additional ordered source-event token).

### E. Prove idempotency and change handling

Conduct tests in this order, with counts and aggregate totals recorded:

1. **Initial Batch A:** transform after Bronze has been loaded. Count unique keys and quarantine reasons.
2. **Same-input rerun:** run Silver twice with no new files. Verify identical key counts, key values, and total USD revenue.
3. **Correction Batch B:** upload a new file with the same `(ERP_US, US-2001, 1)` but a *later* `source_updated_at` and a revised `quantity` or price. Run Bronze then Silver.
4. Verify that the affected Silver row has **new values**, but the key count remains unchanged for that correction.
5. Check that Gold can be rebuilt without double counting (M4).
6. **Quality test:** upload or include invalid rows; confirm correct reasons and that they never contaminate the Silver table.

Do not claim true end-to-end exactly-once delivery; the PoC demonstrates **incremental file ingestion and idempotent business-level target state** under stated assumptions.

## Verification SQL

```sql
-- Duplicate business keys: must return zero rows.
SELECT source_system, order_id, order_line_id, COUNT(*) AS n
FROM global_erp_dev.silver.global_sales_unified
GROUP BY source_system, order_id, order_line_id
HAVING COUNT(*) > 1;

-- Check the deliberately revised source record.
SELECT source_system, order_id, order_line_id,
       quantity, unit_price_local, sales_amount_usd, source_updated_at
FROM global_erp_dev.silver.global_sales_unified
WHERE source_system = 'ERP_US'
  AND order_id = 'US-2001' AND order_line_id = '1';

-- Inspect reasons and provenance for rejected events.
SELECT reject_reason, COUNT(*) AS rejected
FROM global_erp_dev.silver.sales_quarantine
GROUP BY reject_reason ORDER BY rejected DESC;
```

## Common pitfalls

| Pitfall | Mitigation |
|---|---|
| Duplicate keys inside merge source | Window + deterministic ranking **before** MERGE |
| FX lookup joined only on currency | Join on `rate_date` and `currency_code` |
| Currency using floats | Use decimals and an explicit rounding rule |
| Null parsing raises exception | Use safe parsing and quarantine invalid rows |
| Re-running appends to quarantine | Derive a distinct event snapshot or MERGE by event identity |
| Stale correction wins | Compare source update timestamps before update |
| Raw data lost while cleaning | Preserve source provenance and original record in Bronze/quarantine |

## Deliverables and exit gate

- [ ] One unified Silver Delta table with typed, canonical fields.
- [ ] Deterministic FX reference data and successful FX join.
- [ ] Quarantine table populated with traceable errors.
- [ ] A demonstrated merge of a later correction.
- [ ] SQL or notebook assertions proving no duplicate keys and unchanged totals after reruns.
- [ ] Screenshots: transformation code, a corrected record before/after, quarantine reasons, duplicate-key assertion.

## Stretch only if ahead of schedule

Delta Change Data Feed, incremental Silver processing from changes, structured quality metrics table, and more sophisticated schema evolution. None should delay the basic merge-and-test demonstration.

## Official reference

- https://learn.microsoft.com/en-us/azure/databricks/delta/merge
