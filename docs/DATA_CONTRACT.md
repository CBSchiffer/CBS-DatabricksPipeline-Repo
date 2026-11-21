# Data contracts and deterministic test fixtures

This document is the **single source of truth** for simulated ERP fields, canonical fields, keys, quality checks, and expected test behavior. Keep source mappings here instead of allowing notebooks to diverge.

## Regional input files

All files must be **immutable once landed**. A correction is a **new file** with the same business key and a later `source_updated_at`; do not overwrite an existing file in place. Prefer UTF-8 in the MVP (Shift-JIS JP exports are a later extension). For JSON, use **one JSON object per line**.

### ERP_JP — CSV, header=true, UTF-8

| Field | Example | Standard field |
|---|---|---|
| `受注番号` | `JP-1001` | `order_id` |
| `明細番号` | `1` | `order_line_id` |
| `商品コード` | `P-001` | `product_id` |
| `受注日` | `2026/11/01` | `order_date` |
| `数量` | `3` | `quantity` |
| `単価` | `12500.00` | `unit_price_local` |
| `更新日時` | `2026-11-02T01:15:00Z` | `source_updated_at` |

Currency is **JPY**, region is **JP**, and source is **ERP_JP**. Quoted field names containing Japanese characters must be handled explicitly in Spark.

### ERP_US — newline-delimited nested JSON

```json
{"orderId":"US-2001","lineId":"1","orderDate":"11-01-2026","quantity":"2","product":{"sku":"P-001","unitPrice":"75.00"},"modifiedAt":"2026-11-02T01:15:00Z"}
```

Map `product.sku` to `product_id`, `product.unitPrice` to `unit_price_local`, and `modifiedAt` to `source_updated_at`. Currency = **USD**; region = **US**; source = **ERP_US**.

### ERP_EU — pipe-separated CSV, UTF-8

```text
ORDER_NO|LINE_NO|SKU|ORDER_DT|QTY|UNIT_PRICE|UPDATED_AT
EU-3001|1|P-002|01.11.2026|4|49.90|2026-11-02T01:15:00Z
```

Map corresponding columns to canonical fields. Currency = **EUR**; region = **EU**; source = **ERP_EU**. Convert `""`, whitespace-only values, `NULL`, and `N/A` to SQL null **before validation**.

## Canonical Silver contract

| Column | Spark/SQL type | Required | Definition |
|---|---|---|---|
| `source_system` | STRING | Yes | `ERP_JP`, `ERP_US`, `ERP_EU` |
| `region` | STRING | Yes | `JP`, `US`, `EU` |
| `order_id` | STRING | Yes | Source order ID, trimmed |
| `order_line_id` | STRING | Yes | Source line ID, trimmed |
| `product_id` | STRING | Yes | Canonical SKU |
| `order_date` | DATE | Yes | Calendar date of sale |
| `quantity` | INT | Yes | Strictly greater than 0 |
| `unit_price_local` | DECIMAL(18,2) | Yes | Local unit price, nonnegative |
| `currency_code` | STRING | Yes | `JPY`, `USD`, `EUR` |
| `fx_rate_to_usd` | DECIMAL(18,8) | Yes | USD received per **1 local currency unit** |
| `sales_amount_usd` | DECIMAL(20,2) | Yes | `ROUND(quantity × unit_price_local × fx_rate_to_usd, 2)` |
| `source_updated_at` | TIMESTAMP | Yes | Source modification time, normalized to UTC |
| `ingestion_timestamp` | TIMESTAMP | Yes | Processing provenance |
| `source_file` | STRING | Yes | Ingested file path/name |

Business key: **`(source_system, order_id, order_line_id)`**. Record ordering: latest `source_updated_at`; for equal timestamps use a deterministic source-file tie-break. Do not use nondeterministic `first()` for deduplication.

**Timestamp policy:** `order_date` is a DATE (represented as ISO `yyyy-MM-dd` when displayed), not a timezone-adjusted timestamp. `source_updated_at` is a UTC timestamp, independent from regional date formats.

## Reference tables

### `reference.fx_rates`

| Column | Type | Rule |
|---|---|---|
| `rate_date` | DATE | Equals `order_date` |
| `currency_code` | STRING | JPY / USD / EUR |
| `usd_per_unit` | DECIMAL(18,8) | Positive; USD is 1.00000000 |

One unique row per `(rate_date, currency_code)`. Generate deterministic daily rates covering **every synthetic order date**. Rates are *test fixtures*, not real trading rates. Missing rates cause quarantine, not an implicit 1.0 fallback.

### `reference.products`

A small deterministic lookup with unique `product_id`, `product_name`, and `product_category`; every valid transaction should reference an existing product. `dim_region` can be generated from fixed region codes and names.

## Validation and quarantine

Invalid records never enter Silver. Quarantine fields: original **source-shaped payload or JSON representation**, source_system, source_file, ingestion_timestamp, attempted key (if parseable), `reject_reason`, and optional `batch_id`. **Keep enough provenance to investigate the record.** Reasons may be joined into a delimited string if multiple checks fail.

| Rule | Example `reject_reason` |
|---|---|
| Missing/blank order ID or line ID | `MISSING_BUSINESS_KEY` |
| Missing or unknown SKU | `INVALID_PRODUCT` |
| Unparseable order date | `INVALID_ORDER_DATE` |
| Quantity null or <= 0 | `INVALID_QUANTITY` |
| Price missing or < 0 | `INVALID_PRICE` |
| Missing/invalid timestamp | `INVALID_SOURCE_TIMESTAMP` |
| FX rate not available for order date/currency | `MISSING_FX_RATE` |
| Malformed or rescued input payload | `MALFORMED_SOURCE_RECORD` |

Duplicate source versions with valid keys are **not necessarily errors**: retain the newest valid version in Silver. A newly arrived invalid correction should be quarantined; it should not automatically erase a previously valid record.

## Test data batches

Use a **fixed PRNG seed** and generated fixtures small enough to inspect manually. Record the seed and expected outputs in `tests/fixtures_manifest.json` when implemented.

- **Batch A (initial)**: e.g. 20 valid rows per region and a handful of deliberate invalid records, across two November dates. Include multiple orders and multiple lines per order.
- **Batch A replay**: run ingestion/transform *without uploading a new file*. Bronze must not grow; Silver keys and sums must stay identical.
- **Batch B (new files)**: new orders plus a valid correction to `ERP_US / US-2001 / 1` with a later modification timestamp. Silver must update exactly that key.
- **Batch C (quality)**: one record per major reject reason; verify quarantine reason and provenance. If all invalid cases are already represented in A, C is optional.

Keep input filenames unique, e.g. `erp_us_2026-11-01_batch01.jsonl`, `erp_us_2026-11-02_batch02.jsonl` (configure Auto Loader JSON extension/file filtering appropriately). The correction is *not* a rewritten copy of Batch A's file.

## Expected assertions

```sql
-- 1. Silver must have one record per business key.
SELECT source_system, order_id, order_line_id, COUNT(*) AS n
FROM global_erp_dev.silver.global_sales_unified
GROUP BY source_system, order_id, order_line_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- 2. Every Silver record has a valid business key and currency math.
SELECT COUNT(*) AS failures
FROM global_erp_dev.silver.global_sales_unified
WHERE source_system IS NULL OR order_id IS NULL OR order_line_id IS NULL
   OR quantity <= 0 OR fx_rate_to_usd <= 0
   OR ABS(sales_amount_usd - ROUND(quantity * unit_price_local * fx_rate_to_usd, 2)) > 0.01;
-- Expected: 0
```

Replace `global_erp_dev` with the actual configured catalog if necessary. Additional Gold checks: no orphan product/region references, Gold total revenue matches Silver (under the same rules), and monthly sums match fact rows.

## Cross-region caution

Real companies differ on order states, cancellations, timezones, FX conventions, tax treatments, and product master data. This PoC intentionally models **sales lines only** and assumes all valid rows represent recognized sales. Document these simplifications instead of presenting synthetic calculations as production accounting logic.
