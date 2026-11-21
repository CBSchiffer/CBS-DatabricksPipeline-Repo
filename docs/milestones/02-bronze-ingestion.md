# M2 — Bronze: incremental file ingestion with Auto Loader

**Target:** November 23–24, 2026  
**Depends on:** M1  
**Outcome:** Three regional append-only Bronze Delta tables, each updated incrementally through Auto Loader.

## Learning goals

Understand Databricks' file-streaming interface, `cloudFiles`, checkpoint persistence, schema inference, metadata fields, and Delta table writes. Compare this approach with NiFi/Kafka ingestion where useful, while focusing on the Databricks implementation itself.

## Required input/output

| Source path | Target table | Format |
|---|---|---|
| `/Volumes/global_erp_dev/ops/landing/jp/` | `global_erp_dev.bronze.erp_jp_raw` | CSV, Japanese headers |
| `/Volumes/global_erp_dev/ops/landing/us/` | `global_erp_dev.bronze.erp_us_raw` | JSON Lines |
| `/Volumes/global_erp_dev/ops/landing/eu/` | `global_erp_dev.bronze.erp_eu_raw` | Pipe-delimited CSV |

The **Bronze table is source-shaped**: keep original data fields, including nested JSON for US, where possible. Add:

- `source_system` (`ERP_JP`, `ERP_US`, or `ERP_EU`)
- `region` (`JP`, `US`, or `EU`)
- `source_file` (from file metadata)
- `ingestion_timestamp` (timestamp at ingestion)
- `_rescued_data` when using schema rescue, if produced

Do **not** do FX conversion, business deduplication, or quarantine here: those belong in M3. Bronze is append-only and preserves revisions across files.

## Execution plan

### A. Implement Japan first

- [ ] Create `notebooks/01_bronze.py` or equivalent Notebook.
- [ ] Accept notebook parameters for `catalog`, `source_system`, `source_path`, `checkpoint_path`, `schema_path`, and `target_table` (or wrap the shared logic in a reusable function).
- [ ] Use `spark.readStream.format('cloudFiles')` and `.option('cloudFiles.format', 'csv')`.
- [ ] Set CSV options for `header=true`, `encoding=UTF-8`, and read values as strings to avoid type inference surprises.
- [ ] Set `cloudFiles.schemaLocation` to a persistent, source-specific state directory.
- [ ] Add metadata columns, selecting file path before writing.
- [ ] Write to a managed Delta table with `writeStream.format('delta').toTable(...)` and a source-specific `checkpointLocation`.
- [ ] Run with `trigger(availableNow=True)` and wait for completion using `awaitTermination()`.

**Illustrative structure (adapt parameters and syntax as you implement):**

```python
from pyspark.sql import functions as F

source = '/Volumes/global_erp_dev/ops/landing/jp/'
schema_dir = '/Volumes/global_erp_dev/ops/pipeline_state/schemas/jp'
checkpoint_dir = '/Volumes/global_erp_dev/ops/pipeline_state/checkpoints/jp'
target = 'global_erp_dev.bronze.erp_jp_raw'

raw = (
    spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('cloudFiles.schemaLocation', schema_dir)
    .option('header', 'true')
    .option('encoding', 'UTF-8')
    .load(source)
)
bronze = (raw
    .withColumn('source_system', F.lit('ERP_JP'))
    .withColumn('region', F.lit('JP'))
    .withColumn('source_file', F.col('_metadata.file_path'))
    .withColumn('ingestion_timestamp', F.current_timestamp()))

query = (bronze.writeStream.format('delta')
    .outputMode('append')
    .option('checkpointLocation', checkpoint_dir)
    .trigger(availableNow=True)
    .toTable(target))
query.awaitTermination()
```

This is a *starter pattern*, not a guarantee that every workspace/compute combination behaves identically. Validate options in the actual runtime. Use distinct paths for each of the three source streams.

### B. Extend to US and EU

- [ ] US: `cloudFiles.format=json`; preserve nested JSON structure, including product fields.
- [ ] EU: `cloudFiles.format=csv`, `sep='|'`, `header=true`.
- [ ] Set schema/checkpoint directories to `us` and `eu` respectively.
- [ ] Keep the same metadata names in all Bronze tables.
- [ ] Confirm each target table is queryable in Catalog Explorer and Spark SQL.

**Schema caution:** Auto Loader generally infers CSV/JSON columns as strings by default, which is desirable here. If you use explicit schemas or hints, make sure `unitPrice` and date fields do not get prematurely cast or lost. Source-specific structure is part of the demonstration.

### C. Run incremental tests

1. Run Batch A for all regions; capture each Bronze count and distinct source file list.
2. Rerun the same ingestion code **without adding files**. Confirm the count and source file set are unchanged.
3. Upload one **new, uniquely named file** containing several new records to JP only.
4. Run the same ingestion notebook again. Confirm JP count increases by the expected number while US and EU remain unchanged.
5. Keep existing checkpoint/state directories intact. Do not reset them between ordinary reruns.

The testing nuance is important: Bronze's source file count demonstrates *file-level incremental ingestion*; Silver's distinct business keys demonstrate *business-level idempotency*. Those are different properties.

## Suggested verification SQL

```sql
SELECT source_system, COUNT(*) AS rows_ingested,
       COUNT(DISTINCT source_file) AS files_ingested
FROM global_erp_dev.bronze.erp_jp_raw
GROUP BY source_system;

SELECT source_file, COUNT(*) AS rows_per_file
FROM global_erp_dev.bronze.erp_jp_raw
GROUP BY source_file
ORDER BY source_file;
```

Capture the results before and after a rerun. Do not use only total row count as proof of incremental behavior if the source files contain empty records or schema issues.

## Failure handling and tradeoffs

- **Do not overwrite landed files:** by default Auto Loader tracks files as processed; corrections arrive as *new immutable files*.
- **Do not remove the checkpoint** to fix an ordinary retry problem: doing so can replay source files and duplicate Bronze rows.
- **Separate ingestion failures from business errors:** malformed files may need parser-specific handling, but a *well-formed* row with negative quantity should still land in Bronze for M3 quarantine.
- **Avoid continuous streaming for this PoC:** `availableNow` processes available files and exits, enabling lower-cost job execution.
- **Avoid old DBFS-root paths:** volumes provide explicit governance and keep the design aligned with current Databricks conventions.

## Deliverables

- Working Auto Loader notebook or reusable function for three source systems.
- Three Bronze Delta tables with metadata.
- Persistent, distinct checkpoint and schema directories.
- SQL validation notes or screenshots proving that a rerun reads only new files.

## Acceptance criteria

- [ ] Three Bronze tables are created and queryable.
- [ ] Each Bronze row contains `source_file`, `source_system`, `region`, and `ingestion_timestamp`.
- [ ] Repeating ingestion with unchanged landing files adds **zero rows**.
- [ ] Adding a new regional file processes its records on the next run.
- [ ] Ingestion uses a saved checkpoint and does not require manually resetting state.

## Interview demo talking point

> "I used Auto Loader for incremental file discovery with independent per-source checkpoints, and wrote raw, traceable data to append-only Bronze Delta tables. I intentionally kept business validation out of ingestion so I could isolate bad ERP records without losing source provenance."

## Official references

- https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema
- https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/production
- https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/faq
- https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/options
