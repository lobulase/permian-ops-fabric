# Production support runbook

How to troubleshoot the daily load. Start with the alert in Teams; it carries the ADF run ID.

## 1. Where did it fail?

```sql
-- Fabric Warehouse: last 7 days of entity-level results
SELECT * FROM etl.vw_pipeline_health ORDER BY run_date DESC, failed DESC;

-- The exact error for a run
SELECT * FROM etl.pipeline_run_log WHERE run_id = '<adf-run-id>' ORDER BY logged_at;
```

In ADF Monitor, open `pl_master_daily` → the failed child pipeline → the failed activity's error JSON.

## 2. Common failures

| Symptom | Likely cause | Fix |
|---|---|---|
| `Run_ErpIngestion` fails, *login failed* / timeout | ERP password rotated or SHIR offline | Update `erp-sql-password` in Key Vault; check SHIR node status in ADF → Integration runtimes. Re-run the master pipeline; the watermark only advances after a successful copy, so nothing is missed. |
| `Log_NoFilesArrived` row with status `NoData` | SCADA historian export didn't run | Confirm with the SCADA team; when the file lands, re-run `pl_ingest_landing_files` for that folder, then `pl_fabric_transform` with the same `run_date`. |
| `nb_01` fails with *DQ circuit breaker tripped* | >5 % of rows quarantined — usually a source format change | `SELECT _dq_entity, _dq_failed_rules, COUNT(*) FROM silver_dq_quarantine WHERE run_date = '<date>' GROUP BY ALL` — look at `record_json` samples, fix the mapping or have the source corrected, re-run. |
| `Load_StarSchema` fails | Schema drift in gold, or a dim key collision | Error message names the step (`Step fact_production_daily failed: …`). The fact transaction rolled back, so reports still show yesterday's data. Fix and re-run — procs are idempotent. |
| `Fail_Reconciliation` | Warehouse ≠ lakehouse totals | `SELECT * FROM dw.vw_reconciliation`. `unknown_well_rows > 0` means production for a well that isn't in the ERP well master yet — get the master record added; it self-heals on the next run. |
| EIA step fails | API key expired / EIA outage | Non-critical by design: transform still runs and revenue forward-fills the last price. Renew `eia-api-key`. |

## 3. Back-filling a date range

1. Re-drop the source files (or reset the watermark: `UPDATE etl.ingestion_control SET last_watermark = '<date>' WHERE entity_name = 'work_orders'`).
2. Run `pl_fabric_transform` with `run_date` = the last day of the range; for >15 days, set `full_reload = true` on `nb_01`.
3. `EXEC dw.usp_load_all @run_date = '<date>', @reprocess_days = <n>;`
4. Check `dw.vw_reconciliation`, then refresh the semantic model.
