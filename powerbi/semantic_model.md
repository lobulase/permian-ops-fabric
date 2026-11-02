# Power BI semantic model — `sm_permian_ops`

**Storage mode:** Direct Lake on the Fabric Warehouse `wh_permian_ops` (schema `dw`).
No import refresh copies the data; the "refresh" in `pl_fabric_transform` just reframes
the model onto the latest Delta versions, so it takes seconds.

## Tables and relationships

| From (many) | Column | To (one) | Column | Notes |
|---|---|---|---|---|
| fact_production_daily | date_key | dim_date | date_key | active |
| fact_production_daily | well_key | dim_well | well_key | |
| fact_equipment_health_daily | date_key | dim_date | date_key | active |
| fact_equipment_health_daily | equipment_key | dim_equipment | equipment_key | |
| fact_equipment_health_daily | well_key | dim_well | well_key | |
| fact_work_order | opened_date_key | dim_date | date_key | active |
| fact_work_order | closed_date_key | dim_date | date_key | **inactive** — used with `USERELATIONSHIP` |
| fact_work_order | well_key / equipment_key / part_key / failure_mode_key | dims | | |
| fact_inventory_snapshot | snapshot_date_key / part_key / warehouse_key | dims | | |

All relationships single-direction, dimension → fact. `dim_date` marked as the date table.
`dim_well` is SCD2: report visuals filter on `is_current = 1` attributes via the
`Well (current)` hierarchy; history is kept so production stays tied to the operator
that owned the well on that day.

## Report pages

1. **Production overview** — BOE trend, oil/gas/water by county and lift type, uptime %, WTI overlay.
2. **Deferred production** — lost barrels and revenue by well and by failure mode; top-10 deferment wells.
3. **Pump health watchlist** — wells in Watch / Critical with anomaly score trend; drill-through to sensor detail.
4. **Reliability** — failures, MTBF, MTTR, cost per failure by lift type and manufacturer.
5. **MRO inventory** — parts below reorder point, days of cover vs. lead time, inventory value by warehouse.
6. **Pipeline health** (ops page) — rows loaded, quarantined rows by rule, failed runs (from `etl.vw_pipeline_health`).

## Row-level security

Role `Operator` — `dim_well[operator] = LOOKUPVALUE(user_operator_map[operator], user_operator_map[upn], USERPRINCIPALNAME())`
so each operator partner sees only their own wells.

Measures are in [`measures.dax`](measures.dax).
