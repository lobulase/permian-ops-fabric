/* =====================================================================
   Fabric Warehouse: wh_permian_ops  —  reporting views (schema rpt)
   Ad-hoc / analyst access; Power BI uses the dw tables directly.
   ===================================================================== */
IF SCHEMA_ID('rpt') IS NULL EXEC('CREATE SCHEMA rpt');
GO

/* Monthly production and deferment by operator / county */
CREATE OR ALTER VIEW rpt.vw_monthly_production AS
SELECT  d.year_month, w.operator, w.county, w.lift_type,
        SUM(f.oil_bbl)               AS oil_bbl,
        SUM(f.gas_mcf)               AS gas_mcf,
        SUM(f.boe)                   AS boe,
        SUM(f.deferred_oil_bbl)      AS deferred_oil_bbl,
        SUM(f.est_revenue_usd)       AS est_revenue_usd,
        SUM(f.deferred_revenue_usd)  AS deferred_revenue_usd,
        CAST(SUM(f.uptime_hours) / NULLIF(COUNT(*) * 24.0, 0) * 100 AS decimal(5,2)) AS uptime_pct
FROM    dw.fact_production_daily f
JOIN    dw.dim_date d ON d.date_key = f.date_key
JOIN    dw.dim_well w ON w.well_key = f.well_key
GROUP BY d.year_month, w.operator, w.county, w.lift_type;
GO

/* Reliability by lift type: failures, MTBF, MTTR, cost per failure */
CREATE OR ALTER VIEW rpt.vw_lift_reliability AS
WITH failures AS (
    SELECT w.lift_type, COUNT(*) AS failures,
           AVG(wo.downtime_hours) AS mttr_hours,
           SUM(wo.total_cost) AS repair_cost
    FROM dw.fact_work_order wo
    JOIN dw.dim_well w ON w.well_key = wo.well_key
    WHERE wo.wo_type = 'Corrective'
    GROUP BY w.lift_type
), exposure AS (     -- pump-days observed in the health fact
    SELECT w.lift_type, COUNT(*) AS pump_days
    FROM dw.fact_equipment_health_daily h
    JOIN dw.dim_well w ON w.well_key = h.well_key
    GROUP BY w.lift_type
)
SELECT  f.lift_type, f.failures, e.pump_days,
        CAST(e.pump_days * 1.0 / NULLIF(f.failures, 0) AS decimal(10,1)) AS mtbf_days,
        CAST(f.mttr_hours AS decimal(10,1))                             AS mttr_hours,
        CAST(f.repair_cost / NULLIF(f.failures, 0) AS decimal(12,2))    AS avg_cost_per_failure
FROM    failures f LEFT JOIN exposure e ON e.lift_type = f.lift_type;
GO

/* Pumps flagged Watch/Critical today, with what's in stock nearby to fix them */
CREATE OR ALTER VIEW rpt.vw_pump_watchlist AS
WITH latest AS (SELECT MAX(date_key) AS date_key FROM dw.fact_equipment_health_daily)
SELECT  d.[date], w.well_name, w.county, e.equipment_id, e.equipment_type, e.manufacturer,
        h.health_status, h.anomaly_score, h.avg_motor_temp_f, h.avg_vibration_ips, h.avg_motor_amps
FROM    dw.fact_equipment_health_daily h
JOIN    latest l            ON l.date_key = h.date_key
JOIN    dw.dim_date d       ON d.date_key = h.date_key
JOIN    dw.dim_equipment e  ON e.equipment_key = h.equipment_key
JOIN    dw.dim_well w       ON w.well_key = h.well_key
WHERE   h.health_status IN ('Watch', 'Critical');
GO

/* Parts at or below reorder point, valued */
CREATE OR ALTER VIEW rpt.vw_reorder_list AS
SELECT  wh.warehouse_name, p.part_number, p.description, p.category,
        f.qty_on_hand, f.reorder_point, f.days_of_cover, p.lead_time_days,
        p.unit_cost, GREATEST(f.reorder_point * 2 - f.qty_on_hand, 0) AS suggested_order_qty
FROM    dw.fact_inventory_snapshot f
JOIN    dw.dim_part p       ON p.part_key = f.part_key
JOIN    dw.dim_warehouse wh ON wh.warehouse_key = f.warehouse_key
WHERE   f.reorder_flag = 1
  AND   f.snapshot_date_key = (SELECT MAX(snapshot_date_key) FROM dw.fact_inventory_snapshot);
GO
