/* =====================================================================
   Fabric Warehouse: wh_permian_ops  —  dimensional load procedures
   ---------------------------------------------------------------------
   Reads gold/silver Delta tables from the lakehouse in the same workspace
   via cross-database (three-part) names: lh_permian_ops.dbo.<table>.
   Every proc is idempotent: safe to re-run after a failure.
   Entry point: EXEC dw.usp_load_all @run_date = '2026-10-29';
   ===================================================================== */

/* ---------------- dim_well : SCD Type 2 ---------------- */
CREATE OR ALTER PROCEDURE dw.usp_load_dim_well
AS
BEGIN
    DECLARE @now datetime2(6) = SYSUTCDATETIME();

    -- staged source with a hash of the tracked attributes
    DROP TABLE IF EXISTS etl.stg_well;
    CREATE TABLE etl.stg_well AS
    SELECT  w.well_id, w.well_name, w.operator, w.county, w.state, w.basin, w.reservoir, w.lift_type,
            w.lateral_length_ft, CAST(w.first_prod_date AS date) AS first_prod_date,
            CAST(w.latitude AS decimal(9,5)) AS latitude, CAST(w.longitude AS decimal(9,5)) AS longitude, w.status,
            CONVERT(varchar(64), HASHBYTES('SHA2_256', CONCAT_WS('|', w.well_name, w.operator, w.county, w.reservoir,
                    w.lift_type, w.lateral_length_ft, w.status)), 2) AS row_hash
    FROM    lh_permian_ops.dbo.silver_wells AS w;

    -- 1) expire current rows whose attributes changed
    UPDATE d
       SET valid_to = @now, is_current = 0
      FROM dw.dim_well AS d
      JOIN etl.stg_well   AS s ON s.well_id = d.well_id
     WHERE d.is_current = 1 AND d.row_hash <> s.row_hash;

    -- 2) insert new wells + new versions of changed wells
    DECLARE @max_key int = (SELECT COALESCE(MAX(well_key), 0) FROM dw.dim_well);
    INSERT INTO dw.dim_well (well_key, well_id, well_name, operator, county, state, basin, reservoir, lift_type,
                             lateral_length_ft, first_prod_date, latitude, longitude, status, row_hash,
                             valid_from, valid_to, is_current)
    SELECT  @max_key + ROW_NUMBER() OVER (ORDER BY s.well_id),
            s.well_id, s.well_name, s.operator, s.county, s.state, s.basin, s.reservoir, s.lift_type,
            s.lateral_length_ft, s.first_prod_date, s.latitude, s.longitude, s.status, s.row_hash,
            CASE WHEN EXISTS (SELECT 1 FROM dw.dim_well x WHERE x.well_id = s.well_id) THEN @now
                 ELSE '1900-01-01' END,                   -- first version valid for all history
            NULL, 1
    FROM    etl.stg_well AS s
    WHERE   NOT EXISTS (SELECT 1 FROM dw.dim_well d WHERE d.well_id = s.well_id AND d.is_current = 1);

END;
GO

/* ---------------- Type 1 dimensions ---------------- */
CREATE OR ALTER PROCEDURE dw.usp_load_dim_equipment
AS
BEGIN
    UPDATE d SET well_id = s.well_id, equipment_type = s.equipment_type, manufacturer = s.manufacturer,
                 model = s.model, install_date = CAST(s.install_date AS date), rated_hp = s.rated_hp
      FROM dw.dim_equipment d JOIN lh_permian_ops.dbo.silver_equipment s ON s.equipment_id = d.equipment_id;

    DECLARE @max_key int = (SELECT COALESCE(MAX(equipment_key), 0) FROM dw.dim_equipment);
    INSERT INTO dw.dim_equipment (equipment_key, equipment_id, well_id, equipment_type, manufacturer, model, install_date, rated_hp)
    SELECT @max_key + ROW_NUMBER() OVER (ORDER BY s.equipment_id), s.equipment_id, s.well_id, s.equipment_type,
           s.manufacturer, s.model, CAST(s.install_date AS date), s.rated_hp
      FROM lh_permian_ops.dbo.silver_equipment s
     WHERE NOT EXISTS (SELECT 1 FROM dw.dim_equipment d WHERE d.equipment_id = s.equipment_id);
END;
GO

CREATE OR ALTER PROCEDURE dw.usp_load_dim_part_warehouse_failure
AS
BEGIN
    /* parts */
    DROP TABLE IF EXISTS etl.stg_part;
    CREATE TABLE etl.stg_part AS
    SELECT DISTINCT part_number, description, category, CAST(unit_cost AS decimal(12,2)) AS unit_cost, lead_time_days
      FROM lh_permian_ops.dbo.silver_parts_inventory;
    UPDATE d SET description = p.description, category = p.category, unit_cost = p.unit_cost, lead_time_days = p.lead_time_days
      FROM dw.dim_part d JOIN etl.stg_part p ON p.part_number = d.part_number;
    DECLARE @mp int = (SELECT COALESCE(MAX(part_key), 0) FROM dw.dim_part);
    INSERT INTO dw.dim_part (part_key, part_number, description, category, unit_cost, lead_time_days)
    SELECT @mp + ROW_NUMBER() OVER (ORDER BY part_number), part_number, description, category, unit_cost, lead_time_days
      FROM etl.stg_part p WHERE NOT EXISTS (SELECT 1 FROM dw.dim_part d WHERE d.part_number = p.part_number);

    /* warehouses */
    DECLARE @mw int = (SELECT COALESCE(MAX(warehouse_key), 0) FROM dw.dim_warehouse);
    INSERT INTO dw.dim_warehouse (warehouse_key, warehouse_name, region)
    SELECT @mw + ROW_NUMBER() OVER (ORDER BY warehouse), warehouse,
           CASE WHEN warehouse LIKE 'Pecos%' THEN 'Delaware Basin' ELSE 'Midland Basin' END
      FROM (SELECT DISTINCT warehouse FROM lh_permian_ops.dbo.silver_parts_inventory) s
     WHERE NOT EXISTS (SELECT 1 FROM dw.dim_warehouse d WHERE d.warehouse_name = s.warehouse);

    /* failure modes */
    DECLARE @mf int = (SELECT COALESCE(MAX(failure_mode_key), 0) FROM dw.dim_failure_mode);
    INSERT INTO dw.dim_failure_mode (failure_mode_key, failure_mode, failure_category)
    SELECT @mf + ROW_NUMBER() OVER (ORDER BY failure_mode), failure_mode,
           CASE WHEN failure_mode LIKE '%Motor%' OR failure_mode LIKE '%Cable%' OR failure_mode LIKE '%trip%' THEN 'Electrical'
                WHEN failure_mode IN ('Gas lock', 'Injection line freeze', 'Tubing leak')                    THEN 'Flow assurance'
                ELSE 'Mechanical' END
      FROM (SELECT DISTINCT failure_mode FROM lh_permian_ops.dbo.silver_work_orders WHERE failure_mode IS NOT NULL) s
     WHERE NOT EXISTS (SELECT 1 FROM dw.dim_failure_mode d WHERE d.failure_mode = s.failure_mode);
END;
GO

/* ---------------- fact_production_daily : delete/insert for the reprocessed window ---------------- */
CREATE OR ALTER PROCEDURE dw.usp_load_fact_production_daily
    @from_date date
AS
BEGIN
    DECLARE @from_key int = CONVERT(int, FORMAT(@from_date, 'yyyyMMdd'));
    DELETE FROM dw.fact_production_daily WHERE date_key >= @from_key;

    INSERT INTO dw.fact_production_daily
    SELECT  CONVERT(int, FORMAT(g.production_date, 'yyyyMMdd')),
            COALESCE(w.well_key, -1),
            g.oil_bbl, g.gas_mcf, g.water_bbl, g.boe, g.uptime_hours, g.potential_oil_bbl, g.deferred_oil_bbl,
            g.tubing_pressure_psi, g.casing_pressure_psi, g.wti_usd_bbl, g.henry_hub_usd_mmbtu,
            g.est_revenue_usd, g.deferred_revenue_usd, SYSUTCDATETIME()
    FROM    lh_permian_ops.dbo.gold_production_daily AS g
    LEFT JOIN dw.dim_well AS w                        -- point-in-time lookup against SCD2 history
           ON w.well_id = g.well_id
          AND CAST(g.production_date AS datetime2(6)) >= w.valid_from
          AND (w.valid_to IS NULL OR CAST(g.production_date AS datetime2(6)) < w.valid_to)
    WHERE   g.production_date >= @from_date;
END;
GO

CREATE OR ALTER PROCEDURE dw.usp_load_fact_equipment_health_daily
    @from_date date
AS
BEGIN
    DECLARE @from_key int = CONVERT(int, FORMAT(@from_date, 'yyyyMMdd'));
    DELETE FROM dw.fact_equipment_health_daily WHERE date_key >= @from_key;

    INSERT INTO dw.fact_equipment_health_daily
    SELECT  CONVERT(int, FORMAT(h.reading_date, 'yyyyMMdd')),
            COALESCE(e.equipment_key, -1), COALESCE(w.well_key, -1),
            h.avg_motor_temp_f, h.max_motor_temp_f, h.avg_vibration_ips, h.max_vibration_ips, h.avg_motor_amps,
            h.anomaly_score, h.health_status, h.failed_within_7d, h.reading_count, SYSUTCDATETIME()
    FROM    lh_permian_ops.dbo.gold_equipment_health_daily AS h
    LEFT JOIN dw.dim_equipment AS e ON e.equipment_id = h.equipment_id
    LEFT JOIN dw.dim_well      AS w ON w.well_id = e.well_id AND w.is_current = 1
    WHERE   h.reading_date >= @from_date;
END;
GO

/* ---------------- fact_work_order : full refresh (small, and closed WOs can be re-costed) ---------------- */
CREATE OR ALTER PROCEDURE dw.usp_load_fact_work_order
AS
BEGIN
    DELETE FROM dw.fact_work_order;
    INSERT INTO dw.fact_work_order
    SELECT  s.work_order_id,
            CONVERT(int, FORMAT(CAST(s.opened_ts AS date), 'yyyyMMdd')),
            CASE WHEN s.closed_ts IS NOT NULL THEN CONVERT(int, FORMAT(CAST(s.closed_ts AS date), 'yyyyMMdd')) END,
            COALESCE(w.well_key, -1), COALESCE(e.equipment_key, -1), COALESCE(p.part_key, -1), COALESCE(f.failure_mode_key, -1),
            s.wo_type, s.priority, s.status,
            CAST(DATEDIFF(minute, s.opened_ts, s.closed_ts) / 60.0 AS decimal(9,2)),
            s.part_qty, s.labor_hours, s.labor_cost, s.parts_cost,
            COALESCE(s.labor_cost, 0) + COALESCE(s.parts_cost, 0),
            SYSUTCDATETIME()
    FROM    lh_permian_ops.dbo.silver_work_orders AS s
    LEFT JOIN dw.dim_well         w ON w.well_id = s.well_id AND w.is_current = 1
    LEFT JOIN dw.dim_equipment    e ON e.equipment_id = s.equipment_id
    LEFT JOIN dw.dim_part         p ON p.part_number = s.part_number
    LEFT JOIN dw.dim_failure_mode f ON f.failure_mode = s.failure_mode;
END;
GO

CREATE OR ALTER PROCEDURE dw.usp_load_fact_inventory_snapshot
AS
BEGIN
    DELETE FROM dw.fact_inventory_snapshot
     WHERE snapshot_date_key IN (SELECT DISTINCT CONVERT(int, FORMAT(CAST(snapshot_date AS date), 'yyyyMMdd'))
                                       FROM lh_permian_ops.dbo.gold_inventory_risk);
    INSERT INTO dw.fact_inventory_snapshot
    SELECT  CONVERT(int, FORMAT(CAST(r.snapshot_date AS date), 'yyyyMMdd')),
            COALESCE(p.part_key, -1), COALESCE(wh.warehouse_key, -1),
            r.qty_on_hand, r.reorder_point, r.avg_daily_usage, r.days_of_cover, r.inventory_value_usd,
            r.reorder_flag, SYSUTCDATETIME()
    FROM    lh_permian_ops.dbo.gold_inventory_risk r
    LEFT JOIN dw.dim_part      p  ON p.part_number = r.part_number
    LEFT JOIN dw.dim_warehouse wh ON wh.warehouse_name = r.warehouse;
END;
GO

/* ---------------- orchestrator with error handling + audit ---------------- */
CREATE OR ALTER PROCEDURE dw.usp_load_all
    @run_date       date,
    @reprocess_days int = 15,
    @run_id         varchar(64) = NULL
AS
BEGIN
    DECLARE @from_date date = DATEADD(day, -@reprocess_days, @run_date);
    DECLARE @step varchar(100) = 'start';
    SET @run_id = COALESCE(@run_id, CONVERT(varchar(64), NEWID()));

    BEGIN TRY
        -- dimensions first, then facts (facts look up surrogate keys)
        SET @step = 'dim_well';             EXEC dw.usp_load_dim_well;
        SET @step = 'dim_equipment';        EXEC dw.usp_load_dim_equipment;
        SET @step = 'dim_part_wh_failure';  EXEC dw.usp_load_dim_part_warehouse_failure;

        BEGIN TRANSACTION;                  -- facts load atomically: reports never see a half-loaded day
        SET @step = 'fact_production_daily';       EXEC dw.usp_load_fact_production_daily @from_date;
        SET @step = 'fact_equipment_health_daily'; EXEC dw.usp_load_fact_equipment_health_daily @from_date;
        SET @step = 'fact_work_order';             EXEC dw.usp_load_fact_work_order;
        SET @step = 'fact_inventory_snapshot';     EXEC dw.usp_load_fact_inventory_snapshot;
        COMMIT TRANSACTION;

        EXEC etl.usp_log_pipeline_run 'dw.usp_load_all', @run_id, 'warehouse_star_schema', 'Succeeded', 0, NULL;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @msg varchar(4000) = CONCAT('Step ', @step, ' failed: ', ERROR_MESSAGE());
        EXEC etl.usp_log_pipeline_run 'dw.usp_load_all', @run_id, @step, 'Failed', 0, @msg;
        THROW;                              -- surface the failure to the Fabric pipeline
    END CATCH;
END;
GO

/* ---------------- post-load reconciliation: warehouse vs lakehouse ---------------- */
CREATE OR ALTER VIEW dw.vw_reconciliation AS
SELECT 'production_oil_bbl' AS check_name,
       (SELECT CAST(SUM(oil_bbl) AS decimal(18,2)) FROM lh_permian_ops.dbo.gold_production_daily) AS lakehouse_value,
       (SELECT CAST(SUM(oil_bbl) AS decimal(18,2)) FROM dw.fact_production_daily)                  AS warehouse_value
UNION ALL
SELECT 'production_rows',
       (SELECT COUNT_BIG(*) FROM lh_permian_ops.dbo.gold_production_daily),
       (SELECT COUNT_BIG(*) FROM dw.fact_production_daily)
UNION ALL
SELECT 'unknown_well_rows', 0, (SELECT COUNT_BIG(*) FROM dw.fact_production_daily WHERE well_key = -1);
GO
