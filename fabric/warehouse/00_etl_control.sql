/* =====================================================================
   Fabric Warehouse: wh_permian_ops  —  ETL control & audit objects
   ---------------------------------------------------------------------
   * etl.ingestion_control : metadata that drives pl_ingest_erp_incremental
                             (add a row = add a new source table, no pipeline change)
   * etl.pipeline_run_log  : one row per entity per run, written by ADF
   Fabric Warehouse notes: constraints are NOT ENFORCED, no DEFAULT
   constraints, datetime2 max precision 6 — written to respect those.
   ===================================================================== */
IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl');
GO

IF OBJECT_ID('etl.ingestion_control') IS NULL
CREATE TABLE etl.ingestion_control (
    entity_name        varchar(50)   NOT NULL,
    source_system      varchar(20)   NOT NULL,     -- ERP | SCADA | IOT | EIA
    source_schema      varchar(50)   NULL,
    source_table       varchar(100)  NULL,
    load_type          varchar(12)   NOT NULL,     -- FULL | INCREMENTAL
    watermark_column   varchar(50)   NULL,
    last_watermark     datetime2(6)  NULL,
    is_active          bit           NOT NULL,
    updated_at         datetime2(6)  NULL
);
GO

IF OBJECT_ID('etl.pipeline_run_log') IS NULL
CREATE TABLE etl.pipeline_run_log (
    pipeline_name      varchar(100)  NOT NULL,
    run_id             varchar(64)   NOT NULL,
    entity_name        varchar(200)  NULL,
    status             varchar(20)   NOT NULL,     -- Succeeded | Failed | NoData
    rows_copied        bigint        NULL,
    error_message      varchar(4000) NULL,
    logged_at          datetime2(6)  NOT NULL
);
GO

/* seed control rows (idempotent) */
DELETE FROM etl.ingestion_control WHERE source_system = 'ERP';
INSERT INTO etl.ingestion_control
    (entity_name, source_system, source_schema, source_table, load_type, watermark_column, last_watermark, is_active, updated_at)
VALUES
    ('wells',           'ERP', 'ops', 'wells',           'FULL',        'modified_at', '1900-01-01', 1, SYSUTCDATETIME()),
    ('equipment',       'ERP', 'ops', 'equipment',       'FULL',        'modified_at', '1900-01-01', 1, SYSUTCDATETIME()),
    ('parts_inventory', 'ERP', 'ops', 'parts_inventory', 'FULL',        'modified_at', '1900-01-01', 1, SYSUTCDATETIME()),
    ('work_orders',     'ERP', 'ops', 'work_orders',     'INCREMENTAL', 'modified_at', '1900-01-01', 1, SYSUTCDATETIME());
GO

CREATE OR ALTER PROCEDURE etl.usp_update_watermark
    @entity_name   varchar(50),
    @new_watermark varchar(30)
AS
BEGIN
    UPDATE etl.ingestion_control
       SET last_watermark = CAST(@new_watermark AS datetime2(6)),
           updated_at     = SYSUTCDATETIME()
     WHERE entity_name = @entity_name
       AND (last_watermark IS NULL OR CAST(@new_watermark AS datetime2(6)) > last_watermark);
END;
GO

CREATE OR ALTER PROCEDURE etl.usp_log_pipeline_run
    @pipeline_name varchar(100),
    @run_id        varchar(64),
    @entity_name   varchar(200),
    @status        varchar(20),
    @rows_copied   bigint = 0,
    @error_message varchar(4000) = NULL
AS
BEGIN
    INSERT INTO etl.pipeline_run_log (pipeline_name, run_id, entity_name, status, rows_copied, error_message, logged_at)
    VALUES (@pipeline_name, @run_id, @entity_name, @status, @rows_copied, NULLIF(@error_message, ''), SYSUTCDATETIME());
END;
GO

/* Production-support view: what failed or went quiet in the last 7 days? */
CREATE OR ALTER VIEW etl.vw_pipeline_health AS
SELECT  CAST(logged_at AS date)                                  AS run_date,
        pipeline_name,
        entity_name,
        SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END)    AS succeeded,
        SUM(CASE WHEN status = 'Failed'    THEN 1 ELSE 0 END)    AS failed,
        SUM(CASE WHEN status = 'NoData'    THEN 1 ELSE 0 END)    AS no_data,
        SUM(rows_copied)                                         AS rows_copied,
        MAX(CASE WHEN status = 'Failed' THEN error_message END)  AS last_error
FROM    etl.pipeline_run_log
WHERE   logged_at >= DATEADD(day, -7, SYSUTCDATETIME())
GROUP BY CAST(logged_at AS date), pipeline_name, entity_name;
GO
