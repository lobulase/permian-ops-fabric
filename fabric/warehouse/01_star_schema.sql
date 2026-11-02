/* =====================================================================
   Fabric Warehouse: wh_permian_ops  —  dimensional model (schema dw)
   ---------------------------------------------------------------------
   Grain of each fact:
     fact_production_daily        one row per well per day
     fact_equipment_health_daily  one row per pump per day
     fact_work_order              one row per maintenance work order
     fact_inventory_snapshot      one row per part per warehouse per snapshot date
   Conformed dimensions: dim_date, dim_well (SCD Type 2), dim_equipment,
   dim_part, dim_warehouse, dim_failure_mode.
   Surrogate keys are int; -1 = 'Unknown' member so facts never drop rows.
   ===================================================================== */
IF SCHEMA_ID('dw') IS NULL EXEC('CREATE SCHEMA dw');
GO

/* ---------------- dimensions ---------------- */
IF OBJECT_ID('dw.dim_date') IS NULL
CREATE TABLE dw.dim_date (
    date_key        int          NOT NULL,   -- yyyymmdd
    [date]          date         NOT NULL,
    [year]          smallint     NOT NULL,
    quarter_num     smallint      NOT NULL,
    month_num       smallint      NOT NULL,
    month_name      varchar(10)  NOT NULL,
    year_month      char(7)      NOT NULL,   -- 2026-07
    week_of_year    smallint      NOT NULL,
    day_of_week     smallint      NOT NULL,
    day_name        varchar(10)  NOT NULL,
    is_weekend      bit          NOT NULL,
    days_in_month   smallint      NOT NULL
);

IF OBJECT_ID('dw.dim_well') IS NULL
CREATE TABLE dw.dim_well (
    well_key           int           NOT NULL,
    well_id            varchar(20)   NOT NULL,     -- business key (API number)
    well_name          varchar(60)   NULL,
    operator           varchar(60)   NULL,
    county             varchar(40)   NULL,
    state              char(2)       NULL,
    basin              varchar(30)   NULL,
    reservoir          varchar(30)   NULL,
    lift_type          varchar(20)   NULL,
    lateral_length_ft  int           NULL,
    first_prod_date    date          NULL,
    latitude           decimal(9,5)  NULL,
    longitude          decimal(9,5)  NULL,
    status             varchar(20)   NULL,
    row_hash           varchar(64)   NOT NULL,     -- change detection for SCD2
    valid_from         datetime2(6)  NOT NULL,
    valid_to           datetime2(6)  NULL,
    is_current         bit           NOT NULL
);

IF OBJECT_ID('dw.dim_equipment') IS NULL
CREATE TABLE dw.dim_equipment (
    equipment_key   int          NOT NULL,
    equipment_id    varchar(20)  NOT NULL,
    well_id         varchar(20)  NULL,
    equipment_type  varchar(30)  NULL,
    manufacturer    varchar(40)  NULL,
    model           varchar(30)  NULL,
    install_date    date         NULL,
    rated_hp        int          NULL
);

IF OBJECT_ID('dw.dim_part') IS NULL
CREATE TABLE dw.dim_part (
    part_key        int           NOT NULL,
    part_number     varchar(20)   NOT NULL,
    description     varchar(80)   NULL,
    category        varchar(30)   NULL,
    unit_cost       decimal(12,2) NULL,
    lead_time_days  int           NULL
);

IF OBJECT_ID('dw.dim_warehouse') IS NULL
CREATE TABLE dw.dim_warehouse (
    warehouse_key   int          NOT NULL,
    warehouse_name  varchar(40)  NOT NULL,
    region          varchar(30)  NULL
);

IF OBJECT_ID('dw.dim_failure_mode') IS NULL
CREATE TABLE dw.dim_failure_mode (
    failure_mode_key  int          NOT NULL,
    failure_mode      varchar(60)  NOT NULL,
    failure_category  varchar(30)  NULL       -- Electrical | Mechanical | Flow assurance | Planned
);

/* ---------------- facts ---------------- */
IF OBJECT_ID('dw.fact_production_daily') IS NULL
CREATE TABLE dw.fact_production_daily (
    date_key              int            NOT NULL,
    well_key              int            NOT NULL,
    oil_bbl               decimal(12,2)  NULL,
    gas_mcf               decimal(12,2)  NULL,
    water_bbl             decimal(12,2)  NULL,
    boe                   decimal(12,2)  NULL,
    uptime_hours          decimal(5,2)   NULL,
    potential_oil_bbl     decimal(12,2)  NULL,
    deferred_oil_bbl      decimal(12,2)  NULL,
    tubing_pressure_psi   decimal(8,1)   NULL,
    casing_pressure_psi   decimal(8,1)   NULL,
    wti_usd_bbl           decimal(8,2)   NULL,
    henry_hub_usd_mmbtu   decimal(8,3)   NULL,
    est_revenue_usd       decimal(14,2)  NULL,
    deferred_revenue_usd  decimal(14,2)  NULL,
    load_ts               datetime2(6)   NOT NULL
);

IF OBJECT_ID('dw.fact_equipment_health_daily') IS NULL
CREATE TABLE dw.fact_equipment_health_daily (
    date_key             int           NOT NULL,
    equipment_key        int           NOT NULL,
    well_key             int           NOT NULL,
    avg_motor_temp_f     decimal(7,2)  NULL,
    max_motor_temp_f     decimal(7,2)  NULL,
    avg_vibration_ips    decimal(7,4)  NULL,
    max_vibration_ips    decimal(7,4)  NULL,
    avg_motor_amps       decimal(7,2)  NULL,
    anomaly_score        decimal(7,2)  NULL,
    health_status        varchar(10)   NULL,
    failed_within_7d     bit           NULL,
    reading_count        int           NULL,
    load_ts              datetime2(6)  NOT NULL
);

IF OBJECT_ID('dw.fact_work_order') IS NULL
CREATE TABLE dw.fact_work_order (
    work_order_id      varchar(20)    NOT NULL,   -- degenerate dimension
    opened_date_key    int            NOT NULL,
    closed_date_key    int            NULL,
    well_key           int            NOT NULL,
    equipment_key      int            NOT NULL,
    part_key           int            NOT NULL,
    failure_mode_key   int            NOT NULL,
    wo_type            varchar(20)    NULL,
    priority           varchar(5)     NULL,
    status             varchar(15)    NULL,
    downtime_hours     decimal(9,2)   NULL,
    part_qty           int            NULL,
    labor_hours        decimal(8,1)   NULL,
    labor_cost         decimal(12,2)  NULL,
    parts_cost         decimal(12,2)  NULL,
    total_cost         decimal(12,2)  NULL,
    load_ts            datetime2(6)   NOT NULL
);

IF OBJECT_ID('dw.fact_inventory_snapshot') IS NULL
CREATE TABLE dw.fact_inventory_snapshot (
    snapshot_date_key    int            NOT NULL,
    part_key             int            NOT NULL,
    warehouse_key        int            NOT NULL,
    qty_on_hand          int            NULL,
    reorder_point        int            NULL,
    avg_daily_usage      decimal(10,4)  NULL,
    days_of_cover        decimal(10,1)  NULL,
    inventory_value_usd  decimal(14,2)  NULL,
    reorder_flag         bit            NULL,
    load_ts              datetime2(6)   NOT NULL
);
GO

/* Informational keys: not enforced, but the optimizer and Power BI use them. */
IF OBJECT_ID('dw.pk_dim_date') IS NULL ALTER TABLE dw.dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY NONCLUSTERED (date_key)      NOT ENFORCED;
IF OBJECT_ID('dw.pk_dim_well') IS NULL ALTER TABLE dw.dim_well ADD CONSTRAINT pk_dim_well PRIMARY KEY NONCLUSTERED (well_key)      NOT ENFORCED;
IF OBJECT_ID('dw.pk_dim_equipment') IS NULL ALTER TABLE dw.dim_equipment ADD CONSTRAINT pk_dim_equipment PRIMARY KEY NONCLUSTERED (equipment_key) NOT ENFORCED;
IF OBJECT_ID('dw.pk_dim_part') IS NULL ALTER TABLE dw.dim_part ADD CONSTRAINT pk_dim_part PRIMARY KEY NONCLUSTERED (part_key)      NOT ENFORCED;
IF OBJECT_ID('dw.pk_dim_warehouse') IS NULL ALTER TABLE dw.dim_warehouse ADD CONSTRAINT pk_dim_warehouse PRIMARY KEY NONCLUSTERED (warehouse_key) NOT ENFORCED;
IF OBJECT_ID('dw.pk_dim_failure_mode') IS NULL ALTER TABLE dw.dim_failure_mode ADD CONSTRAINT pk_dim_failure_mode PRIMARY KEY NONCLUSTERED (failure_mode_key) NOT ENFORCED;
GO

/* ---------------- dim_date population (2015-01-01 .. 2030-12-31) ---------------- */
IF NOT EXISTS (SELECT 1 FROM dw.dim_date)
BEGIN
    WITH t AS (SELECT v FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) x(v)),
    n AS (   -- 0..9999 tally without relying on system catalog size
        SELECT a.v + b.v*10 + c.v*100 + d.v*1000 AS i FROM t a CROSS JOIN t b CROSS JOIN t c CROSS JOIN t d
        WHERE a.v + b.v*10 + c.v*100 + d.v*1000 <= DATEDIFF(day, '2015-01-01', '2030-12-31')
    ), d AS (SELECT DATEADD(day, i, CAST('2015-01-01' AS date)) AS dt FROM n)
    INSERT INTO dw.dim_date
    SELECT  CONVERT(int, FORMAT(dt, 'yyyyMMdd')), dt, YEAR(dt), DATEPART(quarter, dt), MONTH(dt),
            DATENAME(month, dt), FORMAT(dt, 'yyyy-MM'), DATEPART(week, dt), DATEPART(weekday, dt),
            DATENAME(weekday, dt), CASE WHEN DATENAME(weekday, dt) IN ('Saturday','Sunday') THEN 1 ELSE 0 END,
            DAY(EOMONTH(dt))
    FROM d;
END;
GO

/* Unknown members */
IF NOT EXISTS (SELECT 1 FROM dw.dim_well WHERE well_key = -1)
    INSERT INTO dw.dim_well (well_key, well_id, well_name, row_hash, valid_from, valid_to, is_current)
    VALUES (-1, 'UNKNOWN', 'Unknown well', 'n/a', '1900-01-01', NULL, 1);
IF NOT EXISTS (SELECT 1 FROM dw.dim_equipment WHERE equipment_key = -1)
    INSERT INTO dw.dim_equipment (equipment_key, equipment_id, equipment_type) VALUES (-1, 'UNKNOWN', 'Unknown');
IF NOT EXISTS (SELECT 1 FROM dw.dim_part WHERE part_key = -1)
    INSERT INTO dw.dim_part (part_key, part_number, description) VALUES (-1, 'UNKNOWN', 'Unknown / no part');
IF NOT EXISTS (SELECT 1 FROM dw.dim_failure_mode WHERE failure_mode_key = -1)
    INSERT INTO dw.dim_failure_mode (failure_mode_key, failure_mode, failure_category) VALUES (-1, 'N/A (planned work)', 'Planned');
GO
