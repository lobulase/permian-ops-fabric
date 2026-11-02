/* =====================================================================
   Simulated ERP / CMMS source database (SQL Server or Azure SQL)
   ---------------------------------------------------------------------
   Stands in for the operator's on-prem ERP (think Infor M3 / Epicor P21
   style master + transactional tables). ADF extracts from here
   incrementally using the modified_at watermark column.
   Load with:  python scripts/load_erp_source.py --server <host> --database ERP_Ops
   ===================================================================== */
IF SCHEMA_ID('ops') IS NULL EXEC('CREATE SCHEMA ops');
GO

DROP TABLE IF EXISTS ops.work_orders, ops.parts_inventory, ops.equipment, ops.wells;
GO

CREATE TABLE ops.wells (
    well_id             varchar(20)   NOT NULL PRIMARY KEY,   -- API-number style id
    well_name           varchar(60)   NOT NULL,
    operator            varchar(60)   NOT NULL,
    county              varchar(40)   NOT NULL,
    state               char(2)       NOT NULL,
    basin               varchar(30)   NOT NULL,
    reservoir           varchar(30)   NULL,
    lift_type           varchar(20)   NOT NULL,
    lateral_length_ft   int           NULL,
    spud_date           date          NULL,
    first_prod_date     date          NULL,
    latitude            decimal(9,5)  NULL,
    longitude           decimal(9,5)  NULL,
    status              varchar(20)   NOT NULL,
    modified_at         datetime2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE ops.equipment (
    equipment_id        varchar(20)   NOT NULL PRIMARY KEY,
    well_id             varchar(20)   NOT NULL REFERENCES ops.wells(well_id),
    equipment_type      varchar(30)   NOT NULL,
    manufacturer        varchar(40)   NULL,
    model               varchar(30)   NULL,
    install_date        date          NULL,
    rated_hp            int           NULL,
    modified_at         datetime2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE ops.parts_inventory (
    warehouse           varchar(40)   NOT NULL,
    part_number         varchar(20)   NOT NULL,
    description         varchar(80)   NOT NULL,
    category            varchar(30)   NOT NULL,
    unit_cost           decimal(12,2) NOT NULL,
    qty_on_hand         int           NOT NULL,
    reorder_point       int           NOT NULL,
    lead_time_days      int           NOT NULL,
    snapshot_date       date          NOT NULL,
    modified_at         datetime2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_parts_inventory PRIMARY KEY (warehouse, part_number)
);

CREATE TABLE ops.work_orders (
    work_order_id       varchar(20)   NOT NULL PRIMARY KEY,
    well_id             varchar(20)   NOT NULL REFERENCES ops.wells(well_id),
    equipment_id        varchar(20)   NULL REFERENCES ops.equipment(equipment_id),
    wo_type             varchar(20)   NOT NULL,
    priority            varchar(5)    NOT NULL,
    failure_mode        varchar(60)   NULL,
    opened_ts           datetime2(0)  NOT NULL,
    closed_ts           datetime2(0)  NULL,
    part_number         varchar(20)   NULL,
    part_qty            int           NULL,
    labor_hours         decimal(8,1)  NULL,
    labor_cost          decimal(12,2) NULL,
    parts_cost          decimal(12,2) NULL,
    status              varchar(15)   NOT NULL,
    modified_at         datetime2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX ix_work_orders_modified_at ON ops.work_orders (modified_at);
GO
