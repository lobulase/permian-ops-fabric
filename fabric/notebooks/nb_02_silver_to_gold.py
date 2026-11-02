# Fabric notebook source
# Notebook: nb_02_silver_to_gold
# Default lakehouse: lh_permian_ops

# %% [markdown]
# # Silver → Gold
# Builds business-ready Delta tables that the Warehouse star schema and the Power BI
# Direct Lake semantic model read from:
# * `gold_production_daily` – production, BOE, deferred (lost) production and revenue
# * `gold_equipment_health_daily` – pump health features, anomaly score, failure label
# * `gold_inventory_risk` – MRO parts days-of-cover and reorder flags

# %% parameters
run_date = "2026-10-29"
reprocess_days = 15      # trailing days rebuilt each run to absorb late-arriving restatements

# %%
import datetime, sys
sys.path.insert(0, "./builtin")
from pyspark.sql import functions as F
import transforms as T

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")

LOOKBACK_CONTEXT = 30    # extra days read so 30-day rolling windows are correct at the window edge
run_dt = spark.sql(f"SELECT DATE'{run_date}' AS d").first()["d"]
keep_from = run_dt - datetime.timedelta(days=reprocess_days)            # rows we (re)write
read_from = keep_from - datetime.timedelta(days=LOOKBACK_CONTEXT)      # rows we read

def window_bounds(table):
    """First run -> build full history; afterwards -> rebuild only the recent window."""
    return (None, None) if not spark.catalog.tableExists(table) else (read_from, keep_from)

def write_window(df, table, date_col, keep):
    if keep is None:
        df.write.format("delta").partitionBy(date_col).saveAsTable(table)
        return df.count()
    df = df.filter(F.col(date_col) >= F.lit(keep))
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"{date_col} >= '{keep}'")      # idempotent partial overwrite
       .saveAsTable(table))
    return df.count()

# %%
wells = spark.table("silver_wells")
rd, keep = window_bounds("gold_production_daily")
prod = spark.table("silver_daily_production")
prod = prod if rd is None else prod.filter(F.col("production_date") >= F.lit(rd))
prices = spark.table("silver_spot_prices") if spark.catalog.tableExists("silver_spot_prices") else None
n_prod = write_window(T.build_gold_production(prod, wells, prices), "gold_production_daily", "production_date", keep)

# %%
rd, keep = window_bounds("gold_equipment_health_daily")
tel = spark.table("silver_pump_telemetry")
tel = tel if rd is None else tel.filter(F.col("reading_date") >= F.lit(rd))
wos = spark.table("silver_work_orders")
n_health = write_window(T.build_gold_equipment_health(tel, wos), "gold_equipment_health_daily", "reading_date", keep)

# %%
inv = spark.table("silver_parts_inventory")
risk = T.build_gold_inventory_risk(inv, wos, as_of=run_date)
risk.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_inventory_risk")

# %%
spark.sql("OPTIMIZE gold_production_daily")
spark.sql("OPTIMIZE gold_equipment_health_daily")

import json
mssparkutils.notebook.exit(json.dumps({"gold_production_rows": n_prod, "gold_health_rows": n_health}))
