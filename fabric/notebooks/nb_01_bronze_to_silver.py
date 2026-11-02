# Fabric notebook source
# Notebook: nb_01_bronze_to_silver
# Default lakehouse: lh_permian_ops
# Upload fabric/notebooks/lib/transforms.py to this notebook's Resources (builtin/) folder.

# %% [markdown]
# # Bronze → Silver
# Reads the raw files ADF landed in `Files/bronze/...`, applies typing, data-quality rules and
# de-duplication, and upserts clean Delta tables into the lakehouse `silver_*` tables.
# Rows that fail an error-level rule go to `silver_dq_quarantine` instead of being dropped silently.

# %% parameters
run_date = "2026-10-29"        # overridden by the Fabric pipeline (yyyy-MM-dd)
full_reload = False            # True = reprocess every ingest_date partition

# %%
import sys
sys.path.insert(0, "./builtin")            # notebook resources folder

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
import transforms as T

# Full OneLake path, so reads don't depend on the notebook's default-lakehouse setting
WORKSPACE_ID = mssparkutils.runtime.context["currentWorkspaceId"]
BRONZE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/lh_permian_ops.Lakehouse/Files/bronze"
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")        # V-Order for fast Direct Lake reads
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

def bronze_path(source: str, entity: str) -> str:
    part = "*" if full_reload else f"ingest_date={run_date}"
    return f"{BRONZE}/{source}/{entity}/{part}"

def upsert(df: DataFrame, table: str, keys: list[str]) -> int:
    """Idempotent MERGE into a silver Delta table (creates it on first run)."""
    df = df.withColumn("_silver_updated_at", F.current_timestamp())
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return df.count()
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forName(spark, table).alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    return df.count()

def quarantine(bad: DataFrame) -> int:
    n = bad.count()
    if n:
        (bad.select("_dq_entity", "_dq_failed_rules", "_dq_checked_at",
                    F.to_json(F.struct(*[c for c in bad.columns if not c.startswith("_dq")])).alias("record_json"))
            .withColumn("run_date", F.lit(run_date).cast("date"))
            .write.format("delta").mode("append").saveAsTable("silver_dq_quarantine"))
    return n

metrics = []

# %% [markdown]
# ## Daily production (SCADA historian)

# %%
raw = spark.read.option("header", True).csv(bronze_path("scada", "daily_production"))
good, bad = T.clean_production(raw)
metrics.append(("daily_production", raw.count(), upsert(good, "silver_daily_production", ["well_id", "production_date"]), quarantine(bad)))

# %% [markdown]
# ## Pump telemetry (IoT) — partitioned by reading_date for pruning

# %%
raw = spark.read.option("header", True).csv(bronze_path("iot", "pump_telemetry"))
good, bad = T.clean_telemetry(raw)
if not spark.catalog.tableExists("silver_pump_telemetry"):
    good.withColumn("_silver_updated_at", F.current_timestamp()) \
        .write.format("delta").partitionBy("reading_date").saveAsTable("silver_pump_telemetry")
    n = good.count()
else:
    n = upsert(good, "silver_pump_telemetry", ["equipment_id", "reading_ts"])
metrics.append(("pump_telemetry", raw.count(), n, quarantine(bad)))

# %% [markdown]
# ## ERP / CMMS master + transactional data (Parquet from ADF incremental copy)

# %%
erp_entities = {
    "wells": ["well_id"],
    "equipment": ["equipment_id"],
    "work_orders": ["work_order_id"],
    "parts_inventory": ["warehouse", "part_number"],
}
for entity, keys in erp_entities.items():
    try:
        raw = spark.read.parquet(bronze_path("erp", entity))
    except Exception as e:                        # no new rows today for this entity
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            metrics.append((entity, 0, 0, 0)); continue
        raise
    clean = T.dedupe_latest(raw, keys, "modified_at").drop("_extracted_at")
    good, bad = T.apply_dq(clean, [T.DQRule(f"{k}_not_null", (lambda k=k: F.col(k).isNotNull())) for k in keys], entity)
    metrics.append((entity, raw.count(), upsert(good, f"silver_{entity}", keys), quarantine(bad)))

# %% [markdown]
# ## EIA spot prices → silver_spot_prices (one row per day, WTI + Henry Hub)

# %%
try:
    eia = spark.read.option("multiLine", True).json(f"{BRONZE}/eia/spot_prices/ingest_date={run_date}")
    rows = eia.select(F.explode("response.data").alias("d")).select(
        F.to_date("d.period").alias("price_date"), F.col("d.series").alias("series"),
        F.col("d.value").cast("double").alias("value"))
    prices = (rows.groupBy("price_date")
                  .pivot("series", ["RWTC", "RNGWHHD"]).agg(F.first("value"))
                  .withColumnRenamed("RWTC", "wti_usd_bbl").withColumnRenamed("RNGWHHD", "henry_hub_usd_mmbtu"))
    metrics.append(("spot_prices", rows.count(), upsert(prices, "silver_spot_prices", ["price_date"]), 0))
except Exception as e:
    print(f"EIA prices not available for {run_date} - gold revenue will forward-fill last known price. ({e})")

# %% [markdown]
# ## Run summary → returned to the pipeline (exitValue) and logged

# %%
import json
summary = spark.createDataFrame(metrics, "entity string, rows_read long, rows_upserted long, rows_quarantined long") \
               .withColumn("run_date", F.lit(run_date).cast("date")) \
               .withColumn("layer", F.lit("silver"))
summary.write.format("delta").mode("append").saveAsTable("etl_layer_load_log")
display(summary)

total_bad = sum(m[3] for m in metrics)
total_read = max(sum(m[1] for m in metrics), 1)
if total_bad / total_read > 0.05:          # circuit breaker: >5% quarantined = upstream problem
    raise Exception(f"DQ circuit breaker tripped: {total_bad}/{total_read} rows quarantined")

mssparkutils.notebook.exit(json.dumps({"rows_read": total_read, "rows_quarantined": total_bad}))
