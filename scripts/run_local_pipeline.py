"""
Run the bronze -> silver -> gold logic locally with plain PySpark (no Azure needed).

Useful for demos and for checking transform changes before deploying to Fabric.
Writes Parquet to data/local_lakehouse/{silver,gold}/ and prints a run summary.

    pip install pyspark==3.5.3
    python scripts/run_local_pipeline.py --raw data/raw --out data/local_lakehouse
"""
import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "fabric" / "notebooks" / "lib"))
import transforms as T  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--out", default="data/local_lakehouse")
    ap.add_argument("--as-of", default="2026-10-29")
    a = ap.parse_args()
    raw, out = Path(a.raw), Path(a.out)

    spark = (SparkSession.builder.master("local[*]").appName("permian-local")
             .config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    csv = lambda p: spark.read.option("header", True).option("inferSchema", True).csv(str(p))  # noqa: E731

    prod, prod_bad = T.clean_production(spark.read.option("header", True).csv(str(raw / "daily_production.csv")))
    tel, tel_bad = T.clean_telemetry(spark.read.option("header", True).csv(str(raw / "pump_telemetry")))
    wells, wos, inv = csv(raw / "wells.csv"), csv(raw / "work_orders.csv"), csv(raw / "parts_inventory.csv")

    # stand-in for the EIA feed so revenue columns populate locally
    prices = (prod.select("production_date").distinct()
              .withColumnRenamed("production_date", "price_date")
              .withColumn("wti_usd_bbl", F.round(F.lit(68.0) + F.sin(F.dayofyear("price_date") / 9) * 4, 2))
              .withColumn("henry_hub_usd_mmbtu", F.round(F.lit(2.9) + F.cos(F.dayofyear("price_date") / 11) * 0.4, 3)))

    gold = {
        "gold_production_daily": T.build_gold_production(prod, wells, prices),
        "gold_equipment_health_daily": T.build_gold_equipment_health(tel, wos),
        "gold_inventory_risk": T.build_gold_inventory_risk(inv, wos, as_of=a.as_of),
    }
    for name, df in {"silver_daily_production": prod, "silver_pump_telemetry": tel,
                     "silver_dq_quarantine": prod_bad.unionByName(tel_bad, allowMissingColumns=True)
                                                     .select("_dq_entity", "_dq_failed_rules")}.items():
        df.write.mode("overwrite").parquet(str(out / "silver" / name))
    for name, df in gold.items():
        df.write.mode("overwrite").parquet(str(out / "gold" / name))

    g = {k: spark.read.parquet(str(out / "gold" / k)) for k in gold}
    q = spark.read.parquet(str(out / "silver" / "silver_dq_quarantine"))
    print("\n=== DQ quarantine by rule ===")
    q.select("_dq_entity", F.explode("_dq_failed_rules").alias("rule")).groupBy("_dq_entity", "rule").count() \
     .orderBy("_dq_entity", "rule").show(truncate=False)
    print("=== Gold row counts ===")
    for k, df in g.items():
        print(f"{k:32s} {df.count():>8,}")
    print("\n=== Basin totals ===")
    g["gold_production_daily"].agg(
        F.round(F.sum("oil_bbl")).alias("oil_bbl"), F.round(F.sum("boe")).alias("boe"),
        F.round(F.sum("deferred_oil_bbl")).alias("deferred_oil_bbl"),
        F.round(F.sum("est_revenue_usd")).alias("est_revenue_usd"),
        F.round(F.sum("deferred_revenue_usd")).alias("deferred_revenue_usd")).show()
    print("=== Health model: how often does 'Watch/Critical' precede a failure within 7 days? ===")
    g["gold_equipment_health_daily"].groupBy("health_status").agg(
        F.count("*").alias("pump_days"),
        F.round(F.avg(F.col("failed_within_7d").cast("int")) * 100, 1).alias("pct_failed_within_7d")
    ).orderBy("health_status").show()
    print("=== Parts needing reorder ===")
    g["gold_inventory_risk"].filter("reorder_flag").select(
        "warehouse", "part_number", "qty_on_hand", "reorder_point", "days_of_cover", "lead_time_days"
    ).orderBy("warehouse", "part_number").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
