"""Unit tests for fabric/notebooks/lib/transforms.py  —  run with:  pytest -q"""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "fabric" / "notebooks" / "lib"))
import transforms as T  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


PROD_COLS = ["well_id", "production_date", "oil_bbl", "gas_mcf", "water_bbl", "uptime_hours",
             "tubing_pressure_psi", "casing_pressure_psi", "source_system", "loaded_at"]


def test_production_dq_quarantines_bad_rows_and_dedupes_restatements(spark):
    rows = [
        ("W1", "2026-07-01", "100", "300", "200", "24", "180", "320", "S", "2026-07-02T00:00:00"),
        ("W1", "2026-07-01", "110", "300", "200", "24", "180", "320", "S", "2026-07-10T00:00:00"),  # restatement
        ("W2", "2026-07-01", "-5", "300", "200", "24", "180", "320", "S", "2026-07-02T00:00:00"),   # negative oil
        (None, "2026-07-01", "50", "100", "80", "24", "180", "320", "S", "2026-07-02T00:00:00"),     # null id
        ("W3", "2026-07-01", "80", "120", "90", "30", "180", "320", "S", "2026-07-02T00:00:00"),    # uptime > 24
    ]
    good, bad = T.clean_production(spark.createDataFrame(rows, PROD_COLS))
    g = good.collect()
    assert len(g) == 1 and g[0].well_id == "W1"
    assert g[0].oil_bbl == 110.0, "latest loaded_at version must win"
    assert g[0].boe == pytest.approx(110 + 300 / 6, 0.01)
    rules = {r for row in bad.collect() for r in row._dq_failed_rules}
    assert rules == {"oil_non_negative", "well_id_not_null", "uptime_in_range"}


def test_telemetry_rejects_physically_impossible_readings(spark):
    cols = ["equipment_id", "reading_ts", "motor_temp_f", "vibration_ips", "motor_amps",
            "intake_pressure_psi", "frequency_hz", "strokes_per_min"]
    rows = [("E1", "2026-07-01T00:00:00", "245", "0.1", "60", "850", "58", "0"),
            ("E1", "2026-07-01T01:00:00", "9999", "0.1", "60", "850", "58", "0")]
    good, bad = T.clean_telemetry(spark.createDataFrame(rows, cols))
    assert good.count() == 1 and bad.count() == 1
    assert bad.first()._dq_failed_rules == ["motor_temp_physical"]


def test_gold_production_deferred_volume_and_price_forward_fill(spark):
    prod = spark.createDataFrame(
        [("W1", d, oil, 24.0 if up else 12.0, 0.0, 50.0) for d, oil, up in
         [("2026-07-01", 100.0, True), ("2026-07-02", 100.0, True), ("2026-07-03", 40.0, False)]],
        "well_id string, production_date string, oil_bbl double, uptime_hours double, gas_mcf double, water_bbl double",
    ).selectExpr("well_id", "to_date(production_date) AS production_date", "oil_bbl", "uptime_hours", "gas_mcf", "water_bbl")
    wells = spark.createDataFrame([("W1", "Well 1", "Op", "Midland", "ESP")],
                                  "well_id string, well_name string, operator string, county string, lift_type string")
    prices = spark.createDataFrame([("2026-07-01", 70.0, 3.0)],
                                   "price_date string, wti_usd_bbl double, henry_hub_usd_mmbtu double") \
        .selectExpr("to_date(price_date) AS price_date", "wti_usd_bbl", "henry_hub_usd_mmbtu")
    out = {r.production_date.isoformat(): r for r in T.build_gold_production(prod, wells, prices).collect()}
    day3 = out["2026-07-03"]
    assert day3.deferred_oil_bbl == 60.0          # potential 100 - actual 40
    assert day3.wti_usd_bbl == 70.0               # forward-filled from 07-01
    assert day3.deferred_revenue_usd == 4200.0


def test_equipment_health_flags_degradation_before_failure(spark):
    import datetime as dt
    rows = []
    for d in range(20):
        day = dt.date(2026, 7, 1) + dt.timedelta(days=d)
        temp = 245 + (d % 3) * 0.5 + (40 if d >= 17 else 0)    # stable, then spike
        rows.append(("E1", dt.datetime.combine(day, dt.time()), temp, 0.12, 62.0, day))
    tel = spark.createDataFrame(rows, "equipment_id string, reading_ts timestamp, motor_temp_f double, "
                                      "vibration_ips double, motor_amps double, reading_date date")
    wos = spark.createDataFrame([("E1", "Corrective", "2026-07-20T00:00:00")],
                                "equipment_id string, wo_type string, opened_ts string")
    out = {r.reading_date.day: r for r in T.build_gold_equipment_health(tel, wos).collect()}
    assert out[18].health_status == "Critical"
    assert out[18].failed_within_7d is True
    assert out[5].failed_within_7d is False


def test_inventory_reorder_flag(spark):
    inv = spark.createDataFrame(
        [("WH1", "P1", "d", "ESP", 100.0, 1, 2, 30), ("WH1", "P2", "d", "ESP", 10.0, 50, 10, 5)],
        "warehouse string, part_number string, description string, category string, unit_cost double, "
        "qty_on_hand int, reorder_point int, lead_time_days int")
    wos = spark.createDataFrame([("P1", 1, "2026-10-01T00:00:00")], "part_number string, part_qty int, opened_ts string")
    out = {r.part_number: r for r in T.build_gold_inventory_risk(inv, wos, as_of="2026-10-29").collect()}
    assert out["P1"].reorder_flag is True and out["P2"].reorder_flag is False
