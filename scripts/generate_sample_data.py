"""
Generate realistic synthetic oil & gas operations data for the
Permian Basin Operations Analytics project.

Domains generated (mirroring real upstream / MRO source systems):
  - wells.csv                 : well master data (from a well-master / ERP system)
  - daily_production.csv      : daily oil / gas / water volumes per well (SCADA historian export)
  - pump_telemetry/*.csv      : hourly ESP / rod-pump sensor readings, one file per day (IoT landing zone)
  - equipment.csv             : pump & compressor asset register
  - work_orders.csv           : maintenance work orders (CMMS / ERP export)
  - parts_inventory.csv       : MRO spare-parts stock by warehouse (ERP inventory export)

Intentional data-quality issues are injected so the pipeline's DQ layer has
something real to catch: duplicate rows, negative volumes, null well IDs,
out-of-range sensor values, and late-arriving records.

Usage:
    python scripts/generate_sample_data.py --out data/raw --days 120 --seed 42
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

COUNTIES = {
    # county: (lat, lon, reservoir play)
    "Midland": (31.99, -102.08, "Spraberry"),
    "Martin": (32.31, -101.95, "Spraberry"),
    "Howard": (32.30, -101.44, "Wolfcamp"),
    "Reeves": (31.42, -103.49, "Wolfcamp"),
    "Loving": (31.84, -103.58, "Bone Spring"),
    "Upton": (31.37, -102.04, "Wolfcamp"),
}
OPERATORS = ["Lone Star Energy", "Mesa Ridge Resources", "Caprock Petroleum"]
LIFT_TYPES = ["ESP", "Rod Pump", "Gas Lift"]
WAREHOUSES = ["Midland Service Center", "Odessa Service Center", "Pecos Service Center"]

PARTS = [
    # part_number, description, category, unit_cost, lead_time_days
    ("ESP-MTR-450", "ESP motor 450 HP", "ESP", 48500.0, 42),
    ("ESP-PMP-538", "ESP pump stage assembly 538 series", "ESP", 21500.0, 35),
    ("ESP-SEAL-400", "ESP seal section / protector", "ESP", 9800.0, 28),
    ("ESP-CBL-4AWG", "ESP power cable 4 AWG (per 100 ft)", "ESP", 2650.0, 21),
    ("ROD-PMP-2.25", "Rod pump insert 2-1/4 in", "Rod Lift", 3400.0, 14),
    ("ROD-SKR-0.875", "Sucker rod 7/8 in grade D", "Rod Lift", 145.0, 10),
    ("PU-GBX-640", "Pumping unit gearbox 640D", "Rod Lift", 38500.0, 60),
    ("VLV-CHK-2IN", "Check valve 2 in API 6D", "Valves", 820.0, 7),
    ("VLV-BALL-3IN", "Ball valve 3 in 1500#", "Valves", 1950.0, 14),
    ("SEN-PT-5K", "Pressure transmitter 0-5000 psi", "Instrumentation", 1150.0, 10),
    ("SEN-VIB-3AX", "Tri-axial vibration sensor", "Instrumentation", 890.0, 12),
    ("CMP-VLV-KIT", "Compressor valve rebuild kit", "Compression", 2750.0, 18),
    ("GSK-RTJ-R45", "Ring joint gasket R45", "Consumables", 38.0, 3),
    ("FLT-OIL-HD", "Heavy-duty oil filter", "Consumables", 62.0, 3),
]

FAILURE_MODES = {
    "ESP": ["Motor burnout", "Cable insulation failure", "Pump stage wear", "Gas lock", "Seal leak"],
    "Rod Pump": ["Rod part", "Pump stuck", "Gearbox failure", "Tubing leak", "Valve wear"],
    "Gas Lift": ["Gas lift valve failure", "Compressor trip", "Injection line freeze"],
}


def make_wells(rng: np.random.Generator, n_wells: int) -> pd.DataFrame:
    rows = []
    for i in range(n_wells):
        county = rng.choice(list(COUNTIES))
        lat, lon, play = COUNTIES[county]
        spud = date(2016, 1, 1) + timedelta(days=int(rng.integers(0, 365 * 8)))
        lift = rng.choice(LIFT_TYPES, p=[0.45, 0.40, 0.15])
        rows.append(
            {
                "well_id": f"42-{rng.integers(100, 500):03d}-{30000 + i:05d}",  # API-number style
                "well_name": f"{county.upper()} {rng.choice(['STATE', 'UNIV', 'RANCH', 'UNIT'])} {i + 1:03d}H",
                "operator": rng.choice(OPERATORS),
                "county": county,
                "state": "TX",
                "basin": "Permian",
                "reservoir": play,
                "lift_type": lift,
                "lateral_length_ft": int(rng.normal(10000, 1800)),
                "spud_date": spud.isoformat(),
                "first_prod_date": (spud + timedelta(days=int(rng.integers(60, 150)))).isoformat(),
                "latitude": round(lat + rng.normal(0, 0.12), 5),
                "longitude": round(lon + rng.normal(0, 0.12), 5),
                "status": rng.choice(["Producing", "Producing", "Producing", "Shut-in"], p=[0.3, 0.3, 0.3, 0.1]),
            }
        )
    return pd.DataFrame(rows)


def make_equipment(rng: np.random.Generator, wells: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, w in wells.iterrows():
        eq_type = {"ESP": "ESP", "Rod Pump": "Rod Pump", "Gas Lift": "Gas Lift Compressor"}[w.lift_type]
        mfr = {
            "ESP": ["Baker Hughes", "SLB", "ChampionX"],
            "Rod Pump": ["Lufkin", "Weatherford", "ChampionX"],
            "Gas Lift Compressor": ["Ariel", "Caterpillar", "Exterran"],
        }[eq_type]
        rows.append(
            {
                "equipment_id": f"EQ-{w.well_id[-5:]}",
                "well_id": w.well_id,
                "equipment_type": eq_type,
                "manufacturer": rng.choice(mfr),
                "model": f"{eq_type[:3].upper()}-{rng.integers(100, 999)}",
                "install_date": (date(2023, 1, 1) + timedelta(days=int(rng.integers(0, 600)))).isoformat(),
                "rated_hp": int(rng.choice([150, 250, 350, 450])),
            }
        )
    return pd.DataFrame(rows)


def decline_curve(qi: float, di: float, b: float, t_days: np.ndarray) -> np.ndarray:
    """Arps hyperbolic decline: q(t) = qi / (1 + b*Di*t)^(1/b)."""
    return qi / np.power(1 + b * di * t_days / 365.0, 1.0 / b)


def make_production(rng, wells: pd.DataFrame, start: date, days: int) -> tuple[pd.DataFrame, dict]:
    rows = []
    downtime_events: dict[str, list[tuple[date, int]]] = {}
    dates = [start + timedelta(days=d) for d in range(days)]
    for _, w in wells.iterrows():
        first_prod = date.fromisoformat(w.first_prod_date)
        age0 = max((start - first_prod).days, 30)
        qi = rng.uniform(600, 1400)  # bbl/d initial oil rate
        di = rng.uniform(0.6, 1.1)
        b = rng.uniform(0.8, 1.3)
        gor = rng.uniform(1.5, 4.0)  # mcf/bbl
        wcut = rng.uniform(0.45, 0.75)
        t = np.arange(age0, age0 + days)
        base_oil = decline_curve(qi, di, b, t)

        # random failure / downtime events (feeds work orders later)
        events = []
        n_events = rng.poisson(1.2 if w.lift_type == "ESP" else 0.9)
        for _ in range(n_events):
            d0 = int(rng.integers(0, days - 3))
            dur = int(rng.integers(1, 6))
            events.append((dates[d0], dur))
        downtime_events[w.well_id] = events

        for i, d in enumerate(dates):
            uptime = 24.0
            for ev_start, dur in events:
                if ev_start <= d < ev_start + timedelta(days=dur):
                    uptime = float(rng.choice([0.0, rng.uniform(0, 12)]))
            if w.status == "Shut-in":
                uptime = 0.0
            frac = uptime / 24.0
            oil = base_oil[i] * frac * rng.normal(1.0, 0.04)
            gas = oil * gor * rng.normal(1.0, 0.05)
            water = oil * wcut / (1 - wcut) * rng.normal(1.0, 0.05)
            rows.append(
                {
                    "well_id": w.well_id,
                    "production_date": d.isoformat(),
                    "oil_bbl": round(max(oil, 0), 2),
                    "gas_mcf": round(max(gas, 0), 2),
                    "water_bbl": round(max(water, 0), 2),
                    "uptime_hours": round(uptime, 2),
                    "tubing_pressure_psi": round(rng.normal(180, 35), 1),
                    "casing_pressure_psi": round(rng.normal(320, 50), 1),
                    "source_system": "SCADA_HISTORIAN",
                    "loaded_at": datetime.combine(d + timedelta(days=1), datetime.min.time()).isoformat(),
                }
            )
    prod = pd.DataFrame(rows)

    # ---- inject data-quality issues ----
    n = len(prod)
    dup = prod.sample(n=int(n * 0.01), random_state=1)                    # 1% duplicates
    neg_idx = prod.sample(n=int(n * 0.003), random_state=2).index         # negative volumes
    prod.loc[neg_idx, "oil_bbl"] = -prod.loc[neg_idx, "oil_bbl"]
    null_idx = prod.sample(n=int(n * 0.002), random_state=3).index        # null well ids
    prod.loc[null_idx, "well_id"] = None
    late = prod.sample(n=int(n * 0.005), random_state=4).index            # late-arriving restatements
    prod.loc[late, "loaded_at"] = (
        pd.to_datetime(prod.loc[late, "production_date"]) + pd.Timedelta(days=9)
    ).dt.strftime("%Y-%m-%dT%H:%M:%S")
    prod = pd.concat([prod, dup], ignore_index=True)
    return prod, downtime_events


def make_telemetry(rng, equipment: pd.DataFrame, wells: pd.DataFrame, events: dict,
                   start: date, days: int, out_dir: Path) -> int:
    """Hourly sensor readings for ESP and rod-pump wells, written one file per day."""
    tel_dir = out_dir / "pump_telemetry"
    tel_dir.mkdir(parents=True, exist_ok=True)
    eq = equipment.merge(wells[["well_id", "lift_type"]], on="well_id")
    eq = eq[eq.lift_type.isin(["ESP", "Rod Pump"])]
    total = 0
    for d in range(days):
        day = start + timedelta(days=d)
        rows = []
        for _, e in eq.iterrows():
            # pre-failure degradation: rising vibration & temp in the 5 days before an event
            degrade = 0.0
            for ev_start, _dur in events.get(e.well_id, []):
                lead = (ev_start - day).days
                if 0 <= lead <= 5:
                    degrade = max(degrade, (6 - lead) / 6.0)
            for h in range(24):
                ts = datetime.combine(day, datetime.min.time()) + timedelta(hours=h)
                is_esp = e.lift_type == "ESP"
                rows.append(
                    {
                        "equipment_id": e.equipment_id,
                        "reading_ts": ts.isoformat(),
                        "motor_temp_f": round(rng.normal(245 if is_esp else 160, 6) + 40 * degrade, 1),
                        "vibration_ips": round(abs(rng.normal(0.12, 0.03)) + 0.35 * degrade, 3),
                        "motor_amps": round(rng.normal(62 if is_esp else 38, 3) + 9 * degrade, 1),
                        "intake_pressure_psi": round(rng.normal(850 if is_esp else 0, 40), 1),
                        "frequency_hz": round(rng.normal(58 if is_esp else 0, 1.0), 2),
                        "strokes_per_min": round(0 if is_esp else rng.normal(7.5, 0.4), 2),
                    }
                )
        df = pd.DataFrame(rows)
        # sensor glitches: impossible values the DQ layer must quarantine
        bad = df.sample(frac=0.002, random_state=d).index
        df.loc[bad, "motor_temp_f"] = 9999.0
        df.to_csv(tel_dir / f"telemetry_{day:%Y%m%d}.csv", index=False)
        total += len(df)
    return total


def make_work_orders(rng, wells, equipment, events) -> pd.DataFrame:
    rows = []
    wo = 100000
    lift_by_well = wells.set_index("well_id").lift_type.to_dict()
    eq_by_well = equipment.set_index("well_id").equipment_id.to_dict()
    part_cats = {"ESP": "ESP", "Rod Pump": "Rod Lift", "Gas Lift": "Compression"}
    parts_df = pd.DataFrame(PARTS, columns=["part_number", "description", "category", "unit_cost", "lead"])
    for well_id, evs in events.items():
        lift = lift_by_well[well_id]
        for ev_start, dur in evs:
            wo += 1
            cand = parts_df[parts_df.category.isin([part_cats[lift], "Valves", "Consumables"])]
            part = cand.sample(1, random_state=int(rng.integers(0, 1e6))).iloc[0]
            qty = int(rng.integers(1, 4)) if part.unit_cost < 5000 else 1
            labor_hrs = round(rng.uniform(6, 40), 1)
            rows.append(
                {
                    "work_order_id": f"WO-{wo}",
                    "well_id": well_id,
                    "equipment_id": eq_by_well[well_id],
                    "wo_type": "Corrective",
                    "priority": rng.choice(["P1", "P2"], p=[0.6, 0.4]),
                    "failure_mode": rng.choice(FAILURE_MODES[lift]),
                    "opened_ts": datetime.combine(ev_start, datetime.min.time()).isoformat(),
                    "closed_ts": (datetime.combine(ev_start, datetime.min.time())
                                  + timedelta(days=dur, hours=int(rng.integers(0, 12)))).isoformat(),
                    "part_number": part.part_number,
                    "part_qty": qty,
                    "labor_hours": labor_hrs,
                    "labor_cost": round(labor_hrs * 115.0, 2),
                    "parts_cost": round(qty * part.unit_cost, 2),
                    "status": "Closed",
                }
            )
    # preventive maintenance orders
    for well_id in wells.well_id.sample(frac=0.4, random_state=7):
        wo += 1
        opened = datetime(2026, 7, 1) + timedelta(days=int(rng.integers(0, 110)))
        rows.append(
            {
                "work_order_id": f"WO-{wo}",
                "well_id": well_id,
                "equipment_id": eq_by_well[well_id],
                "wo_type": "Preventive",
                "priority": "P3",
                "failure_mode": None,
                "opened_ts": opened.isoformat(),
                "closed_ts": (opened + timedelta(hours=int(rng.integers(3, 10)))).isoformat(),
                "part_number": "FLT-OIL-HD",
                "part_qty": 2,
                "labor_hours": 4.0,
                "labor_cost": 460.0,
                "parts_cost": 124.0,
                "status": "Closed",
            }
        )
    return pd.DataFrame(rows)


def make_inventory(rng) -> pd.DataFrame:
    rows = []
    for wh in WAREHOUSES:
        for pn, desc, cat, cost, lead in PARTS:
            reorder = int(max(1, rng.integers(1, 6) if cost > 5000 else rng.integers(10, 60)))
            rows.append(
                {
                    "warehouse": wh,
                    "part_number": pn,
                    "description": desc,
                    "category": cat,
                    "unit_cost": cost,
                    "qty_on_hand": int(max(0, rng.normal(reorder * 1.3, reorder * 0.8))),
                    "reorder_point": reorder,
                    "lead_time_days": lead,
                    "snapshot_date": "2026-10-29",
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--days", type=int, default=120)
    ap.add_argument("--wells", type=int, default=60)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start", default="2026-07-01")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    start = date.fromisoformat(args.start)

    wells = make_wells(rng, args.wells)
    equipment = make_equipment(rng, wells)
    prod, events = make_production(rng, wells, start, args.days)
    n_tel = make_telemetry(rng, equipment, wells, events, start, args.days, out)
    wos = make_work_orders(rng, wells, equipment, events)
    inv = make_inventory(rng)

    wells.to_csv(out / "wells.csv", index=False)
    equipment.to_csv(out / "equipment.csv", index=False)
    prod.to_csv(out / "daily_production.csv", index=False)
    wos.to_csv(out / "work_orders.csv", index=False)
    inv.to_csv(out / "parts_inventory.csv", index=False)

    print(f"wells={len(wells)} equipment={len(equipment)} production_rows={len(prod)} "
          f"telemetry_rows={n_tel} work_orders={len(wos)} inventory_rows={len(inv)}")


if __name__ == "__main__":
    main()
