"""
Load the generated ERP/CMMS CSVs into the simulated source SQL Server so ADF
has a real relational source to extract incrementally from.

Prereqs:  pip install pandas pyodbc   (and ODBC Driver 18 for SQL Server)
Usage:
    python scripts/load_erp_source.py --server myserver.database.windows.net \
        --database ERP_Ops --user sqladmin --password '***' --data data/raw
Run sql/source_erp/01_create_erp_source_tables.sql first.
"""
import argparse
from pathlib import Path

import pandas as pd
import pyodbc

TABLES = ["wells", "equipment", "parts_inventory", "work_orders"]  # FK-safe order


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--database", default="ERP_Ops")
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--data", default="data/raw")
    a = ap.parse_args()

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={a.server};DATABASE={a.database};UID={a.user};PWD={a.password};Encrypt=yes;TrustServerCertificate=yes"
    )
    cur = conn.cursor()
    cur.fast_executemany = True
    for t in reversed(TABLES):          # clear children before parents
        cur.execute(f"DELETE FROM ops.{t}")
    conn.commit()
    for t in TABLES:
        df = pd.read_csv(Path(a.data) / f"{t}.csv")
        df = df.astype(object).where(pd.notna(df), None)
        cols = ",".join(df.columns)
        marks = ",".join("?" * len(df.columns))
        cur.executemany(f"INSERT INTO ops.{t} ({cols}) VALUES ({marks})", df.values.tolist())
        conn.commit()
        print(f"ops.{t}: {len(df)} rows")
    conn.close()


if __name__ == "__main__":
    main()
