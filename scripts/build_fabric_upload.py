"""
Prepare everything you upload to Fabric by hand (before ADF is wired up):

  1. fabric_upload/bronze/...   sample data laid out exactly as ADF would land it,
                                so the notebooks run unchanged. Upload the 'bronze'
                                folder into the lakehouse 'Files' section.
  2. fabric_upload/notebooks/*.ipynb   importable Fabric notebooks (Workspace > Import > Notebook).

Usage:
    python scripts/generate_sample_data.py --out data/raw
    python scripts/build_fabric_upload.py --raw data/raw --out fabric_upload --ingest-date 2026-10-29
"""
import argparse
import json
import re
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def build_bronze(raw: Path, out: Path, ingest: str) -> None:
    b = out / "bronze"
    if b.exists():
        shutil.rmtree(b)
    part = f"ingest_date={ingest}"

    d = b / "scada" / "daily_production" / part
    d.mkdir(parents=True)
    shutil.copy(raw / "daily_production.csv", d / "daily_production.csv")

    d = b / "iot" / "pump_telemetry" / part
    d.mkdir(parents=True)
    for f in sorted((raw / "pump_telemetry").glob("*.csv")):
        shutil.copy(f, d / f.name)

    # ERP entities arrive from ADF as Parquet with modified_at + _extracted_at columns
    for entity in ["wells", "equipment", "work_orders", "parts_inventory"]:
        df = pd.read_csv(raw / f"{entity}.csv")
        df["modified_at"] = pd.Timestamp(f"{ingest} 04:00:00")
        df["_extracted_at"] = pd.Timestamp(f"{ingest} 05:30:00")
        d = b / "erp" / entity / part
        d.mkdir(parents=True)
        df.to_parquet(d / f"{entity}.parquet", index=False)


def py_to_ipynb(src: Path, dst: Path) -> None:
    """Convert '# %%' percent-format scripts to .ipynb; the '# %% parameters' cell gets the parameters tag."""
    text = src.read_text()
    chunks = re.split(r"^# %%(.*)$", text, flags=re.M)
    cells = []
    for i in range(1, len(chunks), 2):
        marker, body = chunks[i].strip(), chunks[i + 1].strip("\n")
        if not body.strip():
            continue
        if marker == "[markdown]":
            md = "\n".join(re.sub(r"^# ?", "", ln) for ln in body.splitlines())
            cells.append({"cell_type": "markdown", "metadata": {}, "source": md.splitlines(keepends=True)})
        else:
            meta = {"tags": ["parameters"]} if marker == "parameters" else {}
            cells.append({"cell_type": "code", "metadata": meta, "execution_count": None, "outputs": [],
                          "source": body.splitlines(keepends=True)})
    nb = {"nbformat": 4, "nbformat_minor": 5, "cells": cells,
          "metadata": {"kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark", "language": "Python"},
                       "language_info": {"name": "python"}}}
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(json.dumps(nb, indent=1))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--out", default="fabric_upload")
    ap.add_argument("--ingest-date", default="2026-10-29")
    a = ap.parse_args()
    out = Path(a.out)
    build_bronze(Path(a.raw), out, a.ingest_date)
    for nb in ["nb_01_bronze_to_silver", "nb_02_silver_to_gold"]:
        py_to_ipynb(ROOT / "fabric" / "notebooks" / f"{nb}.py", out / "notebooks" / f"{nb}.ipynb")
    shutil.copy(ROOT / "fabric" / "notebooks" / "lib" / "transforms.py", out / "notebooks" / "transforms.py")
    print(f"Built {out}/bronze and {out}/notebooks")


if __name__ == "__main__":
    main()
