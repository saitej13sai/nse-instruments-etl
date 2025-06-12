# 📊 NSE Instruments ETL Pipeline

This project is a simple and efficient Python ETL pipeline to fetch, clean, and compare stock instrument data from two sources—**Upstox** and **Dhan**—focused only on NSE Equity instruments. The data is stored in MongoDB and SQLite, and the final output highlights common and unique stocks between the two platforms.

---

## ✅ What This Project Does

- 📥 **Extracts** NSE equity data from:
  - Upstox (CSV.gz format)
  - Dhan (CSV format)
- 🔄 **Cleans and filters** the data to keep only NSE Equity instruments
- 💾 **Loads** the processed data into:
  - **MongoDB** (Upstox data into `market_data.upstox_nse`)
  - **SQLite** (Dhan data into a `dhan_nse` table)
- 📊 **Compares** the trading symbols and generates:
  - `common_stocks.csv` – in both Upstox and Dhan
  - `only_in_upstox.csv` – exclusive to Upstox
  - `only_in_dhan.csv` – exclusive to Dhan

---

## 📁 Folder & File Overview

```bash
.
├── etl_pipeline.py         # Main ETL pipeline script
├── sqlite_schema.sql       # Schema for the SQLite table
├── check_mongo.py          # MongoDB inspection script (optional)
├── check_sqlite.py         # SQLite inspection script (optional)
├── check_csv.py            # CSV output validation script (optional)
├── output/                 # Folder for generated CSV comparison files
└── README.md               # You're reading it :)
