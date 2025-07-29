# ================================================================
# 📌 CHARTMOGUL METRICS EXTRACT SCRIPT – FINAL VERSION
# ================================================================
# Extracts monthly ChartMogul metrics using their API.
# Saves raw JSON, and pre-cleaned CSV/Parquet outputs for preview or lightweight use.
#
# 🔹 INPUT:  None (data pulled live from API)
# 🔹 OUTPUT:
#     • JSON → data/INPUT/chartmogul_metrics/raw/chartmogul_metrics_raw.json
#     • CSV  → data/INPUT/chartmogul_metrics/clean/chartmogul_metrics_raw.csv
#     • Parquet → data/INPUT/chartmogul_metrics/clean/chartmogul_metrics_raw.parquet
#
# 🔹 Features:
#     • Secure API access from .env or GitHub Secrets
#     • Full month-by-month history
#     • Robust logging and fallback handling
# ================================================================

import os
import json
import logging
import requests
import pandas as pd
from datetime import date, datetime, timedelta
from dotenv import load_dotenv

# ============================================
# 🪵 LOGGING SETUP
# ============================================

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================
# 📅 MONTH RANGE GENERATOR
# ============================================

def month_range(start_date, end_date):
    current = start_date.replace(day=1)
    while current <= end_date:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield current, (next_month - timedelta(days=1))
        current = next_month

# ============================================
# 🔐 LOAD API KEY
# ============================================

def load_api_key():
    # Load .env only if running locally
    if os.path.exists(".env"):
        load_dotenv()

    api_key = os.getenv("CHARTMOGUL_API_KEY", "").strip()
    if not api_key:
        logging.error("❌ Missing CHARTMOGUL_API_KEY in environment variables.")
        raise ValueError("API key missing. Set CHARTMOGUL_API_KEY in .env or GitHub Secrets.")
    return api_key

# ============================================
# 🚀 MAIN EXTRACTION FUNCTION
# ============================================

def fetch_chartmogul_metrics():
    try:
        # ✅ Load API key
        api_key = load_api_key()

        # ✅ Configuration
        base_url = "https://api.chartmogul.com/v1/metrics/all"
        start_date = datetime.strptime("2024-03-01", "%Y-%m-%d").date()
        end_date = date.today()
        base_path = "data/INPUT/chartmogul_metrics"
        raw_dir = os.path.join(base_path, "raw")
        clean_dir = os.path.join(base_path, "clean")
        base_filename = "chartmogul_metrics_raw"
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(clean_dir, exist_ok=True)

        all_entries = []

        # ✅ Iterate over months
        for month_start, month_end in month_range(start_date, end_date):
            params = {
                "start-date": month_start.isoformat(),
                "end-date": month_end.isoformat(),
                "interval": "month"
            }

            try:
                response = requests.get(base_url, auth=(api_key, ""), params=params)
                response.raise_for_status()
                entries = response.json().get("entries", [])
                if not entries:
                    logging.warning(f"⚠️ No metrics for {month_start.strftime('%Y-%m')}")
                    continue
                all_entries.extend(entries)
            except requests.exceptions.RequestException as e:
                logging.error(f"❌ Failed for {month_start.strftime('%Y-%m')}: {e}")
                continue

        if not all_entries:
            logging.warning("⚠️ No ChartMogul metrics returned at all.")
            return

        df = pd.json_normalize(all_entries)

        # ✅ Save JSON to raw/
        json_path = os.path.join(raw_dir, f"{base_filename}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({"entries": all_entries}, f, indent=2)
        logging.info(f"✅ Saved raw JSON to: {json_path}")

        # ✅ Save CSV to clean/
        csv_path = os.path.join(clean_dir, f"{base_filename}.csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"✅ Saved CSV to: {csv_path}")

        # ✅ Save Parquet to clean/
        parquet_path = os.path.join(clean_dir, f"{base_filename}.parquet")
        df.to_parquet(parquet_path, index=False)
        logging.info(f"✅ Saved Parquet to: {parquet_path}")

        logging.info(f"✅ Total ChartMogul metric entries collected: {len(df)}")

    except Exception as e:
        logging.error(f"❌ Unexpected error during ChartMogul metrics extract: {e}", exc_info=True)
        raise

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    fetch_chartmogul_metrics()

