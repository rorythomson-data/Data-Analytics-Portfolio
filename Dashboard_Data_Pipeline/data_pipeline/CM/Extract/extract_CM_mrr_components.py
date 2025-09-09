# ================================================================
# 📌 CHARTMOGUL MRR COMPONENTS EXTRACT SCRIPT – FINAL VERSION (FIXED)
# ================================================================
# This script fetches monthly MRR components from ChartMogul's API.
# It is part of a larger ETL pipeline for a B2B SaaS business metrics system.
#
# 🔹 INPUT:
#     - Live ChartMogul API requests (no input files required)
#
# 🔹 OUTPUT:
#     - Raw JSON saved in: data/INPUT/chartmogul_mrr_components/raw/
#         • chartmogul_mrr_components_raw.json
#
# 🔹 Features:
#     - Uses API key from .env (CHARTMOGUL_API_KEY) or GitHub Secrets
#     - Logs all actions to logs/pipeline.log
#     - Deduplicates entries by 'date' field
# ================================================================

import os
import json
import logging
from datetime import date, timedelta, datetime
import requests
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
# 📅 HELPER: MONTH RANGE GENERATOR
# ============================================

def month_range(start_date: date, end_date: date):
    """
    Generate tuples of (month_start, month_end) between start_date and end_date.
    Each tuple represents one calendar month.
    """
    current = start_date.replace(day=1)
    while current <= end_date:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield current, (next_month - timedelta(days=1))
        current = next_month

# ============================================
# 🔐 LOAD API KEY
# ============================================

def load_api_key():
    # Load .env only when running locally
    if os.path.exists(".env"):
        load_dotenv()

    api_key = os.getenv("CHARTMOGUL_API_KEY", "").strip()
    if not api_key:
        logging.error("❌ Missing CHARTMOGUL_API_KEY in environment variables.")
        raise ValueError("API key missing. Set CHARTMOGUL_API_KEY in .env or GitHub Secrets.")
    return api_key

# ============================================
# 📤 API REQUEST FOR SINGLE MONTH
# ============================================

def fetch_monthly_data(start: date, end: date, api_key: str):
    """
    Fetch ChartMogul MRR component data for a single month range.
    """
    url = "https://api.chartmogul.com/v1/metrics/mrr"
    params = {
        "start-date": start.isoformat(),
        "end-date": end.isoformat(),
        "interval": "month"
    }
    try:
        response = requests.get(url, auth=(api_key, ''), params=params)
        response.raise_for_status()
        return response.json().get("entries", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API request failed for {start} to {end}: {e}")
        return []

# ============================================
# 🚀 MAIN PIPELINE FUNCTION
# ============================================

def fetch_chartmogul_mrr_components():
    logging.info("🚀 Starting ChartMogul MRR components extract")
    api_key = load_api_key()

    start_date = date(2024, 3, 1)
    end_date = date.today()
    all_entries = []

    for month_start, month_end in month_range(start_date, end_date):
        logging.info(f"📅 Fetching MRR data from {month_start} to {month_end}")
        entries = fetch_monthly_data(month_start, month_end, api_key)
        all_entries.extend(entries)

    # ✅ Remove duplicates by 'date'
    seen_dates = set()
    unique_entries = []
    for entry in all_entries:
        if entry["date"] not in seen_dates:
            unique_entries.append(entry)
            seen_dates.add(entry["date"])

    if not unique_entries:
        logging.warning("⚠️ No MRR data retrieved. Exiting without saving files.")
        return

    # ✅ Output path
    base_dir = "data/INPUT/chartmogul_mrr_components"
    raw_dir = os.path.join(base_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    json_path = os.path.join(raw_dir, "chartmogul_mrr_components_raw.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"entries": unique_entries}, f, indent=2)

    logging.info(f"✅ Saved raw JSON to {json_path}")

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    fetch_chartmogul_mrr_components()

