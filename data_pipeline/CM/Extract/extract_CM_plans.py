# ================================================================
# 📌 CHARTMOGUL PLANS EXTRACT SCRIPT – FINAL VERSION (SECRETS READY)
# ================================================================
# Extracts plan data from ChartMogul's API.
# Part of the SaaS business intelligence pipeline for tracking pricing strategy and product tiers.
#
# 🔹 INPUT: 
#     - None (direct API request)
#
# 🔹 OUTPUT:
#     - JSON in: data/INPUT/chartmogul_plans/raw/chartmogul_plans_raw.json
#
# 🔹 Design Principles:
#     - Extract-only: no CSV/Parquet generation here
#     - Supports both .env (local) and GitHub Secrets
#     - Structured logging
#     - Compatible with downstream transform scripts
# ================================================================

import os
import json
import logging
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
# 🔐 API AUTHENTICATION
# ============================================

def load_api_key() -> str:
    if os.path.exists(".env"):
        load_dotenv()

    api_key = os.getenv("CHARTMOGUL_API_KEY", "").strip()
    if not api_key:
        logging.error("❌ Missing CHARTMOGUL_API_KEY (check .env or GitHub Secrets).")
        raise ValueError("Missing ChartMogul API key.")
    return api_key

# ============================================
# 📤 FETCH PLAN DATA FROM CHARTMOGUL
# ============================================

def fetch_plan_data(api_key: str) -> dict:
    url = "https://api.chartmogul.com/v1/plans"
    try:
        response = requests.get(url, auth=(api_key, ''))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API request failed: {e}")
        raise

# ============================================
# 💾 SAVE RAW JSON
# ============================================

def save_raw_json(data: dict, raw_dir: str, filename: str = "chartmogul_plans_raw.json") -> None:
    os.makedirs(raw_dir, exist_ok=True)
    json_path = os.path.join(raw_dir, filename)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logging.info(f"✅ Saved raw JSON to {json_path}")

# ============================================
# 🚀 MAIN EXTRACT FUNCTION
# ============================================

def run_extract_pipeline():
    logging.info("🚀 Starting ChartMogul plans extract pipeline")
    api_key = load_api_key()
    data = fetch_plan_data(api_key)

    raw_dir = os.path.join("data", "INPUT", "chartmogul_plans", "raw")
    save_raw_json(data, raw_dir)

    logging.info("✅ ChartMogul plans extract pipeline completed.")

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    run_extract_pipeline()

