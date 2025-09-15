# ================================================================
# 📌 CHARTMOGUL CUSTOMERS EXTRACT SCRIPT - CONTEXT & DESIGN
# ================================================================
# This script extracts customer data from ChartMogul's API and saves it
# in structured folders under data/INPUT/chartmogul_customers/
#
# 🔹 INPUT: None (live API)
# 🔹 OUTPUT:
#     - JSON → data/INPUT/chartmogul_customers/raw/chartmogul_customers_raw.json
#
# 🔹 Features:
#     - Secure API access with .env or GitHub Actions secrets
#     - Consistent logging
#     - Structured output
# ================================================================

import os
import json
import logging
import requests
from dotenv import load_dotenv

# ============================================
# 🔧 CONFIGURATION
# ============================================

BASE_DIR = "data/INPUT/chartmogul_customers"
RAW_DIR = os.path.join(BASE_DIR, "raw")
LOG_PATH = "logs/pipeline.log"
API_URL = "https://api.chartmogul.com/v1/customers"
FILENAME_PREFIX = "chartmogul_customers_raw"

# ============================================
# 🪵 LOGGING SETUP
# ============================================

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================
# 🔐 LOAD API KEY
# ============================================

def load_api_key():
    # Load .env file if it exists (local development)
    if os.path.exists(".env"):
        load_dotenv()

    api_key = os.getenv("CHARTMOGUL_API_KEY")
    if not api_key:
        logging.error("❌ Missing CHARTMOGUL_API_KEY in environment variables.")
        raise ValueError("API key missing. Ensure CHARTMOGUL_API_KEY is set in .env or GitHub Secrets.")
    return api_key

# ============================================
# 📤 FETCH DATA
# ============================================

def fetch_chartmogul_customers(api_key: str) -> dict:
    try:
        response = requests.get(API_URL, auth=(api_key, ''))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API request failed: {e}")
        raise

# ============================================
# 💾 SAVE TO DISK
# ============================================

def save_raw_json(data: dict) -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    json_path = os.path.join(RAW_DIR, f"{FILENAME_PREFIX}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logging.info(f"✅ Saved JSON to {json_path}")

# ============================================
# 🚀 MAIN FUNCTION
# ============================================

def run_extract_pipeline():
    logging.info("🚀 Starting ChartMogul customers extract pipeline")
    api_key = load_api_key()
    data = fetch_chartmogul_customers(api_key)
    save_raw_json(data)
    logging.info("✅ ChartMogul customers extract pipeline completed.")

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    run_extract_pipeline()

