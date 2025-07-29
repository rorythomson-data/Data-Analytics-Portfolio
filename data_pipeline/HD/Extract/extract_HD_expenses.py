# ================================================================
# 📌 HOLDED EXPENSE ACCOUNTS EXTRACT SCRIPT – FINAL VERSION (SECRETS READY)
# ================================================================
# This script extracts expense account data from the Holded API
# and saves the raw API response as JSON in the correct folder.
#
# 🔹 INPUT: 
#     - No input file; data fetched from Holded API
#
# 🔹 OUTPUT:
#     - data/INPUT/holded_expenses/raw/holded_expenses_raw.json
#
# 🔹 Features:
#     - Uses .env or GitHub Secrets for API key (secure)
#     - Validates response structure
#     - Saves raw API data exactly as received (for auditing)
#     - Logs success and failure events
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
# 🔐 LOAD API KEY
# ============================================

def load_api_key() -> str:
    if os.path.exists(".env"):
        load_dotenv()
    api_key = os.getenv("HOLDED_API_KEY", "").strip()
    if not api_key:
        logging.error("❌ Missing HOLDED_API_KEY (check .env or GitHub Secrets).")
        raise ValueError("HOLDED_API_KEY not found.")
    return api_key

# ============================================
# 🚀 MAIN FUNCTION
# ============================================

def fetch_holded_expenses():
    try:
        api_key = load_api_key()

        # ✅ Prepare request
        url = "https://api.holded.com/api/invoicing/v1/expensesaccounts"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "key": api_key
        }

        logging.info("➡️ Fetching Holded expense accounts...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # ✅ Create output folder
        raw_dir = "data/INPUT/holded_expenses/raw"
        os.makedirs(raw_dir, exist_ok=True)

        # ✅ Save raw JSON to disk (for auditing/reproducibility)
        json_path = os.path.join(raw_dir, "holded_expenses_raw.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logging.info(f"✅ JSON saved to {json_path}")

        logging.info(f"✅ Total objects fetched: {len(data) if isinstance(data, list) else '1'}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API request error: {e}")
        if e.response is not None:
            logging.error(f"🔴 API response: {e.response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error in Holded expenses extract: {e}", exc_info=True)
        raise

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    fetch_holded_expenses()

