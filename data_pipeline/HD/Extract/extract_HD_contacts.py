# ===========================================================
# 📌 HOLDED CONTACTS EXTRACT SCRIPT – FINAL VERSION
# ===========================================================
# This script extracts contact data from the Holded API and stores it 
# in a structured format inside the project-wide ETL system.
#
# 🔹 INPUT: 
#     - No input file; data fetched directly from Holded API
#
# 🔹 OUTPUT:
#     - Raw data saved as JSON in: data/INPUT/holded_contacts/raw/
#         - holded_contacts_raw.json
#
# 🔹 Features:
#     - Uses .env for secure API key management
#     - Saves response to structured folder for reproducibility
#     - Logs status and errors to logs/pipeline.log
# ===========================================================

import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
import logging

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
# 🚀 MAIN FUNCTION
# ============================================

def fetch_holded_contacts():
    try:
        # ✅ Load API key from .env
        load_dotenv()
        api_key = os.getenv("HOLDED_API_KEY", "").strip()
        if not api_key:
            logging.error("❌ Missing HOLDED_API_KEY in .env")
            raise ValueError("API key is missing")

        # ✅ Define request
        url = "https://api.holded.com/api/invoicing/v1/contacts"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "key": api_key
        }

        # ✅ Send request
        logging.info("➡️ Requesting contacts from Holded API")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # ✅ Create output directory
        raw_dir = "data/INPUT/holded_contacts/raw"
        os.makedirs(raw_dir, exist_ok=True)

        # ✅ Save raw JSON (full fidelity)
        json_path = os.path.join(raw_dir, "holded_contacts_raw.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logging.info(f"✅ Saved raw JSON to {json_path}")

        logging.info(f"✅ Total records received: {len(data)}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"🔴 Response content: {e.response.text}")
        raise

    except Exception as e:
        logging.error(f"❌ Unexpected error in Holded contacts extract: {e}", exc_info=True)
        raise

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    fetch_holded_contacts()
