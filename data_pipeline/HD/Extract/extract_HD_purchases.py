# ================================================================
# 📌 HOLDED PURCHASES EXTRACT SCRIPT – FINAL VERSION
# ================================================================
# Extracts Holded purchase documents using pagination and date filtering.
# Saves raw JSON to disk for transformation.
#
# 🔹 INPUT:  None (calls Holded API with .env key)
# 🔹 OUTPUT: data/INPUT/holded_purchases/raw/holded_purchases_raw.json
# ================================================================

import os
import json
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

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

def fetch_holded_purchases(start_date="2024-01-01", end_date=None):
    try:
        # ✅ Load API key
        load_dotenv()
        api_key = os.getenv("HOLDED_API_KEY", "").strip()
        if not api_key:
            logging.error("❌ Missing HOLDED_API_KEY in .env")
            raise ValueError("API key is missing")

        if end_date is None:
            end_date = datetime.today().strftime("%Y-%m-%d")

        # ✅ Convert dates to UNIX timestamps
        ts_start = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        ts_end = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

        url_base = "https://api.holded.com/api/invoicing/v1/documents/purchase"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "key": api_key
        }

        page = 1
        all_data = []

        while True:
            url = f"{url_base}?page={page}&starttmp={ts_start}&endtmp={ts_end}"
            logging.info(f"➡️ Fetching page {page}: {url}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()
            logging.info(f"📄 Page {page} returned {len(data)} purchases.")
            if not data:
                break

            all_data.extend(data)
            page += 1

        # ✅ Create output folder
        raw_dir = "data/INPUT/holded_purchases/raw"
        os.makedirs(raw_dir, exist_ok=True)

        # ✅ Save raw JSON
        json_path = os.path.join(raw_dir, "holded_purchases_raw.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2)
        logging.info(f"✅ Raw JSON saved to {json_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"🔴 Raw response content: {e.response.text}")
        raise

    except Exception as e:
        logging.error(f"❌ Unexpected error in purchases extract: {e}")
        raise

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    fetch_holded_purchases()
