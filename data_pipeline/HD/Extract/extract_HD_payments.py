# ================================================================
# 📌 HOLDED PAYMENTS EXTRACT SCRIPT – FINAL VERSION
# ================================================================
# This script extracts Holded payments via API with pagination and
# date filtering. Saves raw JSON in standard ETL format.
#
# 🔹 INPUT:
#     - Live API call (no input files)
#     - Optional: start_date, end_date (defaults: 2024-01-01 → today)
#
# 🔹 OUTPUT:
#     - data/INPUT/holded_payments/raw/holded_payments_raw.json
#
# 🔹 Features:
#     - Secure API key from .env
#     - Pagination + timestamp filtering
#     - Standard ETL output layout
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

def fetch_holded_payments(start_date="2024-01-01", end_date=None):
    try:
        # ✅ Load API key
        load_dotenv()
        api_key = os.getenv("HOLDED_API_KEY", "").strip()
        if not api_key:
            logging.error("❌ Missing HOLDED_API_KEY in .env")
            raise ValueError("API key is missing.")

        if end_date is None:
            end_date = datetime.today().strftime("%Y-%m-%d")

        # ✅ Convert to UNIX timestamps
        ts_start = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        ts_end = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

        url_base = "https://api.holded.com/api/invoicing/v1/payments"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "key": api_key
        }

        page = 1
        all_data = []

        while True:
            url = f"{url_base}?page={page}&starttmp={ts_start}&endtmp={ts_end}"
            logging.info(f"➡️ Fetching page {page} from Holded Payments API")
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()
            logging.info(f"📄 Page {page} returned {len(data)} payments.")
            if not data:
                break

            all_data.extend(data)
            page += 1

        if not all_data:
            logging.warning("⚠️ No payments returned from Holded.")
            return

        # ✅ Output path
        output_dir = "data/INPUT/holded_payments/raw"
        os.makedirs(output_dir, exist_ok=True)

        # ✅ Save JSON
        json_path = os.path.join(output_dir, "holded_payments_raw.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2)
        logging.info(f"✅ Raw JSON saved to {json_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"🔴 API response: {e.response.text}")
        raise

    except Exception as e:
        logging.error(f"❌ Unexpected error in payments extract: {e}", exc_info=True)
        raise

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    fetch_holded_payments()
