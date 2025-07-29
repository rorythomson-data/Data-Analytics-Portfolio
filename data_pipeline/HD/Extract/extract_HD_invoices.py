# ================================================================
# 📌 HOLDED INVOICES EXTRACT SCRIPT – FINAL VERSION (SECRETS READY)
# ================================================================
# This script extracts invoice data from the Holded API using
# pagination and date filtering. It saves raw output to JSON, CSV,
# and Parquet formats inside the structured ETL directory.
#
# 🔹 INPUT: None (data pulled directly from API)
# 🔹 OUTPUT:
#     - data/INPUT/holded_invoices/raw/
#         • holded_invoices_raw.json
#         • holded_invoices_raw.csv
#         • holded_invoices_raw.parquet
#
# 🔹 Features:
#     - Date filtering via UNIX timestamps
#     - Pagination to retrieve all records
#     - Nested list/dict column handling
#     - Logging and error handling
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

def fetch_holded_invoices(start_date="2024-01-01", end_date=None):
    logging.info("🚩 Entered fetch_holded_invoices()")

    try:
        api_key = load_api_key()

        # ✅ Define date range
        if end_date is None:
            end_date = datetime.today().strftime("%Y-%m-%d")

        ts_start = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        ts_end = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

        url_base = "https://api.holded.com/api/invoicing/v1/documents/invoice"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "key": api_key
        }

        # ✅ Paginate through API
        page = 1
        all_data = []

        while True:
            url = f"{url_base}?page={page}&starttmp={ts_start}&endtmp={ts_end}"
            logging.info(f"➡️ Fetching page {page} from Holded Invoices API")
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()
            logging.info(f"📄 Page {page} returned {len(data)} invoices.")
            if not data:
                break

            all_data.extend(data)
            page += 1

        # ✅ Create output folder
        raw_dir = "data/INPUT/holded_invoices/raw"
        os.makedirs(raw_dir, exist_ok=True)

        # ✅ Convert to DataFrame
        df = pd.DataFrame(all_data)
        if df.empty:
            logging.warning("⚠️ No invoice data returned.")
            return

        # ✅ Detect and stringify list-type columns
        list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
        if list_columns:
            logging.warning(f"⚠️ List-type columns in invoice data: {list_columns}")
            for col in list_columns:
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

        # ✅ Detect and stringify dict-type columns
        dict_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, dict)).any()]
        if dict_columns:
            logging.warning(f"⚠️ Dict-type columns in invoice data: {dict_columns}")
            for col in dict_columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

        # ✅ Save raw JSON
        json_path = os.path.join(raw_dir, "holded_invoices_raw.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2)
        logging.info(f"✅ JSON saved to {json_path}")

        # ✅ Save to CSV
        csv_path = os.path.join(raw_dir, "holded_invoices_raw.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logging.info(f"✅ CSV saved to {csv_path}")

        # ✅ Save to Parquet
        parquet_path = os.path.join(raw_dir, "holded_invoices_raw.parquet")
        df.to_parquet(parquet_path, index=False)
        logging.info(f"✅ Parquet saved to {parquet_path}")

        logging.info(f"✅ Total invoices extracted: {len(df)}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API request failed: {e}")
        if e.response is not None:
            logging.error(f"🔴 API response content: {e.response.text}")
        raise

    except Exception as e:
        logging.error(f"❌ Unexpected error in invoice extract: {e}", exc_info=True)
        raise

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    fetch_holded_invoices()

