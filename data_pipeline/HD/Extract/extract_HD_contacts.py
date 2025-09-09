# ===========================================================
# HOLDED CONTACTS EXTRACT SCRIPT – FINAL VERSION (SECRETS READY)
# ===========================================================
# Extracts contact data from the Holded API and stores it
# in a structured format inside the ETL system.
#
#   INPUT: 
#     - No input file; data fetched directly from Holded API
#
#   OUTPUT:
#     - Raw data saved as JSON in: data/INPUT/holded_contacts/raw/
#         - holded_contacts_raw.json
#
#   Features:
#     - Uses .env (local) or GitHub Secrets for API key
#     - Logs all actions to logs/pipeline.log
#     - Includes full response logging in case of errors
# ===========================================================

import os
import json
import requests
import logging
from dotenv import load_dotenv

# ============================================
# LOGGING SETUP
# ============================================

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================
# LOAD API KEY
# ============================================

def load_api_key() -> str:
    if os.path.exists(".env"):
        load_dotenv()
    api_key = os.getenv("HOLDED_API_KEY", "").strip()
    if not api_key:
        logging.error("Missing HOLDED_API_KEY (check .env or GitHub Secrets).")
        raise ValueError("HOLDED_API_KEY not found.")
    return api_key

# ============================================
# FETCH AND SAVE CONTACTS
# ============================================

def fetch_holded_contacts():
    try:
        api_key = load_api_key()

        url = "https://api.holded.com/api/invoicing/v1/contacts"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "key": api_key
        }

        logging.info("Requesting contacts from Holded API...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        raw_dir = "data/INPUT/holded_contacts/raw"
        os.makedirs(raw_dir, exist_ok=True)

        json_path = os.path.join(raw_dir, "holded_contacts_raw.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved raw JSON to {json_path}")
        logging.info(f"Total records received: {len(data)}")

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        if e.response is not None:
            logging.error(f"Response content: {e.response.text}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in Holded contacts extract: {e}", exc_info=True)
        raise

# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    fetch_holded_contacts()

