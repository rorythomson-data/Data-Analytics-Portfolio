# ===========================================================
# 📌 HOLDED TREASURY (CASH/BANK) ACCOUNTS – EXTRACT SCRIPT
# ===========================================================
# Pulls the current list of treasury (cash/bank) accounts from
# the Holded API and writes the RAW payload for downstream use.
#
# 🔹 INPUT:
#     - None (fetch from Holded API)
#
# 🔹 OUTPUT (RAW):
#     - data/INPUT/holded_treasury/raw/holded_treasury_raw.json
#
# 🔹 Notes:
#     - Uses HOLDED_API_KEY from .env / environment
#     - Logs to logs/pipeline.log
#     - Leaves parsing/aggregation to transform step(s)
# ===========================================================

import os
import json
import logging
import requests
from dotenv import load_dotenv

# ============================================
# 🪵 LOGGING
# ============================================

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================
# 🔐 API KEY
# ============================================

def load_api_key() -> str:
    if os.path.exists(".env"):
        load_dotenv()
    api_key = (os.getenv("HOLDED_API_KEY") or "").strip()
    if not api_key:
        logging.error("❌ Missing HOLDED_API_KEY (check .env or GitHub Secrets).")
        raise ValueError("HOLDED_API_KEY not found.")
    return api_key

# ============================================
# 🚀 FETCH + SAVE RAW
# ============================================

def fetch_holded_treasury_accounts():
    api_key = load_api_key()

    url = "https://api.holded.com/api/invoicing/v1/treasury"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "key": api_key,
    }

    logging.info("➡️ Requesting Holded treasury accounts …")
    resp = requests.get(url, headers=headers, timeout=30)

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logging.error(f"❌ Treasury API request failed: {e}")
        logging.error(f"🔴 Response status={resp.status_code} body={resp.text[:500]}")
        raise

    # Try to parse JSON; if it fails, still save the raw text for debugging.
    try:
        data = resp.json()
    except ValueError:
        data = resp.text
        logging.warning("⚠️ Response is not valid JSON; saving raw text.")

    raw_dir = os.path.join("data", "INPUT", "holded_treasury", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    out_path = os.path.join(raw_dir, "holded_treasury_raw.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False) if isinstance(data, (dict, list)) else f.write(data)

    if isinstance(data, list):
        logging.info(f"✅ Saved RAW treasury accounts to {out_path} (records: {len(data)})")
    else:
        logging.info(f"✅ Saved RAW treasury payload to {out_path}")

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    fetch_holded_treasury_accounts()
