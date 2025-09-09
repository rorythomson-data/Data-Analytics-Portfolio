import requests
from dotenv import load_dotenv
import os
import json
import pandas as pd
import logging

logging.basicConfig(filename="logs/pipeline.log", level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def fetch_chartmogul_subscriptions():
    load_dotenv()
    api_key = os.getenv("CHARTMOGUL_API_KEY")
    print("API key loaded:", bool(api_key))
    if not api_key:
        logging.error("Missing ChartMogul API key. Exiting.")
        exit(1)

    try:
        # Load customer UUIDs
        df = pd.read_csv("data/clean/chartmogul_customers_clean.csv")
        uuids = df["uuid"].dropna().unique()

        all_subscriptions = []
        for uuid in uuids:
            url = f"https://api.chartmogul.com/v1/customers/{uuid}/subscriptions"
            response = requests.get(url, auth=(api_key, ''))
            if response.status_code == 200:
                data = response.json()
                subs = data.get("subscriptions", [])
                for sub in subs:
                    sub["customer_uuid"] = uuid  # keep customer reference
                all_subscriptions.extend(subs)
            else:
                logging.warning(f"Failed to fetch subscriptions for customer {uuid}: {response.status_code}")

        os.makedirs("data/raw", exist_ok=True)
        with open("data/raw/chartmogul_subscriptions_raw.json", "w", encoding="utf-8") as f:
            json.dump(all_subscriptions, f, indent=2)
        logging.info("✅ All subscriptions saved to data/raw/chartmogul_subscriptions_raw.json")

    except Exception as e:
        logging.error(f"❌ Failed to fetch subscriptions: {e}")
        exit(1)

if __name__ == "__main__":
    fetch_chartmogul_subscriptions()
