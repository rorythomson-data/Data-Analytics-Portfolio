import json
import pandas as pd
import os
import logging

logging.basicConfig(filename="logs/pipeline.log", level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def transform_chartmogul_subscriptions():
    try:
        with open("data/raw/chartmogul_subscriptions_raw.json", "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if not raw_data:
            logging.warning("⚠️ No subscription data found in raw file.")
            return

        df = pd.json_normalize(raw_data)

        os.makedirs("data/clean", exist_ok=True)
        df.to_csv("data/clean/chartmogul_subscriptions_clean.csv", index=False)
        logging.info("✅ Cleaned subscriptions data saved to data/clean/chartmogul_subscriptions_clean.csv")
    except FileNotFoundError:
        logging.error("❌ Subscription raw data file not found.")
    except Exception as e:
        logging.error(f"❌ Failed to transform subscriptions data: {e}")
        exit(1)

if __name__ == "__main__":
    transform_chartmogul_subscriptions()
