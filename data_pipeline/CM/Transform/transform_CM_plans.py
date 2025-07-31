# ===========================================================
# 📌 CHARTMOGUL PLANS TRANSFORM SCRIPT 
# ===========================================================
# This script transforms raw ChartMogul subscription plan data into a structured format.
# It is part of a modular, production-ready ETL pipeline used in SaaS analytics.
#
# 🔹 INPUT:
#     - Raw JSON: data/INPUT/chartmogul_plans/raw/chartmogul_plans_raw.json
#
# 🔹 OUTPUT:
#     - Cleaned plans data saved as:
#         - data/INPUT/chartmogul_plans/clean/chartmogul_plans_clean.csv
#         - data/INPUT/chartmogul_plans/clean/chartmogul_plans_clean.parquet
#
# 🔹 Features:
#     - Normalizes JSON data into a clean tabular format
#     - Creates required folders if they don’t exist
#     - Logs every major step and error
# ===========================================================

import os
import json
import pandas as pd
import logging
import sys

# ============================================
# 🪵 LOGGING SETUP
# ============================================

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================
# 💾 SAVE PARQUET WITH FALLBACK
# ============================================

def save_parquet(df, path):
    """
    Save DataFrame as Parquet, preferring pyarrow but falling back to fastparquet.
    """
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except ImportError:
        print("⚠️ pyarrow not found, falling back to fastparquet.")
        logging.warning("pyarrow not found, falling back to fastparquet.")
        df.to_parquet(path, index=False, engine="fastparquet")

# ============================================
# 🚀 MAIN TRANSFORM FUNCTION
# ============================================

def transform_chartmogul_plans():
    print("🚩 Starting transform_chartmogul_plans()")
    try:
        input_path = "data/INPUT/chartmogul_plans/raw/chartmogul_plans_raw.json"
        output_dir = "data/INPUT/chartmogul_plans/clean"
        base_filename = "chartmogul_plans_clean"

        if not os.path.exists(input_path):
            msg = "❌ Raw ChartMogul plans JSON file not found."
            logging.error(msg)
            print(msg)
            sys.exit(1)

        # ✅ Load raw JSON
        print(f"📥 Loading raw data from {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        plans = raw_data.get("plans", [])
        df = pd.json_normalize(plans)

        if df.empty:
            msg = "⚠️ Plans data is empty after normalization."
            logging.warning(msg)
            print(msg)
            return

        os.makedirs(output_dir, exist_ok=True)

        # ✅ Save to Parquet with fallback
        parquet_path = os.path.join(output_dir, base_filename + ".parquet")
        save_parquet(df, parquet_path)
        logging.info(f"✅ Saved Parquet to {parquet_path}")
        print(f"✅ Saved Parquet to {parquet_path}")

        # ✅ Save to CSV
        csv_path = os.path.join(output_dir, base_filename + ".csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"✅ Saved CSV to {csv_path}")
        print(f"✅ Saved CSV to {csv_path}")

        logging.info(f"✅ ChartMogul plans transformation completed: {len(df)} rows processed.")
        print(f"✅ ChartMogul plans transformation completed: {len(df)} rows processed.")

    except Exception as e:
        logging.error(f"❌ Failed to transform ChartMogul plans data: {e}", exc_info=True)
        print(f"❌ Failed to transform ChartMogul plans data: {e}")
        sys.exit(1)

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_chartmogul_plans()


