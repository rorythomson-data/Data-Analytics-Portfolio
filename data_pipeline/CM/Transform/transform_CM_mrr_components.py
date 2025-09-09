# ================================================================
# 📌 CHARTMOGUL MRR COMPONENTS TRANSFORM SCRIPT – FINAL VERSION (FIXED)
# ================================================================
# This script transforms raw ChartMogul MRR components data into a clean tabular format.
# It is part of the automated financial metrics pipeline for a SaaS B2B company.
#
# 🔹 INPUT:
#     - Raw JSON file:
#         data/INPUT/chartmogul_mrr_components/raw/chartmogul_mrr_components_raw.json
#
# 🔹 OUTPUT:
#     - Cleaned data saved in:
#         • data/INPUT/chartmogul_mrr_components/clean/chartmogul_mrr_components_clean.csv
#         • data/INPUT/chartmogul_mrr_components/clean/chartmogul_mrr_components_clean.parquet
#
# 🔹 Features:
#     - Converts nested/complex fields to JSON strings
#     - Ensures compatibility with Parquet format
#     - Robust logging and error handling
# ================================================================

import os
import json
import pandas as pd
import logging

# ============================================
# 🪵 LOGGING SETUP
# ============================================

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================
# 🚀 MAIN TRANSFORM FUNCTION
# ============================================

def transform_chartmogul_mrr_components():
    try:
        input_path = "data/INPUT/chartmogul_mrr_components/raw/chartmogul_mrr_components_raw.json"
        output_dir = "data/INPUT/chartmogul_mrr_components/clean"
        base_filename = "chartmogul_mrr_components_clean"

        if not os.path.exists(input_path):
            logging.error("❌ MRR components raw JSON file not found.")
            return

        # ✅ Load raw JSON
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        entries = raw_data.get("entries", [])
        if not entries:
            logging.warning("⚠️ No entries found in ChartMogul MRR components data.")
            return

        df = pd.DataFrame(entries)

        if df.empty:
            logging.warning("⚠️ MRR components data is empty after conversion.")
            return

        # ✅ Convert any list/dict columns to JSON strings
        complex_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (list, dict))).any()]
        for col in complex_cols:
            logging.warning(f"⚠️ Converting complex type column to string: {col}")
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        # ✅ Ensure output folder exists
        os.makedirs(output_dir, exist_ok=True)

        # ✅ Save Parquet
        parquet_path = os.path.join(output_dir, base_filename + ".parquet")
        df.to_parquet(parquet_path, index=False)
        logging.info(f"✅ Saved Parquet to {parquet_path}")

        # ✅ Save CSV
        csv_path = os.path.join(output_dir, base_filename + ".csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"✅ Saved CSV to {csv_path}")

        logging.info(f"✅ Total ChartMogul MRR component records cleaned: {len(df)}")

    except Exception as e:
        logging.error(f"❌ Failed to transform MRR components data: {e}", exc_info=True)

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_chartmogul_mrr_components()
