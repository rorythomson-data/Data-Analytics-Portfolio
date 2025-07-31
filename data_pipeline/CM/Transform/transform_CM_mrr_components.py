# ================================================================
# 📌 CHARTMOGUL MRR COMPONENTS TRANSFORM SCRIPT 
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

def transform_chartmogul_mrr_components():
    print("🚩 Starting transform_chartmogul_mrr_components()")
    try:
        input_path = "data/INPUT/chartmogul_mrr_components/raw/chartmogul_mrr_components_raw.json"
        output_dir = "data/INPUT/chartmogul_mrr_components/clean"
        base_filename = "chartmogul_mrr_components_clean"

        if not os.path.exists(input_path):
            msg = "❌ MRR components raw JSON file not found."
            logging.error(msg)
            print(msg)
            sys.exit(1)

        # ✅ Load raw JSON
        print(f"📥 Loading raw data from {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        entries = raw_data.get("entries", [])
        if not entries:
            msg = "⚠️ No entries found in ChartMogul MRR components data."
            logging.warning(msg)
            print(msg)
            return

        df = pd.DataFrame(entries)

        if df.empty:
            msg = "⚠️ MRR components data is empty after conversion."
            logging.warning(msg)
            print(msg)
            return

        # ✅ Convert any list/dict columns to JSON strings
        complex_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (list, dict))).any()]
        for col in complex_cols:
            logging.warning(f"⚠️ Converting complex type column to string: {col}")
            print(f"⚠️ Converting complex type column to string: {col}")
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        # ✅ Ensure output folder exists
        os.makedirs(output_dir, exist_ok=True)

        # ✅ Save Parquet with fallback
        parquet_path = os.path.join(output_dir, base_filename + ".parquet")
        save_parquet(df, parquet_path)
        logging.info(f"✅ Saved Parquet to {parquet_path}")
        print(f"✅ Saved Parquet to {parquet_path}")

        # ✅ Save CSV
        csv_path = os.path.join(output_dir, base_filename + ".csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"✅ Saved CSV to {csv_path}")
        print(f"✅ Saved CSV to {csv_path}")

        logging.info(f"✅ Total ChartMogul MRR component records cleaned: {len(df)}")
        print(f"✅ Total ChartMogul MRR component records cleaned: {len(df)}")

    except Exception as e:
        logging.error(f"❌ Failed to transform MRR components data: {e}", exc_info=True)
        print(f"❌ Failed to transform MRR components data: {e}")
        sys.exit(1)

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_chartmogul_mrr_components()
