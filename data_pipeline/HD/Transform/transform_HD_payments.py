# ================================================================
# 📌 HOLDED PAYMENTS TRANSFORM SCRIPT
# ================================================================
# Transforms Holded payments raw JSON into a clean tabular format.
#
# 🔹 INPUT:
#     - data/INPUT/holded_payments/raw/holded_payments_raw.json
#
# 🔹 OUTPUT:
#     - data/INPUT/holded_payments/clean/
#         • holded_payments_clean.parquet
#         • holded_payments_clean.csv
#
# 🔹 Features:
#     - Stringifies list-type fields
#     - Parses UNIX timestamps
#     - Handles optional nested records gracefully
# ================================================================

import os
import json
import logging
import pandas as pd
import sys

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
# 🧼 TRANSFORMATION FUNCTION
# ============================================

def transform_holded_payments():
    print("🚩 Starting transform_holded_payments()")
    logging.info("🚩 Entered transform_holded_payments()")

    input_path = "data/INPUT/holded_payments/raw/holded_payments_raw.json"
    output_dir = "data/INPUT/holded_payments/clean"
    base_filename = "holded_payments_clean"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # ✅ Load raw JSON
        print(f"📥 Loading data from {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        records = raw_data["data"] if isinstance(raw_data, dict) and "data" in raw_data else raw_data
        df = pd.json_normalize(records)

        if df.empty:
            msg = "⚠️ Empty Holded payments data."
            logging.warning(msg)
            print(msg)
            return

        # ✅ Parse timestamps
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], unit="s", errors="coerce")

        # ✅ Detect and stringify list-type columns
        list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
        if list_columns:
            logging.warning(f"⚠️ List-type columns in Holded payments: {list_columns}")
            print(f"⚠️ List-type columns in Holded payments: {list_columns}")
            for col in list_columns:
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

        # ✅ Stringify problematic object-type fields
        for col in ["vatnumber"]:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # ✅ Save as Parquet
        parquet_path = os.path.join(output_dir, base_filename + ".parquet")
        df.to_parquet(parquet_path, index=False)
        logging.info(f"✅ Parquet saved to: {parquet_path}")
        print(f"✅ Parquet saved to: {parquet_path}")

        # ✅ Save as CSV
        csv_path = os.path.join(output_dir, base_filename + ".csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"✅ CSV saved to: {csv_path}")
        print(f"✅ CSV saved to: {csv_path}")

        logging.info(f"✅ Total cleaned Holded payments: {len(df)}")
        print(f"✅ Total cleaned Holded payments: {len(df)}")

    except Exception as e:
        logging.error(f"❌ Error transforming Holded payments: {e}", exc_info=True)
        print(f"❌ Error transforming Holded payments: {e}")
        sys.exit(1)

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_holded_payments()

