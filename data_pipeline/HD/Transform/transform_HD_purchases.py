# ================================================================
# 📌 HOLDED PURCHASES TRANSFORM SCRIPT – FINAL VERSION (Updated)
# ================================================================
# Transforms Holded purchases raw JSON into a clean tabular format.
# INPUT:  data/INPUT/holded_purchases/raw/holded_purchases_raw.json
# OUTPUT: data/INPUT/holded_purchases/clean/holded_purchases_clean.parquet (default)
#         + CSV also saved for compatibility
# ================================================================

import os
import json
import logging
import pandas as pd
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
# 🔧 TRANSFORMATION FUNCTION
# ============================================

def transform_holded_purchases():
    print("🚩 Starting transform_holded_purchases()")
    logging.info("🚩 Entered transform_holded_purchases()")

    try:
        input_path = "data/INPUT/holded_purchases/raw/holded_purchases_raw.json"
        output_dir = "data/INPUT/holded_purchases/clean"
        base_filename = "holded_purchases_clean"
        os.makedirs(output_dir, exist_ok=True)

        # ✅ Load JSON
        print(f"📥 Loading data from {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        records = raw_data["data"] if isinstance(raw_data, dict) and "data" in raw_data else raw_data
        df = pd.json_normalize(records)

        if df.empty:
            msg = "⚠️ No purchases data found to transform."
            logging.warning(msg)
            print(msg)
            return

        # ✅ Convert relevant Unix timestamps
        timestamp_cols = [col for col in df.columns if col.lower() in ["createdat", "updatedat", "date", "payment_date"]]
        for col in timestamp_cols:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], unit="s", errors="coerce")

        # ✅ Identify and stringify list-type columns
        list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
        if list_columns:
            warning_msg = f"⚠️ List-type columns in purchases data: {list_columns}"
            logging.warning(warning_msg)
            print(warning_msg)
            for col in list_columns:
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

        # ✅ Identify and stringify dict-type columns
        dict_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, dict)).any()]
        if dict_columns:
            warning_msg = f"⚠️ Dict-type columns in purchases data: {dict_columns}"
            logging.warning(warning_msg)
            print(warning_msg)
            for col in dict_columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

        # ✅ Force known problematic fields to string
        for col in ["vatnumber", "supplierId", "customId"]:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # ✅ Save Parquet
        parquet_path = os.path.join(output_dir, base_filename + ".parquet")
        df.to_parquet(parquet_path, index=False)
        logging.info(f"✅ Parquet saved to: {parquet_path}")
        print(f"✅ Parquet saved to: {parquet_path}")

        # ✅ Save CSV
        csv_path = os.path.join(output_dir, base_filename + ".csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"✅ CSV saved to: {csv_path}")
        print(f"✅ CSV saved to: {csv_path}")

        logging.info(f"✅ Total cleaned Holded purchases: {len(df)}")
        print(f"✅ Total cleaned Holded purchases: {len(df)}")

    except Exception as e:
        logging.error(f"❌ Failed to transform Holded purchases data: {e}", exc_info=True)
        print(f"❌ Failed to transform Holded purchases data: {e}")
        sys.exit(1)

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_holded_purchases()



