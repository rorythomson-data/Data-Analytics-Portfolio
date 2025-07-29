# ================================================================
# 📌 HOLDED EXPENSES TRANSFORM SCRIPT – FINAL VERSION
# ================================================================
# This script loads the raw Holded expenses JSON, normalizes it
# into tabular format, handles list-type and fragile fields, and
# saves the cleaned data as CSV and Parquet in the clean folder.
#
# 🔹 INPUT:
#     - data/INPUT/holded_expenses/raw/holded_expenses_raw.json
#
# 🔹 OUTPUT:
#     - data/INPUT/holded_expenses/clean/
#         • holded_expenses_clean.csv
#         • holded_expenses_clean.parquet
#
# 🔹 Features:
#     - Normalizes JSON structure using pandas
#     - Detects and stringifies nested list-type columns
#     - Casts fragile fields to string
#     - Logs success/failure at each step
# ================================================================

import os
import json
import logging
import pandas as pd

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
# 🔧 TRANSFORMATION FUNCTION
# ============================================

def transform_holded_expenses():
    logging.info("🚩 Entered transform_holded_expenses()")
    
    try:
        input_path = "data/INPUT/holded_expenses/raw/holded_expenses_raw.json"
        output_dir = "data/INPUT/holded_expenses/clean"
        base_filename = "holded_expenses_clean"
        os.makedirs(output_dir, exist_ok=True)

        # ✅ Load raw JSON
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        records = raw_data["data"] if isinstance(raw_data, dict) and "data" in raw_data else raw_data
        df = pd.json_normalize(records)

        if df.empty:
            logging.warning("⚠️ Empty Holded expenses data.")
            return

        # ✅ Detect and stringify list-type columns
        list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
        if list_columns:
            logging.warning(f"⚠️ List-type columns in Holded expenses: {list_columns}")
            for col in list_columns:
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

        # ✅ Force problematic fields to string
        for col in ["vatnumber"]:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # ✅ Save to CSV
        csv_path = os.path.join(output_dir, base_filename + ".csv")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logging.info(f"✅ CSV saved to: {csv_path}")

        # ✅ Save to Parquet
        parquet_path = os.path.join(output_dir, base_filename + ".parquet")
        df.to_parquet(parquet_path, index=False)
        logging.info(f"✅ Parquet saved to: {parquet_path}")

        logging.info(f"✅ Total cleaned Holded expenses: {len(df)}")

    except Exception as e:
        logging.error(f"❌ Error transforming Holded expenses: {e}", exc_info=True)
        raise

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_holded_expenses()
