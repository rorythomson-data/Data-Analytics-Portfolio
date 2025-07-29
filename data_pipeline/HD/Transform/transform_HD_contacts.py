# ================================================================
# 📌 HOLDED CONTACTS TRANSFORM SCRIPT 
# ================================================================
# This script loads the raw JSON from the Holded contacts API,
# cleans and flattens nested or complex fields, and saves the
# cleaned output in both CSV and Parquet formats.
#
# 🔹 INPUT:
#     - data/INPUT/holded_contacts/raw/holded_contacts_raw.json
#
# 🔹 OUTPUT:
#     - data/INPUT/holded_contacts/clean/holded_contacts_clean.csv
#     - data/INPUT/holded_contacts/clean/holded_contacts_clean.parquet
#
# 🔹 Features:
#     - Handles nested lists/dicts by stringifying
#     - Converts fragile columns to string to avoid pyarrow issues
#     - Logs success and error messages for auditing
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

def transform_holded_contacts():
    print("🚩 Starting transform_holded_contacts()")
    logging.info("🚩 Entered transform_holded_contacts()")

    input_path = "data/INPUT/holded_contacts/raw/holded_contacts_raw.json"
    output_dir = "data/INPUT/holded_contacts/clean"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # ✅ Load raw JSON
        print(f"📥 Loading raw data from {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        df = pd.DataFrame(raw_data)
        if df.empty:
            msg = "⚠️ No data to transform in Holded contacts."
            logging.warning(msg)
            print(msg)
            return

        # ✅ Normalize list/dict columns by stringifying them
        complex_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (dict, list))).any()]
        for col in complex_cols:
            logging.warning(f"⚠️ Coercing complex type column: {col}")
            print(f"⚠️ Coercing complex type column: {col}")
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

        # ✅ Fix dtype issues (cast fragile fields to string)
        string_cols = [
            "id", "customId", "clientRecord", "supplierRecord",
            "vatnumber", "type", "groupId"
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # ✅ Save cleaned data as CSV
        csv_path = os.path.join(output_dir, "holded_contacts_clean.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logging.info(f"✅ CSV saved to: {csv_path}")
        print(f"✅ CSV saved to: {csv_path}")

        # ✅ Save cleaned data as Parquet
        parquet_path = os.path.join(output_dir, "holded_contacts_clean.parquet")
        df.to_parquet(parquet_path, index=False)
        logging.info(f"✅ Parquet saved to: {parquet_path}")
        print(f"✅ Parquet saved to: {parquet_path}")

        logging.info(f"✅ Total cleaned Holded contacts: {len(df)}")
        print(f"✅ Total cleaned Holded contacts: {len(df)}")

    except Exception as e:
        logging.error(f"❌ Error transforming Holded contacts: {e}", exc_info=True)
        print(f"❌ Error transforming Holded contacts: {e}")
        sys.exit(1)

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_holded_contacts()

