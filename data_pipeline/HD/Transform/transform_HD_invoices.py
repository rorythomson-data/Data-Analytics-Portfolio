# ================================================================
# HOLDED INVOICES TRANSFORM SCRIPT 
# ================================================================
# This script transforms Holded invoices raw JSON into a clean,
# flattened tabular format for downstream analysis.
#
# 🔹 INPUT:
#     - data/INPUT/holded_invoices/raw/holded_invoices_raw.json
#
# 🔹 OUTPUT:
#     - data/INPUT/holded_invoices/clean/
#         • holded_invoices_clean.parquet
#         • holded_invoices_clean.csv
#         • holded_invoices_clean.json (optional debug toggle)
#
# 🔹 Features:
#     - Flatten nested fields
#     - Timestamp conversion
#     - Clean list/dict-type columns
#     - Force fragile columns to string for compatibility
# ================================================================

import os
import json
import logging
import pandas as pd
import sys

# ============================================
# TOGGLE JSON EXPORT (for debugging only)
# ============================================
SAVE_JSON = False

# ============================================
# LOGGING SETUP
# ============================================

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================
# SAVE PARQUET WITH FALLBACK
# ============================================

def save_parquet(df, path):
    """
    Save DataFrame as Parquet, preferring pyarrow but falling back to fastparquet.
    """
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except ImportError:
        print("pyarrow not found, falling back to fastparquet.")
        logging.warning("pyarrow not found, falling back to fastparquet.")
        df.to_parquet(path, index=False, engine="fastparquet")

# ============================================
# TRANSFORMATION FUNCTION
# ============================================

def transform_holded_invoices():
    print("Starting transform_holded_invoices()")
    logging.info("Entered transform_holded_invoices()")

    try:
        input_path = "data/INPUT/holded_invoices/raw/holded_invoices_raw.json"
        output_dir = "data/INPUT/holded_invoices/clean"
        base_filename = "holded_invoices_clean"
        os.makedirs(output_dir, exist_ok=True)

        # Load raw JSON
        print(f"Loading data from {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        records = raw_data["data"] if isinstance(raw_data, dict) and "data" in raw_data else raw_data
        df = pd.json_normalize(records)

        if df.empty:
            msg = "No invoice data found (empty DataFrame)."
            logging.warning(msg)
            print(msg)
            return

        # Convert UNIX timestamps to datetime
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], unit="s", errors="coerce")

        # Detect and stringify list-type columns
        list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
        if list_columns:
            logging.warning(f"List-type columns in Holded invoices: {list_columns}")
            print(f"List-type columns in Holded invoices: {list_columns}")
            for col in list_columns:
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

        # Detect and stringify dict-type columns
        dict_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, dict)).any()]
        if dict_columns:
            logging.warning(f"Dict-type columns in Holded invoices: {dict_columns}")
            print(f"Dict-type columns in Holded invoices: {dict_columns}")
            for col in dict_columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

        # Coerce fragile columns to string (Parquet compatibility)
        force_string_columns = [
            "customId", "vatnumber", "type",
            "clientRecord", "supplierRecord", "groupId"
        ]
        for col in force_string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Save as Parquet (with fallback)
        parquet_path = os.path.join(output_dir, base_filename + ".parquet")
        save_parquet(df, parquet_path)
        logging.info(f"Parquet saved to: {parquet_path}")
        print(f"Parquet saved to: {parquet_path}")

        # Save as CSV
        csv_path = os.path.join(output_dir, base_filename + ".csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"CSV saved to: {csv_path}")
        print(f"CSV saved to: {csv_path}")

        # Optional: Save as JSON
        if SAVE_JSON:
            try:
                json_path = os.path.join(output_dir, base_filename + ".json")
                df.to_json(json_path, orient="records", indent=2)
                logging.info(f"JSON saved to: {json_path}")
                print(f"JSON saved to: {json_path}")
            except Exception as e:
                logging.warning(f"Could not save JSON: {e}")
                print(f"Could not save JSON: {e}")

        logging.info(f"Total invoice records cleaned: {len(df)}")
        print(f"Total invoice records cleaned: {len(df)}")

    except Exception as e:
        logging.error(f"Error transforming Holded invoices: {e}", exc_info=True)
        print(f"Error transforming Holded invoices: {e}")
        sys.exit(1)

# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_holded_invoices()

