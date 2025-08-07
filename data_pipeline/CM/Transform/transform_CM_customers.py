# ================================================================
# CHARTMOGUL CUSTOMERS TRANSFORM SCRIPT
# ================================================================
# Transforms ChartMogul customer data into a clean tabular format.
# INPUT:  data/INPUT/chartmogul_customers/raw/chartmogul_customers_raw.json
# OUTPUT: data/INPUT/chartmogul_customers/clean/chartmogul_customers_clean.parquet
#         + CSV also saved for compatibility
# ================================================================

import os
import json
import logging
import pandas as pd
import sys

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

def transform_chartmogul_customers():
    try:
        input_path = "data/INPUT/chartmogul_customers/raw/chartmogul_customers_raw.json"
        output_dir = "data/INPUT/chartmogul_customers/clean"
        base_filename = "chartmogul_customers_clean"
        os.makedirs(output_dir, exist_ok=True)

        print(f"Reading raw JSON from {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # Extract list of customers
        customers = raw_data.get("customers", raw_data.get("entries", []))
        df = pd.json_normalize(customers)

        if df.empty:
            msg = "Empty ChartMogul customer data."
            logging.warning(msg)
            print(msg)
            return

        # Parse timestamp fields if present
        timestamp_cols = [col for col in df.columns if "date" in col.lower() or "created_at" in col.lower()]
        for col in timestamp_cols:
            if col in df.columns and pd.api.types.is_object_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Detect and stringify list-type columns
        list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
        if list_columns:
            logging.warning(f"List-type columns in ChartMogul customers: {list_columns}")
            for col in list_columns:
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

        # Detect and stringify dict-type columns
        dict_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, dict)).any()]
        if dict_columns:
            logging.warning(f"Dict-type columns in ChartMogul customers: {dict_columns}")
            for col in dict_columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

        # Convert key ID columns to string (avoid pyarrow crashes)
        for col in ["uuid", "external_id", "company", "country"]:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Save to Parquet with fallback
        parquet_path = os.path.join(output_dir, base_filename + ".parquet")
        save_parquet(df, parquet_path)
        logging.info(f"Parquet saved to: {parquet_path}")

        # Save to CSV
        csv_path = os.path.join(output_dir, base_filename + ".csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"CSV saved to: {csv_path}")

        print(f"Cleaned data saved:\n  - {csv_path}\n  - {parquet_path}")
        logging.info(f"Total cleaned ChartMogul customers: {len(df)}")

    except Exception as e:
        logging.error(f"Error transforming ChartMogul customers: {e}", exc_info=True)
        print(f"Error transforming ChartMogul customers: {e}")
        sys.exit(1)

# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_chartmogul_customers()

