# ================================================================
# 📌 CHARTMOGUL METRICS TRANSFORM SCRIPT – FINAL VERSION (FIXED)
# ================================================================
# This script transforms raw ChartMogul monthly metrics data into a
# clean tabular format, preparing it for business intelligence pipelines.
#
# 🔹 INPUT:
#     - Raw JSON: data/INPUT/chartmogul_metrics/raw/chartmogul_metrics_raw.json
#
# 🔹 OUTPUT:
#     - Clean CSV + Parquet: data/INPUT/chartmogul_metrics/clean/
#         • chartmogul_metrics_clean.csv
#         • chartmogul_metrics_clean.parquet
#
# 🔹 Features:
#     - Converts date to standard datetime + adds year/month breakdown
#     - Coerces object-type columns to string for parquet compatibility
#     - Structured logging for debugging and auditing
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
# 🚀 MAIN TRANSFORM FUNCTION
# ============================================

def transform_chartmogul_metrics():
    logging.info("🚩 Starting transform_chartmogul_metrics()")

    try:
        input_path = "data/INPUT/chartmogul_metrics/raw/chartmogul_metrics_raw.json"
        output_dir = "data/INPUT/chartmogul_metrics/clean"
        base_filename = "chartmogul_metrics_clean"

        if not os.path.exists(input_path):
            logging.error("❌ Raw ChartMogul metrics file not found.")
            return

        # ✅ Load raw JSON data
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        entries = raw_data.get("entries", [])
        if not entries:
            logging.warning("⚠️ No entries found in ChartMogul metrics data.")
            return

        df = pd.DataFrame(entries)

        # ========================================
        # 🧼 DATA TRANSFORMATIONS
        # ========================================
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["year"] = df["date"].dt.year
            df["month"] = df["date"].dt.month
            df["month_start"] = df["date"].dt.to_period("M").dt.to_timestamp()

        # Coerce all object columns to strings to avoid pyarrow issues
        object_cols = df.select_dtypes(include=["object"]).columns
        for col in object_cols:
            df[col] = df[col].astype(str).str.strip()

        # ========================================
        # 💾 SAVE OUTPUTS
        # ========================================
        os.makedirs(output_dir, exist_ok=True)

        # Save Parquet
        parquet_path = os.path.join(output_dir, f"{base_filename}.parquet")
        df.to_parquet(parquet_path, index=False)
        logging.info(f"✅ Saved Parquet to {parquet_path}")

        # Save CSV
        csv_path = os.path.join(output_dir, f"{base_filename}.csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"✅ Saved CSV to {csv_path}")

        logging.info(f"✅ Transformation completed: {len(df)} rows processed.")

    except Exception as e:
        logging.error(f"❌ Failed to transform ChartMogul metrics: {e}", exc_info=True)
        raise

# ============================================
# 🟢 ENTRY POINT
# ============================================

if __name__ == "__main__":
    transform_chartmogul_metrics()
