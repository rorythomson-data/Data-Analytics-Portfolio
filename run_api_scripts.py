# ================================================================
# 📌 PIPELINE ORCHESTRATOR SCRIPT - CONTEXT & DESIGN
# ================================================================
# This script runs all data extraction and transformation steps for:
#   - ChartMogul (Customer, Metrics, Plans, MRR Components)
#   - Holded (Invoices, Payments, Contacts, Expenses, Purchases)
#
# 🔹 INPUT:
#     - All extract scripts fetch data via API and save to: data/INPUT/{source}/
#         • Standardized filenames like holded_invoices_raw.json
#
# 🔹 OUTPUT:
#     - All transform scripts clean the raw JSON and save to: data/OUTPUT/{source}/
#         • Standardized filenames like holded_invoices_clean.csv
#
# 🔹 Features:
#     - Modular and centralized script orchestration
#     - Subprocess execution for isolation
#     - Color-coded console feedback and file-based logging
#     - Compatible with automation and scheduling tools (e.g., GitHub Actions, Airflow)
# ================================================================

import subprocess
import logging
import os
import sys
import time

# ================================================================
# 🪵 LOGGING SETUP
# ================================================================

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ANSI color codes for console output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

# ================================================================
# 📂 SCRIPT LIST (ORDERED)
# ================================================================

scripts = [
    # ChartMogul
    "data_pipeline/CM/Extract/extract_CM_customers.py",
    "data_pipeline/CM/Transform/transform_CM_customers.py",
    "data_pipeline/CM/Extract/extract_CM_metrics.py",
    "data_pipeline/CM/Transform/transform_CM_metrics.py",
    "data_pipeline/CM/Extract/extract_CM_plans.py",
    "data_pipeline/CM/Transform/transform_CM_plans.py",
    "data_pipeline/CM/Extract/extract_CM_mrr_components.py",
    "data_pipeline/CM/Transform/transform_CM_mrr_components.py",

    # Holded
    "data_pipeline/HD/Extract/extract_HD_invoices.py",
    "data_pipeline/HD/Transform/transform_HD_invoices.py",
    "data_pipeline/HD/Extract/extract_HD_payments.py",
    "data_pipeline/HD/Transform/transform_HD_payments.py",
    "data_pipeline/HD/Extract/extract_HD_contacts.py",
    "data_pipeline/HD/Transform/transform_HD_contacts.py",
    "data_pipeline/HD/Extract/extract_HD_expenses.py",
    "data_pipeline/HD/Transform/transform_HD_expenses.py",
    "data_pipeline/HD/Extract/extract_HD_purchases.py",
    "data_pipeline/HD/Transform/transform_HD_purchases.py",
]

# ================================================================
# ✅ REQUIRED OUTPUT FILES
# ================================================================

required_files = [
    "data/INPUT/holded_invoices/clean/holded_invoices_clean.csv",
    "data/INPUT/holded_payments/clean/holded_payments_clean.csv",
    "data/INPUT/holded_contacts/clean/holded_contacts_clean.csv",
    "data/INPUT/holded_expenses/clean/holded_expenses_clean.csv",
    "data/INPUT/holded_purchases/clean/holded_purchases_clean.csv",
]

results = []

# ================================================================
# ⚙️ FUNCTION TO RUN EACH SCRIPT
# ================================================================

def run_script(script):
    start_time = time.time()
    if not os.path.exists(script):
        logging.warning(f"⚠️ Script not found: {script}")
        print(f"{YELLOW}⚠️ Script not found: {script}{RESET}")
        results.append((script, "NOT FOUND", 0))
        return

    try:
        logging.info(f"▶ Running {script}")
        print(f"▶ Running {script} ...")

        # UTF-8 safe execution
        result = subprocess.run(
            [sys.executable, script],
            text=True,
            capture_output=True,
            encoding="utf-8",
            errors="replace"  # prevents UnicodeDecodeError
        )

        elapsed = round(time.time() - start_time, 2)
        if result.returncode == 0:
            logging.info(f"✅ Success: {script} ({elapsed}s)")
            print(f"{GREEN}✅ Success: {script} ({elapsed}s){RESET}")
            results.append((script, "SUCCESS", elapsed))
        else:
            logging.error(f"❌ Failed: {script} — {result.stderr}")
            print(f"{RED}❌ Failed: {script} ({elapsed}s){RESET}")
            results.append((script, "FAILED", elapsed))
    except subprocess.CalledProcessError as e:
        elapsed = round(time.time() - start_time, 2)
        logging.error(f"❌ Failed: {script} — {e}")
        print(f"{RED}❌ Failed: {script} ({elapsed}s){RESET}")
        results.append((script, "FAILED", elapsed))

# ================================================================
# 🟢 MAIN EXECUTION
# ================================================================

if __name__ == "__main__":
    logging.info("🚀 Starting full pipeline execution")
    print("🚀 Starting pipeline execution...\n")

    for script in scripts:
        run_script(script)
        # Stop early if a critical script fails
        if results[-1][1] == "FAILED":
            print(f"{RED}❌ Critical failure detected: {script}. Stopping pipeline.{RESET}")
            sys.exit(1)

    # Verify required files
    print("\n🔍 Verifying required files...\n")
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
            print(f"{RED}❌ Missing file: {file_path}{RESET}")
        else:
            print(f"{GREEN}✅ Found: {file_path}{RESET}")

    if missing_files:
        logging.error("Some required files are missing. Pipeline cannot continue.")
        sys.exit(1)

    # 📋 Console Summary
    print("\n📋 Summary:\n")
    for script, status, elapsed in results:
        color = GREEN if status == "SUCCESS" else RED if status == "FAILED" else YELLOW
        print(f"{color}{status:<10}{RESET} — {script} ({elapsed}s)")

    # 📄 Save summary to log file
    with open("logs/pipeline_summary.txt", "w", encoding="utf-8") as f:
        for script, status, elapsed in results:
            f.write(f"{status:<10} — {script} ({elapsed}s)\n")
