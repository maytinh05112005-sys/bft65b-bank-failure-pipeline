"""
BFT65B - Big Data Analytics in Finance
Final Examination: Master End-to-End Pipeline Execution Script
"""

import os
import sys
import time
import subprocess

STEPS = [
    ("Part 1: Storage Layout & Format Benchmark (CSV vs Parquet)", "src/01_storage_benchmark.py"),
    ("Part 2: Data Quality Audit & Clean Point-in-Time Panel", "src/02_data_quality_and_panel.py"),
    ("Part 3: Scalable Processing & Engine Architecture Benchmark", "src/03_scalable_processing.py"),
    ("Part 4: Leak-Free Feature Pipeline & CAMELS Banking Risk Architecture", "src/04_feature_pipeline.py"),
    ("Part 5: Early-Warning Modelling, Spark MLlib Demo & Validation", "src/05_modelling_validation.py"),
]


def main():
    print("=" * 80)
    print(" BFT65B - BIG DATA ANALYTICS IN FINANCE: END-TO-END REPRODUCIBILITY PIPELINE")
    print("=" * 80)
    t_global_start = time.time()

    # Verify raw data presence
    if not os.path.exists("data_raw/fdic/financials") or not os.path.exists("data_raw/fdic/failures.parquet"):
        print("\n[ERROR] Raw FDIC data not found in data_raw/fdic/.")
        print("Please run fdic_download_starter.py first or verify your download.")
        sys.exit(1)

    print(f"\n[INFO] Starting execution of {len(STEPS)} pipeline modules...\n")

    for i, (name, script) in enumerate(STEPS, 1):
        print("-" * 80)
        print(f"[{i}/{len(STEPS)}] Launching: {name}")
        print(f"Script: {script}")
        print("-" * 80)
        t0 = time.time()

        res = subprocess.run([sys.executable, script])
        if res.returncode != 0:
            print(f"\n[FAILED] Error occurred while executing {script} (Exit Code: {res.returncode})")
            sys.exit(res.returncode)

        elapsed = time.time() - t0
        print(f"[SUCCESS] {name} completed in {elapsed:.2f} seconds.\n")

    total_time = time.time() - t_global_start
    print("=" * 80)
    print(f" ALL PIPELINE STAGES COMPLETED SUCCESSFULLY IN {total_time:.2f} SECONDS (~{total_time/60:.1f} minutes)!")
    print(" All tables, figures, and reports are updated in outputs/")
    print("=" * 80)


if __name__ == "__main__":
    main()
