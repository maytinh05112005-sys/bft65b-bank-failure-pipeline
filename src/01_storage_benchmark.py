"""
BFT65B - Big Data Analytics in Finance
Part 1: Storage Layout & Format Benchmark (CSV vs. Parquet)
"""

import os

# Set Matplotlib temp dir to prevent warnings BEFORE importing matplotlib
os.environ["MPLCONFIGDIR"] = os.path.join(os.getcwd(), "outputs", ".matplotlib")
os.makedirs(os.environ["MPLCONFIGDIR"], exist_ok=True)

import time
import shutil
import glob
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")
plt.rcParams.update({"font.size": 11, "figure.autolayout": True})

DATA_RAW_FIN = "data_raw/fdic/financials"
OUT_TABLES = "outputs/tables"
OUT_FIGURES = "outputs/figures"
BENCH_DIR = "data_raw/benchmark_temp"

os.makedirs(OUT_TABLES, exist_ok=True)
os.makedirs(OUT_FIGURES, exist_ok=True)
os.makedirs(BENCH_DIR, exist_ok=True)


def run_benchmark():
    print("=== Part 1: Acquisition & Storage Layout Benchmark ===")
    
    # 1. Load a substantial representative sample (12 quarters across banking crises)
    # This represents ~122,242 rows with all 33 fields.
    crisis_quarters = [
        "19881231", "19891231", "19901231", "19911231",
        "20071231", "20080630", "20081231", "20090630", "20091231", "20100630",
        "20221231", "20230630"
    ]
    files = [os.path.join(DATA_RAW_FIN, f"REPDTE={q}.parquet") for q in crisis_quarters]
    files = [f for f in files if os.path.exists(f)]
    
    print(f"Loading {len(files)} quarters for storage benchmarking...")
    # Load quarters and homogenize column datatypes to float64 for numeric columns to solve FDIC schema drift
    dfs = [pd.read_parquet(f) for f in files]
    df_raw = pd.concat(dfs, ignore_index=True)
    # Ensure standard numeric dtypes
    for col in df_raw.columns:
        if col not in ["CERT", "REPDTE", "STNAME"]:
            df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")
    n_rows, n_cols = df_raw.shape
    print(f"Benchmark sample size: {n_rows:,} rows, {n_cols} columns")

    csv_path = os.path.join(BENCH_DIR, "sample_financials.csv")
    parquet_snappy_path = os.path.join(BENCH_DIR, "sample_financials_snappy.parquet")
    parquet_zstd_path = os.path.join(BENCH_DIR, "sample_financials_zstd.parquet")

    results = []

    # --- Benchmark: CSV ---
    print("\nBenchmarking CSV...")
    t0 = time.perf_counter()
    df_raw.to_csv(csv_path, index=False)
    csv_write_time = time.perf_counter() - t0
    csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)

    # Read All Columns (Pandas)
    t0 = time.perf_counter()
    _ = pd.read_csv(csv_path)
    csv_read_all_time = time.perf_counter() - t0

    # Read Selected Columns (['CERT', 'REPDTE', 'ASSET'])
    t0 = time.perf_counter()
    _ = pd.read_csv(csv_path, usecols=["CERT", "REPDTE", "ASSET"])
    csv_read_subset_time = time.perf_counter() - t0

    # Read with Filter (ASSET > 1,000,000)
    t0 = time.perf_counter()
    _ = pd.read_csv(csv_path).query("ASSET > 1000000")
    csv_filter_time = time.perf_counter() - t0

    results.append({
        "Format": "CSV (Standard)",
        "Compression": "None",
        "Size_MB": csv_size_mb,
        "Write_Time_Sec": csv_write_time,
        "Read_All_Sec": csv_read_all_time,
        "Read_Subset_Sec": csv_read_subset_time,
        "Read_Filter_Sec": csv_filter_time,
    })

    # --- Benchmark: Parquet (Snappy) ---
    print("Benchmarking Parquet (Snappy)...")
    t0 = time.perf_counter()
    df_raw.to_parquet(parquet_snappy_path, compression="snappy", index=False)
    pq_snappy_write_time = time.perf_counter() - t0
    pq_snappy_size_mb = os.path.getsize(parquet_snappy_path) / (1024 * 1024)

    t0 = time.perf_counter()
    _ = pd.read_parquet(parquet_snappy_path)
    pq_snappy_read_all_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    _ = pd.read_parquet(parquet_snappy_path, columns=["CERT", "REPDTE", "ASSET"])
    pq_snappy_read_subset_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    _ = pd.read_parquet(parquet_snappy_path, filters=[("ASSET", ">", 1000000)])
    pq_snappy_filter_time = time.perf_counter() - t0

    results.append({
        "Format": "Parquet",
        "Compression": "Snappy",
        "Size_MB": pq_snappy_size_mb,
        "Write_Time_Sec": pq_snappy_write_time,
        "Read_All_Sec": pq_snappy_read_all_time,
        "Read_Subset_Sec": pq_snappy_read_subset_time,
        "Read_Filter_Sec": pq_snappy_filter_time,
    })

    # --- Benchmark: Parquet (ZSTD) ---
    print("Benchmarking Parquet (ZSTD)...")
    t0 = time.perf_counter()
    df_raw.to_parquet(parquet_zstd_path, compression="zstd", index=False)
    pq_zstd_write_time = time.perf_counter() - t0
    pq_zstd_size_mb = os.path.getsize(parquet_zstd_path) / (1024 * 1024)

    t0 = time.perf_counter()
    _ = pd.read_parquet(parquet_zstd_path)
    pq_zstd_read_all_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    _ = pd.read_parquet(parquet_zstd_path, columns=["CERT", "REPDTE", "ASSET"])
    pq_zstd_read_subset_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    _ = pd.read_parquet(parquet_zstd_path, filters=[("ASSET", ">", 1000000)])
    pq_zstd_filter_time = time.perf_counter() - t0

    results.append({
        "Format": "Parquet",
        "Compression": "ZSTD",
        "Size_MB": pq_zstd_size_mb,
        "Write_Time_Sec": pq_zstd_write_time,
        "Read_All_Sec": pq_zstd_read_all_time,
        "Read_Subset_Sec": pq_zstd_read_subset_time,
        "Read_Filter_Sec": pq_zstd_filter_time,
    })

    # Cleanup temp benchmark folder
    shutil.rmtree(BENCH_DIR)

    # --- Full dataset statistics ---
    all_pq_files = glob.glob(os.path.join(DATA_RAW_FIN, "*.parquet"))
    total_pq_size_mb = sum(os.path.getsize(f) for f in all_pq_files) / (1024 * 1024)
    # Estimate full dataset CSV size based on empirical benchmark ratio
    ratio_csv_to_pq = csv_size_mb / pq_snappy_size_mb
    estimated_full_csv_mb = total_pq_size_mb * ratio_csv_to_pq

    df_bench = pd.DataFrame(results)
    df_bench["Compression_Ratio"] = (csv_size_mb / df_bench["Size_MB"]).round(2)
    df_bench["Speedup_Subset_vs_CSV"] = (csv_read_subset_time / df_bench["Read_Subset_Sec"]).round(2)

    # Save Tables
    csv_out = os.path.join(OUT_TABLES, "part1_storage_benchmark.csv")
    md_out = os.path.join(OUT_TABLES, "part1_storage_benchmark.md")
    df_bench.to_csv(csv_out, index=False)
    
    with open(md_out, "w", encoding="utf-8") as f:
        f.write("# Storage Layout Benchmark: CSV vs Parquet (BFT65B Part 1)\n\n")
        f.write(f"Sample evaluated: {n_rows:,} rows x {n_cols} columns across 12 representative quarters.\n\n")
        cols = list(df_bench.columns)
        header = "| " + " | ".join(cols) + " |\n"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |\n"
        rows_str = ""
        for _, row in df_bench.iterrows():
            row_vals = [f"{val:.2f}" if isinstance(val, (float, int)) and not isinstance(val, bool) and col != "Format" else str(val) for col, val in zip(cols, row)]
            rows_str += "| " + " | ".join(row_vals) + " |\n"
        f.write(header + sep + rows_str)
        f.write("\n\n### Full Dataset Extrapolation Methodology:\n")
        f.write(f"- **Actual Partitioned Parquet (Snappy)**: {total_pq_size_mb:.2f} MB ({len(all_pq_files)} partitioned files for 1,682,616 rows).\n")
        f.write(f"- **Benchmark Empirical Compression Multiplier**: CSV size / Parquet (Snappy) size = {csv_size_mb:.2f} MB / {pq_snappy_size_mb:.2f} MB = **{ratio_csv_to_pq:.3f}x**.\n")
        f.write(f"- **Estimated Full Uncompressed CSV**: {total_pq_size_mb:.2f} MB × {ratio_csv_to_pq:.3f} = **{estimated_full_csv_mb:.2f} MB** (~{estimated_full_csv_mb/1024:.2f} GB).\n")
        f.write(f"- **Space Savings**: {((1 - total_pq_size_mb / estimated_full_csv_mb) * 100):.1f}% reduction in physical disk footprint.\n")
        f.write(f"- **Projection Pushdown Advantage**: Projecting 3 columns from Parquet requires only {pq_snappy_read_subset_time:.3f}s vs {csv_read_subset_time:.3f}s in CSV (**{df_bench.loc[df_bench['Compression']=='Snappy', 'Speedup_Subset_vs_CSV'].values[0]}x speedup**).\n")

    print("\nBenchmark Results:")
    print(df_bench.to_string(index=False))
    print(f"\nFull Dataset: Actual Parquet = {total_pq_size_mb:.2f} MB | Estimated CSV = {estimated_full_csv_mb:.2f} MB")

    # --- Plot Visualization ---
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    # 1. Disk Size
    sns.barplot(data=df_bench, x="Format", y="Size_MB", hue="Compression", ax=axes[0], palette="Blues_r")
    axes[0].set_title("Disk Storage Footprint (MB) - Lower is Better", fontweight="bold")
    axes[0].set_ylabel("File Size (MB)")
    for p in axes[0].patches:
        height = p.get_height()
        if height > 0:
            axes[0].annotate(f"{height:.1f} MB", (p.get_x() + p.get_width() / 2., height),
                             ha='center', va='bottom', fontsize=9, xytext=(0, 3), textcoords='offset points')

    # 2. Read Performance
    read_df = df_bench.melt(
        id_vars=["Format", "Compression"],
        value_vars=["Read_All_Sec", "Read_Subset_Sec", "Read_Filter_Sec"],
        var_name="Query_Type", value_name="Time_Sec"
    )
    read_df["Label"] = read_df["Format"] + " (" + read_df["Compression"] + ")"
    sns.barplot(data=read_df, x="Query_Type", y="Time_Sec", hue="Label", ax=axes[1], palette="Set2")
    axes[1].set_title("Query Read Latency (Seconds) - Lower is Better", fontweight="bold")
    axes[1].set_ylabel("Execution Time (s)")
    axes[1].set_xticks(range(3))
    axes[1].set_xticklabels(["Scan All Cols", "Project 3 Cols", "Predicate Filter"])

    # 3. Write Time
    sns.barplot(data=df_bench, x="Format", y="Write_Time_Sec", hue="Compression", ax=axes[2], palette="Greens_r")
    axes[2].set_title("Write & Serialization Time (Seconds)", fontweight="bold")
    axes[2].set_ylabel("Write Time (s)")
    for p in axes[2].patches:
        height = p.get_height()
        if height > 0:
            axes[2].annotate(f"{height:.2f}s", (p.get_x() + p.get_width() / 2., height),
                             ha='center', va='bottom', fontsize=9, xytext=(0, 3), textcoords='offset points')

    plt.suptitle("BFT65B Final Report - Part 1: Storage Layout Architecture Benchmark", fontsize=14, fontweight="bold", y=1.03)
    fig_path = os.path.join(OUT_FIGURES, "part1_storage_comparison.png")
    plt.savefig(fig_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"\nSaved benchmark plot to {fig_path}")
    print(f"Saved benchmark table to {md_out}")


if __name__ == "__main__":
    run_benchmark()
