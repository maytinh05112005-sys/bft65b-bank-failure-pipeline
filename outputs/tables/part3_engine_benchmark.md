# Part 3: Scalable Engine Benchmark Table (Polars vs. Pandas vs. PySpark)

Evaluated on full FDIC panel: 1,682,616 rows x 33+ columns across 170 quarters.

| Engine | Execution_Mode | Execution_Time_Sec | Speedup_vs_Pandas | Peak_Process_RSS_MB | Net_RAM_Growth_MB | Memory_Profiling_Method | Scalability_Limit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Pandas (Baseline) | Eager / Single-Threaded | 10.72s | 1.0x | 2519.3 MB | 799.6 MB | psutil Process RSS (OS Resident) | ~5-10M rows on 16GB Laptop |
| Polars (Lazy) | Lazy / Vectorized Multi-Threaded (Rust) | 4.427s | 2.42x | 1988.1 MB | 1790.7 MB | psutil Process RSS (OS Resident) | ~50-100M rows on Laptop (Streaming) |
| PySpark (Local) | Distributed Catalyst / Local Multi-core | 73.04s | 0.15x | 1202.5 MB | 1202.5 MB | psutil Process RSS (Driver + JVM) | Billions of rows (Distributed Cluster) |
