# Storage Layout Benchmark: CSV vs Parquet (BFT65B Part 1)

Sample evaluated: 122,242 rows x 33 columns across 12 representative quarters.

| Format | Compression | Size_MB | Write_Time_Sec | Read_All_Sec | Read_Subset_Sec | Read_Filter_Sec | Compression_Ratio | Speedup_Subset_vs_CSV |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CSV (Standard) | None | 28.83 | 3.55 | 0.26 | 0.16 | 0.21 | 1.00 | 1.00 |
| Parquet | Snappy | 14.60 | 0.91 | 0.11 | 0.02 | 0.08 | 1.98 | 6.59 |
| Parquet | ZSTD | 12.54 | 1.05 | 0.11 | 0.03 | 0.09 | 2.30 | 5.92 |


### Full Dataset Extrapolation Methodology:
- **Actual Partitioned Parquet (Snappy)**: 236.78 MB (170 partitioned files for 1,682,616 rows).
- **Benchmark Empirical Compression Multiplier**: CSV size / Parquet (Snappy) size = 28.83 MB / 14.60 MB = **1.975x**.
- **Estimated Full Uncompressed CSV**: 236.78 MB × 1.975 = **467.68 MB** (~0.46 GB).
- **Space Savings**: 49.4% reduction in physical disk footprint.
- **Projection Pushdown Advantage**: Projecting 3 columns from Parquet requires only 0.024s vs 0.157s in CSV (**6.59x speedup**).
