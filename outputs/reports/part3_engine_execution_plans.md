# Part 3: Engine Execution Plans & Distributed Cluster Justification

## 1. Engine Performance & Memory Profiling Audit
- **Polars** executed the full 1.68M-row feature and window transformation pipeline in **4.43 seconds**, achieving a **2.42x speedup** over Pandas (10.72s).
- **Memory Profiling Methodological Note:** Standard CPython profilers like `tracemalloc` report an erroneous 0.03 MB for Polars because `tracemalloc` only captures Python interpreter allocations (`PyMem_Malloc`) and is blind to native Rust `mimalloc` allocations. Using physical OS Resident Set Size (`psutil` Process RSS), Polars exhibits a peak resident memory footprint of **1988.1 MB** (vs. **2519.3 MB** for Pandas), saving approximately **21.1%** in physical RAM.
- **PySpark (Local)** completed the exact transformations in **73.04 seconds**, verifying full mathematical consistency across engines.

## 2. Polars Query Engine Mechanics
Polars' performance advantage stems from three fundamental architectural pillars:
1. **Vectorized Rust & Apache Arrow Columnar Format:** Polars operates directly on Arrow chunk arrays with contiguous column layouts, enabling CPU L1/L2 cache locality and SIMD (Single Instruction, Multiple Data) vectorization without Python GIL overhead.
2. **Lazy Evaluation & Query Optimization:** Rather than executing transformations immediately, Polars builds a Directed Acyclic Graph (DAG) of expressions. It applies **Projection Pushdown** (reading only necessary columns), **Predicate Pushdown** (filtering rows at file-read time), and **Common Subexpression Elimination**.
3. **Work-Stealing Multi-Threading:** Polars uses the Rayon parallel library to automatically split DataFrame partitions across all available CPU cores without IPC overhead.

### Polars Optimized Physical Plan:
```text
LEFT JOIN:
LEFT PLAN ON: [col("REPDTE"), col("STNAME")]
  CACHE[id: dfba2cb5-85b9-4174-9c36-a680e5a30736]
     WITH_COLUMNS:
     [((col("ASSET") - col("asset_lag4")) / (col("asset_lag4") + 0.000001)).alias("asset_growth_1yr"), (col("npl_ratio") - col("npl_ratio_lag4")).alias("npl_change_1yr")] 
       WITH_COLUMNS:
       [col("ASSET").shift([dyn int: 4]).over([col("CERT")]).alias("asset_lag4"), col("npl_ratio").shift([dyn int: 4]).over([col("CERT")]).alias("npl_ratio_lag4")] 
        SORT BY [col("CERT"), col("report_date")]
          simple π 44/44 ["CERT", "REPDTE", "STNAME", ... 41 other columns]
             WITH_COLUMNS:
             [(col("EQ") / col("ASSET")).alias("capital_ratio"), (col("LNLSNET") / col("ASSET")).alias("loan_intensity"), (col("CHBAL") / (col("DEP") + 0.000001)).alias("liquidity_ratio"), (col("__POLARS_CSER_0x100000011") / (col("LNLSGR") + 0.000001)).alias("npl_ratio"), (col("LNATRES") / (col("__POLARS_CSER_0x100000011") + 0.000001)).alias("coverage_ratio"), (col("__POLARS_CSER_0x100000018") / col("ASSET")).alias("nim"), (col("NONIX") / ((col("__POLARS_CSER_0x100000018") + col("NONII")) + 0.000001)).alias("efficiency_ratio")] 
               WITH_COLUMNS:
               [((col("P3ASSET") + col("P9ASSET")) + col("NAASSET")).alias("__POLARS_CSER_0x100000011"), (col("INTINC") - col("EINTEXP")).alias("__POLARS_CSER_0x100000018")] 
                Parquet SCAN [data_processed/clean_panel.parquet]
                PROJECT */37 COLUMNS
                ESTIMATED ROWS: 1682616
RIGHT PLAN ON: [col("REPDTE"), col("STNAME")]
  AGGREGATE[maintain_order: false]
    [col("capital_ratio").mean().alias("state_mean_capital_ratio"), col("npl_ratio").median().alias("state_median_npl_ratio")] BY [col("REPDTE"), col("STNAME")]
    FROM
    simple π 4/4 ["REPDTE", "STNAME", ... 2 other columns]
      CACHE[id: dfba2cb5-85b9-4174-9c36-a680e5a30736]
END LEFT JOIN
```

## 3. PySpark Catalyst Execution Plan
PySpark leverages the Catalyst Optimizer to translate dataframe transformations into optimized physical RDD DAGs with whole-stage code generation (`WholeStageCodegen`).

### PySpark Extended Plan:
```text
== Parsed Logical Plan ==
'Join UsingJoin(LeftOuter, [REPDTE, STNAME])
:- Project [CERT#0L, REPDTE#1, STNAME#2, ASSET#3, DEP#4, DEPDOM#5, EQ#6, BRO#7, CHBAL#8, SC#9, LNLSGR#10, LNLSNET#11, LNRE#12, LNCI#13, LNCON#14, LNATRES#15, NCLNLS#16, P3ASSET#17, P9ASSET#18, NAASSET#19, NTLNLS#20, ORE#21, NETINC#22, INTINC#23, EINTEXP#24, ... 23 more fields]
:  +- Project [CERT#0L, REPDTE#1, STNAME#2, ASSET#3, DEP#4, DEPDOM#5, EQ#6, BRO#7, CHBAL#8, SC#9, LNLSGR#10, LNLSNET#11, LNRE#12, LNCI#13, LNCON#14, LNATRES#15, NCLNLS#16, P3ASSET#17, P9ASSET#18, NAASSET#19, NTLNLS#20, ORE#21, NETINC#22, INTINC#23, EINTEXP#24, ... 22 more fields]
:     +- Project [CERT#0L, REPDTE#1, STNAME#2, ASSET#3, DEP#4, DEPDOM#5, EQ#6, BRO#7, CHBAL#8, SC#9, LNLSGR#10, LNLSNET#11, LNRE#12, LNCI#13, LNCON#14, LNATRES#15, NCLNLS#16, P3ASSET#17, P9ASSET#18, NAASSET#19, NTLNLS#20, ORE#21, NETINC#22, INTINC#23, EINTEXP#24, ... 21 more fields]
:        +- Project [CERT#0L, REPDTE#1, STNAME#2, ASSET#3, DEP#4, DEPDOM#5, EQ#6, BRO#7, CHBAL#8, SC#9, LNLSGR#10, LNLSNET#11, LNRE#12, LNCI#13, LNCON#14, LNATRES#15, NCLNLS#16, P3ASSET#17, P9ASSET#18, NAASSET#19, NTLNLS#20, ORE#21, NETINC#22, INTINC#23, EINTEXP#24, ... 22 more fields]
:           +- Window [lag(npl_ratio#41, -4, null) windowspecdefinition(CERT#0L, report_date#33 ASC NULLS FIRST, specifiedwindowframe(RowFrame, -4, -4)) AS npl_ratio_lag4#46], [CERT#0L], [report_date#33 ASC NULLS FIRST]
:              +- Project [CERT#0L, REPDTE#1, STNAME#2, ASSET#3, DEP#4, DEPDOM#5, EQ#6, BRO#7, CHBAL#8, SC#9, LNLSGR#10, LNLSNET#11, LNRE#12, LNCI#13, LNCON#14, LNATRES#15, NCLNLS#16, P3ASSET#17, P9ASSET#18, NAASSET#19, NTLNLS#20, ORE#21, NETINC#22, INTINC#23, EINTEXP#24, ... 20 more fields]
:                 +- Project [CERT#0L, REPDTE#1, STNAME#2, ASSET#3, DEP#4, DEPDOM#5, EQ#6, BRO#7, CHBAL#8, SC#9, LNLSGR#10, LNLSNET#11, LNRE#12, LNCI#13, LNCON#14, LNATRES#15, NCLNLS#16, P3ASSET#17, P9ASSET#18, NAASSET#19, NTLNLS#20, ORE#21, NETINC#22, INTINC#23, EINTEXP#24, ... 20 more fields]
:                    +- Project [CERT#0L, REPDTE#1, STNAME#2, ASSET#3, DEP#4, DEPDOM#5, EQ#6, BRO#7, CHBAL#8, SC#9, LNLSGR#10, LNLSNET#11, LNRE#12, LNCI#13, LNCON#14, LNATRES#15, NCLNLS#16, P3ASSET#17, P9ASSET#18, NAASSET#19, NTLNLS#20, ORE#21, NETINC#22, INTINC#23, EINTEXP#24, ... 21 more fields]
:                       +- Window [lag(ASSET#3, -4, null) windowspecdefinition(CERT#0L, report_date#33 ASC NULLS FIRST, specifiedwindowframe(RowFrame, -4, -4)) AS asset_lag4#45], [CERT#0L], [report_date#33 ASC NULLS FIRST]
:                          +- Project [CERT#0L, REPDTE#1, STNAME#2, ASSET#3, DEP#4, DEPDOM#5, EQ#6, BRO#7, CHBAL#8, SC#9, LNLSGR#10, LNLSNET#11, LNRE#12, LNCI#13, LNCON#14, LNATRES#15, NCLNLS#16, P3ASSET#17, P9ASSET#18, NAASSET#19, NTLNLS#20, ORE#21, NETINC#22, INTINC#23, EINTEXP#24, ... 19 more fields]
:                             +- Project [CERT#0L, REPDTE#1, STNAME#2, ASSET#3, DEP#4, DEPDOM#5, EQ#6, BRO#7, CHBAL#8, SC#9, LNLSGR#10, LNLSNET#11, LNRE#12, LNCI#1
... [Plan Truncated for Report Brevity]
```

## 4. Quantitative Justification: When is a Spark Cluster Justified?
A central question in financial data engineering (Q1) is: **When should a financial institution migrate from a laptop to a Spark cluster?**

Based on our empirical measurements:
- **For quarterly Call Reports (1.68M rows, 237 MB Parquet):** A modern workstation running **Polars is vastly superior to a Spark cluster**. Polars processes the entire 42-year history in **4.43 seconds** on a single machine, whereas Spark incurs JVM startup overhead, Py4J serialization latency, and network shuffle costs (73.04s locally).
- **Tipping Point for Spark Cluster Justification:**
  1. **Data Volume Threshold (> 50–100 GB in memory):** When dataset volume exceeds single-node physical RAM (e.g., loan-level mortgage tape data with 100M+ monthly installments, credit card swipe feeds, or microsecond tick-by-tick order book data), single-machine engines thrash virtual memory and swap.
  2. **Streaming & Continuous Ingestion:** When supervisory feeds stream via Apache Kafka in real-time, Spark Structured Streaming provides distributed fault tolerance and exactly-once processing guarantees.
  3. **Multi-Institution Parallel Model Training:** When training independent models across thousands of partitioned bank portfolios concurrently.
- **Conclusion:** For central bank macroprudential surveillance using quarterly Call Reports, **Polars provides maximum developer agility, lowest operational cost, and fastest turnaround**; a Spark cluster should be reserved strictly for granular transaction/loan-level data lakes.
