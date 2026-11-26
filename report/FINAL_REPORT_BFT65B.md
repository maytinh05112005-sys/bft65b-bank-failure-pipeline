# NATIONAL ECONOMICS UNIVERSITY
## BFT65B – BIG DATA ANALYTICS IN FINANCE
### FINAL EXAMINATION – INDIVIDUAL RESEARCH REPORT

---

# A Big-Data Pipeline for Bank Failure Early Warning: Scalable Architecture, Point-in-Time Data Engineering, and Honest Supervisory Validation

**Class:** BFT65B – Big Data Analytics in Finance  
**Institution:** National Economics University (NEU)  
**Class Code / Course:** BFT65B – Big Data Analytics in Finance  
**GitHub Repository:** `https://github.com/[Your-Username]/bft65b-bank-failure-pipeline`  
**Dataset Download Source:** U.S. Federal Deposit Insurance Corporation (FDIC) Public BankFind API  
**Data Acquisition Date:** November 2026  
**Temporal Coverage:** 1984Q1 to 2026Q2 (170 Quarters)  
**Total Records Downloaded:** 1,682,616 Call Report quarterly observations; 4,117 Failures & Assistance Cases; 27,835 Institutions  
**Development Period:** 1984Q1 – 2006Q4 (1,194,887 bank-quarters; 8,697 failures / 0.728% prevalence)  
**Embargo Buffer Gap:** 2007Q1 – 2007Q4 (34,754 bank-quarters; 4-quarter zero-leakage purging window)  
**Out-of-Time Test Period:** 2008Q1 – 2012Q4 (157,851 bank-quarters; 1,882 failures / 1.192% prevalence)  
**Post-Test Historical Holdout:** 2013Q1 – 2026Q2 (265 failures, including 2023 SVB, Signature, and First Republic)  

---

## 1. Executive Summary & Core Research Questions

As a senior data engineer tasked with designing the data backbone for an Early-Warning System (EWS) at Vietnam’s central bank (the State Bank of Vietnam - SBV), this report constructs an end-to-end, reproducible, and leak-free big-data pipeline analyzing four decades of U.S. commercial bank Call Reports (1984–2026). The central imperative is not merely predictive accuracy, but the rigorous engineering of storage, cleaning, vectorized scaling, and out-of-sample validation under extreme class imbalance.

This report provides definitive, measurement-backed answers to the three core research questions:

### Q1. How should four decades of bank reports be stored and processed on a laptop, and when would a Spark cluster be needed?
* **Storage Architecture:** Raw Call Reports spanning 170 quarters (1,682,616 rows $\times$ 33 columns) should **never be stored in uncompressed CSV** (~468–500 MB, high parsing latency). Instead, data must be structured as **columnar Parquet files partitioned quarterly (`REPDTE=YYYYMMDD.parquet`) using Snappy or ZSTD compression**. This reduces disk storage to **236.78 MB (a 49.4% to 55% space reduction)** and delivers a **5.92x to 6.59x read speedup** via selective column projection and metadata statistics.
* **Single-Node Processing Engine:** With modern columnar engines like **Polars (vectorized Rust with Apache Arrow memory layout)**, a standard workstation processes the entire 1.68-million-row panel (including multi-column ratios, 4-quarter lag window operations, and state-level aggregations) in **4.43 seconds**—a **2.42x speedup** over Pandas (10.72s).
* **Physical Memory Profiling (Process RSS):** Standard CPython profilers like `tracemalloc` report an erroneous 0.03 MB for Polars because they are blind to Rust's native `mimalloc` allocations. Using physical OS Resident Set Size (`psutil` Process RSS), Polars exhibits a peak resident memory footprint of **1,988.1 MB** (vs. **2,519.3 MB** for Pandas), saving approximately **21.1%** in physical RAM while executing over 2.4x faster.
* **When is a Spark Cluster Justified?** A distributed Spark cluster is **unjustified and counterproductive for quarterly financial panels on a single laptop**, as JVM process startup, Py4J serialization, and local task scheduling make local Spark significantly slower (73.04s) than Polars. A Spark cluster is justified **only when data scales beyond workstation RAM (> 50–100 GB)**, specifically for: (i) loan-level transaction tape data (> 100M rows), (ii) real-time streaming feeds (e.g., payment system logs via Kafka), or (iii) simultaneous multi-portfolio stress-testing across thousands of partitions.

### Q2. What quality problems do the raw data contain, and how does the pipeline handle them without look-ahead bias?
1. **Structural Regulatory Missingness (MNAR):** Missing values in FDIC Call Reports are not random; they reflect historical changes in prudential regulation. For example, risk-weighted capital ratios (`RBC1AAJ`, `RBCRWAJ`) are missing prior to 1990 because Basel I was only implemented in the U.S. in 1990. Past-due loans (`P3ASSET`, `P9ASSET`, `NAASSET`) also experienced reporting threshold revisions during the S&L deregulation era. Our pipeline handles this without leakage by imputing missing values using median parameters derived **strictly from historical development data**.
2. **Schema Drift Across Decades:** Numeric ratios oscillate between `Int64` and `Float64` across quarters in raw files. We enforce an immutable **PyArrow Unified Schema** at ingestion, guaranteeing sub-second dataset loading.
3. **Economically Valid Distressed Signals:** While corrupt non-positive assets (`ASSET <= 0`) are filtered out, **negative equity (`EQ < 0`, 15,659 rows)** is strictly preserved. Under Prompt Corrective Action (PCA) frameworks, negative equity represents technical insolvency—a primary economic predictor of imminent failure.
4. **Exit Disambiguation (Failures vs. Mergers):** Over 17,000 banks exited the US banking system via voluntary mergers (`CHANGEC1 = 223`) following the Riegle-Neal Act of 1994. Mislabelling mergers as failures contaminates early-warning systems. Our pipeline cross-verifies bank exits against official FDIC Receivership resolution records (`RESTYPE == FAILURE`), ensuring open-bank assistance (`ASSISTANCE`, 590 cases) and healthy acquisitions are accurately tagged ($y=0$).
5. **Strict Point-in-Time Label Alignment:** Failure dates are mapped strictly into the future observation window $D_q < \text{FAILDATE} \le D_q + 366\text{ days}$. Any past resolutions or future knowledge are purged.

### Q3. Can the pipeline’s output support an honestly validated early-warning model?
* **Yes.** By enforcing a chronological split with an **embargo buffer gap of 4 quarters (2007Q1–2007Q4)**, the pipeline completely prevents label contamination. 
* Evaluated **once** on the completely held-out 2008–2012 Global Financial Crisis test set (157,851 bank-quarters, 1,882 failures), the gradient-boosted LightGBM early-warning model achieves a **ROC-AUC of 0.9795** and an **Out-of-Time PR-AUC of 0.5901** (compared to the baseline prevalence of 1.192%).
* Using an empirical cost matrix calibrated on historical FDIC resolution costs ($C_{\text{FN}} / C_{\text{FP}} \approx 150:1$), the model's cost-optimal threshold ($p^* = 0.040$) achieves a **96.7% recall rate** on banks that failed during the 2008 crisis, reducing aggregate supervisory loss by **89.2%** compared to unguided supervision.

---

## 2. Part 1: Data Acquisition & Storage Layout Architecture

### 2.1. Acquisition Protocol & Resumability
Data acquisition was executed via `fdic_download_starter.py` querying the public FDIC BankFind API endpoints (`/financials`, `/failures`, `/institutions`). Key engineering features of the acquisition procedure include:
* **Fault-Tolerant Exponential Backoff:** Transient network failures (`ConnectionResetError`, `ChunkedEncodingError`) are automatically retried with exponential back-off ($2^n$ seconds), ensuring complete pipeline stability over continuous downloads.
* **Quarterly Partitioned Persistence:** Call reports are serialized incrementally to `data_raw/fdic/financials/REPDTE=YYYYMMDD.parquet`.
* **State Manifest Verification:** A local metadata ledger (`manifest_financials.json`) records the total rows returned by the API versus saved rows. If the pipeline is interrupted, it resumes from the exact uncompleted quarter, avoiding redundant network egress.
* **Integrity Audit:** Verified zero duplicate `(CERT, REPDTE)` composite keys across all 170 quarters and exactly 0 incomplete quarters.

### 2.2. Storage Layout Benchmark: CSV vs. Parquet (Snappy & ZSTD)
To evaluate storage efficiency, 12 representative quarters spanning crisis and non-crisis regimes (122,242 rows $\times$ 33 columns) were benchmarked across formats.

| Format | Compression | Size (MB) | Write Time (s) | Scan All (s) | Project 3 Cols (s) | Predicate Filter (s) | Compression Ratio | Speedup vs CSV |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **CSV (Standard)** | None | 28.83 | 3.55 | 0.260 | 0.157 | 0.213 | 1.00x | 1.00x |
| **Parquet (Default)** | Snappy | 14.60 | 0.91 | 0.109 | 0.024 | 0.083 | 1.98x | **6.59x** |
| **Parquet (Archive)** | ZSTD | 12.54 | 1.05 | 0.107 | 0.026 | 0.085 | 2.30x | **5.92x** |

* **Compression Ratio:** Parquet Snappy achieves a **1.98x compression ratio**, while ZSTD achieves **2.30x**. For the full 1.68M panel, partitioned Parquet occupies **236.78 MB**, compared to an estimated **467.68 MB** for uncompressed CSV (a 49.4% space saving).
* **Write & Serialization Throughput:** Writing to Parquet is **3.9x faster** than formatting text strings for CSV (0.91s vs. 3.55s).
* **Column Projection Speedup:** When querying only essential columns (`CERT`, `REPDTE`, `ASSET`), Parquet's columnar chunk reading is **6.59x faster** than CSV. CSV parsers must scan every byte of all 33 columns from disk, whereas Parquet reads only the requested byte offsets directly from column chunks via dictionary and bit-packing decoders.

---

## 3. Part 2: Data Quality Audit & Clean Point-in-Time Panel

### 3.1. FDIC Failure Universe Reconciliation (3,039 vs. 4,117)
The raw FDIC Failures API endpoint yields 4,117 records. To construct a rigorous supervisory panel, we performed the following exact reconciliation:
1. **Pre-1977 Non-Digital Charters (488 records):** Pre-1977 records lack computerized certificate numbers (`CERT is null`). Because they predate Call Report digital feeds, they cannot be joined to panel data and are excluded.
2. **Open-Bank Assistance Transactions (590 records):** Under Section 13(c) of the FDI Act, the FDIC provided financial assistance (`RESTYPE == 'ASSISTANCE'`) to prevent closure. The institutions remained open and operational. Treating them as failures would contaminate the binary outcome variable.
3. **Valid Terminal Receivership Closures (3,039 records):** Exactly 3,039 true bank failures with valid `CERT` and documented receiverships post-1934 were linked point-in-time to quarterly reports:
   $$\text{Valid Closures} = 4,117 - 488 - 590 = \mathbf{3,039}$$

### 3.2. Automated Data Quality Audit Findings
Auditing 1,682,616 raw Call Report rows across four regulatory eras revealed three major structural phenomena:

1. **Regulatory Era Shifts (Missing Not At Random):**
   * *1984–1993 (S&L Crisis):* Non-accrual and past-due loan buckets (`P3ASSET`, `P9ASSET`, `NAASSET`) exhibited variable reporting thresholds during the S&L deregulation era of the mid-1980s.
   * *Implication:* Standard listwise deletion (dropping rows with missing values) would completely delete the entire 1980s crisis from the training set! We use median imputation fitted strictly on development data.

2. **Domain Anomaly Detection:**
   * `ASSET <= 0`: 0 records. Total assets are strictly positive across all filed reports.
   * `DEP < 0`: 0 records. Deposits are non-negative.
   * `EQ < 0`: **15,659 records (0.9306%)**. Insolvent banks often continue reporting under regulatory forbearance prior to closure. In accounting, equity is negative when accumulated retained earnings losses exceed capital stock. This is preserved as a vital risk signal.
   * `LNLSGR < 0`: 32 records (0.0019%). Loan balances below zero represent clerical adjustments and are bounded.
   * `ROA Outliers (< -50% or > 50%)`: 1,056 records (0.0628%). Handled via `RobustScaler` clipping.

### 3.3. Bank Exit Disambiguation (Failures vs. Mergers)
In the raw data, over 17,000 banks vanish from Call Reports due to mergers and voluntary liquidations (`CHANGEC1 = 223`). Treating all vanished banks as failures causes massive label corruption.
* We join the panel with `failures.parquet` and filter strictly for terminal receivership closures (`RESTYPE == "FAILURE"`).
* Target label $y_{i,q}$ is defined point-in-time:
  $$y_{i,q} = \mathbf{1}\left\{0 < \text{FAILDATE}_i - \text{REPDTE}_{i,q} \le 366\text{ days} \land \text{RESTYPE}_i = \text{"FAILURE"}\right\}$$
* Across the 1,682,616 bank-quarters, exactly **10,892 observations are positive failure events (0.647% failure rate)**, illustrating the extreme real-world class imbalance of banking supervision.

---

## 4. Part 3: Scalable Processing Engine Benchmark (Polars vs. Pandas vs. PySpark)

### 4.1. Core Transformations Implemented
The transformation pipeline executes:
1. Multi-column financial ratios (Capital Adequacy, Asset Quality, Earnings, Liquidity).
2. Four-quarter rolling window operations per bank (`CERT` group): 1-year asset growth rate ($\Delta \text{ASSET}_{1yr}$) and 1-year change in NPL ratio ($\Delta \text{NPL}_{1yr}$).
3. Group-by peer aggregations: State-quarter mean capital ratio and median NPL ratio joined back to each bank observation.

### 4.2. Performance Benchmark Table

| Engine | Execution Mode | Execution Time (s) | Speedup vs Pandas | Peak Process RSS (MB) | Net RAM Growth (MB) | Memory Profiling Method | Scalability Limit |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| **Pandas (Baseline)** | Eager / Single-Threaded | 10.72 s | 1.00x | 2,519.3 MB | 799.6 MB | psutil Process RSS (OS Resident) | ~5-10M rows on 16GB Laptop |
| **Polars (Lazy)** | Lazy / Vectorized Multi-Threaded | **4.43 s** | **2.42x** | **1,988.1 MB** | 1,790.7 MB | psutil Process RSS (OS Resident) | ~50-100M rows on Laptop (Streaming) |
| **PySpark (Local)** | Catalyst Distributed / JVM | 73.04 s | 0.15x | 1,202.5 MB | 1,202.5 MB | psutil Process RSS (Driver + JVM) | Billions of rows (Distributed Cluster) |

* **Numerical Consistency:** Maximum absolute difference between Polars and Pandas across all computed metrics is **0.00e+00**, proving 100% calculation equivalence.
* **Polars Performance:** Polars completes the entire multi-stage feature extraction in **4.43 seconds**—a **2.42x speedup** over Pandas, using 21.1% less physical resident RAM.
* **Why Polars Outperforms:**
  1. *Vectorized Rust Kernel:* Operates directly on contiguous Arrow buffers with SIMD instructions.
  2. *Lazy Query Optimization:* Merges column projections and eliminates intermediate DataFrame allocations.
  3. *Work-Stealing Concurrency:* Rayon thread pool utilizes all CPU cores in parallel without Python GIL contention.
* **PySpark Analysis:** PySpark takes **73.04 seconds**. On a single workstation, Spark suffers from JVM process orchestration, Py4J IPC socket serialization, and partition task scheduling overhead.

### 4.3. Cluster Justification Argument
* **Call Report Scope:** Call Reports contain ~1.7M rows. Polars executes this in under 5 seconds on a laptop. **A Spark cluster is entirely unjustified for this workload.**
* **When a Cluster is Justified:**
  1. *Granular Loan-Level Tape Data:* Microdata containing hundreds of millions of individual mortgage, corporate loan, or credit card transactions (> 100 GB).
  2. *Real-Time Transactional Auditing:* Streaming payment surveillance (e.g., Fedwire or Vietnamese NAPAS feeds) requiring Kafka integration and fault-tolerant continuous processing.
  3. *Distributed Model Training:* Parallelized hyperparameter sweeps across thousands of financial portfolios.

---

## 5. Part 4: Feature Pipeline & CAMELS Banking Risk Architecture

### 5.1. CAMELS Feature Engineering
We engineered 23 risk features mapped to the international **CAMELS supervisory framework**:

| CAMELS Pillar | Metric | Formula & Economic Rationale |
| :--- | :--- | :--- |
| **C - Capital** | `capital_ratio` | $\text{EQ} / \text{ASSET}$: Primary loss-absorbing buffer. |
| **C - Capital** | `capital_to_loans` | $\text{EQ} / \text{LNLSNET}$: Capital buffer scaled against credit risk assets. |
| **A - Asset Quality** | `npl_ratio` | $(\text{P3ASSET} + \text{P9ASSET} + \text{NAASSET}) / \text{LNLSGR}$: Non-performing loan intensity. |
| **A - Asset Quality** | `coverage_ratio` | $\text{LNATRES} / (\text{NPL} + \epsilon)$: Reserve protection for delinquent loans. |
| **A - Asset Quality** | `net_chargeoff_ratio` | $\text{NCLNLS} / \text{LNLSNET}$: Realized loan losses written off. |
| **A - Asset Quality** | `foreclosed_re_ratio` | $\text{ORE} / \text{ASSET}$: Foreclosed real estate on books (severe crisis signal). |
| **M - Management** | `efficiency_ratio` | $\text{NONIX} / (\text{Net Operating Income})$: Operating cost discipline. |
| **M - Management** | `overhead_ratio` | $\text{NONIX} / \text{ASSET}$: Non-interest operational cost burden. |
| **E - Earnings** | `roa_calc`, `roe_calc` | $\text{NETINC} / \text{ASSET}, \text{NETINC} / \text{EQ}$: Baseline return on assets and equity. |
| **E - Earnings** | `nim_calc` | $(\text{INTINC} - \text{EINTEXP}) / \text{ASSET}$: Net Interest Margin yield. |
| **L - Liquidity** | `cash_to_deposits` | $\text{CHBAL} / \text{DEP}$: High-quality liquid asset buffer. |
| **L - Liquidity** | `core_deposit_ratio` | $\text{DEPDOM} / \text{DEP}$: Stable, sticky retail deposit share. |
| **L - Liquidity** | `brokered_deposit_ratio` | $\text{BRO} / \text{DEP}$: Run-prone wholesale hot money (hallmark of SVB & IndyMac). |
| **L - Liquidity** | `loan_to_deposit_ratio` | $\text{LNLSNET} / \text{DEP}$: Lending aggressiveness vs. stable funding. |
| **S - Sensitivity/Growth** | `log_assets` | $\ln(\text{ASSET})$: Controls for institution size and systemic importance. |
| **S - Dynamics** | `asset_growth_1yr` | $(\text{ASSET}_t - \text{ASSET}_{t-4}) / \text{ASSET}_{t-4}$: Aggressive credit expansion. |
| **S - Dynamics** | `npl_change_1yr` | $\text{NPL}_t - \text{NPL}_{t-4}$: Speed of credit quality deterioration. |
| **Peer Deviation** | `capital_dev_state` | Bank capital ratio minus state-quarter peer median. |

### 5.2. Failure Universe Reconciliation Across Partitions
* **Total Labeled Failures in Clean Panel:** **10,892**
* **Development Set (1984Q1 – 2006Q4):** 8,697 failures (1,194,887 observations, 0.728% prevalence)
* **Purging Gap Embargo (2007Q1 – 2007Q4):** 48 failures (34,754 observations)
* **Out-of-Time Test Set (2008Q1 – 2012Q4):** 1,882 failures (157,851 observations, 1.192% prevalence)
* **Post-Test Holdout (2013Q1 – 2026Q2):** 265 failures (277,595 observations)
* **Complete Reconciliation:**
  $$8,697 + 48 + 1,882 + 265 = \mathbf{10,892} \quad (100.0\% \text{ exact match})$$

### 5.3. Regulatory Publication Lag & Leakage Gate Enforcement
* **Filing and Publication Timelines:** Under FFIEC regulations, commercial banks must file Call Reports within 30 to 35 days post quarter-end, and public availability occurs approximately **45 days post-quarter-end**.
* **Point-in-Time Alignment:** Because our target label predicts failures occurring within $(q, q + 4\text{ quarters}]$, the 90-to-366 day forward horizon provides supervisors with at least 1.5 to 10.5 months of actionable lead time *after* the Call Report is fully verified and published.
* **Temporal Purging Embargo:** The 4-quarter gap in 2007 ensures that no bank failing in early 2008 can contaminate the training period ending in 2006Q4.
* **Zero-Leakage Transformation:** Imputation medians and `RobustScaler` parameters were calculated **strictly on the 1984–2006 Development Set** and frozen. Test data was transformed out-of-sample with zero exposure to future statistics.

---

## 6. Part 5: Modelling, Spark MLlib Demo & Cost-Sensitive Validation

### 6.1. Validation Protocol
Within the Development Period, models were validated chronologically (Train: 1984–1999; Validation: 2000–2006). Random K-Fold Cross-Validation was explicitly rejected because temporal shuffling leaks future macroeconomic regime data into past splits.

### 6.2. Empirical Cost-Sensitive Threshold Optimization
Standard models apply an arbitrary $p = 0.5$ classification threshold, which assumes equal misclassification costs ($C_{\text{FN}} = C_{\text{FP}}$). In prudential supervision, this assumption is disastrous.
* **Cost of Missed Failure ($C_{\text{FN}}$):** Estimated from historical FDIC resolution losses (`COST` in `failures.parquet`). The median resolution cost to the deposit insurance fund is **\$11,236,000**.
* **Cost of False Alarm ($C_{\text{FP}}$):** The administrative cost of conducting an unnecessary comprehensive supervisory on-site inspection is estimated at **\$75,000**.
* **Empirical Cost Ratio:**
  $$\frac{C_{\text{FN}}}{C_{\text{FP}}} = \frac{\$11,236,000}{\$75,000} \approx 150 : 1$$
* Evaluating the expected supervisory loss curve across the validation set identified an **optimal threshold of $p^* = 0.040$**.

### 6.3. Out-of-Time Test Set Evaluation (2008Q1 – 2012Q4 GFC)

| Model Architecture | ROC-AUC | PR-AUC | Brier Score | Threshold | Recall (%) | Precision (%) | False Alarms (FP) | Missed Failures (FN) | Total Cost Units |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Baseline LogisticReg** | 0.9772 | 0.4847 | 0.0262 | 0.50 | 84.43% | 25.04% | 4,758 | 293 | 48,708 |
| **LightGBM Classifier** | **0.9795** | **0.5901** | **0.0227** | **0.040\*** | **96.65%** | 7.96% | 21,022 | **63** | **30,472** |
| **PySpark MLlib (Demo)** | 0.9251 | N/A | N/A | 0.50 | N/A | N/A | N/A | N/A | Demonstration |

*\* Optimal threshold $p^* = 0.040$ derived from empirical cost matrix.*

* **Discrimination:** In a dataset with 1.192% failure prevalence, ROC-AUC can be overly optimistic due to the massive true-negative pool. **PR-AUC (Precision-Recall AUC)** is the decisive metric: LightGBM achieves an outstanding **PR-AUC of 0.5901**, substantially exceeding the 0.4847 of the baseline.
* **Understanding the Validation vs. Test PR-AUC Shift:**
  - In the 2000–2006 Validation Set, the banking sector experienced an extraordinary boom with only 102 failures in ~263k observations (prevalence $\pi = 0.0387\%$). A PR-AUC of 0.0425 represents a **109.7x lift** over random baseline.
  - In the 2008–2012 Test Set, the GFC induced a 30.8-fold surge in failure prevalence ($\pi = 1.1923\%$). Because Precision mathematically scales with positive class prevalence (Davis & Goadrich, 2006), the PR-AUC naturally scales up to 0.5901 (a **49.5x lift**).
  - Furthermore, during the GFC, distressed institutions suffered complete balance-sheet collapse: univariate test ROC-AUC for `capital_ratio` alone reached **0.9522** and `npl_ratio` reached **0.9448**, confirming that high test discrimination is economically genuine and free of data leakage.
* **Failure Capture Rate:** At the cost-optimal threshold, LightGBM successfully flagged **96.65% of all banks (1,819 out of 1,882)** that collapsed during the Global Financial Crisis at least 1 to 4 quarters before regulatory closure.
* **Economic Value:** Compared to an unguided supervisory policy, the LightGBM early-warning pipeline delivered an **89.2% reduction in expected supervisory loss**.
* **Key Predictive Drivers:** Split-importance analysis identified:
  1. `securities_to_assets`: Investment securities liquidity buffer.
  2. `log_assets`: Bank scale and systemic risk profile.
  3. `cash_to_deposits`: Immediate cash reserves.
  4. `loan_to_deposit_ratio`: Lending strain vs deposit funding.
  5. `asset_growth_1yr`: Excessive lending expansion preceding distress.

---

## 7. Part 6: Limitations & Adaptation to the Vietnamese Banking Sector

### 7.1. Pipeline Limitations
1. **Quarterly Reporting Latency:** Call Reports are filed 30–45 days after quarter-end. In high-speed digital bank runs (such as Silicon Valley Bank in 2023, where \$42B was withdrawn in 10 hours), quarterly accounting data is too slow to provide intraday alerts.
2. **Off-Balance-Sheet & Derivative Exposures:** Call Reports provide aggregated balances but obscure complex credit default swap (CDS) linkages, interest rate hedges, and syndicated loan participations.
3. **Contagion & Network Effects:** The model treats each institution as an independent entity, ignoring interbank lending network contagion and panic-driven deposit runs.

### 7.2. Adaptation to Vietnam’s Banking Sector (SBV Context)
If adapting this pipeline for Vietnam's central bank (State Bank of Vietnam - SBV / Ngân hàng Nhà nước), several fundamental structural modifications are mandatory:

1. **Non-Public Supervisory Reporting & Data Infrastructure:**
   * Unlike the U.S. FDIC, Vietnamese Call Reports (Báo cáo thống kê theo Thông tư 35/2015/TT-NHNN) are confidential. The pipeline must integrate with the SBV's internal Credit Information Center (CIC) and Financial Supervision Department data lakes.
2. **Basel II / III Implementation Differences:**
   * While the U.S. implemented Basel I in 1990 and Basel III post-2010, Vietnam adopted Basel II standards via **Circular 41/2016/TT-NHNN** (effective 2020). Historical capital ratios prior to 2020 are calculated under Circular 36/2014, requiring explicit regulatory bridging and separate imputation matrices.
3. **Real Estate Collateral & Non-Performing Loan Masking:**
   * In Vietnam, over 70% of commercial bank loans are secured by real estate. During property market downturns, NPLs are often obscured via debt restructuring mechanisms (such as **Circular 02/2023/TT-NHNN** allowing loan rescheduling without changing debt classification). A Vietnamese early-warning pipeline must incorporate non-financial proxy features, including developer bond maturities, real estate transaction volumes, and loan restructuring proportions.
4. **Institutional Ownership Structure (State-Owned vs. Joint-Stock Commercial Banks):**
   * Vietnam's banking system is bifurcated between the "Big 4" State-Owned Commercial Banks (Vietcombank, BIDV, VietinBank, Agribank) and Private Joint-Stock Commercial Banks (NHTM Cổ phần). The Big 4 enjoy implicit sovereign backing, eliminating systemic liquidation risk. Early-warning systems in Vietnam should focus on liquidity strain and resolution mechanisms (e.g., special receivership / kiểm soát đặc biệt as applied to SCB, DongA Bank, OceanBank) rather than outright bankruptcy liquidation.
5. **Cross-Holding and Corporate Bond Exposures:**
   * A critical risk vector in Vietnam is cross-ownership between commercial banks, real estate conglomerates, and securities firms. Feature engineering must include corporate bond portfolio holdings and interbank deposit ratios.

---

## 8. Reproducibility & Master Execution Guide

The entire pipeline is fully automated and reproducible via `run_all.py`.

```
project/
├── data_raw/fdic/             # 170 quarterly Parquet partitions, failures, institutions
├── data_processed/            # Clean Point-in-Time panel and leak-free feature matrices
├── src/
│   ├── 01_storage_benchmark.py       # Part 1: CSV vs Parquet Snappy/ZSTD benchmarks
│   ├── 02_data_quality_and_panel.py  # Part 2: Automated data quality audit & Point-in-Time panel
│   ├── 03_scalable_processing.py     # Part 3: Polars vs Pandas vs PySpark performance benchmarks
│   ├── 04_feature_pipeline.py        # Part 4: CAMELS feature generation & leak-free preprocessing
│   └── 05_modelling_validation.py    # Part 5: Early-warning modelling & cost-sensitive validation
├── outputs/
│   ├── figures/               # High-resolution benchmark and ROC/PR charts
│   ├── tables/                # Markdown and CSV evaluation tables
│   └── reports/               # Automated data engineering audit reports
├── report/
│   ├── FINAL_REPORT_BFT65B.md   # Comprehensive final research report
│   └── FINAL_REPORT_BFT65B.docx # Formatted Word report with embedded tables & figures
├── PROJECT_AUDIT.md           # Formal project audit against official rubric
├── IMPROVEMENT_LOG.md         # Systematic record of improvements made
├── requirements.txt           # Pinned dependencies
├── run_all.py                 # Master one-click end-to-end execution script
└── README.md                  # Project documentation and academic citations
```

### Quick-Start Command
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Execute the entire pipeline end-to-end
python run_all.py
```

---

## 9. References & Academic Integrity

1. **Cole, R. A., & White, L. J. (2012).** *Déjà Vu All Over Again: The Causes of U.S. Commercial Bank Failures This Time Around.* Journal of Financial Services Research, 42(1), 5–29.
2. **Wheelock, D. C., & Wilson, P. W. (2000).** *Why Do Banks Disappear? The Determinants of U.S. Bank Failures and Acquisitions.* The Review of Economics and Statistics, 82(1), 127–138.
3. **Demirgüç-Kunt, A., & Detragiache, E. (1998).** *The Determinants of Banking Crises in Developing and Developed Countries.* IMF Staff Papers, 45(1), 81–109.
4. **Davis, J., & Goadrich, M. (2006).** *The Relationship Between Precision-Recall and ROC Curves.* Proceedings of the 23rd International Conference on Machine Learning (ICML '06), 233–240.
5. **Federal Deposit Insurance Corporation (FDIC).** *BankFind Suite Public API Documentation & Financial Call Report Definitions (RISVIEW).* `https://api.fdic.gov/banks/docs/`
6. **State Bank of Vietnam (NHNN).** *Circular No. 41/2016/TT-NHNN on Capital Adequacy Ratios for Commercial Banks and Foreign Bank Branches.*
7. **State Bank of Vietnam (NHNN).** *Circular No. 02/2023/TT-NHNN on Debt Rescheduling and Retention of Debt Groups by Credit Institutions.*
8. **Apache Spark Community.** *Apache Spark: Unified Engine for Large-Scale Data Processing.* `https://spark.apache.org/`
9. **Polars Development Team.** *Polars: Fast Multi-Threaded DataFrame Library in Rust & Python.* `https://pola.rs/`
10. **LightGBM Development Team.** *LightGBM: A Highly Efficient Gradient Boosting Decision Tree.* Microsoft Research.
11. **Generative AI Declaration:** In accordance with course guidelines on academic integrity, generative AI (Claude / Gemini) was utilized as a coding assistant to scaffold repetitive API querying scripts, format markdown tables, and assist in drafting report outlines. All empirical computations, schema transformations, and analytical interpretations were rigorously verified and executed on the local environment.
