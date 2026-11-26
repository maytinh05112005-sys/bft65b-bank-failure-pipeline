# BFT65B – Big Data Analytics in Finance
## Final Individual Research Report: Big-Data Pipeline for Bank Failure Early Warning

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Engine: Polars](https://img.shields.io/badge/engine-Polars%202.1-orange.svg)](https://pola.rs/)
[![Engine: PySpark](https://img.shields.io/badge/engine-PySpark%204.3-red.svg)](https://spark.apache.org/)
[![Model: LightGBM](https://img.shields.io/badge/model-LightGBM-green.svg)](https://lightgbm.readthedocs.io/)
[![Status: Reproducible](https://img.shields.io/badge/reproducible-verified-success.svg)]()

> **Role & Context:** You are a senior data engineer at Vietnam’s central bank (State Bank of Vietnam - SBV). The supervision department plans an early-warning system that ranks commercial banks by failure risk. This repository provides a complete, leak-free, reproducible big-data pipeline analyzing 40+ years of U.S. FDIC Call Reports (1984–2026, 1.68 million bank-quarters).

---

## 🏆 Key Results at a Glance

| Evaluation Dimension | Measurement / Metric | Benchmark Comparison | Key Data-Engineering Insight |
| :--- | :---: | :---: | :--- |
| **Storage Optimization** | **236.78 MB** (Parquet) | 467.68 MB (Raw CSV) | **49.4% disk space saving**, 6.59x faster selective column projection. |
| **Processing Speed** | **4.43 seconds** (Polars) | 10.72s (Pandas) / 73.04s (Spark) | **2.42x speedup** via vectorized Rust SIMD & multithreaded Rayon execution. |
| **Memory Footprint** | **1,988.1 MB Process RSS** (Polars) | 2,519.3 MB Process RSS (Pandas) | Evaluated using real OS Resident Set Size (`psutil`); saves 21.1% physical RAM. |
| **Model Discrimination** | **0.9795 ROC-AUC / 0.5901 PR-AUC** | Baseline LR: 0.4847 PR-AUC | High precision on severe class imbalance (1.192% test prevalence). |
| **Out-of-Time Recall** | **96.65%** (2008–2012 GFC) | Baseline LR: 84.43% | Correctly flagged **1,819 out of 1,882 bank failures** prior to closure. |
| **Supervisory Value** | **89.2% Cost Reduction** | Unguided supervision | Derived from empirical FDIC resolution costs ($C_{\text{FN}}/C_{\text{FP}} = 150$). |

---

## 📁 Repository Structure

```
.
├── data_raw/
│   └── fdic/                      # 170 quarterly Parquet partitions (1984Q1 - 2026Q2)
│       ├── financials/            # REPDTE=YYYYMMDD.parquet
│       ├── failures.parquet       # FDIC failure & assistance resolution history
│       ├── institutions.parquet   # Bank structure & exit codes
│       └── manifest_financials.json # Resumable download ledger
├── data_processed/                # Cleaned Point-in-Time panel & feature tables
│   ├── clean_panel.parquet        # Point-in-Time bank-quarter panel (1.68M rows)
│   ├── features_development.parquet # 1984Q1 - 2006Q4 development matrix
│   ├── features_test.parquet      # 2008Q1 - 2012Q4 out-of-time test matrix
│   └── features_metadata.json     # Feature list & temporal split specifications
├── src/
│   ├── 01_storage_benchmark.py    # Part 1: CSV vs. Parquet (Snappy/ZSTD) benchmark
│   ├── 02_data_quality_and_panel.py # Part 2: Automated quality audit & Point-in-Time panel
│   ├── 03_scalable_processing.py  # Part 3: Polars vs. Pandas vs. PySpark benchmark
│   ├── 04_feature_pipeline.py     # Part 4: CAMELS feature pipeline (leak-free)
│   ├── 05_modelling_validation.py # Part 5: Model training, MLlib demo & cost threshold
│   ├── convert_md_to_docx.py      # Generates formatted Word report with figures & tables
│   └── package_submission.py      # Packages clean submission archive
├── outputs/
│   ├── figures/                   # Publication-quality charts (PNG 300 DPI)
│   ├── tables/                    # Benchmark tables (CSV & Markdown)
│   └── reports/                   # Detailed data engineering reports
├── report/
│   ├── FINAL_REPORT_BFT65B.md     # Full academic research report (Markdown)
│   └── FINAL_REPORT_BFT65B.docx   # Submission-ready Word document
├── PROJECT_AUDIT.md               # Formal project audit against official rubric
├── IMPROVEMENT_LOG.md             # Detailed log of all architectural improvements
├── fdic_download_starter.py       # API acquisition script (resumable)
├── requirements.txt               # Pinned dependencies
├── run_all.py                     # Master one-click end-to-end execution script
└── README.md
```

---

## 🚀 Quick-Start & 100% Reproducibility

### 1. Installation
Clone this repository and install the dependencies:
```bash
git clone https://github.com/[Your-Username]/bft65b-bank-failure-pipeline.git
cd bft65b-bank-failure-pipeline
pip install -r requirements.txt
```

### 2. Run Entire Pipeline End-to-End
Execute all five data-engineering and modelling modules with a single command:
```bash
python run_all.py
```
*Total pipeline runtime is ~6.4 minutes on a standard laptop.*

---

## 🔬 Core Findings Answering Research Questions

### Q1: Storage & Scalability on Laptop vs. Spark Cluster
* **Laptop Storage:** Store Call Reports as quarterly partitioned Parquet files (`REPDTE=YYYYMMDD.parquet`) with Snappy compression (**236.78 MB**, 49.4% smaller than uncompressed CSV). Column projection and metadata statistics deliver **6.59x faster query latency**.
* **Engine Choice:** Single-node **Polars** processes the entire 1.68M row panel in **4.43 seconds** (1,988 MB Process RSS), outperforming Pandas (10.72s, 2,519 MB RSS) and local PySpark (73.04s).
* **Memory Measurement Methodology:** CPython's `tracemalloc` reports 0.03 MB for Polars because it cannot inspect native Rust `mimalloc` allocations. Using OS Resident Set Size (`psutil` Process RSS), Polars uses 1,988.1 MB peak RAM, saving 21.1% compared to Pandas.
* **Cluster Justification:** A distributed Spark cluster is **unjustified** for quarterly Call Reports. A cluster is only justified when data scales to **> 50–100 GB** (e.g. loan-level transaction tape microdata with > 100M rows, or streaming payment feeds via Kafka).

### Q2: Data Quality & Zero Look-Ahead Bias
* **Regulatory Reporting Shifts:** Basel capital ratios are missing pre-1990 due to regulatory history. We handle this without leakage by imputing missing values strictly using parameters frozen from the historical development set.
* **Economically Valid Distressed Signals:** While corrupt records (`ASSET <= 0`) are purged, **negative equity (`EQ < 0`, 15,659 records)** is strictly retained as a valid signal of technical insolvency under PCA rules.
* **Bank Exit Disambiguation:** Over 17,000 banks exited via voluntary mergers (`CHANGEC1 = 223`). Mergers are labeled $y=0$. Only official FDIC receivership closures (`RESTYPE == FAILURE`, 3,039 records post-1934) are labeled as failures.
* **Strict Point-in-Time Alignment:** Failure events are strictly aligned in the future window $D_q < \text{FAILDATE} \le D_q + 366\text{ days}$.

### Q3: Honest Out-of-Time Early-Warning Validation
* **Leakage Gate Compliance:** Enforced a **4-quarter embargo gap (2007Q1–2007Q4)** between the Development period (1984–2006) and the Out-of-Time Test period (2008–2012 Global Financial Crisis, containing 1,882 failures).
* **Prevalence Shift Mechanics:** The Validation PR-AUC (0.0425, prevalence 0.0387%) represents a 109.7x lift; the Test PR-AUC (0.5901, prevalence 1.1923%) represents a 49.5x lift. Extreme balance-sheet deterioration during the GFC allows individual features like `capital_ratio` to achieve 0.9522 test ROC-AUC.
* **Cost-Sensitive Thresholding:** Utilizing empirical FDIC resolution losses ($C_{\text{FN}} / C_{\text{FP}} = 150:1$), the optimal alarm threshold ($p^* = 0.040$) yielded **96.65% recall** during the 2008 crisis, reducing expected supervisory loss by **89.2%**.

---

## 📚 Academic Integrity & Citations

1. **Cole, R. A., & White, L. J. (2012).** *Déjà Vu All Over Again: The Causes of U.S. Commercial Bank Failures This Time Around.* Journal of Financial Services Research, 42(1), 5–29.
2. **Wheelock, D. C., & Wilson, P. W. (2000).** *Why Do Banks Disappear? The Determinants of U.S. Bank Failures and Acquisitions.* The Review of Economics and Statistics, 82(1), 127–138.
3. **Davis, J., & Goadrich, M. (2006).** *The Relationship Between Precision-Recall and ROC Curves.* ICML '06.
4. **Federal Deposit Insurance Corporation (FDIC).** *BankFind Suite Public API Documentation.*
5. **Generative AI Declaration:** Generative AI was used as a productivity assistant for code scaffolding and formatting. All empirical computations and analytical models were executed and validated on the local environment.
