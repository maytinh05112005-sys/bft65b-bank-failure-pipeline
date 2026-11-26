# IMPROVEMENT LOG: BFT65B FINAL PROJECT
**Project Title:** A Big-Data Pipeline for Bank Failure Early Warning  
**Course:** BFT65B – Big Data Analytics in Finance  
**Implementation Date:** November 2026  
**Status:** In Progress / Systematic Rollout

---

## I. OVERVIEW OF PLANNED & EXECUTED IMPROVEMENTS

Based on the findings in `PROJECT_AUDIT.md`, this log tracks the exact architectural, algorithmic, and code modifications applied across all project modules.

---

## II. DETAILED COMPONENT IMPROVEMENT SPECIFICATION

### 1. `requirements.txt`
- **Issue:** Missing explicit pinned versions for `psutil` (system memory profiling) and `python-docx` (academic report generation).
- **Modification:** Added `psutil==7.2.2` and `python-docx==1.1.2` with exact pinning.

### 2. `src/01_storage_benchmark.py`
- **Issue:** Seaborn emitted palette warnings; Parquet dictionary encoding and Snappy compression ratios needed clear reporting.
- **Modification:**
  - Standardized Seaborn `barplot` calls with explicit `hue` mapping to eliminate deprecation warnings.
  - Added explicit documentation of full dataset storage extrapolation methodology (actual measured 236.78 MB Snappy Parquet vs. 467.68 MB uncompressed CSV baseline, achieving 49.4% disk space savings and 8–12x query speedup).

### 3. `src/02_data_quality_and_panel.py`
- **Issue:** Raw failures (4,117) vs. filtered valid failures (3,039) lacked itemized stdout logging. Negative equity retention lacked detailed accounting rationale.
- **Modification:**
  - Added explicit breakdown logging in stdout and in `outputs/reports/part2_data_quality_report.md`:
    - 4,117 raw records $\rightarrow$ minus 488 pre-1977 failures with NULL CERT $\rightarrow$ minus 590 non-closure assistance transactions $\rightarrow$ exactly 3,039 terminal receivership failures.
  - Documented supervisory justification for retaining negative equity observations (15,659 rows): Negative equity ($EQ < 0$) is an authentic leading indicator of balance-sheet insolvency under Prompt Corrective Action (PCA) frameworks; dropping them would introduce severe survival/selection bias.

### 4. `src/03_scalable_processing.py`
- **Issue:** `tracemalloc` only captures CPython heap allocations and is blind to Polars' native Rust `mimalloc` memory, resulting in an erroneous 0.03 MB peak memory measurement. Seaborn emitted `palette` without `hue` warnings.
- **Modification:**
  - Implemented a multithreaded `ProcessMemoryTracker` class using `psutil.Process().memory_info().rss` to sample resident set size every 10 ms during the entire lifecycle of Polars, Pandas, and PySpark pipelines.
  - Recorded both Net RSS Growth (MB) and Peak Process RSS (MB) for all three engines.
  - Updated benchmark table generator, execution plan reports, and bar chart visualizations to display accurate physical memory footprints (~750 MB for Polars, ~932 MB for Pandas, ~2048 MB for PySpark JVM heap).
  - Fixed Seaborn color mapping warnings.

### 5. `src/04_feature_pipeline.py`
- **Issue:** Discrepancy of 265 failures between Part 2 total labels (10,892) and Part 4 partitions (10,627) was unexplained in output reports. Regulatory publication lag needed explicit documentation.
- **Modification:**
  - Added transparent reconciliation in stdout and report: Dev (8,697) + Gap (48) + Test (1,882) = 10,627; the remaining 265 failures belong to the post-test historical holdout period (2013Q1–2026Q2).
  - Explicitly documented Call Report filing deadlines (FFIEC 031/041 mandates submission within 30 days for institutions with foreign offices and 35 days for domestic-only, with public availability ~45 days post-quarter-end). Validated that 4-quarter forward horizon ($t+1$ to $t+4$) provides complete supervisory operational buffer.
  - Verified that all rolling features use backward `shift(4)` over bank `CERT`, guaranteeing zero look-ahead bias.

### 6. `src/05_modelling_validation.py`
- **Issue:** LightGBM emitted `eval_set` / callback deprecation warnings. Seaborn emitted palette warnings. Large gap between validation (PR-AUC 0.0425) and test (PR-AUC 0.5901) required mathematical explanation of class prevalence shifts and single-feature crisis separation.
- **Modification:**
  - Modernized LightGBM training call with clean callbacks and silent evaluation.
  - Added mathematical explanation of PR-AUC prevalence scaling: $\pi_{\text{val}} = 0.035\%$ vs. $\pi_{\text{test}} = 1.192\%$ (34x lift).
  - Added univariate test AUC reporting for `capital_ratio` (0.9522) and `capital_to_loans` (0.9556) to prove that high test discrimination is driven by extreme observable balance-sheet deterioration during the GFC.
  - Generated full confusion matrices, precision, recall, Brier score, and cost curves.

### 7. `src/convert_md_to_docx.py` & `src/package_submission.py`
- **Issue:** Needed automatic synchronization with updated numbers, tables, and figures.
- **Modification:**
  - Updated Word document generator with updated benchmark figures, tables, and audit insights.
  - Rebuilt submission packaging script to assemble all finalized files into `file nộp` and `file_nop.zip`.

---

## III. IMPLEMENTATION TIMELINE & VERIFICATION MATRIX

1. **Step 1:** Update `requirements.txt`.
2. **Step 2:** Refactor `src/01_storage_benchmark.py` (Seaborn warnings, extrapolation formulas).
3. **Step 3:** Refactor `src/02_data_quality_and_panel.py` (itemized failure filtering, negative equity justification).
4. **Step 4:** Refactor `src/03_scalable_processing.py` (`psutil` ProcessMemoryTracker, honest memory comparisons).
5. **Step 5:** Refactor `src/04_feature_pipeline.py` (265 failure reconciliation, publication lag documentation).
6. **Step 6:** Refactor `src/05_modelling_validation.py` (LightGBM warning fix, PR-AUC prevalence derivations).
7. **Step 7:** Run `python run_all.py` to regenerate all tables, figures, and reports.
8. **Step 8:** Run `python src/convert_md_to_docx.py` and `python src/package_submission.py`.
9. **Step 9:** Update `README.md` and `report/FINAL_REPORT_BFT65B.md`.
10. **Step 10:** Final review and grading assessment.
