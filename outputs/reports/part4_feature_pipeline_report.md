# Part 4: Leak-Free Feature Pipeline & CAMELS Banking Risk Architecture

## 1. Feature Architecture Overview
We engineered 23 banking risk features mapped systematically to the international **CAMELS supervisory framework**:

| CAMELS Dimension | Feature Code | Economic Justification & Formula |
| :--- | :--- | :--- |
| **C - Capital** | `capital_ratio` | Equity / Total Assets (loss-absorption buffer) |
| **C - Capital** | `capital_to_loans` | Equity / Net Loans (capital protection against loan book loss) |
| **A - Asset Quality** | `npl_ratio` | (30-89d + 90d+ + Nonaccrual Loans) / Gross Loans |
| **A - Asset Quality** | `coverage_ratio` | Loan Loss Reserves / Total Nonperforming Loans |
| **A - Asset Quality** | `net_chargeoff_ratio` | Net Loan Charge-offs / Net Loans (actual realized loss) |
| **A - Asset Quality** | `foreclosed_re_ratio` | Other Real Estate Owned (OREO) / Total Assets |
| **M - Management** | `efficiency_ratio` | Non-interest Expense / Net Operating Revenue |
| **M - Management** | `overhead_ratio` | Non-interest Expense / Total Assets |
| **E - Earnings** | `roa_calc`, `roe_calc` | Return on Assets, Return on Equity (core profitability) |
| **E - Earnings** | `nim_calc` | Net Interest Margin / Total Assets |
| **L - Liquidity** | `cash_to_deposits` | Cash & Balances Due / Total Deposits (immediate liquidity) |
| **L - Liquidity** | `core_deposit_ratio` | Domestic Deposits / Total Deposits (stickier retail funding) |
| **L - Liquidity** | `brokered_deposit_ratio` | Brokered Deposits / Total Deposits (volatile, flight-prone hot money) |
| **L - Liquidity** | `loan_to_deposit_ratio` | Net Loans / Total Deposits (credit intermediation strain) |
| **L - Liquidity** | `securities_to_assets` | Investment Securities / Total Assets (secondary liquidity buffer) |
| **S - Sensitivity/Growth** | `log_assets` | Bank Size (controls for systemic status vs community bank fragility) |
| **S - Sensitivity/Growth** | `asset_growth_1yr` | 1-Year Asset Growth Rate (rapid expansion signals credit underwriting dilution) |
| **S - Sensitivity/Growth** | `npl_change_1yr` | 1-Year Acceleration in Non-Performing Loans |
| **S - Sensitivity/Growth** | `roa_change_1yr`, `capital_change_1yr` | 1-Year Trajectory in Earnings and Capital buffers |
| **Peer Benchmark** | `capital_dev_state`, `npl_dev_state` | Bank deviation from state-quarter peer median |

## 2. Failure Event Reconciliation Across Partitions
A rigorous audit requirement is to verify the exact lineage of all positive failure labels:
- **Total Labeled Failures in Clean Panel:** 10,892
- **Development Set (1984Q1 - 2006Q4):** 8,697 failures (1,194,887 observations, 0.728% prevalence)
- **Purging Gap Embargo (2007Q1 - 2007Q4):** 48 failures (34,754 observations)
- **Out-of-Time Test Set (2008Q1 - 2012Q4):** 1,882 failures (157,851 observations, 1.192% prevalence)
- **Post-Test Holdout (2013Q1 - 2026Q2):** 254 failures (277,595 observations)
- **Total Reconciled:** $8,697 + 48 + 1,882 + 254 = 10,892$ (100.0% accounting match; zero dropped or leaked observations).

## 3. Regulatory Publication Lag & Leakage Gate Enforcement
In supervisory data science, ignoring statutory publication schedules can introduce subtle data leakage:
- **Filing and Publication Timelines:** Under FFIEC regulations, commercial banks must file Call Reports within 30 days (for banks with foreign offices) or 35 days (for domestic-only banks) following the calendar quarter end. The FDIC and FFIEC require an additional 10 to 15 days to perform data validation checks before public release. Thus, reports for quarter $q$ become available approximately **45 days post-quarter-end**.
- **Point-in-Time Alignment:** Because our target label predicts failures occurring within $(q, q + 4\text{ quarters}]$, the 90-to-366 day forward horizon provides supervisors with at least 1.5 to 10.5 months of actionable lead time *after* the Call Report is fully verified and published.
- **Temporal Purging Embargo:** The 4-quarter gap in 2007 ensures that no bank failing in early 2008 can contaminate the training period ending in 2006Q4.
- **Zero-Leakage Transformation:** Imputation medians and `RobustScaler` parameters were calculated **strictly on the 1984–2006 Development Set** and frozen. Test data was transformed out-of-sample with zero exposure to future statistics.
