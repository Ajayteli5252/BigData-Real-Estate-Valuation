# 🏠 Real Estate Price Analysis and Prediction Using Hadoop

[![Course](https://img.shields.io/badge/Course-CSE3712_Big_Data_Analytics-blue?style=for-the-badge)](https://github.com/Ajayteli5252/BigData-Real-Estate-Valuation)
[![Dataset](https://img.shields.io/badge/Dataset-HM_Land_Registry_Price_Paid_(Non--Kaggle)-brightgreen?style=for-the-badge)](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)
[![Processing](https://img.shields.io/badge/Processing-Apache_Hadoop_MapReduce_on_YARN-red?style=for-the-badge)](https://hadoop.apache.org/)
[![Machine Learning](https://img.shields.io/badge/ML-Scikit--Learn_Ridge_Pipeline-orange?style=for-the-badge)](https://scikit-learn.org/)
[![UI](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?style=for-the-badge)](https://streamlit.io/)

---

## 📌 Executive Summary

This project delivers an authentic, end-to-end **Big Data Analytics (BDA)** system for property transaction analysis and price prediction across England & Wales, designed specifically to demonstrate three core competencies to faculty:

1. **Real Data Ingestion & Auditing:** Ingesting **1,939,188 official property transactions** (2022–2023) from the UK HM Land Registry Open Data repository (100% Non-Kaggle).
2. **Distributed Processing with Apache Hadoop:** Implementing a custom Python **MapReduce Streaming** pipeline on **YARN** and **HDFS** that parses, validates, filters, deduplicates, and resolves conflicts with exact counter reconciliation.
3. **Exploratory Visualizations & Machine Learning:** Serving an interactive single-page **Streamlit dashboard** with market price trends and a **Scikit-Learn Ridge Regression** valuation engine achieving a **25.9% error reduction** over baseline out-of-time property valuation.

---

## 🏗️ System Architecture

```mermaid
flowchart TD
    subgraph S1 ["1. Raw Data Storage (HDFS)"]
        R1["HM Land Registry CSVs\n(pp-2022.csv & pp-2023.csv)\n1,939,188 Raw Records"]
        H1["HDFS Raw Directory\n/data/realestate/raw/"]
        R1 -->|hdfs dfs -put| H1
    end

    subgraph S2 ["2. Apache Hadoop MapReduce on YARN"]
        M1["Hadoop Streaming Mapper\n(scripts/mapper.py)\n- Python csv parser\n- £15K–£20M filter\n- Normalization & Date Extraction"]
        SH["Shuffle & Sort Phase\n(Partition & Group by Transaction ID)"]
        RD["Hadoop Streaming Reducer\n(scripts/reducer.py)\n- Deduplication of identical rows\n- Exclusion of conflicting rows\n- Final clean TSV emission"]
        H1 --> M1
        M1 -->|Key: Transaction ID\nValue: Normalized Fields| SH
        SH --> RD
    end

    subgraph S3 ["3. Clean Data Storage & Export"]
        HC["HDFS Clean Output\n/data/realestate/clean/"]
        LC["Local Clean TSV & Audit JSON\ndata/clean/sample/\ndata/clean/full/"]
        RD --> HC
        HC -->|hdfs dfs -getmerge| LC
    end

    subgraph S4 ["4. Analytics, ML & Interactive UI"]
        DASH["Streamlit Web Dashboard\n(app/dashboard.py)\n- Audit Counters Reconciliation\n- 3 Market Visualizations\n- Interactive Price Estimation"]
        ML["Scikit-Learn Ridge Pipeline\n(scripts/train_model.py)\n- Temporal Train 2022 / Test 2023\n- Log-Transformed Target\n- Out-of-Time Test MAE: £155,438"]
        LC --> DASH
        LC --> ML
        ML -->|models/ridge_pipeline.joblib| DASH
    end
```

---

## 🧹 How We Cleaned the Data

All data cleaning is performed inside the **Hadoop Streaming MapReduce job**, reading from and writing to HDFS.

### 1. Selected Fields & Normalization

The official HM Land Registry Price Paid dataset contains 16 CSV columns without headers. The Mapper extracts and standardizes the following 6 core attributes:

| Column Index | Field Name | Transformation & Standardization Applied |
| :---: | :--- | :--- |
| `0` | `transaction_id` | Stripped leading/trailing braces `{...}` to produce a clean UUID string key. |
| `1` | `price` | Converted to integer. Strictly filtered between **£15,000** and **£20,000,000** (removes nominal legal transfers and institutional outliers). |
| `2` | `sale_date` | Parsed from `YYYY-MM-DD HH:MM` into standard `YYYY-MM-DD`. Derived `sale_year` (int) and `sale_month` (int). |
| `4` | `property_type` | Validated against official UK categories: `D` (Detached), `S` (Semi-Detached), `T` (Terraced), `F` (Flat/Maisonette), `O` (Other). |
| `6` | `tenure` | Validated against tenure codes: `F` (Freehold), `L` (Leasehold). |
| `13` | `county` | Stripped whitespace, converted to standard uppercase English county (e.g., `TYNE AND WEAR`, `GREATER LONDON`). |

### 2. Output Column Order (Reducer Output)

The Reducer emits tab-separated records (`TSV`) in the following documented column layout:

$$\text{transaction\_id} \quad \text{price} \quad \text{sale\_date} \quad \text{sale\_year} \quad \text{sale\_month} \quad \text{county} \quad \text{property\_type} \quad \text{tenure}$$

### 3. Actual Raw vs. Cleaned Row Example

#### Actual Raw Input Row (from `data/raw/pp-2023.csv`):
```text
"{01EB45F0-49DB-40F3-E063-4704A8C05FDE}",169950,"2023-07-05 00:00","NE28 7JR","T","N","F","1","","CESTRIUM COURT","","WALLSEND","NORTH TYNESIDE","TYNE AND WEAR","A","A"
```

#### Final Cleaned Output Row (from `data/clean/full/clean_sales.tsv`):
```tsv
01EB45F0-49DB-40F3-E063-4704A8C05FDE	169950	2023-07-05	2023	07	TYNE AND WEAR	T	F
```

---

### 4. Edge-Case Handling (Demonstration Examples)

To test the resilience of the MapReduce job, a set of labeled edge cases was constructed and verified:

#### A. Identical Duplicates (`duplicate_rows_removed`)
* **Input Records:** Two rows sharing identical transaction ID `{TEST-DUP-01}` and identical normalized fields:
  ```text
  "{TEST-DUP-01}",250000,"2022-03-15 00:00","SW1A 1AA","F","N","L","1","","STREET","","LONDON","WESTMINSTER","GREATER LONDON","A","A"
  "{TEST-DUP-01}",250000,"2022-03-15 00:00","SW1A 1AA","F","N","L","1","","STREET","","LONDON","WESTMINSTER","GREATER LONDON","A","A"
  ```
* **Reducer Action:** Emits exactly **one** clean row. The second record increments the Hadoop counter `duplicate_rows_removed` (+1).

#### B. Conflicting Records (`conflicting_rows_excluded`)
* **Input Records:** Two rows sharing the same transaction ID `{TEST-CONF-01}` but having conflicting prices or attributes:
  ```text
  "{TEST-CONF-01}",300000,"2022-04-10 00:00","M1 1AD","S","N","F","10","","HIGH ST","","MANCHESTER","MANCHESTER","GREATER MANCHESTER","A","A"
  "{TEST-CONF-01}",450000,"2022-04-10 00:00","M1 1AD","S","N","F","10","","HIGH ST","","MANCHESTER","MANCHESTER","GREATER MANCHESTER","A","A"
  ```
* **Reducer Action:** Rejects **both** rows entirely to prevent ambiguous data poisoning. Increments the Hadoop counter `conflicting_rows_excluded` (+2). Zero rows for this ID enter the final dataset.

#### C. Invalid / Out-of-Range Records (`rejected_rows`)
* **Input Record (Below minimum threshold):**
  ```text
  "{TEST-INV-01}",5000,"2022-01-01 00:00","B1 1AA","D","N","F","...","WEST MIDLANDS","A","A"
  ```
* **Mapper Action:** Price £5,000 is below the £15,000 threshold. The mapper discards the row immediately, reporting `reporter:counter:DataCleaning,rejected_rows,1` and `reporter:counter:DataCleaning,out_of_range_price,1` to stderr.

---

### 5. Hadoop Counter Reconciliation Accounting

Every single row entering the pipeline is mathematically accounted for according to the strict conservation formula:

$$\text{raw\_rows} = \text{rejected\_rows} + \text{duplicate\_rows\_removed} + \text{conflicting\_rows\_excluded} + \text{final\_clean\_rows}$$

| Pipeline Metric | Sample Mode (Classroom Demo) | Full Mode (Complete Dataset) | Accounting Formula Term |
| :--- | :---: | :---: | :--- |
| **Total Raw Input Rows** | **20,020** | **1,939,188** | $\text{raw\_rows}$ |
| **[-] Mapper Rejected Rows** | 54 | 10,077 | $\text{rejected\_rows}$ |
| *• Missing required fields* | 2 | 0 | |
| *• Invalid price formats* | 2 | 0 | |
| *• Out-of-range prices (<£15k or >£20M)* | 46 | 10,077 | |
| *• Invalid date formats* | 2 | 0 | |
| *• Unsupported category codes* | 2 | 0 | |
| **[-] Duplicate Rows Removed** | 5 | 0 | $\text{duplicate\_rows\_removed}$ |
| **[-] Conflicting Rows Excluded** | 4 | 0 | $\text{conflicting\_rows\_excluded}$ |
| **[=] Final Clean Output Rows** | **19,957** | **1,929,111** | $\text{final\_clean\_rows}$ |
| **Reconciliation Status** | **PASSED (Exact)** | **PASSED (Exact)** | $20,020 == 54 + 5 + 4 + 19,957$ |

---

## 💻 Hadoop HDFS & YARN Execution Commands

### 1. Cluster Setup & Daemon Verification
```bash
# Start HDFS and YARN daemons on WSL2 Ubuntu
start-dfs.sh
start-yarn.sh

# Confirm running Java processes
jps
# Output shows: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```

### 2. Upload Raw Files to HDFS
```bash
# Create HDFS input directories
hdfs dfs -mkdir -p /data/realestate/raw/sample/
hdfs dfs -mkdir -p /data/realestate/raw/full/

# Upload raw CSVs unchanged into HDFS
hdfs dfs -put -f data/sample/raw/sample_raw.csv /data/realestate/raw/sample/
hdfs dfs -put -f data/raw/pp-2022.csv /data/realestate/raw/full/
hdfs dfs -put -f data/raw/pp-2023.csv /data/realestate/raw/full/

# List raw files in HDFS
hdfs dfs -ls /data/realestate/raw/sample/
```

### 3. Submit Hadoop Streaming Job to YARN
```bash
# Reset output directory (safe rerun without touching raw files)
hdfs dfs -rm -r -f /data/realestate/clean/sample/

# Submit MapReduce job
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files scripts/mapper.py,scripts/reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /data/realestate/raw/sample/* \
    -output /data/realestate/clean/sample/
```

### 4. Inspect Hadoop Counters & Output in HDFS
```bash
# Preview first 5 clean records in HDFS
hdfs dfs -cat /data/realestate/clean/sample/part-* | head -n 5

# Export clean output from HDFS to local project
hdfs dfs -getmerge /data/realestate/clean/sample/ data/clean/sample/clean_sales.tsv
```

*(Note: For environments without active Hadoop clusters, `python scripts/run_local_streaming.py --mode sample` executes an identical stream simulation with matching counters).*

---

## 📊 Streamlit Interactive Dashboard

The Streamlit dashboard (`app/dashboard.py`) provides an interactive interface featuring:
1. **Pipeline Mode Selector:** Toggle between *Sample Mode* (demo) and *Full Mode* (1.93M records).
2. **Data Preparation Summary Card:** Real-time Hadoop counter metrics and mathematical balance confirmation.
3. **Three Clean Property Price Charts:**
   - **Chart 1:** Median price for the 10 counties with the highest sales volume.
   - **Chart 2:** Median price broken down by property type (Detached, Semi-Detached, Terraced, Flat, Other).
   - **Chart 3:** Monthly median price chronological trend from January 2022 to December 2023.
4. **Interactive "Estimate Sale Price" Form:** Allows user to select county, property type, tenure, sale year, and month to receive instant Ridge regression predictions.

To launch:
```bash
streamlit run app/dashboard.py
```
*Access at: `http://localhost:8501`*

---

## 🤖 Machine Learning Model & Viva Metrics

We implemented a single, interpretable **Scikit-Learn Ridge Regression** pipeline (`TransformedTargetRegressor` modeling price on $\log(1+y)$ scale):
* **Features:** `county`, `property_type`, `tenure` (OneHotEncoded with `handle_unknown='ignore'`), `sale_year`, `sale_month` (StandardScaler).
* **Train / Test Split:** Strict temporal out-of-time evaluation (trained on 2022 sales, evaluated on 2023 sales).

### Quantitative Results (Full Dataset: 1,929,111 Clean Rows)

| Model Metric | Full Mode Value | Sample Mode Value | Interpretation |
| :--- | :---: | :---: | :--- |
| **Training Records (2022)** | 1,071,952 | 9,997 | Historical baseline |
| **Testing Records (2023)** | 857,159 | 9,960 | Out-of-time future evaluation |
| **Training Median Price** | £280,000 | £290,000 | Naive prediction benchmark |
| **Baseline Test MAE** | £209,672 | £179,543 | Error if predicting median |
| **Ridge Model Test MAE** | **£155,438** | **£142,205** | Actual pipeline test error |
| **MAE Improvement** | **£54,234 (25.9%)** | **£37,338 (20.8%)** | Substantial accuracy boost |
| **Log-Scale $R^2$** | **0.4884** | **0.3341** | Variance explained |

> **🎓 One-Minute Viva Explanation for Faculty:**  
> *"The Ridge regression pipeline achieves an out-of-time test MAE of £155,438 on 2023 sales, which reduces valuation error by £54,234 (a 25.9% improvement) compared to the baseline of always predicting the 2022 training median price of £280,000."*

---

## 🚀 Quickstart & Demo Sequence

### Prerequisites
* Windows 11 with Python 3.10+ installed.
* WSL2 Ubuntu (for native Hadoop execution) or standard Windows PowerShell (for local streaming simulation).

```bash
# 1. Install required dependencies
pip install -r requirements.txt

# 2. Ingest raw 2022 and 2023 data
python scripts/01_ingest_data.py

# 3. Generate sample demo data
python scripts/create_sample_data.py

# 4. Run Hadoop cleaning pipeline (Sample Mode)
python scripts/run_local_streaming.py --mode sample

# 5. Train Ridge regression model
python scripts/train_model.py --mode sample

# 6. Launch web dashboard
python run_dashboard.py
```

---

## 📋 Summary of Changes (Kept vs. Removed)

| Component Category | Retained & Streamlined | Removed Obsolete Components |
| :--- | :--- | :--- |
| **Processing Engine** | Apache Hadoop MapReduce (`mapper.py`, `reducer.py`, `run_hadoop_job.sh`) | DuckDB OLAP scripts, complex Parquet pipelines |
| **Geographic Features** | Direct county field from sales data (`county`) | Postcode directory downloads, Haversine distances to cities, Uber H3 spatial hexagons |
| **Machine Learning** | Single Scikit-Learn Ridge pipeline with temporal split | LightGBM, XGBoost, Quantile loss models, SHAP tree explainability, Arbitrage detection |
| **API & Delivery** | Single-page Streamlit Dashboard (`app/dashboard.py`) | FastAPI microservice (`app/api.py`), API tests, obsolete Jupyter notebooks |
| **Dependencies** | Minimal stack: `pandas`, `numpy`, `scikit-learn`, `streamlit`, `matplotlib`, `joblib` | `duckdb`, `pyarrow`, `h3`, `lightgbm`, `fastapi`, `uvicorn`, `pydantic` |
