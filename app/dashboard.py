"""
Streamlit Web Application: Real Estate Price Analysis and Prediction Using Hadoop
CSE3712 — Big Data Analytics Project

Faculty Demonstration Dashboard:
1. Data Preparation & Hadoop Cleaning Summary (Counters & Mathematical Reconciliation)
2. Exploratory Market Analytics (Three Clean Property Price Charts)
3. Price Valuation Form (Scikit-Learn Ridge Regression Pipeline)
"""

import os
import sys
import json
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import streamlit as st

# Set Page Config
st.set_page_config(
    page_title="Real Estate Price Analysis Using Hadoop",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
<style>
    .reportview-container .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }
    .metric-card {
        background-color: #0e1726;
        border: 1px solid #1e293b;
        border-radius: 10px;
        padding: 16px;
        text-align: center;
    }
    .metric-title {
        font-size: 0.85rem;
        color: #94a3b8;
        text-transform: uppercase;
        font-weight: 600;
        letter-spacing: 0.05em;
    }
    .metric-num {
        font-size: 1.8rem;
        font-weight: 700;
        color: #f8fafc;
        margin-top: 4px;
    }
    .sub-banner {
        background-color: #1e293b;
        border-left: 4px solid #3b82f6;
        padding: 10px 16px;
        border-radius: 4px;
        margin-bottom: 20px;
        color: #cbd5e1;
        font-size: 0.95rem;
    }
    .reconciliation-box {
        background-color: #064e3b;
        border: 1px solid #059669;
        border-radius: 8px;
        padding: 12px 18px;
        color: #ecfdf5;
        font-family: monospace;
        font-size: 0.95rem;
        margin-top: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Readable Mappings
PROPERTY_TYPE_MAP = {
    "D": "Detached",
    "S": "Semi-Detached",
    "T": "Terraced",
    "F": "Flat / Maisonette",
    "O": "Other"
}

TENURE_MAP = {
    "F": "Freehold",
    "L": "Leasehold"
}

COL_NAMES = [
    "transaction_id", "price", "sale_date", "sale_year",
    "sale_month", "county", "property_type", "tenure"
]


@st.cache_data
def load_cleaning_summary(mode: str):
    """Load JSON cleaning summary produced by Hadoop Streaming job."""
    summary_path = os.path.join("data", "clean", mode, "cleaning_summary.json")
    if not os.path.exists(summary_path):
        return None
    with open(summary_path, "r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data
def load_clean_data(mode: str, sample_max_rows: int = 200000):
    """Load clean sales TSV output from Hadoop MapReduce."""
    tsv_path = os.path.join("data", "clean", mode, "clean_sales.tsv")
    if not os.path.exists(tsv_path):
        return None

    # For full mode in interactive dashboard, load representative sample for responsive charts
    nrows = sample_max_rows if mode == "full" else None

    df = pd.read_csv(
        tsv_path,
        sep="\t",
        names=COL_NAMES,
        header=None,
        nrows=nrows,
        dtype={
            "price": "int64",
            "sale_year": "int32",
            "sale_month": "int32",
            "county": "string",
            "property_type": "string",
            "tenure": "string"
        }
    )
    return df


@st.cache_resource
def load_trained_model():
    """Load saved scikit-learn Ridge regression pipeline."""
    model_path = os.path.join("models", "ridge_pipeline.joblib")
    metrics_path = os.path.join("models", "model_metrics.json")
    
    model = joblib.load(model_path) if os.path.exists(model_path) else None
    metrics = None
    if os.path.exists(metrics_path):
        with open(metrics_path, "r", encoding="utf-8") as f:
            metrics = json.load(f)
    return model, metrics


# ---------------------------------------------------------
# Sidebar Controls
# ---------------------------------------------------------
with st.sidebar:
    st.header("⚙️ Pipeline Controls")
    
    # Available modes check
    has_sample = os.path.exists(os.path.join("data", "clean", "sample", "cleaning_summary.json"))
    has_full = os.path.exists(os.path.join("data", "clean", "full", "cleaning_summary.json"))

    mode_options = []
    if has_sample:
        mode_options.append("Sample Mode (Classroom Demo)")
    if has_full:
        mode_options.append("Full Mode (1.93M Transactions)")
    if not mode_options:
        mode_options = ["Sample Mode (Classroom Demo)"]

    selected_mode_label = st.radio(
        "Select Dataset Mode:",
        options=mode_options,
        index=0
    )
    current_mode = "sample" if "Sample" in selected_mode_label else "full"

    st.markdown("---")
    st.subheader("📚 Project Information")
    st.markdown("""
    **Course:** CSE3712 — Big Data Analytics  
    **Topic:** Real Estate Price Analysis and Prediction Using Hadoop  
    **Data Source:** UK HM Land Registry Price Paid (2022–2023)  
    **Processing Engine:** Apache Hadoop Streaming (YARN / HDFS)  
    **ML Model:** Scikit-Learn Ridge Regression Pipeline  
    """)
    st.markdown("---")
    st.caption("Single-node Hadoop cluster configuration with Python mapper/reducer on WSL2 Ubuntu.")


# ---------------------------------------------------------
# Header & Short Summary Banner
# ---------------------------------------------------------
st.title("🏠 Real Estate Price Analysis and Prediction Using Hadoop")
st.markdown(
    f'<div class="sub-banner">'
    f'<strong>CSE3712 Big Data Analytics Demonstration</strong> | '
    f'Processed via Apache Hadoop Streaming on HDFS & YARN | '
    f'Dataset: HM Land Registry Official Price Paid (2022–2023) | '
    f'Active View: <strong>{selected_mode_label.upper()}</strong>'
    f'</div>',
    unsafe_allow_html=True
)

summary_data = load_cleaning_summary(current_mode)
df_clean = load_clean_data(current_mode)
model_pipeline, model_metrics = load_trained_model()

if summary_data is None or df_clean is None:
    st.error(
        f"Cleaned dataset for `{current_mode}` mode not found. "
        f"Please run the Hadoop pipeline first:\n"
        f"`python scripts/run_local_streaming.py --mode {current_mode}`"
    )
    st.stop()


# ---------------------------------------------------------
# SECTION 1: Data Preparation & Hadoop Cleaning Counters
# ---------------------------------------------------------
st.subheader("1. Data Preparation & Hadoop MapReduce Cleaning Audit")
st.markdown(
    "All cleaning rules are executed strictly inside the **Hadoop Streaming Mapper and Reducer**. "
    "Every row is accounted for with zero data leakage or untracked loss."
)

counters = summary_data.get("counters", {})
raw_count = counters.get("raw_rows", 0)
rejected_count = counters.get("rejected_rows", 0)
duplicate_count = counters.get("duplicate_rows_removed", 0)
conflict_count = counters.get("conflicting_rows_excluded", 0)
clean_count = counters.get("final_clean_rows", 0)

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total Raw Rows", f"{raw_count:,}")
with col2:
    st.metric("Mapper Rejected", f"{rejected_count:,}")
with col3:
    st.metric("Duplicates Removed", f"{duplicate_count:,}")
with col4:
    st.metric("Conflicts Excluded", f"{conflict_count:,}")
with col5:
    st.metric("Final Clean Rows", f"{clean_count:,}")

# Reconciliation Check
is_balanced = raw_count == (rejected_count + duplicate_count + conflict_count + clean_count)
status_symbol = "✅" if is_balanced else "❌"
status_text = "PASSED (EXACT MATHEMATICAL MATCH)" if is_balanced else "MISMATCH DETECTED"

st.markdown(
    f'<div class="reconciliation-box">'
    f'{status_symbol} <strong>Hadoop Counter Reconciliation Check:</strong> {status_text}<br/>'
    f'<strong>Formula:</strong> raw_rows ({raw_count:,}) = rejected ({rejected_count:,}) + '
    f'duplicates ({duplicate_count:,}) + conflicts ({conflict_count:,}) + clean ({clean_count:,})'
    f'</div>',
    unsafe_allow_html=True
)

with st.expander("🔍 View Mapper Rejection Breakdown & Hadoop Filter Rules"):
    rej_bk = summary_data.get("rejection_breakdown", {})
    rcol1, rcol2, rcol3 = st.columns(3)
    with rcol1:
        st.write(f"• **Missing Required Fields:** {rej_bk.get('missing_required_field', 0):,}")
        st.write(f"• **Invalid Price Formats:** {rej_bk.get('invalid_price', 0):,}")
    with rcol2:
        st.write(f"• **Out-of-Range Prices (<£15k or >£20M):** {rej_bk.get('out_of_range_price', 0):,}")
        st.write(f"• **Invalid Date Formats:** {rej_bk.get('invalid_date', 0):,}")
    with rcol3:
        st.write(f"• **Unsupported Category Codes:** {rej_bk.get('unsupported_category', 0):,}")
        st.write(f"• **Cleaning Job Elapsed Time:** {summary_data.get('elapsed_seconds', 0):.2f} seconds")

st.markdown("---")


# ---------------------------------------------------------
# SECTION 2: Exploratory Market Analytics (Three Charts)
# ---------------------------------------------------------
st.subheader("2. Market Price Visualizations (Cleaned Dataset)")
st.markdown("Key real estate trends computed directly from the cleaned Hadoop output.")

chart_col1, chart_col2 = st.columns(2)

# Chart 1: Median Price for Top 10 Counties by Volume
with chart_col1:
    st.markdown("##### 📍 Top 10 Counties by Volume — Median Sale Price")
    top_10_counties = df_clean["county"].value_counts().head(10).index
    county_medians = (
        df_clean[df_clean["county"].isin(top_10_counties)]
        .groupby("county")["price"]
        .median()
        .sort_values(ascending=True)
    )

    fig1, ax1 = plt.subplots(figsize=(7, 4.2))
    bars1 = ax1.barh(county_medians.index, county_medians.values, color="#3b82f6", edgecolor="#1e3a8a")
    ax1.set_xlabel("Median Price (£)", fontsize=10)
    ax1.xaxis.set_major_formatter(ticker.StrMethodFormatter("£{x:,.0f}"))
    ax1.grid(axis="x", linestyle="--", alpha=0.3)
    ax1.set_title("Median Price Across Most Active Counties", fontsize=11, fontweight="bold")
    plt.tight_layout()
    st.pyplot(fig1)

# Chart 2: Median Price by Property Type
with chart_col2:
    st.markdown("##### 🏢 Median Price by Property Type")
    df_clean_prop = df_clean.copy()
    df_clean_prop["property_name"] = df_clean_prop["property_type"].map(PROPERTY_TYPE_MAP).fillna(df_clean_prop["property_type"])
    prop_medians = df_clean_prop.groupby("property_name")["price"].median().sort_values(ascending=False)

    fig2, ax2 = plt.subplots(figsize=(7, 4.2))
    bars2 = ax2.bar(prop_medians.index, prop_medians.values, color="#10b981", edgecolor="#065f46")
    ax2.set_ylabel("Median Price (£)", fontsize=10)
    ax2.yaxis.set_major_formatter(ticker.StrMethodFormatter("£{x:,.0f}"))
    ax2.set_title("Median Price by Property Classification", fontsize=11, fontweight="bold")
    ax2.grid(axis="y", linestyle="--", alpha=0.3)
    plt.xticks(rotation=20, ha="right", fontsize=9)
    plt.tight_layout()
    st.pyplot(fig2)

# Chart 3: Monthly Median Price Chronological Trend
st.markdown("##### 📈 Monthly Median Price Chronological Trend (2022 – 2023)")
df_clean["year_month"] = (
    df_clean["sale_year"].astype(str) + "-" + 
    df_clean["sale_month"].astype(str).str.zfill(2)
)
monthly_trend = df_clean.groupby("year_month")["price"].median().sort_index()

fig3, ax3 = plt.subplots(figsize=(12, 3.5))
ax3.plot(monthly_trend.index, monthly_trend.values, marker="o", color="#f59e0b", linewidth=2.5, markersize=6)
ax3.set_ylabel("Median Price (£)", fontsize=10)
ax3.yaxis.set_major_formatter(ticker.StrMethodFormatter("£{x:,.0f}"))
ax3.set_title("Chronological Price Evolution (HM Land Registry Price Paid)", fontsize=11, fontweight="bold")
ax3.grid(True, linestyle="--", alpha=0.3)
plt.xticks(rotation=45, ha="right", fontsize=9)
plt.tight_layout()
st.pyplot(fig3)

st.markdown("---")


# ---------------------------------------------------------
# SECTION 3: Estimate Sale Price (Prediction Model)
# ---------------------------------------------------------
st.subheader("3. Property Valuation Engine (Scikit-Learn Ridge Regression)")
st.markdown(
    "Estimate the fair market value of any property based on historical sales data. "
    "Trained on 2022 transactions and evaluated out-of-time on 2023 transactions."
)

form_col, result_col = st.columns([1.2, 1])

# County list
available_counties = sorted(df_clean["county"].dropna().unique().tolist())
if "GREATER LONDON" in available_counties:
    default_county_idx = available_counties.index("GREATER LONDON")
else:
    default_county_idx = 0

with form_col:
    with st.form("valuation_form"):
        st.markdown("**Enter Property Specifications:**")
        
        county_input = st.selectbox("County:", available_counties, index=default_county_idx)
        
        prop_display_options = [f"{v} ({k})" for k, v in PROPERTY_TYPE_MAP.items()]
        prop_type_sel = st.selectbox("Property Type:", prop_display_options, index=1)
        prop_code = prop_type_sel.split("(")[-1].replace(")", "").strip()
        
        tenure_sel = st.radio("Tenure:", ["Freehold (F)", "Leasehold (L)"], horizontal=True)
        tenure_code = "F" if "Freehold" in tenure_sel else "L"
        
        c1, c2 = st.columns(2)
        with c1:
            year_input = st.selectbox("Sale Year:", [2022, 2023], index=1)
        with c2:
            month_input = st.slider("Sale Month:", min_value=1, max_value=12, value=6)
            
        submit_btn = st.form_submit_button("🔮 Estimate Property Price", use_container_width=True)

with result_col:
    st.markdown("**Estimated Valuation & Viva Metrics:**")
    
    if submit_btn and model_pipeline is not None:
        # Construct input dataframe
        input_data = pd.DataFrame([{
            "county": county_input,
            "property_type": prop_code,
            "tenure": tenure_code,
            "sale_year": int(year_input),
            "sale_month": int(month_input)
        }])
        
        try:
            pred_price = model_pipeline.predict(input_data)[0]
            pred_price = max(pred_price, 15000.0)  # Floor at project minimum
            
            st.success(f"### Estimated Sale Price: £{pred_price:,.0f}")
            st.markdown(f"""
            - **Location:** {county_input}
            - **Property Type:** {PROPERTY_TYPE_MAP.get(prop_code, prop_code)}
            - **Tenure:** {TENURE_MAP.get(tenure_code, tenure_code)}
            - **Period:** {year_input}-{month_input:02d}
            """)
        except Exception as e:
            st.error(f"Prediction failed: {e}")
    else:
        st.info("Select property attributes on the left and click **Estimate Property Price**.")

    if model_metrics:
        with st.container():
            st.markdown("##### 🎓 Faculty Viva Model Metrics")
            st.markdown(f"""
            - **Model Algorithm:** Scikit-Learn `Ridge(alpha=100.0)` in `TransformedTargetRegressor` (Log-Price Scale)
            - **Test Dataset (2023) MAE:** **£{model_metrics.get('test_mae_gbp', 0):,.0f}**
            - **Baseline MAE (Median £{model_metrics.get('training_median_price_gbp', 0):,.0f}):** £{model_metrics.get('baseline_mae_gbp', 0):,.0f}
            - **Error Reduction:** **{model_metrics.get('mae_improvement_pct', 0):.1f}%** (£{model_metrics.get('mae_improvement_gbp', 0):,.0f} improvement)
            - **Viva Summary:** *"{model_metrics.get('viva_explanation', '')}"*
            """)
