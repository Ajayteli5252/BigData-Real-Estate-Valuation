"""
Streamlit Web Application: Property Finder
England & Wales · HM Land Registry recorded sales · 2022–2023
Search-first historical property sales platform powered by Apache Hadoop MapReduce
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

# -----------------------------------------------------------------------------
# Domain Mappings
# -----------------------------------------------------------------------------
PROPERTY_TYPE_MAP = {
    "D": "Detached",
    "S": "Semi-Detached",
    "T": "Terraced",
    "F": "Flat / Maisonette",
    "O": "Other"
}

PROPERTY_TYPE_REVERSE_MAP = {v: k for k, v in PROPERTY_TYPE_MAP.items()}

TENURE_MAP = {
    "F": "Freehold",
    "L": "Leasehold"
}

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# -----------------------------------------------------------------------------
# Data & Model Caching
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_cleaning_summary(mode: str):
    """Load JSON cleaning audit produced by Hadoop MapReduce."""
    summary_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clean", mode, "cleaning_summary.json")
    if not os.path.exists(summary_path):
        return None
    try:
        with open(summary_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def load_clean_data(mode: str):
    """
    Load clean sales TSV output from Hadoop MapReduce.
    Schema is always 10 columns:
      transaction_id, price, sale_date, sale_year, sale_month,
      county, property_type, tenure, postcode, town
    """
    # Resolve path relative to the project root (BDA/), not the CWD at runtime
    _project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tsv_path = os.path.join(_project_root, "data", "clean", mode, "clean_sales.tsv")
    if not os.path.exists(tsv_path):
        return None

    try:
        # Hardcode the 10-column schema — both Sample and Full outputs have this layout.
        col_names = [
            "transaction_id", "price", "sale_date", "sale_year",
            "sale_month", "county", "property_type", "tenure",
            "postcode", "town"
        ]
        dtypes = {
            "transaction_id": "string",
            "price": "int32",
            "sale_date": "string",
            "sale_year": "int16",
            "sale_month": "int8",
            "county": "string",
            "property_type": "string",
            "tenure": "string",
            "postcode": "string",
            "town": "string"
        }

        df = pd.read_csv(
            tsv_path,
            sep="\t",
            names=col_names,
            header=None,
            dtype=dtypes,
            on_bad_lines="skip"
        )
        if "postcode" not in df.columns:
            df["postcode"] = ""
        if "town" not in df.columns:
            df["town"] = ""
        
        df["postcode"] = df["postcode"].fillna("").astype(str)
        df["town"] = df["town"].fillna("").astype(str)
        df["county"] = df["county"].fillna("").astype(str)
        return df
    except Exception as e:
        print(f"Error loading {mode} dataset: {e}")
        return None


@st.cache_resource(show_spinner=False)
def load_trained_model(mode: str):
    """
    Load saved scikit-learn Ridge regression pipeline and mode-specific metrics.
    Ensures model metrics match the selected dataset mode.
    """
    _project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    mode_model_path = os.path.join(_project_root, "models", f"ridge_pipeline_{mode}.joblib")
    mode_metrics_path = os.path.join(_project_root, "models", f"model_metrics_{mode}.json")
    default_model_path = os.path.join(_project_root, "models", "ridge_pipeline.joblib")
    default_metrics_path = os.path.join(_project_root, "models", "model_metrics.json")

    model = None
    metrics = None

    if os.path.exists(mode_model_path) and os.path.exists(mode_metrics_path):
        model = joblib.load(mode_model_path)
        with open(mode_metrics_path, "r", encoding="utf-8") as f:
            metrics = json.load(f)
    elif os.path.exists(default_model_path) and os.path.exists(default_metrics_path):
        with open(default_metrics_path, "r", encoding="utf-8") as f:
            candidate_metrics = json.load(f)
        if candidate_metrics.get("dataset_mode") == mode:
            model = joblib.load(default_model_path)
            metrics = candidate_metrics

    return model, metrics


# -----------------------------------------------------------------------------
# Main Dashboard UI Runner
# -----------------------------------------------------------------------------
def main():
    # Page Configuration
    st.set_page_config(
        page_title="Property Finder · England & Wales",
        page_icon="🏠",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    # Compact Dark Theme & Visual Styling
    st.markdown("""
    <style>
        /* Global layout */
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2.5rem;
            max-width: 1240px;
        }
        header, footer { visibility: hidden; }

        /* Header styling */
        .header-title {
            font-size: 1.85rem;
            font-weight: 700;
            color: #f8fafc;
            letter-spacing: -0.02em;
            margin: 0;
            line-height: 1.2;
        }
        .header-sub {
            font-size: 0.88rem;
            color: #94a3b8;
            margin-top: 3px;
            margin-bottom: 0;
        }

        /* Summary Card Styling */
        .metric-card {
            background-color: #111827;
            border: 1px solid #1f2937;
            border-radius: 8px;
            padding: 14px 18px;
            transition: border-color 0.15s ease;
        }
        .metric-card:hover {
            border-color: #0d9488;
        }
        .metric-label {
            font-size: 0.76rem;
            color: #9ca3af;
            text-transform: uppercase;
            font-weight: 600;
            letter-spacing: 0.05em;
        }
        .metric-value {
            font-size: 1.7rem;
            font-weight: 700;
            color: #f9fafb;
            margin-top: 4px;
            line-height: 1.15;
        }
        .metric-delta-pos {
            font-size: 0.82rem;
            color: #34d399;
            font-weight: 600;
            margin-top: 4px;
        }
        .metric-delta-neg {
            font-size: 0.82rem;
            color: #f87171;
            font-weight: 600;
            margin-top: 4px;
        }
        .metric-delta-neutral {
            font-size: 0.82rem;
            color: #9ca3af;
            font-weight: 500;
            margin-top: 4px;
        }

        /* Result Card Styling */
        .sale-card {
            background-color: #0f172a;
            border: 1px solid #1e293b;
            border-radius: 8px;
            padding: 16px 20px;
            margin-bottom: 12px;
            transition: transform 0.1s ease, border-color 0.15s ease;
        }
        .sale-card:hover {
            border-color: #14b8a6;
        }
        .sale-price {
            font-size: 1.45rem;
            font-weight: 700;
            color: #14b8a6;
            letter-spacing: -0.01em;
        }
        .sale-location {
            font-size: 1.05rem;
            font-weight: 600;
            color: #f8fafc;
            margin-top: 2px;
        }
        .sale-meta {
            font-size: 0.84rem;
            color: #94a3b8;
            margin-top: 4px;
        }
        .tag {
            display: inline-block;
            background-color: #1e293b;
            color: #cbd5e1;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            margin-right: 6px;
        }
        .tag-tenure {
            background-color: #1e3a8a;
            color: #bfdbfe;
        }

        /* Detail view panel */
        .detail-panel {
            background-color: #0f172a;
            border: 1px solid #1e293b;
            border-radius: 10px;
            padding: 24px;
            margin-top: 12px;
            margin-bottom: 24px;
        }
    </style>
    """, unsafe_allow_html=True)

    # Session State Initialization
    if "active_view" not in st.session_state:
        st.session_state.active_view = "finder"  # 'finder', 'details', 'trends', 'estimate', 'audit'
    if "selected_tx_id" not in st.session_state:
        st.session_state.selected_tx_id = None
    if "search_location" not in st.session_state:
        st.session_state.search_location = ""
    if "search_prop_type" not in st.session_state:
        st.session_state.search_prop_type = "All"
    if "search_min_price" not in st.session_state:
        st.session_state.search_min_price = None
    if "search_max_price" not in st.session_state:
        st.session_state.search_max_price = None
    if "search_sort_by" not in st.session_state:
        st.session_state.search_sort_by = "Most recent sale"
    if "search_page" not in st.session_state:
        st.session_state.search_page = 1

    # Header Bar & Mode Selector
    _proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_available = os.path.exists(os.path.join(_proj, "data", "clean", "full", "clean_sales.tsv"))
    sample_available = os.path.exists(os.path.join(_proj, "data", "clean", "sample", "clean_sales.tsv"))

    mode_options = []
    if full_available:
        mode_options.append("Full Dataset (1.93M sales)")
    if sample_available:
        mode_options.append("Sample Dataset (20K sales)")
    if not mode_options:
        st.error("No cleaned data found in data/clean. Run scripts/run_hadoop_job.sh first.")
        st.stop()

    head_col1, head_col2 = st.columns([3, 1.2])

    with head_col1:
        st.markdown('<h1 class="header-title">Property Finder</h1>', unsafe_allow_html=True)
        st.markdown(
            '<p class="header-sub">England & Wales · HM Land Registry recorded sales · 2022–2023</p>',
            unsafe_allow_html=True
        )

    with head_col2:
        default_idx = 0 if full_available else (1 if len(mode_options) > 1 else 0)
        selected_mode_label = st.selectbox(
            "Active Dataset:",
            options=mode_options,
            index=default_idx,
            label_visibility="collapsed"
        )
        if not selected_mode_label:
            selected_mode_label = mode_options[0]
        current_mode = "full" if "Full" in str(selected_mode_label) else "sample"

    # Clear stale prediction if mode toggled
    if "last_mode" not in st.session_state:
        st.session_state.last_mode = current_mode
    elif st.session_state.last_mode != current_mode:
        st.session_state.last_mode = current_mode
        st.session_state.search_page = 1
        if "predicted_price" in st.session_state:
            del st.session_state["predicted_price"]

    # Load Data
    df = load_clean_data(current_mode)
    summary_data = load_cleaning_summary(current_mode)
    model_pipeline, model_metrics = load_trained_model(current_mode)

    if df is None or len(df) == 0:
        st.error(f"Cleaned dataset for `{current_mode}` mode is empty or missing.")
        st.stop()

    # Top Navigation Tabs
    nav_col1, nav_col2, nav_col3, nav_col4 = st.columns(4)
    with nav_col1:
        if st.button("🔍 Property Finder", use_container_width=True, type="primary" if st.session_state.active_view in ("finder", "details") else "secondary"):
            st.session_state.active_view = "finder"
            st.rerun()
    with nav_col2:
        if st.button("📈 Market Trends", use_container_width=True, type="primary" if st.session_state.active_view == "trends" else "secondary"):
            st.session_state.active_view = "trends"
            st.rerun()
    with nav_col3:
        if st.button("🏷️ Price Estimator", use_container_width=True, type="primary" if st.session_state.active_view == "estimate" else "secondary"):
            st.session_state.active_view = "estimate"
            st.rerun()
    with nav_col4:
        if st.button("⚙️ Data Audit", use_container_width=True, type="primary" if st.session_state.active_view == "audit" else "secondary"):
            st.session_state.active_view = "audit"
            st.rerun()

    st.markdown("<hr style='margin-top: 10px; margin-bottom: 16px; border: 0; border-top: 1px solid #1e293b;'/>", unsafe_allow_html=True)

    # -------------------------------------------------------------------------
    # VIEW: PROPERTY DETAILS (CLICK-THROUGH)
    # -------------------------------------------------------------------------
    if st.session_state.active_view == "details" and st.session_state.selected_tx_id:
        tx_id = st.session_state.selected_tx_id
        matched_records = df[df["transaction_id"] == tx_id]

        if st.button("← Back to Search Results"):
            st.session_state.active_view = "finder"
            st.rerun()

        if matched_records.empty:
            st.warning(f"Transaction ID `{tx_id}` not found in the {current_mode} dataset.")
        else:
            rec = matched_records.iloc[0]
            price = int(rec["price"])
            date_str = str(rec["sale_date"])
            year = int(rec["sale_year"])
            county = str(rec["county"])
            ptype_code = str(rec["property_type"])
            ptype_name = PROPERTY_TYPE_MAP.get(ptype_code, ptype_code)
            tenure_name = TENURE_MAP.get(str(rec["tenure"]), str(rec["tenure"]))
            postcode = str(rec.get("postcode", "")).strip()
            town = str(rec.get("town", "")).strip()

            location_display = []
            if postcode:
                location_display.append(postcode)
            if town:
                location_display.append(town)
            if county:
                location_display.append(county)
            full_loc = ", ".join(location_display) if location_display else "England & Wales"

            # Calculate Area Benchmark for Comparison
            peer_mask = (df["county"] == county) & (df["property_type"] == ptype_code) & (df["sale_year"] == year)
            peers = df[peer_mask]
            num_peers = len(peers)
            peer_median = peers["price"].median() if num_peers > 0 else None

            st.markdown(f"""
            <div class="detail-panel">
                <div style="font-size: 0.85rem; color: #94a3b8; text-transform: uppercase; font-weight: 600;">Recorded Transaction Details</div>
                <div style="font-size: 2.2rem; font-weight: 700; color: #14b8a6; margin-top: 4px;">Sold for £{price:,}</div>
                <div style="font-size: 1.25rem; font-weight: 600; color: #f8fafc; margin-top: 4px;">{full_loc}</div>
                <div style="margin-top: 10px;">
                    <span class="tag">{ptype_name}</span>
                    <span class="tag tag-tenure">{tenure_name}</span>
                    <span class="tag">Sale Date: {date_str}</span>
                </div>
                <div style="font-size: 0.82rem; color: #64748b; font-family: monospace; margin-top: 14px;">
                    Transaction ID: {tx_id}
                </div>
            </div>
            """, unsafe_allow_html=True)

            # Benchmark Comparison Section
            if peer_median is not None and num_peers > 0:
                diff = price - peer_median
                diff_pct = (diff / peer_median) * 100
                diff_label = f"+£{abs(diff):,.0f} (+{diff_pct:.1f}%)" if diff >= 0 else f"-£{abs(diff):,.0f} ({diff_pct:.1f}%)"
                comp_class = "metric-delta-pos" if diff >= 0 else "metric-delta-neg"
                status_text = "above" if diff > 0 else ("below" if diff < 0 else "equal to")

                st.subheader("Market Context & Area Benchmark")
                b_col1, b_col2, b_col3 = st.columns(3)
                with b_col1:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label">Sale Price</div>
                        <div class="metric-value">£{price:,}</div>
                        <div class="metric-delta-neutral">{date_str}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with b_col2:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label">{county} {year} Median</div>
                        <div class="metric-value">£{int(peer_median):,}</div>
                        <div class="metric-delta-neutral">Based on {num_peers:,} sales</div>
                    </div>
                    """, unsafe_allow_html=True)
                with b_col3:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label">Difference vs Median</div>
                        <div class="metric-value" style="font-size: 1.5rem;">{diff_label}</div>
                        <div class="{comp_class}">{status_text.capitalize()} local market median</div>
                    </div>
                    """, unsafe_allow_html=True)

                st.caption(
                    f"Benchmark is computed using all {num_peers:,} verified recorded sales of "
                    f"**{ptype_name}** properties in **{county}** transacted during calendar year **{year}**."
                )

            if st.button("Return to Results"):
                st.session_state.active_view = "finder"
                st.rerun()

    # -------------------------------------------------------------------------
    # VIEW: PROPERTY FINDER (HOME SEARCH SCREEN)
    # -------------------------------------------------------------------------
    elif st.session_state.active_view == "finder":
        # Scope Filter: Location and Property Type (No Budget restriction on Market Median)
        df_scope = df

        loc_query = st.session_state.search_location.strip()
        if loc_query:
            loc_clean = loc_query.upper().replace(" ", "")
            county_mask = df["county"].astype(str).str.contains(loc_query, case=False, na=False)
            town_mask = df["town"].astype(str).str.contains(loc_query, case=False, na=False)
            # Match postcode with or without spaces
            postcode_clean = df["postcode"].astype(str).str.replace(" ", "").str.upper()
            postcode_mask = postcode_clean.str.contains(loc_clean, case=False, na=False)
            df_scope = df_scope[county_mask | town_mask | postcode_mask]

        selected_type_label = st.session_state.search_prop_type
        if selected_type_label != "All":
            code = PROPERTY_TYPE_REVERSE_MAP.get(selected_type_label)
            if code:
                df_scope = df_scope[df_scope["property_type"] == code]

        # Calculate 3 Summary Cards based on Market Scope
        total_scope_sales = len(df_scope)
        if total_scope_sales > 0:
            overall_median = df_scope["price"].median()
            
            m_2022 = df_scope[df_scope["sale_year"] == 2022]["price"].median()
            m_2023 = df_scope[df_scope["sale_year"] == 2023]["price"].median()
            n_2022 = len(df_scope[df_scope["sale_year"] == 2022])
            n_2023 = len(df_scope[df_scope["sale_year"] == 2023])

            if pd.notna(m_2022) and pd.notna(m_2023) and m_2022 > 0 and n_2022 > 0 and n_2023 > 0:
                pct_change = ((m_2023 - m_2022) / m_2022) * 100
                diff_abs = m_2023 - m_2022
                delta_str = f"{pct_change:+.1f}% (£{diff_abs:+,.0f})"
                delta_class = "metric-delta-pos" if pct_change >= 0 else "metric-delta-neg"
            else:
                delta_str = "Insufficient data"
                delta_class = "metric-delta-neutral"
        else:
            overall_median = 0
            delta_str = "No records"
            delta_class = "metric-delta-neutral"

        # Scope Label
        scope_loc = loc_query.upper() if loc_query else "England & Wales"
        scope_type = selected_type_label if selected_type_label != "All" else "All Property Types"
        current_scope_label = f"Market Scope: {scope_loc} · {scope_type}"

        # Render Summary Cards
        sc_col1, sc_col2, sc_col3, sc_col4 = st.columns([1.2, 1.2, 1.2, 1.0])
        with sc_col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Median Sale Price</div>
                <div class="metric-value">£{int(overall_median):,}</div>
                <div class="metric-delta-neutral">Historical median</div>
            </div>
            """, unsafe_allow_html=True)

        with sc_col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">2023 vs 2022 Change</div>
                <div class="metric-value" style="font-size: 1.45rem;">{delta_str}</div>
                <div class="{delta_class}">Annual price shift</div>
            </div>
            """, unsafe_allow_html=True)

        with sc_col3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Recorded Sales</div>
                <div class="metric-value">{total_scope_sales:,}</div>
                <div class="metric-delta-neutral">Verified transactions</div>
            </div>
            """, unsafe_allow_html=True)

        with sc_col4:
            st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
            if st.button("📈 View Trends", use_container_width=True):
                st.session_state.active_view = "trends"
                st.rerun()
            st.caption(f"<div style='font-size: 0.74rem; color: #94a3b8; margin-top: 6px;'>{current_scope_label}</div>", unsafe_allow_html=True)

        st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)

        # Compact Search Form
        prop_type_options = ["All", "Detached", "Semi-Detached", "Terraced", "Flat / Maisonette", "Other"]

        with st.form("property_search_form", clear_on_submit=False):
            f_col1, f_col2, f_col3, f_col4, f_col5 = st.columns([2.2, 1.5, 1.2, 1.2, 1.2])

            with f_col1:
                form_loc = st.text_input(
                    "Location",
                    value=st.session_state.search_location,
                    placeholder="County, town, or postcode (e.g. London, NE28 7JR)"
                )
            with f_col2:
                form_type = st.selectbox(
                    "Property Type",
                    options=prop_type_options,
                    index=prop_type_options.index(st.session_state.search_prop_type) if st.session_state.search_prop_type in prop_type_options else 0
                )
            with f_col3:
                form_min = st.number_input(
                    "Min Price (£)",
                    min_value=0,
                    max_value=20000000,
                    step=10000,
                    value=int(st.session_state.search_min_price) if st.session_state.search_min_price else 0
                )
            with f_col4:
                form_max = st.number_input(
                    "Max Price (£)",
                    min_value=0,
                    max_value=20000000,
                    step=25000,
                    value=int(st.session_state.search_max_price) if st.session_state.search_max_price else 0
                )
            with f_col5:
                st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
                search_submitted = st.form_submit_button("Search sales", use_container_width=True, type="primary")

        # Form Submission Handler
        if search_submitted:
            st.session_state.search_location = form_loc
            st.session_state.search_prop_type = form_type
            st.session_state.search_min_price = form_min if form_min > 0 else None
            st.session_state.search_max_price = form_max if form_max > 0 else None
            st.session_state.search_page = 1
            st.rerun()

        # Clear filters button
        c_btn1, c_btn2 = st.columns([6, 1])
        with c_btn2:
            if st.button("Clear filters", use_container_width=True):
                st.session_state.search_location = ""
                st.session_state.search_prop_type = "All"
                st.session_state.search_min_price = None
                st.session_state.search_max_price = None
                st.session_state.search_page = 1
                st.rerun()

        # Apply Budget Range to Search Results
        min_budget = st.session_state.search_min_price
        max_budget = st.session_state.search_max_price

        if min_budget and max_budget and min_budget > max_budget:
            st.error("Invalid budget range: Minimum price cannot exceed Maximum price.")
            df_results = pd.DataFrame()
        else:
            df_results = df_scope
            if min_budget:
                df_results = df_results[df_results["price"] >= min_budget]
            if max_budget:
                df_results = df_results[df_results["price"] <= max_budget]

        total_matches = len(df_results)

        # Results Header & Sort Controls
        rh_col1, rh_col2 = st.columns([3, 1.5])
        with rh_col1:
            st.markdown(
                f"<div style='font-size: 1.05rem; font-weight: 600; color: #f8fafc; padding-top: 8px;'>"
                f"Found {total_matches:,} recorded sales"
                f"</div>",
                unsafe_allow_html=True
            )
        with rh_col2:
            sort_choice = st.selectbox(
                "Sort By:",
                options=["Most recent sale", "Lowest price", "Highest price"],
                index=0 if st.session_state.search_sort_by == "Most recent sale" else (1 if st.session_state.search_sort_by == "Lowest price" else 2),
                label_visibility="collapsed"
            )
            if sort_choice != st.session_state.search_sort_by:
                st.session_state.search_sort_by = sort_choice
                st.session_state.search_page = 1
                st.rerun()

        # Sorting
        if total_matches > 0:
            if st.session_state.search_sort_by == "Most recent sale":
                df_results = df_results.sort_values(by=["sale_date", "price"], ascending=[False, False])
            elif st.session_state.search_sort_by == "Lowest price":
                df_results = df_results.sort_values(by=["price", "sale_date"], ascending=[True, False])
            else:
                df_results = df_results.sort_values(by=["price", "sale_date"], ascending=[False, False])

        # Pagination
        PAGE_SIZE = 25
        total_pages = max(1, (total_matches + PAGE_SIZE - 1) // PAGE_SIZE)
        curr_page = min(st.session_state.search_page, total_pages)
        start_idx = (curr_page - 1) * PAGE_SIZE
        end_idx = min(start_idx + PAGE_SIZE, total_matches)

        page_records = df_results.iloc[start_idx:end_idx] if total_matches > 0 else pd.DataFrame()

        # Results Render
        if total_matches == 0:
            st.markdown("""
            <div style="background-color: #0f172a; border: 1px dashed #334155; border-radius: 8px; padding: 36px; text-align: center; margin-top: 16px;">
                <div style="font-size: 1.15rem; font-weight: 600; color: #cbd5e1;">No recorded sales match your search criteria</div>
                <div style="font-size: 0.88rem; color: #94a3b8; margin-top: 6px;">
                    Try searching for a county name (e.g. "Greater London", "Surrey") or adjusting your minimum/maximum price limits.
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            for _, row in page_records.iterrows():
                t_id = row["transaction_id"]
                price_val = int(row["price"])
                p_date = str(row["sale_date"])
                county_val = str(row["county"])
                p_type_code = str(row["property_type"])
                p_type_name = PROPERTY_TYPE_MAP.get(p_type_code, p_type_code)
                tenure_name = TENURE_MAP.get(str(row["tenure"]), str(row["tenure"]))
                pc = str(row.get("postcode", "")).strip()
                tw = str(row.get("town", "")).strip()

                loc_parts = []
                if pc:
                    loc_parts.append(pc)
                if tw:
                    loc_parts.append(tw)
                if county_val:
                    loc_parts.append(county_val)
                loc_str = " · ".join(loc_parts)

                r_col1, r_col2 = st.columns([5, 1.2])
                with r_col1:
                    st.markdown(f"""
                    <div class="sale-card">
                        <div class="sale-price">Sold for £{price_val:,}</div>
                        <div class="sale-location">{loc_str}</div>
                        <div class="sale-meta">
                            <span class="tag">{p_type_name}</span>
                            <span class="tag tag-tenure">{tenure_name}</span>
                            <span class="tag">Sold on {p_date}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                with r_col2:
                    st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)
                    if st.button("View details", key=f"btn_{t_id}", use_container_width=True):
                        st.session_state.selected_tx_id = t_id
                        st.session_state.active_view = "details"
                        st.rerun()

            # Pagination Controls
            pg_col1, pg_col2, pg_col3 = st.columns([1, 2, 1])
            with pg_col1:
                if curr_page > 1 and st.button("← Previous Page"):
                    st.session_state.search_page = curr_page - 1
                    st.rerun()
            with pg_col2:
                st.markdown(
                    f"<div style='text-align: center; color: #94a3b8; font-size: 0.88rem; padding-top: 8px;'>"
                    f"Page {curr_page} of {total_pages} (Showing {start_idx + 1}–{end_idx} of {total_matches:,} sales)"
                    f"</div>",
                    unsafe_allow_html=True
                )
            with pg_col3:
                if curr_page < total_pages and st.button("Next Page →"):
                    st.session_state.search_page = curr_page + 1
                    st.rerun()

    # -------------------------------------------------------------------------
    # VIEW: MARKET TRENDS
    # -------------------------------------------------------------------------
    elif st.session_state.active_view == "trends":
        if st.button("← Back to Property Finder"):
            st.session_state.active_view = "finder"
            st.rerun()

        loc_scope = st.session_state.search_location.strip()
        trend_df = df
        scope_title = "England & Wales"

        if loc_scope:
            c_mask = df["county"].astype(str).str.contains(loc_scope, case=False, na=False)
            t_mask = df["town"].astype(str).str.contains(loc_scope, case=False, na=False)
            trend_df = df[c_mask | t_mask]
            scope_title = loc_scope.upper()

        st.markdown(f"""
        <div style="border-bottom: 1px solid #1e293b; padding-bottom: 8px; margin-bottom: 20px;">
            <h2 style="font-size: 1.5rem; font-weight: 700; color: #f8fafc; margin: 0;">Market Trends Analytics</h2>
            <div style="font-size: 0.88rem; color: #14b8a6; margin-top: 4px;">Scope: {scope_title} · {current_mode.upper()} Mode ({len(trend_df):,} sales)</div>
        </div>
        """, unsafe_allow_html=True)

        st.subheader("1. Monthly Median Price Trajectory (2022–2023)")
        monthly_data = trend_df.groupby(["sale_year", "sale_month"])["price"].median().reset_index()
        monthly_data["period"] = monthly_data["sale_year"].astype(str) + "-" + monthly_data["sale_month"].apply(lambda x: f"{x:02d}")
        monthly_data = monthly_data.sort_values(by="period")

        if len(monthly_data) > 0:
            fig, ax = plt.subplots(figsize=(10, 3.8), dpi=100)
            fig.patch.set_facecolor("#0b0f19")
            ax.set_facecolor("#111827")

            ax.plot(
                monthly_data["period"],
                monthly_data["price"],
                marker="o",
                color="#14b8a6",
                linewidth=2.2,
                markersize=5,
                label=f"Monthly Median ({scope_title})"
            )

            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f"£{int(x/1000):,}k"))
            ax.tick_params(colors="#94a3b8", labelsize=8)
            plt.xticks(rotation=45, ha="right")
            ax.grid(True, linestyle="--", alpha=0.15, color="#64748b")
            for spine in ax.spines.values():
                spine.set_color("#1e293b")

            ax.set_ylabel("Median Price (GBP)", color="#94a3b8", fontsize=9)
            plt.tight_layout()
            st.pyplot(fig)
            plt.close()
        else:
            st.info("Insufficient monthly sales for the selected scope.")

        t_col1, t_col2 = st.columns(2)

        with t_col1:
            st.subheader("2. Price by Property Type")
            ptype_stats = trend_df.groupby("property_type")["price"].agg(["median", "count"]).dropna().reset_index()
            ptype_stats["type_name"] = ptype_stats["property_type"].map(PROPERTY_TYPE_MAP)
            ptype_stats = ptype_stats.sort_values(by="median", ascending=False)

            if len(ptype_stats) > 0:
                fig, ax = plt.subplots(figsize=(5.5, 4.0), dpi=100)
                fig.patch.set_facecolor("#0b0f19")
                ax.set_facecolor("#111827")

                bars = ax.bar(
                    ptype_stats["type_name"],
                    ptype_stats["median"],
                    color="#0ea5e9",
                    edgecolor="#0284c7",
                    width=0.55
                )
                ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f"£{int(x/1000):,}k"))
                ax.tick_params(colors="#94a3b8", labelsize=8)
                plt.xticks(rotation=25, ha="right")
                ax.grid(axis="y", linestyle="--", alpha=0.15, color="#64748b")
                for spine in ax.spines.values():
                    spine.set_color("#1e293b")

                for bar in bars:
                    h = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width() / 2, h + 3000, f"£{int(h/1000)}k", ha="center", va="bottom", color="#cbd5e1", fontsize=7.5)

                plt.tight_layout()
                st.pyplot(fig)
                plt.close()

        with t_col2:
            st.subheader("3. Top Counties by Volume")
            county_stats = trend_df.groupby("county").agg(
                sales_count=("price", "count"),
                median_price=("price", "median")
            ).sort_values(by="sales_count", ascending=False).head(10).reset_index()

            if len(county_stats) > 0:
                fig, ax = plt.subplots(figsize=(5.5, 4.0), dpi=100)
                fig.patch.set_facecolor("#0b0f19")
                ax.set_facecolor("#111827")

                y_pos = np.arange(len(county_stats))
                ax.barh(y_pos, county_stats["median_price"], color="#0d9488", edgecolor="#0f766e", height=0.6)
                ax.set_yticks(y_pos)
                ax.set_yticklabels(county_stats["county"], color="#cbd5e1", fontsize=8)
                ax.invert_yaxis()
                ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f"£{int(x/1000):,}k"))
                ax.tick_params(colors="#94a3b8", labelsize=8)
                ax.grid(axis="x", linestyle="--", alpha=0.15, color="#64748b")
                for spine in ax.spines.values():
                    spine.set_color("#1e293b")

                plt.tight_layout()
                st.pyplot(fig)
                plt.close()

    # -------------------------------------------------------------------------
    # VIEW: PRICE ESTIMATOR (RIDGE REGRESSION)
    # -------------------------------------------------------------------------
    elif st.session_state.active_view == "estimate":
        if st.button("← Back to Property Finder"):
            st.session_state.active_view = "finder"
            st.rerun()

        st.markdown("""
        <div style="border-bottom: 1px solid #1e293b; padding-bottom: 8px; margin-bottom: 18px;">
            <h2 style="font-size: 1.5rem; font-weight: 700; color: #f8fafc; margin: 0;">Estimate Sale Price</h2>
            <div style="font-size: 0.88rem; color: #94a3b8; margin-top: 4px;">
                Scikit-Learn Ridge Regression Pipeline · Evaluated on out-of-time sales transactions
            </div>
        </div>
        """, unsafe_allow_html=True)

        if model_pipeline is None or model_metrics is None:
            st.warning(
                f"Trained Ridge model for **{current_mode.upper()} mode** is not loaded. "
                f"Please run `python scripts/train_model.py --mode {current_mode}`."
            )
        else:
            county_list = list(df["county"].value_counts().index)

            with st.form("price_estimation_form"):
                e_col1, e_col2, e_col3 = st.columns(3)
                with e_col1:
                    est_county = st.selectbox("County", options=county_list, index=0)
                    est_type = st.selectbox(
                        "Property Type",
                        options=["Detached", "Semi-Detached", "Terraced", "Flat / Maisonette", "Other"],
                        index=3
                    )
                with e_col2:
                    est_tenure = st.selectbox("Tenure Duration", options=["Freehold", "Leasehold"], index=1)
                    est_year = st.selectbox("Sale Year (Historical)", options=[2022, 2023], index=1)
                with e_col3:
                    est_month_name = st.selectbox("Sale Month", options=MONTH_NAMES, index=5)
                    est_month = MONTH_NAMES.index(est_month_name) + 1
                    st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
                    calc_submitted = st.form_submit_button("Calculate Estimated Price", type="primary", use_container_width=True)

            if calc_submitted:
                type_code = PROPERTY_TYPE_REVERSE_MAP[est_type]
                tenure_code = "F" if est_tenure == "Freehold" else "L"

                input_features = pd.DataFrame([{
                    "county": est_county,
                    "property_type": type_code,
                    "tenure": tenure_code,
                    "sale_year": int(est_year),
                    "sale_month": int(est_month)
                }])

                try:
                    pred_price = model_pipeline.predict(input_features)[0]
                    st.session_state.predicted_price = pred_price
                except Exception as e:
                    st.error(f"Prediction failed: {e}")

            if "predicted_price" in st.session_state:
                pred = st.session_state.predicted_price
                test_mae = model_metrics.get("test_mae_gbp", 0)
                baseline_mae = model_metrics.get("baseline_mae_gbp", 0)
                mae_improvement_pct = model_metrics.get("mae_improvement_pct", 0)
                test_records = model_metrics.get("test_records_2023", 0)

                st.markdown(f"""
                <div style="background-color: #0f172a; border: 1px solid #0d9488; border-radius: 8px; padding: 22px; margin-top: 16px;">
                    <div style="font-size: 0.85rem; color: #14b8a6; text-transform: uppercase; font-weight: 600;">Estimated Valuation</div>
                    <div style="font-size: 2.3rem; font-weight: 700; color: #f8fafc; margin-top: 4px;">
                        Estimated price: £{pred:,.2f}
                    </div>
                    <div style="font-size: 0.90rem; color: #94a3b8; margin-top: 6px;">
                        Calculated for {est_type} ({est_tenure}) in {est_county} during {est_month_name} {est_year}.
                    </div>
                </div>
                """, unsafe_allow_html=True)

                st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
                m_col1, m_col2, m_col3 = st.columns(3)
                with m_col1:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label">Model Test MAE</div>
                        <div class="metric-value">£{int(test_mae):,}</div>
                        <div class="metric-delta-pos">Evaluated on {test_records:,} 2023 sales</div>
                    </div>
                    """, unsafe_allow_html=True)
                with m_col2:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label">Baseline Median Error</div>
                        <div class="metric-value">£{int(baseline_mae):,}</div>
                        <div class="metric-delta-neutral">Predicting training median</div>
                    </div>
                    """, unsafe_allow_html=True)
                with m_col3:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label">Accuracy Boost</div>
                        <div class="metric-value">+{mae_improvement_pct:.1f}%</div>
                        <div class="metric-delta-pos">Error reduction vs baseline</div>
                    </div>
                    """, unsafe_allow_html=True)

                st.caption(
                    f"**Model Evaluation Note:** "
                    f"The Ridge regression model achieves an out-of-time test MAE of £{int(test_mae):,} "
                    f"on 2023 sales, which reduces error by {mae_improvement_pct:.1f}% compared to the baseline "
                    f"of predicting the historical median. All figures reflect verified `{current_mode.upper()}` mode."
                )

    # -------------------------------------------------------------------------
    # VIEW: DATA AUDIT & HADOOP CLEANING (FACULTY DEMO)
    # -------------------------------------------------------------------------
    elif st.session_state.active_view == "audit":
        if st.button("← Back to Property Finder"):
            st.session_state.active_view = "finder"
            st.rerun()

        st.markdown("""
        <div style="border-bottom: 1px solid #1e293b; padding-bottom: 8px; margin-bottom: 18px;">
            <h2 style="font-size: 1.5rem; font-weight: 700; color: #f8fafc; margin: 0;">Data Preparation & Hadoop Cleaning Audit</h2>
            <div style="font-size: 0.88rem; color: #94a3b8; margin-top: 4px;">
                Strict Mathematical Reconciliation via Apache Hadoop Streaming MapReduce on YARN & HDFS
            </div>
        </div>
        """, unsafe_allow_html=True)

        if summary_data:
            counters = summary_data.get("counters", {})
            raw_count = counters.get("raw_rows", 0)
            rejected_count = counters.get("rejected_rows", 0)
            duplicate_count = counters.get("duplicate_rows_removed", 0)
            conflict_count = counters.get("conflicting_rows_excluded", 0)
            clean_count = counters.get("final_clean_rows", 0)

            is_balanced = raw_count == (rejected_count + duplicate_count + conflict_count + clean_count)
            status_text = "PASSED (EXACT MATCH ✅)" if is_balanced else "MISMATCH ❌"

            ac1, ac2, ac3, ac4, ac5 = st.columns(5)
            with ac1:
                st.metric("Raw Rows", f"{raw_count:,}")
            with ac2:
                st.metric("Mapper Rejected", f"{rejected_count:,}")
            with ac3:
                st.metric("Duplicates Removed", f"{duplicate_count:,}")
            with ac4:
                st.metric("Conflicts Excluded", f"{conflict_count:,}")
            with ac5:
                st.metric("Final Clean Rows", f"{clean_count:,}")

            st.markdown(f"""
            <div style="background-color: #064e3b; border: 1px solid #059669; border-radius: 6px; padding: 10px 16px; color: #ecfdf5; font-family: monospace; font-size: 0.90rem; margin-top: 10px; margin-bottom: 18px;">
                Conservation Formula: {raw_count:,} raw = {rejected_count:,} rejected + {duplicate_count:,} duplicates + {conflict_count:,} conflicts + {clean_count:,} clean — [{status_text}]
            </div>
            """, unsafe_allow_html=True)

            st.subheader("Real Preprocessing Example (Same Transaction ID)")
            st.markdown("""
| Field Attribute | Raw Input Record (`sample_raw.csv`) | Cleaned Output Record (`clean_sales.tsv`) | Preprocessing Action |
| :--- | :--- | :--- | :--- |
| **Transaction ID** | `"{01EB45F0-49DB-40F3-E063-4704A8C05FDE}"` | `01EB45F0-49DB-40F3-E063-4704A8C05FDE` | Stripped quotes and curly braces `{}` |
| **Sale Price** | `169950` | `169950` | Integer validation; passed £15k–£20M price boundary filter |
| **Sale Date** | `"2023-07-05 00:00"` | `2023-07-05` | Removed unneeded time component; formatted `YYYY-MM-DD` |
| **Sale Year** | *(Not present in raw CSV)* | `2023` | **Derived feature:** Temporal partition for out-of-time evaluation |
| **Sale Month** | *(Not present in raw CSV)* | `07` | **Derived feature:** Seasonal cycle feature for predictive modeling |
| **County** | `"TYNE AND WEAR"` | `TYNE AND WEAR` | Trimmed quotes and whitespace; converted to uppercase |
| **Property Type** | `"T"` | `T` | Terraced house code (`T`); validated against whitelist |
| **Tenure** | `"F"` | `F` | Freehold tenure code (`F`); validated against whitelist |
| **Postcode** | `"NE28 7JR"` | `NE28 7JR` | Extracted optional location field for search |
| **Town / City** | `"WALLSEND"` | `WALLSEND` | Extracted optional location field for search |
            """)

            with st.expander("Hadoop MapReduce Job & Rejection Breakdown Details"):
                st.json({
                    "mode": current_mode,
                    "execution_engine": summary_data.get("execution_engine"),
                    "application_id": summary_data.get("application_id", "N/A"),
                    "job_id": summary_data.get("job_id", "N/A"),
                    "job_status": summary_data.get("job_status", "SUCCEEDED"),
                    "elapsed_seconds": summary_data.get("elapsed_seconds"),
                    "rejection_breakdown": summary_data.get("rejection_breakdown", {})
                })
        else:
            st.info("No cleaning audit summary found for active dataset.")

    # Small Footer Link for Faculty Demo
    st.markdown("<hr style='margin-top: 30px; margin-bottom: 12px; border: 0; border-top: 1px solid #1e293b;'/>", unsafe_allow_html=True)
    foot_col1, foot_col2 = st.columns([3, 1])
    with foot_col1:
        st.caption("CSE3712 Big Data Analytics · UK HM Land Registry Price Paid Data (2022–2023) · Apache Hadoop Streaming (HDFS & YARN)")
    with foot_col2:
        if st.button("Hadoop Cleaning Audit & Preprocessing Demo →", key="foot_audit_btn"):
            st.session_state.active_view = "audit"
            st.rerun()


if __name__ == "__main__":
    main()
