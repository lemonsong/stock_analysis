import streamlit as st
import subprocess
import sys, logging
from datetime import datetime
import pandas as pd
from utils.constants import PROJECT_PATH
from utils.streamlit_helper import setup_page_config, clear_cache, clean_expired_cache

setup_page_config(page_title="Invest Monitor Platform", page_icon="💰")

# Sidebar
st.sidebar.title("💰 Invest Monitor")
st.sidebar.markdown("---")
st.sidebar.markdown("### 📑 Navigation")
st.sidebar.markdown("""
- 📊 [Buy Signals](2_Buy_Signals)
- 📅 [Period Buy Signals](4_Period_Buy_Signals)
- 📈 [Fundamental Analysis](3_Fundamental_Analysis)
- 💼 [My Holdings](6_My_Holdings)
- 🔙 [Backtest Overview](8_Backtest_Overview)
- 💦 [Data Pipeline](5app_monitor)
""")
st.sidebar.markdown("---")

# 缓存管理
with st.sidebar.expander("Cache Management", expanded=False):
    if st.button("🔄 Clear Expired Cache", use_container_width=True):
        clean_expired_cache()
        st.sidebar.success("Expired cache cleared")

    if st.button("🗑️ Clear All Cache", use_container_width=True):
        clear_cache()
        st.sidebar.success("All cache cleared")

# Main Content
st.title("💰 Investment Monitoring & Analysis Platform")
st.markdown("Welcome! This platform provides comprehensive tools for A-share analysis.")

st.markdown("""
### 📋 Feature Overview

1.  **📊 Buy Signals** (`pages/2_Buy_Signals.py`)
    *   Monitor daily buy/sell signals based on technical indicators (RSI, MACD, etc.).
    *   Filter by Industry, Fundamental Rank, and Price.
    *   View distribution of signals across industries.

2.  **📅 Period Buy Signals** (`pages/4_Period_Buy_Signals.py`) **[NEW]**
    *   Analyze buy signals over a specific date range.
    *   Identify stocks with consistent positive signals.

3.  **📈 Fundamental Analysis** (`pages/3_Fundamental_Analysis.py`)
    *   Deep dive into financial reports and key metrics (ROE, Net Cash, Debt/Asset).
    *   View historical trends and rankings.

4.  **💼 My Holdings** (`pages/6_My_Holdings.py`)
    *   Track the performance of your portfolio against calculated signals.

5.  **💦 Data Pipeline** (This Page)
    *   Manage and trigger data update scripts.
""")

st.divider()

st.header("💦 Data Refresh Pipeline")

# 1. Date Input Boxes
col_daily_kline_start, col_daily_kline_end = st.columns(2)
with col_daily_kline_start:
    daily_kline_start_date = st.date_input("Start Date", value=datetime(2026, 1, 1))
with col_daily_kline_end:
    daily_kline_end_date = st.date_input("End Date", value=datetime.now())

# 2. Start Button
if st.button("Run CN Stock - Daily Kline Pipeline", icon="📈", type="primary"):
    # Convert dates to strings
    start_str = daily_kline_start_date.strftime("%Y-%m-%d")
    end_str = daily_kline_end_date.strftime("%Y-%m-%d")

    scripts = [
        {"file": "0extract_tushare_daily_kline.py", "desc": "Fetch daily kline data"},
        {"file": "2prep_tushare_daily_kline.py", "desc": "Append newest kline data to each single stock CSV file (Parallel Optimized)"},
        {"file": "3analysis_all_metrics_on_all_stocks_daily_kline.py", "desc": "Calculate buy/sell metrics & Update History"},
        {"file": "4app_data.py", "desc": "Format data for App"}
    ]

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        for i, script in enumerate(scripts):
            status_text.info(f"Step {i + 1}/{len(scripts)}: {script['desc']}...")
            cmd = [sys.executable, script['file']]
            if script['file'] == "0extract_tushare_daily_kline.py":
                cmd.extend(["--start", start_str, "--end", end_str])
            if script['file'] == "2prep_tushare_daily_kline.py":
                cmd.extend(["--end", end_str])

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            progress_bar.progress((i + 1) / len(scripts))
            st.write(f"✅ {script['file']} finished successfully.")

        st.success("🎉 All scripts executed successfully!")

    except subprocess.CalledProcessError as e:
        st.error(f"❌ Error in {script['file']}: {e.stderr}")

st.divider()

st.header("Fundamentals Pipeline")
# 1. Filter Input
stock_filtered_df = pd.read_csv(f"{PROJECT_PATH}/data/dwa/app_decision.csv") if (Path(PROJECT_PATH) / "data/dwa/app_decision.csv").exists() else pd.DataFrame(columns=['overall_signal_count', 'industry_category_name', 'industry_sub_category_name', 'industry_type_name'])

boards_regex = st.text_input(
                "Board Regex Filter",
                value="MSCI|沪深300|科创|高盛|贝莱德",
                key="filter_boards_regex",
                help="Regex to match 'boards' column"
            )

options_overall_signal_count = ['All'] + ([str(item) for item in stock_filtered_df.overall_signal_count.unique()] if not stock_filtered_df.empty else [])
choice_overall_signal_count = st.segmented_control('Overall Signal Count',
                                                   options_overall_signal_count,
                                                   selection_mode="single",
                                                   default='All'
                                                   )

options_industry_category_name = ['All'] + ([str(item) for item in stock_filtered_df.industry_category_name.unique()] if not stock_filtered_df.empty else [])
choice_industry_category_name = st.segmented_control('Industry Category',
                                               options_industry_category_name,
                                               selection_mode="single",
                                                 default='All'
                                               )

options_industry_sub_category_name = ['All'] + ([str(item) for item in stock_filtered_df.industry_sub_category_name.unique()] if not stock_filtered_df.empty else [])
choice_industry_sub_category_name = st.segmented_control('Industry Sub Category',
                                               options_industry_sub_category_name,
                                               selection_mode="single",
                                                 default='All'
                                               )
with st.container(height=100):
    options_industry_type_name = ['All'] + ([str(item) for item in stock_filtered_df.industry_type_name.unique()] if not stock_filtered_df.empty else [])
    choice_industry_type_name = st.segmented_control('Industry Type',
                                                   options_industry_type_name,
                                                   selection_mode="single",
                                                 default='All'
                                                   )


choice_row_range = st.segmented_control('Stock Range',
                                                   ['All', '0-30','30-60','60-90','90-120','120-150','150-180'],
                                                   selection_mode="single",
                                        default='All'
                                                   )
text_stock_list = st.text_input("Custom Stock List (comma separated)", "")

# 2. Start Button
if st.button("Run CN Stock - Fundamental Pipeline", icon="📊", type="primary"):
    scripts = [
        {"file": "0extract_ak_fundamental_by_yearly.py", "desc": "Fetch fundamental data stock by stock"},
        {"file": "2_0prep_ak_fundamental_by_yearly_concat.py", "desc": "Concatenate fundamentals (Parallel + Auto-fetch missing)"},
        {"file": "2_1prep_ak_fundamental_market_value.py", "desc": "Calculate yearly latest market value"},
        {"file": "2_2prep_ak_fundamental_by_yearly_calculate.py", "desc": "Calculate fundamental metrics"},
        {"file": "3analysis_rank_ak_fundamental_by_yearly.py", "desc": "Rank key fundamental metrics"},
        {"file": "4app_data.py", "desc": "Format data for App"}

    ]

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        for i, script in enumerate(scripts):
            status_text.info(f"Step {i + 1}/{len(scripts)}: {script['desc']}...")

            cmd = [sys.executable, script['file']]
            if script['file'] == "0extract_ak_fundamental_by_yearly.py":
                cmd.extend(["--boards_regex", boards_regex,
                            "--choice_overall_signal_count", choice_overall_signal_count,
                            "--choice_industry_category_name", choice_industry_category_name,
                            "--choice_industry_sub_category_name", choice_industry_sub_category_name,
                            "--choice_industry_type_name", choice_industry_type_name,
                            "--choice_row_range", choice_row_range,
                            "--text_stock_list", text_stock_list])

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            progress_bar.progress((i + 1) / len(scripts))
            st.write(f"✅ {script['file']} finished successfully.")

        st.success("🎉 All scripts executed successfully!")

    except subprocess.CalledProcessError as e:
        st.error(f"❌ Error in {script['file']}: {e.stderr}")
