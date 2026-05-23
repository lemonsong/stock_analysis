import streamlit as st
import subprocess
import sys, logging

from datetime import datetime
import pandas as pd
from utils.constants import PROJECT_PATH
# page config
from utils.streamlit_helper import setup_page_config
setup_page_config()
# logger
from streamlit.logger import get_logger
from utils.streamlit_helper import clear_cache, clean_expired_cache, setup_page_config

setup_page_config()

class StreamlitLogHandler(logging.Handler):
    def __init__(self, widget_update_func):
        super().__init__()
        self.widget_update_func = widget_update_func

    def emit(self, record):
        msg = self.format(record)
        self.widget_update_func(msg)

# 缓存管理
with st.sidebar.expander("缓存管理", expanded=False):
    if st.button("🔄 清理过期缓存", use_container_width=True):
        clean_expired_cache()
        st.sidebar.success("已清理过期缓存")

    if st.button("🗑️ 清除所有缓存", use_container_width=True):
        clear_cache()
        st.sidebar.success("已清除所有缓存")

st.title("💦 Data Refresh Pipeline")


######################### Section 1 #########################
st.header("Daily Kline Pipeline", divider=True)

# 1. Date Input Boxes
col_daily_kline_start, col_daily_kline_end = st.columns(2)
with col_daily_kline_start:
    daily_kline_start_date = st.date_input("Start Date", value=datetime.now())
with col_daily_kline_end:
    daily_kline_end_date = st.date_input("End Date", value=datetime.now())

# 2. Start Button
if st.button("Run CN Stock - Daily Kline Pipeline", icon="📈", type="primary"):
    # Convert dates to strings
    start_str = daily_kline_start_date.strftime("%Y-%m-%d")
    end_str = daily_kline_end_date.strftime("%Y-%m-%d")

    scripts = [
        {"file": "0extract_tushare_daily_kline.py", "desc": "Fetch daily kline data"},
        {"file": "2prep_tushare_daily_kline.py", "desc": "Append newest kline data to each single stock CSV file"},
        {"file": "3analysis_all_metrics_on_all_stocks_daily_kline.py", "desc": "Calculate buy/sell metrics"},
        {"file": "4app_data.py", "desc": "Format data for App"}
    ]

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        for i, script in enumerate(scripts):
            status_text.info(f"Step {i + 1}/{len(scripts)}: {script['desc']}...")

            # Prepare the command.
            # We pass dates to A.py as arguments.
            # If B and C also need dates, add them to the list below.
            cmd = [sys.executable, script['file']]
            if script['file'] == "0extract_tushare_daily_kline.py":
                cmd.extend(["--start", start_str, "--end", end_str])
            if script['file'] == "2prep_tushare_daily_kline.py":
                cmd.extend(["--end", end_str])

            # Run the script and wait for it to finish

            logger = get_logger("daily kline")
            logger.handlers.clear()
            handler = StreamlitLogHandler(st.empty().code)
            logger.addHandler(handler)

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Update progress
            progress_bar.progress((i + 1) / len(scripts))
            st.write(f"✅ {script['file']} finished successfully.")

        st.success("🎉 All scripts executed successfully!")

    except subprocess.CalledProcessError as e:
        st.error(f"❌ Error in {script['file']}: {e.stderr}")

######################### Section 2 #########################
st.divider()  # 👈 Draws a horizontal rule

st.header("Fundamentals Pipeline", divider=True)
# 1. Filter Input
stock_filtered_df = pd.read_csv(f"{PROJECT_PATH}/data/dwa/app_decision.csv")
boards_regex = st.text_input(
                "板块正则筛选",
                value="MSCI|沪深300|科创|高盛|贝莱德",
                key="filter_boards_regex",
                help="正则匹配 boards 列，例如: MSCI|沪深300。留空显示全部"
            )
# st.subheader("Overal Buy/Sell count")
options_overall_signal_count = ['All']+[str(item) for item in stock_filtered_df.overall_signal_count.unique()]
choice_overall_signal_count = st.segmented_control('Pick one overall_signal_count',
                                                   options_overall_signal_count,
                                                   selection_mode="single",
                                                   width="stretch",
                                                   default='All'
                                                   )
# st.subheader("Overal Industry Category")
options_industry_category_name = ['All']+[str(item) for item in stock_filtered_df.industry_category_name.unique()]
choice_industry_category_name = st.segmented_control('Pick one industry_category_name',
                                               options_industry_category_name,
                                               selection_mode="single",
                                                 width="stretch",
                                                 default='All'
                                               )

options_industry_sub_category_name = ['All']+[str(item) for item in stock_filtered_df.industry_sub_category_name.unique()]
choice_industry_sub_category_name = st.segmented_control('Pick one industry_sub_category_name',
                                               options_industry_sub_category_name,
                                               selection_mode="single",
                                                 width="stretch",
                                                 default='All'
                                               )
with st.container(height=300):
    # st.subheader("Overal Industry Type")
    options_industry_type_name = ['All']+[str(item) for item in stock_filtered_df.industry_type_name.unique()]
    choice_industry_type_name = st.segmented_control('Pick one industry_type_name',
                                                   options_industry_type_name,
                                                   selection_mode="single",
                                                 width="stretch",
                                                 default='All'
                                                   )


choice_row_range = st.segmented_control('Pick stock range',
                                                   ['All', '0-30','30-60','60-90','90-120','120-150','150-180'],
                                                   selection_mode="single", width="stretch",
                                        default='All'
                                                   )
text_stock_list = st.text_input("Enter list of stocks separated by comma", "")
fetch_relevant_symbols = st.checkbox("Fetch financial metrics for relevant symbols via Gemini", value=False)
fetch_relevant_symbols_financial_threshold = st.text_input("Enter financial scoring threshold of symbols to fetch relecant symbols", "4")

st.write(f"Get data for:")
st.write(f"Fetch Relevant Symbols: {fetch_relevant_symbols} with financial score >= {fetch_relevant_symbols_financial_threshold}")
st.write(f"Board Regex: {boards_regex}")
st.write(f"Buy/Sell Signal Count: {choice_overall_signal_count}")
st.write(f"Industry Category: {choice_industry_category_name}")
st.write(f"Industry Sub Category: {choice_industry_sub_category_name}")
st.write(f"Industry Type: {choice_industry_type_name}")
st.write(f"Row Range: {choice_row_range}")
st.write(f"Customized Stock List: {text_stock_list}")

# 2. Start Button
log_placeholder = st.empty()
if st.button("Run CN Stock - Fundamental Pipeline", icon="📊", type="primary"):
    scripts = [
        {"file": "_ak_financial_0extract_by_report.py", "desc": "Fetch fundamental data stock by stock"},
        {"file": "_ak_financial_2_0prep_yearly.py", "desc": "Concatenate fundamentals of stocks into one file; remove columns with sparse value"},
        {"file": "_ak_financial_2_1prep_market_value.py", "desc": "Calculate yearly latest market value"},
        {"file": "_ak_financial_2_2prep_yearly_calculate.py", "desc": "Calculate fundamental metrics"},
        {"file": "_ak_financial_3_0analysis_rank_by_yearly.py", "desc": "Rank key fundamental metrics"},
        {"file": "4app_data.py", "desc": "Format data for App"}

    ]

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        for i, script in enumerate(scripts):
            status_text.info(f"Step {i + 1}/{len(scripts)}: {script['desc']}...")

            # Prepare the command.
            # We pass dates to A.py as arguments.
            # If B and C also need dates, add them to the list below.
            cmd = [sys.executable, script['file']]
            if script['file'] == "_ak_financial_0extract_by_report.py":
                cmd.extend(["--boards_regex", boards_regex,
                            "--choice_overall_signal_count", choice_overall_signal_count,
                            "--choice_industry_category_name", choice_industry_category_name,
                            "--choice_industry_sub_category_name", choice_industry_sub_category_name,
                            "--choice_industry_type_name", choice_industry_type_name,
                            "--choice_row_range", choice_row_range,
                            "--text_stock_list", text_stock_list,
                            "--fetch_relevant_symbols", str(fetch_relevant_symbols),
                            "--fetch_relevant_symbols_financial_threshold", fetch_relevant_symbols_financial_threshold])

            # Run the script and wait for it to finish
            # process = subprocess.Popen(
            #     cmd,
            #     stdout=subprocess.PIPE,
            #     stderr=subprocess.STDOUT,  # Redirect errors to the same pipe
            #     text=True
            # )
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # # Read the output line by line as it happens
            # full_log = ""
            # # We create a code block inside the placeholder
            # while True:
            #     line = process.stdout.readline()
            #     if not line and process.poll() is not None:
            #         break
            #     if line:
            #         full_log += line
            #         # Update the UI with the accumulated logs
            #         # Using height=300 makes it a scrollable box
            #         log_placeholder.code(full_log, language="log")
            #
            # if process.returncode == 0:
            #     st.success("Pipeline Completed Successfully!")
            # else:
            #     st.error("Pipeline Failed. Check logs above.")

            # Update progress
            progress_bar.progress((i + 1) / len(scripts))
            st.write(f"✅ {script['file']} finished successfully.")



        st.success("🎉 All scripts executed successfully!")

    except subprocess.CalledProcessError as e:
        st.error(f"❌ Error in {script['file']}: {e.stderr}")

######################### Section 3 #########################
st.header("Realtime Price", divider=True)

# 2. Start Button
if st.button("Run CN Stock - Realtime Price Pipeline", icon="📈", type="primary"):
    scripts = [
        {"file": "_realtime_0extract_ak.py", "desc": "Fetch realtime stock price"}
    ]

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        for i, script in enumerate(scripts):
            status_text.info(f"Step {i + 1}/{len(scripts)}: {script['desc']}...")
            cmd = [sys.executable, script['file']]

            # Run the script and wait for it to finish

            logger = get_logger("realtime price")
            logger.handlers.clear()
            handler = StreamlitLogHandler(st.empty().code)
            logger.addHandler(handler)

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Update progress
            progress_bar.progress((i + 1) / len(scripts))
            st.write(f"✅ {script['file']} finished successfully.")

        st.success("🎉 All scripts executed successfully!")

    except subprocess.CalledProcessError as e:
        st.error(f"❌ Error in {script['file']}: {e.stderr}")

######################### Section 4 #########################
st.header("Financial Report Refresh Date", divider=True)

# 2. Start Button
if st.button("Run Financial Report Refresh Date Pipeline", icon="📅", type="primary"):
    scripts = [
        {"file": "_ak_financial_0_0extract_report_date.py", "desc": "Fetch financial report refresh date"}
    ]

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        for i, script in enumerate(scripts):
            status_text.info(f"Step {i + 1}/{len(scripts)}: {script['desc']}...")
            cmd = [sys.executable, script['file']]

            # Run the script and wait for it to finish

            logger = get_logger("financial report refresh date")
            logger.handlers.clear()
            handler = StreamlitLogHandler(st.empty().code)
            logger.addHandler(handler)

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Update progress
            progress_bar.progress((i + 1) / len(scripts))
            st.write(f"✅ {script['file']} finished successfully.")

        st.success("🎉 All scripts executed successfully!")

    except subprocess.CalledProcessError as e:
        st.error(f"❌ Error in {script['file']}: {e.stderr}")