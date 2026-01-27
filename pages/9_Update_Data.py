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
class StreamlitLogHandler(logging.Handler):
    def __init__(self, widget_update_func):
        super().__init__()
        self.widget_update_func = widget_update_func

    def emit(self, record):
        msg = self.format(record)
        self.widget_update_func(msg)


st.title("💦 Data Refresh Pipeline")


######################### Section 1 #########################
st.header("Daily Kline Pipeline", divider=True)

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
stock_filtered_df = pd.read_csv(f"{PROJECT_PATH}/data_app/app_decision.csv")
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
st.write(f"Get data for:")
st.write(f"Buy/Sell Signal Count: {choice_overall_signal_count}")
st.write(f"Industry Category: {choice_industry_category_name}")
st.write(f"Industry Type: {choice_industry_type_name}")
st.write(f"Customized Stock List: {text_stock_list}")
st.write(f"Row Range: {choice_row_range}")
# 2. Start Button
log_placeholder = st.empty()
if st.button("Run CN Stock - Fundamental Pipeline", icon="📊", type="primary"):
    scripts = [
        {"file": "0extract_ak_fundamental_by_yearly.py", "desc": "Fetch fundamental data stock by stock"},
        {"file": "2_0prep_ak_fundamental_by_yearly_concat.py", "desc": "Concatenate fundamentals of stocks into one file; remove columns with sparse value"},
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

            # Prepare the command.
            # We pass dates to A.py as arguments.
            # If B and C also need dates, add them to the list below.
            cmd = [sys.executable, script['file']]
            if script['file'] == "0extract_ak_fundamental_by_yearly.py":
                cmd.extend(["--choice_overall_signal_count", choice_overall_signal_count,
                            "--choice_industry_category_name", choice_industry_category_name,
                            "--choice_industry_type_name", choice_industry_type_name,
                            "--choice_row_range", choice_row_range,
                            "--text_stock_list", text_stock_list])

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
