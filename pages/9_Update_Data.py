import streamlit as st
import subprocess
import sys
from datetime import datetime

st.title("Data Refresh Pipeline")

# 1. Date Input Boxes
col_daily_kline_start, col_daily_kline_end = st.columns(2)
with col_daily_kline_start:
    daily_kline_start_date = st.date_input("Start Date", value=datetime(2026, 1, 1))
with col_daily_kline_end:
    daily_kline_end_date = st.date_input("End Date", value=datetime.now())

# 2. Start Button
if st.button("Run Daily Kline Pipeline"):
    # Convert dates to strings
    start_str = daily_kline_start_date.strftime("%Y-%m-%d")
    end_str = daily_kline_end_date.strftime("%Y-%m-%d")

    scripts = [
        {"file": "0extract_tushare_daily_kline.py", "desc": "Fetching Daily Kline Data"},
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
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Update progress
            progress_bar.progress((i + 1) / len(scripts))
            st.write(f"✅ {script['file']} finished successfully.")

        st.success("🎉 All scripts executed successfully!")

    except subprocess.CalledProcessError as e:
        st.error(f"❌ Error in {script['file']}: {e.stderr}")