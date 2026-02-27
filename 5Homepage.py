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
with st.sidebar.expander("缓存管理", expanded=False):
    if st.button("🔄 清理过期缓存", use_container_width=True):
        clean_expired_cache()
        st.sidebar.success("已清理过期缓存")

    if st.button("🗑️ 清除所有缓存", use_container_width=True):
        clear_cache()
        st.sidebar.success("已清除所有缓存")

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
