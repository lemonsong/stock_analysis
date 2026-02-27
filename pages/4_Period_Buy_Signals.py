"""
页面4: 周期性买入信号监测
"""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from utils.constants import PROJECT_PATH, INDUSTRY_COL_DICT
from utils.streamlit_helper import setup_page_config, clear_cache, clean_expired_cache

setup_page_config()

@st.cache_data
def load_history_data():
    """加载历史信号数据"""
    history_file = Path(PROJECT_PATH) / 'data/dwa/kline_analysis_history.csv'

    if not history_file.exists():
        st.error(f"未找到历史数据文件: {history_file}")
        return None

    try:
        df = pd.read_csv(history_file)
        # Ensure date column is datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
        return df
    except Exception as e:
        st.error(f"读取历史数据文件失败: {e}")
        return None

@st.cache_data
def load_basic_info():
    """加载股票基本信息(名称、行业等)"""
    info_file = Path(PROJECT_PATH) / 'data/basic/stock_name_industry.csv'
    if not info_file.exists():
        return None
    return pd.read_csv(info_file)

def filter_data(df, start_date, end_date, min_signal_count, industry_filter):
    """
    根据日期范围和信号强度筛选数据
    Deduplication logic: Keep the latest signal for each symbol within the period
    """
    # Date filter
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    filtered = df.loc[mask].copy()

    # Signal Strength filter
    if 'overall_signal_count' in filtered.columns:
        filtered = filtered[filtered['overall_signal_count'] >= min_signal_count]

    # Deduplicate: sort by date descending, then drop duplicates keeping first (latest)
    filtered = filtered.sort_values('date', ascending=False)
    filtered = filtered.drop_duplicates(subset=['symbol'], keep='first')

    # Join with Industry Info if available
    basic_info = load_basic_info()
    if basic_info is not None:
        filtered = filtered.merge(basic_info[['symbol', 'company', 'industry_category_name', 'industry_sub_category_name', 'industry_type_name']], on='symbol', how='left')

        # Industry Filter
        if industry_filter and industry_filter != '全部':
             # Check which column to filter on based on the options usually provided or just check all?
             # For simplicity, let's assume we filter on Category Name if the user select logic is implemented,
             # but here we might just show all industries.
             # If we want to support the same sidebar filter as Buy Signals, we can.
             # For now, let's implement a simple filter based on Category.
             filtered = filtered[filtered['industry_category_name'] == industry_filter]

    return filtered

def main():
    st.title("📅 周期性买入信号监测")

    # --- Sidebar ---
    st.sidebar.header("筛选条件")

    # Date Range
    today = datetime.now().date()
    default_start = today - timedelta(days=7)

    col_d1, col_d2 = st.sidebar.columns(2)
    start_date = col_d1.date_input("开始日期", value=default_start)
    end_date = col_d2.date_input("结束日期", value=today)

    if start_date > end_date:
        st.sidebar.error("开始日期不能晚于结束日期")
        st.stop()

    # Signal Count Threshold
    min_signal = st.sidebar.slider("最小综合信号值 (Overall Signal >=)", min_value=1, max_value=10, value=1)

    # Load Data
    df_history = load_history_data()
    if df_history is None:
        st.stop()

    # Industry Filter (Simple)
    basic_info = load_basic_info()
    industry_options = ["全部"]
    if basic_info is not None and 'industry_category_name' in basic_info.columns:
        cats = basic_info['industry_category_name'].dropna().unique().tolist()
        industry_options += sorted(cats)

    industry_selected = st.sidebar.selectbox("行业筛选 (门类)", options=industry_options)

    # --- Process Data ---
    result_df = filter_data(df_history, start_date, end_date, min_signal, industry_selected)

    # --- Display ---
    st.markdown(f"### 🔍 筛选结果 ({start_date} 至 {end_date})")
    st.info(f"共发现 **{len(result_df)}** 只股票在选定周期内出现过 `综合信号 >= {min_signal}` (已去重，显示最新日期的信号)。")

    if result_df.empty:
        st.warning("未找到符合条件的数据。")
    else:
        # Columns to display
        # Construct Symbol URL
        if 'symbol' in result_df.columns:
            result_df.insert(0, 'symbol_url', "https://xueqiu.com/S/" + result_df['symbol'].astype(str))

        display_cols = [
            'symbol_url', 'company', 'date', 'close',
            'overall_signal_count', 'buy_signal_count', 'sell_signal_count',
            'industry_category_name', 'industry_type_name'
        ]

        # Filter only existing columns
        final_cols = [c for c in display_cols if c in result_df.columns]

        # Config
        column_config = {
            "symbol_url": st.column_config.LinkColumn("股票代码", display_text=r"https://xueqiu\.com/S/(.*)"),
            "company": "名称",
            "date": st.column_config.DateColumn("最新信号日期"),
            "close": st.column_config.NumberColumn("收盘价", format="%.2f"),
            "overall_signal_count": "综合信号",
            "buy_signal_count": "买入信号数",
            "sell_signal_count": "卖出信号数",
            "industry_category_name": "行业门类",
            "industry_type_name": "行业大类"
        }

        st.dataframe(
            result_df[final_cols],
            column_config=column_config,
            use_container_width=True,
            hide_index=True,
            height=600
        )

        # Download
        csv = result_df[final_cols].to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "📥 下载结果 CSV",
            csv,
            f"period_signals_{start_date}_{end_date}.csv",
            "text/csv"
        )

if __name__ == "__main__":
    main()
