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
def load_history_data(start_date, end_date):
    """加载指定日期范围的历史信号数据"""
    history_dir = Path(PROJECT_PATH) / 'data/dwa/kline_analysis_history'

    if not history_dir.exists() or not history_dir.is_dir():
        st.error(f"未找到历史数据文件夹: {history_dir}")
        return None

    # Load all CSVs in the date range
    df_list = []

    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    for file_path in history_dir.glob("*.csv"):
        # Extract date from filename: YYYY-MM-DD.csv
        file_date_str = file_path.stem
        # Ensure it falls within the range
        if start_str <= file_date_str <= end_str:
            try:
                df = pd.read_csv(file_path)

                # We need overall_signal_count > 0 as required
                if 'overall_signal_count' in df.columns:
                    df = df[df['overall_signal_count'] > 0]

                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date']).dt.date
                elif 'update_date' in df.columns:
                    df['date'] = pd.to_datetime(df['update_date']).dt.date

                if not df.empty:
                    df_list.append(df)
            except Exception as e:
                st.error(f"读取历史数据文件失败 {file_path}: {e}")

    if not df_list:
        return pd.DataFrame()

    merged_df = pd.concat(df_list, ignore_index=True)
    return merged_df

@st.cache_data
def load_basic_info():
    """加载股票基本信息(名称、行业等)"""
    info_file = Path(PROJECT_PATH) / 'data/basic/stock_name_industry.csv'
    if not info_file.exists():
        return None
    return pd.read_csv(info_file)

def filter_data(df, min_signal_count, industry_filter):
    """
    根据信号强度和行业筛选数据
    Deduplication logic: Keep the latest signal for each symbol within the period
    """
    if df.empty:
        return df

    filtered = df.copy()

    # Signal Strength filter
    if 'overall_signal_count' in filtered.columns:
        filtered = filtered[filtered['overall_signal_count'] >= min_signal_count]

    # Deduplicate: sort by date descending (to get latest update date), then drop duplicates keeping first (latest)
    if 'date' in filtered.columns:
        filtered = filtered.sort_values('date', ascending=False)
    elif 'update_date' in filtered.columns:
        filtered = filtered.sort_values('update_date', ascending=False)

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
    df_history = load_history_data(start_date, end_date)
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
    result_df = filter_data(df_history, min_signal, industry_selected)

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
