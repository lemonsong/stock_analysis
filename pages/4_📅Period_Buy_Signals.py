"""
页面4: 周期性买入信号监测
"""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from utils.constants import PROJECT_PATH, INDUSTRY_COL_DICT, FUNDAMENTAL_KEY_COLS, SEQUENTIAL_COLOR
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

    # Merge app_decision.csv data to get all required columns similar to pages/2_💡Buy_Signals.py
    app_decision_file = Path(PROJECT_PATH) / 'data/dwa/app_decision.csv'
    if app_decision_file.exists():
        app_df = pd.read_csv(app_decision_file)
        # Drop columns that already exist in result_df to avoid conflicts (except symbol for merging)
        # We only keep 'date' and maybe 'overall_signal_count' etc from history, but let's actually
        # keep the history values for signals, and merge the rest from app_decision.
        cols_to_drop = [c for c in app_df.columns if c in result_df.columns and c != 'symbol']
        app_df = app_df.drop(columns=cols_to_drop)
        result_df = pd.merge(result_df, app_df, on='symbol', how='left')


    # --- Display ---
    st.markdown(f"### 🔍 筛选结果 ({start_date} 至 {end_date})")
    st.info(f"共发现 **{len(result_df)}** 只股票在选定周期内出现过 `综合信号 >= {min_signal}` (已去重，显示最新日期的信号)。")

    if result_df.empty:
        st.warning("未找到符合条件的数据。")
    else:
        # Columns to display (Using logic from Buy Signals page)
        filtered_df = result_df.copy()

        # 准备用于显示的DataFrame
        display_df = filtered_df.copy()
        # 列分组定义
        col_groups = {
            "基本信息": ['symbol_url', 'company', 'close', 'market_cap'],
            "行业信息": ['industry_category_name', 'industry_sub_category_name', 'industry_type_name'],
            "信号指标": [col for col in filtered_df.columns if 'signal' in col.lower()],
            "分红指标": [col for col in filtered_df.columns if 'total_dividend' in col and 'yield' not in col],
            "分红率(%)": [col for col in filtered_df.columns if 'total_dividend_yield' in col],
            "股价增长(%)": [col for col in filtered_df.columns if 'growth_' in col],
            "基本面指标": FUNDAMENTAL_KEY_COLS,
            "基本面排名": [col for col in filtered_df.columns if 'fundamental' in col.lower()],
            "技术指标": [col for col in filtered_df.columns if col.endswith('_buy') or col.endswith('_sell')],
            "净流入":[col for col in filtered_df.columns if 'inflow' in col],
        }

        # 构造URL: https://xueqiu.com/S/{stock_symbol}
        if 'symbol_url' not in display_df.columns:
            display_df.insert(0, 'symbol_url', "https://xueqiu.com/S/" + display_df['symbol'].astype(str), allow_duplicates=True)

        # ratio will display as percentage
        for col in col_groups["股价增长(%)"]:
            if col in display_df.columns:
                display_df[col] = display_df[col] * 100
        for col in col_groups["分红率(%)"]:
            if col in display_df.columns:
                display_df[col] = display_df[col] * 100
        if 'roe' in display_df.columns:
            display_df['roe'] = display_df['roe'] * 100
        if 'debt_to_asset' in display_df.columns:
            display_df['debt_to_asset'] = display_df['debt_to_asset'] * 100

        col_col_selection_mode, col_sort_by, col_ascending = st.columns(3)
        with col_col_selection_mode:
            col_selection_mode = st.radio(
                "显示模式",
                options=["显示默认列", "显示所有列", "自定义选择"],
                horizontal=True
            )

        # 默认显示的列
        default_display_cols = []
        for group_name in ["基本信息", "行业信息", "信号指标", "股价增长(%)", "分红率(%)", "股价增长", "基本面指标", "基本面排名", "净流入"]:
            if group_name in col_groups:
                available_cols = [col for col in col_groups[group_name] if col in display_df.columns]
                default_display_cols.extend(available_cols)

        final_display_cols = []
        if col_selection_mode == "显示默认列":
            final_display_cols = default_display_cols
        elif col_selection_mode == "显示所有列":
            final_display_cols = list(display_df.columns)
            if 'symbol' in final_display_cols and 'symbol_url' in final_display_cols:
                final_display_cols.remove('symbol')
        else:
            selected_cols = []
            for group_name, cols in col_groups.items():
                available_cols = [col for col in cols if col in display_df.columns]
                if available_cols:
                    default_selection = available_cols if group_name in ["基本信息", "行业信息", "信号指标", "分红率(%)", "基本面排名"] else []
                    with st.expander(f"{group_name} ({len(available_cols)}列)", expanded=(group_name == "基本信息")):
                        selected = st.multiselect(
                            f"选择{group_name}",
                            options=available_cols,
                            default=default_selection,
                            key=f"cols_{group_name}"
                        )
                        selected_cols.extend(selected)
            final_display_cols = selected_cols if selected_cols else default_display_cols

        if not final_display_cols:
            st.warning("请至少选择一列进行显示")
            st.stop()

        sort_options = [col for col in final_display_cols]
        default_sort_index = 0
        if 'overall_signal_count' in sort_options:
            default_sort_index = sort_options.index('overall_signal_count')

        with col_sort_by:
            sort_by = st.selectbox("排序方式", options=sort_options, index=default_sort_index)
        with col_ascending:
            ascending = st.checkbox("升序", value=False)

        display_df_sorted = display_df.sort_values(sort_by, ascending=ascending)
        display_df_final = display_df_sorted[final_display_cols]

        styled_display = display_df_final.style

        for col in display_df_final.columns:
            if col not in ['symbol_url', 'company'] and display_df_final[col].dtype in ['int64', 'float64']:
                col_min = display_df_final[col].min()
                col_max = display_df_final[col].max()
                if col_max != col_min:
                    styled_display = styled_display.background_gradient(
                        subset=[col],
                        cmap=SEQUENTIAL_COLOR,
                        vmin=col_min,
                        vmax=col_max
                    )

        column_config = {
            "symbol_url": st.column_config.LinkColumn("股票代码", help="点击查看雪球详情", display_text=r"https://xueqiu\.com/S/(.*)", width="small"),
            "company": st.column_config.Column("名称", help="company"),
            "close": st.column_config.NumberColumn("收盘价", help="close", format="¥%.2f"),
            "market_cap": st.column_config.NumberColumn("市值", help="最近一年年报发布日的总股本*最近股价", format="¥%.0e"),
            "industry_category_name": st.column_config.Column("门类", help="industry_category_name", width="small"),
            "industry_sub_category_name": st.column_config.Column("次类", help="industry_sub_category_name", width="small"),
            "industry_type_name": st.column_config.Column("大类", help="industry_type_name"),
            "overall_signal_count": st.column_config.NumberColumn("总体信号数", help="买入信号数减卖出信号数", format="%d"),
            "buy_signal_count": st.column_config.NumberColumn("买入信号数", format="%d"),
            "sell_signal_count": st.column_config.NumberColumn("卖出信号数", format="%d"),
            "total_dividend_yield_1Y": st.column_config.NumberColumn("近1年股息率", help="过去12个月累计现金分红收益率", format="%.2f%%"),
            "total_dividend_yield_3Y": st.column_config.NumberColumn("近3年股息率", help="过去3年累计现金分红收益率", format="%.2f%%"),
            "total_dividend_yield_5Y": st.column_config.NumberColumn("近5年股息率", help="过去5年累计现金分红收益率", format="%.2f%%"),
            "total_dividend_1Y": st.column_config.NumberColumn("近1年股息", help="过去12个月累计现金分红收益", format="%.2f"),
            "total_dividend_3Y": st.column_config.NumberColumn("近3年股息", help="过去3年累计现金分红收益", format="%.2f"),
            "total_dividend_5Y": st.column_config.NumberColumn("近5年股息", help="过去5年累计现金分红收益", format="%.2f"),
            "growth_1Y": st.column_config.NumberColumn("近1年均价增长", format="%.2f%%"),
            "growth_2Y": st.column_config.NumberColumn("近2年均价增长", format="%.2f%%"),
            "growth_3Y": st.column_config.NumberColumn("近3年均价增长", format="%.2f%%"),
            "fundamental_score": st.column_config.NumberColumn("基本面评分", help="基于各项财务指标的综合评分（越高越好）", format="%.2f"),
            "fundamental_rank": st.column_config.NumberColumn("基本面排名", help="基于综合评分的年度排名（越小越好）", format="%d"),
            "fundamental_fiscal_year": st.column_config.NumberColumn("财报年份", help="排名所基于的财报年份", format="%d"),
            "roe": st.column_config.NumberColumn("ROE", help="", format="%.2f%%", width="small"),
            "netcash_operate_over_net_profit": st.column_config.NumberColumn("Net cash operate/Net profit", help="", format="%.2f", width="small"),
            "debt_to_asset": st.column_config.NumberColumn("Debt/Asset", help="", format="%.2f%%", width="small"),
            "inventory_turnover": st.column_config.NumberColumn("Inventory Turnover", help="", format="%.1f 次/年", width="small"),
            "ev_over_ebitda": st.column_config.NumberColumn("EV/EBITDA", help="", format="%.1f x", width="small"),
            "big_money_net_inflow_ratio_10d": st.column_config.NumberColumn("主力净流入比", help="", format="%.2f", width="small"),
        }

        st.dataframe(
            styled_display,
            use_container_width=True,
            height=700,
            column_config=column_config,
            hide_index=True
        )

        st.markdown("#### 📊 数据统计")
        st.write(f"显示 {len(display_df_final)} 行，{len(final_display_cols)} 列")

        csv = display_df_final.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "📥 下载结果 CSV",
            csv,
            f"period_signals_{start_date}_{end_date}.csv",
            "text/csv"
        )

if __name__ == "__main__":
    main()
