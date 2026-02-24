import streamlit as st
import pandas as pd
from utils.constants import INDUSTRY_COL_DICT, INDUSTRY_COL_DEFAULT_TO_USE


def setup_page_config(page_title="投资监测分析平台", page_icon="💰", layout="wide", initial_sidebar_state="expanded"):
    st.set_page_config(
        page_title=page_title,
        page_icon=page_icon,
        layout=layout,
        initial_sidebar_state=initial_sidebar_state
    )


def clear_cache():
    st.cache_data.clear()
    st.cache_resource.clear()


def clean_expired_cache():
    # Streamlit automatically manages cache expiration, but we can force clear
    st.cache_data.clear()


def render_filter_sidebar(df, default_filter_mode=0, default_stock_input_list='', default_stock_search_list=[]):
    """
    Renders the shared stock filter component in the sidebar.

    Args:
        df: The dataframe containing stock data.
            Must contain 'symbol'.
            Should contain 'company', 'industry_category_name', 'industry_type_name', 'industry_sub_category_name' for full functionality.
        default_filter_mode: default use 0-"列表导入" or 1-"组合筛选".
        default_stock_input_list: default stock list string when use "列表导入"
    Returns:
        pd.DataFrame: The filtered dataframe.
    """
    st.sidebar.header("股票筛选")

    # Selection Mode
    filter_mode = st.sidebar.radio(
        "筛选模式",
        ["列表导入", "行业筛选", "股票搜索"],
        index=default_filter_mode,
        horizontal=True,
        key="filter_mode_radio"
    )

    available_stocks = df[['symbol', 'company']].drop_duplicates()
    stock_map = {
        row['symbol']: f"{row['symbol']} - {row['company']}"
        for _, row in available_stocks.iterrows()
    }

    filtered_df = df.copy()

    # Ensure symbol is string
    if 'symbol' in filtered_df.columns:
        filtered_df['symbol'] = filtered_df['symbol'].astype(str)

    if filter_mode == "列表导入":
        st.sidebar.caption("输入股票代码，用逗号分隔 (例如: SH600000,SZ000001)")

        input_text = st.sidebar.text_area("📝 股票代码列表", default_stock_input_list, key="filter_list_input")

        if input_text:
            # Parse input
            symbols = [s.strip() for s in input_text.replace('，', ',').split(',') if s.strip()]
            if symbols:
                valid_symbols = [s for s in symbols if s in stock_map]
                invalid_symbols = [s for s in symbols if s not in stock_map]
                if invalid_symbols:
                    st.sidebar.warning(f"未找到代码: {','.join(invalid_symbols)}")
                # Filter
                selected_symbols = valid_symbols
                filtered_df = filtered_df[filtered_df['symbol'].isin(selected_symbols)]


    elif filter_mode == "行业筛选":
        # 2. Industry Filter (First to narrow down options)
        st.sidebar.markdown("#### 🏭 行业筛选")

        # Toggle for Multi-select
        allow_multi = st.sidebar.checkbox("行业多选模式", value=False, key="filter_ind_multi")
        industry_filters = {}

        for col, label in INDUSTRY_COL_DICT.items():
            if col in df.columns:
                # Use filtered_df to implement cascading filters (options narrow down as you filter)
                # But if we want to sort by *count*, we do it here.
                counts = filtered_df[col].value_counts(dropna=False)
                options = counts.index.tolist()

                if not allow_multi:
                    # Single select with "All"
                    options = ["全部"] + options
                    selected = st.sidebar.selectbox(
                        f"{label}",
                        options=options,
                        index=0,
                        key=f"filter_ind_{col}"
                    )
                    if selected != "全部":
                        filtered_df = filtered_df[filtered_df[col] == selected]
                else:
                    selected = st.sidebar.multiselect(
                        f"{label}",
                        options=options,
                        default=[],
                        key=f"filter_ind_{col}"
                    )
                    if selected:
                        filtered_df = filtered_df[filtered_df[col].isin(selected)]

    elif filter_mode == "股票搜索":

        # 1. Search & Select (After industry filter)
        st.sidebar.markdown("#### 🔍 股票搜索")

        # # Ensure we are working on a copy to avoid SettingWithCopyWarning
        # filtered_df = filtered_df.copy()

        # Prepare options
        # Ensure company exists
        if 'company' not in filtered_df.columns:
            filtered_df['company'] = ''

        filtered_df['display_name'] = filtered_df['symbol'] + "-" + filtered_df['company'].fillna('').astype(str)

        options = filtered_df['display_name'].unique().tolist()

        selected_items = st.sidebar.multiselect(
            "搜索并选择 (留空显示所有筛选结果)",
            options=options,
            key="filter_stock_select",
            # default=default_stock_search_list,
        )

        if selected_items:
            filtered_df = filtered_df[filtered_df['display_name'].isin(selected_items)]
        else:
            filtered_df = filtered_df.iloc[:5, :]

    return filtered_df
