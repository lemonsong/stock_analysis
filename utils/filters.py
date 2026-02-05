import streamlit as st
import pandas as pd

def render_filter_sidebar(df, key="default", enable_submit_button=True):
    """
    Renders the shared stock filter component in the sidebar.

    Args:
        df: The dataframe containing stock data.
            Must contain 'symbol'.
            Should contain 'company', 'industry_category_name', 'industry_type_name', 'industry_sub_category_name' for full functionality.
        key: A unique key prefix for session state management.
        enable_submit_button: If True (default), filters are applied only when the 'Apply Filters & Render' button is clicked.
                              If False, filters are applied immediately (reactive).

    Returns:
        pd.DataFrame: The filtered dataframe.
    """
    st.sidebar.markdown("### 股票筛选")

    # Selection Mode
    filter_mode = st.sidebar.radio(
        "筛选模式",
        ["组合筛选", "列表导入"],
        index=0,
        horizontal=True,
        key=f"{key}_filter_mode_radio"
    )

    # Ensure we are working on a copy to avoid SettingWithCopyWarning
    filtered_df = df.copy()

    # Ensure symbol is string
    if 'symbol' in filtered_df.columns:
        filtered_df['symbol'] = filtered_df['symbol'].astype(str)

    if filter_mode == "列表导入":
        st.sidebar.caption("输入股票代码，用逗号分隔 (例如: SH600000,SZ000001)")
        default_list = ""

        input_text = st.sidebar.text_area("股票代码列表", default_list, key=f"{key}_filter_list_input")

        if input_text:
            # Parse input
            symbols = [s.strip() for s in input_text.replace('，', ',').split(',') if s.strip()]

            # Filter
            if symbols:
                filtered_df = filtered_df[filtered_df['symbol'].isin(symbols)]

    else: # 组合筛选
        # 2. Industry Filter (First to narrow down options)
        st.sidebar.markdown("#### 🏭 行业筛选")

        # Check if industry columns exist
        ind_cols = {
            'industry_category_name': '门类',
            'industry_type_name': '大类',
            'industry_sub_category_name': '次类'
        }

        # Toggle for Multi-select
        allow_multi = st.sidebar.checkbox("行业多选模式", value=False, key=f"{key}_filter_ind_multi")

        for col, label in ind_cols.items():
            if col in df.columns:
                # Use filtered_df to implement cascading filters (options narrow down as you filter)
                # But if we want to sort by *count*, we do it here.
                counts = filtered_df[col].value_counts()
                options = counts.index.tolist()

                if allow_multi:
                    selected = st.sidebar.multiselect(
                        f"{label}",
                        options=options,
                        default=[],
                        key=f"{key}_filter_ind_{col}"
                    )
                    if selected:
                        filtered_df = filtered_df[filtered_df[col].isin(selected)]
                else:
                    # Single select with "All"
                    options = ["全部"] + options
                    selected = st.sidebar.selectbox(
                        f"{label}",
                        options=options,
                        index=0,
                        key=f"{key}_filter_ind_{col}"
                    )
                    if selected != "全部":
                        filtered_df = filtered_df[filtered_df[col] == selected]

        # 1. Search & Select (After industry filter)
        st.sidebar.markdown("#### 🔍 股票搜索")

        # Ensure we are working on a copy to avoid SettingWithCopyWarning
        filtered_df = filtered_df.copy()

        # Prepare options
        # Ensure company exists
        if 'company' not in filtered_df.columns:
            filtered_df['company'] = ''

        filtered_df['display_name'] = filtered_df['symbol'] + " " + filtered_df['company'].fillna('').astype(str)

        options = filtered_df['display_name'].unique().tolist()

        selected_items = st.sidebar.multiselect(
            "搜索并选择 (留空显示所有筛选结果)",
            options=options,
            key=f"{key}_filter_stock_select"
        )

        if selected_items:
            filtered_df = filtered_df[filtered_df['display_name'].isin(selected_items)]

    # --- Apply / Render Button ---
    if enable_submit_button:
        apply_btn = st.sidebar.button("Apply Filters & Render", key=f"{key}_apply_btn")

        # Store the filtered result in session state
        session_key = f"{key}_active_filtered_df"

        # If button clicked OR first run (not in session state), update
        # Note: If we want initial state to be fully loaded, we check `session_key not in st.session_state`
        if apply_btn or session_key not in st.session_state:
            st.session_state[session_key] = filtered_df

        return st.session_state[session_key]
    else:
        # Reactive mode: Return filtered_df immediately
        return filtered_df
