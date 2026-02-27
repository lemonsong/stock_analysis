"""
页面2: A股买卖信号展示
"""
import re

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
from pathlib import Path
from utils.constants import FUNDAMENTAL_KEY_COLS, SEQUENTIAL_COLOR, PROJECT_PATH, INDUSTRY_COL_DICT
from utils.streamlit_helper import clear_cache, clean_expired_cache, setup_page_config, render_filter_sidebar

setup_page_config()

@st.cache_data
def load_decision_data():
    """加载决策数据"""
    decision_file = Path(PROJECT_PATH) / 'data/dwa/app_decision.csv'

    if not decision_file.exists():
        st.error(f"未找到数据文件: {decision_file}")
        return None

    try:
        df = pd.read_csv(decision_file)
        return df
    except Exception as e:
        st.error(f"读取数据文件失败: {e}")
        return None

def get_numeric_columns(df):
    """获取数值类型的列"""
    numeric_cols = []
    for col in df.columns:
        if col not in ['symbol', 'company']:
            if df[col].dtype in ['int64', 'float64']:
                numeric_cols.append(col)
            elif df[col].dtype == 'bool':
                # 布尔列也作为筛选选项
                pass
            else:
                # 尝试转换为数值
                try:
                    pd.to_numeric(df[col], errors='raise')
                    numeric_cols.append(col)
                except:
                    pass
    return numeric_cols

def setup_sidebar(df):
    """设置侧边栏筛选条件并返回筛选后的数据"""
    # 1. 股票筛选 (Refactored)
    filtered_df = render_filter_sidebar(df, default_filter_mode=1)

    # 2. 板块筛选 (regex on boards column)
    if "boards" in filtered_df.columns:
        with st.sidebar.expander("板块筛选", expanded=True):
            boards_regex = st.text_input(
                "板块正则筛选",
                value="MSCI|沪深300|科创|高盛|贝莱德",
                key="filter_boards_regex",
                help="正则匹配 boards 列，例如: MSCI|沪深300。留空显示全部"
            )
        if boards_regex.strip():
            try:
                filtered_df = filtered_df[
                    filtered_df["boards"].astype(str).str.contains(
                        boards_regex, case=False, na=False, regex=True
                    )
                ]
            except re.error:
                st.sidebar.warning("正则表达式无效，已忽略板块筛选")

    st.sidebar.markdown("#### 📊 数值列筛选")

    numeric_cols = get_numeric_columns(df)

    # 3. 信号指标筛选
    signal_filters = {}
    with st.sidebar.expander("信号指标", expanded=True):
        signal_cols = [col for col in numeric_cols if 'signal' in col.lower()]
        for col in signal_cols:
            if col in df.columns:
                col_min = float(df[col].min())
                col_max = float(df[col].max())
                signal_filters[col] = st.slider(
                    f"{col}",
                    min_value=col_min,
                    max_value=col_max,
                    value=(0.0 if col == 'overall_signal_count' else col_min, col_max),
                    key=f"filter_{col}"
                )


    # 4. 基本面指标筛选
    fundamental_filters = {}
    with st.sidebar.expander("基本面指标", expanded=True):
        include_null_value = st.checkbox("包含未知指标值", value=True, key="include_null_value")
        fundamental_cols = FUNDAMENTAL_KEY_COLS
        for col in fundamental_cols:
            if col in df.columns:
                col_min = float(df[col].min())
                # col_max = float(df[col].max())
                col_max = df[col].replace([np.inf, -np.inf], np.nan).max()

                # Handle potential all-NaN column
                if pd.isna(col_min) or pd.isna(col_max):
                    continue

                fundamental_filters[col] = st.slider(
                    f"{col}",
                    min_value=col_min,
                    max_value=col_max,
                    # value=(col_min, col_max),
                    value=(0.0 if col_min < 0 else col_min, col_max),
                    step=1.0,
                    key=f"filter_{col}"
                )
        fundamental_rank_range = None
        if 'fundamental_rank' in df.columns:
            fundamental_rank_min = float(df['fundamental_rank'].min())
            fundamental_rank_max = float(df['fundamental_rank'].max())
            fundamental_rank_range = st.slider(
                "基本面排名",
                min_value=fundamental_rank_min,
                max_value=fundamental_rank_max,
                value=(fundamental_rank_min, fundamental_rank_max),
                key="filter_fundamental_rank"
            )

    # 5. 其他筛选
    price_range = None
    growth_1Y_range = None
    big_money_inflow_range = None
    total_dividend_yield_1Y_range = None
    if 'close' in numeric_cols:
        with st.sidebar.expander("其他", expanded=False):
            price_min = float(df['close'].min())
            price_max = float(df['close'].max())
            price_range = st.slider(
                "价格",
                min_value=price_min,
                max_value=price_max,
                value=(price_min, price_max),
                key="filter_close"
            )
            growth_1Y_min = float(df['growth_1Y'].min())
            growth_1Y_max = float(df['growth_1Y'].max())
            growth_1Y_range = st.slider(
                "1年均价增长",
                min_value=growth_1Y_min,
                max_value=growth_1Y_max,
                value=(growth_1Y_min, growth_1Y_max),
                key="filter_growth_1Y"
            )
            # big_money_inflow_min = float(df['big_money_net_inflow_ratio_10d'].min())
            # big_money_inflow_max = float(df['big_money_net_inflow_ratio_10d'].max())
            # big_money_inflow_range = st.slider(
            #     "10日主力净流入比",
            #     min_value=big_money_inflow_min,
            #     max_value=big_money_inflow_max,
            #     value=(big_money_inflow_min, big_money_inflow_max),
            #     key="filter_big_money_inflow"
            # )
            total_dividend_yield_1Y_min = float(df['total_dividend_yield_1Y'].min())
            total_dividend_yield_1Y_max = float(df['total_dividend_yield_1Y'].max())
            total_dividend_yield_1Y_range = st.slider(
                "近1年股息率",
                min_value=total_dividend_yield_1Y_min,
                max_value=total_dividend_yield_1Y_max,
                value=(total_dividend_yield_1Y_min, total_dividend_yield_1Y_max),
                key="filter_total_dividend_yield_1Y"
            )

    # --- 应用筛选 ---
    # filtered_df starts as the result of render_filter_sidebar

    # 应用信号筛选
    for col, (min_val, max_val) in signal_filters.items():
        filtered_df = filtered_df[
            (filtered_df[col] >= min_val) &
            (filtered_df[col] <= max_val)
        ]

    # 应用基本面筛选
    for col, (min_val, max_val) in fundamental_filters.items():
        if include_null_value:
            filtered_df = filtered_df[
                ((filtered_df[col] >= min_val) & (filtered_df[col] <= max_val)) |
                filtered_df[col].isna()
            ]
        else:
            filtered_df = filtered_df[
                (filtered_df[col] >= min_val) &
                (filtered_df[col] <= max_val)
            ]

    if fundamental_rank_range:
        if include_null_value:
            filtered_df = filtered_df[
                ((filtered_df['fundamental_rank'] >= fundamental_rank_range[0]) &
                (filtered_df['fundamental_rank'] <= fundamental_rank_range[1])) |
                filtered_df['fundamental_rank'].isna()
            ]
        else:
            filtered_df = filtered_df[
                (filtered_df['fundamental_rank'] >= fundamental_rank_range[0]) &
                (filtered_df['fundamental_rank'] <= fundamental_rank_range[1])
            ]


    # 应用其他筛选
    if price_range:
        filtered_df = filtered_df[
            (filtered_df['close'] >= price_range[0]) &
            (filtered_df['close'] <= price_range[1])
            ]

    if growth_1Y_range:
        filtered_df = filtered_df[
            (filtered_df['growth_1Y'] >= growth_1Y_range[0]) &
            (filtered_df['growth_1Y'] <= growth_1Y_range[1])
            ]

    # if big_money_inflow_range:
    #     filtered_df = filtered_df[
    #         (filtered_df['big_money_net_inflow_ratio_10d'] >= growth_1Y_range[0]) &
    #         (filtered_df['big_money_net_inflow_ratio_10d'] <= growth_1Y_range[1])
    #         ]
    #
    if total_dividend_yield_1Y_range:
        filtered_df = filtered_df[
            (filtered_df['total_dividend_yield_1Y'] >= total_dividend_yield_1Y_range[0]) &
            (filtered_df['total_dividend_yield_1Y'] <= total_dividend_yield_1Y_range[1])
            ]

    # 缓存管理
    with st.sidebar.expander("缓存管理", expanded=False):
        if st.button("🔄 清理过期缓存", use_container_width=True):
            clean_expired_cache()
            st.sidebar.success("已清理过期缓存")

        if st.button("🗑️ 清除所有缓存", use_container_width=True):
            clear_cache()
            st.sidebar.success("已清除所有缓存")
    return filtered_df

def display_metrics(filtered_df):
    """显示统计指标"""
    st.markdown("### 📈 信号统计")

    # Use columns to layout metrics and the first chart
    col_metrics, col_chart = st.columns([1, 1])

    with col_metrics:
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("总股票数", len(filtered_df))
        with m2:
            if 'buy_signal_count' in filtered_df.columns:
                avg_buy = filtered_df['buy_signal_count'].mean()
                st.metric("平均买入信号", f"{avg_buy:.1f}")
        with m3:
            if 'sell_signal_count' in filtered_df.columns:
                avg_sell = filtered_df['sell_signal_count'].mean()
                st.metric("平均卖出信号", f"{avg_sell:.1f}")
        with m4:
            if 'overall_signal_count' in filtered_df.columns:
                avg_overall = filtered_df['overall_signal_count'].mean()
                st.metric("平均综合信号", f"{avg_overall:.1f}")

    # 1. 综合信号数量分布 (Placed next to Metrics)
    if 'overall_signal_count' in filtered_df.columns:
        with col_chart:
            fig_overall = px.histogram(
                filtered_df,
                x='overall_signal_count',
                height=150, # Smaller height to fit better
                nbins=9,
                title='综合信号数量分布',
                labels={'overall_signal_count': '综合信号数量', 'count': '股票数量'}
            )
            fig_overall.update_layout(margin=dict(l=20, r=20, t=40, b=20))
            fig_overall.update_traces(texttemplate='%{y}', textposition='inside')

            st.plotly_chart(fig_overall, use_container_width=True)
    else:
        with col_chart:
            st.warning("综合信号数据不可用")

def display_charts(filtered_df, raw_df):
    colt, cols = st.columns(2)
    with colt:

        st.markdown("### 📊 行业信号分布")
    with cols:
        # Industry Selection Filter
        industry_options = {label: col for col, label in INDUSTRY_COL_DICT.items()}
        selected_industry_label = st.radio(
            "选择行业分类标准:",
            options=list(industry_options.keys()),
            index=0, # Default to the first one (e.g., Category/门类)
            horizontal=True
        )
        selected_industry_col = industry_options[selected_industry_label]

    if selected_industry_col not in filtered_df.columns:
        st.warning(f"数据中缺少列: {selected_industry_col}")
        return

    col1, col2 = st.columns(2)

    # Chart 1: Stock Count per Industry (Bar Chart)
    with col1:
        # 确保overall_signal_count作为分类变量进行堆叠
        chart_df = filtered_df.copy()
        chart_df['overall_signal_str'] = chart_df['overall_signal_count'].astype(str)

        # 为了图例排序，我们可以指定category_orders
        sorted_signals = sorted(filtered_df['overall_signal_count'].unique())
        sorted_signals_str = [str(x) for x in sorted_signals]

        fig_industry = px.histogram(
            chart_df,
            y=selected_industry_col,  # 行业名称改到 y 轴
            x=None,  # histogram 默认会对 y 进行计数，x 保持 None 即可
            color='overall_signal_str',
            orientation='h',  # 设置为横向显示
            title=f'各{selected_industry_label}综合信号分布(#)',
            labels={
                selected_industry_col: selected_industry_label,
                'count': '股票数量',
                'overall_signal_str': '综合信号'
            },
            height=600,
            category_orders={'overall_signal_str': sorted_signals_str},
            barmode='group'  # https://plotly.github.io/plotly.py-docs/generated/plotly.express.histogram.html

        )

        # 优化 UI：在条形图上显示具体数值，并让 y 轴标签更易读
        fig_industry.update_traces(texttemplate='%{x}', textposition='inside')
        fig_industry.update_layout(
            xaxis_title="股票数量",
            yaxis_title="行业分类",
            # yaxis={'categoryorder': 'total ascending'}  # 按总量从小到大排序，方便查看
        )

        st.plotly_chart(fig_industry, use_container_width=True)

    # Chart 2: Signal Distribution per Industry (Stacked Bar, Percentage relative to Total Industry Count)
    with col2:
        if 'overall_signal_count' in filtered_df.columns:
            # 1. Calculate Total Count per Industry from RAW data
            if selected_industry_col in raw_df.columns:
                total_counts = raw_df[selected_industry_col].value_counts().reset_index()
                total_counts.columns = [selected_industry_col, 'total_count']
            else:
                st.warning("原始数据中缺少行业列，无法计算总数。")
                return

            # 2. Calculate Filtered Counts per Industry & Signal
            chart_df = filtered_df.copy()
            chart_df['overall_signal_str'] = chart_df['overall_signal_count'].astype(str)
            grouped = chart_df.groupby([selected_industry_col, 'overall_signal_str']).size().reset_index(name='count')

            # 3. Merge Total Counts into Grouped
            grouped = grouped.merge(total_counts, on=selected_industry_col, how='left')

            # 4. Calculate "Filtered Out" (Missing) Count for each Industry
            # Sum filtered counts per industry
            filtered_totals = grouped.groupby(selected_industry_col)['count'].sum().reset_index(name='filtered_sum')
            filtered_totals = filtered_totals.merge(total_counts, on=selected_industry_col, how='left')
            filtered_totals['missing_count'] = filtered_totals['total_count'] - filtered_totals['filtered_sum']

            # Create rows for "Filtered Out"
            missing_rows = []
            for _, row in filtered_totals.iterrows():
                if row['missing_count'] > 0:
                    missing_rows.append({
                        selected_industry_col: row[selected_industry_col],
                        'overall_signal_str': 'Filtered Out',
                        'count': row['missing_count'],
                        'total_count': row['total_count']
                    })

            if missing_rows:
                missing_df = pd.DataFrame(missing_rows)
                grouped = pd.concat([grouped, missing_df], ignore_index=True)

            # 5. Calculate Percentage
            grouped['percentage'] = (grouped['count'] / grouped['total_count']) * 100

            # 6. Sort signals numerically for consistent legend, keep "Filtered Out" separate or at end
            unique_signals = [s for s in grouped['overall_signal_str'].unique() if s != 'Filtered Out']
            try:
                sorted_signals = sorted(unique_signals, key=lambda x: float(x))
            except:
                sorted_signals = sorted(unique_signals)

            # Add Filtered Out to the end
            category_orders = sorted_signals + ['Filtered Out']

            # Define colors
            # Use default plotly colors for signals, force grey for Filtered Out
            color_map = {'Filtered Out': 'lightgrey'}

            fig_dist = px.bar(
                grouped,
                x='percentage',
                y=selected_industry_col,
                color='overall_signal_str',
            orientation='h',  # 设置为横向显示

                title=f'各{selected_industry_label}综合信号分布(%)',
                labels={
                    selected_industry_col: selected_industry_label,
                    'percentage': '百分比 (%)',
                    'overall_signal_str': '综合信号'
                },
                category_orders={'overall_signal_str': category_orders},
                color_discrete_map=color_map, # This maps specific keys
                height=600,
            barmode='group'  # https://plotly.github.io/plotly.py-docs/generated/plotly.express.histogram.html

            )
            # fig_dist = px.bar(
            #     grouped,
            #     x=selected_industry_col,
            #     y='percentage',
            #     color='overall_signal_str',
            #     title=f'各{selected_industry_label}综合信号分布(%)',
            #     labels={
            #         selected_industry_col: selected_industry_label,
            #         'percentage': '百分比 (%)',
            #         'overall_signal_str': '综合信号'
            #     },
            #     category_orders={'overall_signal_str': category_orders},
            #     color_discrete_map=color_map,  # This maps specific keys
            #     height=500,
            # )
            fig_dist.update_traces(texttemplate='%{x}', textposition='inside')

            # Ensure other colors are still assigned automatically if not in map?
            # px.bar uses color_discrete_sequence if map doesn't cover all.
            # But mixing map and sequence is tricky.
            # Better to not use map if we want dynamic colors for numbers, or build a full map.
            # Let's try update_traces to override specific trace color? No, easier to just not map specific values if possible or use a trick.
            # Plotly Express: if color_discrete_map is provided, keys missing from it will likely be black or default?
            # Let's verify. Usually it's better to build the full map or use a sequence and ensure order.

            # Alternative: Assign specific color to 'Filtered Out' via marker settings?
            # Or build a map for all known signals.
            # Since signals are integers (mostly), we can use a qualitative sequence.

            if 'Filtered Out' in grouped['overall_signal_str'].values:
                 # Update traces for 'Filtered Out' to be grey
                 fig_dist.for_each_trace(
                     lambda t: t.update(marker_color='lightgrey') if t.name == 'Filtered Out' else None
                 )

            st.plotly_chart(fig_dist, use_container_width=True)
        else:
            st.warning("综合信号数据不可用")


def display_detailed_data(filtered_df):
    """显示详细数据表格"""
    st.markdown("### 📋 详细数据")

    # 准备用于显示的DataFrame
    display_df = filtered_df.copy()
    # 列分组定义
    col_groups = {
        "基本信息": ['symbol_url', 'company', 'close', 'market_cap'], # 使用symbol_url替代symbol
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

    # 添加链接转换
    # 构造URL: https://xueqiu.com/S/{stock_symbol}
    # display_df['symbol'] 保持原样，通过column_config配置为LinkColumn
    # 但是LinkColumn需要数据本身是URL。
    # 所以我们需要创建一个新列或者修改symbol列。
    # 为了保持排序和搜索的直观性，我们创建一个新的url列，或者修改symbol列为url但显示为代码。
    # 这里直接修改symbol列为URL，利用LinkColumn的display_text正则来显示代码。
    display_df.insert(0, 'symbol_url', "https://xueqiu.com/S/" + display_df['symbol'].astype(str), allow_duplicates=True)
    # display_df['symbol_url'] = "https://xueqiu.com/S/" + display_df['symbol'].astype(str)
    # ratio will display as percentage
    display_df[col_groups["股价增长(%)"]] = display_df[col_groups["股价增长(%)"]]*100
    display_df[col_groups["分红率(%)"]] = display_df[col_groups["分红率(%)"]]*100
    display_df['roe'] = display_df['roe']*100
    display_df['debt_to_asset'] = display_df['debt_to_asset']*100


    col_col_selection_mode, col_sort_by, col_ascending = st.columns(3)
    with col_col_selection_mode:
        ### 列选择器 ###
        col_selection_mode = st.radio(
            "显示模式",
            options=["显示默认列", "显示所有列", "自定义选择"],
            horizontal=True
        )

    # 默认显示的列
    default_display_cols = []
    for group_name in ["基本信息", "行业信息", "信号指标", "股价增长(%)", "分红率(%)", "股价增长", "基本面指标", "基本面排名", "净流入"]:
        if group_name in col_groups:
            # 注意：filtered_df中没有symbol_url，只有display_df有。
            # col_groups["基本信息"] 包含了 symbol_url。
            available_cols = []
            for col in col_groups[group_name]:
                if col in display_df.columns:
                    available_cols.append(col)
            default_display_cols.extend(available_cols)

    final_display_cols = []
    if col_selection_mode == "显示默认列":
        final_display_cols = default_display_cols
    elif col_selection_mode == "显示所有列":
        final_display_cols = list(display_df.columns)
        # 移除symbol (如果存在)，只保留symbol_url
        if 'symbol' in final_display_cols and 'symbol_url' in final_display_cols:
            final_display_cols.remove('symbol')
    else:
        # 自定义选择
        selected_cols = []
        for group_name, cols in col_groups.items():
            available_cols = [col for col in cols if col in display_df.columns]
            if available_cols:
                # 默认选择
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
        return

    # 排序
    # 因为display_df中有symbol_url，如果用户想按代码排序，其实按symbol_url排序效果一样
    sort_options = [col for col in final_display_cols]

    # 尝试把 'overall_signal_count' 作为默认排序
    default_sort_index = 0
    if 'overall_signal_count' in sort_options:
        default_sort_index = sort_options.index('overall_signal_count')

    with col_sort_by:
        sort_by = st.selectbox("排序方式", options=sort_options, index=default_sort_index)
    with col_ascending:
        ascending = st.checkbox("升序", value=False)

    display_df_sorted = display_df.sort_values(sort_by, ascending=ascending)
    display_df_final = display_df_sorted[final_display_cols]

    # 样式设置
    styled_display = display_df_final.style

    for col in display_df_final.columns:
        if col not in ['symbol_url', 'company'] and display_df_final[col].dtype in ['int64', 'float64']:
            col_min = display_df_final[col].min()
            col_max = display_df_final[col].max()
            if col_max != col_min:
                # 使用背景色渐变（绿色=低值，红色=高值）
                styled_display = styled_display.background_gradient(
                    subset=[col],
                    cmap=SEQUENTIAL_COLOR,
                    vmin=col_min,
                    vmax=col_max
                )

    # 配置列
    column_config = {
        "symbol_url": st.column_config.LinkColumn(
            "股票代码",
            help="点击查看雪球详情",
            display_text=r"https://xueqiu\.com/S/(.*)",
            width="small"
        ),
        "company": st.column_config.Column(
            "名称",
            help="company",
        ),
        "close": st.column_config.NumberColumn(
            "收盘价",
            help="close",
            format="¥%.2f"
        ),
        "market_cap": st.column_config.NumberColumn(
            "市值",
            help="最近一年年报发布日的总股本*最近股价",
            format="¥%.0e"
        ),
        "industry_category_name": st.column_config.Column(
            "门类",
            help="industry_category_name",
            width="small"
        ),
        "industry_sub_category_name": st.column_config.Column(
            "次类",
            help="industry_sub_category_name",
            width="small"
        ),
        "industry_type_name": st.column_config.Column(
            "大类",
            help="industry_type_name",
        ),
        "overall_signal_count": st.column_config.NumberColumn(
            "总体信号数",
            help="买入信号数减卖出信号数",
            format="%d"
        ),
        "buy_signal_count": st.column_config.NumberColumn(
            "买入信号数",
            format="%d"
        ),
        "sell_signal_count": st.column_config.NumberColumn(
            "卖出信号数",
            format="%d"
        ),
        "total_dividend_yield_1Y": st.column_config.NumberColumn(
            "近1年股息率",
            help="过去12个月累计现金分红收益率",
            format="%.2f%%"  # Note: format handles the display
        ),
        "total_dividend_yield_3Y": st.column_config.NumberColumn(
            "近3年股息率",
            help="过去3年累计现金分红收益率",
            format="%.2f%%"  # Note: format handles the display
        ),
        "total_dividend_yield_5Y": st.column_config.NumberColumn(
            "近5年股息率",
            help="过去5年累计现金分红收益率",
            format="%.2f%%"  # Note: format handles the display
        ),
        "total_dividend_1Y": st.column_config.NumberColumn(
            "近1年股息",
            help="过去12个月累计现金分红收益",
            format="%.2f"  # Note: format handles the display
        ),
        "total_dividend_3Y": st.column_config.NumberColumn(
            "近3年股息",
            help="过去3年累计现金分红收益",
            format="%.2f"  # Note: format handles the display
        ),
        "total_dividend_5Y": st.column_config.NumberColumn(
            "近5年股息",
            help="过去5年累计现金分红收益",
            format="%.2f"  # Note: format handles the display
        ),
        "growth_1Y": st.column_config.NumberColumn("近1年均价增长", format="%.2f%%"),
        "growth_2Y": st.column_config.NumberColumn("近2年均价增长", format="%.2f%%"),
        "growth_3Y": st.column_config.NumberColumn("近3年均价增长", format="%.2f%%"),

        "fundamental_score": st.column_config.NumberColumn(
            "基本面评分",
            help="基于各项财务指标的综合评分（越高越好）",
            format="%.2f"
        ),
        "fundamental_rank": st.column_config.NumberColumn(
            "基本面排名",
            help="基于综合评分的年度排名（越小越好）",
            format="%d"
        ),
        "fundamental_fiscal_year": st.column_config.NumberColumn(
            "财报年份",
            help="排名所基于的财报年份",
            format="%d"
        ),
        "roe": st.column_config.NumberColumn(
            "ROE",
            help="",
            format="%.2f%%",
            width="small"),
        "netcash_operate_over_net_profit": st.column_config.NumberColumn(
            "Net cash operate/Net profit",
            help="",
            format="%.2f",
            width="small"),
        "debt_to_asset": st.column_config.NumberColumn(
            "Debt/Asset",
            help="",
            format="%.2f%%",
            width="small"),
        "inventory_turnover": st.column_config.NumberColumn(
            "Inventory Turnover",
            help="",
            format="%.1f 次/年",
        width="small"),
        "ev_over_ebitda": st.column_config.NumberColumn(
            "EV/EBITDA",
            help="",
            format="%.1f x",
        width="small"),
        "big_money_net_inflow_ratio_10d": st.column_config.NumberColumn(
            "主力净流入比",
            help="",
            format="%.2f",
            width="small"),

    }

    # 显示表格
    st.dataframe(
        styled_display,
        use_container_width=True,
        height=700,
        column_config=column_config,
        hide_index=True
    )

    # 显示统计和下载
    st.markdown("#### 📊 数据统计")
    st.write(f"显示 {len(display_df_final)} 行，{len(final_display_cols)} 列")

    csv = display_df_final.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 下载筛选后的数据 (CSV)",
        data=csv,
        file_name=f"ashare_signals_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

    symbol_string = ",".join(filtered_df["symbol"].astype(str).tolist())
    # 3. Display in a code block with the copy button
    st.write("Click the icon on the right to copy filtered symbols:")
    st.code(symbol_string, language=None)

def main():
    st.title("📊 A股买卖信号监测")

    df = load_decision_data()
    if df is None or df.empty:
        st.warning("数据文件为空")
        st.stop()
    # df['big_money_net_inflow_ratio_10d'] = df['big_money_net_inflow_ratio_10d'].astype(float)
    filtered_df = setup_sidebar(df)
    display_metrics(filtered_df)
    display_charts(filtered_df, df)
    display_detailed_data(filtered_df)

if __name__ == "__main__":
    main()
