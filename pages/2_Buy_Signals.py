"""
页面2: A股买卖信号展示
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from pathlib import Path

from config import PROJECT_PATH

st.set_page_config(page_title="A股买卖信号", layout="wide")

st.title("📊 A股买卖信号监测")

# 读取数据
decision_file = Path(PROJECT_PATH) / 'data_app/app_decision.csv'

if not decision_file.exists():
    st.error(f"未找到数据文件: {decision_file}")
    st.stop()

@st.cache_data
def load_decision_data():
    """加载决策数据"""
    try:
        df = pd.read_csv(decision_file)
        return df
    except Exception as e:
        st.error(f"读取数据文件失败: {e}")
        return None

df = load_decision_data()
print(df.head())
if df is None or df.empty:
    st.warning("数据文件为空")
    st.stop()

# 侧边栏：筛选选项
st.sidebar.header("筛选条件")

# 识别数值列（排除布尔列和文本列）
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

# 符号和公司合并搜索
st.sidebar.markdown("#### 🔍 股票搜索")
search_mode = st.sidebar.radio(
    "搜索模式",
    options=["分别搜索", "合并搜索"],
    index=0,
    help="分别搜索：在symbol或company中搜索；合并搜索：在symbol+company的组合中搜索"
)

if search_mode == "分别搜索":
    search_term = st.sidebar.text_input("搜索股票代码或名称", "")
else:
    search_term = st.sidebar.text_input("搜索股票代码+名称组合", "", 
                                        help="例如：SH600000平安银行")

# 数值列筛选器 - 使用折叠面板组织
st.sidebar.markdown("#### 📊 数值列筛选")
# 获取唯一行业列表，并添加“全部”选项
# industry_category_name,industry_sub_category_name,industry_type_name
industry_category_name_list = ["全部"] + list(df['industry_category_name'].unique())
selected_industry_category_name = st.sidebar.selectbox("industry_category_name", industry_category_name_list)

industry_sub_category_name_list = ["全部"] + list(df['industry_sub_category_name'].unique())
selected_industry_sub_category_name = st.sidebar.selectbox("industry_sub_category_name", industry_sub_category_name_list)

industry_type_name_list = ["全部"] + list(df['industry_type_name'].unique())
selected_industry_type_name = st.sidebar.selectbox("industry_type_name", industry_type_name_list)


# 信号相关列
with st.sidebar.expander("信号指标", expanded=True):
    signal_cols = [col for col in numeric_cols if 'signal' in col.lower()]
    for col in signal_cols:
        if col in df.columns:
            col_min = float(df[col].min())
            col_max = float(df[col].max())
            col_range = st.slider(
                f"{col}",
                min_value=col_min,
                max_value=col_max,
                value=(col_min, col_max),
                key=f"filter_{col}"
            )

# # 技术指标列
# with st.sidebar.expander("技术指标", expanded=False):
#     tech_cols = [col for col in numeric_cols if col not in signal_cols and col != 'close']
#     for col in tech_cols[:10]:  # 限制显示前10个，避免界面过长
#         if col in df.columns:
#             col_min = float(df[col].min())
#             col_max = float(df[col].max())
#             col_range = st.slider(
#                 f"{col}",
#                 min_value=col_min,
#                 max_value=col_max,
#                 value=(col_min, col_max),
#                 key=f"filter_{col}"
#             )

# 价格筛选
if 'close' in numeric_cols:
    with st.sidebar.expander("价格", expanded=False):
        price_min = float(df['close'].min())
        price_max = float(df['close'].max())
        price_range = st.slider(
            "价格",
            min_value=price_min,
            max_value=price_max,
            value=(price_min, price_max),
            key="filter_close"
        )

# 应用筛选
filtered_df = df.copy()
# 应用行业筛选
if selected_industry_category_name == "全部":
    filtered_df = filtered_df
else:
    filtered_df = filtered_df[filtered_df['industry_category_name'] == selected_industry_category_name]
if selected_industry_sub_category_name == "全部":
    filtered_df = filtered_df
else:
    filtered_df = filtered_df[filtered_df['industry_sub_category_name'] == selected_industry_sub_category_name]
if selected_industry_type_name == "全部":
    filtered_df = filtered_df
else:
    filtered_df = filtered_df[filtered_df['industry_type_name'] == selected_industry_type_name]

# 应用数值列筛选
for col in numeric_cols:
    if col in filtered_df.columns:
        filter_key = f"filter_{col}"
        if filter_key in st.session_state:
            col_range = st.session_state[filter_key]
            # 只有当范围不是全范围时才应用筛选
            col_min = float(df[col].min())
            col_max = float(df[col].max())
            if col_range[0] > col_min or col_range[1] < col_max:
                filtered_df = filtered_df[
                    (filtered_df[col] >= col_range[0]) &
                    (filtered_df[col] <= col_range[1])
                ]

# 应用搜索筛选
if search_term:
    if search_mode == "分别搜索":
        mask = (
            filtered_df['symbol'].str.contains(search_term, case=False, na=False) |
            filtered_df['company'].str.contains(search_term, case=False, na=False)
        )
    else:  # 合并搜索
        # 创建symbol+company的组合列进行搜索
        combined = (filtered_df['symbol'].astype(str) + filtered_df['company'].astype(str))
        mask = combined.str.contains(search_term, case=False, na=False)
    filtered_df = filtered_df[mask]

# 显示统计信息
st.markdown("### 📈 信号统计")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("总股票数", len(filtered_df))
with col2:
    if 'buy_signal_count' in filtered_df.columns:
        avg_buy = filtered_df['buy_signal_count'].mean()
        st.metric("平均买入信号", f"{avg_buy:.1f}")
with col3:
    if 'sell_signal_count' in filtered_df.columns:
        avg_sell = filtered_df['sell_signal_count'].mean()
        st.metric("平均卖出信号", f"{avg_sell:.1f}")
with col4:
    if 'overall_signal_count' in filtered_df.columns:
        avg_overall = filtered_df['overall_signal_count'].mean()
        st.metric("平均综合信号", f"{avg_overall:.1f}")

# 可视化
st.markdown("### 📊 信号分布")

log_path = '/Users/yilin/Documents/Projects/stock_analysis/.cursor/debug.log'

# #region agent log
try:
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps({"sessionId":"debug-session","runId":"pre-fix","hypothesisId":"A","location":"pages/2_Buy_Signals.py:125","message":"Checking overall_signal_count column existence","data_all_list":{"has_overall":'overall_signal_count' in filtered_df.columns,"filtered_df_shape":filtered_df.shape,"columns":list(filtered_df.columns)},"timestamp":pd.Timestamp.now().timestamp()*1000})+'\n')
except: pass
# #endregion

# #region agent log
try:
    with open(log_path, 'a', encoding='utf-8') as f:
        overall_data = filtered_df['overall_signal_count'] if 'overall_signal_count' in filtered_df.columns else None
        f.write(json.dumps({"sessionId":"debug-session","runId":"pre-fix","hypothesisId":"B","location":"pages/2_Buy_Signals.py:130","message":"Checking overall_signal_count for NaN values","data_all_list":{"has_nan":int(overall_data.isna().sum()) if overall_data is not None else None,"dtype":str(overall_data.dtype) if overall_data is not None else None,"min":float(overall_data.min()) if overall_data is not None and not overall_data.empty else None,"max":float(overall_data.max()) if overall_data is not None and not overall_data.empty else None},"timestamp":pd.Timestamp.now().timestamp()*1000})+'\n')
except: pass
# #endregion

# Display overall_signal_count histogram instead of separate buy/sell histograms
if 'overall_signal_count' in filtered_df.columns:
    # #region agent log
    try:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"pre-fix","hypothesisId":"D","location":"pages/2_Buy_Signals.py:135","message":"Creating overall_signal_count histogram","data_all_list":{"data_points":len(filtered_df['overall_signal_count'].dropna()),"nbins":20},"timestamp":pd.Timestamp.now().timestamp()*1000})+'\n')
    except: pass
    # #endregion
    
    fig_overall = px.histogram(
        filtered_df,
        x='overall_signal_count',
        nbins=10,
        title='综合信号数量分布',
        labels={'overall_signal_count': '综合信号数量', 'count': '股票数量'}
    )
    st.plotly_chart(fig_overall, use_container_width=True)
    
    # #region agent log
    try:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"pre-fix","hypothesisId":"D","location":"pages/2_Buy_Signals.py:145","message":"Histogram created successfully","data_all_list":{"status":"success"},"timestamp":pd.Timestamp.now().timestamp()*1000})+'\n')
    except: pass
    # #endregion
else:
    # #region agent log
    try:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"pre-fix","hypothesisId":"A","location":"pages/2_Buy_Signals.py:148","message":"overall_signal_count column not found","data_all_list":{"available_columns":list(filtered_df.columns)},"timestamp":pd.Timestamp.now().timestamp()*1000})+'\n')
    except: pass
    # #endregion
    st.warning("综合信号数据不可用")

# 数据表格
st.markdown("### 📋 详细数据")

# 列分组定义
col_groups = {
    "基本信息": ['symbol', 'company', 'close'],
    "行业信息": ['industry_category_name', 'industry_sub_category_name', 'industry_type_name'],
    "信号指标": [col for col in filtered_df.columns if 'signal' in col.lower()],
    "技术指标": [col for col in filtered_df.columns if col not in ['symbol', 'company', 'close'] and 'signal' not in col.lower() and filtered_df[col].dtype in ['int64', 'float64']],
    "布尔指标": [col for col in filtered_df.columns if filtered_df[col].dtype == 'bool'],
    "分红指标": [col for col in filtered_df.columns if 'Total_Yield' in col],
}

# 计算默认显示的列（基本信息 + 信号指标）
default_display_cols = []
for group_name in ["基本信息", "行业信息", "信号指标", "分红指标"]:
    if group_name in col_groups:
        available_cols = [col for col in col_groups[group_name] if col in filtered_df.columns]
        default_display_cols.extend(available_cols)

# 如果没有默认列，则使用所有列
if not default_display_cols:
    default_display_cols = list(filtered_df.columns)

# 列选择器 - 允许用户选择要显示的列
st.markdown("#### 列选择")
col_selection_mode = st.radio(
    "显示模式",
    options=["显示所有列", "自定义选择"],
    horizontal=True
)

if col_selection_mode == "显示所有列":
    display_cols = list(filtered_df.columns)
else:
    # 自定义选择模式 - 默认选择基本信息 + 信号指标
    selected_cols = []
    for group_name, cols in col_groups.items():
        available_cols = [col for col in cols if col in filtered_df.columns]
        if available_cols:
            # 默认选择：基本信息全部选中，信号指标全部选中，其他组不选中
            default_selection = available_cols if group_name in ["基本信息", "信号指标"] else []
            
            with st.expander(f"{group_name} ({len(available_cols)}列)", expanded=(group_name == "基本信息")):
                selected = st.multiselect(
                    f"选择{group_name}",
                    options=available_cols,
                    default=default_selection,
                    key=f"cols_{group_name}"
                )
                selected_cols.extend(selected)
    
    # 如果用户没有选择任何列，使用默认列
    display_cols = selected_cols if selected_cols else default_display_cols

# 排序选项 - 包含所有数值列
sort_options = ['symbol', 'company'] + numeric_cols
sort_by = st.selectbox(
    "排序方式",
    options=sort_options,
    index=sort_options.index('overall_signal_count') if 'overall_signal_count' in sort_options else 0,
    format_func=lambda x: {
        'overall_signal_count': '综合信号',
        'buy_signal_count': '买入信号',
        'sell_signal_count': '卖出信号',
        'close': '价格',
        'symbol': '股票代码',
        'company': '公司名称'
    }.get(x, x)
)

if sort_by in filtered_df.columns:
    ascending = st.checkbox("升序", value=False)
    filtered_df_sorted = filtered_df.sort_values(sort_by, ascending=ascending)
else:
    filtered_df_sorted = filtered_df

# 确保display_cols中的列都存在
display_cols = [col for col in display_cols if col in filtered_df_sorted.columns]

# 创建带颜色映射的数据表格
def style_dataframe(df_subset):
    """为数值列添加颜色映射"""
    styled_df = df_subset.copy()
    
    # 为每个数值列添加颜色样式
    for col in df_subset.columns:
        if col not in ['symbol', 'company'] and df_subset[col].dtype in ['int64', 'float64']:
            # 计算颜色映射（使用线性映射）
            col_min = df_subset[col].min()
            col_max = df_subset[col].max()
            
            if col_max != col_min:
                # 归一化到0-1范围
                normalized = (df_subset[col] - col_min) / (col_max - col_min)
                # 应用颜色（绿色到红色）
                colors = normalized.apply(lambda x: f"background-color: rgba({int(255*(1-x))}, {int(255*x)}, 0, 0.3)")
                styled_df[col] = colors
            else:
                styled_df[col] = ""
    
    return styled_df

# 显示数据表格（使用styler进行颜色映射）
if display_cols:
    display_df = filtered_df_sorted[display_cols].copy()
    
    # 创建样式化的DataFrame
    styled_display = display_df.style
    
    # 为每个数值列应用颜色背景
    for col in display_df.columns:
        if col not in ['symbol', 'company'] and display_df[col].dtype in ['int64', 'float64']:
            col_min = display_df[col].min()
            col_max = display_df[col].max()
            
            if col_max != col_min:
                # 使用背景色渐变（绿色=低值，红色=高值）
                styled_display = styled_display.background_gradient(
                    subset=[col],
                    # cmap='RdYlGn',  # 红-黄-绿
                    cmap='PuBu',  # 红-黄-绿
                    vmin=col_min,
                    vmax=col_max
                )
    
    st.dataframe(
        styled_display,
        use_container_width=True,
        height=600
    )
    
    # 显示数据统计
    st.markdown("#### 📊 数据统计")
    st.write(f"显示 {len(display_df)} 行，{len(display_cols)} 列")
    
    # 下载按钮
    csv = filtered_df_sorted[display_cols].to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 下载筛选后的数据 (CSV)",
        data=csv,
        file_name=f"ashare_signals_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )
else:
    st.warning("请至少选择一列进行显示")
