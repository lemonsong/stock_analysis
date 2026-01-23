"""
页面3: A股基本面分析和比较
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pathlib import Path
import io
import sys
# 添加根目录到sys.path以便导入utils
# sys.path.append(str(Path(__file__).parent.parent))
from utils.streamlit_helper import setup_page_config

from config import PROJECT_PATH

setup_page_config()

st.title("📊 A股基本面分析和比较")

# 数据文件路径
fundamental_file = Path(PROJECT_PATH) / 'data_ak_fundamental' / 'fundamental_calculated_metrics.csv'

if not fundamental_file.exists():
    st.error(f"未找到基本面数据文件: {fundamental_file}")
    st.stop()

@st.cache_data
def load_fundamental_data():
    """加载基本面数据"""
    try:
        df = pd.read_csv(fundamental_file)
        # 确保fiscal_year是数值类型
        if 'fiscal_year' in df.columns:
            df['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"读取数据文件失败: {e}")
        return None

@st.cache_data
def get_stock_list(_df):
    """获取股票列表（缓存）"""
    return _df[['symbol', 'SECURITY_NAME_ABBR']].drop_duplicates()

@st.cache_data
def filter_stock_data(_df, symbols, year_range):
    """筛选股票数据（缓存）"""
    return _df[
        (_df['symbol'].isin(symbols)) &
        (_df['fiscal_year'] >= year_range[0]) &
        (_df['fiscal_year'] <= year_range[1])
    ]

import streamlit as st

def render_indicator_help():
    """
    在 Streamlit 中渲染可折叠的财务指标帮助信息
    使用 Tabs 布局分类展示 24 个核心指标
    """
    with st.expander("📘 财务指标深度解读手册 (点击展开/折叠)", expanded=False):
        # 创建六个标签页对应六大类指标
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "短期风险", "长期偿债", "获利能力", "资产效率", "现金质量", "估值指标","综合"
        ])

        with tab1:
            st.subheader("一、 流动性与偿债能力（短期风险）")
            st.markdown("""
            - **Current Ratio (流动比率)**：流动资产/流动负债。衡量短期还债能力。一般 >2 为健壮。  
              ⚠️ **警惕**：比例过高可能意味着资金闲置或存货积压。
            - **Quick Ratio (速动比率)**：(流动资产-存货)/流动负债。剔除变现慢的存货。一般 >1 为安全。  
              ⚠️ **警惕**：若远低于流动比率，说明企业极度依赖卖货来还债。
            - **Cash Ratio (现金比率)**：现金及等价物/流动负债。最严苛的变现能力指标。  
              ⚠️ **警惕**：过低代表一旦银行断贷，企业立即面临违约。
            - **Total Debt (总负债)**：公司承担的所有债务总额。  
              ⚠️ **警惕**：绝对值增长速度若超过利润增长，需关注利息压力。
            - **Net Debt (净负债)**：总负债 - 现金。反映真实的债务负担。负值代表“现金多于债务”。  
              ⚠️ **警惕**：净负债剧增通常预示着大规模扩张或失血。
            """)

        with tab2:
            st.subheader("二、 资本结构与长期偿债")
            st.markdown("""
            - **Debt to Equity (产权比率)**：总负债/股东权益。衡量财务杠杆。  
              ⚠️ **警惕**：>1 意味着债权人出的钱比股东多，风险较高。
            - **Debt to Asset (资产负债率)**：总负债/总资产。衡量总资产中借钱的占比。  
              ⚠️ **警惕**：超过 60%-70%（非金融行业）需警惕破产风险或商誉减值冲击。
            - **Interest Coverage (利息保障倍数)**：息税前利润/利息支出。衡量赚的钱够付几次利息。建议 >3。  
              ⚠️ **警惕**：<1 说明赚的钱连利息都不够付，极度危险。
            """)

        with tab3:
            st.subheader("三、 经营规模与获利能力")
            st.markdown("""
            - **Revenue (营业收入)**：生意规模。  
              ⚠️ **警惕**：营收停滞但利润上升，往往是靠“省钱”而非“增长”。
            - **Gross Profit (毛利)**：营收 - 直接成本。反映产品竞争力。  
              ⚠️ **警惕**：连续多年下降说明行业进入惨烈价格战。
            - **Net Profit (净利润)**：最终到手的钱。  
              ⚠️ **警惕**：需结合非经常性损益看，谨防变卖资产凑出来的“虚假盈利”。
            - **Gross Margin (毛利率)**：毛利/营收。产品溢价能力。  
              ⚠️ **警惕**：突降通常意味着原材料暴涨或竞争对手降价。
            - **Operating Margin (营业利润率)**：营业利润/营收。反映管理和销售效率。  
              ⚠️ **警惕**：与毛利率背离说明内部开支（如研发、管理）失控。
            - **Profit Margin (净利率)**：净利润/营收。  
              ⚠️ **警惕**：极低的净利率意味着企业容错率极低，任何成本波动都能导致亏损。
            """)

        with tab4:
            st.subheader("四、 资产效率（翻台率）")
            st.markdown("""
            - **Asset Turnover (资产周转率)**：营收/总资产。衡量利用资产赚钱的效率。  
              ⚠️ **警惕**：逐年下降说明公司资产变得“沉重”，效率降低。
            - **Inventory Turnover (存货周转率)**：营收/存货。货卖得快不快。  
              ⚠️ **警惕**：骤降代表产品可能过时或滞销，面临计提损失。
            - **Receivables Turnover (应收账款周转率)**：营收/应收账款。钱收得回不回。  
              ⚠️ **警惕**：持续下降说明公司对下游话语权丧失，或在虚增收入。
            """)

        with tab5:
            st.subheader("五、 股东回报与现金质量")
            st.markdown("""
            - **ROE (净资产收益率)**：股东出的一块钱能赚多少钱。核心指标。  
              ⚠️ **警惕**：靠高负债强行推高的 ROE 极具风险。
            - **ROA (总资产收益率)**：衡量所有资产（含借的钱）的赚钱效率。  
              ⚠️ **警惕**：若 ROA 远低于 ROE，说明杠杆加得非常大。
            - **Netcash Operate over Net Profit (净现比)**：经营现金流净额/净利润。利润含金量。  
              ⚠️ **警惕**：长期 <1 说明利润多是“纸上富贵”，没有真现金流入。
            - **Free Cash Flow Conversion (自由现金流转化率)**：FCF / 净利润。  
              ⚠️ **警惕**：转化率低甚至为负，说明公司是个“吞金兽”，赚的钱全投回设备更新了。
            - **Change in Working Capital (营运资本变动)**：经营中压进去的钱。  
              ⚠️ **警惕**：正值过大代表现金被应收和存货占满，现金流会枯竭。
            """)

        with tab6:
            st.subheader("六、 估值指标")
            st.markdown("""
            - **Net Debt over EBITDA (净债务/EBITDA)**：衡量还清债务需要多少年经营利润。  
              ⚠️ **警惕**：>3 通常被银行视为高风险。
            - **EV over EBITDA (企业价值倍数)**：收购成本/经营现金流能力。比 PE 更稳健的估值。  
              ⚠️ **警惕**：需与行业均值对比，过高代表溢价过大。
            """)

        with tab7:
            st.subheader("七、 综合")
            st.markdown("""
            1. ROE (净资产收益率) —— 【核心回报】
地位：它是财务分析的“定海神针”。

理由：ROE 综合了净利率、资产周转率和财务杠杆（杜邦分析法）。它直接告诉你股东投入的每一块钱净资产产生了多少回报。如果只能看一个指标，那就是 ROE。

看板标准：持续多年稳定在 15% 以上 的通常是长跑冠军。

2. 净现比 (Netcash/Net Profit) —— 【获利质量】
地位：它是利润的“防伪标签”。

理由：纸面利润可以通过会计手段调节，但现金流很难伪造。这个指标衡量赚到的 1 块钱利润里，真正落袋为安的现金有多少。

看板标准：理想值应 ≥ 1.0。如果长期低于 0.8，说明公司可能存在大量收不回来的欠款（应收账款）或卖不掉的库存。

3. 资产负债率 (Debt to Asset) —— 【生存底线】
地位：它是公司的“安全带”。

理由：即使公司再赚钱，如果杠杆过高，一旦遇到行业寒冬或信用收紧，极易发生资金断裂。它定义了公司的抗风险边界。

看板标准：非金融行业通常 < 60% 为安全。若该指标极高且利息保障倍数低，公司随时有暴雷风险。

4. 存货/应收账款周转率 —— 【营运效率】
地位：它是企业的“代谢能力”。

理由：反映了资产变现的速度。货卖得快不快？钱收回得顺不顺？在你的数据集中，这两个指标可以合并看作运营能力的代表。

看板标准：逐年递增或保持稳定。如果周转率突然大幅下降，通常预示着产品竞争力下滑或下游客户违约风险增加。

5. EV/EBITDA (企业价值倍数) —— 【估值定价】
地位：它是专业的“买入标尺”。

理由：相比 PE（市盈率），它剔除了非经常性损益、利息和税收的干扰，更公平地反映了企业核心业务的估值。它是机构投资者判断“买得值不值”的核心工具。

看板标准：寻找“低估值 + 高ROE”的交叉点。
            """)





df = load_fundamental_data()

if df is None or df.empty:
    st.warning("数据文件为空")
    st.stop()

# 侧边栏：股票选择
st.sidebar.header("选择股票")

# 功能1: 股票搜索功能
st.sidebar.markdown("#### 🔍 搜索股票")
search_term = st.sidebar.text_input("搜索股票代码或名称", "")

# 获取所有可用的股票
available_stocks = get_stock_list(df)

# 应用搜索过滤
if search_term:
    available_stocks = available_stocks[
        (available_stocks['symbol'].str.contains(search_term, case=False, na=False)) |
        (available_stocks['SECURITY_NAME_ABBR'].str.contains(search_term, case=False, na=False))
    ]

stock_options = {
    f"{row['symbol']} - {row['SECURITY_NAME_ABBR']}": row['symbol']
    for _, row in available_stocks.iterrows()
}

# 功能1: 快速选择按钮
st.sidebar.markdown("#### ⚡ 快速选择")
if st.sidebar.button("选择前5只股票", use_container_width=True):
    if len(list(stock_options.keys())) >= 5:
        selected_stock_keys = list(stock_options.keys())[:5]
        st.session_state['selected_stocks'] = selected_stock_keys
    else:
        st.sidebar.warning("可用股票不足5只")

# 从session_state恢复选择（如果有）
if 'selected_stocks' in st.session_state:
    default_selection = st.session_state['selected_stocks']
else:
    default_selection = []

selected_stock_keys = st.sidebar.multiselect(
    "选择要对比的股票（最多5只）",
    options=list(stock_options.keys()),
    default=default_selection,
    max_selections=5
)

# 保存选择到session_state
if selected_stock_keys:
    st.session_state['selected_stocks'] = selected_stock_keys

selected_symbols = [stock_options[key] for key in selected_stock_keys]

# 功能4: 数据质量检查
st.sidebar.markdown("#### 📊 数据质量")
if 'fiscal_year' in df.columns:
    data_coverage = df.groupby('symbol')['fiscal_year'].count()
    avg_years = data_coverage.mean()
    st.sidebar.metric("平均数据年份数", f"{avg_years:.1f}")
    
    if selected_symbols:
        selected_coverage = df[df['symbol'].isin(selected_symbols)].groupby('symbol')['fiscal_year'].count()
        st.sidebar.metric("选中股票平均年份", f"{selected_coverage.mean():.1f}")

# 如果没有选择股票，显示提示
if not selected_symbols:
    st.info("请在左侧选择要分析的股票（最多5只）")
    
    # 显示可用股票列表
    st.markdown("### 可用股票列表")
    st.dataframe(
        available_stocks.sort_values('SECURITY_NAME_ABBR'),
        use_container_width=True,
        height=400
    )
    st.stop()

# 筛选选中的股票数据
filtered_df = df[df['symbol'].isin(selected_symbols)].copy()

# 获取股票名称映射
stock_names = {
    row['symbol']: row['SECURITY_NAME_ABBR']
    for _, row in filtered_df[['symbol', 'SECURITY_NAME_ABBR']].drop_duplicates().iterrows()
}

# 功能5: 重新组织的财务指标分类
financial_metrics = {
    'Liquidity Ratios（流动性比率）': ['current_ratio', 'quick_ratio', 'cash_ratio'],
    'Leverage Ratios（杠杆比率）': ['total_debt', 'net_debt', 'debt_to_equity', 'debt_to_asset', 'interest_coverage'],
    'Efficiency Ratios（效率比率）': ['revenue', 'gross_profit', 'net_profit', 'asset_turnover', 'inventory_turnover', 'receivables_turnover'],
    'Profitability Ratios（盈利能力比率）': ['gross_margin', 'operating_margin', 'profit_margin', 'roe', 'roa'],
    'Cash Flow & Valuation Metrics（现金流和估值指标）': ['netcash_operate_over_net_profit', 'free_cash_flow_conversion_rate', 'change_in_working_capital', 'net_debt_over_ebitda', 'ev_over_ebitda']
}

# 获取所有可用的指标
available_metrics = [col for col in df.columns 
                     if col not in ['symbol', 'SECURITY_NAME_ABBR', 'fiscal_year', 'ORG_TYPE']]

# 主内容区域
st.markdown("### 📈 财务指标对比")

# 选择要展示的指标类别
metric_category = st.selectbox(
    "选择指标类别",
    options=list(financial_metrics.keys()) + ['自定义'],
    index=0
)

if metric_category == '自定义':
    selected_metrics = st.multiselect(
        "选择要对比的指标",
        options=available_metrics,
        default=['roe','netcash_operate_over_net_profit','debt_to_asset','inventory_turnover','ev_over_ebitda']
        # default=available_metrics[:5] if len(available_metrics) >= 5 else available_metrics
    )
else:
    selected_metrics = [m for m in financial_metrics[metric_category] if m in available_metrics]

if not selected_metrics:
    st.warning("请选择要展示的指标")
    st.stop()

# 功能4: 显示缺失数据警告
missing_data = filtered_df[selected_metrics].isna().sum()
if missing_data.sum() > 0:
    missing_info = {k: int(v) for k, v in missing_data[missing_data > 0].items()}
    st.warning(f"⚠️ 部分数据缺失：{missing_info}")

# 选择时间范围
if 'fiscal_year' in filtered_df.columns:
    min_year = int(filtered_df['fiscal_year'].min())
    max_year = int(filtered_df['fiscal_year'].max())
    
    year_range = st.slider(
        "选择年份范围",
        min_value=min_year,
        max_value=max_year,
        value=(max(min_year, max_year - 5), max_year)
    )
    
    # 使用缓存的筛选函数
    filtered_df = filter_stock_data(df, selected_symbols, year_range)

# 调用指标介绍
render_indicator_help()

# 创建标签页展示不同类型的分析
tab1, tab2, tab3 = st.tabs(["📊 指标对比表", "📈 趋势图表", "💹 综合分析"])


with tab1:
    st.markdown("#### 财务指标对比表")
    
    # 功能2: 创建带颜色映射的透视表
    for metric in selected_metrics:
        if metric not in filtered_df.columns:
            continue
        
        st.markdown(f"##### {metric}")
        
        # 创建透视表：股票 x 年份
        pivot_data = filtered_df.pivot_table(
            index='symbol',
            columns='fiscal_year',
            values=metric,
            aggfunc='first'
        )
        
        # 添加股票名称
        pivot_data.index = [
            f"{idx} - {stock_names.get(idx, idx)}"
            for idx in pivot_data.index
        ]
        
        # 功能2: 添加颜色映射
        try:
            styled_pivot = pivot_data.style.background_gradient(
                cmap='RdYlGn',
                axis=1  # 按行（股票）应用颜色
            )
            st.dataframe(
                styled_pivot,
                use_container_width=True
            )
        except:
            # 如果样式化失败，显示普通表格
            st.dataframe(
                pivot_data,
                use_container_width=True
            )
        
        st.divider()

with tab2:
    st.markdown("#### 财务指标趋势图")
    
    # 功能3: 多指标对比模式
    compare_mode = st.radio(
        "对比模式",
        options=["单指标多股票", "多指标单股票", "多指标多股票"],
        horizontal=True
    )
    
    if compare_mode == "单指标多股票":
        # 原有模式：为每个指标创建趋势图
        for metric in selected_metrics:
            if metric not in filtered_df.columns:
                continue
            
            st.markdown(f"##### {metric}")
            
            # 准备数据
            plot_df = filtered_df[['symbol', 'fiscal_year', metric]].copy()
            plot_df = plot_df.dropna(subset=[metric])
            
            if plot_df.empty:
                st.warning(f"指标 {metric} 无可用数据")
                continue
            
            # 创建图表
            fig = px.line(
                plot_df,
                x='fiscal_year',
                y=metric,
                color='symbol',
                markers=True,
                labels={
                    'fiscal_year': '年份',
                    metric: metric,
                    'symbol': '股票代码'
                },
                title=f"{metric} 趋势对比"
            )
            
            # 更新布局
            fig.update_layout(
                hovermode='x unified',
                height=400,
                template='plotly_white'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            st.divider()
    
    elif compare_mode == "多指标单股票":
        # 功能3: 多指标单股票对比
        selected_stock_for_comparison = st.selectbox(
            "选择要分析的股票",
            options=selected_symbols,
            format_func=lambda x: f"{x} - {stock_names.get(x, x)}"
        )
        
        # 创建子图
        n_metrics = len(selected_metrics)
        fig = make_subplots(
            rows=n_metrics,
            cols=1,
            subplot_titles=selected_metrics,
            vertical_spacing=0.05
        )
        
        for i, metric in enumerate(selected_metrics, 1):
            if metric not in filtered_df.columns:
                continue
            
            stock_data = filtered_df[
                (filtered_df['symbol'] == selected_stock_for_comparison) & 
                (filtered_df[metric].notna())
            ]
            
            if not stock_data.empty:
                fig.add_trace(
                    go.Scatter(
                        x=stock_data['fiscal_year'],
                        y=stock_data[metric],
                        name=f"{metric}",
                        mode='lines+markers',
                        line=dict(width=2)
                    ),
                    row=i, col=1
                )
        
        fig.update_layout(height=400 * n_metrics, showlegend=False)
        fig.update_xaxes(title_text="年份", row=n_metrics, col=1)
        st.plotly_chart(fig, use_container_width=True)
    
    else:  # 多指标多股票
        # 功能3: 多指标多股票对比
        fig = make_subplots(
            rows=len(selected_metrics),
            cols=1,
            subplot_titles=selected_metrics,
            vertical_spacing=0.05
        )
        
        for i, metric in enumerate(selected_metrics, 1):
            if metric not in filtered_df.columns:
                continue
            
            for symbol in selected_symbols:
                symbol_data = filtered_df[
                    (filtered_df['symbol'] == symbol) & 
                    (filtered_df[metric].notna())
                ]
                
                if not symbol_data.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=symbol_data['fiscal_year'],
                            y=symbol_data[metric],
                            name=f"{symbol} - {metric}",
                            mode='lines+markers',
                            legendgroup=symbol
                        ),
                        row=i, col=1
                    )
        
        fig.update_layout(height=400 * len(selected_metrics))
        fig.update_xaxes(title_text="年份", row=len(selected_metrics), col=1)
        st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.markdown("#### 综合分析")
    
    # 选择基准年份（最新年份）
    if 'fiscal_year' in filtered_df.columns:
        latest_year = int(filtered_df['fiscal_year'].max())
        baseline_year = st.selectbox(
            "选择基准年份",
            options=sorted(filtered_df['fiscal_year'].unique(), reverse=True),
            index=0
        )
        
        # 获取基准年份的数据
        baseline_data = filtered_df[filtered_df['fiscal_year'] == baseline_year].copy()
        
        if not baseline_data.empty:
            # 功能6: 改进的雷达图（归一化处理）
            radar_metrics = ['roe', 'netcash_operate_over_net_profit', 'debt_to_asset', 'inventory_turnover','ev_over_ebitda']
            # radar_metrics = ['roe', 'roa', 'profit_margin', 'current_ratio', 'asset_turnover']
            radar_metrics = [m for m in radar_metrics if m in baseline_data.columns]
            
            if len(radar_metrics) >= 3:
                st.markdown("##### 关键指标雷达图（归一化）")
                
                # 功能6: 归一化处理
                normalized_data = baseline_data[radar_metrics].copy()
                for metric in radar_metrics:
                    col_min = normalized_data[metric].min()
                    col_max = normalized_data[metric].max()
                    if col_max != col_min:
                        normalized_data[metric] = (normalized_data[metric] - col_min) / (col_max - col_min) * 100
                    else:
                        normalized_data[metric] = 50  # 如果所有值相同，设为中间值
                
                # 创建雷达图
                fig = go.Figure()
                
                for idx, row in baseline_data.iterrows():
                    values = [normalized_data.loc[idx, m] for m in radar_metrics]
                    fig.add_trace(go.Scatterpolar(
                        r=values,
                        theta=radar_metrics,
                        fill='toself',
                        name=f"{row['symbol']} - {stock_names.get(row['symbol'], row['symbol'])}"
                    ))
                
                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, 100]
                        )
                    ),
                    showlegend=True,
                    title="关键财务指标雷达图（归一化到0-100）",
                    height=500
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            # 功能7: 统计摘要
            st.markdown("##### 📊 统计摘要")
            
            summary_cols = st.columns(len(selected_symbols))
            for i, symbol in enumerate(selected_symbols):
                with summary_cols[i]:
                    st.markdown(f"**{stock_names.get(symbol, symbol)}**")
                    symbol_data = baseline_data[baseline_data['symbol'] == symbol]
                    if not symbol_data.empty:
                        row = symbol_data.iloc[0]
                        # 显示关键指标
                        key_metrics = ['roe', 'netcash_operate_over_net_profit', 'debt_to_asset',
                         'inventory_turnover','ev_over_ebitda']
                        # key_metrics = ['roe', 'roa', 'profit_margin', 'current_ratio']
                        for metric in key_metrics:
                            if metric in row and pd.notna(row[metric]):
                                # 判断是否为百分比指标
                                if metric in ['roe',  'debt_to_asset']:
                                    st.metric(metric.upper(), f"{row[metric]:.2%}")
                                elif metric=="netcash_operate_over_net_profit":
                                    st.metric(metric.upper(), f"{row[metric]:.2f}")
                                elif metric=="ev_over_ebitda":
                                    st.metric(metric.upper(), f"{row[metric]:.2f} x")
                                elif metric=="inventory_turnover":
                                    st.metric(metric.upper(), f"{row[metric]:.1f} 次/年")
                                else:
                                    st.metric(metric.upper(), f"{row[metric]:.2f}")
            
            st.divider()
            
            # 最新年份数据汇总表
            st.markdown("##### 最新年份数据汇总")
            
            display_cols = ['symbol', 'SECURITY_NAME_ABBR', 'fiscal_year'] + selected_metrics
            display_cols = [col for col in display_cols if col in baseline_data.columns]
            
            display_df = baseline_data[display_cols].copy()
            display_df['symbol'] = display_df.apply(
                lambda row: f"{row['symbol']} - {row['SECURITY_NAME_ABBR']}",
                axis=1
            )
            display_df = display_df.drop(columns=['SECURITY_NAME_ABBR'], errors='ignore')
            
            # 功能2: 为汇总表添加颜色映射
            try:
                styled_display = display_df.style
                numeric_cols_in_display = [col for col in display_df.columns 
                                          if col != 'symbol' and col != 'fiscal_year' 
                                          and display_df[col].dtype in ['int64', 'float64']]
                
                for col in numeric_cols_in_display:
                    styled_display = styled_display.background_gradient(
                        subset=[col],
                        cmap='RdYlGn'
                    )
                
                st.dataframe(
                    styled_display,
                    use_container_width=True,
                    height=300
                )
            except:
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=300
                )
            
            # 功能9: 增强的导出功能
            st.markdown("##### 📥 导出数据")
            export_col1, export_col2, export_col3 = st.columns(3)
            
            with export_col1:
                # CSV导出
                csv = display_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 下载CSV",
                    data=csv,
                    file_name=f"fundamental_comparison_{baseline_year}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with export_col2:
                # Excel导出
                try:
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        # 为每个指标创建sheet
                        for metric in selected_metrics:
                            if metric in filtered_df.columns:
                                pivot_data = filtered_df.pivot_table(
                                    index='symbol',
                                    columns='fiscal_year',
                                    values=metric,
                                    aggfunc='first'
                                )
                                pivot_data.index = [
                                    f"{idx} - {stock_names.get(idx, idx)}"
                                    for idx in pivot_data.index
                                ]
                                pivot_data.to_excel(writer, sheet_name=metric[:31])  # Excel sheet名称限制31字符
                        
                        # 添加汇总表
                        display_df.to_excel(writer, sheet_name='汇总', index=False)
                    
                    st.download_button(
                        label="📥 下载Excel",
                        data=excel_buffer.getvalue(),
                        file_name=f"fundamental_analysis_{baseline_year}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                except ImportError:
                    st.info("需要安装openpyxl: pip install openpyxl")
                except Exception as e:
                    st.error(f"Excel导出失败: {e}")
            
            with export_col3:
                # JSON导出
                json_data = baseline_data.to_json(orient='records', date_format='iso', force_ascii=False)
                st.download_button(
                    label="📥 下载JSON",
                    data=json_data,
                    file_name=f"fundamental_analysis_{baseline_year}.json",
                    mime="application/json",
                    use_container_width=True
                )
                
# """
# 页面3: A股基本面分析和比较
# """
# import streamlit as st
# import pandas as pd
# import plotly.graph_objects as go
# import plotly.express as px
# from pathlib import Path

# from config import PROJECT_PATH

# st.set_page_config(page_title="A股基本面分析", layout="wide")

# st.title("📊 A股基本面分析和比较")

# # 数据文件路径
# fundamental_file = Path(PROJECT_PATH) / 'data_ak_fundamental' / 'fundamental_calculated_metrics.csv'

# if not fundamental_file.exists():
#     st.error(f"未找到基本面数据文件: {fundamental_file}")
#     st.stop()

# @st.cache_data
# def load_fundamental_data():
#     """加载基本面数据"""
#     try:
#         df = pd.read_csv(fundamental_file)
#         # 确保fiscal_year是数值类型
#         if 'fiscal_year' in df.columns:
#             df['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce')
#         return df
#     except Exception as e:
#         st.error(f"读取数据文件失败: {e}")
#         return None

# df = load_fundamental_data()

# if df is None or df.empty:
#     st.warning("数据文件为空")
#     st.stop()

# # 侧边栏：股票选择
# st.sidebar.header("选择股票")

# # 获取所有可用的股票
# available_stocks = df[['symbol', 'SECURITY_NAME_ABBR']].drop_duplicates()
# stock_options = {
#     f"{row['symbol']} - {row['SECURITY_NAME_ABBR']}": row['symbol']
#     for _, row in available_stocks.iterrows()
# }

# selected_stock_keys = st.sidebar.multiselect(
#     "选择要对比的股票（最多5只）",
#     options=list(stock_options.keys()),
#     max_selections=5
# )

# selected_symbols = [stock_options[key] for key in selected_stock_keys]

# # 如果没有选择股票，显示提示
# if not selected_symbols:
#     st.info("请在左侧选择要分析的股票（最多5只）")
    
#     # 显示可用股票列表
#     st.markdown("### 可用股票列表")
#     st.dataframe(
#         available_stocks.sort_values('SECURITY_NAME_ABBR'),
#         use_container_width=True,
#         height=400
#     )
#     st.stop()

# # 筛选选中的股票数据
# filtered_df = df[df['symbol'].isin(selected_symbols)].copy()

# # 获取股票名称映射
# stock_names = {
#     row['symbol']: row['SECURITY_NAME_ABBR']
#     for _, row in filtered_df[['symbol', 'SECURITY_NAME_ABBR']].drop_duplicates().iterrows()
# }

# # 财务指标分类
# financial_metrics = {
#     '盈利能力': ['roe', 'roa', 'profit_margin', 'gross_margin', 'operating_margin'],
#     '偿债能力': ['current_ratio', 'quick_ratio', 'cash_ratio', 'debt_to_equity', 'debt_to_asset'],
#     '运营能力': ['asset_turnover', 'inventory_turnover', 'receivables_turnover'],
#     '现金流': ['netcash_operate_over_net_profit', 'free_cash_flow_conversion_rate'],
#     '成长性': ['revenue', 'gross_profit', 'net_profit']
# }

# # 获取所有可用的指标
# available_metrics = [col for col in df.columns 
#                      if col not in ['symbol', 'SECURITY_NAME_ABBR', 'fiscal_year', 'ORG_TYPE']]

# # 主内容区域
# st.markdown("### 📈 财务指标对比")

# # 选择要展示的指标类别
# metric_category = st.selectbox(
#     "选择指标类别",
#     options=list(financial_metrics.keys()) + ['自定义'],
#     index=0
# )

# if metric_category == '自定义':
#     selected_metrics = st.multiselect(
#         "选择要对比的指标",
#         options=available_metrics,
#         default=available_metrics[:5] if len(available_metrics) >= 5 else available_metrics
#     )
# else:
#     selected_metrics = [m for m in financial_metrics[metric_category] if m in available_metrics]

# if not selected_metrics:
#     st.warning("请选择要展示的指标")
#     st.stop()

# # 选择时间范围
# if 'fiscal_year' in filtered_df.columns:
#     min_year = int(filtered_df['fiscal_year'].min())
#     max_year = int(filtered_df['fiscal_year'].max())
    
#     year_range = st.slider(
#         "选择年份范围",
#         min_value=min_year,
#         max_value=max_year,
#         value=(max(min_year, max_year - 5), max_year)
#     )
    
#     filtered_df = filtered_df[
#         (filtered_df['fiscal_year'] >= year_range[0]) &
#         (filtered_df['fiscal_year'] <= year_range[1])
#     ]

# # 创建标签页展示不同类型的分析
# tab1, tab2, tab3 = st.tabs(["📊 指标对比表", "📈 趋势图表", "💹 综合分析"])

# with tab1:
#     st.markdown("#### 财务指标对比表")
    
#     # 创建透视表
#     for metric in selected_metrics:
#         if metric not in filtered_df.columns:
#             continue
        
#         st.markdown(f"##### {metric}")
        
#         # 创建透视表：股票 x 年份
#         pivot_data = filtered_df.pivot_table(
#             index='symbol',
#             columns='fiscal_year',
#             values=metric,
#             aggfunc='first'
#         )
        
#         # 添加股票名称
#         pivot_data.index = [
#             f"{idx} - {stock_names.get(idx, idx)}"
#             for idx in pivot_data.index
#         ]
        
#         st.dataframe(
#             pivot_data,
#             use_container_width=True
#         )
        
#         st.divider()

# with tab2:
#     st.markdown("#### 财务指标趋势图")
    
#     # 为每个指标创建趋势图
#     for metric in selected_metrics:
#         if metric not in filtered_df.columns:
#             continue
        
#         st.markdown(f"##### {metric}")
        
#         # 准备数据
#         plot_df = filtered_df[['symbol', 'fiscal_year', metric]].copy()
#         plot_df = plot_df.dropna(subset=[metric])
        
#         if plot_df.empty:
#             st.warning(f"指标 {metric} 无可用数据")
#             continue
        
#         # 创建图表
#         fig = px.line(
#             plot_df,
#             x='fiscal_year',
#             y=metric,
#             color='symbol',
#             markers=True,
#             labels={
#                 'fiscal_year': '年份',
#                 metric: metric,
#                 'symbol': '股票代码'
#             },
#             title=f"{metric} 趋势对比"
#         )
        
#         # 更新布局
#         fig.update_layout(
#             hovermode='x unified',
#             height=400,
#             template='plotly_white'
#         )
        
#         st.plotly_chart(fig, use_container_width=True)
        
#         st.divider()

# with tab3:
#     st.markdown("#### 综合分析")
    
#     # 选择基准年份（最新年份）
#     if 'fiscal_year' in filtered_df.columns:
#         latest_year = int(filtered_df['fiscal_year'].max())
#         baseline_year = st.selectbox(
#             "选择基准年份",
#             options=sorted(filtered_df['fiscal_year'].unique(), reverse=True),
#             index=0
#         )
        
#         # 获取基准年份的数据
#         baseline_data = filtered_df[filtered_df['fiscal_year'] == baseline_year].copy()
        
#         if not baseline_data.empty:
#             # 雷达图：选择关键指标
#             radar_metrics = ['roe', 'roa', 'profit_margin', 'current_ratio', 'asset_turnover']
#             radar_metrics = [m for m in radar_metrics if m in baseline_data.columns]
            
#             if len(radar_metrics) >= 3:
#                 st.markdown("##### 关键指标雷达图")
                
#                 # 准备雷达图数据
#                 radar_data = []
#                 for _, row in baseline_data.iterrows():
#                     values = []
#                     for metric in radar_metrics:
#                         val = row[metric]
#                         if pd.isna(val):
#                             val = 0
#                         values.append(float(val))
                    
#                     radar_data.append({
#                         'symbol': f"{row['symbol']} - {stock_names.get(row['symbol'], row['symbol'])}",
#                         'values': values
#                     })
                
#                 # 创建雷达图
#                 fig = go.Figure()
                
#                 for data_all_list in radar_data:
#                     fig.add_trace(go.Scatterpolar(
#                         r=data_all_list['values'],
#                         theta=radar_metrics,
#                         fill='toself',
#                         name=data_all_list['symbol']
#                     ))
                
#                 fig.update_layout(
#                     polar=dict(
#                         radialaxis=dict(
#                             visible=True,
#                             range=[0, max([max(d['values']) for d in radar_data]) * 1.2]
#                         )),
#                     showlegend=True,
#                     title="关键财务指标雷达图",
#                     height=500
#                 )
                
#                 st.plotly_chart(fig, use_container_width=True)
            
#             # 最新年份数据汇总表
#             st.markdown("##### 最新年份数据汇总")
            
#             display_cols = ['symbol', 'SECURITY_NAME_ABBR', 'fiscal_year'] + selected_metrics
#             display_cols = [col for col in display_cols if col in baseline_data.columns]
            
#             display_df = baseline_data[display_cols].copy()
#             display_df['symbol'] = display_df.apply(
#                 lambda row: f"{row['symbol']} - {row['SECURITY_NAME_ABBR']}",
#                 axis=1
#             )
#             display_df = display_df.drop(columns=['SECURITY_NAME_ABBR'], errors='ignore')
            
#             st.dataframe(
#                 display_df,
#                 use_container_width=True,
#                 height=300
#             )
            
#             # 下载按钮
#             csv = display_df.to_csv(index=False).encode('utf-8-sig')
#             st.download_button(
#                 label="📥 下载对比数据 (CSV)",
#                 data_all_list=csv,
#                 file_name=f"fundamental_comparison_{baseline_year}.csv",
#                 mime="text/csv"
#             )
