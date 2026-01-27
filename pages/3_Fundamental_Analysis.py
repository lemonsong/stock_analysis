"""
页面3: A股基本面分析和比较
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pathlib import Path
from utils.streamlit_helper import setup_page_config
from utils.constants import FUNDAMENTAL_KEY_COLS, SEQUENTIAL_COLOR, PROJECT_PATH
import io, os
import sys
# 添加根目录到sys.path以便导入utils
# sys.path.append(str(Path(__file__).parent.parent))

setup_page_config()

st.title("📊 A股基本面分析和比较")

# 数据文件路径
fundamental_file = Path(PROJECT_PATH) / 'data_ak_fundamental' / 'fundamental_calculated_metrics.csv'
industry_file = Path(PROJECT_PATH) / 'data_app' / 'app_decision.csv'

if not fundamental_file.exists():
    st.error(f"未找到基本面数据文件: {fundamental_file}")
    st.stop()

@st.cache_data
def load_fundamental_data():
    """加载基本面数据，并合并行业信息"""
    try:
        df = pd.read_csv(fundamental_file)
        # 确保fiscal_year是数值类型
        if 'fiscal_year' in df.columns:
            df['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce')

        if industry_file.exists():
            try:
                df_ind = pd.read_csv(industry_file)
                df['symbol'] = df['symbol'].astype(str)
                df_ind['symbol'] = df_ind['symbol'].astype(str)
                df = pd.merge(df, df_ind[['symbol', 'industry_category_name', 'industry_type_name']], on='symbol', how='left')
                df.rename(columns={'industry_type_name': 'industry'}, inplace=True)
            except Exception as e:
                st.warning(f"加载行业数据失败: {e}")

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

def load_kline_data(symbol):
    """加载单只股票的日K线数据"""
    file_path = Path(PROJECT_PATH) / 'data_tushare' / 'daily' / f'{symbol}.csv'
    if file_path.exists():
        try:
            df = pd.read_csv(file_path)
            # 处理日期列名
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception:
            return None
    return None

def setup_sidebar(df):
    """设置侧边栏股票选择逻辑"""
    st.sidebar.header("选择股票")

    selection_method = st.sidebar.radio(
        "选择方式",
        ["📝 列表输入", "🏭 行业筛选", "🔍 搜索股票", ],
        index=0,
        key="selection_method"
    )

    selected_symbols = []

    available_stocks = get_stock_list(df)
    stock_map = {
        row['symbol']: f"{row['symbol']} - {row['SECURITY_NAME_ABBR']}"
        for _, row in available_stocks.iterrows()
    }


    if selection_method == "📝 列表输入":
        st.sidebar.caption("输入股票代码，用逗号分隔")
        input_text = st.sidebar.text_area("股票代码列表", "SZ000629,SH600295,SZ000672,SH600808,SH603612,SZ000959,SH601702,SH603399,SZ002787,SH603688,SH603995,SH600801,SH600293,SZ002080", key="list_input")
        if input_text:
            symbols = [s.strip() for s in input_text.replace('，', ',').split(',') if s.strip()]
            valid_symbols = [s for s in symbols if s in stock_map]
            invalid_symbols = [s for s in symbols if s not in stock_map]

            if invalid_symbols:
                st.sidebar.warning(f"未找到代码: {', '.join(invalid_symbols)}")

            selected_symbols = valid_symbols

    elif selection_method == "🏭 行业筛选":
        if 'industry' not in df.columns:
            st.sidebar.error("未找到行业数据")
        else:
            industries = sorted(df['industry'].dropna().unique())
            selected_industry = st.sidebar.selectbox("选择行业", industries, key="industry_select")

            if selected_industry:
                industry_stocks = df[df['industry'] == selected_industry]['symbol'].unique()

                st.sidebar.markdown("##### 财务指标筛选 (最新年份)")

                filter_metrics = {
                    'roe': 'ROE (%)',
                    'netcash_operate_over_net_profit': '净现比',
                    'debt_to_asset': '资产负债率',
                    'inventory_turnover': '存货周转率',
                    'ev_over_ebitda': 'EV/EBITDA'
                }

                latest_year = df['fiscal_year'].max()
                latest_df = df[(df['fiscal_year'] == latest_year) & (df['symbol'].isin(industry_stocks))]

                filtered_industry_stocks = latest_df.copy()

                with st.sidebar.expander("指标筛选条件", expanded=True):
                    for metric, label in filter_metrics.items():
                        if metric not in latest_df.columns:
                            continue

                        min_val = float(latest_df[metric].min())
                        max_val = float(latest_df[metric].max())

                        if min_val == max_val:
                            continue

                        val_range = st.slider(
                            f"{label}",
                            min_value=min_val,
                            max_value=max_val,
                            value=(min_val, max_val),
                            step=(max_val-min_val)/100 if max_val != min_val else 1.0,
                            key=f"slider_{metric}"
                        )

                        filtered_industry_stocks = filtered_industry_stocks[
                            (filtered_industry_stocks[metric] >= val_range[0]) &
                            (filtered_industry_stocks[metric] <= val_range[1])
                        ]

                selected_symbols = filtered_industry_stocks['symbol'].tolist()
                st.sidebar.success(f"筛选出 {len(selected_symbols)} 只股票")

                if len(selected_symbols) > 0:
                     with st.sidebar.expander("查看筛选结果"):
                         st.table(filtered_industry_stocks[['symbol', 'SECURITY_NAME_ABBR']])
    elif selection_method == "🔍 搜索股票":
        search_term = st.sidebar.text_input("搜索股票代码或名称", "", key="search_term")

        filtered_stocks = available_stocks
        if search_term:
            filtered_stocks = available_stocks[
                (available_stocks['symbol'].str.contains(search_term, case=False, na=False)) |
                (available_stocks['SECURITY_NAME_ABBR'].str.contains(search_term, case=False, na=False))
                ]

        stock_options = {
            f"{row['symbol']} - {row['SECURITY_NAME_ABBR']}": row['symbol']
            for _, row in filtered_stocks.iterrows()
        }

        default_selection = st.session_state.get('selected_stocks_search', [])
        default_selection = [s for s in default_selection if s in stock_options.keys()]

        selected_keys = st.sidebar.multiselect(
            "选择股票（多选）",
            options=list(stock_options.keys()),
            default=default_selection,
            key="search_multiselect"
        )
        st.session_state['selected_stocks_search'] = selected_keys
        selected_symbols = [stock_options[k] for k in selected_keys]

    return selected_symbols, stock_map

def render_indicator_help():
    """渲染财务指标帮助信息"""
    with st.expander("📘 财务指标深度解读手册 (点击展开/折叠)", expanded=False):
        # 创建六个标签页对应六大类指标
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "流动性", "杠杆", "效率", "盈利能力", "现金流和估值指标", "综合"
        ])

        with tab1:
            st.subheader("一、 流动性")
            st.markdown("""
            - **Current Ratio (流动比率)**：流动资产/流动负债。衡量短期还债能力。一般 >2 为健壮。  
              ⚠️ **警惕**：比例过高可能意味着资金闲置或存货积压。
            - **Quick Ratio (速动比率)**：(流动资产-存货)/流动负债。剔除变现慢的存货。一般 >1 为安全。  
              ⚠️ **警惕**：若远低于流动比率，说明企业极度依赖卖货来还债。
            - **Cash Ratio (现金比率)**：现金及等价物/流动负债。最严苛的变现能力指标。  
              ⚠️ **警惕**：过低代表一旦银行断贷，企业立即面临违约。
            """)

        with tab2:
            st.subheader("二、 杠杆")
            st.markdown("""
            - **Total Debt (总负债)**：公司承担的所有债务总额。  
              ⚠️ **警惕**：绝对值增长速度若超过利润增长，需关注利息压力。
            - **Net Debt (净负债)**：总负债 - 现金。反映真实的债务负担。负值代表“现金多于债务”。  
              ⚠️ **警惕**：净负债剧增通常预示着大规模扩张或失血。
            - **Debt to Equity (产权比率)**：总负债/股东权益。衡量财务杠杆。  
              ⚠️ **警惕**：>1 意味着债权人出的钱比股东多，风险较高。
            - **Debt to Asset (资产负债率)**：总负债/总资产。衡量总资产中借钱的占比。  
              ⚠️ **警惕**：超过 60%-70%（非金融行业）需警惕破产风险或商誉减值冲击。
            - **Interest Coverage (利息保障倍数)**：息税前利润/利息支出。衡量赚的钱够付几次利息。建议 >3。  
              ⚠️ **警惕**：<1 说明赚的钱连利息都不够付，极度危险。
            """)

        with tab3:
            st.subheader("三、 效率")
            st.markdown("""
            - **Revenue (营业收入)**：生意规模。  
              ⚠️ **警惕**：营收停滞但利润上升，往往是靠“省钱”而非“增长”。
            - **Gross Profit (毛利)**：营收 - 直接成本。反映产品竞争力。  
              ⚠️ **警惕**：连续多年下降说明行业进入惨烈价格战。
            - **Net Profit (净利润)**：最终到手的钱。  
              ⚠️ **警惕**：需结合非经常性损益看，谨防变卖资产凑出来的“虚假盈利”。
            - **Asset Turnover (资产周转率)**：营收/总资产。衡量利用资产赚钱的效率。  
              ⚠️ **警惕**：逐年下降说明公司资产变得“沉重”，效率降低。
            - **Inventory Turnover (存货周转率)**：营收/存货。货卖得快不快。  
              ⚠️ **警惕**：骤降代表产品可能过时或滞销，面临计提损失。
            - **Receivables Turnover (应收账款周转率)**：营收/应收账款。钱收得回不回。  
              ⚠️ **警惕**：持续下降说明公司对下游话语权丧失，或在虚增收入。
            """)

        with tab4:
            st.subheader("四、 盈利能力")
            st.markdown("""
            - **Gross Margin (毛利率)**：毛利/营收。产品溢价能力。  
              ⚠️ **警惕**：突降通常意味着原材料暴涨或竞争对手降价。
            - **Operating Margin (营业利润率)**：营业利润/营收。反映管理和销售效率。  
              ⚠️ **警惕**：与毛利率背离说明内部开支（如研发、管理）失控。
            - **Profit Margin (净利率)**：净利润/营收。  
              ⚠️ **警惕**：极低的净利率意味着企业容错率极低，任何成本波动都能导致亏损。
            - **ROE (净资产收益率)**：股东出的一块钱能赚多少钱。核心指标。  
              ⚠️ **警惕**：靠高负债强行推高的 ROE 极具风险。
            - **ROA (总资产收益率)**：衡量所有资产（含借的钱）的赚钱效率。  
              ⚠️ **警惕**：若 ROA 远低于 ROE，说明杠杆加得非常大。
            """)

        with tab5:
            st.subheader("五、 现金流和估值指标")
            st.markdown("""
            - **Netcash Operate over Net Profit (净现比)**：经营现金流净额/净利润。利润含金量。  
              ⚠️ **警惕**：长期 <1 说明利润多是“纸上富贵”，没有真现金流入。
            - **Free Cash Flow Conversion (自由现金流转化率)**：FCF / 净利润。  
              ⚠️ **警惕**：转化率低甚至为负，说明公司是个“吞金兽”，赚的钱全投回设备更新了。
            - **Change in Working Capital (营运资本变动)**：经营中压进去的钱。  
              ⚠️ **警惕**：正值过大代表现金被应收和存货占满，现金流会枯竭。
            - **Net Debt over EBITDA (净债务/EBITDA)**：衡量还清债务需要多少年经营利润。  
              ⚠️ **警惕**：>3 通常被银行视为高风险。
            - **EV over EBITDA (企业价值倍数)**：收购成本/经营现金流能力。比 PE 更稳健的估值。  
              ⚠️ **警惕**：需与行业均值对比，过高代表溢价过大。
            """)
        with tab6:
            st.subheader("六、 综合")
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

4. 存货/应收账款周转率 (Inventory Turnover) —— 【营运效率】
地位：它是企业的“代谢能力”。

理由：反映了资产变现的速度。货卖得快不快？钱收回得顺不顺？在你的数据集中，这两个指标可以合并看作运营能力的代表。

看板标准：逐年递增或保持稳定。如果周转率突然大幅下降，通常预示着产品竞争力下滑或下游客户违约风险增加。

5. 企业价值倍数 (EV/EBITDA) —— 【估值定价】
地位：它是专业的“买入标尺”。

理由：相比 PE（市盈率），它剔除了非经常性损益、利息和税收的干扰，更公平地反映了企业核心业务的估值。它是机构投资者判断“买得值不值”的核心工具。

看板标准：寻找“低估值 + 高ROE”的交叉点。
            """)

def render_comprehensive_tab(df, selected_symbols, stock_names):
    """渲染综合分析 Tab"""
    st.markdown("#### 综合分析")

    if 'fiscal_year' in df.columns:
        latest_year = int(df['fiscal_year'].max())
        baseline_year = st.selectbox(
            "选择基准年份",
            options=sorted(df['fiscal_year'].unique(), reverse=True),
            index=0,
            key="baseline_year_select"
        )
        # 获取基准年份的数据
        baseline_data = df[df['fiscal_year'] == baseline_year].copy()
        
        if not baseline_data.empty:
            # 1. 雷达图
            radar_metrics = ['roe', 'netcash_operate_over_net_profit', 'debt_to_asset', 'inventory_turnover','ev_over_ebitda']
            radar_metrics = [m for m in radar_metrics if m in baseline_data.columns]
            
            if len(radar_metrics) >= 3:
                st.markdown("##### 关键指标雷达图（归一化）")
                normalized_data = baseline_data[radar_metrics].copy()
                for metric in radar_metrics:
                    col_min = normalized_data[metric].min()
                    col_max = normalized_data[metric].max()
                    if col_max != col_min:
                        normalized_data[metric] = (normalized_data[metric] - col_min) / (col_max - col_min) * 100
                    else:
                        normalized_data[metric] = 50

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
                    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                    showlegend=True,
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)

            # 2. 统计摘要
            st.markdown("##### 📊 统计摘要")
            summary_cols = st.columns(min(len(selected_symbols), 4))
            for i, symbol in enumerate(selected_symbols):
                col_idx = i % 4
                with summary_cols[col_idx]:
                    with st.container(border=True):
                        st.markdown(f"**{symbol} - {stock_names.get(symbol, symbol)}**")
                        symbol_data = baseline_data[baseline_data['symbol'] == symbol]
                        if not symbol_data.empty:
                            row = symbol_data.iloc[0]
                            key_metrics = ['fundamental_rank', 'fundamental_score', 'roe',
                                           'netcash_operate_over_net_profit', 'debt_to_asset', 'inventory_turnover',
                                           'ev_over_ebitda']
                            for metric in key_metrics:
                                if metric in row and pd.notna(row[metric]):
                                    val = row[metric]
                                    if metric == 'fundamental_rank':
                                        st.metric("综合排名", f"#{int(val)}")
                                    elif metric == 'fundamental_score':
                                        st.metric("基本面得分", f"{val:.2f}")
                                    elif metric in ['roe', 'debt_to_asset']:
                                        st.metric(metric.upper(), f"{val:.2%}")
                                    elif metric == "netcash_operate_over_net_profit":
                                        st.metric(metric.upper(), f"{val:.2f}")
                                    elif metric == "ev_over_ebitda":
                                        st.metric(metric.upper(), f"{val:.2f} x")
                                    elif metric == "inventory_turnover":
                                        st.metric(metric.upper(), f"{val:.1f} 次/年")
                                    else:
                                        st.metric(metric.upper(), f"{val:.2f}")
            st.divider()

            # 3. 数据汇总表
            st.markdown("##### 最新年份数据汇总")
            display_cols = ['display_name', 'fiscal_year', 'fundamental_rank', 'fundamental_score'] + radar_metrics
            display_cols = [col for col in display_cols if col in baseline_data.columns]
            display_df = baseline_data[display_cols].copy()
            display_df.rename(columns={'display_name': 'symbol'}, inplace=True)

            try:
                st.dataframe(display_df.style.background_gradient(cmap=SEQUENTIAL_COLOR), use_container_width=True)
            except:
                st.dataframe(display_df, use_container_width=True)
            
            # 4. 日K线展示
            st.markdown("##### 📈 股价走势 (日K线)")
            kline_cols = st.columns(min(len(selected_symbols), 2))
            for i, symbol in enumerate(selected_symbols):
                col_idx = i % 2
                with kline_cols[col_idx]:
                    kline_df = load_kline_data(symbol)
                    if kline_df is not None and not kline_df.empty:
                        st.caption(f"{stock_names.get(symbol, symbol)} - 日K线")
                        if 'date' in kline_df.columns:
                            # 确保按日期排序
                            kline_df = kline_df.sort_values('date')
                            if 'open' in kline_df.columns and 'close' in kline_df.columns and 'high' in kline_df.columns and 'low' in kline_df.columns:
                                fig_k = go.Figure(data=[go.Candlestick(
                                    x=kline_df['date'],
                                    open=kline_df['open'],
                                    high=kline_df['high'],
                                    low=kline_df['low'],
                                    close=kline_df['close']
                                )])
                            else:
                                fig_k = px.line(kline_df, x='date', y='close')

                            fig_k.update_layout(height=300, margin=dict(l=0, r=0, t=20, b=0))
                            st.plotly_chart(fig_k, use_container_width=True)
                        else:
                            st.warning("数据缺少日期列，无法绘图")
                    else:
                        st.caption(f"{stock_names.get(symbol, symbol)} - 暂无K线数据")

def render_trends_tab(df, selected_symbols, selected_metrics, stock_names):
    """渲染趋势与指标 Tab"""
    st.markdown("#### 财务指标趋势与对比")

    compare_mode = st.radio(
        "对比模式",
        options=["单指标多股票", "多指标单股票"],
        horizontal=True,
        key="compare_mode_select"
    )
    if compare_mode == "单指标多股票":
        for metric in selected_metrics:
            if metric not in df.columns: continue

            st.markdown(f"##### {metric}")
            
            # 1. 趋势图
            plot_df = df[['display_name', 'fiscal_year', metric]].dropna(subset=[metric])
            if not plot_df.empty:
                fig = px.line(
                    plot_df,
                    x='fiscal_year',
                    y=metric,
                    color='display_name',
                    markers=True,
                    labels={'fiscal_year': '年份', metric: metric, 'display_name': '股票'},
                    title=f"{metric} 趋势"
                )
                st.plotly_chart(fig, use_container_width=True)

            # 2. 对比表 (紧接在图表下方)
            pivot_data = df.pivot_table(index='display_name', columns='fiscal_year', values=metric, aggfunc='first')
            try:
                st.dataframe(pivot_data.style.background_gradient(cmap='RdYlGn', axis=1), use_container_width=True)
            except:
                st.dataframe(pivot_data, use_container_width=True)

            st.divider()

    elif compare_mode == "多指标单股票":
        selected_stock = st.selectbox(
            "选择要分析的股票",
            options=selected_symbols,
            format_func=lambda x: f"{x} - {stock_names.get(x, x)}"
        )

        # 图表
        n_metrics = len(selected_metrics)
        fig = make_subplots(rows=n_metrics, cols=1, subplot_titles=selected_metrics, vertical_spacing=0.05)

        for i, metric in enumerate(selected_metrics, 1):
            if metric not in df.columns: continue
            stock_data = df[(df['symbol'] == selected_stock) & (df[metric].notna())]
            if not stock_data.empty:
                fig.add_trace(
                    go.Scatter(x=stock_data['fiscal_year'], y=stock_data[metric], name=metric, mode='lines+markers'),
                    row=i, col=1
                )
        fig.update_layout(height=300 * n_metrics, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        # 表格 (显示该股票的所有选中指标)
        st.markdown(f"###### {stock_names.get(selected_stock, selected_stock)} - 指标详情")
        stock_data_all = df[df['symbol'] == selected_stock]
        display_cols = ['fiscal_year'] + selected_metrics
        st.dataframe(stock_data_all[display_cols], use_container_width=True)


# --- 主程序 ---

df = load_fundamental_data()

if df is None or df.empty:
    st.warning("数据文件为空")
    st.stop()

# 侧边栏设置
selected_symbols, stock_map = setup_sidebar(df)

# 数据质量检查
st.sidebar.markdown("#### 📊 数据质量")
if 'fiscal_year' in df.columns:
    data_coverage = df.groupby('symbol')['fiscal_year'].count()
    st.sidebar.metric("平均数据年份数", f"{data_coverage.mean():.1f}")

# 指标介绍
render_indicator_help()

# 检查是否选择了股票
if not selected_symbols:
    st.info("请在侧边栏选择股票")
    st.stop()

# 筛选数据
filtered_df = df[df['symbol'].isin(selected_symbols)].copy()
filtered_df['display_name'] = filtered_df['symbol'] + ' - ' + filtered_df['SECURITY_NAME_ABBR']
stock_names = {row['symbol']: row['SECURITY_NAME_ABBR'] for _, row in filtered_df[['symbol', 'SECURITY_NAME_ABBR']].drop_duplicates().iterrows()}

# 指标选择
financial_metrics = {
    'Liquidity Ratios（流动性比率）': ['current_ratio', 'quick_ratio', 'cash_ratio'],
    'Leverage Ratios（杠杆比率）': ['total_debt', 'net_debt', 'debt_to_equity', 'debt_to_asset', 'interest_coverage'],
    'Efficiency Ratios（效率比率）': ['revenue', 'gross_profit', 'net_profit', 'asset_turnover', 'inventory_turnover', 'receivables_turnover'],
    'Profitability Ratios（盈利能力比率）': ['gross_margin', 'operating_margin', 'profit_margin', 'roe', 'roa'],
    'Cash Flow & Valuation Metrics（现金流和估值指标）': ['netcash_operate_over_net_profit', 'free_cash_flow_conversion_rate', 'change_in_working_capital', 'net_debt_over_ebitda', 'ev_over_ebitda']
}
available_metrics = [col for col in df.columns if col not in ['symbol', 'SECURITY_NAME_ABBR', 'fiscal_year', 'ORG_TYPE', 'industry']]

st.markdown("### 📈 财务指标对比")
metric_category = st.selectbox("选择指标类别", options=list(financial_metrics.keys()) + ['自定义'], index=0, key="metric_category_select")
if metric_category == '自定义':
    selected_metrics = st.multiselect("选择指标", options=available_metrics, default=FUNDAMENTAL_KEY_COLS, key="metric_multiselect")
else:
    selected_metrics = [m for m in financial_metrics[metric_category] if m in available_metrics]

if not selected_metrics:
    st.stop()

# 时间范围
if 'fiscal_year' in filtered_df.columns:
    min_year = int(filtered_df['fiscal_year'].min())
    max_year = int(filtered_df['fiscal_year'].max())
    year_range = st.slider("选择年份范围", min_value=min_year, max_value=max_year, value=(max(min_year, max_year - 5), max_year), key="year_range_slider")
    filtered_df = filter_stock_data(df, selected_symbols, year_range).copy()
    filtered_df['display_name'] = filtered_df['symbol'] + ' - ' + filtered_df['SECURITY_NAME_ABBR']

# 新的 Tab 布局
tab1, tab2 = st.tabs(["💹 综合分析", "📊 财务指标趋势 & 对比"])

with tab1:
    render_comprehensive_tab(filtered_df, selected_symbols, stock_names)

with tab2:
    render_trends_tab(filtered_df, selected_symbols, selected_metrics, stock_names)
