"""
页面3.2: 财务基本面分析
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pathlib import Path
from utils.streamlit_helper import clear_cache, clean_expired_cache, setup_page_config, render_filter_sidebar
from utils.constants import FUNDAMENTAL_KEY_COLS, SEQUENTIAL_COLOR, DISCRETE_COLOR, PROJECT_PATH
import io, os
import sys

setup_page_config()

st.title("📊 A股财务分析和比较")

# 数据文件路径
fundamental_file = Path(PROJECT_PATH) / 'data/ak_financial' / 'financial_calculated_metrics.csv'
industry_file = Path(PROJECT_PATH) / 'data/dwa' / 'app_decision.csv'

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
                if 'SECURITY_NAME_ABBR' in df.columns:
                    df = df.drop('SECURITY_NAME_ABBR', axis=1)
                df_ind['symbol'] = df_ind['symbol'].astype(str)
                df = pd.merge(df, df_ind[
                    ['symbol', 'company', 'industry_category_name', 'industry_sub_category_name', 'industry_type_name']],
                              on='symbol', how='left')
                df['industry'] = df['industry_type_name']  # For backward compatibility
            except Exception as e:
                st.warning(f"加载行业数据失败: {e}")

        return df
    except Exception as e:
        st.error(f"读取数据文件失败: {e}")
        return None


@st.cache_data
def filter_stock_data(_df, symbols, year_range):
    """筛选股票数据（缓存）"""
    return _df[
        (_df['symbol'].isin(symbols)) &
        (_df['fiscal_year'] >= year_range[0]) &
        (_df['fiscal_year'] <= year_range[1])
        ]


def render_indicator_help():
    """渲染财务指标帮助信息"""
    with st.expander("📘 财务指标深度解读手册 (点击展开/折叠)", expanded=False):
        # 创建六个标签页对应六大类指标
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "流动性", "杠杆", "效率", "盈利能力", "现金流与估值", "综合"
        ])

        with tab1:
            st.subheader("一、 流动性")
            st.markdown("""
            - **Current Ratio (流动比率)**：流动资产/流动负债。衡量短期还债能力。一般 **>2** 为健壮。  
              - 🏭 **非金融**：过高可能意味着资金闲置。
              - 🏦 **金融业**：该指标参考意义有限，银行更关注**流动性覆盖率 (LCR)**。
            - **Quick Ratio (速动比率)**：(流动资产-存货)/流动负债。剔除变现慢的存货。一般 **>1** 为安全。  
              - ⚠️ 若远低于流动比率，说明企业极度依赖卖货来还债。
            - **Cash Ratio (现金比率)**：现金及等价物/流动负债。最严苛的变现能力指标。  
              - ⚠️ 过低代表一旦银行断贷，企业立即面临违约。
            """)

        with tab2:
            st.subheader("二、 杠杆")
            st.markdown("""
            - **Total Debt (总负债)**：公司承担的所有债务总额。  
              - ⚠️ 绝对值增长速度若超过利润增长，需关注利息压力。
            - **Net Debt (净负债)**：总负债 - 现金。反映真实的债务负担。负值代表“现金多于债务”。  
              - ⚠️ 净负债剧增通常预示着大规模扩张或失血。
            - **Debt to Equity (产权比率)**：总负债/股东权益。衡量财务杠杆。  
              - ⚠️ >1 意味着债权人出的钱比股东多，风险较高。
            - **Debt to Asset (资产负债率)**：总负债/总资产。衡量总资产中借钱的占比。 
              - 🏭 **非金融**：建议 **<60%**。超过 70% 需警惕资金链风险。
              - 🏦 **金融业**：使用了核心一级资本充足率 (Core Tier-1 Capital Adequacy Ratio)，核心一级资本（如普通股、盈余公积）与风险加权资产（RWA）的比率。监管要求通常 **>7.5%-8.5%**。它反映了当发生极端坏账时，银行自身的“本金”是否足以吸收损失而不至于倒闭。
              ~~- 🏦 **金融业**：使用了TOTAL_LIABILITIES/TOTAL_ASSETS。通常在 **90% 左右**。此时应关注 **核心一级资本充足率**（监管要求通常 **>7.5%-8.5%**）。~~
            - **Interest Coverage (利息保障倍数)**：息税前利润/利息支出。衡量赚的钱够付几次利息。建议 >3。  
              - ⚠️ <1 说明赚的钱连利息都不够付，极度危险。
            """)

        with tab3:
            st.subheader("三、 效率")
            st.markdown("""
            - **Revenue (营业收入)**：生意规模。  
              - ⚠️ 营收停滞但利润上升，往往是靠“省钱”而非“增长”。
              - 🏦 **金融业**：替换为 OPERATE_INCOME
            - **Gross Profit (毛利)**：营收 - 直接成本。反映产品竞争力。  
              - ⚠️ 连续多年下降说明行业进入惨烈价格战。
              - 🏦 **金融业**：替换为 OPERATE_INCOME
            - **Net Profit (净利润)**：最终到手的钱。  
              - ⚠️ 需结合非经常性损益看，谨防变卖资产凑出来的“虚假盈利”。
            - **Asset Turnover (资产周转率)**：营收/总资产。衡量利用资产赚钱的效率。  
              - ⚠️ 逐年下降说明公司资产变得“沉重”，效率降低。
              - 🏦 **金融业**：由于银行总资产极大，该值通常远低于工业企业（常低于 0.1）。
            - **Inventory Turnover (存货周转率)**：营收/存货。货卖得快不快。  
              - 🏭 **非金融**：越高越好，骤降代表产品可能过时或滞销，面临计提损失。
              - 🏦 **金融业**：替换为 **存贷比 (Loan-to-Deposit Ratio)**。
                - 💡 **阈值**：通常在 **70%-80%** 之间。过高面临流动性压力，过低说明资金利用率不足。
            - **Receivables Turnover (应收账款周转率)**：营收/应收账款。钱收得回不回。  
              - ⚠️ 持续下降说明公司对下游话语权丧失，或在虚增收入。
            """)

        with tab4:
            st.subheader("四、 盈利能力")
            st.markdown("""
            - **Gross Margin (毛利率)**：毛利/营收。产品溢价能力。  
              - ⚠️ 突降通常意味着原材料暴涨或竞争对手降价。
              - 🏦 **金融业 (适配)**：关注 **净利差 (NIM)**。
                - 💡 **阈值**：优秀银行 NIM 通常维持在 **2% 以上**。
            - **Operating Margin (营业利润率)**：营业利润/营收。反映管理和销售效率。  
              - ⚠️ 与毛利率背离说明内部开支（如研发、管理）失控。
            - **Profit Margin (净利率)**：净利润/营收。  
              - ⚠️ 极低的净利率意味着企业容错率极低，任何成本波动都能导致亏损。
            - **ROE (净资产收益率)**：股东出的一块钱能赚多少钱。核心指标。  
              - ⚠️ 靠高负债强行推高的 ROE 极具风险。
              - 💡 **阈值**：持续 **>15%** 是优秀企业的象征。
              - 🏦 **金融业**：需配合 **ROA (总资产收益率)** 观察。若 ROE 很高但 ROA **<0.8%**，说明是靠极高杠杆驱动，风险较大。
            - **ROA (总资产收益率)**：衡量所有资产（含借的钱）的赚钱效率。  
              - ⚠️ 若 ROA 远低于 ROE，说明杠杆加得非常大 (ROE = ROA * 权益乘数)。
              - 银行是典型的高杠杆生意。如果一家银行的 ROE 很高（如 >15%） 但 ROA 极低（如 <0.8%），说明它是在通过极高的杠杆倍数来强行推高回报。这种盈利模式在经济波动期极具风险。
            """)

        with tab5:
            st.subheader("五、 现金流和估值指标")
            st.markdown("""
            - **Netcash Operate over Net Profit (净现比)**：经营现金流净额/净利润。利润含金量。  
              - 🏭 **非金融**：建议 **≥ 1.0**。长期 <1 说明利润多是“纸上富贵”，没有真现金流入。长期 <0.8 说明利润“含金量”低。
              - 🏦 **金融业**：这里实际值为**PPOP (拨备前利润) 增长率**。反映剔除坏账计提操纵后的真实增长。
            - **Free Cash Flow Conversion (自由现金流转化率)**：FCF / 净利润。  
              - ⚠️ 转化率低甚至为负，说明公司是个“吞金兽”，赚的钱全投回设备更新了。
            - **Change in Working Capital (营运资本变动)**：经营中压进去的钱。  
              - ⚠️ 正值过大代表现金被应收和存货占满，现金流会枯竭。
            - **Net Debt over EBITDA (净债务/EBITDA)**：衡量还清债务需要多少年经营利润。  
              - ⚠️ >3 通常被银行视为高风险。
            - **EV over EBITDA (企业价值倍数)**：收购成本/经营现金流能力。比 PE 更稳健的估值。  
              - 🏭 **非金融**：比 PE 更稳健。需与行业均值对比，过高代表溢价过大。一般 **<10-12** 可能存在低估。
              - 🏦 **金融业**：这里实际值为 **P/B (市净率)**。
                - 💡 **阈值**：**P/B < 1** (破净) 通常代表市场对资产质量有疑虑；**P/B > 1.5** 往往代表高溢价。
            """)
        with tab6:
            st.subheader("六、 综合")
            st.table(pd.DataFrame({
                "维度": ["盈利能力 (ROE)", "收益质量 (净现比)", "杠杆风险", "周转效率", "估值水平"],
                "非金融企业": ["ROE (净资产收益率)", "经营现金流 / 净利润", "有息负债 / 总资产", "存货周转率", "EV / EBITDA"],
                "金融机构": ["ROE (净资产收益率)", "拨备前利润 (PPOP) 增长", "总负债 / 总资产", "存贷比 (Loan-to-Deposit)", 'P/B (市净率)']
            }))
            st.markdown("""
**1️⃣ ROE (净资产收益率) —— 【核心回报】**
地位：它是财务分析的“定海神针”。

- 理由：ROE 综合了净利率、资产周转率和财务杠杆（杜邦分析法）。它直接告诉你股东投入的每一块钱净资产产生了多少回报。如果只能看一个指标，那就是 ROE。

- 💡 看板标准：持续多年稳定在 15% 以上 的通常是长跑冠军。

- 🏦 **金融业**也使用ROE，需配合 **ROA (总资产收益率)** 观察。若 ROE 很高但 ROA **<0.8%**，说明是靠极高杠杆驱动，风险较大。

**2️⃣ 净现比 (Netcash/Net Profit) —— 【获利质量】**
地位：它是利润的“防伪标签”。

- 理由：纸面利润可以通过会计手段调节，但现金流很难伪造。这个指标衡量赚到的 1 块钱利润里，真正落袋为安的现金有多少。

- 💡 看板标准：理想值应 ≥ 1.0。如果长期低于 0.8，说明公司可能存在大量收不回来的欠款（应收账款）或卖不掉的库存。

- 🏦 **金融业**用拨备前利润 (PPOP) 增长。PPOP 能剔除坏账计提的人为调节，反映真实获利能力。

**3️⃣ 资产负债率 (Debt to Asset) —— 【生存底线】**
地位：它是公司的“安全带”。

- 理由：即使公司再赚钱，如果杠杆过高，一旦遇到行业寒冬或信用收紧，极易发生资金断裂。它定义了公司的抗风险边界。

- 💡 看板标准：非金融行业用有息负债 / 总资产，通常 < 60% 为安全。若该指标极高且利息保障倍数低，公司随时有暴雷风险。

- 🏦 **金融业**：使用了核心一级资本充足率 (Core Tier-1 Capital Adequacy Ratio)，监管要求通常 **>7.5%-8.5%**。

~~- 🏦 **金融业**用总负债 / 总资产。如果该比例异常升高（例如超过 95%），意味着银行的杠杆极高，微小的资产波动（如房价下跌导致抵押品贬值）就可能导致净资产清零。普通企业关注借钱成本；银行的“负债”主要是客户存款（生产资料），因此关注全口径负债率或核心一级资本充足率。~~

**4️⃣ 存货/应收账款周转率 (Inventory Turnover) —— 【营运效率】**
地位：它是企业的“代谢能力”。

理由：反映了资产变现的速度。货卖得快不快？钱收回得顺不顺？在你的数据集中，这两个指标可以合并看作运营能力的代表。

- 💡 看板标准：逐年递增或保持稳定。如果周转率突然大幅下降，通常预示着产品竞争力下滑或下游客户违约风险增加。

- 🏦 **金融业**用存贷比 (Loan-to-Deposit)，反映了将吸收的存款转化为放贷资产的效率。

**5️⃣ 企业价值倍数 (EV/EBITDA) —— 【估值定价】**
地位：它是专业的“买入标尺”。

理由：相比 PE（市盈率），它剔除了非经常性损益、利息和税收的干扰，更公平地反映了企业核心业务的估值。它是机构投资者判断“买得值不值”的核心工具。

- 💡 看板标准：寻找“低估值 + 高ROE”的交叉点。

- 🏦 **金融业**用P/B (市净率)。银行的利息支出是经营成本而非财务费用，EBITDA 无意义。P/B 是银行估值的金标准，直接反映资产净值的溢价。

    - 由于银行经营具有顺周期性，当 P/B 低于 1 时（如当前的许多内银股），通常意味着市场对其资产质量（坏账隐忧）存在折价。
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
            radar_metrics = FUNDAMENTAL_KEY_COLS
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
                            key_metrics = ['latest_financial_score',  'roe',
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

            st.divider()

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
                    color_discrete_sequence=DISCRETE_COLOR,
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
        # TODO: Specify which metrics on right/left when "多指标单股票" is selected in Financial_Aalysis.py page.
        # 图表
        fig = go.Figure()

        for metric in selected_metrics:
            if metric not in df.columns: continue
            stock_data = df[(df['symbol'] == selected_stock) & (df[metric].notna())]
            if not stock_data.empty:
                fig.add_trace(
                    go.Scatter(x=stock_data['fiscal_year'], y=stock_data[metric], name=metric, mode='lines+markers')
                )
        fig.update_layout(
            title=f"{stock_names.get(selected_stock, selected_stock)} 多指标趋势",
            xaxis_title="年份",
            yaxis_title="指标值",
            height=500,
            showlegend=True
        )
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
filtered_df_sidebar = render_filter_sidebar(df, default_filter_mode=0, default_stock_input_list='SZ301611,SH688386', default_stock_search_list=['SH600054-黄山旅游'])
selected_symbols = filtered_df_sidebar['symbol'].unique().tolist()
# 缓存管理
with st.sidebar.expander("缓存管理", expanded=False):
    if st.button("🔄 清理过期缓存", use_container_width=True):
        clean_expired_cache()
        st.sidebar.success("已清理过期缓存")

    if st.button("🗑️ 清除所有缓存", use_container_width=True):
        clear_cache()
        st.sidebar.success("已清除所有缓存")

# 创建stock_map
# SECURITY_NAME_ABBR
_df_unique = df[['symbol', 'company']].drop_duplicates()
stock_map = dict(zip(_df_unique['symbol'], _df_unique['symbol'] + " - " + _df_unique['company']))

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
filtered_df['display_name'] = filtered_df['symbol'] + ' - ' + filtered_df['company']
_filtered_unique = filtered_df[['symbol', 'company']].drop_duplicates()
stock_names = dict(zip(_filtered_unique['symbol'], _filtered_unique['company']))

# 指标选择
financial_metrics = {
    'Liquidity Ratios（流动性比率）': ['current_ratio', 'quick_ratio', 'cash_ratio'],
    'Leverage Ratios（杠杆比率）': ['total_debt', 'net_debt', 'debt_to_equity', 'debt_to_asset', 'interest_coverage'],
    'Efficiency Ratios（效率比率）': ['revenue', 'gross_profit', 'net_profit', 'asset_turnover', 'inventory_turnover',
                                    'receivables_turnover'],
    'Profitability Ratios（盈利能力比率）': ['gross_margin', 'operating_margin', 'profit_margin', 'roe', 'roa', 'equity_multiplier'],
    'Cash Flow & Valuation Metrics（现金流和估值指标）': ['netcash_operate_over_net_profit',
                                                        'free_cash_flow_conversion_rate', 'change_in_working_capital',
                                                        'net_debt_over_ebitda', 'ev_over_ebitda']
}
available_metrics = [col for col in df.columns if
                     col not in ['symbol', 'company', 'fiscal_year', 'ORG_TYPE', 'industry']]

st.markdown("### 📈 财务指标对比")
metric_category = st.segmented_control("选择指标类别", options=list(financial_metrics.keys()) + ['自定义'],
                                       selection_mode='single',
                                       default=list(financial_metrics.keys())[0],
                               key="metric_category_select")
# metric_category = st.selectbox("选择指标类别", options=list(financial_metrics.keys()) + ['自定义'], index=0,
#                                key="metric_category_select")
if metric_category == '自定义':
    selected_metrics = st.multiselect("选择指标", options=available_metrics, default=FUNDAMENTAL_KEY_COLS,
                                      key="metric_multiselect")
else:
    selected_metrics = [m for m in financial_metrics[metric_category] if m in available_metrics]

if not selected_metrics:
    st.stop()

# 时间范围
if 'fiscal_year' in filtered_df.columns:
    min_year = int(filtered_df['fiscal_year'].min())
    max_year = int(filtered_df['fiscal_year'].max())
    year_range = st.slider("选择年份范围", min_value=min_year, max_value=max_year,
                           value=(max(min_year, max_year - 5), max_year), key="year_range_slider")
    filtered_df = filter_stock_data(df, selected_symbols, year_range).copy()
    filtered_df['display_name'] = filtered_df['symbol'] + ' - ' + filtered_df['company']

# 新的 Tab 布局
tab1, tab2 = st.tabs(["💹 综合分析", "📊 财务指标趋势 & 对比"])

with tab1:
    render_comprehensive_tab(filtered_df, selected_symbols, stock_names)

with tab2:
    render_trends_tab(filtered_df, selected_symbols, selected_metrics, stock_names)
