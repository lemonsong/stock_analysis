"""
页面3.2: A股市值与K线分析
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from utils.streamlit_helper import setup_page_config, render_filter_sidebar
from utils.constants import PROJECT_PATH
import sys

setup_page_config()

st.title("📊 股价走势与市值分析")

# 数据文件路径
fundamental_file = Path(PROJECT_PATH) / 'data/ak_fundamental' / 'fundamental_calculated_metrics.csv'
industry_file = Path(PROJECT_PATH) / 'data/dwa' / 'app_decision.csv'

if not fundamental_file.exists():
    st.error(f"未找到基本面数据文件: {fundamental_file}")
    st.stop()

@st.cache_data
def load_fundamental_data():
    """加载基本面数据，并合并行业信息"""
    try:
        df = pd.read_csv(fundamental_file)
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
                df['industry'] = df['industry_type_name']
            except Exception as e:
                st.warning(f"加载行业数据失败: {e}")

        return df
    except Exception as e:
        st.error(f"读取数据文件失败: {e}")
        return None

def load_kline_data(symbol):
    """加载单只股票的日K线数据"""
    file_path = Path(PROJECT_PATH) / 'data' / 'tushare_kline' / 'daily' / f'{symbol}.csv'
    if file_path.exists():
        try:
            df = pd.read_csv(file_path)
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception:
            return None
    return None

@st.cache_data
def load_all_market_caps():
    mc_path = Path(PROJECT_PATH) / 'data' / 'ak_fundamental' / 'daily_market_cap.csv'
    if mc_path.exists():
        mc_df = pd.read_csv(mc_path)
        mc_df['date'] = pd.to_datetime(mc_df['date'])
        return mc_df
    return None

def render_market_cap_charts(df, selected_symbols, stock_names):
    """渲染日K线和市值图表"""
    all_mcs = load_all_market_caps()

    kline_cols = st.columns(min(len(selected_symbols), 2) if len(selected_symbols) > 0 else 1)
    for i, symbol in enumerate(selected_symbols):
        col_idx = i % 2
        with kline_cols[col_idx]:
            with st.container(border=True):
                st.markdown(f"#### {symbol} - {stock_names.get(symbol, symbol)}")
                kline_df = load_kline_data(symbol)

                if kline_df is not None and not kline_df.empty:
                    if 'date' in kline_df.columns:
                        kline_df = kline_df.sort_values('date')

                        # Merge market cap
                        if all_mcs is not None:
                            sym_mc = all_mcs[all_mcs['symbol'] == symbol]
                            if not sym_mc.empty:
                                kline_df = pd.merge(kline_df, sym_mc[['date', 'market_cap']], on='date', how='left')

                        # Merge revenue
                        if 'revenue' in df.columns and 'fiscal_year' in df.columns:
                            sym_gp = df[(df['symbol'] == symbol) & (df['revenue'].notna())][['fiscal_year', 'revenue']]
                            if not sym_gp.empty:
                                kline_df['year'] = kline_df['date'].dt.year
                                kline_df = pd.merge(kline_df, sym_gp, left_on='year', right_on='fiscal_year', how='left')

                        # --- Chart 1: 股价走势 (日K线) ---
                        st.markdown("##### 股价走势 (日K线)")
                        fig1 = go.Figure()
                        if 'open' in kline_df.columns and 'close' in kline_df.columns and 'high' in kline_df.columns and 'low' in kline_df.columns:
                            fig1.add_trace(go.Candlestick(
                                x=kline_df['date'],
                                open=kline_df['open'],
                                high=kline_df['high'],
                                low=kline_df['low'],
                                close=kline_df['close'],
                                name="股价 (Price)"
                            ))
                        else:
                            fig1.add_trace(go.Scatter(x=kline_df['date'], y=kline_df['close'], name="股价 (Price)", mode='lines'))

                        fig1.update_layout(
                            height=300,
                            margin=dict(l=0, r=0, t=20, b=0),
                            showlegend=False,
                            yaxis_title="<b>股价 (Price)</b>"
                        )
                        fig1.update_xaxes(rangeslider_visible=False)
                        st.plotly_chart(fig1, use_container_width=True)

                        # --- Chart 2: 市值 (Market Cap) & 总营收(Revenue) ---
                        st.markdown("##### 市值 (Market Cap) & 总营收(Revenue)")
                        fig2 = make_subplots(specs=[[{"secondary_y": True}]])

                        if 'market_cap' in kline_df.columns:
                            fig2.add_trace(
                                go.Bar(x=kline_df['date'], y=kline_df['market_cap'], name="市值 (Market Cap)", opacity=0.6, marker_color='blue'),
                                secondary_y=False,
                            )

                        if 'revenue' in kline_df.columns:
                            fig2.add_trace(
                                go.Scatter(x=kline_df['date'], y=kline_df['revenue'], name="总营收(Revenue)", mode='lines', line=dict(color='orange')),
                                secondary_y=True,
                            )

                        fig2.update_layout(
                            height=300,
                            margin=dict(l=0, r=0, t=20, b=0),
                            showlegend=True,
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                        )
                        fig2.update_yaxes(title_text="<b>市值 (Market Cap)</b>", secondary_y=False)
                        fig2.update_yaxes(title_text="<b>总营收(Revenue)</b>", secondary_y=True, showgrid=False)

                        st.plotly_chart(fig2, use_container_width=True)

                    else:
                        st.warning("数据缺少日期列，无法绘图")
                else:
                    st.caption(f"{stock_names.get(symbol, symbol)} - 暂无K线数据")

# --- 主程序 ---

df = load_fundamental_data()

if df is None or df.empty:
    st.warning("数据文件为空")
    st.stop()

# 侧边栏设置
filtered_df_sidebar = render_filter_sidebar(df, default_filter_mode=0, default_stock_input_list='SZ300726,SH688138,SZ002180,SZ300458,SZ000725,SH600707', default_stock_search_list=['SH600054-黄山旅游'])
selected_symbols = filtered_df_sidebar['symbol'].unique().tolist()

stock_names = {row['symbol']: row['company'] for _, row in
               df[['symbol', 'company']].drop_duplicates().iterrows()}

# 检查是否选择了股票
if not selected_symbols:
    st.info("请在侧边栏选择股票")
    st.stop()

render_market_cap_charts(df, selected_symbols, stock_names)
