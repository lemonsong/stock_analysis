import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from utils.streamlit_helper import render_filter_sidebar, setup_page_config

import os
import sys

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
setup_page_config()

st.title("📈 Backtest Overview")

# File Paths
# Try using relative paths from CWD (Repo Root)
BACKTEST_FILE = Path('data/zipline_backtest/zipline_summary.csv')
INDUSTRY_FILE = Path('data/dwa/app_decision.csv')

# Strategies and Metrics definitions (for parsing)
STRATEGIES = [
    "Rolling Window Mean", "Simple Moving Averages", "RSI",
    "MACD Crossover", "RSI + MACD", "TRIX",
    "Williams %R", "Bollinger Band"
]

METRICS = [
    "Annual Return", "Cumulative Return",
    "Sharpe Ratio", "Annual Volatility", "Max Drawdown"
]

@st.cache_data
def load_data():
    if not BACKTEST_FILE.exists():
        st.error(f"Backtest file not found: {BACKTEST_FILE}")
        return None

    df = pd.read_csv(BACKTEST_FILE)

    # Load Industry Data
    if INDUSTRY_FILE.exists():
        df_ind = pd.read_csv(INDUSTRY_FILE)
        # Ensure symbol columns are strings and consistent
        df['symbol'] = df['symbol'].astype(str)
        df_ind['symbol'] = df_ind['symbol'].astype(str)

        # Merge
        df = df.merge(
            df_ind[['symbol', 'company', 'industry_category_name', 'industry_sub_category_name', 'industry_type_name']],
            on='symbol',
            how='left'
        )
    else:
        st.warning("Industry data not found.")

    return df

def parse_columns(df):
    """
    Melt the dataframe to long format: Symbol, Strategy, Metric, Value, Industry...
    """
    # Identify value columns (those that contain strategy and metric names)
    value_vars = []
    meta_vars = [col for col in df.columns if col not in value_vars]

    # We need to distinguish between metadata and metric columns
    # Metadata: Symbol, symbol, industry_...
    # Metric columns: Starts with a Strategy name

    long_data = []

    # Iterate over rows
    for idx, row in df.iterrows():
        base_info = row[meta_vars].to_dict()
        # Remove potential duplicates like 'symbol' from merge
        if 'symbol' in base_info:
            del base_info['symbol']

        for col in df.columns:
            if col in meta_vars:
                continue

            # Parse Column Name
            # We check if the column starts with any known strategy
            matched_strategy = None
            matched_metric = None

            for strategy in STRATEGIES:
                if col.startswith(strategy):
                    # Check if the rest is a valid metric
                    # The column format is "{Strategy} {Metric}"
                    suffix = col[len(strategy):].strip()
                    if suffix in METRICS:
                        matched_strategy = strategy
                        matched_metric = suffix
                        break

            if matched_strategy and matched_metric:
                entry = base_info.copy()
                entry['Strategy'] = matched_strategy
                entry['Metric'] = matched_metric
                entry['Value'] = row[col]
                long_data.append(entry)

    return pd.DataFrame(long_data)

def fast_melt(df):
    """
    Faster melting using pandas melt + vectorized string operations
    """
    # Identify non-metric columns
    # We assume metric columns match the pattern

    metric_cols = []
    for col in df.columns:
        for strategy in STRATEGIES:
            if col.startswith(strategy):
                 suffix = col[len(strategy):].strip()
                 if suffix in METRICS:
                     metric_cols.append(col)
                     break

    id_vars = [c for c in df.columns if c not in metric_cols]

    melted = df.melt(id_vars=id_vars, value_vars=metric_cols, var_name='Strategy_Metric', value_name='Value')

    # Extract Strategy and Metric
    # This is slightly complex because strategies can contain spaces.
    # We can use a regex or just apply a function. Given small data size (20-100 stocks), apply is fine.
    # But for larger data, we want to be efficient.

    # Create a mapping from full column name to (Strategy, Metric)
    col_map = {}
    for col in metric_cols:
        for strategy in STRATEGIES:
            if col.startswith(strategy):
                suffix = col[len(strategy):].strip()
                if suffix in METRICS:
                    col_map[col] = (strategy, suffix)
                    break

    # Apply mapping
    def get_strat(x):
        return col_map.get(x, (None, None))[0]
    def get_metric(x):
        return col_map.get(x, (None, None))[1]

    melted['Strategy'] = melted['Strategy_Metric'].map(lambda x: col_map[x][0])
    melted['Metric'] = melted['Strategy_Metric'].map(lambda x: col_map[x][1])

    return melted.drop(columns=['Strategy_Metric'])

# --- Main App ---

df_raw = load_data()
if df_raw is None:
    st.stop()

# Filter using shared component
# Use df_raw (one row per stock) for filtering to ensure correct counts
filtered_raw = render_filter_sidebar(df_raw, default_filter_mode=0, default_stock_input_list='SZ002249')

if filtered_raw.empty:
    st.warning("No data matches the filters.")
    st.stop()

# Preprocess
# Melt only the filtered data
filtered_df = fast_melt(filtered_raw)

# For "Symbol Analysis" tab, we need selected symbols list
selected_symbols = filtered_raw['symbol'].unique().tolist()

# Industry levels for the second tab (Keep this for the aggregation tab logic)
industry_levels = {
    'Category': 'industry_category_name',
    'Sub-Category': 'industry_sub_category_name',
    'Type': 'industry_type_name',
}

# Summary Section
st.header("📊 Backtest Summary")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Symbols", filtered_df['symbol'].nunique())

# Best Strategy (Avg Annual Return)
avg_returns = filtered_df[filtered_df['Metric'] == 'Annual Return'].groupby('Strategy')['Value'].mean()
if not avg_returns.empty:
    best_strat = avg_returns.idxmax()
    best_val = avg_returns.max()
    with col2:
        st.metric("Best Strategy (Avg Return)", best_strat, f"{best_val:.2%}")

# Worst Max Drawdown (Avg)
avg_dd = filtered_df[filtered_df['Metric'] == 'Max Drawdown'].groupby('Strategy')['Value'].mean()
if not avg_dd.empty:
    worst_dd_strat = avg_dd.idxmin() # Drawdown is negative, so min is worst (largest magnitude)
    worst_val = avg_dd.min()
    with col3:
        st.metric("Largest Avg Drawdown", worst_dd_strat, f"{worst_val:.2%}")


# Tabs
tab1, tab2 = st.tabs(["🧩 Symbol Analysis", "🏭 Industry Comparison"])

with tab1:
    st.subheader("Symbol Strategy Comparison")

    # Filter columns to only what's needed for summary
    display_df = filtered_df[['symbol', 'company', 'Strategy', 'Metric', 'Value']]

    # User selects which metric to view
    selected_metric = st.selectbox("Select Metric to View", METRICS, index=0)

    # Pivot the data for the data editor
    pivot_df = display_df[display_df['Metric'] == selected_metric].pivot_table(
        index=['symbol', 'company'],
        columns='Strategy',
        values='Value',
        aggfunc='first'
    ).reset_index()

    st.markdown(f"**{selected_metric} by Strategy**")

    # Configure styling for the dataframe depending on the metric
    # Only format columns that actually exist in the pivot_df
    available_strats = [col for col in pivot_df.columns if col not in ['symbol', 'company']]
    if "Return" in selected_metric or "Ratio" in selected_metric:
        # Higher is better
        styled_df = pivot_df.style.background_gradient(cmap='Greens', subset=available_strats, vmin=0)
    else:
        # Lower is better (e.g. Drawdown, Volatility)
        styled_df = pivot_df.style.background_gradient(cmap='Reds', subset=available_strats)

    st.dataframe(styled_df, use_container_width=True, hide_index=True)

    # Aggregate Chart
    st.markdown("### Aggregated View")
    agg_df = display_df[display_df['Metric'] == selected_metric].groupby('Strategy')['Value'].mean().reset_index()
    fig = px.bar(
        agg_df,
        x='Strategy',
        y='Value',
        title=f"Average {selected_metric} across all selected symbols",
        text_auto='.2f',
        color='Strategy'
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Industry Comparison")

    col_ctrl1, col_ctrl2 = st.columns(2)
    with col_ctrl1:
        ind_level_label = st.selectbox("Select Industry Level", list(industry_levels.keys()), index=2)
        ind_col = industry_levels[ind_level_label]

    with col_ctrl2:
        agg_method = st.selectbox("Aggregation Method", ["Average", "Median"])

    if ind_col not in filtered_df.columns:
        st.error(f"Industry column {ind_col} not available.")
    else:
        # Aggregate
        agg_func = 'mean' if agg_method == "Average" else 'median'

        # We aggregate by [Industry, Strategy, Metric]
        df_agg = filtered_df.groupby([ind_col, 'Strategy', 'Metric'])['Value'].agg(agg_func).reset_index()

        selected_metric_ind = st.selectbox("Select Metric to View (Industry)", METRICS, index=0)

        pivot_agg_df = df_agg[df_agg['Metric'] == selected_metric_ind].pivot_table(
            index=ind_col,
            columns='Strategy',
            values='Value',
            aggfunc='first'
        ).reset_index()

        st.markdown(f"**{agg_method} {selected_metric_ind} by Industry and Strategy**")

        # Configure styling
        available_strats_ind = [col for col in pivot_agg_df.columns if col != ind_col]
        if "Return" in selected_metric_ind or "Ratio" in selected_metric_ind:
            styled_agg = pivot_agg_df.style.background_gradient(cmap='Greens', subset=available_strats_ind, vmin=0)
        else:
            styled_agg = pivot_agg_df.style.background_gradient(cmap='Reds', subset=available_strats_ind)

        st.dataframe(styled_agg, use_container_width=True, hide_index=True)

        # Draw a single chart grouping all industries for the selected metric
        st.markdown("### Visualization")
        fig2 = px.bar(
            df_agg[df_agg['Metric'] == selected_metric_ind],
            x=ind_col,
            y='Value',
            color='Strategy',
            barmode='group',
            title=f"{selected_metric_ind} Comparison by Industry",
        )
        st.plotly_chart(fig2, use_container_width=True)
