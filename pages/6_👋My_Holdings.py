import streamlit as st
import pandas as pd
from pathlib import Path
from utils.constants import PROJECT_PATH, SEQUENTIAL_COLOR
from utils.streamlit_helper import setup_page_config
from utils.feishu_helper import load_feishu_invest_data

setup_page_config()

st.title("💼 My Holdings Monitor")

# Paths
HOLDING_FILE = Path(PROJECT_PATH) / 'data/dwa/my_holding.csv'
DECISION_FILE = Path(PROJECT_PATH) / 'data/dwa/app_decision.csv'
DAILY_DATA_DIR = Path(PROJECT_PATH) / 'data/tushare_kline/daily'


@st.cache_data
def load_holdings():
    if not HOLDING_FILE.exists():
        st.error(f"Holdings file not found: {HOLDING_FILE}")
        return None
    return pd.read_csv(HOLDING_FILE)


@st.cache_data
def load_decision_data():
    if not DECISION_FILE.exists():
        st.error(f"Decision file not found: {DECISION_FILE}")
        return None
    try:
        return pd.read_csv(DECISION_FILE)
    except Exception as e:
        st.error(f"Error reading decision file: {e}")
        return None


def calculate_growth(symbol, periods):
    """
    Calculates growth for specified periods (in days).
    Returns a dict { 'growth_X': value, ... }
    """
    file_path = DAILY_DATA_DIR / f"{symbol}.csv"
    if not file_path.exists():
        return {f"growth_{p}": None for p in periods}

    try:
        df = pd.read_csv(file_path)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date', ascending=True).reset_index(drop=True)
        else:
            # Assume sorted if no date? Or cannot proceed.
            return {f"growth_{p}": None for p in periods}

        if df.empty:
            return {f"growth_{p}": None for p in periods}

        current_price = df.iloc[-1]['close']
        results = {}

        for days in periods:
            # Simple index offset (assuming daily data is contiguous trading days)
            # 1D=1, 1W=5, 1M=20, 1Q=60, 6M=120, 1Y=250

            idx = len(df) - 1 - days
            if idx >= 0:
                past_price = df.iloc[idx]['close']
                if past_price != 0:
                    growth = (current_price - past_price) / past_price
                else:
                    growth = 0
                results[f"growth_{days}"] = growth
            else:
                results[f"growth_{days}"] = None

        return results
    except Exception as e:
        return {f"growth_{p}": None for p in periods}


# --- Main ---
df_holdings = load_holdings()
df_decision = load_decision_data()
df_invest = load_feishu_invest_data()

if df_holdings is None or df_holdings.empty:
    st.warning("No holdings found or file is empty.")
    st.stop()

# Merge decision data
if df_decision is not None:
    # Ensure string
    df_holdings['symbol'] = df_holdings['symbol'].astype(str)
    df_decision['symbol'] = df_decision['symbol'].astype(str)

    # Merge only necessary columns to avoid clutter if needed, or all.
    merged_df = pd.merge(df_holdings, df_decision, on='symbol', how='left')
else:
    merged_df = df_holdings.copy()

# Merge invest data
if df_invest is not None and not df_invest.empty:
    df_invest['symbol'] = df_invest['symbol'].astype(str)
    merged_df = pd.merge(merged_df, df_invest, on='symbol', how='left')

# Calculate Growth
# Map offset days to Labels
periods_map = {
    1: '1D',
    5: '1W',
    20: '1M',
    60: '1Q',
    120: 'Half Year',
    250: '1 Year'
}
periods_days = list(periods_map.keys())

growth_data = []
# Ensure merged_df has symbol
if 'symbol' in merged_df.columns:
    for symbol in merged_df['symbol']:
        g = calculate_growth(symbol, periods_days)
        g['symbol'] = symbol
        growth_data.append(g)

    if growth_data:
        growth_df = pd.DataFrame(growth_data)
        final_df = pd.merge(merged_df, growth_df, on='symbol', how='left')
    else:
        final_df = merged_df
else:
    st.error("Holdings data missing 'symbol' column.")
    st.stop()

# Display
st.markdown("### 📊 Holdings Overview")

# Define columns to show
# Fundamentals to show: roe, fundamental_score
cols = ['symbol', 'company', 'close', 'target_buy', 'overall_signal_count', 'roe', 'fundamental_score'] + \
       [f"growth_{d}" for d in periods_days]

# Filter existing columns
display_cols = [c for c in cols if c in final_df.columns]
display_df = final_df[display_cols].copy()

# Pre-format percentages for Display (since we use number formatting in column config)
# Actually, let's keep them as fractions and use format="%.2f%%" IF Streamlit supports it correctly.
# But often Streamlit's NumberColumn needs value * 100 for %.
# Let's multiply by 100.
for d in periods_days:
    col = f"growth_{d}"
    if col in display_df.columns:
        display_df[col] = display_df[col].apply(lambda x: x * 100 if pd.notnull(x) else x)

if 'roe' in display_df.columns:
    display_df['roe'] = display_df['roe'].apply(lambda x: x * 100 if pd.notnull(x) else x)

# Column Config
col_config = {
    "symbol": st.column_config.TextColumn("Symbol"),
    "company": st.column_config.TextColumn("Company"),
    "close": st.column_config.NumberColumn("Price", format="%.2f"),
    "target_buy": st.column_config.TextColumn("Target Buy"),
    "overall_signal_count": st.column_config.NumberColumn("Signal Score"),
    "roe": st.column_config.NumberColumn("ROE", format="%.2f%%"),
    "fundamental_score": st.column_config.ProgressColumn("Fund. Score", min_value=0, max_value=100, format="%d"),
}

for d, label in periods_map.items():
    col_key = f"growth_{d}"
    col_config[col_key] = st.column_config.NumberColumn(
        f"{label} Growth",
        format="%.2f%%"
    )

# Style
styled = display_df.style
# Apply gradient to growth columns
growth_cols_present = [f"growth_{d}" for d in periods_days if f"growth_{d}" in display_df.columns]
if growth_cols_present:
    styled = styled.background_gradient(subset=growth_cols_present, cmap=SEQUENTIAL_COLOR, vmin=-20, vmax=20)

st.dataframe(
    styled,
    column_config=col_config,
    use_container_width=True,
    height=600,
    hide_index=True
)
symbol_string = ",".join(display_df["symbol"].astype(str).tolist())
# 3. Display in a code block with the copy button
st.write("Click the icon on the right to copy filtered symbols:")
st.code(symbol_string, language=None)