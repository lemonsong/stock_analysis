import os
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="US Industry Trend Forecast", layout="wide")
st.title("US Leading Indicator and ETF Forecast (LSTM)")

# Configuration
TRAIN_FILE = "data/dwa/forecast/US_industry_train.csv"
PRED_FILE = "data/dwa/forecast/US_industry_pred.csv"

# ETF columns to calculate change
ETF_COLS = ['VOX', 'VCR', 'VDC', 'VDE', 'VFH', 'VHT', 'VIS', 'VGT', 'VAW', 'VNQ', 'VPU', 'QQQ', 'VOO', 'VTV', 'VIGAX', 'VO', 'VB', 'VGK']

@st.cache_data
def load_data(filepath):
    if not os.path.exists(filepath):
        return None
    df = pd.read_csv(filepath)
    df['activity_date'] = pd.to_datetime(df['activity_date'])
    df = df.sort_values('activity_date').reset_index(drop=True)
    return df

def main():
    df_train = load_data(TRAIN_FILE)
    df_pred = load_data(PRED_FILE)

    if df_train is None:
        st.error(f"Training file not found: {TRAIN_FILE}")
        return

    if df_pred is None:
        st.error(f"Prediction file not found: {PRED_FILE}. Please run `3pred_industry_US.py` first.")
        return

    st.subheader("Data Overview")
    st.write("Original Time Series Data (Tail):")
    st.dataframe(df_train.tail())

    st.write("Predicted Data (Head):")
    st.dataframe(df_pred.head())

    # ETF Absolute and Percentage Change Table
    st.subheader("ETF Predicted Change (Next 90 Days)")

    last_train_values = df_train.iloc[-1]
    last_pred_values = df_pred.iloc[-1]

    changes = []
    for col in ETF_COLS:
        if col in df_train.columns and col in df_pred.columns:
            orig_val = last_train_values[col]
            pred_val = last_pred_values[col]
            abs_change = pred_val - orig_val
            pct_change = (abs_change / orig_val) * 100 if orig_val != 0 else 0

            changes.append({
                'ETF': col,
                'Last Actual': f"{orig_val:.2f}",
                'Predicted (Day 90)': f"{pred_val:.2f}",
                'Absolute Change': f"{abs_change:.2f}",
                'Percentage Change (%)': f"{pct_change:.2f}%"
            })

    df_changes = pd.DataFrame(changes)
    st.table(df_changes)

    # Visualization
    st.subheader("Visualization")
    feature_cols = [c for c in df_train.columns if c != 'activity_date']
    default_cols = [col for col in ETF_COLS[:3] if col in feature_cols]
    if not default_cols:
        default_cols = feature_cols[:3] if len(feature_cols) >= 3 else feature_cols
    selected_cols = st.multiselect("Select columns to plot", feature_cols, default=default_cols)

    if selected_cols:
        fig, ax = plt.subplots(figsize=(14, 7))
        for col in selected_cols:
            # Plot training data
            ax.plot(df_train['activity_date'], df_train[col], label=f'{col} (Actual)')
            # Plot predicted data
            ax.plot(df_pred['activity_date'], df_pred[col], label=f'{col} (Predicted)', linestyle='--')

        ax.set_title("Historical and Predicted Values")
        ax.set_xlabel("Date")
        ax.set_ylabel("Value")
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        st.pyplot(fig)

if __name__ == "__main__":
    main()
