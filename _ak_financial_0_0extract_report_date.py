import os
import logging
import pandas as pd
import akshare as ak
from datetime import datetime
from utils.constants import PROJECT_PATH
import time

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)

def get_latest_completed_quarters(num=2):
    today = datetime.now()
    current_quarter = (today.month - 1) // 3 + 1

    dates = []
    y = today.year
    q = current_quarter - 1
    if q <= 0:
        q = 4
        y -= 1

    for _ in range(num):
        if q == 1:
            dates.append(f"{y}0331")
        elif q == 2:
            dates.append(f"{y}0630")
        elif q == 3:
            dates.append(f"{y}0930")
        elif q == 4:
            dates.append(f"{y}1231")

        q -= 1
        if q <= 0:
            q = 4
            y -= 1
    return dates

def fetch_with_retry(period, max_retries=3):
    for i in range(max_retries):
        try:
            df = ak.stock_yysj_em(symbol="沪深A股", date=period)
            return df
        except Exception as e:
            logging.warning(f"Error fetching data for {period} (attempt {i+1}/{max_retries}): {e}")
            if i < max_retries - 1:
                time.sleep(2)
    return None

def main():
    periods = get_latest_completed_quarters(2)
    logging.info(f"Fetching report dates for periods: {periods}")

    all_data = []
    for period in periods:
        logging.info(f"Fetching data for period {period}")
        df = fetch_with_retry(period)
        if df is not None and not df.empty:
            df['REPORT_PERIOD'] = period
            all_data.append(df)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        # Select required columns
        output_df = combined_df[['股票代码', '实际披露时间', 'REPORT_PERIOD']].copy()
        output_df.rename(columns={'股票代码': 'symbol', '实际披露时间': 'actual_disclosure_date'}, inplace=True)

        # Drop rows where actual disclosure date is empty
        output_df.dropna(subset=['actual_disclosure_date'], inplace=True)
        # Filter out rows where date is NaT string representation or empty string
        output_df = output_df[~output_df['actual_disclosure_date'].astype(str).isin(['NaT', 'nan', ''])]

        output_path = f"{PROJECT_PATH}/data/ak_financial/latest_report_dates.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        output_df.to_csv(output_path, index=False)
        logging.info(f"Saved report dates to {output_path} with {len(output_df)} records.")
    else:
        logging.warning("No data fetched.")

if __name__ == "__main__":
    main()
