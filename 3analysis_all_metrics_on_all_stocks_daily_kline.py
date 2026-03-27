'''
Calculate the metrics for the latest date of daily stock data_all_list to get buy/sell signal
'''
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
import os, sys
from utils.constants import BUY_SIGNAL_COLS, SELL_SIGNAL_COLS

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,  # DEBUG,INFO,WARNING, ERROR, CRITICAL
)

from stockstats import StockDataFrame as sdf

from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path, get_today_date_string
from utils.constants import PROJECT_PATH
from utils.alltick_helper import (calculate_ndays, get_single_stock_price_hist,
                                  process_single_stock_price_hist, get_all_stock_list,
                                  get_extracted_stock_list)

# path_to_stock_csv = f'{PROJECT_PATH}/data/zipline/daily'
# path_to_stock_csv = f'{PROJECT_PATH}/data/zipline/daily_test'
# path_to_stock_csv = f'{PROJECT_PATH}/data/dolt/daily'
path_to_stock_csv = f'{PROJECT_PATH}/data/tushare_kline/daily'
dwa_path = f'{PROJECT_PATH}/data/dwa'
# TODO: add sys argument for this script, and adjsut the default argument in the refresh pipeline page
if len(sys.argv) > 1:
    logging.info(f"Fetching data via streamlit input")
    fetch_historical = False
    update_date = None
else:
    logging.info(f"Fetching data via manual input")
    # when update_date is None, use all data by default,
    # and refer update with the latest date in kline data,
    # then save to both kline_analysis.csv and dwa/kline_analysis_history
    fetch_historical = False
    update_date = None
    # fetch_historical = True
    # update_date = pd.to_datetime('2026-03-24')
# validation step
if (fetch_historical and isinstance(update_date, pd.Timestamp)) or (fetch_historical == False and update_date is None):
    logging.info(f"Correct inputs")
else:
    logging.warning(f"Error inputs")
    exit()

file_list = get_file_paths_pathlib(path_to_stock_csv)
close_col = 'close' # chose from: close or adjclose

# 3. Define trading days (approximate)
# 1 Year ≈ 252 days, 3 Years ≈ 756 days, 5 Years ≈ 1260 days
growth_periods = {
    '1Y': 252,
    '2Y': 504,
    '3Y': 756,
    # '5Y': 1260
}
growth_cols = [f'growth_{col}' for col in growth_periods.keys()]
def calculate_growth_rate(raw_df, smoothed_price_col_name):
    """
    Assumes df has columns: 'date', 'symbol', 'close'
    Sorted by symbol and date.
    """
    # must ensure date is datetime and sort in prior steps
    # must use smoothed price
    df =raw_df.copy()
    # logging.info(f'{df=}')


    for label, days in growth_periods.items():
        # Use shift() to get the price from X days ago within each group
        df[f'price_lag_{label}'] = df[smoothed_price_col_name].shift(days)

        # Calculate Growth: (Current Smoothed / Lag Smoothed) - 1
        df[f'growth_{label}'] = (df[smoothed_price_col_name] / df[f'price_lag_{label}']) - 1

    # 4. Clean up: Replace inf (from division by zero) and handle large values
    df[growth_cols] = df[growth_cols].replace([np.inf, -np.inf], np.nan)

    # Optional: Replace inf with the max finite value of that column
    for col in growth_cols:
        max_val = df.loc[np.isfinite(df[col]), col].max()
        df[col] = df[col].fillna(max_val)
    # logging.info(f'{df=}')
    return df[growth_cols].tail(1)

def get_stock_metrics(stock_df, stock_symbol):
    stock = sdf.retype(stock_df[["date", "open", "high", "low", "close"]])
    stock_metrics = stock.get(
        ["close", 'rsi_12', "boll", "boll_ub", "boll_lb", "close_xu_close_20_sma", 'close_10_sma_xd_close_50_sma',
         'macds', 'macd', 'trix', 'wr_20'])

    # metrics need to use the whole historical close price to calculate
    # tech signals are stored in stock_metrics
    stock_metrics['rolling_mean_90d'] = stock_metrics['close'].ewm(span=90, adjust=True, ignore_na=True).mean()
    # stock_metrics['rolling_mean_20d'] = stock_metrics['close'].ewm(span=20, adjust=True, ignore_na=True).mean()

    stock_metrics['rolling_mean_short_term'] = stock_metrics['rolling_mean_90d'].ewm(span=5, adjust=True, ignore_na=True).mean()
    stock_metrics['rolling_mean_long_term'] = stock_metrics['rolling_mean_90d'].ewm(span=30, adjust=True, ignore_na=True).mean()
    # growth rate is stored in stock_growth_metrics
    stock_growth_metrics = calculate_growth_rate(stock_metrics[['close', 'rolling_mean_90d']],
                                                 'rolling_mean_90d')

    # metrics need to use the last 2 rows of historical close price to calculate
    # tech signal
    stock_metrics = stock_metrics.iloc[-2:, :]

    stock_metrics['macd_buy'] = (stock_metrics.macd>stock_metrics.macds) & (stock_metrics.macd.shift(1)<=stock_metrics.macds.shift(1))
    stock_metrics['macd_sell']= (stock_metrics.macd<stock_metrics.macds) & (stock_metrics.macd.shift(1)>=stock_metrics.macds.shift(1))

    stock_metrics['rsi_plus_macd_buy'] = (stock_metrics.rsi_12 < 50) & (stock_metrics.macd > stock_metrics.macds) & (
                stock_metrics.macd.shift(1) <= stock_metrics.macds.shift(1))
    stock_metrics['rsi_plus_macd_sell'] = (stock_metrics.rsi_12 > 50) & (stock_metrics.macd < stock_metrics.macds) & (
                stock_metrics.macd.shift(1) >= stock_metrics.macds.shift(1))

    stock_metrics['trix_buy'] = (stock_metrics.trix < 0) & (
            stock_metrics.trix.shift(1) > 0)
    stock_metrics['trix_sell'] = (stock_metrics.trix > 0) & (
                stock_metrics.trix.shift(1) < 0)
    # only need to keep the tech signla in the latest date
    stock_metrics = stock_metrics.iloc[-1:, :]
    # combine with growth metrics
    stock_metrics = pd.concat([stock_growth_metrics, stock_metrics], axis=1, ignore_index=False)
    stock_metrics['symbol'] = stock_symbol
    # logging.info(f'{stock_metrics=}')
    return stock_metrics


all_stock_metrics = pd.DataFrame()

if not file_list:
    logging.warning("No stock files found. Please ensure data is present.")
else:
    for file_path in file_list:
        stock_symbol = extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',to_format='MARKETnumber') # use MARKETnumber for data_dolt, number_MARKET for zipline_data folder
        logging.info(f'Reading stock {stock_symbol}')
        stock_df = pd.read_csv(file_path)
        stock_df['date'] = pd.to_datetime(stock_df['date'])
        if not fetch_historical:
            # when not fetch_historical, use all data
            stock_df = stock_df[["date", "open", "high", "low", close_col]]
        else:
            # when fetch_historical, filter by update_date
            stock_df = stock_df.loc[stock_df.date <= update_date, ["date", "open", "high", "low", close_col]]
        stock_df = stock_df[["date", "open", "high", "low", close_col]]
        stock_df = stock_df.rename(columns={close_col: 'close'})
        stock_metrics = get_stock_metrics(stock_df, stock_symbol)
        all_stock_metrics = pd.concat([all_stock_metrics, stock_metrics], axis=0, ignore_index=True)
        # use the first stock_df to identify the update date for the kline analysis
        if not fetch_historical:
            update_date = stock_df['date'].max()
if not all_stock_metrics.empty:
    col = all_stock_metrics.pop('symbol')
    all_stock_metrics.insert(0, 'symbol', col)

    # strategy use one row to decide buy or sell
    all_stock_metrics['rsi_less_than_10_buy'] = all_stock_metrics.rsi_12<10
    all_stock_metrics['rsi_more_than_90_sell'] = all_stock_metrics.rsi_12>90
    all_stock_metrics['close_less_than_boll_lb_buy'] = all_stock_metrics.close < all_stock_metrics.boll_lb
    all_stock_metrics['close_more_than_boll_ub_sell'] = all_stock_metrics.close > all_stock_metrics.boll_ub
    all_stock_metrics['wr_less_than_10_sell'] = all_stock_metrics.wr_20<10
    all_stock_metrics['wr_more_than_90_buy'] = all_stock_metrics.wr_20>90
    all_stock_metrics['ewm_short_term_more_than_long_term_buy'] = all_stock_metrics.rolling_mean_short_term>all_stock_metrics.rolling_mean_long_term
    all_stock_metrics['ewm_short_term_less_than_long_term_sell'] = all_stock_metrics.rolling_mean_short_term<=all_stock_metrics.rolling_mean_long_term
    # overall signal count
    all_stock_metrics['buy_signal_count'] = all_stock_metrics[BUY_SIGNAL_COLS].sum(axis=1)
    all_stock_metrics['sell_signal_count'] = all_stock_metrics[SELL_SIGNAL_COLS].sum(axis=1)
    all_stock_metrics['overall_signal_count'] = all_stock_metrics['buy_signal_count'] - all_stock_metrics['sell_signal_count']

    all_stock_metrics = all_stock_metrics.sort_values('overall_signal_count', ascending=False)
    # ensure date format for update_date
    if isinstance(update_date, pd.Timestamp):
        update_date_str = update_date.strftime('%Y-%m-%d')
    else:
        update_date_str = pd.to_datetime(update_date).strftime('%Y-%m-%d')

    all_stock_metrics['update_date'] = update_date_str

    # --- Save Current ---
    if not fetch_historical:
        # Save to current analysis
        all_stock_metrics.to_csv(f'{dwa_path}/kline_analysis.csv', index=False, encoding='utf-8')

    # --- Save History ---
    # Save each day's analysis as an individual file
    history_dir = f'{dwa_path}/kline_analysis_history'
    os.makedirs(history_dir, exist_ok=True)

    history_path = f'{history_dir}/{update_date_str}.csv'
    all_stock_metrics.to_csv(history_path, index=False, encoding='utf-8')
    logging.info(f"Saved history to {history_path}, total rows: {len(all_stock_metrics)}")
else:
    logging.warning("No metrics calculated.")
