'''
get the latest stock close price for each stock and year
The output is used to join with fundamental data_all_list and calculate financial metrics
'''
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd

from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path
from utils.constants import PROJECT_PATH

PATH_TO_KLINE_CSV = f'{PROJECT_PATH}/data/tushare_kline/daily'
PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_fundamental'
# close_col = 'close' # TODO: close or adjclose. If use this, we need to rename the columns name to close, so that the program can run smoothly when join to fundamental data_all_list and calculate financial metrics

import os

# Try to load SHARE_CAPITAL from fundamental data to calculate daily market cap
fundamental_file = f'{PROGRAM_PATH}/fundamental_cleaned.csv'
if os.path.exists(fundamental_file):
    fundamental_df = pd.read_csv(fundamental_file)
    # Extract fiscal_year from REPORT_DATE_NAME
    fundamental_df['fiscal_year'] = fundamental_df['REPORT_DATE_NAME'].apply(lambda l: int(str(l)[:4]))
    # Keep only necessary columns: symbol, fiscal_year, SHARE_CAPITAL
    if 'symbol' in fundamental_df.columns and 'SHARE_CAPITAL' in fundamental_df.columns:
        fundamental_capital = fundamental_df[['symbol', 'fiscal_year', 'SHARE_CAPITAL']].copy()
    else:
        fundamental_capital = pd.DataFrame()
else:
    fundamental_capital = pd.DataFrame()

# read kline file list
file_list = get_file_paths_pathlib(PATH_TO_KLINE_CSV)
all_stock_df = pd.DataFrame()

# list to store all daily data for market cap calculation
all_daily_data = []

for file_path in file_list:
    # extract stock symbol
    stock_symbol = extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',to_format='MARKETnumber') # use MARKETnumber for data_dolt, number_MARKET for zipline_data folder
    try:
        stock_df = pd.read_csv(file_path)
        if stock_df.empty:
            continue

        # only keep the columns that we need
        stock_df = stock_df[['date', 'close']]
        stock_df['symbol'] = stock_symbol
        stock_df['date'] = pd.to_datetime(stock_df['date'])
        # extract year
        stock_df['fiscal_year'] = stock_df['date'].dt.year

        # Append to all daily data
        all_daily_data.append(stock_df.copy())

        # keep the max date for each year
        max_indices = stock_df.groupby(['fiscal_year'])['date'].idxmax()
        stock_df_yearly = stock_df.loc[max_indices, :]
        # merge the single stock data_all_list to all stock data_all_list
        all_stock_df = pd.concat([all_stock_df, stock_df_yearly], axis=0, ignore_index=True)
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

logging.info(f'{len(all_stock_df)} yearly rows')
# remove the date column
if not all_stock_df.empty:
    all_stock_df = all_stock_df.drop('date', axis=1)
    all_stock_df.to_csv(f'{PROGRAM_PATH}/0latest_stock_price_by_yearly.csv', encoding='utf-8', index=False)

# Calculate Daily Market Cap if fundamental_capital is available
if not fundamental_capital.empty and all_daily_data:
    all_daily_df = pd.concat(all_daily_data, axis=0, ignore_index=True)

    # Merge daily kline with fundamental capital on symbol and fiscal_year
    daily_mc_df = all_daily_df.merge(fundamental_capital, on=['symbol', 'fiscal_year'], how='left')

    # Forward fill missing SHARE_CAPITAL for the same symbol
    daily_mc_df['SHARE_CAPITAL'] = daily_mc_df.groupby('symbol')['SHARE_CAPITAL'].ffill()
    # Backward fill to handle early years without data
    daily_mc_df['SHARE_CAPITAL'] = daily_mc_df.groupby('symbol')['SHARE_CAPITAL'].bfill()

    # Calculate market cap
    daily_mc_df['market_cap'] = daily_mc_df['close'] * daily_mc_df['SHARE_CAPITAL']

    # Select final columns and save
    daily_mc_df = daily_mc_df[['symbol', 'date', 'close', 'SHARE_CAPITAL', 'market_cap']]
    daily_mc_df.to_csv(f'{PROGRAM_PATH}/daily_market_cap.csv', index=False, encoding='utf-8')
    logging.info(f"Saved daily_market_cap.csv with {len(daily_mc_df)} rows")
else:
    logging.warning("Skipped calculating daily_market_cap.csv due to missing fundamental data or kline data")


