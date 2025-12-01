'''
get the latest stock close price for each stock and year
The output is used to join with fundamental data and calculate financial metrics
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
from config import PROJECT_PATH

PATH_TO_KLINE_CSV = '/Users/yilin/Documents/Projects/stock_analysis/data_tushare/daily'
PROGRAM_PATH = f'{PROJECT_PATH}/data_ak_fundamental'
# close_col = 'close' # TODO: close or adjclose. If use this, we need to rename the columns name to close, so that the program can run smoothly when join to fundamental data and calculate financial metrics

# read kline file list
file_list = get_file_paths_pathlib(PATH_TO_KLINE_CSV)
all_stock_df = pd.DataFrame()
for file_path in file_list:
    # extract stock symbol
    stock_symbol = extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',to_format='MARKETnumber') # use MARKETnumber for data_dolt, number_MARKET for zipline_data folder
    stock_df = pd.read_csv(file_path)
    # only keep the columns that we need
    stock_df = stock_df[['date', 'close']]
    stock_df['symbol'] = stock_symbol
    stock_df['date'] = pd.to_datetime(stock_df['date'])
    # extract year
    stock_df['fiscal_year'] = stock_df['date'].dt.year
    # keep the max date for each year
    max_indices = stock_df.groupby(['fiscal_year'])['date'].idxmax()
    stock_df = stock_df.iloc[max_indices, :]
    # merge the single stock data to all stock data
    all_stock_df = pd.concat([all_stock_df, stock_df], axis=0, ignore_index=True)
logging.info(f'{len(all_stock_df)} rows')
# remove the date column
all_stock_df = all_stock_df.drop(
    'date', axis=1
)
all_stock_df.to_csv(f'{PROGRAM_PATH}/0latest_stock_price_by_yearly.csv', encoding='utf-8', index=False)


