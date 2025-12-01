''''
Update single stock of ALL stock kline data between start_date and end_date
'''
import sys

import pandas as pd
import tushare as ts
from utils.constants import TUSHARE_API_KEY
from config import PROJECT_PATH
import os
from utils.common import format_stock_symbol, get_file_paths_pathlib, extract_stock_symbol_from_path
from utils.tushare_helper import format_tushare_kline_to_dolt_style
from datetime import datetime
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import exchange_calendars as xcals

PROGRAM_PATH = f'{PROJECT_PATH}/data_tushare'

start_date = "2025-11-14" # TODO
end_date = '2025-11-25' # TODO
kline_file_path = f'{PROGRAM_PATH}/0kline_{start_date}_to_{end_date}.csv'
write_log_file_path = f'{PROGRAM_PATH}/0daily_data_write_log.csv'
is_test = False # TODO
if is_test:
    daily_folder='daily_test'
else:
    daily_folder = 'daily'
xshg = xcals.get_calendar("XSHG")
xshg_dates = xshg.sessions_in_range(start_date,end_date)
xshg_dates_li = xshg_dates.tolist()
xshg_dates_li_ymd = [i.strftime('%Y%m%d') for i in xshg_dates_li]

### get kline file so that we can append new kline data to the single stock files in the daily folder
if os.path.isfile(kline_file_path):
    kline_df = pd.read_csv(kline_file_path)
else:
    ### fetch daily kline of all stock between start_date and end_date from TUSHARE day by day ###
    ts_api = ts.pro_api(TUSHARE_API_KEY)
    kline_df = pd.DataFrame()
    for xshg_date in xshg_dates_li_ymd:
        kline_df_sub = ts_api.daily(trade_date=xshg_date)
        logging.info(xshg_date)
        if len(kline_df_sub) == 0:
            logging.critical("""We haven't fetched new kline_df_sub""")
            sys.exit()
        logging.info(kline_df_sub.head())
        kline_df = pd.concat([kline_df,
                                kline_df_sub], axis=0, ignore_index=True)

    # save to CSV
    kline_df.to_csv(kline_file_path, index=False, encoding='utf-8')
    logging.info('Saved CSV before convert kline_df ')
    # convert kline_df so that it is similar to the data from extracted from dolt
    kline_df = format_tushare_kline_to_dolt_style(kline_df)
    # save to CSV
    kline_df.to_csv(kline_file_path, index=False, encoding='utf-8')
    logging.info('Saved CSV after convert kline_df ')
logging.info(kline_df.head())


### append new kline to single stock file ###
# get log data file to record how single stock were changed
if os.path.isfile(write_log_file_path):
    write_log_df = pd.read_csv(write_log_file_path)
else:
    write_log_df = pd.DataFrame()
# get list of single stock data
file_list = get_file_paths_pathlib(f'{PROGRAM_PATH}/{daily_folder}')
for file_path in file_list:
    logging.info(file_path)
    stock_symbol = extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',to_format='MARKETnumber') # use MARKETnumber for data_dolt, number_MARKET for zipline_data folder
    stock_df = pd.read_csv(file_path)
    kline_df_sub = kline_df.loc[kline_df.symbol == stock_symbol, ['date', 'high', 'low', 'open', 'close', 'adjclose', 'volume', 'amount']].copy()
    write_log_df_sub = pd.DataFrame({
        'folder': daily_folder,
        'symbol': [stock_symbol],
        'old_data_max_date': [stock_df['date'].max()],
        'new_date_min_date': [kline_df['date'].min()],
        'update_time': datetime.now(),
        'method': 'append'
    })
    # append new kline data to single stock file
    stock_df = pd.concat([stock_df, kline_df_sub], axis=0, ignore_index=True)
    stock_df.to_csv(file_path, index=False, encoding='utf-8')
    # append wrote stock symbol to write_log_df_sub for reference
    write_log_df = pd.concat([write_log_df, write_log_df_sub], axis=0, ignore_index=True)
    write_log_df.to_csv(write_log_file_path, index=False, encoding='utf-8')
logging.info(f"""{daily_folder} folder updated. Check {write_log_file_path} for written log. Pay attention to the stock with different new/old date """)



