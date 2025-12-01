'''
prep Tushare kline data
 by ffill the empty trading date,
 drop duplicated date,x
 and remove date after the end_date_str
'''
import pandas as pd
import tushare as ts
from utils.constants import TUSHARE_API_KEY
from utils.tushare_helper import format_tushare_kline_to_dolt_style
import random
import time

from config import PROJECT_PATH
import os
from utils.common import format_stock_symbol, get_file_paths_pathlib, extract_stock_symbol_from_path
from utils.dolt_helper import clean_daily_by_dates
from datetime import datetime
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
is_test = False # TODO
if is_test:
    daily_folder='daily_test'
else:
    daily_folder = 'daily'
# list of symbol identified having problem based on 1data_quality_dolt_daily_kline_by_stock

PROGRAM_PATH = f'{PROJECT_PATH}/data_tushare'
# start_date_ymd = '20221201'
# end_date_ymd = '20251107'

# this is the end date of single stock kline files.
# It should be the last date of signle stock kline file.
# Or else the kline after this date will be removed in the clean_daily_by_dates() step
end_date_str = '2025-11-25' # TODO:
end_date_d = datetime.strptime(end_date_str, '%Y-%m-%d').date()
write_log_file_path = f'{PROGRAM_PATH}/0daily_data_write_log.csv'
# TODO: automate this step
# symbol_li = ['SZ000430', 'SH000905', 'SH600169', 'SZ002387', 'SH000906', 'SZ300277',
#              'SH603843', 'SH603897', 'SH600319', 'SH000985', 'SZ300353', 'SZ300018',
#              'SH688588', 'SZ300424', 'SH603118', 'SH000852', 'SZ399300', 'SZ300131',
#              'SZ000407', 'SH000300']
file_list = get_file_paths_pathlib(f'{PROGRAM_PATH}/{daily_folder}')
symbol_li = [extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',
                                                  to_format='MARKETnumber') for file_path in file_list]

### write new kline to single stock file ###
# get log data file to record how single stock were changed
if os.path.isfile(write_log_file_path):
    write_log_df = pd.read_csv(write_log_file_path)
else:
    write_log_df = pd.DataFrame()
#
ts_api = ts.pro_api(TUSHARE_API_KEY)
# for stock_symbol in ['SZ000430']:
for stock_symbol in symbol_li:
    stock_kline_file_path = f'{PROGRAM_PATH}/{daily_folder}/{stock_symbol}.csv'
    logging.info(f'Read {stock_kline_file_path}')
    #
    stock_kline_df = pd.read_csv(stock_kline_file_path)
    stock_kline_df['date'] = pd.to_datetime(stock_kline_df['date']).dt.date
    n_row_old = str(len(stock_kline_df))
    logging.info(f'Clean {stock_symbol}')
    # logging.info(stock_kline_df.tail(5))
    stock_kline_df = clean_daily_by_dates(stock_kline_df, stock_symbol, calendar_name='XSHG',
                                          calender_start="2020-12-01",
                                          calendar_end=end_date_d,
                                          must_end_date=end_date_d)
    logging.info(f'Save to {stock_kline_file_path}')
    stock_kline_df.to_csv(stock_kline_file_path, index=False, encoding='utf-8')
    # update write log
    write_log_df_sub = pd.DataFrame({
        'folder': daily_folder,
        'symbol': [stock_symbol],
        'old_data_max_date': n_row_old,
        'new_date_min_date': str(len(stock_kline_df)),
        'update_time': datetime.now(),
        'method': 'clean'
    })
    # append wrote stock symbol to write_log_df_sub for reference
    write_log_df = pd.concat([write_log_df, write_log_df_sub], axis=0, ignore_index=True)
# save the write log to csv
write_log_df.to_csv(write_log_file_path, index=False, encoding='utf-8')





'''
The follofwing is
DEPRECATED
no data even after we refetched data
'''

# ### fetch daily kline of stocks in symbol_li ###
# symbol_li_number_dot_market_format = [format_stock_symbol(i, from_format='MARKETnumber', to_format='number.MARKET') for i in symbol_li]
# symbol_str_number_dot_market_format = ','.join(symbol_li_number_dot_market_format)
# daily_kline_multi_stocks_file_path = f'{PROGRAM_PATH}/0kline_multi_stock.csv'
#
# if os.path.isfile(daily_kline_multi_stocks_file_path):
#     multi_stock_df = pd.read_csv(daily_kline_multi_stocks_file_path)
# else:
#     ts_api = ts.pro_api(TUSHARE_API_KEY)
#     multi_stock_df = ts_api.query('daily', ts_code=symbol_str_number_dot_market_format, start_date=start_date_ymd, end_date=end_date_ymd)
#     multi_stock_df = format_tushare_kline_to_dolt_style(multi_stock_df)
#     # save to CSV
#     multi_stock_df = multi_stock_df.to_csv(daily_kline_multi_stocks_file_path, index=False, encoding='utf-8')
#     logging.info('Saved CSV after convert multi_stock_df ')
# logging.info(multi_stock_df.head())

# ### write new kline to single stock file ###
# # get log data file to record how single stock were changed
# if os.path.isfile(write_log_file_path):
#     write_log_df = pd.read_csv(write_log_file_path)
# else:
#     write_log_df = pd.DataFrame()
# #
# ts_api = ts.pro_api(TUSHARE_API_KEY)
# for stock_symbol in symbol_li[0:2]:
#     daily_kline_stock_file_path = f'{PROGRAM_PATH}/{daily_folder}/{stock_symbol}.csv'
#     daily_kline_stock_df = ts_api.query('daily',
#                             ts_code=format_stock_symbol(stock_symbol, from_format='MARKETnumber', to_format='number.MARKET'),
#                             start_date=start_date_ymd,
#                             end_date=end_date_ymd)
#     logging.info(f'Fetched {stock_symbol}')
#     daily_kline_stock_df = format_tushare_kline_to_dolt_style(daily_kline_stock_df)
#     logging.info(f'Formatted {stock_symbol}')
#     daily_kline_stock_df.to_csv(daily_kline_stock_file_path, index=False, encoding='utf-8')
#     logging.info(f'Saved CSV to {daily_kline_stock_file_path}')
#     # update write log
#     write_log_df_sub = pd.DataFrame({
#         'folder': daily_folder,
#         'symbol': [stock_symbol],
#         'old_data_max_date': None,
#         'new_date_min_date': [daily_kline_stock_df['date'].min()],
#         'update_time': datetime.now(),
#         'method': 'overwrite'
#     })
#     # append wrote stock symbol to write_log_df_sub for reference
#     write_log_df = pd.concat([write_log_df, write_log_df_sub], axis=0, ignore_index=True)
#     random_sleep_time = random.randint(10, 30)
#     logging.info(f'Sleep for {random_sleep_time}s')
#     time.sleep(random_sleep_time)
# # save the write log to csv
# write_log_df.to_csv(write_log_file_path, index=False, encoding='utf-8')
#




