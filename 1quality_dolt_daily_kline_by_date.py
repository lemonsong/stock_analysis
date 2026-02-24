'''
During building Zipline bundle step, use this script to
examine who doesn't have data_all_list in which trading dates
'''
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
from stockstats import StockDataFrame as sdf
from utils.constants import PROJECT_PATH


from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path, get_today_date_string
from utils.alltick_helper import (calculate_ndays, get_single_stock_price_hist,
                                  process_single_stock_price_hist, get_all_stock_list,
                                  get_extracted_stock_list)
# from zipline.utils.calendar_utils import get_calendar
import exchange_calendars as xcals

import pytz

# path_to_stock_csv = f'{PROJECT_PATH}/data/zipline/daily'
# path_to_stock_csv = f'{PROJECT_PATH}/data/zipline/daily_test'
# path_to_stock_csv = f'{PROJECT_PATH}/data/dolt/daily'
# check TUSHARE data_all_list
path_to_stock_csv = f'{PROJECT_PATH}/data/tushare_kline/daily'
end_date_str = '2026-02-24' # TODO:


print('start')
xshg = xcals.get_calendar("XSHG")

xshg_dates = xshg.sessions_in_range("2020-12-01", end_date_str
                                    # datetime.now().strftime('%Y-%m-%d')
                                    )
xshg_dates_li = [i.strftime('%Y-%m-%d') for i in xshg_dates.tolist()]
file_list = get_file_paths_pathlib(path_to_stock_csv)

def find_duplicates(input_list):
    seen = set()
    duplicates = set()
    for item in input_list:
        if item in seen:
            duplicates.add(item)
        else:
            seen.add(item)
    return list(duplicates)

symbol_w_problem_li = []
for file_path in file_list:
    stock_symbol = extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',to_format='MARKETnumber') # use MARKETnumber for data_dolt, number_MARKET for zipline_data folder
    stock_df = pd.read_csv(file_path)
    # stock_df['date'] = stock_df['date'].dt.strftime('%Y-%m-%d')
    s_dates_li = stock_df['date'].tolist()

    # Exam data_all_list quality
    # need to change calendar's start and end date accordingly to deal with the dates at the beginning/end

    # TYPE 1: missing data_all_list for the trading dates
    missing_dates_li = [i for i in xshg_dates_li if i not in s_dates_li]
    if len(missing_dates_li)>0:
        symbol_w_problem_li.append(stock_symbol)
        print(f"""{stock_symbol} missed data_all_list in {','.join(missing_dates_li)}""")

    # TYPE 2: duplicated rows by the date column value
    if len(stock_df['date'].tolist()) > len(stock_df['date'].unique().tolist()):
        symbol_w_problem_li.append(stock_symbol)
        print(stock_symbol)
        logging.debug(find_duplicates(stock_df['date'].tolist()))

    # TYPE 3: extra data_all_list for the non trading dates
    extra_dates_li = [i for i in s_dates_li if i not in xshg_dates_li]
    if len(extra_dates_li)>0:
        symbol_w_problem_li.append(stock_symbol)
        print(f"""{stock_symbol} has extra data_all_list in {','.join(extra_dates_li)}""")


print(symbol_w_problem_li)



