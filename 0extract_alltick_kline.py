
'''
Get tick data with ALLTICK API
https://alltick.co/
'''
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time

import random
from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path, get_today_date_string
from utils.alltick_helper import (calculate_ndays, get_single_stock_price_hist,
                                  process_single_stock_price_hist, get_all_stock_list,
                                  get_extracted_stock_list)

# define input variables for API
end_date_str=get_today_date_string()
n_days = calculate_ndays(start_date_str='2018-01-01', end_date_str=end_date_str)

# get the whole ALLTICK Stock list
alltick_stock_df_list = get_all_stock_list()

# get stock list already fetched
path_to_stock_csv_folder = '/Users/yilin/Documents/Projects/stock_analysis/zipline_data/daily'
extracted_stock_list = get_extracted_stock_list(path_to_stock_csv_folder)

# get stock list to fetch
stock_to_fetch_list = [i for i in alltick_stock_df_list if i not in extracted_stock_list]

print(f'Number of all stocks: {len(alltick_stock_df_list)}')
print(f'Number of extracted stocks: {len(extracted_stock_list)}')
print(f'Number of stocks to extract: {len(stock_to_fetch_list)}')

#### Use kline to collect stock one by one ####

for stock_symbol in stock_to_fetch_list:
    stock_symbol_str = stock_symbol.replace('.', '_')
    # for stock, AllTick API allows us to get history with end_date 0, and num_days > 500
    # AllTick API documentation: https://github.com/alltick/alltick-realtime-forex-crypto-stock-tick-finance-websocket-api/blob/main/http_interface/kline_query_cn.md
    data = get_single_stock_price_hist(stock_symbol=stock_symbol, end_date=0, num_days=n_days)
    data_df = process_single_stock_price_hist(stock_symbol_str=stock_symbol_str, alltick_api_result=data)
    print(f'fetched {stock_symbol}')
    random_sleeptime = random.randint(30,60)
    time.sleep(random_sleeptime)