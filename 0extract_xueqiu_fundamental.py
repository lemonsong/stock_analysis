''''
WIP
extract financial statements data from Xueqiu
'''
import sys

import pandas as pd
import tushare as ts
import pysnowball as ball
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

PROGRAM_PATH = f'{PROJECT_PATH}/data_xueqiu_fundamental'


symbol_df = pd.read_csv(f'{PROJECT_PATH}/0decision.csv')
symbol_li = symbol_df.symbol.to_list()


# for stock_symbol in symbol_li:
for stock_symbol in symbol_li:

    indicator_dict = ball.indicator(symbol='SZ002027', is_annals=1, count=5)
    if indicator_dict['error_code'] = 0:
        indicator_df =
    income_dict = ball.income(symbol='SZ300251', is_annals=1, count=5)
    balance_dict = ball.balance(symbol='SZ300251', is_annals=1, count=5)
    cf_dict = ball.cash_flow(symbol='SZ300251', is_annals=1, count=5)