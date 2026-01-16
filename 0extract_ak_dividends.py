'''
Fetch dividends info every half year
'''
import os, random, time
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd
import akshare as ak
from config import PROJECT_PATH

'''
https://akshare.akfamily.xyz/data/stock/stock.html#id165
Manually change the date_input
'''
PROGRAM_PATH = f'{PROJECT_PATH}/data_ak_dividends/single_file/'
# TODO: update the date_input
date_input = '20251231' # choice of {"XXXX0630", "XXXX1231"}; 从 19901231 开始
stock_fhps_em_df = ak.stock_fhps_em(date=date_input)
stock_fhps_em_df.to_csv(f'{PROGRAM_PATH}{date_input}.csv',
                                               encoding='utf-8',
                                               index=False)
