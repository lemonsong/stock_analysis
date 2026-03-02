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
from utils.constants import PROJECT_PATH

'''
https://akshare.akfamily.xyz/data/stock/stock.html#id165
Manually change the date_input
'''
PROGRAM_PATH = f'{PROJECT_PATH}/data/macro/bond_yield'
# TODO: update the date_input
year_start_str_li = [f'{str(i)}0101' for i in range(2000,2010)]
logging.info(year_start_str_li)

for i in range(0,len(year_start_str_li)-1):
    output_path = f'{PROGRAM_PATH}/bond_yield_{year_start_str_li[i]}_to_{year_start_str_li[i+1]}.csv'
    if os.path.isfile(output_path): #TODO: [Critical] consider the last year not finish
        logging.info(f"{output_path} existed")
    else:
        # 单次返回所有指定日期间 start_date 到 end_date 需要小于一年的所有数据
        bond_china_yield_df = ak.bond_china_yield(start_date=year_start_str_li[i], end_date=year_start_str_li[i+1])
        bond_china_yield_df.to_csv(output_path, encoding='utf-8',
                                                   index=False)
        logging.info(f"{output_path} saved")
