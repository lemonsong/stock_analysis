'''
Prep a single A stock dividends data
Combine data_ak_dividend into one file then format it
'''
import pandas as pd
import random
import time

from utils.constants import PROJECT_PATH
import os
from utils.common import format_stock_symbol, get_file_paths_pathlib, extract_stock_symbol_from_path, format_df_column_name
from datetime import datetime
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)


PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_dividend'

dividends_file_list = get_file_paths_pathlib(f'{PROGRAM_PATH}/single_file')
all_dividends_df = pd.DataFrame()

for dividends_file_path in dividends_file_list:
    single_dividends_df = pd.read_csv(dividends_file_path)
    logging.info(f'Read {dividends_file_path}; file has {len(single_dividends_df)=} rows.')
    all_dividends_df = pd.concat([all_dividends_df, single_dividends_df], axis=0)

# format dataframe
all_dividends_df = all_dividends_df[['代码', '名称', '送转股份-送转总比例', '送转股份-送转比例', '送转股份-转股比例', '现金分红-现金分红比例',
      '现金分红-股息率', '除权除息日']]
all_dividends_df = format_df_column_name(all_dividends_df)
all_dividends_df['symbol'] = all_dividends_df['symbol'].map(lambda x: format_stock_symbol(x,'number','MARKETnumber'))
logging.info(f'Before drop_duplicates, file has {len(all_dividends_df)=} rows.')
all_dividends_df = all_dividends_df.drop_duplicates()
logging.info(f'After drop_duplicates, file has {len(all_dividends_df)=} rows.')

# save to csv
all_dividends_df.to_csv(f'{PROGRAM_PATH}/stock_dividend.csv', index=False, encoding='utf-8')

