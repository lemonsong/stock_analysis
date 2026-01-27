import os, random, time, sys
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd
import akshare as ak
from utils.common import format_stock_symbol,basic_formatter
from utils.constants import PROJECT_PATH

PROGRAM_PATH = f'{PROJECT_PATH}/data_ak_industry/'

is_test = False # TODO: change to False
########### TODO: define stock_li to fetch industry data_all_list ##########
# get all current stock
# METHOD 1:
# stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
# METHOD 2:
decision_df = pd.read_csv(f'{PROJECT_PATH}/0decision2.csv')
decision_df['symbol'] = decision_df['symbol'].map(lambda x: format_stock_symbol(x, 'MARKETnumber', 'number'))
critical_df = decision_df.iloc[40:80,:].copy()
logging.info(f"{critical_df=}")

# get stocks having industry data_all_list
stock_industry_df = pd.read_csv(f'{PROGRAM_PATH}/stock_industry.csv')
logging.info(f"{stock_industry_df.shape=}")
# stock list to fetch industry data_all_list
stock_li = critical_df.symbol.tolist()
existed_li = stock_industry_df.symbol.tolist()
stock_li = [i for i in stock_li if i not in existed_li]
logging.info(f"{stock_li=}")

###########################

if is_test:
    individual_stock_info_df_raw = pd.read_csv(f'{PROGRAM_PATH}/test.csv')
    stock_industry_df.loc[len(stock_industry_df)] = individual_stock_info_df_raw['value'].T.values
    stock_industry_df.to_csv(f'{PROGRAM_PATH}/stock_industry.csv', encoding='utf-8', index=False)
    sys.exit(0)
for stock_symbol in stock_li:
    random_sleep_time = random.randint(11, 30)
    logging.info(f"Fetching industry data of {stock_symbol}")
    individual_stock_info_df_raw = ak.stock_individual_info_em(symbol=stock_symbol)
    stock_industry_df.loc[len(stock_industry_df)] = individual_stock_info_df_raw['value'].T.values
    stock_industry_df.to_csv(f'{PROGRAM_PATH}/stock_industry.csv', encoding='utf-8', index=False)
    logging.info(f"Fetched industry data of {stock_symbol}, shape of stock_industry data: {stock_industry_df.shape=}/n")
    time.sleep(random_sleep_time)
