'''

'''
import requests
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
from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path


PROGRAM_PATH = f'{PROJECT_PATH}/data_ak_fundamental'
PATH_TO_INFO_CSV= f'{PROGRAM_PATH}/0stock_info.csv'
decision_df = pd.read_csv(f'{PROJECT_PATH}/0decision.csv')
# critical_df = decision_df.loc[decision_df.overall_signal_count==0].copy()
# logging.info(f"{critical_df=}")

# stock list to fetch fundamentals data
stock_li = decision_df.symbol.tolist()
# stock_li = ['SZ000573']
if os.path.isfile(PATH_TO_INFO_CSV):
    stock_w_info_df = pd.read_csv(PATH_TO_INFO_CSV)
    stock_w_info_li = stock_w_info_df.symbol.tolist()
else:
    stock_w_info_df = pd.DataFrame()
    stock_w_info_li = []
stock_to_fetch_info_li= [i for i in stock_li if i not in stock_w_info_li]

for stock_symbol in stock_to_fetch_info_li[0:30]:
    logging.info(f"Fetching info for {stock_symbol} ...")

    info_df = ak.stock_individual_info_em(symbol=stock_symbol)
    info_df.to_csv(f'{PROGRAM_PATH}/single_info/{stock_symbol}_info.csv', encoding='utf-8', index=False)
    random_sleeptime = random.randint(11, 50)
    logging.info(f"Wait for {random_sleeptime} seconds...")
    time.sleep(random_sleeptime)
    #
    # try:
    #     logging.info(f"Fetching info for {stock_symbol} ...")
    #
    #     info_df = ak.stock_individual_info_em(symbol=stock_symbol)
    #     info_df.to_csv(f'{PROGRAM_PATH}/single_info/{stock_symbol}_info.csv', encoding='utf-8', index=False)
    #     random_sleeptime = random.randint(11, 50)
    #     logging.info(f"Wait for {random_sleeptime} seconds...")
    #     time.sleep(random_sleeptime)
    # except requests.exceptions.ProxyError as e:
    #     logging.info(f"Failed to connect to the proxy: {e}")
    # except requests.exceptions.ConnectTimeout:
    #     logging.info("Proxy connection timed out.")
    # except requests.exceptions.RequestException as e:
    #     logging.info(f"An unexpected request error occurred: {e}")
    # except Exception as e:
    #     logging.info(f"An unhandled error occurred: {e}")

stock_info_files = get_file_paths_pathlib(f'{PROGRAM_PATH}/single_info')
info_li = []
for stock_info_file in stock_info_files:
    info_df = pd.read_csv(stock_info_file)
    info_li.append(info_df)
stock_info_df_raw = pd.concat(info_li, axis=1)
stock_info_df = pd.DataFrame(data=stock_info_df_raw['value'].T.values, columns=stock_info_df_raw.iloc[:,0].tolist())
stock_info_df = stock_info_df.rename(columns={"代码":'symbol','名称':'name',
                             "股票代码":'symbol','股票简称':'name'})
stock_info_df.to_csv(PATH_TO_INFO_CSV, encoding='utf-8', index=False)

