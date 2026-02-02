'''fetch and format net inflow data'''
import os, random, time
import logging
from utils.common import format_df_column_name,format_stock_symbol

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,  # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd
import numpy as np
import akshare as ak
from utils.constants import PROJECT_PATH

stock_individual_fund_flow_path = f'{PROJECT_PATH}/data_other/stock_individual_fund_flow_rank_df.csv'
if not os.path.exists(stock_individual_fund_flow_path):
    stock_individual_fund_flow_rank_df = ak.stock_individual_fund_flow_rank(indicator="10日")
else:
    stock_individual_fund_flow_rank_df = pd.read_csv(stock_individual_fund_flow_path)
# rename col names
stock_individual_fund_flow_rank_df = format_df_column_name(stock_individual_fund_flow_rank_df)
# map symbol
stock_individual_fund_flow_rank_df['symbol'] = stock_individual_fund_flow_rank_df['symbol'].map(
    lambda x: format_stock_symbol(x, from_format='number', to_format='MARKETnumber'))
# change t0 numeric column
stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank_df.replace('-','')
stock_individual_fund_flow_rank_df['big_money_net_inflow_ratio_10d'] = pd.to_numeric(stock_individual_fund_flow_rank_df['big_money_net_inflow_ratio_10d'], errors='coerce')
stock_individual_fund_flow_rank_df['big_money_net_inflow_ratio_10d'] = stock_individual_fund_flow_rank_df['big_money_net_inflow_ratio_10d'].replace(np.nan, 0)
stock_individual_fund_flow_rank_df.to_csv(stock_individual_fund_flow_path, index=False, encoding='utf-8')


