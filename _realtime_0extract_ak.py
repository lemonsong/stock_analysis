'''
Task to save the current price and pct of change.
Run one time after stock market open daily for analysis.
'''
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd
import akshare as ak
from io import BytesIO
from utils.common import format_stock_symbol, format_df_column_name
from utils.constants import PROJECT_PATH

PROGRAM_PATH = f'{PROJECT_PATH}/data/basic'


stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
# stock_zh_a_spot_em_df.to_csv(f'{PROGRAM_PATH}/realtime_price.csv', index=False, encoding='utf-8')
# stock_zh_a_spot_em_df = pd.read_csv(f'{PROGRAM_PATH}/realtime_price.csv')
stock_zh_a_spot_em_df = format_df_column_name(stock_zh_a_spot_em_df)
# map symbol
stock_zh_a_spot_em_df['symbol'] = stock_zh_a_spot_em_df['symbol'].map(
    lambda x: format_stock_symbol(x, from_format='number', to_format='MARKETnumber'))
stock_zh_a_spot_em_df.to_csv(f'{PROGRAM_PATH}/realtime_price.csv', index=False, encoding='utf-8')