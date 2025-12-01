'''
compare the single stock csv extracted from DOLT data
to the TUSHARE single date data (having all stock) in certain date
in order to check DOLT data quality
'''
import pandas as pd
import tushare as ts
from utils.constants import TUSHARE_API_KEY
from config import PROJECT_PATH
import os
from utils.common import format_stock_symbol, get_file_paths_pathlib, extract_stock_symbol_from_path
from datetime import datetime
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)

PROGRAM_PATH = f'{PROJECT_PATH}/data_tushare'
new_kline_trade_date_str='20251030'
new_kline_file_path = f'{PROGRAM_PATH}/kline_{new_kline_trade_date_str}.csv'


if os.path.isfile(new_kline_file_path):
    new_kline_df = pd.read_csv(new_kline_file_path)
else:
    ts_api = ts.pro_api(TUSHARE_API_KEY)
    new_kline_df =ts_api.daily(trade_date=new_kline_trade_date_str)
    # format ts_code column(stock symbol) donwloaded from API to symbol column in 'MARKETnumber' format
    new_kline_df.insert(0, 'symbol',
                        new_kline_df.ts_code.map(
        lambda x: format_stock_symbol(x, from_format='number.MARKET', to_format='MARKETnumber')
                        )
                        )
    new_kline_df = new_kline_df.drop('ts_code', axis=1)
    new_kline_df.to_csv(new_kline_file_path, index=False, encoding='utf-8')

# TODO
# exam whether historical 20251030 value is unmatched.
# If yes, download the stock full data to the f'{PROJECT_PATH}/data_tushare/daily_to_confirm'. Then manually compare and save
file_list = get_file_paths_pathlib(f'{PROGRAM_PATH}/daily')
date_to_compare = datetime.strptime(new_kline_trade_date_str, '%Y%m%d').strftime('%Y-%m-%d')
unmatched_stock_li = []
compare_df = pd.DataFrame()
for file_path in file_list:
    stock_symbol = extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',to_format='MARKETnumber') # use MARKETnumber for data_dolt, number_MARKET for zipline_data folder
    stock_df = pd.read_csv(file_path)
    stock_df.insert(0, 'symbol',stock_symbol)
    compare_df = pd.concat([compare_df,
                            stock_df.loc[stock_df.date == date_to_compare, ['symbol', 'close']]
                            ], axis=0, ignore_index=True)

compare_df = compare_df.merge(new_kline_df[['symbol', 'close']], how='left', on=['symbol'])
logging.info(compare_df)
compare_df['abs_diff'] = (compare_df['close_y']/compare_df['close_x']-1).abs()
def define_type(row):
    if pd.isna(row['close_y']):
        return 'close not available'
    elif row.abs_diff >=0 and row.abs_diff <0.02:
        return 'ok'
    elif row.abs_diff >=0.02:
        return 'large diff'
    else:
        return 'unsure'
compare_df['type'] = compare_df.apply(lambda row: define_type(row), axis=1)
logging.info(compare_df.groupby('type')['symbol'].nunique())
compare_df.to_csv(f'{PROGRAM_PATH}/0compare.csv', index=False, encoding='utf-8')

symbol_w_diff_list = compare_df.loc[compare_df['type']=='large diff','symbol'].tolist()
if len(symbol_w_diff_list)>0:
    logging.CRITICAL(f"""On {new_kline_trade_date_str}, the close price of these stock symbols may not match""")
    logging.CRITICAL(symbol_w_diff_list)
    symbol_w_diff_list_formatted = [format_stock_symbol(i, from_format='MARKETnumber', to_format='number.MARKET') for i in symbol_w_diff_list]
    symbol_w_diff_formatted_str = ','.join(symbol_w_diff_list_formatted)
    ts_api = ts.pro_api(TUSHARE_API_KEY)
    symbol_w_diff_kline_df = ts_api.query('daily', ts_code=symbol_w_diff_formatted_str, start_date='20201201', end_date=new_kline_trade_date_str)
    symbol_w_diff_kline_df.to_csv(f'{PROGRAM_PATH}/0symbol_w_diff_kline.csv', index=False, encoding='utf-8')
    ## TODO split the symbol_w_diff_kline_df into each stock
