import akshare as ak
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd

from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path,format_df_column_name
from utils.constants import PROJECT_PATH

PROGRAM_PATH = f'{PROJECT_PATH}/data/basic'
PATH_TO_STOCK_CSV = f'{PROGRAM_PATH}/stock_name_industry.csv'

# 上海证券交易所股票代码和简称数据
stock_info_sh_name_code_df = ak.stock_info_sh_name_code(symbol="主板A股")
stock_info_sh_name_code_df = format_df_column_name(stock_info_sh_name_code_df)
stock_info_sh_name_code_df['symbol'] = stock_info_sh_name_code_df['symbol'].map(lambda x: 'SH'+x)
stock_info_sh_name_code_df.to_csv(f'{PROGRAM_PATH}/stock_info/上证A.csv')
#深证证券交易所股票代码和股票简称数据
stock_info_sz_name_code_df = ak.stock_info_sz_name_code(symbol="A股列表")
stock_info_sz_name_code_df = format_df_column_name(stock_info_sz_name_code_df)
stock_info_sz_name_code_df['symbol'] = stock_info_sz_name_code_df['symbol'].map(lambda x: 'SZ'+x)
stock_info_sz_name_code_df.to_csv(f'{PROGRAM_PATH}/stock_info/深证A.csv')
# 北京证券交易所股票代码和简称数据
stock_info_bj_name_code_df = ak.stock_info_bj_name_code()
stock_info_bj_name_code_df = format_df_column_name(stock_info_bj_name_code_df)
stock_info_bj_name_code_df['symbol'] = stock_info_bj_name_code_df['symbol'].map(lambda x: 'BJ'+x)
stock_info_bj_name_code_df.to_csv(f'{PROGRAM_PATH}/stock_info/北交.csv')

# all_stock_list_date
all_stock_list_date = pd.concat([stock_info_sh_name_code_df[['symbol','list_date']],
                                 stock_info_sz_name_code_df[['symbol','list_date']],
                                 stock_info_bj_name_code_df[['symbol','list_date']]], axis=0, ignore_index=True)

# merge to stock list
stock_df = pd.read_csv(PATH_TO_STOCK_CSV)
if 'list_date' in stock_df.columns:
    stock_df = stock_df.drop('list_date', axis=1)
stock_df = stock_df.merge(all_stock_list_date, on='symbol', how='outer')
stock_df.to_csv(PATH_TO_STOCK_CSV, index=False, encoding='utf-8')


