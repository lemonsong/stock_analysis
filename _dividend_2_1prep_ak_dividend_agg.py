import logging

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,  # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path
from utils.constants import PROJECT_PATH
from functools import reduce

# ref: https://www.tidy-finance.org/python/financial-statement-analysis.html#combining-financial-ratios

PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_dividend'
# get current date as baseline date
current_date = pd.Timestamp(datetime.now().date())
# load data
dividend_df = pd.read_csv(f'{PROGRAM_PATH}/stock_dividend.csv')

# preprocess data
# 转换日期格式
dividend_df['execution_date'] = pd.to_datetime(dividend_df['execution_date'])


# define calculation
def get_total_dividend(df, years, col):
    start_date = current_date - relativedelta(years=years)
    # 筛选在时间窗口内的记录
    mask = (df['execution_date'] >= start_date) & (df['execution_date'] <= current_date)
    # 按 symbol 分组求和
    return df[mask].groupby('symbol')[col].sum()


# calcuate
yield_1y = get_total_dividend(dividend_df, 1, 'cash_dividend_yield').rename('total_dividend_yield_1Y').reset_index()
yield_3y = get_total_dividend(dividend_df, 3, 'cash_dividend_yield').rename('total_dividend_yield_3Y').reset_index()
yield_5y = get_total_dividend(dividend_df, 5, 'cash_dividend_yield').rename('total_dividend_yield_5Y').reset_index()
dividend_1y = get_total_dividend(dividend_df, 1, 'cash_dividend').rename('total_dividend_1Y').reset_index()
dividend_3y = get_total_dividend(dividend_df, 3, 'cash_dividend').rename('total_dividend_3Y').reset_index()
dividend_5y = get_total_dividend(dividend_df, 5, 'cash_dividend').rename('total_dividend_5Y').reset_index()

# combine
result = yield_1y.merge(yield_3y, on=['symbol'], how='outer').merge(yield_5y, on=['symbol'], how='outer') \
    .merge(dividend_1y, on=['symbol'], how='outer').merge(dividend_3y, on=['symbol'], how='outer').merge(dividend_5y,
                                                                                                         on=['symbol'],
                                                                                                         how='outer')
# result = pd.concat([yield_1y, yield_3y, yield_5y], axis=1).fillna(0)
result.to_csv(f'{PROGRAM_PATH}/stock_cash_dividend_yield_by_periods.csv', encoding='utf-8', index=False)
