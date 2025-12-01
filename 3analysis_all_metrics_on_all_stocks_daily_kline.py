'''
Calculate the metrics for the latest date of daily stock data to get buy/sell signal
'''
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from stockstats import StockDataFrame as sdf

from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path, get_today_date_string
from utils.alltick_helper import (calculate_ndays, get_single_stock_price_hist,
                                  process_single_stock_price_hist, get_all_stock_list,
                                  get_extracted_stock_list)

# path_to_stock_csv = '/Users/yilin/Documents/Projects/stock_analysis/zipline_data/daily'
# path_to_stock_csv = '/Users/yilin/Documents/Projects/stock_analysis/zipline_data/daily_test'
# path_to_stock_csv = '/Users/yilin/Documents/Projects/stock_analysis/data_dolt/daily'
path_to_stock_csv = '/Users/yilin/Documents/Projects/stock_analysis/data_tushare/daily'



file_list = get_file_paths_pathlib(path_to_stock_csv)
close_col = 'close' # TODO: close or adjclose
def get_stock_metrics(stock_df, stock_symbol):
    stock = sdf.retype(stock_df[["date", "open", "high", "low", "close"]])

    stock_metrics = stock.get(
        ["close", 'rsi_12', "boll", "boll_ub", "boll_lb", "close_xu_close_20_sma", 'close_10_sma_xd_close_50_sma',
         'macds', 'macd', 'trix', 'wr_6'])

    stock_metrics['rolling_mean_short_term'] = stock_metrics['close'].ewm(span=5, adjust=True, ignore_na=True).mean()
    stock_metrics['rolling_mean_long_term'] = stock_metrics['close'].ewm(span=30, adjust=True, ignore_na=True).mean()

    # only need to use the last 2 rows to decide buy to sell
    stock_metrics = stock_metrics.iloc[-2:, :]

    stock_metrics['macd_buy'] = (stock_metrics.macd>stock_metrics.macds) & (stock_metrics.macd.shift(1)<=stock_metrics.macds.shift(1))
    stock_metrics['macd_sell']= (stock_metrics.macd<stock_metrics.macds) & (stock_metrics.macd.shift(1)>=stock_metrics.macds.shift(1))

    stock_metrics['rsi_plus_macd_buy'] = (stock_metrics.rsi_12 < 50) & (stock_metrics.macd > stock_metrics.macds) & (
                stock_metrics.macd.shift(1) <= stock_metrics.macds.shift(1))
    stock_metrics['rsi_plus_macd_sell'] = (stock_metrics.rsi_12 > 50) & (stock_metrics.macd < stock_metrics.macds) & (
                stock_metrics.macd.shift(1) >= stock_metrics.macds.shift(1))

    stock_metrics['trix_buy'] = (stock_metrics.trix < 0) & (
            stock_metrics.trix.shift(1) > 0)
    stock_metrics['trix_sell'] = (stock_metrics.trix > 0) & (
                stock_metrics.trix.shift(1) < 0)

    stock_metrics = stock_metrics.iloc[-1:, :]
    stock_metrics['symbol'] = stock_symbol
    return stock_metrics

all_stock_metrics = pd.DataFrame()

for file_path in file_list:
    stock_symbol = extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',to_format='MARKETnumber') # use MARKETnumber for data_dolt, number_MARKET for zipline_data folder
    stock_df = pd.read_csv(file_path)
    stock_df = stock_df[["date", "open", "high", "low", close_col]]
    stock_df = stock_df.rename(columns={close_col: 'close'})
    stock_metrics = get_stock_metrics(stock_df, stock_symbol)
    all_stock_metrics = pd.concat([all_stock_metrics, stock_metrics], axis=0, ignore_index=True)

col = all_stock_metrics.pop('symbol')
all_stock_metrics.insert(0, 'symbol', col)

# strategy use one row to decide buy or sell
all_stock_metrics['rsi_less_than_10'] = all_stock_metrics.rsi_12<10
all_stock_metrics['rsi_more_than_90'] = all_stock_metrics.rsi_12>90
all_stock_metrics['close_less_than_boll_lb'] = all_stock_metrics.close < all_stock_metrics.boll_lb
all_stock_metrics['close_more_than_boll_ub'] = all_stock_metrics.close > all_stock_metrics.boll_ub
all_stock_metrics['wr_less_than_10'] = all_stock_metrics.wr_6<10
all_stock_metrics['wr_more_than_90'] = all_stock_metrics.wr_6>90
all_stock_metrics['ewm_short_term_more_than_long_term'] = all_stock_metrics.rolling_mean_short_term>all_stock_metrics.rolling_mean_long_term
all_stock_metrics['ewm_short_term_less_than_long_term'] = all_stock_metrics.rolling_mean_short_term<=all_stock_metrics.rolling_mean_long_term



buy_signal_cols = ['rsi_less_than_10','close_less_than_boll_lb',
                   'ewm_short_term_more_than_long_term', 'macd_buy', 'rsi_plus_macd_buy',
                   'trix_buy', 'wr_more_than_90']
sell_signal_cols = ['rsi_more_than_90', 'close_more_than_boll_ub',
                    'ewm_short_term_less_than_long_term', 'macd_sell', 'rsi_plus_macd_sell',
                    'trix_sell','wr_less_than_10']

all_stock_metrics['buy_signal_count'] = all_stock_metrics[buy_signal_cols].sum(axis=1)
all_stock_metrics['sell_signal_count'] = all_stock_metrics[sell_signal_cols].sum(axis=1)
all_stock_metrics['overall_signal_count'] = all_stock_metrics['buy_signal_count'] - all_stock_metrics['sell_signal_count']


all_stock_metrics = all_stock_metrics.sort_values('overall_signal_count', ascending=False)
all_stock_metrics.to_csv('/Users/yilin/Documents/Projects/stock_analysis/0decision.csv', index=False, encoding='utf-8')