import pandas as pd
from utils.common import format_stock_symbol
from datetime import datetime

def format_tushare_kline_to_dolt_style(kline_df):
    # convert kline_df so that it is similar to the data from extracted from dolt
    kline_df['date'] = kline_df['trade_date'].map(lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%Y-%m-%d'))
    kline_df = kline_df.rename(columns={'pre_close': 'adjclose',
                                        'vol': 'volume'})
    kline_df.insert(0, 'symbol',
                            kline_df.ts_code.map(
            lambda x: format_stock_symbol(x, from_format='number.MARKET', to_format='MARKETnumber')
                            )
                            )
    kline_df = kline_df.drop(['ts_code','trade_date','change','pct_chg'], axis=1)
    return kline_df