import pandas as pd
import pytest
from utils.tushare_helper import format_tushare_kline_to_dolt_style

def test_format_tushare_kline_to_dolt_style():
    # Create sample tushare kline dataframe
    data = {
        'ts_code': ['000001.SZ', '600000.SH'],
        'trade_date': ['20230101', '20230102'],
        'open': [10.0, 20.0],
        'high': [11.0, 21.0],
        'low': [9.0, 19.0],
        'close': [10.5, 20.5],
        'pre_close': [10.0, 20.0],
        'change': [0.5, 0.5],
        'pct_chg': [5.0, 2.5],
        'vol': [1000, 2000],
        'amount': [10500, 41000]
    }
    df = pd.DataFrame(data)

    # Call the function
    result_df = format_tushare_kline_to_dolt_style(df.copy())

    # Verify column renames
    assert 'adjclose' in result_df.columns
    assert 'volume' in result_df.columns
    assert 'pre_close' not in result_df.columns
    assert 'vol' not in result_df.columns

    # Verify column drops
    assert 'ts_code' not in result_df.columns
    assert 'trade_date' not in result_df.columns
    assert 'change' not in result_df.columns
    assert 'pct_chg' not in result_df.columns

    # Verify symbol formatting and insertion at index 0
    assert result_df.columns[0] == 'symbol'
    assert result_df['symbol'].iloc[0] == 'SZ000001'
    assert result_df['symbol'].iloc[1] == 'SH600000'

    # Verify date formatting
    assert 'date' in result_df.columns
    assert result_df['date'].iloc[0] == '2023-01-01'
    assert result_df['date'].iloc[1] == '2023-01-02'

    # Verify data integrity for other columns
    assert result_df['open'].iloc[0] == 10.0
    assert result_df['adjclose'].iloc[0] == 10.0
    assert result_df['volume'].iloc[0] == 1000
