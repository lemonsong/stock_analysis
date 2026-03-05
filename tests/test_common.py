import pytest
from utils.common import format_stock_symbol, extract_stock_symbol_from_path

def test_format_stock_symbol_market_number():
    assert format_stock_symbol('SH600000', 'MARKETnumber', 'MARKETnumber') == 'SH600000'
    assert format_stock_symbol('SH600000', 'MARKETnumber', 'number.MARKET') == '600000.SH'
    assert format_stock_symbol('SH600000', 'MARKETnumber', 'number') == '600000'
    with pytest.raises(SystemExit):
        format_stock_symbol('SH600000', 'MARKETnumber', 'invalid')

def test_format_stock_symbol_number_market():
    assert format_stock_symbol('600000_SH', 'number_MARKET', 'number.MARKET') == '600000.SH'
    assert format_stock_symbol('600000_SH', 'number_MARKET', 'MARKETnumber') == 'SH600000'
    with pytest.raises(SystemExit):
        format_stock_symbol('600000_SH', 'number_MARKET', 'invalid')

def test_format_stock_symbol_number_dot_market():
    assert format_stock_symbol('600000.SH', 'number.MARKET', 'MARKETnumber') == 'SH600000'
    with pytest.raises(SystemExit):
        format_stock_symbol('600000.SH', 'number.MARKET', 'invalid')

def test_format_stock_symbol_market_number_xxx():
    assert format_stock_symbol('SH600000_something', 'MARKETnumber_xxx', 'MARKETnumber') == 'SH600000'
    with pytest.raises(SystemExit):
        format_stock_symbol('SH600000_something', 'MARKETnumber_xxx', 'invalid')

def test_format_stock_symbol_number():
    # SH
    assert format_stock_symbol('600000', 'number', 'MARKETnumber') == 'SH600000'
    assert format_stock_symbol('900999', 'number', 'MARKETnumber') == 'SH900999'
    # SZ
    assert format_stock_symbol('000001', 'number', 'MARKETnumber') == 'SZ000001'
    assert format_stock_symbol('300001', 'number', 'MARKETnumber') == 'SZ300001'
    assert format_stock_symbol('200001', 'number', 'MARKETnumber') == 'SZ200001'
    # BJ
    assert format_stock_symbol('430001', 'number', 'MARKETnumber') == 'BJ430001'
    assert format_stock_symbol('830001', 'number', 'MARKETnumber') == 'BJ830001'
    assert format_stock_symbol('920001', 'number', 'MARKETnumber') == 'BJ920001'
    # Other
    assert format_stock_symbol('123456', 'number', 'MARKETnumber') == '123456'

    with pytest.raises(SystemExit):
        format_stock_symbol('600000', 'number', 'invalid')

def test_format_stock_symbol_invalid_from_format():
    with pytest.raises(SystemExit):
        format_stock_symbol('600000', 'invalid', 'MARKETnumber')

def test_extract_stock_symbol_from_path():
    assert extract_stock_symbol_from_path('/path/to/SH600000.csv', 'MARKETnumber', 'MARKETnumber') == 'SH600000'
    assert extract_stock_symbol_from_path('SH600000.csv', 'MARKETnumber', 'number.MARKET') == '600000.SH'
    assert extract_stock_symbol_from_path('/some/dir/600000_SH.csv', 'number_MARKET', 'MARKETnumber') == 'SH600000'
    assert extract_stock_symbol_from_path('600000_SH.csv', 'number_MARKET', 'number_MARKET') == '600000_SH'
