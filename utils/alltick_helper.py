import requests
import os

import json
from urllib.parse import quote
from datetime import datetime
import pandas as pd
from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path

def calculate_ndays(
    start_date_str: str,
    end_date_str: str
) -> int:
    '''
    get number of days between 2 dates string.
    We use this to get stock history with the AllTick API.
    '''
    start = datetime.strptime(start_date_str, '%Y-%m-%d')
    end = datetime.strptime(end_date_str, '%Y-%m-%d')
    return (end - start).days + 1

def get_single_stock_price_hist(stock_symbol, end_date, num_days):
    # 1. 定义请求参数的Python字典结构
    request_payload = {
        "trace": "my_test_trace_id",
        "data": {
            "code": stock_symbol,
            "kline_type": 8,
            # 1是1分钟K，2是5分钟K，3是15分钟K，4是30分钟K，5是小时K，6是2小时K(股票不支持2小时)，7是4小时K(股票不支持4小时)，8是日K，9是周K，10是月K （注：股票不支持2小时K、4小时K）
            # 从指定时间往前查询K线
            # 1、传0表示从当前最新的交易日往前查k线
            # 2、指定时间请传时间戳，传时间戳表示从该时间戳往前查k线
            # 3、只有外汇贵金属加密货币支持传时间戳，股票类的code不支持
            "kline_timestamp_end": end_date,
            "query_kline_num": num_days,
            "adjust_type": 0
        }
    }

    # 2. 将字典编码为URL安全的JSON字符串
    query_param = quote(json.dumps(request_payload))

    # 3. 构造完整的请求URL
    base_url = 'https://quote.alltick.co/quote-stock-b-api/kline'
    token = os.environ['ALLTICK_API_KEY']
    final_url = f'{base_url}?token={token}&query={query_param}'

    # 4. 发送GET请求
    try:
        response = requests.get(final_url)
        response.raise_for_status()  # 检查HTTP错误
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return
    return data




def process_single_stock_price_hist(stock_symbol_str, alltick_api_result):
    data_df = pd.DataFrame(alltick_api_result['data']['kline_list'])
    # convert timestamp to date
    data_df.timestamp = data_df.timestamp.astype(int)
    data_df['date'] = data_df.timestamp.map(lambda x: datetime.fromtimestamp(x))
    # convert columns
    data_df = data_df.rename(
        columns={'open_price': 'open', 'close_price': 'close', 'high_price': 'high', 'low_price': 'low'})
    data_df = data_df[['date', 'open', 'high', 'low', 'close', 'volume']]
    # add split and dividend column
    data_df['split'] = 1
    data_df['dividend'] = 0
    # save to csv
    data_df.to_csv(f"zipline_data/daily/{stock_symbol_str}.csv", index=False, encoding='utf-8')
    return data_df
def get_batch_kline(stock_symbol, end_date, num_days):
    # 1. 定义请求参数的Python字典结构
    request_payload = {
        "trace": "batch_kline",
        "data": {
            "code": stock_symbol,
            "kline_type": 8,
            # 1是1分钟K，2是5分钟K，3是15分钟K，4是30分钟K，5是小时K，6是2小时K(股票不支持2小时)，7是4小时K(股票不支持4小时)，8是日K，9是周K，10是月K （注：股票不支持2小时K、4小时K）
            # 从指定时间往前查询K线
            # 1、传0表示从当前最新的交易日往前查k线
            # 2、指定时间请传时间戳，传时间戳表示从该时间戳往前查k线
            # 3、只有外汇贵金属加密货币支持传时间戳，股票类的code不支持
            "kline_timestamp_end": end_date,
            "query_kline_num": num_days,
            "adjust_type": 0
        }
    }

    # 2. 将字典编码为URL安全的JSON字符串
    query_param = quote(json.dumps(request_payload))

    # 3. 构造完整的请求URL
    base_url = 'https://quote.alltick.co/quote-stock-b-api/kline'
    token = os.environ['ALLTICK_API_KEY']
    final_url = f'{base_url}?token={token}&query={query_param}'

    # 4. 发送GET请求
    try:
        response = requests.get(final_url)
        response.raise_for_status()  # 检查HTTP错误
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return
    return data

def get_all_stock_list():
    '''
    Get the list of stocks that cant be downloaded from ALLTICK API.
    The file is provided by Alltick: https://docs.google.com/spreadsheets/d/1avkeR1heZSj6gXIkDeBt8X3nv4EzJetw4yFuKjSDYtA/edit?gid=1702052913#gid=1702052913
    https://github.com/alltick/alltick-realtime-forex-crypto-stock-tick-finance-websocket-api/blob/main/access_guide_cn.md
    :return: list
    '''
    alltick_stock_df = pd.read_excel(
        '/Users/yilin/Documents/Projects/stock_analysis/data/Alltick Supported products(产品列表).xlsx', header=1,
        sheet_name='A股code（China stocks code）')
    alltick_stock_df = alltick_stock_df.loc[
        pd.notna(alltick_stock_df['Code']) & alltick_stock_df['Code'].str.startswith(('3', '0', '6'))].copy()
    alltick_stock_df_list = alltick_stock_df.Code.unique().tolist()
    return alltick_stock_df_list
def get_extracted_stock_list(path_to_stock_csv_folder):
    path_to_stock_csv = '/Users/yilin/Documents/Projects/stock_analysis/zipline_data/daily'
    file_list = get_file_paths_pathlib(path_to_stock_csv_folder)
    extracted_stock_list = [extract_stock_symbol_from_path(i, from_format='number_MARKET', to_format='number.MARKET') for i in file_list]
    return extracted_stock_list