'''
1. convert the fund info CSV in data_ak_fund_detail_info_xq folder
2. merge all converted info into one 0_all_fund_info.csv to compare fee schedule
'''
import os, random, time
import logging

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,  # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path

import pandas as pd
from utils.constants import PROJECT_PATH

PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_fund_info'
file_list = get_file_paths_pathlib(f'{PROGRAM_PATH}/single')
fund_symbol_li = [extract_stock_symbol_from_path(i, from_format='number', to_format='number')
                   for i in file_list]
logging.info(f'{fund_symbol_li=}')

# TODO: continue aggregate detail into a large csv
def agg_fund_detail(symbol, df_original):
    # 2. 计算买入规则字典
    buy_dict = df_original[df_original["费用类型"] == "买入规则"].set_index("条件或名称")["费用"].to_dict()

    # 3. 计算卖出规则字典
    sell_dict = df_original[df_original["费用类型"] == "卖出规则"].set_index("条件或名称")["费用"].to_dict()

    # 4. 计算其他费用之和
    other_dict = df_original[~df_original["费用类型"].isin(["买入规则", "卖出规则"])].set_index("条件或名称")[
        "费用"].to_dict()

    other_fees_sum = df_original[df_original["费用类型"] == "其他费用"]["费用"].sum()

    # 5. 生成新的 DataFrame
    new_data = {
        "规则": ["买入规则", "卖出规则",
                 "其他",
                 "其他费用之和"],
        "费用": [buy_dict, sell_dict,
                 other_dict,
                 round(other_fees_sum, 2)]
    }

    df_result = pd.DataFrame(new_data).set_index('规则').T

    # 3. (可选) 如果不需要保留“费用”这个行索引名，可以清空它
    df_result.index.name = None
    df_result["基金代码"] = symbol
    return df_result

funds_df = pd.DataFrame()
for symbol in fund_symbol_li:
    fund_df = pd.read_csv(f"{PROGRAM_PATH}/single/{symbol}.csv")
    fund_df_converted = agg_fund_detail(symbol, fund_df)
    funds_df = pd.concat([funds_df, fund_df_converted], axis=0, ignore_index=True)

all_fund_df = pd.read_csv(f'{PROGRAM_PATH}/0_all_fund.csv', dtype={'基金代码': str})
funds_df = all_fund_df.merge(funds_df, on ="基金代码", how='inner')
funds_df = funds_df.sort_values(by=["基金类型", "其他费用之和"], ascending=True)
funds_df.to_csv(f'{PROGRAM_PATH}/0_all_fund_info.csv', index='utf-8', encoding=False)






# a = agg_fund_detail(fund_individual_detail_info_xq_df)