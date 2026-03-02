"""
unused
Extract board constituent stock data from akshare.
Saves one CSV per board in data_other/board_industry/ with columns: board_name, symbol.
"""
import os
import random
import re
import time
import logging

import pandas as pd
import akshare as ak

from utils.common import format_stock_symbol
from utils.constants import PROJECT_PATH

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)
PROGRAM_PATH = os.path.join(PROJECT_PATH, "data/macro")


def sanitize_filename(name: str) -> str:
    """Replace invalid filename chars with underscore."""
    return re.sub(r'[<>:"/\\|?*]', "_", name)

# macro_dict = {
#     'm2''macro_china_m2_yearly',
#     'bone_yield':'bond_china_yield',
#
#
# }

def main():
    os.makedirs(PROGRAM_PATH, exist_ok=True)
    m2_df = ak.macro_china_m2_yearly()
    if m2_df is None or m2_df.empty:
        logging.warning(f"No data for M2")
    else:
        out_path = f'{PROGRAM_PATH}/m2.csv'
        m2_df.to_csv(f'{PROGRAM_PATH}/m2.csv', index=False, encoding="utf-8")
        logging.info(f"Saved {len(m2_df)} rows to {out_path}")

    random_sleep = random.randint(2, 30)
    time.sleep(random_sleep)


if __name__ == "__main__":
    main()
#
# # M2
# m2 = ak.macro_china_m2()
# print(m2.tail())
#
# # 获取国债收益率曲线数据
# bond_yield_df = ak.bond_china_yield()
#
# # 筛选10年和1年期国债收益率， e.g. ten_year_col = [col for col in bond_yield_df.columns if "10年" in col][0]
# # calculate bond rate for 10 year minus bond rate for 1 year as long vs short term bond rate diff
#
# deposit_survey_df = ak.macro_china_deposit_survey()
#
#
# # 获取PMI数据
# pmi = ak.macro_china_pmi()
# print(pmi.tail())
#
# # 获取消费者信心指数
# consumer_confidence = ak.macro_china_consumer_confidence()
# print(consumer_confidence.tail())
#
# # 获取房屋新开工面积
# housing = ak.macro_china_housing_start()