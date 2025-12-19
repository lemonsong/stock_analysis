import os, random, time
import logging

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,  # DEBUG,INFO,WARNING, ERROR, CRITICAL
)

import akshare as ak
import pandas as pd
from config import PROJECT_PATH

PROGRAM_PATH = f'{PROJECT_PATH}/data_ak_fund_detail_info_xq'
all_fund_df = pd.read_csv(f'{PROJECT_PATH}/0_all_fund.csv',
                          dtype={'基金代码': str} # read fund symbol as string
                          )
# subset of funds to fetch info
## TODO: change when needed
fetch_fund_df = all_fund_df.loc[(all_fund_df['基金类型'].str.contains('债券'))
                        & (all_fund_df['基金简称'].str.contains("易方达|南方|招商"))
                        & (all_fund_df['基金简称'].str.contains("A"))]
fetch_fund_li = fetch_fund_df['基金代码'].unique().tolist()
logging.info(f"Funds to fetch: {fetch_fund_li=}")

# fetch funds info
for fund in fetch_fund_li:
    random_sleep_time = random.randint(11, 30)
    # balance sheet
    logging.info(f"Fetching detailed info of {fund}")
    path_to_fund = f'{PROGRAM_PATH}/{fund}.csv'
    if os.path.isfile(path_to_fund):
        logging.info(f"Balance sheet of {fund} existed")
        continue
    else:
        fund_individual_detail_info_xq_df = ak.fund_individual_detail_info_xq(symbol=f"{fund}")
        fund_individual_detail_info_xq_df.to_csv(path_to_fund,
                                                   encoding='utf-8',
                                                   index=False)
        logging.info(f"Detailed info of {fund} saved. Sleep for {random_sleep_time}s ...")
    time.sleep(random_sleep_time)

