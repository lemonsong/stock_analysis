import os, random, time
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd
import akshare as ak
from config import PROJECT_PATH

PROGRAM_PATH = f'{PROJECT_PATH}/data_ak_fundamental/single_file/'

########### TODO: define stock_li to fetch fundamental data ##########
decision_df = pd.read_csv(f'{PROJECT_PATH}/0decision.csv')
# critical_df = decision_df.loc[decision_df.overall_signal_count==1].copy()
critical_df = decision_df.head(30).copy()

logging.info(f"{critical_df=}")

# stock list to fetch fundamentals data
stock_li = critical_df.symbol.tolist()
# stock_li = ['SZ000573']
###########################
# DONE level 1: get file name all files in the path. only fetch and write data if the symbol_xx_sheet not existed.
# TODO level 2(in 2026): get file name all files in the path. if symbol existed, only update the files if the report date is 2 year from April of current year(TBD) .
for stock_symbol in stock_li:
    random_sleep_time = random.randint(11, 30)
    # balance sheet
    logging.info(f"Fetching balance sheet of {stock_symbol}")
    path_to_balance = f'{PROGRAM_PATH}/{stock_symbol}_balance.csv'
    if os.path.isfile(path_to_balance):
        logging.info(f"Balance sheet of {stock_symbol} existed")
        continue
    else:
        stock_balance_sheet_by_yearly_em_df = ak.stock_balance_sheet_by_yearly_em(symbol=stock_symbol)
        stock_balance_sheet_by_yearly_em_df.to_csv(path_to_balance,
                                                   encoding='utf-8',
                                                   index=False)
        logging.info(f"Balance sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
    time.sleep(random_sleep_time)
    # profit sheet
    logging.info(f"Fetching profit sheet of {stock_symbol}")
    path_to_profit = f'{PROGRAM_PATH}/{stock_symbol}_profit.csv'

    if os.path.isfile(path_to_profit):
        logging.info(f"Profit sheet of {stock_symbol} existed")
        continue
    else:
        stock_profit_sheet_by_yearly_em = ak.stock_profit_sheet_by_yearly_em(symbol=stock_symbol)
        stock_profit_sheet_by_yearly_em.to_csv(path_to_profit,
                                               encoding='utf-8',
                                               index=False)
        logging.info(f"Profit sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
    time.sleep(random_sleep_time)
    # cash flow sheet
    logging.info(f"Fetching cash flow sheet of {stock_symbol}")
    path_to_cash_flow = f'{PROGRAM_PATH}/{stock_symbol}_cash_flow.csv'
    if os.path.isfile(path_to_cash_flow):
        logging.info(f"Cash flow sheet of {stock_symbol} existed")
        continue
    else:
        stock_cash_flow_sheet_by_yearly_em = ak.stock_cash_flow_sheet_by_yearly_em(symbol=stock_symbol)
        stock_cash_flow_sheet_by_yearly_em.to_csv(path_to_cash_flow,
                                                  encoding='utf-8',
                                                  index=False)
        logging.info(f"Cash flow sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
    # # Profile
    # # TODO: profile data is empty. Change to other API
    # logging.info(f"Fetching profile sheet of {stock_symbol}")
    # path_to_profile = f'{PROGRAM_PATH}/{stock_symbol}_profile.csv'
    # if os.path.isfile(path_to_profile):
    #     logging.info(f"Profile of {stock_symbol} existed")
    #     continue
    # else:
    #     stock_profile_cninfo_df = ak.stock_profile_cninfo(symbol=stock_symbol)
    #     stock_profile_cninfo_df.to_csv(path_to_profile,
    #                                               encoding='utf-8',
    #                                               index=False)
    #     logging.info(f"Profile of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")




