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
from utils.constants import PROJECT_PATH

import argparse, sys

########### define stock_li to fetch fundamental data_all_list ##########
if len(sys.argv) > 1:
    # read arguments: choice_overall_signal_count, choice_industry_category_name, choice_industry_type_name, choice_row_range
    parser = argparse.ArgumentParser()
    parser.add_argument('--choice_overall_signal_count', type=str, required=True)
    parser.add_argument('--choice_industry_category_name', type=str, required=True)
    parser.add_argument('--choice_industry_sub_category_name', type=str, required=True)
    parser.add_argument('--choice_industry_type_name', type=str, required=True)
    parser.add_argument('--choice_row_range', type=str, required=True)
    parser.add_argument('--boards_regex', type=str, required=False)
    parser.add_argument('--text_stock_list', type=str, required=True)

    args = parser.parse_args()
    logging.info(f"Get data for:")
    logging.info(f"Buy/Sell Signal Count: {args.choice_overall_signal_count}")
    logging.info(f"Industry Category: {args.choice_industry_category_name}")
    logging.info(f"Industry Sub Category: {args.choice_industry_sub_category_name}")
    logging.info(f"Industry Type: {args.choice_industry_type_name}")
    logging.info(f"Boards regex: {args.boards_regex}")
    logging.info(f"Row Range: {args.choice_row_range}")
    logging.info(f"Customized Stock List: {args.text_stock_list}")
    # filter data based on arguments
    if len(args.text_stock_list) == 0:
        logging.info(f"Fetching data via field&value filter")
        stock_filtered_df = pd.read_csv(f"{PROJECT_PATH}/data/dwa/app_decision.csv")
        if args.choice_overall_signal_count != 'All':
            stock_filtered_df = stock_filtered_df.loc[
                stock_filtered_df.overall_signal_count == int(args.choice_overall_signal_count)]
        if args.choice_industry_category_name != 'All':
            stock_filtered_df = stock_filtered_df.loc[
                stock_filtered_df.industry_category_name == args.choice_industry_category_name]
        if args.choice_industry_sub_category_name != 'All':
            stock_filtered_df = stock_filtered_df.loc[
                stock_filtered_df.industry_sub_category_name == args.choice_industry_sub_category_name]
        if args.choice_industry_type_name != 'All':
            stock_filtered_df = stock_filtered_df.loc[
                stock_filtered_df.industry_type_name == args.choice_industry_type_name]
        if args.boards_regex.strip():
            stock_filtered_df = stock_filtered_df[
                stock_filtered_df["boards"].astype(str).str.contains(
                    args.boards_regex, case=False, na=False, regex=True
                )
            ]
        if args.choice_row_range != 'All':
            row_range = [int(item) for item in args.choice_row_range.split('-')]
            stock_filtered_df = stock_filtered_df.iloc[row_range[0]:row_range[1],:]
        stock_li = stock_filtered_df.symbol.tolist()
    else:
        logging.info(f"Fetching data via customized stock list")
        stock_li = [item.strip() for item in args.text_stock_list.split(',')]
else:
    logging.info(f"Fetching data via manual input")

    # decision_df = pd.read_csv(f'{PROJECT_PATH}/data/dwa/kline_analysis.csv')
    # critical_df = decision_df.loc[decision_df.overall_signal_count == 1].head(160).copy()
    # stock_li = critical_df.symbol.tolist()

    # OR

    # decision_df = pd.read_csv(f'{PROJECT_PATH}/data/dwa/kline_analysis.csv')
    # critical_df = decision_df.iloc[400:450,:].copy()
    # stock_li = critical_df.symbol.tolist()

    # stock list to fetch fundamentals data_all_list
    # stock_li = ['SZ300377','SZ300468']
    # stock_li = ['SH603309','SZ300097','SZ002209']
    # stock_li = ['SZ000070','SH603042']
    # stock_li = ['SZ002105','SH605001']
    # stock_li = ['SZ002555','SZ002315','SH603444','SZ300533','SH601360']
    # stock_li = ['SH600901','SH601077','SH601318','SZ002142','SH601555']
    stock_li = ['SZ000410']
logging.info(f"{stock_li=}")


PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_fundamental/single_file/'

###########################
# DONE level 1: get file name all files in the path. only fetch and write data_all_list if the symbol_xx_sheet not existed.
# TODO level 2(in 2026): get file name all files in the path. if symbol existed, only update the files if the report date is 2 year from April of current year(TBD) .
for index, stock_symbol in enumerate(stock_li):
    random_sleep_time = random.randint(11, 30)
    # balance sheet
    logging.info(f"Fetching balance sheet of {index}: {stock_symbol}")
    path_to_balance = f'{PROGRAM_PATH}/{stock_symbol}_balance.csv'
    if os.path.isfile(path_to_balance):
        logging.info(f"Balance sheet of {stock_symbol} existed")
        # continue
    else:
        stock_balance_sheet_by_yearly_em_df = ak.stock_balance_sheet_by_yearly_em(symbol=stock_symbol)
        stock_balance_sheet_by_yearly_em_df.to_csv(path_to_balance,
                                                   encoding='utf-8',
                                                   index=False)
        logging.info(f"Balance sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
        time.sleep(random_sleep_time)
    # profit sheet
    logging.info(f"Fetching profit sheet of {index}: {stock_symbol}")
    path_to_profit = f'{PROGRAM_PATH}/{stock_symbol}_profit.csv'
    if os.path.isfile(path_to_profit):
        logging.info(f"Profit sheet of {stock_symbol} existed")
        # continue
    else:
        stock_profit_sheet_by_yearly_em = ak.stock_profit_sheet_by_yearly_em(symbol=stock_symbol)
        stock_profit_sheet_by_yearly_em.to_csv(path_to_profit,
                                               encoding='utf-8',
                                               index=False)
        logging.info(f"Profit sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
        time.sleep(random_sleep_time)
    # cash flow sheet
    logging.info(f"Fetching cash flow sheet of {index}: {stock_symbol}")
    path_to_cash_flow = f'{PROGRAM_PATH}/{stock_symbol}_cash_flow.csv'
    if os.path.isfile(path_to_cash_flow):
        logging.info(f"Cash flow sheet of {stock_symbol} existed")
        # continue
    else:
        stock_cash_flow_sheet_by_yearly_em = ak.stock_cash_flow_sheet_by_yearly_em(symbol=stock_symbol)
        stock_cash_flow_sheet_by_yearly_em.to_csv(path_to_cash_flow,
                                                  encoding='utf-8',
                                                  index=False)
        logging.info(f"Cash flow sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
        time.sleep(random_sleep_time)
    # # Profile
    # # TODO: profile data_all_list is empty. Change to other API
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




