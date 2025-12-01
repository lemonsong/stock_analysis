'''
concatenate
'''
import sys
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd

from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path
from config import PROJECT_PATH
from functools import reduce
from utils.constants import AK_FUNDAMENTAL_KEEP_COMMON_COLS

# TODO: iterate code to handle the situation that one stock only have one sheet, e.g. balance sheet rather than all the 3, becuase the extract step we teminated manually

PROGRAM_PATH = f'{PROJECT_PATH}/data_ak_fundamental'
### get stock symbols ###
# get list of fundamental sheets files
fundamental_yearly_files = get_file_paths_pathlib(f'{PROGRAM_PATH}/single_file')
# extract stock symbol from fundamental_yearly_files
stock_symbol_li_containing_duplicates = [extract_stock_symbol_from_path(i, from_format='MARKETnumber_xxx', to_format = 'MARKETnumber')
                   for i in fundamental_yearly_files]
# as one stock symbol has 3 files, we need to drop duplicates from the stock_symbol_li_containing_duplicates
stock_symbol_li = list(set(stock_symbol_li_containing_duplicates))
del stock_symbol_li_containing_duplicates

### define important fundamental sheets columns ###
all_common_cols =['SECUCODE',	'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'ORG_CODE', 'ORG_TYPE', 'REPORT_DATE', 'REPORT_TYPE', 'REPORT_DATE_NAME', 'SECURITY_TYPE_CODE', 'NOTICE_DATE', 'UPDATE_DATE', 'CURRENCY',
                    'OPINION_TYPE', 'OSOPINION_TYPE']
# the following is the common columns to keep:
fundamental_types = ['balance', 'profit', 'cash_flow']

drop_cols = [i for i in all_common_cols if i not in AK_FUNDAMENTAL_KEEP_COMMON_COLS]
drop_cols_balance = drop_cols + ['UNCONFIRM_INVEST_LOSS', 'UNCONFIRM_INVEST_LOSS_YOY',
                                 'OTHER_COMPRE_INCOME','OTHER_COMPRE_INCOME_YOY',
                                 'CONVERT_DIFF','CONVERT_DIFF_YOY']
drop_cols_profit = drop_cols
drop_cols_cash_flow = drop_cols + ['MINORITY_INTEREST', 'MINORITY_INTEREST_YOY',
                                   'NETPROFIT','NETPROFIT_YOY',
                                   'FINANCE_EXPENSE','FINANCE_EXPENSE_YOY']

# "BOND_PAYABLE",  "DEFER_INCOME_1YEAR"

### get fundamental_df for all sheets in the data_ak_fundamental/single_file folder ###
# merge fundamental files
fundamental_df = pd.DataFrame()
for stock_symbol in stock_symbol_li:
    # read 3 types of fundamental data as balance_df, profit_df and cash_flow_df
    for sheet in fundamental_types:
        logging.info(f'get {sheet} sheet for {stock_symbol}')
        exec(f"""{sheet}_df = pd.read_csv('{PROGRAM_PATH}/single_file/{stock_symbol}_{sheet}.csv')""")
        # only keep the dataframe with column list: AK_FUNDAMENTAL_KEEP_COMMON_COLS and sheet_cols
        # exec(
        #     f"""{sheet}_df = {sheet}_df[AK_FUNDAMENTAL_KEEP_COMMON_COLS+{sheet}_cols].copy()"""
        # )
        exec(
            f"""{sheet}_df = {sheet}_df[[i for i in {sheet}_df.columns.tolist() if i not in drop_cols_{sheet}]].copy()"""
        )
    # merge 3 fundamental data into 1
    stock_df = reduce(lambda left, right: pd.merge(left, right, on=AK_FUNDAMENTAL_KEEP_COMMON_COLS, how='inner'), [balance_df, profit_df, cash_flow_df])
    # insert stock symbol to the first column
    stock_df.insert(0, 'symbol', stock_symbol)
    # concatenate the new stock's fundamental data to the existed fundatmentals data
    fundamental_df = pd.concat([fundamental_df, stock_df], axis=0, ignore_index=True)

# check whether any column existed in 2 sheets causing the creation of xxx_y columns after merge
for col in fundamental_df.columns:
    if '_y' in col:
        logging.critical(f'please check which 2 sheets contain {col}, and decide whether need to keep both')
        sys.exit(1)

### save to CSV ###
# saved the merged then concatenated fundamental dataframe
fundamental_df.to_csv(f'{PROGRAM_PATH}/fundamental.csv', index=False, encoding='utf-8')
logging.info(fundamental_df.shape)
# check_cols = fundamental_df.columns.tolist()

# remove the much keep columns. These are columns used on financial metrics calculation
keep_cols = ['BOND_PAYABLE', 'DEFER_INCOME_1YEAR','FE_INTEREST_EXPENSE', "FA_IR_DEPR", "OILGAS_BIOLOGY_DEPR", "IR_DEPR"
                        , "IA_AMORTIZE", "LPE_AMORTIZE", "DEFER_INCOME_AMORTIZE"]
check_cols = [i for i in fundamental_df.columns if i not in keep_cols]
# use 0.5 * number of rows as the threshold for columns
col_threshold = int(len(fundamental_df) * 0.5)
fundamental_df_cleaned = pd.concat([
    fundamental_df[check_cols].dropna(axis=1, thresh=col_threshold),
    fundamental_df[keep_cols]],
                                   axis=1)
logging.info(fundamental_df_cleaned.shape)
fundamental_df_cleaned.to_csv(f'{PROGRAM_PATH}/fundamental_cleaned.csv', index=False, encoding='utf-8')
