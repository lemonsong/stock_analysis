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
from utils.feishu_helper import append_feishu_quarterly_eval_data, update_feishu_quarterly_eval_author

import argparse, sys

########### define stock_li to fetch fundamental data_all_list ##########
if len(sys.argv) > 1:
    # read arguments: choice_overall_signal_count, choice_industry_category_name, choice_industry_type_name, choice_row_range
    # CMD for testing: python _ak_financial_0extract_by_report.py --choice_overall_signal_count 2 --fetch_relevant_symbols True --choice_row_range All --choice_industry_category_name All  --choice_industry_sub_category_name All --choice_industry_type_name All --text_stock_list '' --fetch_relevant_symbols_financial_threshold 4
    # python _ak_financial_0extract_by_report.py --choice_overall_signal_count 2 --fetch_relevant_symbols False --choice_row_range All --choice_industry_category_name All  --choice_industry_sub_category_name All --choice_industry_type_name All --text_stock_list ''
    parser = argparse.ArgumentParser()
    parser.add_argument('--boards_regex', type=str, required=False)
    parser.add_argument('--choice_overall_signal_count', type=str, required=True)
    parser.add_argument('--choice_industry_category_name', type=str, required=True)
    parser.add_argument('--choice_industry_sub_category_name', type=str, required=True)
    parser.add_argument('--choice_industry_type_name', type=str, required=True)
    parser.add_argument('--choice_row_range', type=str, required=True)
    parser.add_argument('--text_stock_list', type=str, required=True)
    parser.add_argument('--fetch_relevant_symbols', type=str, required=False, default='False')
    parser.add_argument('--fetch_relevant_symbols_financial_threshold', type=str, required=False, default='4')


    args = parser.parse_args()
    logging.info(f"Get data for:")
    logging.info(f"Fetch relevant symbols: {args.fetch_relevant_symbols} with financial score >= {args.fetch_relevant_symbols_financial_threshold}")
    logging.info(f"Boards regex: {args.boards_regex}")
    logging.info(f"Buy/Sell Signal Count: {args.choice_overall_signal_count}")
    logging.info(f"Industry Category: {args.choice_industry_category_name}")
    logging.info(f"Industry Sub Category: {args.choice_industry_sub_category_name}")
    logging.info(f"Industry Type: {args.choice_industry_type_name}")
    logging.info(f"Row Range: {args.choice_row_range}")
    logging.info(f"Customized Stock List: {args.text_stock_list}")
    # filter data based on arguments

    if len(args.text_stock_list) == 0:
        logging.info(f"Fetching data via field&value filter")
        stock_filtered_df = pd.read_csv(f"{PROJECT_PATH}/data/dwa/app_decision.csv")
        if args.boards_regex and args.boards_regex.strip():
            stock_filtered_df = stock_filtered_df[
                stock_filtered_df["boards"].astype(str).str.contains(
                    args.boards_regex, case=False, na=False, regex=True
                )
            ]
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
        if args.choice_row_range != 'All':
            row_range = [int(item) for item in args.choice_row_range.split('-')]
            stock_filtered_df = stock_filtered_df.iloc[row_range[0]:row_range[1],:]
        stock_li = stock_filtered_df.sort_values(by='latest_financial_score',ascending=False).symbol.tolist()
    else:
        logging.info(f"Fetching data via customized stock list")
        stock_li = [s.strip() for s in args.text_stock_list.replace('，', ',').split(',') if s.strip()]

    # # check whether fetch relative stock
    # if args.fetch_relevant_symbols == 'True':
    #     relative_stock_df = pd.read_csv(f'{PROJECT_PATH}/data/basic/relative_stock.csv')
    #     collected_symbols = []
    #     if stock_filtered_df: #TODO:fix bug
    #         stock_filtered_df = pd.read_csv(f"{PROJECT_PATH}/data/dwa/app_decision.csv")
    #     top_df = stock_filtered_df.loc[stock_filtered_df.symbol.isin(stock_li) & stock_filtered_df.latest_financial_score >= 4]
    #     for symbol in top_df['symbol'].tolist():
    #         relative_stock_li = [s.strip() for s in relative_stock_df.loc[relative_stock_df.symbol==symbol]['relative_stock'].values[0].split(',') if s.strip()]
    #         collected_symbols = collected_symbols + relative_stock_li
    #     stock_li = collected_symbols
else:
    logging.info(f"Fetching data via manual input")
    decision_df = pd.read_csv(f'{PROJECT_PATH}/data/dwa/kline_analysis.csv')
    critical_df = decision_df.loc[decision_df.overall_signal_count == 2].copy()
    stock_li = critical_df.symbol.tolist()

    # OR

    # decision_df = pd.read_csv(f'{PROJECT_PATH}/data/dwa/kline_analysis.csv')
    # critical_df = decision_df.iloc[400:450,:].copy()
    # stock_li = critical_df.symbol.tolist()

    # stock list to fetch fundamentals data_all_list
    stock_li = ['SZ300592','SZ000506','SH688508','SZ300240','SH688668',
                'SZ301591','SH688155','SH603189','SZ301221','SZ002835',
                'SH688002','SZ000786','SH600353','SH600273','SZ002033',
                'SH688163','SZ000880','SZ002700','SZ000957','SZ002593',
                'SZ300771','SZ000715','SZ300349','SH600621','SZ002284',
                'SH603993','SZ300817','SH600820','SH600834','SZ002928',
                'SH600640','SH603859','SZ300576','SH688472','SZ300358',
                'SH688768','SH688249','SH603737','SH600496','SZ300770',
                'SZ001367','SH603355','SH603408','SH688403','SZ002048',
                'SH603387','SH601801','SH603020','SZ301263','SZ300955',
                'SZ300820','SH601198','SH603219','SZ002728','SH600711',
                'SH688410','SH605080','SZ000923','SH603895','SH605566',
                'SH600819','SH603926','SH603259','SZ300707','SZ301033',
                'SZ300863','SH600790','SH688690','SH603238','SH605488',
                'SZ300401','SZ300871','SH600346','SH600686','SZ300247',
                'SZ301333','SZ300131','BJ920019','SZ000532','SH688053',
                'SZ002214','SH688720','SZ300699','SH688798','SZ301234',
                'SH688484','SH601865','SH600675','SH603956','SZ001239',
                'SZ000528','SZ300947','SZ002253','SH603520']
    # stock_li = ['SH603659','SZ002709','SH601857','SZ002245','SZ000725','SZ002938','SZ002250','SZ301035','SH601077','SH688019','SZ300628','SH600210','SH688293',
    #             'SH600031','SH603259','SZ002284','SZ002821','SH603238','SZ002516','SH688019','SZ300073']

logging.info(f"{stock_li=}")


PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_financial/single_file/'
os.makedirs(PROGRAM_PATH, exist_ok=True)

###########################
# Get file name all files in the path. only fetch and write data_all_list if the symbol_xx_sheet not existed.
# TODO level 2(in 2026): get file name all files in the path. if symbol existed, only update the files if the report date is 2 year from April of current year(TBD) .

for index, stock_symbol in enumerate(stock_li):
    random_sleep_time = random.randint(11, 30)
    # balance sheet
    logging.info(f"Fetching balance sheet of {index}: {stock_symbol}")
    path_to_balance = f'{PROGRAM_PATH}/{stock_symbol}_balance.csv'
    if os.path.isfile(path_to_balance):
        logging.info(f"Balance sheet of {stock_symbol} existed")
    else:
        stock_balance_sheet_by_report_em_df = ak.stock_balance_sheet_by_report_em(symbol=stock_symbol)
        stock_balance_sheet_by_report_em_df.to_csv(path_to_balance,
                                                   encoding='utf-8',
                                                   index=False)
        logging.info(f"Balance sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
        time.sleep(random_sleep_time)

    # profit sheet
    logging.info(f"Fetching profit sheet of {index}: {stock_symbol}")
    path_to_profit = f'{PROGRAM_PATH}/{stock_symbol}_profit.csv'
    if os.path.isfile(path_to_profit):
        logging.info(f"Profit sheet of {stock_symbol} existed")
    else:
        stock_profit_sheet_by_report_em = ak.stock_profit_sheet_by_report_em(symbol=stock_symbol)
        stock_profit_sheet_by_report_em.to_csv(path_to_profit,
                                               encoding='utf-8',
                                               index=False)
        logging.info(f"Profit sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
        time.sleep(random_sleep_time)

    # cash flow sheet
    logging.info(f"Fetching cash flow sheet of {index}: {stock_symbol}")
    path_to_cash_flow = f'{PROGRAM_PATH}/{stock_symbol}_cash_flow.csv'
    if os.path.isfile(path_to_cash_flow):
        logging.info(f"Cash flow sheet of {stock_symbol} existed")
    else:
        stock_cash_flow_sheet_by_report_em = ak.stock_cash_flow_sheet_by_report_em(symbol=stock_symbol)
        stock_cash_flow_sheet_by_report_em.to_csv(path_to_cash_flow,
                                                  encoding='utf-8',
                                                  index=False)
        logging.info(f"Cash flow sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
        time.sleep(random_sleep_time)
