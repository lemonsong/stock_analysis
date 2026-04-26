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
    # python _ak_financial_0extract_by_report.py --choice_overall_signal_count 1 --fetch_relevant_symbols False --choice_row_range '0-100' --choice_industry_category_name All  --choice_industry_sub_category_name All --choice_industry_type_name All --text_stock_list ''
    # python _ak_financial_0extract_by_report.py --choice_overall_signal_count All --fetch_relevant_symbols False --choice_row_range All --choice_industry_category_name All  --choice_industry_sub_category_name All --choice_industry_type_name All --fetch_relevant_symbols_financial_threshold 4 --fetch_relevant_symbols True --text_stock_list ''
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

    # check whether fetch relevant stock
    if args.fetch_relevant_symbols == 'True':
        relevant_stock_df = pd.read_csv(f'{PROJECT_PATH}/data/basic/relevant_stock.csv')
        collected_symbols = []
        if 'stock_filtered_df' not in locals():
            stock_filtered_df = pd.read_csv(f"{PROJECT_PATH}/data/dwa/app_decision.csv")

        try:
            threshold = float(args.fetch_relevant_symbols_financial_threshold)
        except ValueError:
            threshold = 4.0

        top_df = stock_filtered_df.loc[stock_filtered_df.symbol.isin(stock_li) & (stock_filtered_df.latest_financial_score >= threshold)]
        for symbol in top_df['symbol'].tolist():
            symbol_matches = relevant_stock_df.loc[relevant_stock_df.symbol==symbol]['relevant_stock']
            if not symbol_matches.empty:
                relevant_stock_li = [s.strip() for s in str(symbol_matches.values[0]).split(',') if s.strip()]
                collected_symbols = collected_symbols + relevant_stock_li
        # Add original stock_li and remove duplicates while preserving order to some extent
        stock_li = list(dict.fromkeys(stock_li + collected_symbols))
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

logging.info(f"Total number of stock to fetch financial statement: {len(stock_li)}")
logging.info(f"Stocks to fetch financial statement: {stock_li}")


PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_financial/single_file/'
os.makedirs(PROGRAM_PATH, exist_ok=True)

###########################
# Load latest report dates
latest_report_dates_path = f"{PROJECT_PATH}/data/ak_financial/latest_report_dates.csv"
latest_report_df = pd.DataFrame()
if os.path.isfile(latest_report_dates_path):
    latest_report_df = pd.read_csv(latest_report_dates_path, dtype={'symbol': str})
    # Convert actual_disclosure_date to datetime for comparison
    latest_report_df['actual_disclosure_date'] = pd.to_datetime(latest_report_df['actual_disclosure_date'])
    # Sort by REPORT_PERIOD descending to grab the most recent reporting period safely
    latest_report_df = latest_report_df.sort_values(by='REPORT_PERIOD', ascending=False)
else:
    logging.warning(f"Report dates file not found at {latest_report_dates_path}. Continuing without update check.")

for index, stock_symbol in enumerate(stock_li):
    random_sleep_time = random.randint(11, 30)

    # Check if we already have local files and if they need updating
    symbol_code = stock_symbol[2:] # Strip SH/SZ/BJ

    needs_update = False
    max_local_report_date = None

    path_to_balance = f'{PROGRAM_PATH}/{stock_symbol}_balance.csv'
    path_to_profit = f'{PROGRAM_PATH}/{stock_symbol}_profit.csv'
    path_to_cash_flow = f'{PROGRAM_PATH}/{stock_symbol}_cash_flow.csv'

    files_exist = os.path.isfile(path_to_balance) and os.path.isfile(path_to_profit) and os.path.isfile(path_to_cash_flow)

    if files_exist and not latest_report_df.empty:
        try:
            local_df = pd.read_csv(path_to_balance)
            if 'REPORT_DATE' in local_df.columns:
                local_df['REPORT_DATE'] = pd.to_datetime(local_df['REPORT_DATE'])
                max_local_report_date = local_df['REPORT_DATE'].max()

                # Check against latest report dates
                stock_reports = latest_report_df[latest_report_df['symbol'].astype(str) == symbol_code]
                if not stock_reports.empty:
                    latest_disclosure = stock_reports.iloc[0]
                    # We compare the actual disclosure date. Note that actual disclosure dates are later than REPORT_DATE.
                    # A better way is to see if the REPORT_PERIOD (e.g. 20251231 -> 2025-12-31) is > max_local_report_date
                    report_period_str = str(latest_disclosure['REPORT_PERIOD'])
                    if len(report_period_str) == 8:
                        latest_period_date = pd.to_datetime(report_period_str, format='%Y%m%d')
                        if max_local_report_date is pd.NaT or latest_period_date > max_local_report_date:
                            needs_update = True
                            logging.info(f"{stock_symbol} needs update: local max={max_local_report_date}, latest period={latest_period_date}")
        except Exception as e:
            logging.warning(f"Error checking local files for {stock_symbol}: {e}")
            needs_update = True # Fallback to fetch if error reading

    # fetch balance sheet
    logging.info(f"Progress {index+1}/{len(stock_li)}: Fetching balance sheet of {stock_symbol}")
    if os.path.isfile(path_to_balance) and not needs_update:
        logging.info(f"Balance sheet of {stock_symbol} existed and is up to date")
    else:
        try:
            stock_balance_sheet_by_report_em_df = ak.stock_balance_sheet_by_report_em(symbol=stock_symbol)
            stock_balance_sheet_by_report_em_df.to_csv(path_to_balance, encoding='utf-8', index=False)
            logging.info(f"Balance sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
            time.sleep(random_sleep_time)
        except Exception as e:
            logging.error(f"Error fetching balance sheet for {stock_symbol}: {e}")

    # fetch profit sheet
    logging.info(f"Fetching profit sheet of {index+1}: {stock_symbol}")
    if os.path.isfile(path_to_profit) and not needs_update:
        logging.info(f"Profit sheet of {stock_symbol} existed and is up to date")
    else:
        try:
            stock_profit_sheet_by_report_em = ak.stock_profit_sheet_by_report_em(symbol=stock_symbol)
            stock_profit_sheet_by_report_em.to_csv(path_to_profit, encoding='utf-8', index=False)
            logging.info(f"Profit sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
            time.sleep(random_sleep_time)
        except Exception as e:
            logging.error(f"Error fetching profit sheet for {stock_symbol}: {e}")

    # fetch cash flow sheet
    logging.info(f"Fetching cash flow sheet of {index+1}: {stock_symbol}")
    if os.path.isfile(path_to_cash_flow) and not needs_update:
        logging.info(f"Cash flow sheet of {stock_symbol} existed and is up to date")
    else:
        try:
            stock_cash_flow_sheet_by_report_em = ak.stock_cash_flow_sheet_by_report_em(symbol=stock_symbol)
            stock_cash_flow_sheet_by_report_em.to_csv(path_to_cash_flow, encoding='utf-8', index=False)
            logging.info(f"Cash flow sheet of {stock_symbol} saved. Sleep for {random_sleep_time}s ...")
            time.sleep(random_sleep_time)
        except Exception as e:
            logging.error(f"Error fetching cash flow sheet for {stock_symbol}: {e}")
