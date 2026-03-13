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
    parser = argparse.ArgumentParser()
    parser.add_argument('--boards_regex', type=str, required=False)
    parser.add_argument('--choice_overall_signal_count', type=str, required=True)
    parser.add_argument('--choice_industry_category_name', type=str, required=True)
    parser.add_argument('--choice_industry_sub_category_name', type=str, required=True)
    parser.add_argument('--choice_industry_type_name', type=str, required=True)
    parser.add_argument('--choice_row_range', type=str, required=True)
    parser.add_argument('--text_stock_list', type=str, required=True)
    parser.add_argument('--fetch_relevant_symbols', type=str, required=False, default='False')

    args = parser.parse_args()
    logging.info(f"Get data for:")
    logging.info(f"Fetch relevant symbols: {args.fetch_relevant_symbols}")
    logging.info(f"Boards regex: {args.boards_regex}")
    logging.info(f"Buy/Sell Signal Count: {args.choice_overall_signal_count}")
    logging.info(f"Industry Category: {args.choice_industry_category_name}")
    logging.info(f"Industry Sub Category: {args.choice_industry_sub_category_name}")
    logging.info(f"Industry Type: {args.choice_industry_type_name}")
    logging.info(f"Row Range: {args.choice_row_range}")
    logging.info(f"Customized Stock List: {args.text_stock_list}")
    # filter data based on arguments

    if args.fetch_relevant_symbols == 'True':
        logging.info("Fetching relevant symbols via Gemini...")
        import google.generativeai as genai
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            logging.warning("GEMINI_API_KEY is not set. Generating mock data.")
        else:
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-2.5-flash')

        stock_filtered_df = pd.read_csv(f"{PROJECT_PATH}/data/dwa/app_decision.csv")
        # filter for overall_signal_count == 2
        stock_filtered_df = stock_filtered_df[stock_filtered_df['overall_signal_count'] == 2]

        if 'latest_financial_score' in stock_filtered_df.columns:
            stock_filtered_df = stock_filtered_df.sort_values(by='latest_financial_score', ascending=False)

        if args.choice_row_range != 'All':
            row_range = [int(item) for item in args.choice_row_range.split('-')]
            stock_filtered_df = stock_filtered_df.iloc[row_range[0]:row_range[1],:]

        target_symbols_companies = stock_filtered_df[['symbol', 'company']].to_dict('records')
        collected_symbols = set()

        # Keep track of ranges added to Feishu to update them later
        feishu_ranges = {}

        for item in target_symbols_companies:
            symbol = item['symbol']
            company = item['company']

            current_symbol_relative_stocks = set()

            prompt = (f"""
            ### 任务步骤
            1. **业务溯源**：请先搜索并分析股票代码 {symbol} 的主营业务、核心产品及其在产业链中的具体位置。
            2. **对标分析**：基于上述分析，找出 A 股上市且未退市的类似业务公司。
            
            ### 对标颗粒度准则（关键）
            - **同质性优先**：优先寻找与 {symbol} 在“产品用途、生产工艺、目标客户”上高度重合的公司。
            - **产业链对齐**：如果该行业具有明显的上中下游，请确保对标公司处于同一环节（例如：同为设备商或同为材料商）。
            - **剔除干扰**：排除仅有少量边缘业务重合、但主营业务完全不同的公司。
        
            ### 输出格式要求
            - **仅输出代码清单**（用于程序解析）：
               {symbol},代码2,代码3,代码4,代码5,代码6,代码7 (请找齐 6-8 个标的，我提供的代码放第一个)

            """)

            try:
                if api_key:
                    response = model.generate_content(prompt)
                    rel_symbols = response.text.strip()
                    import re
                    symbols_found = re.findall(r'[A-Z]{2}[0-9]{6}', rel_symbols)
                    if symbols_found:
                        for s in symbols_found:
                            current_symbol_relative_stocks.add(s)
                            collected_symbols.add(s)
                    time.sleep(1)
                else:
                    current_symbol_relative_stocks.add(symbol)
                    collected_symbols.add(symbol)
                    # collected_symbols.add('SH688433')
                    # collected_symbols.add('SZ000969')
            except Exception as e:
                logging.error(f"Error calling Gemini for {symbol}: {e}")

            # Write original symbol and relevant stocks to Feishu before fetching financial statements
            relevant_stocks_str = ",".join(list(current_symbol_relative_stocks))
            feishu_range = append_feishu_quarterly_eval_data(symbol, relevant_stocks_str)
            if feishu_range:
                for s in current_symbol_relative_stocks:
                    if s not in feishu_ranges:
                        feishu_ranges[s] = []
                    feishu_ranges[s].append(feishu_range)

        stock_li = list(collected_symbols)

    elif len(args.text_stock_list) == 0:
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
        stock_li = stock_filtered_df.symbol.tolist()
    else:
        logging.info(f"Fetching data via customized stock list")
        stock_li = [s.strip() for s in args.text_stock_list.replace('，', ',').split(',') if s.strip()]

else:
    logging.info(f"Fetching data via manual input")
    # decision_df = pd.read_csv(f'{PROJECT_PATH}/data/dwa/kline_analysis.csv')
    # critical_df = decision_df.loc[decision_df.overall_signal_count == 2].copy()
    # stock_li = critical_df.symbol.tolist()

    # OR

    # decision_df = pd.read_csv(f'{PROJECT_PATH}/data/dwa/kline_analysis.csv')
    # critical_df = decision_df.iloc[400:450,:].copy()
    # stock_li = critical_df.symbol.tolist()

    # stock list to fetch fundamentals data_all_list
    # stock_li = ["SH688798",'SH688484','SZ000528','SZ300746','SH601949']
    stock_li = ['SH603659','SZ002709','SH601857','SZ002245','SZ000725','SZ002938','SZ002250','SZ301035','SH601077','SH688019','SZ300628','SH600210','SH688293',
                'SH600031','SH603259','SZ002284','SZ002821','SH603238','SZ002516','SH688019','SZ300073']

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

    # After fetched statements, update Feishu Author column to 'AI' if applicable
    if args.fetch_relevant_symbols == 'True':
        if 'feishu_ranges' in locals() and stock_symbol in feishu_ranges:
            for frange in feishu_ranges[stock_symbol]:
                update_feishu_quarterly_eval_author(frange, "AI")
            # Clear it so we don't update multiple times unnecessarily if another stock maps to the same range
            del feishu_ranges[stock_symbol]
