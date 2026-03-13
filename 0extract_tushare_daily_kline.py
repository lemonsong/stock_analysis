''''
Update single stock of ALL stock kline data_all_list between start_date and end_date
'''
import sys
import pandas as pd
import tushare as ts
from utils.constants import TUSHARE_API_KEY, PROJECT_PATH
import os
from utils.common import format_stock_symbol, get_file_paths_pathlib, extract_stock_symbol_from_path, get_today_date_string
from utils.tushare_helper import format_tushare_kline_to_dolt_style
from datetime import datetime
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import exchange_calendars as xcals
import argparse
import concurrent.futures

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        msg = f"not a valid date: {s!r}"
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser()
parser.add_argument('--start', type=valid_date, default=get_today_date_string(), help="Start date in YYYY-MM-DD format")
parser.add_argument('--end', type=valid_date, default=get_today_date_string(), help="End date in YYYY-MM-DD format")
args = parser.parse_args()

start_date = args.start
end_date = args.end

if start_date > end_date:
    parser.error(f"start_date {start_date} cannot be after end_date {end_date}")

logging.info(f"Fetching data: {start_date} to {end_date}")

PROGRAM_PATH = f'{PROJECT_PATH}/data/tushare_kline'
kline_file_path = f'{PROGRAM_PATH}/kline_download/0kline_{start_date}_to_{end_date}.csv'
write_log_file_path = f'{PROGRAM_PATH}/0daily_data_write_log.csv'

is_test = False # TODO
if is_test:
    daily_folder='daily_test'
else:
    daily_folder = 'daily'

xshg = xcals.get_calendar("XSHG")
xshg_dates = xshg.sessions_in_range(start_date,end_date)
xshg_dates_li = xshg_dates.tolist()
xshg_dates_li_ymd = [i.strftime('%Y%m%d') for i in xshg_dates_li]

### get kline file so that we can append new kline data_all_list to the single stock files in the daily folder
if os.path.isfile(kline_file_path):
    kline_df = pd.read_csv(kline_file_path)
else:
    ### fetch daily kline of all stock between start_date and end_date from TUSHARE day by day ###
    ts_api = ts.pro_api(TUSHARE_API_KEY)
    kline_df = pd.DataFrame()
    for xshg_date in xshg_dates_li_ymd:
        kline_df_sub = ts_api.daily(trade_date=xshg_date)
        logging.info(xshg_date)
        if len(kline_df_sub) == 0:
            logging.critical("""We haven't fetched new kline_df_sub""")
            sys.exit()
        logging.info(kline_df_sub.head())
        kline_df = pd.concat([kline_df,
                                kline_df_sub], axis=0, ignore_index=True)

    # save to CSV
    kline_df.to_csv(kline_file_path, index=False, encoding='utf-8')
    logging.info('Saved CSV before convert kline_df ')
    # convert kline_df so that it is similar to the data_all_list from extracted from dolt
    kline_df = format_tushare_kline_to_dolt_style(kline_df)
    # save to CSV
    kline_df.to_csv(kline_file_path, index=False, encoding='utf-8')
    logging.info('Saved CSV after convert kline_df ')
logging.info(kline_df.head())


### append new kline to single stock file ###
# get log data_all_list file to record how single stock were changed
if os.path.isfile(write_log_file_path):
    write_log_df = pd.read_csv(write_log_file_path)
else:
    write_log_df = pd.DataFrame()
# get list of single stock data_all_list
file_list = get_file_paths_pathlib(f'{PROGRAM_PATH}/{daily_folder}')

def process_stock_file(file_path):
    try:
        stock_symbol = extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',to_format='MARKETnumber') # use MARKETnumber for data_dolt, number_MARKET for zipline_data folder
        stock_df = pd.read_csv(file_path)
        kline_df_sub = kline_df.loc[kline_df.symbol == stock_symbol, ['date', 'high', 'low', 'open', 'close', 'adjclose', 'volume', 'amount']].copy()

        if kline_df_sub.empty:
            return None

        write_log_df_sub = pd.DataFrame({
            'folder': daily_folder,
            'symbol': [stock_symbol],
            'old_data_max_date': [stock_df['date'].max()],
            'new_date_min_date': [kline_df['date'].min()],
            'update_time': datetime.now(),
            'method': 'append'
        })
        # append new kline data_all_list to single stock file
        stock_df = pd.concat([stock_df, kline_df_sub], axis=0, ignore_index=True)
        stock_df.to_csv(file_path, index=False, encoding='utf-8')
        return write_log_df_sub
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return None

logging.info(f"Processing {len(file_list)} files with ThreadPoolExecutor...")
write_logs = []
with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
    # Use list() to consume the generator and wait for all tasks to complete
    results = list(executor.map(process_stock_file, file_list))

for log_sub in results:
    if log_sub is not None:
        write_logs.append(log_sub)

if write_logs:
    write_log_df = pd.concat([write_log_df] + write_logs, axis=0, ignore_index=True)
    write_log_df.to_csv(write_log_file_path, index=False, encoding='utf-8')

logging.info(f"""{daily_folder} folder updated. Check {write_log_file_path} for written log. Pay attention to the stock with different new/old date """)



