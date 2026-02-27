'''
prep Tushare kline data_all_list
 by ffill the empty trading date,
 drop duplicated date,x
 and remove date after the end_date_str
'''
import pandas as pd
import tushare as ts
from utils.constants import TUSHARE_API_KEY, PROJECT_PATH
from utils.tushare_helper import format_tushare_kline_to_dolt_style
import random
import time

import os
from utils.common import format_stock_symbol, get_file_paths_pathlib, extract_stock_symbol_from_path
from utils.dolt_helper import clean_daily_by_dates
from datetime import datetime
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import argparse, sys

# Configure logging
# Note: Logging from multiple processes to a single file/stderr can be messy.
# For simplicity, we'll keep the basic config, but be aware lines might interleave.
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)

PROGRAM_PATH = f'{PROJECT_PATH}/data/tushare_kline'
write_log_file_path = f'{PROGRAM_PATH}/0daily_data_write_log.csv'

def get_end_date():
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument('--end', type=str, required=True)
        args = parser.parse_args()
        end_date_str = args.end
        logging.info(f"Fetching data via sys argument parser: {end_date_str}")
    else:
        end_date_str = '2026-02-25' # TODO:
        logging.info(f"Fetching data via manual input: {end_date_str}")
    return datetime.strptime(end_date_str, '%Y-%m-%d').date()

def process_single_stock(stock_symbol, daily_folder, end_date_d):
    """
    Reads, cleans, and saves a single stock's daily kline data.
    Returns a dictionary with log info or None on failure.
    """
    stock_kline_file_path = f'{PROGRAM_PATH}/{daily_folder}/{stock_symbol}.csv'

    try:
        # logging.info(f'Read {stock_kline_file_path}')
        stock_kline_df = pd.read_csv(stock_kline_file_path)
        stock_kline_df['date'] = pd.to_datetime(stock_kline_df['date']).dt.date

        # Check old data
        n_row_old = len(stock_kline_df)
        if n_row_old > 0:
            old_min_date = stock_kline_df['date'].min()
            old_max_date = stock_kline_df['date'].max()
        else:
            old_min_date = None
            old_max_date = None

        # logging.info(f'Clean {stock_symbol}')
        stock_kline_df = clean_daily_by_dates(
            stock_kline_df,
            stock_symbol,
            calendar_name='XSHG',
            calender_start="2020-12-01",
            calendar_end=end_date_d,
            must_end_date=end_date_d
        )

        # Check new data
        n_row_new = len(stock_kline_df)
        if n_row_new > 0:
            new_min_date = stock_kline_df['date'].min()
            new_max_date = stock_kline_df['date'].max()
        else:
            new_min_date = None
            new_max_date = None

        # logging.info(f'Save to {stock_kline_file_path}')
        stock_kline_df.to_csv(stock_kline_file_path, index=False, encoding='utf-8')

        return {
            'folder': daily_folder,
            'symbol': stock_symbol,
            'old_count': n_row_old,
            'old_min_date': old_min_date,
            'old_max_date': old_max_date,
            'new_count': n_row_new,
            'new_min_date': new_min_date,
            'new_max_date': new_max_date,
            'update_time': datetime.now(),
            'method': 'clean'
        }

    except Exception as e:
        logging.error(f"Error processing {stock_symbol}: {e}")
        return None

def main():
    end_date_d = get_end_date()

    is_test = False # TODO
    if is_test:
        daily_folder='daily_test'
    else:
        daily_folder = 'daily'

    # Get file list
    file_list = get_file_paths_pathlib(f'{PROGRAM_PATH}/{daily_folder}')
    symbol_li = [extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',
                                                      to_format='MARKETnumber') for file_path in file_list]

    logging.info(f"Found {len(symbol_li)} stocks to process.")

    # Prepare log dataframe
    if os.path.isfile(write_log_file_path):
        write_log_df = pd.read_csv(write_log_file_path)
    else:
        write_log_df = pd.DataFrame()

    new_logs = []

    # Process in parallel
    with ProcessPoolExecutor() as executor:
        # Map futures to symbols
        futures = {executor.submit(process_single_stock, sym, daily_folder, end_date_d): sym for sym in symbol_li}

        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                new_logs.append(result)

            if (i + 1) % 100 == 0:
                logging.info(f"Processed {i + 1}/{len(symbol_li)} stocks.")

    # Update write log
    if new_logs:
        new_log_df = pd.DataFrame(new_logs)
        write_log_df = pd.concat([write_log_df, new_log_df], axis=0, ignore_index=True)
        write_log_df.to_csv(write_log_file_path, index=False, encoding='utf-8')
        logging.info(f"Updated log file with {len(new_logs)} entries.")
    else:
        logging.warning("No stocks were successfully processed.")

if __name__ == "__main__":
    main()
