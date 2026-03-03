'''
concatenate
'''
import sys
import logging
import os
import time
import subprocess
import pandas as pd
from functools import reduce
from concurrent.futures import ProcessPoolExecutor, as_completed
from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path
from utils.constants import AK_FUNDAMENTAL_KEEP_COMMON_COLS, PROJECT_PATH

# Configure logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)

PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_fundamental'
SINGLE_FILE_PATH = f'{PROGRAM_PATH}/single_file'

# Columns to drop for each sheet type
all_common_cols =['SECUCODE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'ORG_CODE', 'ORG_TYPE', 'REPORT_DATE', 'REPORT_TYPE', 'REPORT_DATE_NAME', 'SECURITY_TYPE_CODE', 'NOTICE_DATE', 'UPDATE_DATE', 'CURRENCY',
                    'OPINION_TYPE', 'OSOPINION_TYPE']
drop_cols = [i for i in all_common_cols if i not in AK_FUNDAMENTAL_KEEP_COMMON_COLS]

drop_cols_balance = drop_cols + ['UNCONFIRM_INVEST_LOSS', 'UNCONFIRM_INVEST_LOSS_YOY',
                                 'OTHER_COMPRE_INCOME','OTHER_COMPRE_INCOME_YOY',
                                 'CONVERT_DIFF','CONVERT_DIFF_YOY',
                                 'EXTRACT_INSURANCE_RESERVE','EXTRACT_INSURANCE_RESERVE_YOY',
                                 'EXTRACT_UNEXPIRE_RESERVE','EXTRACT_UNEXPIRE_RESERVE_YOY']
drop_cols_profit = drop_cols
drop_cols_cash_flow = drop_cols + ['MINORITY_INTEREST', 'MINORITY_INTEREST_YOY',
                                   'NETPROFIT','NETPROFIT_YOY',
                                   'FINANCE_EXPENSE','FINANCE_EXPENSE_YOY',
                                   'EXTRACT_INSURANCE_RESERVE','EXTRACT_INSURANCE_RESERVE_YOY',
                                 'EXTRACT_UNEXPIRE_RESERVE','EXTRACT_UNEXPIRE_RESERVE_YOY'
                                   ]

def get_stock_symbols():
    """Get list of unique stock symbols from the single_file directory."""
    fundamental_yearly_files = get_file_paths_pathlib(SINGLE_FILE_PATH)
    # The format is typically MARKETnumber_sheet.csv, we want MARKETnumber
    stock_symbol_li_containing_duplicates = [
        extract_stock_symbol_from_path(i, from_format='MARKETnumber_xxx', to_format='MARKETnumber')
        for i in fundamental_yearly_files
    ]
    return list(set(stock_symbol_li_containing_duplicates))

def check_missing_sheets(stock_symbol_li):
    """Check if any stock is missing one of the 3 required sheets."""
    fundamental_types = ['balance', 'profit', 'cash_flow']
    missing_symbols = []

    for stock_symbol in stock_symbol_li:
        for sheet in fundamental_types:
            file_path = f'{SINGLE_FILE_PATH}/{stock_symbol}_{sheet}.csv'
            if not os.path.exists(file_path):
                logging.warning(f"Missing {sheet} sheet for {stock_symbol}")
                missing_symbols.append(stock_symbol)
                break

    return list(set(missing_symbols))

def fetch_missing_data(missing_symbols):
    """Call 0extract_ak_fundamental_by_yearly.py to fetch missing data."""
    if not missing_symbols:
        return

    logging.info(f"Fetching missing data for {len(missing_symbols)} stocks: {missing_symbols}")

    # Construct the command
    script_path = f"{PROJECT_PATH}/0extract_ak_fundamental_by_yearly.py"

    # We need to pass dummy arguments for the required args that we don't use when text_stock_list is provided
    cmd = [
        sys.executable, script_path,
        "--boards_regex", "",
        "--choice_overall_signal_count", "All",
        "--choice_industry_category_name", "All",
        "--choice_industry_sub_category_name", "All",
        "--choice_industry_type_name", "All",
        "--choice_row_range", "All",
        "--text_stock_list", ",".join(missing_symbols)
    ]

    try:
        subprocess.run(cmd, check=True)
        logging.info("Finished fetching missing data.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error fetching data: {e}")

def process_single_stock(stock_symbol):
    """Read, clean, and merge sheets for a single stock."""
    fundamental_types = ['balance', 'profit', 'cash_flow']
    dfs = []

    try:
        for sheet in fundamental_types:
            file_path = f'{SINGLE_FILE_PATH}/{stock_symbol}_{sheet}.csv'
            if not os.path.exists(file_path):
                # If still missing after fetch attempt, return None or empty DF
                logging.warning(f"Still missing {sheet} for {stock_symbol}, skipping.")
                return None

            df = pd.read_csv(file_path)

            # Drop unwanted columns
            # We need to dynamically select the drop list based on sheet name
            current_drop_cols = globals()[f'drop_cols_{sheet}']
            cols_to_keep = [c for c in df.columns if c not in current_drop_cols]
            df = df[cols_to_keep].copy()
            dfs.append(df)

        # Merge the 3 dataframes
        # Use inner join on common columns
        stock_df = reduce(lambda left, right: pd.merge(left, right, on=AK_FUNDAMENTAL_KEEP_COMMON_COLS, how='inner'), dfs)

        # Check for merge issues (duplicate columns resulting in _x, _y)
        for col in stock_df.columns:
            if '_y' in col:
                logging.warning(f"Duplicate column issue in {stock_symbol}: {col}")
                return None

        # Insert symbol column
        stock_df.insert(0, 'symbol', stock_symbol)

        return stock_df

    except Exception as e:
        logging.error(f"Error processing {stock_symbol}: {e}")
        return None

def main():
    stock_symbol_li = get_stock_symbols()
    logging.info(f"Found {len(stock_symbol_li)} unique stocks.")

    # 1. Check for missing sheets and fetch if necessary
    missing_symbols = check_missing_sheets(stock_symbol_li)
    if missing_symbols:
        fetch_missing_data(missing_symbols)
        # Re-get the list? No, the list of symbols shouldn't change, just the files availability.

    # 2. Process stocks in parallel
    fundamental_dfs = []

    # Use ProcessPoolExecutor for CPU/IO bound tasks
    # Adjust max_workers based on your machine
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_single_stock, sym): sym for sym in stock_symbol_li}

        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result is not None:
                fundamental_dfs.append(result)

            if (i + 1) % 100 == 0:
                logging.info(f"Processed {i + 1}/{len(stock_symbol_li)} stocks.")

    if not fundamental_dfs:
        logging.error("No data processed.")
        sys.exit(1)

    # 3. Concatenate all results
    logging.info("Concatenating all stock data...")
    fundamental_df = pd.concat(fundamental_dfs, axis=0, ignore_index=True)

    # Save the raw merged file
    fundamental_df.to_csv(f'{PROGRAM_PATH}/fundamental.csv', index=False, encoding='utf-8')
    logging.info(f"Saved fundamental.csv with shape: {fundamental_df.shape}")

    # 4. Clean columns (Remove sparse columns)
    logging.info("Cleaning sparse columns...")

    # Columns to definitely keep
    keep_cols = ['BOND_PAYABLE', 'DEFER_INCOME_1YEAR','FE_INTEREST_EXPENSE', "FA_IR_DEPR", "OILGAS_BIOLOGY_DEPR", "IR_DEPR"
                            , "IA_AMORTIZE", "LPE_AMORTIZE", "DEFER_INCOME_AMORTIZE", 'LOAN_ADVANCE', 'ACCEPT_DEPOSIT','CREDIT_IMPAIRMENT_LOSS']

    check_cols = [i for i in fundamental_df.columns if i not in keep_cols]

    # Threshold: 50% of rows must have data
    col_threshold = int(len(fundamental_df) * 0.5)

    fundamental_df_cleaned = pd.concat([
        fundamental_df[check_cols].dropna(axis=1, thresh=col_threshold),
        fundamental_df[keep_cols]
    ], axis=1)

    logging.info(f"Saved fundamental_cleaned.csv with shape: {fundamental_df_cleaned.shape}")
    fundamental_df_cleaned.to_csv(f'{PROGRAM_PATH}/fundamental_cleaned.csv', index=False, encoding='utf-8')

if __name__ == "__main__":
    main()
