'''
Split data into yearly data and TTM data then concatenate
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

PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_financial'
SINGLE_FILE_PATH = f'{PROGRAM_PATH}/single_file'

# Columns to drop for each sheet type
all_common_cols =['SECUCODE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'ORG_CODE', 'ORG_TYPE', 'REPORT_DATE', 'REPORT_TYPE', 'REPORT_DATE_NAME', 'SECURITY_TYPE_CODE', 'NOTICE_DATE', 'UPDATE_DATE', 'CURRENCY',
                    'OPINION_TYPE', 'OSOPINION_TYPE', 'LISTING_STATE']
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
    """Call _ak_financial_0extract_by_report.py to fetch missing data."""
    if not missing_symbols:
        return

    logging.info(f"Fetching missing data for {len(missing_symbols)} stocks: {missing_symbols}")

    script_path = f"{PROJECT_PATH}/_ak_financial_0extract_by_report.py"

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
    dfs_yearly = []
    dfs_ttm = []

    try:
        for sheet in fundamental_types:
            file_path = f'{SINGLE_FILE_PATH}/{stock_symbol}_{sheet}.csv'
            if not os.path.exists(file_path):
                # If still missing after fetch attempt, return None or empty DF
                logging.warning(f"Still missing {sheet} for {stock_symbol}, skipping.")
                return None, None

            df = pd.read_csv(file_path)
            # Ensure sorting by date
            df['REPORT_DATE'] = pd.to_datetime(df['REPORT_DATE'])
            df = df.sort_values(by='REPORT_DATE')

            current_drop_cols = globals()[f'drop_cols_{sheet}']
            cols_to_keep = [c for c in df.columns if c not in current_drop_cols]
            df = df[cols_to_keep].copy()

            # 1. Yearly dataframe
            df_yearly = df[df['REPORT_TYPE'] == '年报'].copy()
            dfs_yearly.append(df_yearly)

            # 2. TTM dataframe
            # If the latest is a yearly report, TTM is just that.
            # Else, for balance sheet, use latest. For profit/cash flow, sum the last 4 periods
            if df.empty:
                dfs_ttm.append(pd.DataFrame(columns=df.columns))
                continue

            latest_report_date = df['REPORT_DATE'].max()
            latest_record = df[df['REPORT_DATE'] == latest_report_date].iloc[0]

            if latest_record['REPORT_TYPE'] == '年报':
                df_ttm_part = df[df['REPORT_DATE'] == latest_report_date].copy()
                dfs_ttm.append(df_ttm_part)
            else:
                if sheet == 'balance':
                    # For balance sheet, just use the latest record
                    df_ttm_part = df[df['REPORT_DATE'] == latest_report_date].copy()
                    dfs_ttm.append(df_ttm_part)
                else:
                    # Profit and Cash Flow: TTM = latest record + latest yearly report - row with same report_type before the latest yearly report
                    df_yearly_history = df[df['REPORT_TYPE'] == '年报']

                    if not df_yearly_history.empty:
                        latest_yearly = df_yearly_history.iloc[-1]

                        # Find the same reporting period from the prior year (or before the latest yearly)
                        same_period_history = df[(df['REPORT_TYPE'] == latest_record['REPORT_TYPE']) &
                                                 (df['REPORT_DATE'] <= latest_yearly['REPORT_DATE'])]

                        if not same_period_history.empty:
                            prev_period = same_period_history.iloc[-1]

                            numeric_cols = df.select_dtypes(include='number').columns
                            ttm_record = latest_record.copy()

                            for col in numeric_cols:
                                ttm_record[col] = latest_record[col] + latest_yearly[col] - prev_period[col]

                            df_ttm_part = pd.DataFrame([ttm_record])
                            dfs_ttm.append(df_ttm_part)
                        else:
                            # Not enough history to calculate TTM, return empty or fallback
                            dfs_ttm.append(pd.DataFrame(columns=df.columns))
                    else:
                        # No yearly report available to calculate TTM
                        dfs_ttm.append(pd.DataFrame(columns=df.columns))

        # Merge yearly
        if len(dfs_yearly) == 3 and not all(d.empty for d in dfs_yearly):
            stock_df_yearly = reduce(lambda left, right: pd.merge(left, right, on=AK_FUNDAMENTAL_KEEP_COMMON_COLS, how='inner'), dfs_yearly)
            # Insert symbol column
            stock_df_yearly.insert(0, 'symbol', stock_symbol)
            stock_df_yearly['type'] = 'yearly'
        else:
            stock_df_yearly = pd.DataFrame()

        # Merge TTM
        if len(dfs_ttm) == 3 and not all(d.empty for d in dfs_ttm):
            stock_df_ttm = reduce(lambda left, right: pd.merge(left, right, on=AK_FUNDAMENTAL_KEEP_COMMON_COLS, how='inner'), dfs_ttm)
            stock_df_ttm.insert(0, 'symbol', stock_symbol)
            stock_df_ttm['type'] = 'TTM'
            stock_df_ttm['REPORT_DATE_NAME'] = stock_df_ttm.REPORT_DATE.dt.year.astype(str)+"年报"
        else:
            stock_df_ttm = pd.DataFrame()

        return stock_df_yearly, stock_df_ttm

    except Exception as e:
        logging.error(f"Error processing {stock_symbol}: {e}")
        return None, None

def main():
    stock_symbol_li = get_stock_symbols()
    logging.info(f"Found {len(stock_symbol_li)} unique stocks.")

    # 1. Check for missing sheets and fetch if necessary
    missing_symbols = check_missing_sheets(stock_symbol_li)
    if missing_symbols:
        fetch_missing_data(missing_symbols)
        # Re-get the list? No, the list of symbols shouldn't change, just the files availability.

    # 2. Process stocks in parallel
    fundamental_dfs_yearly = []
    fundamental_dfs_ttm = []

    # Use ProcessPoolExecutor for CPU/IO bound tasks
    # Adjust max_workers based on your machine
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_single_stock, sym): sym for sym in stock_symbol_li}

        for i, future in enumerate(as_completed(futures)):
            yearly_result, ttm_result = future.result()

            if yearly_result is not None and not yearly_result.empty:
                fundamental_dfs_yearly.append(yearly_result)
            if ttm_result is not None and not ttm_result.empty:
                fundamental_dfs_ttm.append(ttm_result)

            if (i + 1) % 100 == 0:
                logging.info(f"Processed {i + 1}/{len(stock_symbol_li)} stocks.")

    if not fundamental_dfs_yearly and not fundamental_dfs_ttm:
        logging.error("No data processed.")
        sys.exit(1)

    # 3. Concatenate all results
    logging.info("Concatenating all stock data...")
    df_yearly_all = pd.concat(fundamental_dfs_yearly, axis=0, ignore_index=True) if fundamental_dfs_yearly else pd.DataFrame()
    df_ttm_all = pd.concat(fundamental_dfs_ttm, axis=0, ignore_index=True) if fundamental_dfs_ttm else pd.DataFrame()

    # If max year of TTM rows equal to the max year of yearly rows, drop the TTM rows
    if not df_yearly_all.empty and not df_ttm_all.empty:
        df_yearly_all['year'] = pd.to_datetime(df_yearly_all['REPORT_DATE']).dt.year
        df_ttm_all['year'] = pd.to_datetime(df_ttm_all['REPORT_DATE']).dt.year

        yearly_max = df_yearly_all.groupby('symbol')['year'].max().to_dict()
        ttm_max = df_ttm_all.groupby('symbol')['year'].max().to_dict()

        filtered_ttm = []
        for index, row in df_ttm_all.iterrows():
            sym = row['symbol']
            if sym in yearly_max and sym in ttm_max and yearly_max[sym] == ttm_max[sym]:
                continue
            filtered_ttm.append(row)

        df_ttm_filtered = pd.DataFrame(filtered_ttm)
        df_yearly_all = df_yearly_all.drop(columns=['year'])
        if not df_ttm_filtered.empty:
            df_ttm_filtered = df_ttm_filtered.drop(columns=['year'])
            fundamental_df = pd.concat([df_yearly_all, df_ttm_filtered], ignore_index=True)
        else:
            fundamental_df = df_yearly_all
    else:
        fundamental_df = pd.concat([df_yearly_all, df_ttm_all], ignore_index=True)

    # fundamental_df = fundamental_df.sort(by=['symbol','REPORT_DATE_NAME'], ascending=[True, True])
    fundamental_df.to_csv(f'{PROGRAM_PATH}/financial_all.csv', index=False, encoding='utf-8')
    logging.info(f"Saved financial_dense.csv with shape: {fundamental_df.shape}")

    # 4. Clean columns (Remove sparse columns)
    logging.info("Cleaning sparse columns...")

    # Columns to definitely keep
    keep_cols = ['BOND_PAYABLE', 'DEFER_INCOME_1YEAR','FE_INTEREST_EXPENSE', "FA_IR_DEPR", "OILGAS_BIOLOGY_DEPR", "IR_DEPR"
                            , "IA_AMORTIZE", "LPE_AMORTIZE", "DEFER_INCOME_AMORTIZE", 'LOAN_ADVANCE', 'ACCEPT_DEPOSIT','CREDIT_IMPAIRMENT_LOSS',
                 'SHORT_LOAN','LONG_LOAN','NOTE_PAYABLE']

    check_cols = [i for i in fundamental_df.columns if i not in keep_cols]

    # Threshold: 50% of rows must have data
    col_threshold = int(len(fundamental_df) * 0.5)

    financial_dense = pd.concat([
        fundamental_df[check_cols].dropna(axis=1, thresh=col_threshold),
        fundamental_df[keep_cols]
    ], axis=1)

    logging.info(f"Saved fundamental_cleaned.csv with shape: {financial_dense.shape}")
    financial_dense.to_csv(f'{PROGRAM_PATH}/financial_dense.csv', index=False, encoding='utf-8')


if __name__ == "__main__":
    main()
