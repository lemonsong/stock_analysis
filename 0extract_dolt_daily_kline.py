'''
Initial set-up

Run the following command in terminal first to run the mysql server
$ cd ~/dolt/investment_data
$ dolt pull
$ dolt sql-server
'''
import shutil
import os
import mysql.connector
import pandas as pd
from utils.constants import (
    PROJECT_PATH,
    DATABASE_HOST,
    DATABASE_USER,
    DATABASE_PW,
    DATABASE_DB,
    DATABASE_PORT
)
from utils.dolt_helper import clean_daily_by_dates

mydb = mysql.connector.connect(
    host=DATABASE_HOST,
    user=DATABASE_USER,
    password=DATABASE_PW,
    database=DATABASE_DB,
    port=DATABASE_PORT
)
# get all symbol to fetch data_all_list one by one. This is the list of symbol that we care about.
recent_symbol_q = """
    SELECT symbol 
    FROM final_a_stock_eod_price 
    WHERE tradedate>=CURRENT_DATE() - INTERVAL 30 DAY
    GROUP BY symbol"""
recent_symbol_df = pd.read_sql(recent_symbol_q, mydb)

# changelog: remove the traded in 2020-12-01 filter
# hist_symbol_q = """
#         SELECT symbol
#         FROM final_a_stock_eod_price
#         WHERE  tradedate>='2020-12-01' and tradedate<'2021-01-01'
#         GROUP BY symbol"""
# hist_symbol_df = pd.read_sql(hist_symbol_q, mydb)
#
# all_symbol_df = pd.merge(recent_symbol_df, hist_symbol_df, how='inner')
all_symbol_df = recent_symbol_df.copy()
print(all_symbol_df.head())
print("Number of stock: "+str(len(all_symbol_df)))
all_symbol_df.to_csv(f'{PROJECT_PATH}/data/dolt/all_current_stock.csv', encoding='utf-8', index=False)


# clean the folder first
folder_path = f'{PROJECT_PATH}/data/dolt/daily' # Replace with the actual path to the folder
if os.path.exists(folder_path):
    try:
        shutil.rmtree(folder_path)
        print(f"Folder '{folder_path}' and its contents deleted successfully.")
        os.makedirs(folder_path)
        print(f"Empty folder '{folder_path}' recreated.")
    except OSError as e:
        print(f"Error: {e.strerror} - {folder_path}")
else:
    print(f"Folder '{folder_path}' does not exist.")

# write csv of each stock to the folder
n = 0
for s in all_symbol_df.symbol.tolist():
    stock_kline_q = """
        SELECT tradedate AS date, high, low, open, close, adjclose, volume, amount
        FROM final_a_stock_eod_price
        WHERE tradedate>='2020-12-01'
            AND symbol=%s"""
    stock_kline_df = pd.read_sql(stock_kline_q, mydb, params=(s,))
    stock_kline_df = clean_daily_by_dates(stock_kline_df, s, calendar_name = 'XSHG',
                           calender_start = "2020-12-01",
                           calendar_end= '2026-02-24')

    stock_kline_df.to_csv(f'{PROJECT_PATH}/data/dolt/daily/{s}.csv', encoding='utf-8', index=False)
    n += 1
    print(f"Fetched data of {n} stocks")

mydb.close() #close the connection