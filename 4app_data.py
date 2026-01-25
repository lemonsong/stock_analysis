'''
Create the final data to display in the signal page

Meanwhile, join fundamental rank to fundamental metrics. and listed helpful fundamental metrics and rank to the signal data
'''
import pandas as pd
from utils.constants import BUY_SIGNAL_COLS, SELL_SIGNAL_COLS
from config import PROJECT_PATH
import os

# read stock_name_industry.csv to map symbol to company and industry
symbol_to_company_df = pd.read_csv("data_all_list/china_stock/stock_name_industry.csv")[['symbol', 'company', 'industry_category_name', 'industry_sub_category_name', 'industry_type_name']]
# buy/sell signal
stock_decision_metrics_df = pd.read_csv('0decision.csv')[['symbol','close','overall_signal_count','buy_signal_count','sell_signal_count'] + BUY_SIGNAL_COLS + SELL_SIGNAL_COLS]
# dividends
stock_cash_dividend_yield_by_periods_df = pd.read_csv("data_ak_dividend/stock_cash_dividend_yield_by_periods.csv")
# create decision signal
app_decision_df = symbol_to_company_df.merge(stock_decision_metrics_df, how='right',on='symbol')
app_decision_df = app_decision_df.merge(stock_cash_dividend_yield_by_periods_df, how='left', on='symbol')
# calculate yield ratio
# TODO: need to run _dividend pipeline when we need to refresh dividend calcualtion (around every half year)
for dividend in ['Total_Yield_1Y','Total_Yield_3Y','Total_Yield_5Y']:
    app_decision_df['Yield_Ratio_' + dividend[-2:]] = app_decision_df[dividend]/app_decision_df['close']
    app_decision_df[dividend] = app_decision_df[dividend].fillna(0)
    app_decision_df['Yield_Ratio_' + dividend[-2:]] = app_decision_df['Yield_Ratio_' + dividend[-2:]].fillna(0)

# Join fundamental ranking to fundamental metrics and join the latest year fundamental ranking to decision df
rank_path = os.path.join(PROJECT_PATH, 'data_ak_fundamental', 'fundamental_rank_prediction.csv')
if os.path.exists(rank_path):
    rank_df = pd.read_csv(rank_path)
    
    # 1. Update fundamental_calculated_metrics.csv
    fundamental_path = os.path.join(PROJECT_PATH, 'data_ak_fundamental', 'fundamental_calculated_metrics.csv')
    if os.path.exists(fundamental_path):
        fundamental_df = pd.read_csv(fundamental_path)
        # Drop existing columns to avoid duplicates
        cols_to_use = ['fundamental_score', 'fundamental_rank']
        fundamental_df = fundamental_df.drop(columns=[c for c in cols_to_use if c in fundamental_df.columns], errors='ignore')
        
        fundamental_df = fundamental_df.merge(rank_df[['symbol', 'fiscal_year'] + cols_to_use], 
                                      on=['symbol', 'fiscal_year'], how='left')
        fundamental_df.to_csv(fundamental_path, index=False)
        print(f"Updated {fundamental_path}")

    # 2. Join latest year fundamental_rank_prediction to app_decision_df
    # Get latest rank for each symbol
    latest_rank_df = rank_df.sort_values('fiscal_year').groupby('symbol').tail(1)
    
    # Rename columns to indicate they are fundamental info
    latest_rank_df = latest_rank_df[['symbol', 'fundamental_score', 'fundamental_rank', 'fiscal_year']]
    latest_rank_df = latest_rank_df.rename(columns={'fiscal_year': 'fundamental_fiscal_year'})
    
    app_decision_df = app_decision_df.merge(latest_rank_df, on='symbol', how='left')

app_decision_df.to_csv('data_app/app_decision.csv', index=False, encoding='utf-8')
