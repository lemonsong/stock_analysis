'''
Create the final data to display in the signal page
'''
import pandas as pd
from utils.constants import BUY_SIGNAL_COLS, SELL_SIGNAL_COLS
from config import PROJECT_PATH
import os

# read stock_name_industry.csv to map symbol to company and industry
symbol_to_company_df = pd.read_csv("data_all_list/china_stock/stock_name_industry.csv")[['symbol', 'company', 'industry_category_name', 'industry_sub_category_name', 'industry_type_name']]
stock_decision_metrics_df = pd.read_csv('0decision.csv')[['symbol','close','overall_signal_count','buy_signal_count','sell_signal_count'] + BUY_SIGNAL_COLS + SELL_SIGNAL_COLS]
stock_cash_dividend_yield_by_periods_df = pd.read_csv("data_ak_dividend/stock_cash_dividend_yield_by_periods.csv")

# Refactored: result -> df_merged
df_merged = symbol_to_company_df.merge(stock_decision_metrics_df, how='right',on='symbol')
df_merged = df_merged.merge(stock_cash_dividend_yield_by_periods_df, how='left', on='symbol')

for dividend in ['Total_Yield_1Y','Total_Yield_3Y','Total_Yield_5Y']:
    df_merged['Yield_Ratio_' + dividend[-2:]] = df_merged[dividend]/df_merged['close']
    df_merged[dividend] = df_merged[dividend].fillna(0)
    df_merged['Yield_Ratio_' + dividend[-2:]] = df_merged['Yield_Ratio_' + dividend[-2:]].fillna(0)

# Join fundamental ranking
rank_path = os.path.join(PROJECT_PATH, 'data_ak_fundamental', 'fundamental_rank_prediction.csv')
if os.path.exists(rank_path):
    df_rank = pd.read_csv(rank_path)

    # 1. Update fundamental_calculated_metrics.csv
    metrics_path = os.path.join(PROJECT_PATH, 'data_ak_fundamental', 'fundamental_calculated_metrics.csv')
    if os.path.exists(metrics_path):
        df_metrics = pd.read_csv(metrics_path)
        # Drop existing columns to avoid duplicates
        cols_to_use = ['fundamental_score', 'fundamental_rank']
        df_metrics = df_metrics.drop(columns=[c for c in cols_to_use if c in df_metrics.columns], errors='ignore')

        df_metrics = df_metrics.merge(df_rank[['symbol', 'fiscal_year'] + cols_to_use],
                                      on=['symbol', 'fiscal_year'], how='left')
        df_metrics.to_csv(metrics_path, index=False)
        print(f"Updated {metrics_path}")

    # 2. Join latest year fundamental_rank_prediction to df_merged
    # Get latest rank for each symbol
    latest_ranks = df_rank.sort_values('fiscal_year').groupby('symbol').tail(1)

    # Rename columns to indicate they are fundamental info
    latest_ranks = latest_ranks[['symbol', 'fundamental_score', 'fundamental_rank', 'fiscal_year']]
    latest_ranks = latest_ranks.rename(columns={'fiscal_year': 'fundamental_fiscal_year'})

    df_merged = df_merged.merge(latest_ranks, on='symbol', how='left')

df_merged.to_csv('data_app/app_decision.csv', index=False, encoding='utf-8')
print("Saved data_app/app_decision.csv")
