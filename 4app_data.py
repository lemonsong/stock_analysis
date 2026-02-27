'''
Create the final data to display in the signal page

Meanwhile, join fundamental rank to fundamental metrics. and listed helpful fundamental metrics and rank to the signal data
'''
import pandas as pd
from utils.constants import BUY_SIGNAL_COLS, SELL_SIGNAL_COLS, FUNDAMENTAL_KEY_COLS, PROJECT_PATH
import os

# read stock_name_industry.csv to map symbol to company and industry
symbol_to_company_df = pd.read_csv(f"{PROJECT_PATH}/data/basic/stock_name_industry.csv")[['symbol', 'company', 'industry_category_name', 'industry_sub_category_name', 'industry_type_name']]
# buy/sell signal
stock_decision_metrics_df = pd.read_csv(f'{PROJECT_PATH}/data/dwa/kline_analysis.csv')[['symbol','close','overall_signal_count','buy_signal_count','sell_signal_count'] + BUY_SIGNAL_COLS + SELL_SIGNAL_COLS + ['growth_1Y','growth_2Y','growth_3Y']]
# dividends
stock_cash_dividend_yield_by_periods_df = pd.read_csv(f"{PROJECT_PATH}/data/ak_dividend/stock_cash_dividend_yield_by_periods.csv")
# create decision signal
app_decision_df = symbol_to_company_df.merge(stock_decision_metrics_df, how='right',on='symbol')
app_decision_df = app_decision_df.merge(stock_cash_dividend_yield_by_periods_df, how='left', on='symbol')
# # TODO: need to run _dividend pipeline when we need to refresh dividend calcualtion (around every half year)


# Join fundamental ranking to fundamental metrics and join the latest year fundamental ranking to decision df
rank_path = os.path.join(PROJECT_PATH, 'data/ak_fundamental', 'fundamental_rank_prediction.csv')
if os.path.exists(rank_path):
    rank_df = pd.read_csv(rank_path)
    
    # 1. Update fundamental_calculated_metrics.csv
    fundamental_path = os.path.join(PROJECT_PATH, 'data/ak_fundamental', 'fundamental_calculated_metrics.csv')
    if os.path.exists(fundamental_path):
        fundamental_df = pd.read_csv(fundamental_path)
        # Drop existing columns to avoid duplicates
        cols_to_use = ['fundamental_score', 'fundamental_rank']
        fundamental_df = fundamental_df.drop(columns=[c for c in cols_to_use if c in fundamental_df.columns], errors='ignore')
        
        fundamental_df = fundamental_df.merge(rank_df[['symbol', 'fiscal_year'] + cols_to_use], 
                                      on=['symbol', 'fiscal_year'], how='left')
        fundamental_df.to_csv(fundamental_path, index=False)
        print(f"Updated {fundamental_path}")

    # 2. Join latest year key fundamental metrics and fundamental rank metrics to app_decision_df
    # Get latest year data for each symbol
    # Use fundamental_df have both fundamental metrics and fundamental rank metrics
    latest_rank_df = fundamental_df.sort_values('fiscal_year').groupby('symbol').tail(1)
    
    # Rename columns to indicate they are fundamental info
    latest_rank_df = latest_rank_df[['symbol', 'fundamental_rank']+FUNDAMENTAL_KEY_COLS]
    latest_rank_df = latest_rank_df.rename(columns={'fiscal_year': 'fundamental_fiscal_year'})
    
    app_decision_df = app_decision_df.merge(latest_rank_df, on='symbol', how='left')

# add big money net inflow
stock_individual_fund_flow_rank_df = pd.read_csv(f'{PROJECT_PATH}/data/basic/stock_individual_fund_flow_rank.csv')
app_decision_df = app_decision_df.merge(stock_individual_fund_flow_rank_df[['symbol', 'big_money_net_inflow_ratio_10d']], how='left', on='symbol')

# add board tags
# board_li = ["MSCI A Share", "沪深300"]  # boards to track
board_path = os.path.join(PROJECT_PATH, 'data/board', 'board_concat.csv')

if os.path.exists(board_path):
    board_df = pd.read_csv(board_path)
    # board_df = board_df[board_df["board_name"].isin(board_li)]
    if not board_df.empty:
        symbol_boards_df = board_df.groupby("symbol")["board_name"].apply(lambda x: ",".join(x)).reset_index()
        symbol_boards_df.columns = ["symbol", "boards"]
        app_decision_df = app_decision_df.merge(symbol_boards_df, on="symbol", how="left")
        app_decision_df["boards"] = app_decision_df["boards"].fillna("")
    else:
        app_decision_df["boards"] = ""
else:
    app_decision_df["boards"] = ""

# --- Add Latest Market Value (Price) ---
market_value_path = os.path.join(PROJECT_PATH, 'data/ak_fundamental', '0latest_stock_price_by_yearly.csv')
if os.path.exists(market_value_path):
    market_value_df = pd.read_csv(market_value_path)
    # Get latest year's price for each symbol
    latest_market_value_df = market_value_df.sort_values('fiscal_year').groupby('symbol').tail(1)
    # Rename close to latest_yearly_close to avoid conflict or clarify source
    latest_market_value_df = latest_market_value_df[['symbol', 'close']].rename(columns={'close': 'latest_yearly_close'})

    app_decision_df = app_decision_df.merge(latest_market_value_df, on='symbol', how='left')
    print(f"Merged latest market value from {market_value_path}")
else:
    print(f"Warning: {market_value_path} not found.")

# fillna with 0
dividend_cols = [col for col in app_decision_df.columns if 'dividend' in col]
app_decision_df[dividend_cols+['big_money_net_inflow_ratio_10d']] = app_decision_df[dividend_cols+['big_money_net_inflow_ratio_10d']].fillna(0)
# app_decision_df = app_decision_df.sort_values(['overall_signal_count','buy_signal_count','sell_signal_count','industry_category_name', 'industry_sub_category_name', 'industry_type_name'])
app_decision_df.to_csv(f'{PROJECT_PATH}/data/dwa/app_decision.csv', index=False, encoding='utf-8')
