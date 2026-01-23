'''
Create the final data to display in the signal page
'''
import pandas as pd
from utils.constants import BUY_SIGNAL_COLS, SELL_SIGNAL_COLS

# read stock_name_industry.csv to map symbol to company and industry
symbol_to_company_df = pd.read_csv("data_all_list/china_stock/stock_name_industry.csv")[['symbol', 'company', 'industry_category_name', 'industry_sub_category_name', 'industry_type_name']]
stock_decision_metrics_df = pd.read_csv('0decision.csv')[['symbol','close','overall_signal_count','buy_signal_count','sell_signal_count'] + BUY_SIGNAL_COLS + SELL_SIGNAL_COLS]
stock_cash_dividend_yield_by_periods_df = pd.read_csv("data_ak_dividend/stock_cash_dividend_yield_by_periods.csv")
result =symbol_to_company_df.merge(stock_decision_metrics_df, how='right',on='symbol')
result =result.merge(stock_cash_dividend_yield_by_periods_df, how='left', on='symbol')

for dividend in ['Total_Yield_1Y','Total_Yield_3Y','Total_Yield_5Y']:
    result['Yield_Ratio_' + dividend[-2:]] = result[dividend]/result['close']
    result[dividend] = result[dividend].fillna(0)
    result['Yield_Ratio_' + dividend[-2:]] = result['Yield_Ratio_' + dividend[-2:]].fillna(0)


result.to_csv('data_app/app_decision.csv', index=False, encoding='utf-8')