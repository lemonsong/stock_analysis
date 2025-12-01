import sys
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd

from utils.common import get_file_paths_pathlib, extract_stock_symbol_from_path
from config import PROJECT_PATH
from functools import reduce

# ref: https://www.tidy-finance.org/python/financial-statement-analysis.html#combining-financial-ratios

PROGRAM_PATH = f'{PROJECT_PATH}/data_ak_fundamental'
fundamental_df_cleaned = pd.read_csv(f'{PROGRAM_PATH}/fundamental_cleaned.csv')

# extract fiscal_year for following transformations
# fundamental_df_cleaned['fiscal_year'] =
fundamental_df_cleaned.insert(2, 'fiscal_year', fundamental_df_cleaned['REPORT_DATE_NAME'].apply(lambda l: int(l[:4])))

fundamental_df_cleaned = fundamental_df_cleaned.drop('REPORT_DATE_NAME', axis=1)
# fundamental_df_cleaned = ((fundamental_df_cleaned
#                   .assign(
#     fiscal_year=lambda x: x['REPORT_DATE_NAME'].apply(lambda l: int(l[:4])))
# )


# merge yearly stock price data
latest_stock_price_by_yearly_df = pd.read_csv(f'{PROGRAM_PATH}/0latest_stock_price_by_yearly.csv')
fundamental_df_cleaned = fundamental_df_cleaned.merge(latest_stock_price_by_yearly_df, on=['symbol','fiscal_year'], how='left')

# For metrics need to use average
# sort columns, or else it'll impact the average metrics calculation
fundamental_df_cleaned = fundamental_df_cleaned.sort_values(
    ['symbol','fiscal_year'], ascending=True
)
average_cols = ['TOTAL_ASSETS',  'TOTAL_CURRENT_ASSETS', 'INVENTORY', 'ACCOUNTS_RECE', 'PREPAYMENT','NOTE_RECE',
                'TOTAL_CURRENT_LIAB', 'ACCOUNTS_PAYABLE','ADVANCE_RECEIVABLES','STAFF_SALARY_PAYABLE','TAX_PAYABLE','OTHER_PAYABLE',
                 'PARENT_EQUITY_BALANCE', 'TOTAL_PARENT_EQUITY']
average_cols_prior = [i+'_ly' for i in average_cols]
average_cols_avg = [i+'_avg' for i in average_cols]
fundamental_df_cleaned[average_cols_prior] = (
    fundamental_df_cleaned.groupby('SECURITY_NAME_ABBR')[average_cols].shift(1)
)
for col in average_cols:
    fundamental_df_cleaned[f'{col}_avg'] = (fundamental_df_cleaned[col] + fundamental_df_cleaned[f'{col}_ly'])/2
# drop the earliest report row for each stock, as it doesn't have prior year value for period
min_indices = fundamental_df_cleaned.groupby('SECURITY_NAME_ABBR')['fiscal_year'].idxmin()
fundamental_df_cleaned = fundamental_df_cleaned.drop(min_indices)

# fill empty value with 0 for financial metrics calcualtion
fundamental_df_cleaned = fundamental_df_cleaned.fillna(0)
# calculate metrics
fundamental_df_cleaned = (fundamental_df_cleaned
                  .assign(
    ### Liquidity Ratios ###
    current_ratio=lambda x: x["TOTAL_CURRENT_ASSETS_avg"] / x["TOTAL_CURRENT_LIAB_avg"],
    quick_ratio=lambda x: (x["TOTAL_CURRENT_ASSETS_avg"] - x["INVENTORY_avg"]) / x["TOTAL_CURRENT_LIAB_avg"],
    cash_ratio=lambda x: x["MONETARYFUNDS"] / x["TOTAL_CURRENT_LIAB_avg"],

    ### Leverage Ratios ###
    total_debt=lambda x: x["SHORT_LOAN"] + x["LONG_LOAN"] + x["BOND_PAYABLE"] + x["NOTE_PAYABLE"] + x["DEFER_INCOME_1YEAR"],
    net_debt=lambda x: x["total_debt"] - x["MONETARYFUNDS"],
    # total_debt=lambda x: x["SHORT_LOAN"] + x["LONG_LOAN"] + x["NOTE_PAYABLE"],
    debt_to_equity=lambda x: x["total_debt"] / x["TOTAL_EQUITY"],
    debt_to_asset=lambda x: x["total_debt"] / x["TOTAL_ASSETS"],
    interest_coverage=lambda x: x["OPERATE_INCOME"] / x["FE_INTEREST_EXPENSE"],

    ### Efficiency Ratios ###
    # we have OPERATE_INCOME	TOTAL_OPERATE_INCOME	OPERATE_COST	TOTAL_OPERATE_COST	OPERATE_PROFIT	TOTAL_PROFIT	NETPROFIT
    # In the akshare dataset, OPERATE_INCOME = TOTAL_OPERATE_INCOME, but OPERATE_COST < TOTAL_OPERATE_COST
    # I use OPERATE_INCOME as revenue; OPERATE_COST as cost_of_revenue; TOTAL_PROFIT as gross_profit; NETPROFIT as net income
    revenue=lambda x: x['TOTAL_OPERATE_INCOME'],
    gross_profit = lambda x: x['TOTAL_OPERATE_INCOME'] - x['OPERATE_COST'],
    net_profit = lambda x: x['NETPROFIT'],
    # use average for TOTAL_ASSETS
    asset_turnover=lambda x: x["revenue"] / x["TOTAL_ASSETS_avg"],
    # use OPERATE_COST rather than TOTAL_OPERATE_COST
    # use average for  inventory
    inventory_turnover=lambda x: x["OPERATE_COST"] / x["INVENTORY_avg"],
    # use average for ACCOUNTS_RECE
    receivables_turnover=lambda x: x["revenue"] / x["ACCOUNTS_RECE_avg"],

    ### Profitability Ratios ###
    gross_margin=lambda x: x["gross_profit"] / x["revenue"],
    operating_margin = lambda x: x['net_profit'] / x['revenue'],
    profit_margin=lambda x: x["net_profit"] / x["revenue"],
    # roe: return on equity: return for normal shareholders, so we need to use Parent-level data.
    #  PARENT_EQUITY needs to be the average value
    roe=lambda x: x['PARENT_NETPROFIT'] / x['TOTAL_PARENT_EQUITY_avg'],
    roa = lambda x: x['net_profit'] / x['TOTAL_ASSETS_avg'],

    ### Cash Flow & Valuation Metrics ###
    depreciation_and_amortization = lambda x: x["FA_IR_DEPR"] + x["OILGAS_BIOLOGY_DEPR"] + x["IR_DEPR"]
                                              + x["IA_AMORTIZE"] + x["LPE_AMORTIZE"] + x["DEFER_INCOME_AMORTIZE"],
    # EBITDA = Net Income + Interest + Taxes + Depreciation + Amortization
    # EBITDA = Operating Income + Depreciation + Amortization
    ebit = lambda x: x['OPERATE_PROFIT'] + x['FINANCE_EXPENSE'],
    ebitda = lambda x: x["ebit"] + x['depreciation_and_amortization'],
    net_debt_over_ebitda = lambda x: x['net_debt'] / x['ebitda'],
    netcash_operate_over_net_profit = lambda x: x['NETCASH_OPERATE'] / x['net_profit'],

    ## Free Cash Flow Conversion Rate = (Free Cash Flow / EBITDA) ##
    # Free Cash Flow = Cash Flow from Operations minus Capital Expenditures.
    # CONSTRUCT_LONG_ASSET = CapEx
    free_cash_flow = lambda x: x['NETCASH_OPERATE'] - x['CONSTRUCT_LONG_ASSET'],
    free_cash_flow_conversion_rate = lambda x: x['free_cash_flow'] / x['ebitda'],
    ## CWC ##
    change_in_non_cash_current_asset = lambda x: x['INVENTORY']+x['ACCOUNTS_RECE']+x['PREPAYMENT']+x['NOTE_RECE']
                                                 - (x['INVENTORY_ly']+x['ACCOUNTS_RECE_ly']+x['PREPAYMENT_ly']+x['NOTE_RECE_ly']),
    change_in_non_interest_current_liability = lambda x: x['ACCOUNTS_PAYABLE']+x['ADVANCE_RECEIVABLES']+x['STAFF_SALARY_PAYABLE']+x['TAX_PAYABLE']+x['OTHER_PAYABLE']
                                                         - (x['ACCOUNTS_PAYABLE_ly']+x['ADVANCE_RECEIVABLES_ly']+x['STAFF_SALARY_PAYABLE_ly']+x['TAX_PAYABLE_ly']+x['OTHER_PAYABLE_ly']),
    change_in_working_capital = lambda x: x['change_in_non_cash_current_asset']-x['change_in_non_interest_current_liability'],

    # ev_over_ebitda
    market_cap = lambda x: x['close'] * x['SHARE_CAPITAL'],
    ev = lambda x: x['market_cap'] + x['net_debt'],
    ev_over_ebitda=lambda x: x['ev'] / x['ebitda'],

    # # label=lambda x: np.where(x["symbol"].isin(selected_symbols), x["symbol"], np.nan)

    # TODO: interest_coverage  and market_cap
    )
)

fundamental_df_cleaned.to_csv(f'{PROGRAM_PATH}/fundamental_calculated.csv', encoding='utf-8', index=False)

# 定义要展示的指标列
metrics_to_show_cols = [
    'current_ratio', 'quick_ratio', 'cash_ratio',
    'total_debt','net_debt', 'debt_to_equity', 'debt_to_asset', 'interest_coverage',
    'revenue','gross_profit','net_profit', 'asset_turnover', 'inventory_turnover', 'receivables_turnover',
    'gross_margin', 'operating_margin', 'profit_margin', 'roe', 'roa',
    'netcash_operate_over_net_profit', 'free_cash_flow_conversion_rate','change_in_working_capital',
    'net_debt_over_ebitda', 'ev_over_ebitda'
]
must_cols = ['symbol', 'SECURITY_NAME_ABBR', 'fiscal_year', 'ORG_TYPE']
fundamental_df_cleaned[must_cols + metrics_to_show_cols].to_csv(f'{PROGRAM_PATH}/fundamental_calculated_metrics.csv', encoding='utf-8', index=False)
