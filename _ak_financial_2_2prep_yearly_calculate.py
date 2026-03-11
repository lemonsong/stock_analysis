import sys
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pandas as pd
import numpy as np
from utils.constants import PROJECT_PATH

PROGRAM_PATH = f'{PROJECT_PATH}/data/ak_financial'
financial_df_cleaned = pd.read_csv(f'{PROGRAM_PATH}/financial_yearly_concat.csv')
industry_df = pd.read_csv(f'{PROJECT_PATH}/data/basic/stock_name_industry.csv')[['symbol','industry_category_name']]
financial_df_cleaned = financial_df_cleaned.merge(industry_df, 'left', 'symbol')

# extract fiscal_year for following transformations
financial_df_cleaned.insert(2, 'fiscal_year', pd.to_datetime(financial_df_cleaned['REPORT_DATE']).dt.year)

# merge yearly stock price data
latest_stock_price_by_yearly_df = pd.read_csv(f'{PROGRAM_PATH}/0latest_stock_price_by_yearly.csv')
financial_df_cleaned = financial_df_cleaned.merge(latest_stock_price_by_yearly_df, on=['symbol','fiscal_year'], how='left')

# For metrics need to use average
financial_df_cleaned = financial_df_cleaned.sort_values(
    ['symbol','fiscal_year', 'type'], ascending=[True, True, False] # Type sorted so yearly comes before TTM in case of tie, wait we don't tie often.
)
average_cols = ['TOTAL_ASSETS',  'TOTAL_CURRENT_ASSETS', 'INVENTORY', 'ACCOUNTS_RECE', 'PREPAYMENT','NOTE_RECE',
                'TOTAL_CURRENT_LIAB', 'ACCOUNTS_PAYABLE','ADVANCE_RECEIVABLES','STAFF_SALARY_PAYABLE','TAX_PAYABLE','OTHER_PAYABLE',
                 'PARENT_EQUITY_BALANCE', 'TOTAL_PARENT_EQUITY',
                # 银行新增
                'LOAN_ADVANCE', 'ACCEPT_DEPOSIT']
average_cols_prior = [i+'_ly' for i in average_cols]
average_cols_avg = [i+'_avg' for i in average_cols]

# To correctly get _ly, we need to handle that we might have both yearly and TTM for the current year.
# But let's just use shift by symbol directly, assuming it aligns appropriately.
financial_df_cleaned[average_cols_prior] = (
    financial_df_cleaned.groupby('SECURITY_NAME_ABBR')[average_cols].shift(1)
)
for col in average_cols:
    financial_df_cleaned[f'{col}_avg'] = (financial_df_cleaned[col] + financial_df_cleaned[f'{col}_ly'])/2

# drop the earliest report row for each stock
min_indices = financial_df_cleaned.groupby('SECURITY_NAME_ABBR')['fiscal_year'].idxmin()
financial_df_cleaned = financial_df_cleaned.drop(min_indices, errors='ignore')

# fill empty value with 0 for financial metrics calcualtion
financial_df_cleaned = financial_df_cleaned.fillna(0)

# calculate metrics
financial_df_cleaned = (financial_df_cleaned
                  .assign(
    # decide whether is bank
    is_bank = lambda x: x['industry_category_name'] == '金融业',

    ### Liquidity Ratios ###
    current_ratio=lambda x: x["TOTAL_CURRENT_ASSETS_avg"] / x["TOTAL_CURRENT_LIAB_avg"].replace(0, np.nan),
    quick_ratio=lambda x: (x["TOTAL_CURRENT_ASSETS_avg"] - x["INVENTORY_avg"]) / x["TOTAL_CURRENT_LIAB_avg"].replace(0, np.nan),
    cash_ratio=lambda x: x["MONETARYFUNDS"] / x["TOTAL_CURRENT_LIAB_avg"].replace(0, np.nan),

    ### Leverage Ratios ###
    total_debt=lambda x: x["SHORT_LOAN"] + x["LONG_LOAN"] + x["BOND_PAYABLE"] + x["NOTE_PAYABLE"],
    net_debt=lambda x: x["total_debt"] - x["MONETARYFUNDS"],
    debt_to_equity=lambda x: x["total_debt"] / x["TOTAL_EQUITY"].replace(0, np.nan),
    debt_to_asset=lambda x: np.where(
        x['is_bank'],
        (x['TOTAL_PARENT_EQUITY'] - x.get('OTHER_EQUITY_TOOL', 0)) / x['TOTAL_ASSETS'].replace(0, np.nan),
        x["total_debt"] / x["TOTAL_ASSETS"].replace(0, np.nan)
    ),
    interest_coverage=lambda x: x["OPERATE_INCOME"] / x["FE_INTEREST_EXPENSE"].replace(0, np.nan),

    ### Efficiency Ratios ###
    revenue=lambda x: np.where(x['is_bank'], x['OPERATE_INCOME'], x['TOTAL_OPERATE_INCOME']),
    gross_profit = lambda x: np.where(x['is_bank'], x['OPERATE_INCOME'], x['TOTAL_OPERATE_INCOME'] - x['OPERATE_COST']),
    net_profit = lambda x: x['NETPROFIT'],
    asset_turnover=lambda x: x["revenue"] / x["TOTAL_ASSETS_avg"].replace(0, np.nan),
    inventory_turnover=lambda x: np.where(
        x['is_bank'],
        x['LOAN_ADVANCE_avg'] / x['ACCEPT_DEPOSIT_avg'].replace(0, np.nan),
        x['OPERATE_COST'] / x['INVENTORY_avg'].replace(0, np.nan)
    ),
    receivables_turnover=lambda x: x["revenue"] / x["ACCOUNTS_RECE_avg"].replace(0, np.nan),

    ### Profitability Ratios ###
    gross_margin=lambda x: x["gross_profit"] / x["revenue"].replace(0, np.nan),
    operating_margin = lambda x: x['net_profit'] / x['revenue'].replace(0, np.nan),
    profit_margin=lambda x: x["net_profit"] / x["revenue"].replace(0, np.nan),
    roe=lambda x: x['PARENT_NETPROFIT'] / x['TOTAL_PARENT_EQUITY_avg'].replace(0, np.nan),
    roa = lambda x: x['net_profit'] / x['TOTAL_ASSETS_avg'].replace(0, np.nan),

    ### Cash Flow & Valuation Metrics ###
    depreciation_and_amortization = lambda x: x["FA_IR_DEPR"] + x["OILGAS_BIOLOGY_DEPR"] + x["IR_DEPR"]
                                              + x["IA_AMORTIZE"] + x["LPE_AMORTIZE"] + x["DEFER_INCOME_AMORTIZE"],
    ebit = lambda x: x['OPERATE_PROFIT'] + x['FINANCE_EXPENSE'],
    ebitda = lambda x: x["ebit"] + x['depreciation_and_amortization'],
    net_debt_over_ebitda = lambda x: x['net_debt'] / x['ebitda'].replace(0, np.nan),

    ppop=lambda x: x['OPERATE_PROFIT'] + x['CREDIT_IMPAIRMENT_LOSS'],
    ppop_ly=lambda x: x.groupby('symbol')['ppop'].shift(1),
    ppop_growth=lambda x: (x['ppop'] - x['ppop_ly']) / x['ppop_ly'].replace(0, np.nan),
    netcash_operate_over_net_profit = lambda x: np.where(
        x['is_bank'],
        x['ppop_growth'],
        x['NETCASH_OPERATE'] / x['net_profit'].replace(0, np.nan)
    ),

    free_cash_flow = lambda x: x['NETCASH_OPERATE'] - x['CONSTRUCT_LONG_ASSET'],
    free_cash_flow_conversion_rate = lambda x: x['free_cash_flow'] / x['ebitda'].replace(0, np.nan),

    change_in_non_cash_current_asset = lambda x: x['INVENTORY']+x['ACCOUNTS_RECE']+x['PREPAYMENT']+x['NOTE_RECE']
                                                 - (x['INVENTORY_ly']+x['ACCOUNTS_RECE_ly']+x['PREPAYMENT_ly']+x['NOTE_RECE_ly']),
    change_in_non_interest_current_liability = lambda x: x['ACCOUNTS_PAYABLE']+x['ADVANCE_RECEIVABLES']+x['STAFF_SALARY_PAYABLE']+x['TAX_PAYABLE']+x['OTHER_PAYABLE']
                                                         - (x['ACCOUNTS_PAYABLE_ly']+x['ADVANCE_RECEIVABLES_ly']+x['STAFF_SALARY_PAYABLE_ly']+x['TAX_PAYABLE_ly']+x['OTHER_PAYABLE_ly']),
    change_in_working_capital = lambda x: x['change_in_non_cash_current_asset']-x['change_in_non_interest_current_liability'],

    market_cap = lambda x: x['close'] * x['SHARE_CAPITAL'],
    pb_ratio = lambda x: x['market_cap'] / x['TOTAL_PARENT_EQUITY'].replace(0, np.nan),
    ev = lambda x: x['market_cap'] + x['net_debt'],
    ev_over_ebitda=lambda x: np.where(
        x['is_bank'],
        x['pb_ratio'],
        x['ev'] / x['ebitda'].replace(0, np.nan)
    )
    )
)

financial_df_cleaned.to_csv(f'{PROGRAM_PATH}/fundamental_calculated.csv', encoding='utf-8', index=False)

metrics_to_show_cols =[
    'current_ratio', 'quick_ratio', 'cash_ratio',
    'total_debt','net_debt', 'debt_to_equity', 'debt_to_asset', 'interest_coverage',
    'revenue','gross_profit','net_profit', 'asset_turnover', 'inventory_turnover', 'receivables_turnover',
    'gross_margin', 'operating_margin', 'profit_margin', 'roe', 'roa',
    'netcash_operate_over_net_profit', 'free_cash_flow_conversion_rate','change_in_working_capital',
    'net_debt_over_ebitda', 'ev_over_ebitda',
    'market_cap'
]
must_cols = ['symbol', 'SECURITY_NAME_ABBR', 'fiscal_year', 'ORG_TYPE', 'type']
financial_df_cleaned[must_cols + metrics_to_show_cols].to_csv(f'{PROGRAM_PATH}/fundamental_calculated_metrics.csv', encoding='utf-8', index=False)
