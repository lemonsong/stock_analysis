import akshare as ak

fund_name_em_df = ak.fund_name_em()
fund_name_em_df.to_csv('0_all_fund.csv', index=False, encoding='utf-8')