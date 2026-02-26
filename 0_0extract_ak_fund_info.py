'''
Fetch list of all funds as csv
'''
import akshare as ak
from utils.constants import PROJECT_PATH

fund_name_em_df = ak.fund_name_em()
fund_name_em_df.to_csv(f'{PROJECT_PATH}/data/ak_fund_info/0_all_fund.csv', index=False, encoding='utf-8')