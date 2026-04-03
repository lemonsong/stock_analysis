import dotenv
import os
# PROJECT_PATH = '/Users/yilin/Documents/Projects_stock/stock_analysis'
#os.path.dirname(os.path.abspath(__file__))
from pathlib import Path
import plotly.express as px


# PROJECT_PATH = '/Users/yilin/Documents/Projects_stock/stock_analysis'
# Use relative path to make it portable
PROJECT_PATH = str(Path(__file__).parent.parent)


# project_dir = os.path.join(os.path.dirname(__file__), os.pardir)
dotenv_path = os.path.join(PROJECT_PATH, '.env')
dotenv.load_dotenv(dotenv_path)

# QUANDL_API_KEY = os.getenv("QUANDL_API_KEY")
# FRED_API_KEY = os.getenv("FRED_API_KEY")
DATABASE_DB = os.getenv("DATABASE_DB", "investment_data")
DATABASE_USER = os.getenv("DATABASE_USER", "root")
DATABASE_PW = os.getenv("DATABASE_PW", "")
DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", "3306"))
# AMERITRADE_API_KEY = os.getenv("AMERITRADE_API_KEY")
# DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
TUSHARE_API_KEY = os.getenv("TUSHARE_API_KEY")
# DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
# DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL")
# DEEPSEEK_ENABLED = os.getenv("DEEPSEEK_ENABLED")
# ALLTICK_API_KEY = os.getenv("ALLTICK_API_KEY")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID")
FEISHU_APP_KEY = os.getenv("FEISHU_APP_KEY")
FEISHU_WIKI_TOKEN = os.getenv("FEISHU_WIKI_TOKEN")


# plotly chart: https://plotly.com/python/builtin-colorscales/
SEQUENTIAL_COLOR ='PuBu' #'YlGnBu' #'GnBu'
# discrete color: https://plotly.com/python/discrete-color/
DISCRETE_COLOR = px.colors.qualitative.Light24
# ak_fundamental_by_yearly
AK_FUNDAMENTAL_KEEP_COMMON_COLS = ['SECURITY_NAME_ABBR', 'ORG_TYPE', 'REPORT_DATE_NAME','REPORT_DATE','REPORT_TYPE']


# buy_sell_signal_column_type
BUY_SIGNAL_COLS = ['rsi_less_than_10_buy','close_less_than_boll_lb_buy',
                   'ewm_short_term_more_than_long_term_buy', 'macd_buy', 'rsi_plus_macd_buy',
                   'trix_buy', 'wr_more_than_90_buy']
SELL_SIGNAL_COLS = ['rsi_more_than_90_sell', 'close_more_than_boll_ub_sell',
                    'ewm_short_term_less_than_long_term_sell', 'macd_sell', 'rsi_plus_macd_sell',
                    'trix_sell','wr_less_than_10_sell']

#
FUNDAMENTAL_KEY_COLS = ['roe','netcash_operate_over_net_profit','debt_to_asset','inventory_turnover','ev_over_ebitda']
INDUSTRY_COL_DICT = {
            'industry_category_name': '门类',
            'industry_sub_category_name': '次类',
            'industry_type_name': '大类',
        }
INDUSTRY_COL_DEFAULT_TO_USE = 2