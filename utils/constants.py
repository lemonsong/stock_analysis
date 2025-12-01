import dotenv
import os

project_dir = os.path.join(os.path.dirname(__file__), os.pardir)
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

QUANDL_API_KEY = os.getenv("QUANDL_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")
# DATABASE_DB= os.getenv("DATABASE_DB")
# DATABASE_USER= os.getenv("DATABASE_USER")
# DATABASE_PW= os.getenv("DATABASE_PW")
# DATABASE_HOST= os.getenv("DATABASE_HOST")
# DATABASE_PORT= os.getenv("DATABASE_PORT")
AMERITRADE_API_KEY = os.getenv("AMERITRADE_API_KEY")
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
TUSHARE_API_KEY = os.getenv("TUSHARE_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL")
DEEPSEEK_ENABLED = os.getenv("DEEPSEEK_ENABLED")
ALLTICK_API_KEY = os.getenv("ALLTICK_API_KEY")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

# ak_fundamental_by_yearly
AK_FUNDAMENTAL_KEEP_COMMON_COLS = ['SECURITY_NAME_ABBR', 'ORG_TYPE', 'REPORT_DATE_NAME']
