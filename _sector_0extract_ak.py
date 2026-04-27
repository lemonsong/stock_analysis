import os
import random
import re
import time
import logging

import pandas as pd
import akshare as ak

from utils.common import format_stock_symbol
from utils.constants import PROJECT_PATH

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)
PROGRAM_PATH = os.path.join(PROJECT_PATH, "data/basic", "sector")

# stock_industry_sina_df = ak.stock_sector_spot(indicator="新浪行业")
# out_path = os.path.join(PROGRAM_PATH, f"新浪行业.csv")
# stock_industry_sina_df.to_csv(out_path, index=False, encoding="utf-8")
# logging.info(f"Saved {len(stock_industry_sina_df)} rows to {out_path}")


stock_sector_detail_df = ak.stock_sector_detail(sector="hangye_ZC23")
out_path = os.path.join(PROGRAM_PATH, f"hangye_ZC23.csv")
stock_sector_detail_df.to_csv(out_path, index=False, encoding="utf-8")
logging.info(f"Saved {len(stock_sector_detail_df)} rows to {out_path}")