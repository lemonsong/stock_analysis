"""
unused
Extract board constituent stock data from akshare.
Saves one CSV per board in data_other/board_industry/ with columns: board_name, symbol.
"""
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
# BK0821-MSCI
BOARD_SYMBOL_LIST = [ "小金属"]  # User config: add board names from ak.stock_board_industry_name_em()
PROGRAM_PATH = os.path.join(PROJECT_PATH, "data/basic", "board_industry")


def sanitize_filename(name: str) -> str:
    """Replace invalid filename chars with underscore."""
    return re.sub(r'[<>:"/\\|?*]', "_", name)


def main():
    os.makedirs(PROGRAM_PATH, exist_ok=True)

    for board_name in BOARD_SYMBOL_LIST:
        try:
            logging.info(f"Fetching board constituents: {board_name}")
            df = ak.stock_board_industry_cons_em(symbol=board_name)

            if df is None or df.empty:
                logging.warning(f"No data for board: {board_name}")
                continue

            # API returns 代码 column; convert to MARKETnumber format
            code_col = "代码" if "代码" in df.columns else df.columns[1]
            symbols = []
            for code in df[code_col].astype(str):
                try:
                    sym = format_stock_symbol(code.strip(), "number", "MARKETnumber")
                    symbols.append(sym)
                except Exception:
                    symbols.append(code)

            out_df = pd.DataFrame({"board_name": board_name, "symbol": symbols})
            out_path = os.path.join(PROGRAM_PATH, f"{sanitize_filename(board_name)}.csv")
            out_df.to_csv(out_path, index=False, encoding="utf-8")
            logging.info(f"Saved {len(out_df)} rows to {out_path}")

        except Exception as e:
            logging.error(f"Failed to fetch {board_name}: {e}")

        random_sleep = random.randint(2, 5)
        time.sleep(random_sleep)


if __name__ == "__main__":
    main()
