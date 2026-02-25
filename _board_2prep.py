"""
Concatenate all board constituent CSVs in data_other/board_industry/
into a single long-format file: data_other/stock_board_industry_cons_em.csv
"""
import logging
import os

import pandas as pd

from utils.common import get_file_paths_pathlib
from utils.constants import PROJECT_PATH

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)

BOARD_DIR = os.path.join(PROJECT_PATH, "data", "board")
# TODO: review file name
OUTPUT_PATH = os.path.join(PROJECT_PATH, "data/board", "board_concat2.csv")


def main():
    if not os.path.isdir(BOARD_DIR):
        logging.error(f"Directory not found: {BOARD_DIR}")
        return

    paths = get_file_paths_pathlib(BOARD_DIR)
    if not paths:
        logging.warning(f"No CSV files in {BOARD_DIR}")
        return

    dfs = []
    for p in paths:
        df = pd.read_csv(p, encoding="utf-8")
        if "board_name" not in df.columns or "symbol" not in df.columns:
            # Try to normalize common column names
            cols = {c: c for c in df.columns}
            if "board_name" not in cols and len(df.columns) >= 2:
                df = df.rename(columns={df.columns[0]: "board_name", df.columns[1]: "symbol"})
            else:
                logging.warning(f"Skipping {p}: missing board_name or symbol")
                continue
        dfs.append(df)

    if not dfs:
        logging.error("No valid board CSVs found")
        return

    combined = pd.concat(dfs, ignore_index=True)
    combined.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    logging.info(f"Saved {len(combined)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
