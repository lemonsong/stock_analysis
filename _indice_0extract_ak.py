import os
import akshare as ak
import logging
from utils.constants import PROJECT_PATH

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)

def fetch_major_indices():
    """
    Fetch daily k-line data for a representative set of global major indices using akshare.
    Saves the data to data/macro/indice/
    """
    out_dir = f"{PROJECT_PATH}/data/macro/indice"
    os.makedirs(out_dir, exist_ok=True)

    # A representative list of major global indices
    # We will use stock_zh_index_daily_em for Chinese indices
    # and stock_us_hist for US/Global indices via akshare, or index_global_em.

    # Chinese Major Indices (A shares)
    zh_indices = {
        "sh000001": "SSE_Composite", # 上证指数
        "sz399001": "SZSE_Component", # 深证成指
        "sz399006": "ChiNext", # 创业板指
        "sh000300": "CSI_300", # 沪深300
    }

    for symbol, name in zh_indices.items():
        logging.info(f"Fetching {name} ({symbol})")
        try:
            df = ak.stock_zh_index_daily_em(symbol=symbol)
            if not df.empty:
                df.to_csv(f"{out_dir}/{symbol}_{name}.csv", index=False, encoding='utf-8')
                logging.info(f"Saved {name} with {len(df)} records")
            else:
                logging.warning(f"No data found for {name}")
        except Exception as e:
            logging.error(f"Failed to fetch {name}: {e}")

    # For global indices, we can use stock_zh_index_daily_tx or similar,
    # but index_global_em provides daily prices if properly configured.
    # Alternatively, use stock_us_hist for US indices.

    # Let's use standard global indices supported by akshare (东方财富)
    # The symbol formats for index_global_em:
    global_indices = {
        "NDX": "Nasdaq_100", # 纳斯达克
        "SPX": "S_P_500",    # 标普500
        "DJI": "Dow_Jones",  # 道琼斯
        "N225": "Nikkei_225" # 日经225
    }

    for symbol, name in global_indices.items():
        logging.info(f"Fetching Global Index: {name} ({symbol})")
        try:
            # We use stock_us_hist for US indices as it's reliable in AKShare
            df = ak.stock_us_hist(symbol=symbol, start_date="20000101", end_date="20991231", adjust="qfq")
            if not df.empty:
                df.to_csv(f"{out_dir}/{symbol}_{name}.csv", index=False, encoding='utf-8')
                logging.info(f"Saved {name} with {len(df)} records")
            else:
                logging.warning(f"No data found for {name}")
        except Exception as e:
            logging.error(f"Failed to fetch {name}: {e}")

if __name__ == "__main__":
    fetch_major_indices()
