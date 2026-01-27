
import os
import sys
import argparse
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import warnings
from datetime import datetime
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
# Zipline imports
from zipline import run_algorithm
from zipline.api import order_target_percent, symbol, set_commission
from zipline.finance.commission import PerTrade
from zipline.utils.calendar_utils import get_calendar

# Analysis imports
import pyfolio as pf
from stockstats import StockDataFrame as sdf

warnings.filterwarnings('ignore')

# Strategy definitions
class BaseStrategy:
    def __init__(self, stock_symbol, rolling_window=90):
        self.stock_symbol = stock_symbol
        self.rolling_window = rolling_window

    def initialize(self, context):
        context.stock = symbol(self.stock_symbol)
        context.rolling_window = self.rolling_window
        set_commission(PerTrade(cost=5))

    def handle_data(self, context, data):
        pass

class RollingWindowMeanStrategy(BaseStrategy):
    def handle_data(self, context, data):
        price_hist = data.history(context.stock, "close", context.rolling_window, "1d")
        if price_hist[-1] > price_hist.mean():
            order_target_percent(context.stock, 1.0)
        else:
            order_target_percent(context.stock, 0.0)

class SimpleMovingAveragesStrategy(BaseStrategy):
    def handle_data(self, context, data):
        price_hist = data.history(context.stock, "close", context.rolling_window, "1d")
        rolling_mean_short_term = price_hist.rolling(window=45, center=False).mean()
        rolling_mean_long_term = price_hist.rolling(window=90, center=False).mean()

        if rolling_mean_short_term.iloc[-1] > rolling_mean_long_term.iloc[-1]:
            order_target_percent(context.stock, 1.0)
        elif rolling_mean_short_term.iloc[-1] < rolling_mean_long_term.iloc[-1]:
            order_target_percent(context.stock, 0.0)

class RSIStrategy(BaseStrategy):
    def __init__(self, stock_symbol):
        super().__init__(stock_symbol, rolling_window=20)

    def handle_data(self, context, data):
        price_hist = data.history(context.stock, ["open", "high", "low", "close", "volume"], context.rolling_window, "1d").copy()
        price_hist['amount'] = price_hist['volume'] * price_hist['close']
        # print(f"DEBUG: columns={price_hist.columns}")
        stock = sdf.retype(price_hist)
        rsi = stock.get('rsi_12')

        if rsi[-1] > 90:
            order_target_percent(context.stock, 0.0)
        elif rsi[-1] < 10:
            order_target_percent(context.stock, 1.0)

class MACDCrossoverStrategy(BaseStrategy):
    def __init__(self, stock_symbol):
        super().__init__(stock_symbol, rolling_window=20)

    def handle_data(self, context, data):
        price_hist = data.history(context.stock, ["open", "high", "low", "close", "volume"], context.rolling_window, "1d").copy()
        price_hist['amount'] = price_hist['volume'] * price_hist['close']
        stock = sdf.retype(price_hist)
        signal = stock['macds']
        macd = stock['macd']

        if macd[-1] > signal[-1] and macd[-2] <= signal[-2]:
            order_target_percent(context.stock, 1.0)
        elif macd[-1] < signal[-1] and macd[-2] >= signal[-2]:
            order_target_percent(context.stock, 0.0)

class RSIMACDStrategy(BaseStrategy):
    def __init__(self, stock_symbol):
        super().__init__(stock_symbol, rolling_window=20)

    def handle_data(self, context, data):
        price_hist = data.history(context.stock, ["open", "high", "low", "close", "volume"], context.rolling_window, "1d").copy()
        price_hist['amount'] = price_hist['volume'] * price_hist['close']
        stock = sdf.retype(price_hist)
        rsi = stock.get('rsi_12')
        signal = stock['macds']
        macd = stock['macd']

        if rsi[-1] < 50 and macd[-1] > signal[-1] and macd[-2] <= signal[-2]:
            order_target_percent(context.stock, 1.0)
        elif rsi[-1] > 50 and macd[-1] < signal[-1] and macd[-2] >= signal[-2]:
            order_target_percent(context.stock, 0.0)

class TRIXStrategy(BaseStrategy):
    def __init__(self, stock_symbol):
        super().__init__(stock_symbol, rolling_window=20)

    def handle_data(self, context, data):
        price_hist = data.history(context.stock, ["open", "high", "low", "close", "volume"], context.rolling_window, "1d").copy()
        price_hist['amount'] = price_hist['volume'] * price_hist['close']
        stock = sdf.retype(price_hist)
        trix = stock.get('trix')

        if trix[-1] > 0 and trix[-2] < 0:
            order_target_percent(context.stock, 0.0)
        elif trix[-1] < 0 and trix[-2] > 0:
            order_target_percent(context.stock, 1.0)

class WilliamsRStrategy(BaseStrategy):
    def __init__(self, stock_symbol):
        super().__init__(stock_symbol, rolling_window=20)

    def handle_data(self, context, data):
        price_hist = data.history(context.stock, ["open", "high", "low", "close", "volume"], context.rolling_window, "1d").copy()
        price_hist['amount'] = price_hist['volume'] * price_hist['close']
        stock = sdf.retype(price_hist)
        wr = stock.get('wr_6')

        if wr[-1] < 10:
            order_target_percent(context.stock, 0.0)
        elif wr[-1] > 90:
            order_target_percent(context.stock, 1.0)

class BollingerBandStrategy(BaseStrategy):
    def __init__(self, stock_symbol):
        super().__init__(stock_symbol, rolling_window=20)

    def handle_data(self, context, data):
        price_hist = data.history(context.stock, "close", context.rolling_window, "1d")
        middle_base_line = price_hist.mean()
        std_line = price_hist.std()
        lower_band = middle_base_line - 2 * std_line
        upper_band = middle_base_line + 2 * std_line

        if price_hist[-1] < lower_band:
            order_target_percent(context.stock, 1.0)
        elif price_hist[-1] > upper_band:
            order_target_percent(context.stock, 0.0)

STRATEGIES = {
    "Rolling Window Mean": RollingWindowMeanStrategy,
    "Simple Moving Averages": SimpleMovingAveragesStrategy,
    "RSI": RSIStrategy,
    "MACD Crossover": MACDCrossoverStrategy,
    "RSI + MACD": RSIMACDStrategy,
    "TRIX": TRIXStrategy,
    "Williams %R": WilliamsRStrategy,
    "Bollinger Band": BollingerBandStrategy
}

def analyze_strategy(perf, strategy_name, stock_symbol, output_dir):
    try:
        returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(perf)

        # Save metrics
        perf_stats = pf.timeseries.perf_stats(returns)
        # Decide not to save chart due to file size
        # # Save detailed report (plots)
        # # Create a figure for the report
        # # We can't use create_returns_tear_sheet directly to save to file easily without display
        # # But we can try to save the figures it generates if we can hook into matplotlib
        #
        # # Create directory for symbol if not exists
        # symbol_dir = os.path.join(output_dir, stock_symbol)
        # os.makedirs(symbol_dir, exist_ok=True)
        #
        # # Save CSV of returns
        # perf.to_csv(os.path.join(symbol_dir, f"{strategy_name}_perf.csv"))
        #
        # # Create tear sheet and save
        # # We force matplotlib to non-interactive mode
        # plt.ioff()
        #
        # # Clear existing figures
        # plt.close('all')
        #
        # try:
        #     pf.create_returns_tear_sheet(returns, benchmark_rets=None, return_fig=True)
        # except Exception as e:
        #     # Older pyfolio might not support return_fig=True, it usually just plots
        #     # Check if we can capture current figures
        #     try:
        #         pf.create_returns_tear_sheet(returns, benchmark_rets=None)
        #     except:
        #         pass
        #
        # # Save all open figures
        # fignums = plt.get_fignums()
        # for i, num in enumerate(fignums):
        #     fig = plt.figure(num)
        #     fig.savefig(os.path.join(symbol_dir, f"{strategy_name}_fig_{i}.png"))
        #     plt.close(fig)

        return perf_stats
    except Exception as e:
        print(f"Error analyzing strategy {strategy_name} for {stock_symbol}: {e}")
        return pd.Series()

def run_strategies(stocks, start_date, end_date, bundle_name, output_dir):
    summary_results = []

    # Check if bundle is ingested
    try:
        from zipline.data.bundles import load
        load(bundle_name)
    except Exception as e:
        print(f"Bundle {bundle_name} not found or load failed: {e}. Attempting ingest...")
        os.system(f"zipline ingest -b {bundle_name}")

    processed_count = 0
    total_stocks = len(stocks)
    for stock in stocks:
        processed_count += 1
        print(f"Processing {stock}... ({processed_count}/{total_stocks}). Left: {total_stocks - processed_count}")
        stock_summary = {'Symbol': stock}

        for strategy_name, StrategyClass in STRATEGIES.items():
            print(f"  Running {strategy_name}...")
            strategy = StrategyClass(stock)

            try:
                perf = run_algorithm(
                    start=start_date,
                    end=end_date,
                    initialize=strategy.initialize,
                    handle_data=strategy.handle_data,
                    capital_base=10000,
                    data_frequency='daily',
                    trading_calendar=get_calendar("XSHG", start=start_date),
                    bundle=bundle_name
                )

                stats = analyze_strategy(perf, strategy_name, stock, output_dir)

                # Extract specific metrics for summary
                stock_summary[f"{strategy_name} Annual Return"] = stats.get('Annual return', np.nan)
                stock_summary[f"{strategy_name} Cumulative Return"] = stats.get('Cumulative returns', np.nan)
                stock_summary[f"{strategy_name} Annual Volatility"] = stats.get('Annual volatility', np.nan)
                stock_summary[f"{strategy_name} Sharpe Ratio"] = stats.get('Sharpe ratio', np.nan)
                stock_summary[f"{strategy_name} Max Drawdown"] = stats.get('Max drawdown', np.nan)

            except Exception as e:
                print(f"  Failed to run {strategy_name} for {stock}: {e}")
                # import traceback
                # traceback.print_exc()

        summary_results.append(stock_summary)

        # Save summary every 10 stocks
        if processed_count % 10 == 0:
            summary_df = pd.DataFrame(summary_results)
            number_cols = [col for col in summary_df.columns if col != 'symbol']
            summary_df[number_cols] = summary_df[number_cols].round(6)
            summary_df.to_csv(os.path.join(output_dir, "zipline_summary.csv"), index=False)
            logging.info(f"Summary saved to {os.path.join(output_dir, 'zipline_summary.csv')} (Progress: {processed_count}/{total_stocks})")

    # Save summary
    if summary_results:
        summary_df = pd.DataFrame(summary_results)
        number_cols = [col for col in summary_df.columns if col != 'symbol']
        summary_df[number_cols] = summary_df[number_cols].round(6)
        summary_df.to_csv(os.path.join(output_dir, "zipline_summary.csv"), index=False)
        logging.info(f"Summary saved to {os.path.join(output_dir, 'zipline_summary.csv')}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Zipline Backtests")
    parser.add_argument("--stocks", nargs="+", help="List of stock symbols"
            #             ,default=['SH603105'
            # # , 'SH000905'
            #                       ]
                        )
    parser.add_argument("--input_dir", default="data_tushare", help="Input directory for stock data")
    parser.add_argument("--output_dir", default="data_zipline_backtest", help="Output directory for results")
    parser.add_argument("--start_date", default="2023-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", default="2026-01-23", help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    input_dir = args.input_dir
    # Handle if input_dir is data_tushare but data is in data_tushare/daily
    if os.path.isdir(os.path.join(input_dir, "daily")):
        scan_dir = os.path.join(input_dir, "daily")
    else:
        scan_dir = input_dir

    if args.stocks:
        stocks = args.stocks
    else:
        # List CSVs in input dir
        if os.path.exists(scan_dir):
            files = [f for f in os.listdir(scan_dir) if f.endswith(".csv")]
            stocks = [os.path.splitext(f)[0] for f in files]
            # the above is similar to the following code
            # file_list = get_file_paths_pathlib(f'{PROGRAM_PATH}/{daily_folder}')
            # symbol_li = [extract_stock_symbol_from_path(file_path, from_format='MARKETnumber',
            #                                             to_format='MARKETnumber') for file_path in file_list]
        else:
            print(f"Input directory {scan_dir} does not exist.")
            sys.exit(1)

    if not stocks:
        print("No stocks found.")
        sys.exit(1)

    print(f"Found {len(stocks)} stocks.")

    os.makedirs(args.output_dir, exist_ok=True)

    start_date = pd.Timestamp(args.start_date)
    end_date = pd.Timestamp(args.end_date)

    run_strategies(stocks, start_date, end_date, "cn-daily-bundle", args.output_dir)
