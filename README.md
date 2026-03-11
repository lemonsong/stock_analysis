# Stock Analysis & Monitoring Platform

This project is a comprehensive platform for analyzing Chinese A-shares, monitoring buy/sell signals, and backtesting strategies. It leverages data from various sources (Tushare, Akshare) and provides a Streamlit-based user interface.
![Buy/Sell Signal](img/投资监测分析平台(1).png)
![Financial Comparison](img/投资监测分析平台(2).png)
![Financial Trend Comparison](img/投资监测分析平台(3).png)
![Market Cap and PS Ratio](img/投资监测分析平台(4).png)
![Data Refresh Pipeline](img/投资监测分析平台(5).png)



## Key Features

*   **Data Pipeline**: Automated scripts to fetch, clean, and process daily stock price data (Tushare) and fundamental financial data (Akshare).
*   **Parallel Processing**: Optimized data preparation scripts (`2_0prep...` and `2prep...`) use multi-processing to handle thousands of stocks efficiently.
*   **Buy/Sell Signals**: Calculates technical indicators (RSI, MACD, Bollinger Bands, etc.) to generate daily buy/sell signals.
    *   **Real-time Monitoring**: `pages/2_Buy_Signals.py` displays the latest signals with advanced filtering (Industry, Fundamental Rank, etc.).
    *   **Period Analysis**: `pages/4_Period_Buy_Signals.py` allows analyzing signals over a specific date range to catch trends.
*   **Fundamental Analysis**:
    *   `pages/3_Fundamental_Analysis.py` for deep dives into financial metrics.
    *   `3analysis_rank_ak_fundamental_by_yearly.py` ranks stocks based on key fundamental indicators grouped into Profitability, Cash Quality, Efficiency, Growth, Valuation, and Risk. It uses an Industry-Relative Scoring (Z-score approach) to evaluate and rank metrics across different industries objectively.
*   **Backtesting**: Integration with Zipline for backtesting strategies (see `pages/8_Backtest_Overview.py`).
*   **Portfolio Monitoring**: `pages/6_My_Holdings.py` to track your personal portfolio performance against generated signals.

## Project Structure

*   `data/`: Stores raw and processed data (CSV files).
    *   `ak_fundamental/`: Fundamental data (Balance Sheet, Profit, Cash Flow).
    *   `tushare_kline/`: Daily price data.
    *   `dwa/`: Data Warehouse for App (processed signals, decision tables).
*   `pages/`: Streamlit pages.
    *   `2_Buy_Signals.py`: Daily Signal Monitor.
    *   `4_Period_Buy_Signals.py`: **[NEW]** Historical Signal Analysis.
    *   `5Homepage.py`: **[NEW]** Data Pipeline Manager GUI.
*   `utils/`: Helper functions.
*   `*.py`:
    *   `0extract_*.py`: Data fetchers.
    *   `2prep_*.py`: Data cleaning and preparation (Parallelized).
    *   `3analysis_*.py`: Logic for metric calculation and ranking.
    *   `4app_data.py`: Final data aggregation for the App.

## Getting Started

1.  **Environment Setup**:
    ```bash
    pip install -r requirements.txt
    # Ensure you have .env file with TUSHARE_API_KEY
    ```

2.  **Run the App**:
    ```bash
    streamlit run 5Homepage.py
    ```
    Use this page to trigger data updates:
    *   **Daily Kline Pipeline**: Updates price data and recalculates signals.
    *   **Fundamentals Pipeline**: Updates financial reports and recalculates fundamental scores.

3.  **Explore Analysis**:
    Navigate to `2_Buy_Signals` or `4_Period_Buy_Signals` in the sidebar to view actionable insights.

## Recent Updates

*   **Performance**: `2_0prep_ak_fundamental_by_yearly_concat.py` and `2prep_tushare_daily_kline.py` rewritten to use multi-processing, significantly reducing run time.
*   **Robustness**: Fundamental data prep now automatically attempts to fetch missing sheets.
*   **Features**:
    *   Added "Period Buy Signals" page.
    *   Added "Latest Market Value" to decision data.
    *   Enhanced visualization in Buy Signals page (Industry distribution charts).
