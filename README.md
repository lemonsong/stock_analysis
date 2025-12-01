## Pipeline

### Fundamentals
1. *Extract Step*[0extract_ak_fundamental_by_yearly.py](0extract_ak_fundam
2. ental_by_yearly.py): fetch fundamental data according to the critical stock symbols in 0decision.csv



### Daily kline
1. [0extract_tushare_daily_kline.py](0extract_tushare_daily_kline.py): extract daily kline and write into individual stock csv
2. [1quality_dolt_daily_kline_by_date.py](1quality_dolt_daily_kline_by_date.py): check kline data quality
2.0 update end_date_str then run
2.1 If a lot of stocks have missing date, delete the csv file downloaded and waite until 8pm to rerun the first step
2.2 run zipline to check missing/extra date
2.3 If the data looks normal, and only need to ffill kline for ST and delete duplicated date, go to step 3
3. [2prep_tushare_daily_kline.py](2prep_tushare_daily_kline.py):ffill and clean data by date
4. [3analysis_all_metrics_on_all_stocks_daily_kline.py](3analysis_all_metrics_on_all_stocks_daily_kline.py): calculate multiple buy/sell signal for all stocks
5. [0extract_ak_fundamental_by_yearly.py](0extract_ak_fundamental_by_yearly.py): get fundamental data
5.1 usually we check [0decision.csv](0decision.csv), and get the highest value of *overall_signal_count* column
5.2 then get fundamental data for the stock with highest *overall_signal_count* by editing [0extract_ak_fundamental_by_yearly.py](0extract_ak_fundamental_by_yearly.py)
6. [2_0prep_ak_fundamental_by_yearly_concat.py](2_0prep_ak_fundamental_by_yearly_concat.py): concatenate fundamental data
7. [2_1prep_ak_fundamental_market_value.py](2_1prep_ak_fundamental_market_value.py): fetch latest stock price in each year by stock to calculate market value
8. [2_2prep_ak_fundamental_by_yearly_calculate.py](2_2prep_ak_fundamental_by_yearly_calculate.py): calculate financial metrics

### TODO
[3analysis_all_metrics_on_all_stocks_daily_kline.py](3analysis_all_metrics_on_all_stocks_daily_kline.py)
add update date as column. so that streamlit app can display
