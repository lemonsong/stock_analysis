import exchange_calendars as xcals
import numpy as np
import pandas as pd
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
def clean_daily_by_dates(stock_kline_df,
                           stock_symbol,
                           calendar_name = 'XSHG',
                           calender_start = "2021-1-01",
                           calendar_end= '2025-10-20',
                        must_end_date = None
                           ):
    '''
    停牌 causes missing rows for DOLT data.
    We get the missing trading dates, and forword fill the missing values.
    Rare stock has rows in non trading dates, then we remove such rows.

    :param calender_start:
    :param calendar_end:
    :param stock_kline:
    :return:
    '''
    # get trading dates
    cl = xcals.get_calendar(calendar_name)
    cl_dates = cl.sessions_in_range(calender_start, calendar_end)
    # cl_dates_li = [i.strftime('%Y-%m-%d') for i in cl_dates.tolist()]
    # dolt data exported from database use date type, so we use the same type for trading dates list
    cl_dates_li = [i.date() for i in cl_dates.tolist()]

    #
    s_dates_li = stock_kline_df['date'].tolist()
    missing_dates_li = [i for i in cl_dates_li if i not in s_dates_li]
    extra_dates_li = [i for i in s_dates_li if i not in cl_dates_li]


    # ffill rows for the trading dates but no data in the DOLT data
    if missing_dates_li:
        # create dataframe to append
        logging.info(f"""{stock_symbol} missed data in {str(missing_dates_li)}""")
        s_cols = stock_kline_df.columns.tolist()
        s_cols.remove("date")
        missing_dates_data = {'date': missing_dates_li}
        # Use NaN for other columns
        for col in s_cols:
            missing_dates_data[col] = np.nan
        missing_dates_data_df = pd.DataFrame(missing_dates_data)
        # concatenate the data of
        logging.debug(stock_kline_df.tail(5))
        logging.debug(stock_kline_df.dtypes)

        logging.debug(missing_dates_data_df.tail(5))
        logging.debug(missing_dates_data_df.dtypes)

        stock_kline_df = pd.concat([stock_kline_df, missing_dates_data_df], ignore_index=True, axis=0)
        stock_kline_df = stock_kline_df.sort_values('date', ascending=True)
        stock_kline_df = stock_kline_df.ffill()
        logging.info(f"""{stock_symbol} has {str(len(stock_kline_df))} rows now!""")

    # remove rows for the non trading dates but existed in DOLT data
    if extra_dates_li:
        stock_kline_df = stock_kline_df.loc[ ~ stock_kline_df['date'].isin(extra_dates_li)]
    # only keep the rows in date before must_end_date
    if must_end_date:
        stock_kline_df = stock_kline_df.loc[stock_kline_df['date'] <= must_end_date]
    stock_kline_df = stock_kline_df.drop_duplicates().sort_values('date', ascending=True)
    # make sure the df returned outside of the if statement, as no matter what we'll return the df
    return stock_kline_df


