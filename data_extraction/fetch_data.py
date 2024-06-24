from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os
from dotenv import load_dotenv

load_dotenv()
daily_row_stock_path = os.getenv("DAILY_ROW_STOCK_PATH")
now = pd.Timestamp.now()
premium_api_key = os.getenv("ALPHA_VANTAGE_KEY")
fd = FundamentalData(premium_api_key)
ts = TimeSeries(key=premium_api_key, output_format='pandas')


def fetch_daily_row_stock_data(ticker, last_n_years=5):
    """
    Fetch historical stock data for the given ticker, limited to the last 'n' years.

    Args:
        ticker (str): Stock ticker symbol.
        last_n_years (int): Number of years of historical data to retrieve.

    Returns:
        DataFrame: Pandas DataFrame containing the historical stock data.
    """
    all_data = read_daily_row_stock_data(ticker)
    start_date = now - pd.DateOffset(years=last_n_years)
    result = all_data.loc[:start_date]
    return result


def read_daily_row_stock_data(ticker):
    # if not os.path.exists(daily_row_stock_data):
    # init_daily_row_stock_data(ticker)
    # if exists, update the data
    pass


def update_daily_row_stock_data(ticker):
    # Check if the data is up to date
    # if time is after 4:15 PM, check the last date is today,
    # if time is before 4:15 PM, check the last date is yesterday
    # if not, update the data
    pass


def init_daily_row_stock_data(ticker):
    daily_adjusted_data, meta_data = ts.get_daily_adjusted(
        symbol=ticker, outputsize='full')
    daily_adjusted_data = daily_adjusted_data.rename(columns={"1. open": "open",
                                                              "2. high": "high",
                                                              "3. low": "low",
                                                              "5. adjusted close": "close",
                                                              "6. volume": "volume"})
    csv_file_path = os.path.join(
        daily_row_stock_path, f'{ticker}.csv')
    daily_adjusted_data.to_csv(csv_file_path)
