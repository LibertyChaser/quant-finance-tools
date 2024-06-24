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
    """
    Read the daily stock data from the CSV file. If the file does not exist, initialize it.

    Args:
        ticker (str): Stock ticker symbol.

    Returns:
        DataFrame: Pandas DataFrame containing the stock data.
    """
    csv_file_path = os.path.join(daily_row_stock_path, f'{ticker}.csv')

    if not os.path.exists(csv_file_path):
        init_daily_row_stock_data(ticker)
    else:
        update_daily_row_stock_data(ticker)

    return pd.read_csv(csv_file_path, index_col='date', parse_dates=True)


def update_daily_row_stock_data(ticker):
    # Check if the data is up to date
    # if time is after 4:15 PM, check the last date is today,
    # if time is before 4:15 PM, check the last date is yesterday
    # if not, update the data
    pass


def init_daily_row_stock_data(ticker):
    """
    Initialize the stock data CSV file with historical data from Alpha Vantage.

    Args:
        ticker (str): Stock ticker symbol.
    """
    daily_adjusted_data, meta_data = ts.get_daily_adjusted(
        symbol=ticker, outputsize='full')
    daily_adjusted_data = daily_adjusted_data.rename(columns={"1. open": "open",
                                                              "2. high": "high",
                                                              "3. low": "low",
                                                              "4. close": "close",
                                                              "5. adjusted close": "adjusted_close",
                                                              "6. volume": "volume",
                                                              "7. dividend amount": "dividend",
                                                              "8. split coefficient": "split_coefficient"})
    csv_file_path = os.path.join(
        daily_row_stock_path, f'{ticker}.csv')
    daily_adjusted_data.to_csv(csv_file_path)
