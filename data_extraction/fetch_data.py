from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
import pandas as pd
import os
from dotenv import load_dotenv


class StockDataLoader:
    def __init__(self):
        load_dotenv()
        self.premium_api_key = os.getenv("ALPHA_VANTAGE_KEY")
        self.now = pd.Timestamp.now()
        self.ts = TimeSeries(key=self.premium_api_key, output_format='pandas')
        self.fd = FundamentalData(self.premium_api_key)


class DailyStockDataLoader(StockDataLoader):
    def __init__(self):
        super().__init__()
        self.daily_row_stock_path = os.getenv("DAILY_ROW_STOCK_PATH")

    def load_daily_row_stock_data(self, ticker, last_n_years=5):
        """
        Fetch historical stock data for the given ticker, limited to the last 'n' years.

        Args:
            ticker (str): Stock ticker symbol.
            last_n_years (int): Number of years of historical data to retrieve.

        Returns:
            DataFrame: Pandas DataFrame containing the historical stock data.
        """
        all_data = self.read_daily_row_stock_data(ticker)
        start_date = self.now - pd.DateOffset(years=last_n_years)
        result = all_data.loc[:start_date]
        return result

    def read_daily_row_stock_data(self, ticker):
        """
        Read the daily stock data from the CSV file. If the file does not exist, initialize it.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            DataFrame: Pandas DataFrame containing the stock data.
        """
        csv_file_path = os.path.join(
            self.daily_row_stock_path, f'{ticker}.csv')

        if not os.path.exists(csv_file_path):
            self.init_daily_row_stock_data(ticker)
        else:
            self.update_daily_row_stock_data(ticker)

        return pd.read_csv(csv_file_path, index_col='date', parse_dates=True)

    def update_daily_row_stock_data(self, ticker):
        """
        Update the stock data CSV file with the latest data if it is outdated.

        Args:
            ticker (str): Stock ticker symbol.
        """
        csv_file_path = os.path.join(
            self.daily_row_stock_path, f'{ticker}.csv')
        df = pd.read_csv(csv_file_path, index_col='date', parse_dates=True)

        # Get the latest date in the existing data
        last_date = df.index.max()

        # Fetch new data from the API
        new_data = self.get_daily_renamed_adjusted(ticker)
        new_data.index = pd.to_datetime(new_data.index)

        # Get the latest date in the new data
        latest_new_date = new_data.index.max()

        # If the latest date in new data is more recent than the last date in the existing data, append the new data
        if latest_new_date > last_date:
            # Filter new data to include only the rows that are more recent than the last date in the existing data
            new_data_to_add = new_data.loc[:last_date + pd.Timedelta(days=1)]
            # Concatenate the new data with the existing data
            df = pd.concat([new_data_to_add, df])
            # Save the updated dataframe back to the CSV file
            df.to_csv(csv_file_path)
            print(f"Data for {ticker} has been updated.")

    def init_daily_row_stock_data(self, ticker):
        """
        Initialize the stock data CSV file with historical data from Alpha Vantage.

        Args:
            ticker (str): Stock ticker symbol.
        """
        daily_adjusted_data = self.get_daily_renamed_adjusted(
            ticker, outputsize='full')
        csv_file_path = os.path.join(
            self.daily_row_stock_path, f'{ticker}.csv')
        daily_adjusted_data.to_csv(csv_file_path)
        print(f"Data saved to {csv_file_path}")

    def get_daily_renamed_adjusted(self, ticker, outputsize='compact'):
        """
        Get the daily adjusted stock data for the given ticker.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            DataFrame: Pandas DataFrame containing the daily adjusted stock data.
        """
        data, meta_data = self.ts.get_daily_adjusted(
            symbol=ticker, outputsize=outputsize)
        data = data.rename(columns={"1. open": "open",
                                    "2. high": "high",
                                    "3. low": "low",
                                    "4. close": "close",
                                    "5. adjusted close": "adjusted_close",
                                    "6. volume": "volume",
                                    "7. dividend amount": "dividend",
                                    "8. split coefficient": "split_coefficient"})
        return data


class IntradayStockDataLoader(StockDataLoader):
    def __init__(self):
        super().__init__()
        self.intraday_stock_path = os.getenv("INTRADAY_STOCK_PATH")
