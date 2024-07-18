from data_extraction.fetch_financial_data import DailyStockDataLoader, FundamentalDataLoader
import pandas as pd
import numpy as np
import os
import ta as ta


class FeatureEngineering:
    def __init__(self):
        self.loader = DailyStockDataLoader()
        self.processed_daily_stock_path = os.getenv(
            "PROCESSED_DAILY_STOCK_PATH")
        self.now = pd.Timestamp.now()
        self.csv_file_path = None

    def load_commen_features(self, ticker, last_n_years=5):
        """
        Load common features from the daily stock data

        Args:
            ticker (str): The stock ticker
            last_n_days (int): The number of days to load
        """
        all_data = self.read_commen_features(ticker)
        start_date = self.now - pd.DateOffset(years=last_n_years)
        result = all_data.loc[:start_date]
        return result

    def read_commen_features(self, ticker):
        """
        Read common features from the daily stock data

        Args:
            ticker (str): The stock ticker
            last_n_days (int): The number of days to load
        """
        self.csv_file_path = os.path.join(
            self.processed_daily_stock_path, f"features_{ticker}.csv")

        if not os.path.exists(self.csv_file_path):
            self.init_commen_features(ticker)
        else:
            self.update_commen_features(ticker)

        return pd.read_csv(self.csv_file_path, index_col='date', parse_dates=True)

    def init_commen_features(self, ticker):
        """
        Initialize common features from the daily stock data

        Args:
            ticker (str): The stock ticker
        """
        data = self.loader.load_daily_row_stock_data(ticker, last_n_years=10)

        data = self.process_commen_features(data)

        data.to_csv(self.csv_file_path, index=True)

        print(f"Data saved to {self.csv_file_path}")

    def process_commen_features(self, data):

        data = data[::-1].copy()

        data.loc[:, 'log_return'] = np.log(
            data['adjusted_close'] / data['adjusted_close'].shift(1))

        data.loc[:, 'volatility'] = data["log_return"].rolling(
            window=252).std() * np.sqrt(252)

        data.loc[:, 'volatility_change'] = data['volatility'].diff()

        data.loc[:, 'log_volume'] = np.log(data['volume'])

        data.loc[:, 'daily_returns'] = data['adjusted_close'].diff()

        data.loc[:, 'MA-5'] = data['adjusted_close'].rolling(window=5).mean()

        data.loc[:, 'MA-30'] = data['adjusted_close'].rolling(window=30).mean()

        RSI = ta.momentum.RSIIndicator(data['adjusted_close'], window=14)
        data.loc[:, 'RSI'] = RSI.rsi()

        data.loc[:,
                 '5-day_variance'] = data['adjusted_close'].rolling(window=5).var()

        WILLR = ta.momentum.WilliamsRIndicator(high=data['high'],
                                               low=data['low'],
                                               close=data['adjusted_close'],
                                               lbp=14)
        data.loc[:, 'Williams_%R'] = WILLR.williams_r()

        data.loc[:, 'z_score'] = (data['adjusted_close'] - data['adjusted_close'].rolling(window=10).mean()
                                  ) / data['adjusted_close'].rolling(window=10).std()

        data.loc[:, 'SMA10'] = data['adjusted_close'].rolling(window=10).mean()

        data.loc[:, 'EMA12'] = data['adjusted_close'].ewm(
            span=12, adjust=False).mean()

        MACD = ta.trend.MACD(
            close=data['adjusted_close'], window_fast=12, window_slow=26)
        data.loc[:, 'MACD'] = MACD.macd()

        RoC = ta.momentum.ROCIndicator(close=data['adjusted_close'], window=1)
        data.loc[:, 'RoC'] = RoC.roc()

        low_min = data['low'].rolling(window=15).min()
        high_max = data['high'].rolling(window=15).max()
        data.loc[:, 'K15'] = ((data['adjusted_close'] - low_min) /
                              (high_max - low_min)) * 100

        data.loc[:, 'Bollinger_M'] = data['adjusted_close'].rolling(
            window=20).mean()
        data.loc[:, 'Bollinger_U'] = data['Bollinger_M'] + \
            2 * data['adjusted_close'].rolling(window=20).std()
        data.loc[:, 'Bollinger_L'] = data['Bollinger_M'] - \
            2 * data['adjusted_close'].rolling(window=20).std()

        data.loc[:, 'MOM12'] = data['adjusted_close'] - \
            data['adjusted_close'].shift(12)

        data = data.iloc[::-1]

        return data

    def update_commen_features(self, ticker):
        df = pd.read_csv(self.csv_file_path,
                         index_col='date', parse_dates=True)

        # Get the latest date in the existing data
        last_date = df.index.max()

        # Fetch new data from the API
        new_data = self.loader.load_daily_row_stock_data(
            ticker, last_n_years=2)
        new_data.index = pd.to_datetime(new_data.index)

        new_data = self.process_commen_features(new_data)

        # Get the latest date in the new data
        latest_new_date = new_data.index.max()

        # If the latest date in new data is more recent than the last date in the existing data, append the new data
        if latest_new_date > last_date:
            # Filter new data to include only the rows that are more recent than the last date in the existing data
            new_data_to_add = new_data.loc[:last_date + pd.Timedelta(days=1)]
            # Concatenate the new data with the existing data
            df = pd.concat([new_data_to_add, df])
            # Save the updated dataframe back to the CSV file
            df.to_csv(self.csv_file_path)
            print(f"Data for {ticker} has been updated.")
        else:
            print(f"No new data available for {ticker}.")

