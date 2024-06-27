from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
import pandas as pd
import os
from dotenv import load_dotenv
import pandas_market_calendars as mcal


class DataLoader:
    def __init__(self):
        load_dotenv()
        self.premium_api_key = os.getenv("ALPHA_VANTAGE_KEY")
        self.now = pd.Timestamp.now(tz='America/New_York')


class FundamentalDataLoader(DataLoader):
    def __init__(self):
        super().__init__()
        self.fd = FundamentalData(self.premium_api_key)
        self.row_financial_reports_path = os.getenv(
            "ROW_FINANCIAL_REPORTS_PATH")
        self.csv_file_path = None
        # Define a mapping of report types to their corresponding function calls
        self.report_function_mapping = {
            'income_statement': {
                'annual': self.fd.get_income_statement_annual,
                'quarterly': self.fd.get_income_statement_quarterly
            },
            'balance_sheet': {
                'annual': self.fd.get_balance_sheet_annual,
                'quarterly': self.fd.get_balance_sheet_quarterly
            },
            'cash_flow': {
                'annual': self.fd.get_cash_flow_annual,
                'quarterly': self.fd.get_cash_flow_quarterly
            }
        }

    def get_company_overview(self, ticker):
        """
        Get the company overview data for the given ticker.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            dict: Dictionary containing the company overview data.
        """
        data, meta_data = self.fd.get_company_overview(symbol=ticker)
        data_df = pd.DataFrame.from_dict(data, orient='index')
        return data_df

    def load_financial_reports(self, ticker, time_period, report_type, past_years=5):
        """
        Load the financial reports for the given report type and time period.

        Args:
            report_type (str): Type of financial report to load.
            time_period (str): Time period for the financial report.
            past_years (int, optional): Number of years of historical data to retrieve. Defaults to 5.

        Returns:
            DataFrame: Pandas DataFrame containing the financial report data.
        """
        all_data = self.read_financial_reports(
            ticker, time_period, report_type)
        if time_period == 'annual':
            df = pd.DataFrame(all_data.iloc[:past_years])
        elif time_period == 'quarterly':
            df = pd.DataFrame(all_data.iloc[:past_years * 4])
        return df

    def read_financial_reports(self, ticker, time_period, report_type):
        """
        Read the financial reports from the CSV file. If the file does not exist, initialize it.

        Args:
            ticker (str): Stock ticker symbol.
            report_type (str): Type of financial report to load.
            time_period (str): Time period for the financial report.

        Returns:
            DataFrame: Pandas DataFrame containing the financial report data.
        """
        self.csv_file_path = os.path.join(
            self.row_financial_reports_path, f'{ticker}_{time_period}_{report_type}.csv')

        if not os.path.exists(self.csv_file_path):
            self.init_financial_reports(ticker, time_period, report_type)
        else:
            self.update_financial_reports(ticker, time_period, report_type)

        return pd.read_csv(self.csv_file_path, index_col='fiscalDateEnding', parse_dates=True)

    def init_financial_reports(self, ticker, time_period, report_type):
        """
        Initialize the financial reports CSV file with historical data from Alpha Vantage.

        Args:
            ticker (str): Stock ticker symbol.
            report_type (str): Type of financial report to load.
        """
        # Fetch the data using the mapping
        data_function = self.report_function_mapping.get(
            report_type, {}).get(time_period)

        if data_function:
            data, _ = data_function(ticker)
        else:
            raise ValueError(
                f"Invalid report type '{report_type}' or time period '{time_period}'")

        data.to_csv(self.csv_file_path, index=False)

        print(f"Data saved to {self.csv_file_path}")

    def update_financial_reports(self, ticker, time_period, report_type):
        """
        Update the financial reports CSV file with the latest data if it is outdated.

        Args:
            ticker (str): Stock ticker symbol.
            report_type (str): Type of financial report to load.
        """

        df = pd.read_csv(self.csv_file_path,
                         index_col='fiscalDateEnding', parse_dates=True)

        # Get the latest date in the existing data
        last_date = df.index.max()

        # Fetch the data using the mapping
        data_function = self.report_function_mapping.get(
            report_type, {}).get(time_period)

        if data_function:
            new_data, _ = data_function(ticker)
        else:
            raise ValueError(
                f"Invalid report type '{report_type}' or time period '{time_period}'")

        new_data.set_index('fiscalDateEnding', inplace=True)
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
            df.to_csv(self.csv_file_path)
            print(f"Data for {ticker} has been updated.")
        else:
            print(
                f"No new data available for {ticker} {time_period} {report_type}.")


class StockDataLoader(DataLoader):
    def __init__(self):
        super().__init__()
        self.ts = TimeSeries(key=self.premium_api_key, output_format='pandas')


class DailyStockDataLoader(StockDataLoader):
    def __init__(self):
        super().__init__()
        self.row_daily_stock_path = os.getenv("ROW_DAILY_STOCK_PATH")
        self.csv_file_path = None

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
        result = all_data.loc[:start_date.tz_localize(None)]
        return result

    def read_daily_row_stock_data(self, ticker):
        """
        Read the daily stock data from the CSV file. If the file does not exist, initialize it.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            DataFrame: Pandas DataFrame containing the stock data.
        """
        self.csv_file_path = os.path.join(
            self.row_daily_stock_path, f'{ticker}.csv')

        if not os.path.exists(self.csv_file_path):
            self.init_daily_row_stock_data(ticker)
        else:
            self.update_daily_row_stock_data(ticker)

        return pd.read_csv(self.csv_file_path, index_col='date', parse_dates=True)

    def init_daily_row_stock_data(self, ticker):
        """
        Initialize the stock data CSV file with historical data from Alpha Vantage.

        Args:
            ticker (str): Stock ticker symbol.
        """
        daily_adjusted_data = self.get_daily_renamed_adjusted(
            ticker, outputsize='full')

        daily_adjusted_data.to_csv(self.csv_file_path, index=True)

        print(f"Data saved to {self.csv_file_path}")

    def update_daily_row_stock_data(self, ticker):
        """
        Update the stock data CSV file with the latest data if it is outdated.

        Args:
            ticker (str): Stock ticker symbol.
        """
        df = pd.read_csv(self.csv_file_path,
                         index_col='date', parse_dates=True)

        # Get the latest date in the existing data
        last_date = df.index.max()

        nyse = mcal.get_calendar('NYSE')
        market_date_gap = nyse.schedule(
            start_date=last_date, end_date=self.now)
        
        if market_date_gap.shape[0] < 2 or (market_date_gap.shape[0] == 2 and self.now.time() < pd.Timestamp('16:30').time()):
            print(f"No new data available for {ticker}.")
        else:
            # Fetch new data from the API
            new_data = self.get_daily_renamed_adjusted(ticker)
            new_data.index = pd.to_datetime(new_data.index)

            # Get the latest date in the new data
            latest_new_date = new_data.index.max()
            
            # Filter new data to include only the rows that are more recent than the last date in the existing data
            new_data_to_add = new_data.loc[:last_date + pd.Timedelta(days=1)]
            # Concatenate the new data with the existing data
            df = pd.concat([new_data_to_add, df])
            # Save the updated dataframe back to the CSV file
            df.to_csv(self.csv_file_path)
            print(f"Data for {ticker} has been updated.")

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
        self.intraday_stock_path = os.getenv("ROW_INTRADAY_STOCK_PATH")
