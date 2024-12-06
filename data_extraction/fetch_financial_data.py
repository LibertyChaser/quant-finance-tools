# TODO: Replace alpha_vantage with request url
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
import pandas as pd
import os
from dotenv import load_dotenv
import pandas_market_calendars as mcal
import requests


class DataLoader:
    def __init__(self):
        load_dotenv()
        self.premium_api_key = os.getenv("ALPHA_VANTAGE_KEY")
        self.now = pd.Timestamp.now(tz='America/New_York')
        self.av_url = 'https://www.alphavantage.co/query'
        
    def get_earnings(self, ticker):
        url = (
            f'{self.av_url}?function=EARNINGS&symbol={ticker}&apikey={self.premium_api_key}')
        response = requests.get(url)
        data = response.json()
        return data


class FundamentalDataLoader(DataLoader):
    def __init__(self):
        super().__init__()
        self.fd = FundamentalData(self.premium_api_key)
        
        self.compressed_financial_reports_path = os.getenv(
            "FIN_DATA_COMPRESSED_FINANCIAL_REPORTS_PATH")
        self.compressed_company_overview_path = os.getenv(
            "FIN_DATA_COMPRESSED_COMPANY_OVERVIEW_PATH")
        self.compressed_company_eaernings_path = os.getenv(
            "FIN_DATA_COMPRESSED_COMPANY_EARNINGS_PATH")
        self.compressed_company_news_path = os.getenv(
            "FIN_DATA_COMPRESSED_COMPANY_NEWS_PATH")
        
        self.compressed_report_file_path = None
        
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

    def load_company_overview(self, ticker, update=False):
        """
        Get the company overview data for the given ticker.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            dict: Dictionary containing the company overview data.
        """
        compressed_company_overview_file_path = os.path.join(
            self.compressed_company_overview_path, f'{ticker}.gz')
        if not update and os.path.exists(compressed_company_overview_file_path):
            data = pd.read_csv(compressed_company_overview_file_path, index_col=0)
            return data
        else:
            data, meta_data = self.fd.get_company_overview(symbol=ticker)
            data_df = pd.DataFrame.from_dict(data, orient='index')
            data_df.to_csv(compressed_company_overview_file_path, index=True, compression='gzip')
            return data_df
        
    def load_company_earnings(self, ticker, time_period='quarterly', update=False):
        compressed_company_earnings_file_path = os.path.join(
            self.compressed_company_eaernings_path, f'{ticker}_{time_period}_earnings.gz')
        if not update and os.path.exists(compressed_company_earnings_file_path):
            data = pd.read_csv(
                compressed_company_earnings_file_path, index_col='fiscalDateEnding', parse_dates=True)
            return data
        else:
            data = self.get_earnings(ticker)
            quart_df = pd.DataFrame(data['quarterlyEarnings'])
            quart_df.set_index('fiscalDateEnding', inplace=True)
            quart_df.index = pd.to_datetime(quart_df.index)
            quart_path = os.path.join(
                self.compressed_company_eaernings_path, f'{ticker}_quarterly_earnings.gz')
            quart_df.to_csv(quart_path, index=True, compression='gzip')
            
            annual_df = pd.DataFrame(data['annualEarnings'])
            annual_df.set_index('fiscalDateEnding', inplace=True)
            annual_df.index = pd.to_datetime(annual_df.index)
            annual_path = os.path.join(
                self.compressed_company_eaernings_path, f'{ticker}_annual_earnings.gz')
            annual_df.to_csv(annual_path, index=True, compression='gzip')
            
            if 'quarterly' in time_period:
                return quart_df
            elif 'annual' in time_period:
                return annual_df
        
    def get_company_news(self, ticker, update=False):
        pass

    def load_financial_reports(self, ticker, time_period, report_type, begin_date='2020-01-01', end_date='2021-01-01'):
        """
        Load the financial reports for the given report type and time period.

        Args:
            report_type (str): Type of financial report to load.
            time_period (str): Time period for the financial report.
            begin_date (str): Start date for the financial report data.
            end_date (str): End date for the financial report data.

        Returns:
            DataFrame: Pandas DataFrame containing the financial report data.
        """

        self.compressed_report_file_path = os.path.join(
            self.compressed_financial_reports_path, f'{ticker}_{time_period}_{report_type}.gz')

        if not os.path.exists(self.compressed_report_file_path):
            self.init_financial_reports(ticker, time_period, report_type)

        fin_report = pd.read_csv(
            self.compressed_report_file_path, index_col='fiscalDateEnding', parse_dates=True)

        if time_period == 'quarterly':
            fin_report['reportedDate'] = pd.to_datetime(fin_report['reportedDate'])
            last_date = fin_report['reportedDate'].max()
        elif time_period == 'annual':
            last_date = fin_report.index.max()
            
        if pd.Timestamp(end_date) > last_date:
            today_date = pd.Timestamp.now()
            date_diff = (today_date - last_date).days
            if (time_period == 'annual' and date_diff > 365) or (time_period == 'quarterly' and date_diff > 94):
                # self.update_financial_reports(ticker, time_period, report_type)
                # print(f'Updated {ticker} {time_period} {report_type} data.')
                fin_report = pd.read_csv(
                    self.compressed_report_file_path, index_col='fiscalDateEnding', parse_dates=True)

        fin_report = fin_report.sort_index().loc[begin_date:end_date].sort_index(ascending=False)
        
        fin_report = fin_report[~fin_report.index.duplicated(keep='first')]
        fin_report['reportedDate'] = pd.to_datetime(fin_report['reportedDate'])
        return fin_report

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
            report, _ = data_function(ticker)
        else:
            raise ValueError(
                f"Invalid report type '{report_type}' or time period '{time_period}'")
            
        earnings = self.load_company_earnings(ticker, time_period)
        
        report.set_index('fiscalDateEnding', inplace=True)
        report.index = pd.to_datetime(report.index)
        
        for col in earnings.columns:
            if col not in report.columns:
                report[col] = None
        
        for index, row in report.iterrows():
            rounded_index = index + pd.offsets.MonthEnd(0)
            if rounded_index in earnings.index:
                report.loc[index, earnings.columns] = earnings.loc[rounded_index].values
            else:
                rounded_index = index + pd.offsets.MonthEnd(-1)
                if rounded_index in earnings.index:
                    report.loc[index, earnings.columns] = earnings.loc[rounded_index].values

        report.to_csv(self.compressed_report_file_path, index=True, compression='gzip')

        print(f'Data saved to {self.compressed_report_file_path}')

    def update_financial_reports(self, ticker, time_period, report_type):
        """
        Update the financial reports CSV file with the latest data if it is outdated.

        Args:
            ticker (str): Stock ticker symbol.
            report_type (str): Type of financial report to load.
        """
        # TODO: Update the function with update of earnings
        curr_fin_report_df = pd.read_csv(self.compressed_report_file_path,
                                         index_col='fiscalDateEnding', parse_dates=True)

        # Get the latest date in the existing data
        last_date = curr_fin_report_df.index.max()

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
        new_date_last_date = new_data.index.max()

        # If the latest date in new data is more recent than the last date in the existing data, append the new data
        if new_date_last_date > last_date:
            # Filter new data to include only the rows that are more recent than the last date in the existing data
            new_data_to_add = new_data.loc[last_date:]
            # Concatenate the new data with the existing data
            curr_fin_report_df = pd.concat(
                [new_data_to_add, curr_fin_report_df])
            # Save the updated dataframe back to the CSV file
            curr_fin_report_df.to_csv(
                self.compressed_report_file_path, index=False, compression='gzip')
            print(f"Data for {ticker} has been updated.")
        else:
            print(
                f"No new data available for {ticker} {time_period} {report_type}. Last date: {last_date}")


class StockDataLoader(DataLoader):
    def __init__(self):
        super().__init__()
        self.ts = TimeSeries(key=self.premium_api_key, output_format='pandas')


class DailyStockDataLoader(StockDataLoader):
    def __init__(self):
        super().__init__()
        self.compressed_daily_stock_path = os.getenv(
            "FIN_DATA_COMPRESSED_DAILY_STOCK_PATH")
        self.gz_file_path = None

    def load_daily_stock_data(self, ticker, begin_date='2020-01-01', end_date='2021-01-01'):
        """
        Fetch historical stock data for the given ticker, limited to the last 'n' years.

        Args:
            ticker (str): Stock ticker symbol.
            begin_date (str): Start date for the historical stock data.
            end_date (str): End date for the historical stock data.

        Returns:
            DataFrame: Pandas DataFrame containing the historical stock data.
        """
        
        self.gz_file_path = os.path.join(
            self.compressed_daily_stock_path, f'{ticker}.gz')

        if not os.path.exists(self.gz_file_path):
            self.init_daily_stock_data(ticker)

        data = pd.read_csv(
            self.gz_file_path, index_col='date', parse_dates=True)
        
        last_date = data.index.max()
        if pd.Timestamp(end_date) > last_date:
            self.update_daily_stock_data(ticker)
            data = pd.read_csv(
                self.gz_file_path, index_col='date', parse_dates=True)
            
        data = data.sort_index().loc[begin_date:end_date].sort_index(
            ascending=False)
        return data

    def init_daily_stock_data(self, ticker):
        """
        Initialize the stock data CSV file with historical data from Alpha Vantage.

        Args:
            ticker (str): Stock ticker symbol.
        """
        daily_adjusted_data = self.get_daily_renamed_adjusted(
            ticker, outputsize='full')

        daily_adjusted_data.to_csv(self.gz_file_path, index=True, compression='gzip')

        print("Current time: ", self.now)
        print(f"Data saved to {self.gz_file_path}")
        print(
            f"Data Date Range: {daily_adjusted_data.index.min()} to {daily_adjusted_data.index.max()}")

    def update_daily_stock_data(self, ticker):
        """
        Update the stock data CSV file with the latest data if it is outdated.

        Args:
            ticker (str): Stock ticker symbol.
        """
        df = pd.read_csv(self.gz_file_path,
                         index_col='date', parse_dates=True)

        # Get the latest date in the existing data
        current_last_date = df.index.max()

        nyse = mcal.get_calendar('NYSE')
        market_date_gap = nyse.schedule(
            start_date=current_last_date, end_date=self.now)

        print("Current time: ", self.now)
        if market_date_gap.shape[0] < 2 or (market_date_gap.shape[0] == 2 and self.now.time() < pd.Timestamp('16:30').time()):
            print(f"No new data available for {ticker}.")
        else:
            # Fetch new data from the API
            new_data = self.get_daily_renamed_adjusted(ticker)
            new_data.index = pd.to_datetime(new_data.index)

            # Get the latest date in the new data
            latest_new_date = new_data.index.max()

            # Filter new data to include only the rows that are more recent than the last date in the existing data
            new_data_to_add = new_data.loc[:current_last_date +
                                           pd.Timedelta(days=1)]
            # Concatenate the new data with the existing data
            df = pd.concat([new_data_to_add, df])
            # Save the updated dataframe back to the CSV file
            df.to_csv(self.gz_file_path, index=True, compression='gzip')

            print(f"Data for {ticker} has been updated.")
            print(
                f"Updated Data Date Range: {new_data_to_add.index.min()} to {new_data_to_add.index.max()}")

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
        self.intraday_stock_path = os.getenv("RAW_INTRADAY_STOCK_PATH")
