from process_brokerage_data.securities_data import SecuritiesData
from process_brokerage_data.securities_data_types import PriceHistoryOptions
from datetime import datetime
import pandas as pd
import pandas_market_calendars as mcal


class Indicators:
    def __init__(self):
        self.sec_data = SecuritiesData()
        self.memoized_data = {}

    @staticmethod
    def _lookback_date(num_days):
        """Convenience method to find date num_days back excluding holidays"""
        num_days -= 1
        today = pd.Timestamp.now().tz_localize('UTC').normalize()
        start_date = pd.to_datetime('2000-01-01').tz_localize('UTC').normalize()
        nyse = mcal.get_calendar('NYSE')
        valid_days = nyse.valid_days(start_date=start_date, end_date=today)
        # Check if today is business day
        if pd.Timestamp(today) not in valid_days:
            today = valid_days[valid_days < today][-1]

        # Find the index of today in valid_days
        today_idx = valid_days.get_loc(today)

        # Calculate start date by moving back num_days trading days
        if today_idx >= num_days:
            start_date = valid_days[today_idx - num_days]
        else:
            raise ValueError("Not enough trading days in the data")
        return {
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": today.strftime('%Y-%m-%d')
        }

    def daily_moving_average(self, symbol, window, live_data=None):
        """Find the moving average of a given symbol"""
        today = datetime.today().strftime('%Y-%m-%d')
        date = self._lookback_date(window)
        price_options: PriceHistoryOptions = {
            "periodType": "year",
            "period": 1,
            "frequencyType": "daily",
            "frequency": 1,
            "extendedHours": False,
            "needPreviousClose": False,
            "startDate": date["start_date"],
            "endDate": date["end_date"]
        }
        key = f"{today}_{symbol}_{window}"
        if f"{today}_{symbol}_{window}" not in self.memoized_data:
            data = self.sec_data.price_history(symbol=symbol, price_history_options=price_options)
            ohlc_dataframe = pd.DataFrame(data['candles'])
            self.memoized_data[key] = ohlc_dataframe
        else:
            ohlc_dataframe = self.memoized_data[key]
        return ohlc_dataframe['close'].mean()
