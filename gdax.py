from datetime import datetime, timedelta
from time import sleep

import pandas
import requests


class GDAX(object):
    """Class for fetching candle data for a given currency pair."""

    def __init__(self, pair):
        """Create the exchange object.

        Args:
        pair (str): Examples: 'BTC-USD', 'ETH-USD'...
        """
        self.pair = pair
        self.uri = 'https://api.gdax.com/products/{pair}/candles'.format(pair=self.pair)
        self.market = 'GDAX'

    @staticmethod
    def __date_to_iso8601(date):
        """Convert a datetime object to the ISO-8601 date format (expected by the GDAX API).

        Args:
        date (datetime): The date to be converted.

        Returns:
        str: The ISO-8601 formatted date.
        """

        utc_time = '{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}'.format(
            year=date.year,
            month=date.month,
            day=date.day,
            hour=date.hour,
            minute=date.minute,
            second=date.second)

        return utc_time

    @staticmethod
    def __store_hdf(df, mkt, date, granularity):
        pass

    @staticmethod
    def __unix_to_datetime(ut):
        return datetime.fromtimestamp(ut)

    @staticmethod
    def __unix_to_utcdatetime(ut):
        return datetime.utcfromtimestamp(ut)

    @staticmethod
    def __datetime_to_str(dt):
        return dt.strftime("%Y/%m/%d %H:%M")

    @staticmethod
    def __datetime_to_intdate(dt):
        return int(dt.strftime("%Y%m%d"))

    @staticmethod
    def __datetime_to_inttime(dt):
        return int(dt.strftime("%H%M%S"))

    @staticmethod
    def __float_to_int(num):
        return int(num * 10000)

    def request_slice(self, start, end, granularity):
        # Allow 3 retries (we might get rate limited).
        retries = 3
        for retry_count in range(0, retries):
            # From https://docs.gdax.com/#get-historic-rates the response is in the format:
            # [[time, low, high, open, close, volume], ...]
            response = requests.get(self.uri, {
                'start': GDAX.__date_to_iso8601(start),
                'end': GDAX.__date_to_iso8601(end),
                'granularity': granularity * 60  # GDAX API granularity is in seconds.
            })

            if response.status_code != 200 or not len(response.json()):
                if retry_count + 1 == retries:
                    raise Exception('Failed to get exchange data for ({}, {})!'.format(start, end))
                else:
                    # Exponential back-off.
                    sleep(1.5 * retry_count)
            else:
                # Sort the historic rates (in ascending order) based on the timestamp.
                result = sorted(response.json(), key=lambda x: x[0])
                return result

    def fetch(self, start, end, granularity):
        """Fetch the candle data for a given range and granularity.

        Args:
        start (datetime): The start of the date range.
        end (datetime): The end of the date range (excluded).
        granularity (int): The granularity of the candles data (in minutes).

        Returns:
        (pandas.DataFrame): A data frame of the OHLC and volume information, indexed by their unix timestamp.
        """
        data = []

        # We will fetch the candle data in windows of maximum 100 items.
        # GDAX has a limit of returning maximum of 200, per request.
        delta = timedelta(minutes=granularity * 100)

        slice_start = start
        while slice_start != end:
            print(slice_start)
            slice_end = min(slice_start + delta, end)
            data += self.request_slice(slice_start, slice_end, granularity)
            slice_start = slice_end

        data_frame = pandas.DataFrame(data=data, columns=['time', 'low', 'high', 'open', 'close', 'volume'])

        data_frame = data_frame.assign(settle=0, oi=0, turnover=0, total_volume=0, total_turnover=0,
                                       date=0, trade_date=0, index=0,
                                       askprice1=0, askprice2=0, askprice3=0, askprice4=0, askprice5=0,
                                       bidprice1=0, bidprice2=0, bidprice3=0, bidprice4=0, bidprice5=0,
                                       askvolume1=0, askvolume2=0, askvolume3=0, askvolume4=0, askvolume5=0,
                                       bidvolume1=0, bidvolume2=0, bidvolume3=0, bidvolume4=0, bidvolume5=0)

        data_frame['low'] = data_frame['low'].apply(GDAX.__float_to_int)
        data_frame['high'] = data_frame['high'].apply(GDAX.__float_to_int)
        data_frame['open'] = data_frame['open'].apply(GDAX.__float_to_int)
        data_frame['close'] = data_frame['close'].apply(GDAX.__float_to_int)

        data_frame['index'] = data_frame['time'].apply(GDAX.__unix_to_datetime)
        data_frame['time'] = data_frame['index'].apply(GDAX.__datetime_to_inttime)
        data_frame['date'] = data_frame['index'].apply(GDAX.__datetime_to_intdate)
        data_frame['trade_date'] = data_frame['index'].apply(GDAX.__datetime_to_intdate)

        # data_frame.set_index('index', inplace=True)

        return data_frame

    def fetch_tb(self, start, end, granularity):
        """Fetch the candle data for the using of tb for a given range and granularity.

        Args:
        start (datetime): The start of the date range. (in utc time zone)
        end (datetime): The end of the date range (excluded in utc time zone).
        granularity (int): The granularity of the candles data (in minutes).

        Returns:
        (pandas.DataFrame): A data frame of the OHLC and volume information, indexed by their unix timestamp.
        """
        data = []

        # We will fetch the candle data in windows of maximum 100 items.
        # GDAX has a limit of returning maximum of 200, per request.
        delta = timedelta(minutes=granularity * 100)

        slice_start = start
        while slice_start != end:
            print(slice_start)
            slice_end = min(slice_start + delta, end)
            data += self.request_slice(slice_start, slice_end, granularity)
            slice_start = slice_end

        data_frame = pandas.DataFrame(data=data, columns=['time', 'low', 'high', 'open', 'close', 'volume'])
        data_frame = data_frame.assign(oi=0)

        data_frame['time'] = data_frame['time'].apply(GDAX.__unix_to_utcdatetime)
        data_frame['time'] = data_frame['time'].apply(GDAX.__datetime_to_str)

        newdf = data_frame[['time', 'open', 'high', 'low', 'close']]

        return newdf
