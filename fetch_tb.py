from datetime import datetime
import pandas as pd
from gdax import GDAX
import os


def cypto_candle(instrument, start, end, granularity):
    return GDAX(instrument).fetch_tb(start, end, granularity)


if __name__ == "__main__":
    granularity = 1
    market = 'GDAX'
    start = datetime(2017, 4, 1)
    end = datetime(2018, 2, 27)

    ltc_data = cypto_candle('LTC-USD', start, end, granularity)
    btc_data = cypto_candle('BTC-USD', start, end, granularity)
    eth_data = cypto_candle('ETH-USD', start, end, granularity)

    base_dir = '/home/hang/data/tb'
    base_dir = os.path.join(base_dir, market)

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    btc_filename = '{dir}/{instrument}({granularity}M).csv'.format(dir=base_dir, instrument="btcusd", granularity=granularity)
    ltc_filename = '{dir}/{instrument}({granularity}M).csv'.format(dir=base_dir, instrument="ltcusd", granularity=granularity)
    eth_filename = '{dir}/{instrument}({granularity}M).csv'.format(dir=base_dir, instrument="ethusd", granularity=granularity)

    btc_data.to_csv(btc_filename, sep=',', encoding='utf-8', header=False, index=False)
    ltc_data.to_csv(ltc_filename, sep=',', encoding='utf-8', header=False, index=False)
    eth_data.to_csv(eth_filename, sep=',', encoding='utf-8', header=False, index=False)
