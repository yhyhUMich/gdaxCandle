from datetime import datetime
import pandas as pd
from gdax import GDAX
import os


def cypto_candle(instrument, start, end, granularity):
    return GDAX(instrument).fetch(start, end, granularity)


if __name__ == "__main__":
    granularity = 5
    market = 'GDAX'
    start = datetime(2017, 1, 1)
    end = datetime(2018, 2, 20)

    ltc_data = cypto_candle('LTC-USD', start, end, granularity)
    btc_data = cypto_candle('BTC-USD', start, end, granularity)
    eth_data = cypto_candle('ETH-USD', start, end, granularity)
    
    btc_agg = btc_data.groupby('date')
    eth_agg = eth_data.groupby('date')
    ltc_agg = ltc_data.groupby('date')

    base_dir = '/home/hang/data' 
    base_dir = os.path.join(base_dir, market)
    base_dir = os.path.join(base_dir, granularity)

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    for date_btc, sub_df_btc in btc_agg:
            print(date_btc)

            year_dir = os.path.join(base_dir, str(sub_df_btc.iloc[0]['index'].year))
            if not os.path.exists(year_dir):
                os.makedirs(year_dir)

            filename = '{dir}/{mkt}{date}-{granularity}M.h5'.format(dir=year_dir, mkt=market, date=date_btc, granularity=granularity)
            store = pd.HDFStore(filename, "a")
            store['BTCUSD.GDAX'] = sub_df_btc
            store['ETHUSD.GDAX'] = eth_agg.get_group(date_btc)
            store['LTCUSD.GDAX'] = ltc_agg.get_group(date_btc)
            store.close()
