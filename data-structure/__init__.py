from DataStructures import DataStructures
from DataStructures import *
from data_preprocess import *
from binance.client import Client
# client configuration

def main():
    threshold_dollar = 10000
    threshold_volume = 1000
    threshold_tick = 100

    dollar_bar = RealTimeBars(threshold=threshold_dollar, bar_type='dollar')
    volume_bar = RealTimeBars(threshold=threshold_volume, bar_type='volume')
    tick_bar = RealTimeBars(threshold=threshold_tick, bar_type='tick')

    ws_trades(dollar_bar, volume_bar, tick_bar)


if __name__ == "__main__":
    main()
