import pandas as pd

import pandas as pd
import threading
import time

class RealTimeBars:
    def __init__(self, threshold, bar_type, batch_size=1024):
        self.threshold = threshold
        self.bar_type = bar_type
        self.accumulated = 0
        self.bars = []
        self.cum_buy_volume = 0
        self.cum_ticks = 0
        self.cum_vol_value = 0
        self.open_price = self.bucket_low_price = self.bucket_high_price = 9999999999
        self.next_bar_start = True
        self.lock = threading.Lock()
        self.batch_size = batch_size

    def handle_trade(self, trade):
        with self.lock:
            if self.next_bar_start:
                self.open_price = self.bucket_low_price = self.bucket_high_price = trade['Price']
                self.next_bar_start = False
            if trade['Price'] < self.bucket_low_price:
                self.bucket_low_price = trade['Price']
            if trade['Price'] > self.bucket_high_price:
                self.bucket_high_price = trade['Price']

            if self.bar_type == 'dollar':
                self.accumulated += trade['Price'] * trade['Volume']
            elif self.bar_type == 'volume':
                self.accumulated += trade['Volume']
            elif self.bar_type == 'tick':
                self.accumulated += 1

            self.cum_buy_volume += trade['Volume']
            self.cum_ticks += 1
            self.cum_vol_value += trade['Volume']

            if self.accumulated >= self.threshold:
                high_price = self.bucket_high_price
                low_price = self.bucket_low_price
                close_price = trade['Price']

                self.bars.append(
                    [trade['Date'], trade['Time'], self.cum_ticks, self.open_price, high_price, low_price, close_price,
                     self.accumulated, self.cum_buy_volume, self.cum_vol_value])
                self.next_bar_start = True
                self.accumulated = 0
                print(f"{self.bar_type.capitalize()} Bar created: {self.bars[-1]}")

    def get_bars(self):
        columns = ['date', 'time', 'cum_ticks', 'open',
                   'high', 'low', 'close', f'cum_{self.bar_type}_value',
                   'cum_buy_volume', 'cum_volume']
        return pd.DataFrame(self.bars, columns=columns)


class DataStructures:
    @staticmethod
    def get_volume_bars(file_path, threshold, batch_size=1000000, verbose=False, to_csv=False, output_path=None):
        # Read the CSV file in chunks
        chunk_iter = pd.read_csv(file_path, chunksize=batch_size)

        # Initialize variables a a
        volume_accumulated = 0
        bars = []
        cum_buy_volume = 0
        cum_ticks = 0
        cum_dollar_value = 0
        open_price, bucket_low_price, bucket_high_price = 9999999999, 9999999999, 9999999999
        next_vol_start = True

        for chunk in chunk_iter:
            for index, row in chunk.iterrows():
                if next_vol_start is True:
                    open_price, bucket_low_price, bucket_high_price = row['Price'], row['Price'], row['Price']
                    next_vol_start = False
                if row['Price'] < bucket_low_price:
                    bucket_low_price = row['Price']
                if row['Price'] > bucket_high_price:
                    bucket_high_price = row['Price']
                volume_accumulated += row['Volume']
                cum_buy_volume += row['Volume']  # Assuming all are buy for this
                cum_ticks += 1
                cum_dollar_value += row['Price'] * row['Volume']

                if volume_accumulated >= threshold:
                    # open_price = chunk.loc[chunk.index[0], 'Price']
                    high_price = bucket_high_price
                    low_price = bucket_low_price
                    close_price = row['Price']

                    bars.append([row['Date'], row['Time'], cum_ticks, open_price, high_price, low_price, close_price,
                                 volume_accumulated, cum_buy_volume, cum_dollar_value])
                    # Reset
                    next_vol_start = True
                    volume_accumulated = 0
                    if verbose:
                        print(f"Bar created: {bars[-1]}")

        # Create a DataFrame from the bars
        columns = ['date', 'time', 'cum_ticks', 'open',
                   'high', 'low', 'close', 'cum_volume',
                   'cum_buy_volume', 'cum_dollar_value']
        bar_df = pd.DataFrame(bars, columns=columns)

        if to_csv:
            bar_df.to_csv(output_path, index=False)

        return bar_df

    @staticmethod
    def get_tick_bars(file_path, threshold, batch_size=1000000, verbose=False, to_csv=False, output_path=None):
        # Read the CSV file in chunks
        chunk_iter = pd.read_csv(file_path, chunksize=batch_size)

        # Initialize variables a a
        ticks_accumulated = 0
        bars = []
        cum_buy_volume = 0
        cum_vol = 0
        cum_dollar_value = 0
        open_price, bucket_low_price, bucket_high_price = 9999999999, 9999999999, 9999999999
        next_tick_start = True

        for chunk in chunk_iter:
            for index, row in chunk.iterrows():
                if next_tick_start is True:
                    open_price, bucket_low_price, bucket_high_price = row['Price'], row['Price'], row['Price']
                    next_tick_start = False
                if row['Price'] < bucket_low_price:
                    bucket_low_price = row['Price']
                if row['Price'] > bucket_high_price:
                    bucket_high_price = row['Price']
                ticks_accumulated += 1
                cum_buy_volume += row['Volume']  # Assuming all are buy for this
                cum_vol += row['Volume']
                cum_dollar_value += row['Price'] * row['Volume']

                if ticks_accumulated >= threshold:
                    # open_price = chunk.loc[chunk.index[0], 'Price']
                    high_price = bucket_high_price
                    low_price = bucket_low_price
                    close_price = row['Price']

                    bars.append([row['Date'], row['Time'], cum_vol, open_price, high_price, low_price, close_price,
                                 ticks_accumulated, cum_buy_volume, cum_dollar_value])
                    # Reset
                    next_tick_start = True
                    ticks_accumulated = 0
                    # if verbose:
                    #     print(f"Bar created: {bars[-1]}")

        # Create a DataFrame from the bars
        columns = ['date', 'time', 'volume', 'open',
                   'high', 'low', 'close', 'cum_ticks',
                   'cum_buy_volume', 'cum_dollar_value']
        bar_df = pd.DataFrame(bars, columns=columns)

        if to_csv:
            bar_df.to_csv(output_path, index=False)
        return bar_df

    @staticmethod
    def get_dollar_bars(file_path, threshold, batch_size=1000000, verbose=False, to_csv=False, output_path=None):
        # Read the CSV file in chunks
        start_time = time.time()
        chunk_iter = pd.read_csv(file_path, chunksize=batch_size)

        dollar_accumulated = 0
        bars = []
        cum_buy_volume = 0
        cum_ticks = 0
        cum_vol_value = 0
        open_price, bucket_low_price, bucket_high_price = 9999999999, 9999999999, 9999999999
        next_dollar_start = True

        for chunk in chunk_iter:
            for index, row in chunk.iterrows():
                if next_dollar_start is True:
                    open_price, bucket_low_price, bucket_high_price = row['Price'], row['Price'], row['Price']
                    next_dollar_start = False
                if row['Price'] < bucket_low_price:
                    bucket_low_price = row['Price']
                if row['Price'] > bucket_high_price:
                    bucket_high_price = row['Price']
                dollar_accumulated += row['Price'] * row['Volume']
                cum_buy_volume += row['Volume']  # Assuming all are buy for this
                cum_ticks += 1
                cum_vol_value += row['Volume']

                if dollar_accumulated >= threshold:
                    high_price = bucket_high_price
                    low_price = bucket_low_price
                    close_price = row['Price']

                    bars.append([row['Date'], row['Time'], cum_ticks, open_price, high_price, low_price, close_price,
                                 dollar_accumulated, cum_buy_volume, cum_vol_value])
                    # Reset
                    next_dollar_start = True
                    dollar_accumulated = 0
                    # if verbose:
                    #     print(f"Bar created: {bars[-1]}")

        # Create a DataFrame from the bars
        columns = ['date', 'time', 'cum_ticks', 'open',
                   'high', 'low', 'close', 'cum_dollar_value',
                   'cum_buy_volume', 'cum_volume']
        bar_df = pd.DataFrame(bars, columns=columns)

        if to_csv:

            bar_df.to_csv(output_path, index=False)
            end_time = time.time()
            elapsed_time = (end_time - start_time)  # converting seconds to milliseconds

            print(f"Time taken to generate bars: {elapsed_time:.2f} seconds")
            return None

        end_time = time.time()
        elapsed_time = (end_time - start_time)  # converting seconds to milliseconds

        print(f"Time taken to generate bars: {elapsed_time:.2f} seconds")

        return bar_df
