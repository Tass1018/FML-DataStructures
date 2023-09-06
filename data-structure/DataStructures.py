import pandas as pd


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
                   'high', 'low', 'close', 'volume',
                   'cum_buy_volume', 'cum_dollar_value']
        bar_df = pd.DataFrame(bars, columns=columns)

        if to_csv:
            bar_df.to_csv(output_path, index=False)

        return bar_df
