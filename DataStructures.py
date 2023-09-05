import pandas as pd

class DataStructures:
    
    @staticmethod
    def get_volume_bars(file_path, threshold, batch_size=1000000, verbose=False, to_csv=False, output_path=None):
        """
        Generate the volume bars from a given CSV file.

        Parameters:    
            file_path (str): The path to the CSV file.
            threshold (int): The threshold volume to create a new bar.
            batch_size (int, optional): The number of rows to read from the CSV file at a time. Defaults to 1000000.
            verbose (bool, optional): Whether to print the created bars. Defaults to False.
            to_csv (bool, optional): Whether to save the generated bars to a CSV file. Defaults to False.
            output_path (str, optional): The path to save the generated bars CSV file. Defaults to None.

        Returns:
            pandas.DataFrame: The volume bars containing the open, high, low, close prices, volume, cumulative buy volume, cumulative ticks, and cumulative dollar value.
        """
        # Read the CSV file in chunks
        chunk_iter = pd.read_csv(file_path, chunksize=batch_size)
        
        # Initialize variables
        volume_accumulated = 0
        bars = []
        cum_buy_volume = 0
        cum_ticks = 0
        cum_dollar_value = 0
        
        for chunk in chunk_iter:
            for index, row in chunk.iterrows():
                volume_accumulated += row['Volume']
                cum_buy_volume += row['Volume']  # Assuming all are buy for this example
                cum_ticks += 1
                cum_dollar_value += row['Price'] * row['Volume']
                
                if volume_accumulated >= threshold:
                    open_price = chunk.loc[chunk.index[0], 'Price']
                    high_price = chunk['Price'].max()
                    low_price = chunk['Price'].min()
                    close_price = row['Price']
                    
                    bars.append([row['Date'], row['Time'], cum_ticks, open_price, high_price, low_price, close_price, volume_accumulated, cum_buy_volume, cum_dollar_value])
                    
                    # Reset volume_accumulated
                    volume_accumulated = 0
                    
                    if verbose:
                        print(f"Bar created: {bars[-1]}")
        
        # Create a DataFrame from the bars
        columns = ['date_time', 'tick_num', 'open', 'high', 'low', 'close', 'volume', 'cum_buy_volume', 'cum_ticks', 'cum_dollar_value']
        bar_df = pd.DataFrame(bars, columns=columns)
        
        if to_csv:
            bar_df.to_csv(output_path, index=False)
        
        return bar_df

# Example usage
data_structures = DataStructures()
data_structures.get_volume_bars('./raw_tick_data.csv', threshold=28000, verbose=True, to_csv=True, output_path='./volume_bars.csv')
