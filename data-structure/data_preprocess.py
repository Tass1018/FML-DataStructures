import csv
from datetime import datetime
from logger import logger
from binance_historical_data import BinanceDataDumper
from typing import Tuple, Union, Generator, Iterable, Optional
import pandas as pd

class DataDumper:
    from binance_historical_data import BinanceDataDumper
    def __init__(self, path_dir_where_to_dump='.', asset_class='spot', data_type='trades', data_frequency='30m'):
        self.path_dir_where_to_dump = path_dir_where_to_dump
        self.asset_class = asset_class
        self.data_type = data_type
        self.data_frequency = data_frequency
        self.data_dumper = BinanceDataDumper(
            path_dir_where_to_dump=path_dir_where_to_dump,
            asset_class=asset_class,  # spot, um, cm
            data_type=data_type,  # aggTrades, klines, trades
            data_frequency=data_frequency,
        )

    def dump(self, start: Union[Iterable[int], int], end: None):
        if isinstance(start, Iterable):
            sy, sm, sd = start
        if isinstance(start, int) and end is None:
            sy, sm, sd = start, 1, 1
        self.data_dumper.dump_data(
            tickers=['ETHUSDT'],
            date_start=datetime.date(year=sy, month=sm, day=sd),
            date_end=datetime.date(year=2023, month=9, day=28),
            is_to_update_existing=False,
        )


class BinanceTickData:
    def __init__(self):
        self.trade_id = 0
        self.price = 1
        self.qt = 2
        self.quote_qt = 3
        self.unix_time = 4
        self.maker_buying = 5
        self.exec = 6


class ExtractData:
    def __init__(self, data_src=BinanceTickData()):
        self.data_src = data_src

    def extract_and_save_tick(self, input_file, output_file, writerow=None):
        if writerow is None:
            writerow = ['Datetime', 'Price', 'Volume']

        try:
            with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
                # Initialize CSV reader and writer
                reader = csv.reader(infile)
                writer = csv.writer(outfile)

                # Write header to the output file
                writer.writerow(writerow)

                for row in reader:
                    try:
                        unix_time = int(row[self.data_src.unix_time])
                        price = row[self.data_src.price]
                        volume = row[self.data_src.qt]

                        # Convert UNIX time to Date and Time
                        dt = datetime.utcfromtimestamp(unix_time / 1000)
                        date = dt.strftime('%Y-%m-%d')
                        time = dt.strftime('%H:%M:%S.%f')[:-3]  # Include milliseconds and truncate to 3 decimal places
                        # Write the extracted data to the output file
                        writer.writerow([unix_time, price, volume])
                    except ValueError as ve:
                        logger.value_error(ve)
                    except IndexError as ie:
                        logger.index_error(ie, row)
                logger.success_save(output_file)
        except FileNotFoundError:
            logger.file_not_found(input_file)
        except Exception as e:
            logger.unexpected_error(e)


def convert_datetime(df: pd.DataFrame, to_index: bool) -> pd.DataFrame:
    """
    Convert the 'datetime' column of a DataFrame from UNIX format to standard datetime format if necessary.

    :param df: Input DataFrame.
    :return: DataFrame with converted 'datetime' column.
    """

    # Check the first entry of 'datetime' column to see if it's in UNIX format
    # UNIX timestamps are typically large integers (e.g., 1617859850 for '2021-04-08 01:57:30')
    # We'll use a simple heuristic: if it's a large integer, we assume it's UNIX timestamp
    X = df.copy()
    first_entry = X['date_time'].iloc[0]

    try:
        if isinstance(first_entry,
                      (int, float)) and first_entry > 1e9:  # 1e9 is just an arbitrary large number as a threshold
            # Convert UNIX timestamps to datetime format
            X['date_time'] = pd.to_datetime(X['date_time'], unit='ms')  # 's' specifies seconds since epoch
            print(f"Converted UNIX timestamp to datetime format: {X['date_time'].iloc[0]}")
    except:
        pass
    if to_index:
        X.set_index('date_time', inplace=True)
    return X

# Usage
# extractor = ExtractData()
# input_file = 'path_to_your_input_file.csv'
# output_file = 'path_to_your_output_file.csv'
# extractor.extract_and_save(input_file, output_file)
