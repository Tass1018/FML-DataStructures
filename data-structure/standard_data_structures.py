"""
Advances in Financial Machine Learning, Marcos Lopez de Prado
Chapter 2: Financial Data Structures

This module contains the functions to help users create structured financial data from raw unstructured data,
in the form of time, tick, volume, and dollar bars.

These bars are used throughout the text book (Advances in Financial Machine Learning, By Marcos Lopez de Prado, 2018,
pg 25) to build the more interesting features for predicting financial time series data.

These financial data structures have better statistical properties when compared to those based on fixed time interval
sampling. A great paper to read more about this is titled: The Volume Clock: Insights into the high frequency paradigm,
Lopez de Prado, et al.

Many of the projects going forward will require Dollar and Volume bars.
"""

# Imports
from typing import Union, Iterable, Optional
import numpy as np
import pandas as pd
import time
from base_bars import BaseBars


class StandardBars(BaseBars):
    def __init__(self, metric: str, threshold: int = 50000, batch_size: int = 20000000):
        """
        Constructor

        :param metric: (str) Type of run bar to create. Example: "dollar_run"
        :param threshold: (int) Threshold at which to sample
        :param batch_size: (int) Number of rows to read in from the csv, per batch
        """
        super().__init__(metric, batch_size)
        self.threshold = threshold
        self.cum_dollar_value = 0
        self.cum_ticks = 0
        self.cum_volume = 0
        self.open_price = None
        self.high_price = None
        self.low_price = None

    def _reset_cache(self):
        """
        Implementation of abstract method _reset_cache for standard bars
        """
        self.cum_dollar_value = 0
        self.cum_ticks = 0
        self.cum_volume = 0
        self.open_price = None
        self.high_price = None
        self.low_price = None

    def _extract_bars(self, data: Union[list, tuple, np.ndarray]) -> list:
        """
        For loop which compiles the various bars: dollar, volume, or tick.

        :param data: (tuple) Contains 3 columns - date_time, price, and volume.
        :return: (list) Extracted bars
        """
        bars = []
        for index, row in data.iterrows():
            date_time, price, volume = row
            self.cum_dollar_value += price * volume
            self.cum_ticks += 1
            self.cum_volume += volume

            if self.open_price is None:
                self.open_price = price

            self._update_high_low(price)
            if (self.metric == 'dollar' and self.cum_dollar_value >= self.threshold) or (
                    self.metric == 'volume' and self.cum_volume >= self.threshold) or (
                    self.metric == 'tick' and self.cum_ticks >= self.threshold):
                self._create_bars(date_time, price, bars)
                self._reset_cache()

        return bars


def get_dollar_bars(file_path_or_df: Union[str, Iterable[str], pd.DataFrame],
                    threshold: Union[int, pd.Series] = 70000000,
                    batch_size: int = 1024, verbose: bool = True,
                    to_csv: bool = False, output_path: Optional[str] = None, timer: bool = False) -> pd.DataFrame:
    """
    Creates the dollar bars: date_time, open, high, low, close, volume, cum_buy_volume, cum_ticks, cum_dollar_value.

    :param file_path_or_df: (str, iterable of str, or pd.DataFrame) Path to the csv file(s) or Pandas Data Frame containing raw tick data
                            in the format[date_time, price, volume]
    :param threshold: (float, or pd.Series) A cumulative value above this threshold triggers a sample to be taken.
    :param batch_size: (int) The number of rows per batch. Less RAM = smaller batch size.
    :param verbose: (bool) Print out batch numbers (True or False)
    :param to_csv: (bool) Save bars to csv after every batch run (True or False)
    :param output_path: (str) Path to csv file, if to_csv is True
    :return: (pd.DataFrame) Dataframe of dollar bars
    """
    # Initialize the bar creation object
    dollar_bars_generator = StandardBars(metric='dollar', threshold=threshold, batch_size=batch_size)
    start_time = time.time()
    # Generate the bars
    bars_df = dollar_bars_generator.batch_run(file_path_or_df, verbose=verbose, to_csv=to_csv, output_path=output_path)
    end_time = time.time()
    elapsed_time = (end_time - start_time)  # converting seconds to milliseconds
    if timer:
        print(f"Time taken to generate bars: {elapsed_time:.2f} seconds")
    return bars_df


def get_volume_bars(file_path_or_df: Union[str, Iterable[str], pd.DataFrame],
                    threshold: Union[int, pd.Series] = 70000000,
                    batch_size: int = 1024, verbose: bool = True,
                    to_csv: bool = False, output_path: Optional[str] = None, timer: bool = False) -> pd.DataFrame:
    # Initialize the bar creation object
    volume_bars_generator = StandardBars(metric='volume', threshold=threshold, batch_size=batch_size)
    start_time = time.time()
    # Generate the bars
    bars_df = volume_bars_generator.batch_run(file_path_or_df, verbose=verbose, to_csv=to_csv, output_path=output_path)
    end_time = time.time()
    elapsed_time = (end_time - start_time)  # converting seconds to milliseconds
    if timer:
        print(f"Time taken to generate bars: {elapsed_time:.2f} seconds")
    return bars_df


def get_tick_bars(file_path_or_df: Union[str, Iterable[str], pd.DataFrame],
                  threshold: Union[int, pd.Series] = 70000000,
                  batch_size: int = 1024, verbose: bool = True,
                  to_csv: bool = False, output_path: Optional[str] = None, timer: bool = False) -> pd.DataFrame:

    # Initialize the bar creation object
    tick_bars_generator = StandardBars(metric='tick', threshold=threshold, batch_size=batch_size)
    start_time = time.time()
    # Generate the bars
    bars_df = tick_bars_generator.batch_run(file_path_or_df, verbose=verbose, to_csv=to_csv, output_path=output_path)
    end_time = time.time()
    elapsed_time = (end_time - start_time)  # converting seconds to milliseconds
    if timer:
        print(f"Time taken to generate bars: {elapsed_time:.2f} seconds")
    return bars_df
