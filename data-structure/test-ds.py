import unittest
import pandas as pd
import numpy as np
from DataStructures import DataStructures  # Replace with actual module name

class TestDataStructures(unittest.TestCase):

    def setUp(self):
        # Set up a random DataFrame for testing
        np.random.seed(42)
        n_rows = 1000
        self.df = pd.DataFrame({
            'Date': pd.date_range(start='1/1/2023', periods=n_rows, freq='T'),
            'Time': pd.date_range(start='1/1/2023', periods=n_rows, freq='T').time,
            'Price': np.random.rand(n_rows) * 100,
            'Volume': np.random.randint(1, 100, n_rows)
        })
        self.df.to_csv('test_data.csv', index=False)
        self.threshold = 100

    def test_get_volume_bars(self):
        volume_bars = DataStructures.get_volume_bars('test_data.csv', self.threshold)
        self.assertTrue(isinstance(volume_bars, pd.DataFrame))
        self.assertTrue('cum_volume' in volume_bars.columns)
        self.assertTrue(volume_bars['cum_volume'].iloc[-1] >= self.threshold)

    def test_get_tick_bars(self):
        tick_bars = DataStructures.get_tick_bars('test_data.csv', self.threshold)
        self.assertTrue(isinstance(tick_bars, pd.DataFrame))
        self.assertTrue('cum_ticks' in tick_bars.columns)
        self.assertTrue(tick_bars['cum_ticks'].iloc[-1] >= self.threshold)

    def test_get_dollar_bars(self):
        dollar_bars = DataStructures.get_dollar_bars('test_data.csv', self.threshold)
        self.assertTrue(isinstance(dollar_bars, pd.DataFrame))
        self.assertTrue('cum_dollar_value' in dollar_bars.columns)
        self.assertTrue(dollar_bars['cum_dollar_value'].iloc[-1] >= self.threshold)

    def tearDown(self):
        # Clean up the test data file
        import os
        os.remove('test_data.csv')

if __name__ == '__main__':
    unittest.main()
