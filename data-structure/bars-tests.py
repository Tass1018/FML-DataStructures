import unittest
import pandas as pd
from base_bars import _crop_data_frame_in_batches
small_tick_fd = 0
mid_tick_fd = './raw-data/tick_data.csv'
big_tick_fd = 0

fd = [small_tick_fd, mid_tick_fd, big_tick_fd]

class base_bars(unittest.TestCase):
    def test__crop_data_frame_in_batches(self):
        df = pd.read_csv(mid_tick_fd)
        batch_list = _crop_data_frame_in_batches(df, 5000)
        res = int(df.shape[0] / 5000) + 1
        self.assertEqual(res, len(batch_list))

    def test_null_data(self):
        self.assertEqual(True, True)  # add assertion here


    def test_open_close_price(self):
        self.assertEqual(True, True)  # add assertion here

if __name__ == '__main__':
    unittest.main()
