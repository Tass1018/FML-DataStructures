import unittest


class Bars(unittest.TestCase):
    def test_short_data(self):
        self.assertEqual(True, False)  # add assertion here

    def test_null_data(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
