import csv
from datetime import datetime


class BinanceTickData:
    def __init__(self):
        self.Trade_id = 0
        self.price = 1
        self.qt = 2
        self.quote_qt = 3
        self.unix_time = 4
        self.maker_buying = 5,
        self.exec = 6


class ExtractData:
    def extract_and_save(self, input_file, output_file, writerow=None,
                         data_src=BinanceTickData()):
        if writerow is None:
            writerow = ['Date', 'Time', 'Price', 'Volume']
        with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
            # Initialize CSV reader and writer
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # Write header to the output file
            writer.writerow(writerow)

            for row in reader:
                # Extract the required columns
                unix_time = int(row[data_src.unix_time])
                price = row[data_src.price]
                volume = row[data_src.qt]

                # Convert UNIX time to Date and Time
                dt = datetime.utcfromtimestamp(unix_time / 1000)
                date = dt.strftime('%Y-%m-%d')
                time = dt.strftime('%H:%M:%S')

                # Write the extracted data to the output file
                writer.writerow([date, time, price, volume])
