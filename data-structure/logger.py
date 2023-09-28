import logging


class logger:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)

    def file_not_found(input_file):
        logging.error(f"FileNotFoundError: {input_file} not found.")

    def success_save(self, output_file):
        logging.info(f"Data saved successfully to {output_file}")

    def value_error(self, value):
        logging.error(f"ValueError: {value}")

    def index_error(self, ie, row):
        logging.error(f"IndexError: {ie} on row {row}")

    def unexpected_error(self, e):
        logging.error(f"An unexpected error occurred: {e}")


logger = logger()