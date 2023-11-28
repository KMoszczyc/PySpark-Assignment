import logging
import argparse
from src.constants import LOGS_PATH, LOGGER_NAME, CLIENTS_DATA_RAW_PATH, DETAILS_DATA_RAW_PATH
from logging.handlers import RotatingFileHandler


class Utils:
    @staticmethod
    def parse_args():
        """Parse user input"""
        parser = argparse.ArgumentParser()
        parser.add_argument("--src-clients-path", help="Path to src clients personal raw_data csv",
                            type=str, default=CLIENTS_DATA_RAW_PATH)
        parser.add_argument("--src-details-path", help="Path to src financial client details csv",
                            type=str, default=DETAILS_DATA_RAW_PATH)
        parser.add_argument("--countries", help="A list of countries for filtering raw_data",
                            nargs='+', default=['Netherlands', 'United Kingdom'])
        args = parser.parse_args()

        return args

    @staticmethod
    def initialise_logger():
        """Initialise a rotating logger"""
        handler = logging.handlers.RotatingFileHandler(
            filename=LOGS_PATH,
            mode='a',
            maxBytes=1024 * 1024,
            backupCount=1)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%d-%m-%Y %H:%M:%S')
        handler.setFormatter(formatter)

        logger = logging.getLogger(LOGGER_NAME)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        return logger
