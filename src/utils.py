import logging
import argparse
import os
import re
import sys

from logging.handlers import RotatingFileHandler
from pyspark.sql import DataFrame

from src.constants import LOGS_PATH, LOGGER_NAME, CLIENTS_DATA_RAW_PATH, DETAILS_DATA_RAW_PATH


class Utils:
    """Class with utility functions such as arg parsing, logging or schema validation."""

    @staticmethod
    def parse_args() -> argparse.Namespace:
        """Parse user input
        :return: Namespace
        """

        parser = argparse.ArgumentParser()
        parser.add_argument("--src-clients-path", help="Path to src clients personal raw_data csv",
                            type=str, default=CLIENTS_DATA_RAW_PATH)
        parser.add_argument("--src-details-path", help="Path to src financial client details csv",
                            type=str, default=DETAILS_DATA_RAW_PATH)
        parser.add_argument("--countries", help="A list of countries for filtering raw_data",
                            nargs='+', default=['Netherlands', 'United Kingdom'])

        if re.search('pytest', sys.argv[0]):
            args = parser.parse_args([])
        else:
            args = parser.parse_args(sys.argv[1:])

        return args

    @staticmethod
    def initialise_logger() -> logging.Logger:
        """Initialise a rotating logger
        :return: Logger
        """
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

    @staticmethod
    def do_path_exist(path: str) -> bool:
        """Check if file under the path exists.

        :param path: str - path to the csv file
        :return: bool
        """
        return os.path.exists(path)

    @staticmethod
    def validate_df_schema(df: DataFrame, schema: DataFrame.schema) -> bool:
        """

        :param df: DataFrame used for validation
        :param schema: expected schema
        :return: bool
        """
        return df.schema == schema
