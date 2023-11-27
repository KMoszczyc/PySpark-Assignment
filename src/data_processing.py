import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

CLIENTS_DATA_RAW_PATH = 'data/raw/dataset_one.csv'
DETAILS_DATA_RAW_PATH = 'data/raw/dataset_two.csv'
LOGS_PATH = 'logs/data_processing.log'

class DataProcessing:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.initialise_logger()

    def initialise_logger(self):
        handler = logging.handlers.RotatingFileHandler(
            filename=LOGS_PATH,
            mode='a',
            maxBytes=1000,
            backupCount=3)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%d-%m-%Y %H:%M:%S')
        handler.setFormatter(formatter)

        self.logger = logging.getLogger('Data Processing Log')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler)

    def load_data(self, path):
        try:
            df = self.spark.read.format('csv').options(delimiter=",", header=True).load(path)
            self.logger.info(f'Loaded {path} with {df.count()} records')
            return df
        except FileNotFoundError as e:
            self.logger.error(f"File in {path} doesn't exist")
        except Exception as e:
            self.logger.error(e)

    def process(self):
        clients_raw_df = self.load_data(CLIENTS_DATA_RAW)
        details_raw_df = self.load_data(DETAILS_DATA_RAW)

        clients_filtered_df = clients_raw_df.filter(col('country').isin(['Netherlands', 'United Kingdom']))
        clients_dropped_df = clients_filtered_df.drop('first_name', 'last_name')
        details_dropped_df = details_raw_df.drop('cc_n')


        merged_df = clients_dropped_df.join(details_dropped_df, on="id")
        merged_renamed_df = merged_df.withColumnRenamed("id", "client_identifier") \
            .withColumnRenamed("btc_a", "bitcoin_address") \
            .withColumnRenamed("cc_t", "credit_card_type")
        merged_sorted_df = merged_renamed_df.sort('email')

        print(merged_sorted_df.show(10))

        self.logger.warning('hello')
