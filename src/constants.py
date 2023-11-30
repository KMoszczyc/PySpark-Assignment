import os
from pathlib import Path

ROOT_PATH = Path(__file__).parent.parent
CLIENTS_DATA_RAW_PATH = os.path.join(ROOT_PATH, 'raw_data/dataset_one.csv')
DETAILS_DATA_RAW_PATH = os.path.join(ROOT_PATH, 'raw_data/dataset_two.csv')
OUTPUT_PATH = os.path.join(ROOT_PATH, 'client_data/output.csv')
LOGS_PATH = os.path.join(ROOT_PATH, 'logs/data_processing.log')

LOGGER_NAME = 'Data Processing Log'
SPARK_APP_NAME = 'DataProcessingApp'
