from pyspark.sql import DataFrame
import logging
from src.constants import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class CustomDataframe(DataFrame):
    def __new__(self):
        super(DataFrame, self).__init__()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def withColumnRenamed(self, existing: str, new: str):
        super().withColumnRenamed(existing, new)

        logger.info(f'Renamed column: {existing} -> {new}')
