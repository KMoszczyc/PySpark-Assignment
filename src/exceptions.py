import logging
from src.constants import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class IncorrectSchemaException(Exception):
    def __init__(self, df_schema, expected_schema, src_path):
        self.df_schema = df_schema
        self.expected_schema = expected_schema
        message = f"Incorrect schema error, given schema: {self.df_schema}, expected schema: {self.expected_schema}, data path: {src_path}"
        logger.info(message)
        super().__init__(message)
