from enum import Enum

from pyspark.sql.types import StructType, StructField, StringType


class SchemaID(Enum):
    """Enum for validating loaded dataframes on specific schemas"""
    CLIENT = 1
    DETAILS = 2


client_schema = StructType([
    StructField('id', StringType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('country', StringType()),
])

details_schema = StructType([
    StructField('id', StringType()),
    StructField('btc_a', StringType()),
    StructField('cc_t', StringType()),
    StructField('cc_n', StringType()),
])

schema_map = {
    SchemaID.CLIENT: client_schema,
    SchemaID.DETAILS: details_schema
}
