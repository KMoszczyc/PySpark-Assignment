import pytest
from pyspark.sql import SparkSession

from src.constants import SPARK_APP_NAME
from src.data_processing import DataProcessing


@pytest.fixture(scope="session")
def spark_session(request) -> SparkSession:
    """ A fixture for passing the same spark session to all tests
    :return: SparkSession
    """
    spark = (SparkSession.builder.master("local[1]")
             .appName(SPARK_APP_NAME)
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    return spark


@pytest.fixture
def data_processing() -> DataProcessing:
    """ A fixture for passing the same DataProcessing() instance to all tests"""
    return DataProcessing()
