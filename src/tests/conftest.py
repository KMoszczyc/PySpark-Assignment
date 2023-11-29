import pytest
from pyspark.sql import SparkSession
from src.constants import SPARK_APP_NAME


@pytest.fixture(scope="session")
def spark_session():
    spark = (SparkSession.builder.master("local[1]")
             .appName(SPARK_APP_NAME)
             .getOrCreate())
    yield spark
    spark.stop()
