import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import DataFrame, SparkSession

from src.utils import Utils
from src.data_processing import DataProcessing

test_data = [(1, "Arthur", "Morgan", 182),
             (2, "Logen", "Ninefingers", 194),
             (3, "Anomander", "Rake", 198),
             (4, "Spike", "Spiegel", 187), ]
test_schema = StructType([
    StructField('id', LongType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('height', LongType()),
])
test_columns = ["id", "first_name", "last_name", "height"]


@pytest.fixture
def example_df(spark_session):
    return spark_session.createDataFrame(data=test_data, schema=test_schema)


@pytest.fixture
def data_processing():
    return DataProcessing()


def test_rename_columns(data_processing: DataProcessing, example_df: DataFrame):
    """Test column renaming

    :param data_processing: DataProcessing() instance
    :param example_df: example DataFrame for testing
    """
    example_columns_to_be_renamed_dict = {
        "first_name": "middle_name",
        "height": "weight"
    }
    expected_columns = ["id", "middle_name", "last_name", "weight"]

    renamed_df = data_processing.rename_columns(example_df, example_columns_to_be_renamed_dict)
    unchanged_df = data_processing.rename_columns(example_df, {})

    assert renamed_df.columns == expected_columns
    assert example_df.columns == unchanged_df.columns


def test_drop_columns(data_processing: DataProcessing, example_df: DataFrame):
    """Test dropping columns

    :param data_processing: DataProcessing() instance
    :param example_df: example DataFrame for testing
    """
    columns_to_drop = ['id', 'first_name']
    expected_columns = ['last_name', 'height']

    dropped_df = data_processing.drop_columns(example_df, columns_to_drop)
    unchanged_df = data_processing.drop_columns(example_df, [])

    assert dropped_df.columns == expected_columns
    assert unchanged_df.columns == example_df.columns


def test_schema_validation(spark_session: SparkSession, example_df: DataFrame):
    """Test schema validation

    :param spark_session: spark session
    :param example_df: example DataFrame for testing
    """
    incorrect_schema_v1 = StructType([
        StructField('height', LongType()),
        StructField('first_name', StringType()),
        StructField('last_name', StringType()),
        StructField('id', LongType()),
    ])

    incorrect_schema_v2 = StructType([
        StructField('id', StringType()),
        StructField('first_name', LongType()),
        StructField('last_name', LongType()),
        StructField('id', StringType()),
    ])

    incorrect_schema_v3 = StructType([
        StructField('id', StringType())
    ])

    assert Utils.validate_df_schema(example_df, test_schema)
    assert not Utils.validate_df_schema(example_df, incorrect_schema_v1)
    assert not Utils.validate_df_schema(example_df, incorrect_schema_v2)
    assert not Utils.validate_df_schema(example_df, incorrect_schema_v3)
