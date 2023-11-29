import pytest
from src.data_processing import DataProcessing
from chispa import assert_df_equality, assert_column_equality

test_data = [(1, "Arthur", "Morgan", 182),
             (2, "Logen", "Ninefingers", 194),
             (3, "Anomander", "Rake", 198),
             (4, "Spike", "Spiegel", 187), ]
test_columns = ["id", "first_name", "last_name", "height"]
# data_processing = DataProcessing()

@pytest.fixture
def example_df(spark_session):
    return spark_session.createDataFrame(data=test_data, schema=test_columns)


@pytest.fixture
def data_processing():
    return DataProcessing()


def test_rename_columns(data_processing, example_df):
    example_columns_to_be_renamed_dict = {
        "first_name": "middle_name",
        "height": "weight"
    }
    new_test_columns = ["id", "middle_name", "last_name", "weight"]

    renamed_df = data_processing.rename_columns(example_df, example_columns_to_be_renamed_dict)
    assert renamed_df.columns == new_test_columns
