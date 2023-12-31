from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

from src.utils import Utils
from src.constants import OUTPUT_PATH, SPARK_APP_NAME
from src.schemas import SchemaID, schema_map
from src.exceptions import IncorrectSchemaException


class DataProcessing:
    def __init__(self):
        self.spark = (SparkSession.builder.master("local[1]")
                      .appName(SPARK_APP_NAME)
                      .getOrCreate())
        self.logger = Utils.initialise_logger()
        self.args = Utils.parse_args()
        self.logger.info('=======================================================================================')

    def load_data(self, src_path: str, schema_id: SchemaID) -> DataFrame:
        """Read csv file

        :param src_path: Source path to a file that will be loaded
        :param schema_id: ID of the schema for validating the loaded DataFrame schema
        :return: DataFrame
        """
        if not Utils.do_path_exist(src_path):
            self.logger.error(f"File in: {src_path} doesn't exist")
            raise FileNotFoundError

        try:
            schema = schema_map[schema_id]
            df = self.spark.read.format('csv').options(delimiter=",", header=True).load(src_path)

            if not Utils.validate_df_schema(df, schema):
                raise IncorrectSchemaException(df.schema, schema, src_path)

            self.logger.info(f'Loaded: {src_path} with {df.count()} records')
            return df
        except AnalysisException as e:
            self.logger.error(f"Issue with reading csv from: {src_path} \n{e}")
            raise
        except IOError as e:
            self.logger.info(f"Issue with reading csv from: {src_path} \n{e}")
            raise

    def save_data(self, df: DataFrame, dst_path: str):
        """Save DataFrame to csv using pandas as native saving with pyspark uses Hadoop which would need to be installed
        seperately.

        :param df: DataFrame
        :param dst_path: Output csv path
        """
        try:
            df.toPandas().to_csv(dst_path, header=True)
            self.logger.info(f"DataFrame saved to: {dst_path}")
        except IOError as e:
            self.logger.info(f"Issue with saving DataFrame to: {dst_path} \n{e}")

    def run(self):
        """Main raw_data processing flow that filters bitcoin trading raw_data based on user input"""
        clients_raw_df = self.load_data(self.args.src_clients_path, SchemaID.CLIENT)
        details_raw_df = self.load_data(self.args.src_details_path, SchemaID.DETAILS)

        clients_filtered_df = self.filter_column(clients_raw_df, 'country', self.args.countries)
        clients_dropped_df = self.drop_columns(clients_filtered_df, ['first_name', 'last_name'])
        details_dropped_df = self.drop_columns(details_raw_df, ['cc_n'])
        merged_df = self.join_dataframes(clients_dropped_df, details_dropped_df, on="id", how='inner')

        columns_to_be_renamed = {
            "id": "client_identifier",
            "btc_a": "bitcoin_address",
            "cc_t": "credit_card_type"
        }
        output_df = self.rename_columns(merged_df, columns_to_be_renamed)
        self.save_data(output_df, OUTPUT_PATH)

        print(output_df.show(10))

    def join_dataframes(self, df1: DataFrame, df2: DataFrame, on: str, how: str = 'inner') -> DataFrame:
        """Join two DataFrames

        :param df1: DataFrame
        :param df2: DataFrame
        :param on: str - column name on which 2 DataFrames will be joined
        :param how: str - join method, must be one of (inner, cross, outer, full, fullouter, full_outer, left, leftouter,
            left_outer, right, rightouter, right_outer, semi, leftsemi, left_semi, anti, leftanti and left_ant)
        :return: DataFrame
        """
        self.logger.info(f"DataFrames joined on {on}, using {how} join")
        return df1.join(df2, on=on, how=how)

    def filter_column(self, df: DataFrame, column_name: str, values: list) -> DataFrame:
        """Filter DataFrame rows where a column value is in the values list

        :param df: DataFrame
        :param column_name: string - name of the column on which filtering is performed
        :param values: list - a list of column values based on which the DataFrame will be filtered
        :return: DataFrame
        """

        count_before = df.count()
        df = df.filter(col(column_name).isin(values))
        count_after = df.count()
        self.logger.info(
            f"Rows: {values} in column: {column_name} removed, resulting in row's change: {count_before} -> {count_after}")
        return df

    def drop_columns(self, df: DataFrame, column_names: list) -> DataFrame:
        """Drop DataFrame columns given in a list

        :param df: DataFrame
        :param column_names: list - a list of column names to be dropped
        :return: DataFrame
        """
        df = df.drop(*column_names)
        self.logger.info(f"Columns dropped: {', '.join(column_names)}")
        return df

    def rename_columns(self, df: DataFrame, column_dict: dict) -> DataFrame:
        """Rename DataFrame columns given in a dict

        :param df: DataFrame
        :param column_dict: dict - a map of columns,
                            example = {
                                "old_col_1": "new_col_1",
                                "old_col_2": "new_col_2" ..
                            }
        :return: DataFrame
        """
        message = "Renamed columns: "
        cols_renamed = []
        for i, (old_column_name, new_column_name) in enumerate(column_dict.items()):
            df = df.withColumnRenamed(old_column_name, new_column_name)
            cols_renamed.append(f"{old_column_name} -> {new_column_name}")

        message += ', '.join(cols_renamed)
        self.logger.info(message)
        return df
