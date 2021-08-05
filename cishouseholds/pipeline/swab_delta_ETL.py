from pyspark.AccumulatorParam import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from cishouseholds.derive import assign_column_convert_to_date
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import derive_ctpattern
from cishouseholds.derive import mean_across_columns
from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.validation_schema import swab_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import create_spark_session
from cishouseholds.validate import validate_and_filter


def swab_delta_ETL(delta_file_path: str):
    spark_session = create_spark_session()
    swab_spark_schema = convert_cerberus_schema_to_pyspark(swab_validation_schema)
    df = spark_session.read.csv(
        delta_file_path, header=True, timestampFormat="yyyy-MM-dd HH:mm:ss UTC", schema=swab_spark_schema
    )
    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )

    df = clean_swab_delta(df)
    df = validate_and_filter(spark_session, df, swab_validation_schema, error_accumulator)
    df = transform_swab_delta(spark_session, df)
    df = load_swab_delta(spark_session, df)


def clean_swab_delta(df: DataFrame) -> DataFrame:
    """Clean column names and drop unused data from swab delta."""
    df = rename_column_names(df, swab_variable_name_map)
    df = df.drop("test_kit")
    return df


def transform_swab_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    """
    Call functions to process input for swab deltas.

    Parameters
    ----------
    df
    spark_session

    Notes
    -----
    Functions implemented:
        D13: assign_column_convert_to_date
        D7: derive_ctpattern
        D9: mean_across_columns
        D10: assign_isin_list
    """
    df = assign_column_convert_to_date(df, "result_mk_date", "Date Tested")
    df = derive_ctpattern(df, ["CH1-Cq", "CH2-Cq", "CH3-Cq"], spark_session)
    df = mean_across_columns(df, "ct_mean", ["CH1-Cq", "CH2-Cq", "CH3-Cq"])
    df = assign_isin_list(df, "ctonetarget", "ctpattern", ["N only", "OR only", "S only"])

    return df


def load_swab_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    return df
