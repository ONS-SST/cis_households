from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def filter_all_not_null(df: DataFrame, reference_columns: List[str]) -> DataFrame:
    """
    Filter rows which have NULL values in all the specified columns.
    From households_aggregate_processes.xlsx, filter number 2.
    Parameters
    ----------
    df
    reference_columns
        Columns to check for missing values in, all
        must be missing for the record to be dropped.
    """
    return df.na.drop(how="all", subset=reference_columns)


def filter_duplicates_by_time_and_threshold(
    df: DataFrame,
    first_reference_column: str,
    second_reference_column: str,
    third_reference_column: str,
    fourth_reference_column: str,
    time_threshold: float = 1.5,
    float_threshold: float = 0.00001,
) -> DataFrame:
    """
    Drop duplicates based on two identitical column values if third and fourth column and not both within
    a threshold difference from the first duplicate record.
    From households_aggregate_processes.xlsx, filter number 4.
    Parameters
    ----------
    df
    first_reference_column
        First column with duplicate value
    second_reference_column
        Second column with duplicate value
    third_reference_column
        Column used for time based threshold difference, timestamp
    fourth_reference_column
        Column used for numeric based threshold difference, float
    """

    window = Window.partitionBy(first_reference_column, second_reference_column).orderBy(third_reference_column)

    df = df.withColumn("duplicate_id", F.row_number().over(window))

    df = df.withColumn(
        "within_time_threshold",
        (
            F.abs(
                F.first(third_reference_column).over(window).cast("long") - F.col(third_reference_column).cast("long")
            )
            / (60 * 60)
        )
        < time_threshold,
    )

    df = df.withColumn(
        "within_float_threshold",
        F.abs(F.first(fourth_reference_column).over(window) - F.col(fourth_reference_column)) < float_threshold,
    )

    df = df.filter((F.col("duplicate_id") == 1) | ~(F.col("within_time_threshold") & (F.col("within_float_threshold"))))

    return df.drop("duplicate_id", "within_time_threshold", "within_float_threshold")


def filter_by_cq_diff(
    df: DataFrame, comparing_column: str, ordering_column: str, tolerance: float = 0.00001
) -> DataFrame:
    """
    This function works out what columns have a float value difference less than 10-^5 or 0.00001
        (or any other tolerance value inputed) given all the other columns are the same and
        considers it to be the same dropping or deleting the repeated values and only keeping one entry.
    Parameters
    ----------
    df
    comparing_column
    ordering_column
    tolerance
    """
    column_list = df.columns
    column_list.remove(comparing_column)

    windowSpec = Window.partitionBy(column_list).orderBy(ordering_column)
    df = df.withColumn("first_value_in_duplicates", F.first(comparing_column).over(windowSpec))
    df = df.withColumn(
        "duplicates_first_record", F.abs(F.col("first_value_in_duplicates") - F.col(comparing_column)) < tolerance
    )

    difference_window = Window.partitionBy(column_list + ["duplicates_first_record"]).orderBy(ordering_column)
    df = df.withColumn("duplicate_number", F.row_number().over(difference_window))

    df = df.filter(~(F.col("duplicates_first_record") & (F.col("duplicate_number") != 1)))
    df = df.drop("first_value_in_duplicates", "duplicates_first_record", "duplicate_number")

    return df
