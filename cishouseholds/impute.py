import sys
from typing import Callable
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def impute_by_distribution(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    group_by_columns: List[str],
    first_imputation_value: Union[str, bool, int, float],
    second_imputation_value: Union[str, bool, int, float],
    rng_seed: int = None,
) -> DataFrame:
    """
    Calculate a imputation value from a missing value using a probability
    threshold determined by the proportion of a sub group.

    N.B. Defined only for imputing a binary column.

    Parameters
    ----------
    df
    column_name_to_assign
        The colum that will be created with the impute values
    reference_column
        The column for which imputation values will be calculated
    group_columns
        Grouping columns used to determine the proportion of the reference values
    first_imputation_value
        Imputation value if random number less than proportion
    second_imputation_value
        Imputation value if random number greater than or equal to proportion
    rng_seed
        Random number generator seed for making function deterministic.

    Notes
    -----
    Function provides a column value for each record that needs to be imputed.
    Where the value does not need to be imputed the column value created will be null.
    """
    # .rowsBetween(-sys.maxsize, sys.maxsize) fixes null issues for counting proportions
    window = Window.partitionBy(*group_by_columns).orderBy(reference_column).rowsBetween(-sys.maxsize, sys.maxsize)

    df = df.withColumn(
        "numerator", F.sum(F.when(F.col(reference_column) == first_imputation_value, 1).otherwise(0)).over(window)
    )

    df = df.withColumn("denominator", F.sum(F.when(F.col(reference_column).isNotNull(), 1).otherwise(0)).over(window))

    df = df.withColumn("proportion", F.col("numerator") / F.col("denominator"))

    df = df.withColumn("random", F.rand(rng_seed))

    df = df.withColumn(
        "individual_impute_value",
        F.when(F.col("proportion") > F.col("random"), first_imputation_value)
        .when(F.col("proportion") <= F.col("random"), second_imputation_value)
        .otherwise(None),
    )

    # Make flag easier (non null values will be flagged)
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(reference_column).isNull(), F.col("individual_impute_value")).otherwise(None),
    )

    return df.drop("proportion", "random", "denominator", "numerator", "individual_impute_value")


def impute_and_flag(df: DataFrame, imputation_function: Callable, reference_column: str, **kwargs) -> DataFrame:
    """
    Wrapper function for calling imputations, flagging imputed records and recording methods.
    Parameters
    ----------
    df
    imputation_function
        The function that calculates the imputation for a given
        reference column
    reference_column
        The column that will be imputed
    **kwargs
        Key word arguments for imputation_function
    Notes
    -----
    imputation_function is expected to create new column with the values to
    be imputed, and NULL where imputation is not needed.
    """
    df = imputation_function(
        df, column_name_to_assign="temporary_imputation_values", reference_column=reference_column, **kwargs
    )

    status_column = reference_column + "_is_imputed"
    status_other = F.col(status_column) if status_column in df.columns else None

    df = df.withColumn(
        status_column,
        F.when(F.col("temporary_imputation_values").isNotNull(), 1)
        .when(F.col("temporary_imputation_values").isNull(), 0)
        .otherwise(status_other)
        .cast("integer"),
    )

    method_column = reference_column + "_imputation_method"
    method_other = F.col(method_column) if method_column in df.columns else None
    df = df.withColumn(
        reference_column + "_imputation_method",
        F.when(F.col(status_column) == 1, imputation_function.__name__).otherwise(method_other).cast("string"),
    )

    df = df.withColumn(reference_column, F.coalesce(reference_column, "temporary_imputation_values"))

    return df.drop("temporary_imputation_values")


def impute_by_mode(df: DataFrame, column_name_to_assign: str, reference_column: str, group_by_column: str) -> DataFrame:
    """
    Get imputation value from given column by most repeated UNIQUE value
    Parameters
    ----------
    df
    column_name_to_assign
        The column that will be created with the impute values
    reference_column
        The column to be imputed
    group_by_column
        Column name for the grouping
    Notes
    -----
    Function provides a column value for each record that needs to be imputed.
    Where the value does not need to be imputed the column value created will be null.
    """
    value_count_by_group = (
        df.groupBy(group_by_column, reference_column)
        .agg(F.count(reference_column).alias("_value_count"))
        .filter(F.col(reference_column).isNotNull())
    )

    group_window = Window.partitionBy(group_by_column)
    deduplicated_modes = (
        value_count_by_group.withColumn("_is_mode", F.col("_value_count") == F.max("_value_count").over(group_window))
        .filter(F.col("_is_mode"))
        .withColumn("_is_tied_mode", F.count(reference_column).over(group_window) > 1)
        .filter(~F.col("_is_tied_mode"))
        .withColumnRenamed(reference_column, "_imputed_value")
        .drop("_value_count", "_is_mode", "_is_tied_mode")
    )

    imputed_df = (
        df.join(deduplicated_modes, on=group_by_column, how="left")
        .withColumn(column_name_to_assign, F.when(F.col(reference_column).isNull(), F.col("_imputed_value")))
        .drop("_imputed_value")
    )

    return imputed_df


def impute_by_ordered_fill_forward(
    df: DataFrame,
    column_name_to_assign: str,
    column_identity: str,
    reference_column: str,
    order_by_column: str,
    order_type="asc",
) -> DataFrame:
    """
    Impute the last observation of a given field by given identity column.
    Parameters
    ----------
    df
    column_name_to_assign
        The colum that will be created with the impute values
    column_identity
        Identifies any records that the reference_column is missing forward
        This column is normally intended for user_id, participant_id, etc.
    reference_column
        The column for which imputation values will be calculated.
    orderby_column
        the "direction" of the observation will be defined by a ordering column
        within the dataframe. For example: date.
    order_type
        the "direction" of the observation can be ascending by default or
        descending. Chose ONLY 'asc' or 'desc'.
    Notes
    ----
    If the observation carried forward by a specific column like date, and
        the type of order (order_type) is descending, the direction will be
        reversed and the function would do a last observation carried backwards.
    """
    # the id column with a unique monotonically_increasing_id is auxiliary and
    # intends to add an arbitrary number to each row.
    # this will NOT affect the ordering of the rows by orderby_column parameter.
    df = df.withColumn("id", F.monotonically_increasing_id())

    if order_type == "asc":
        ordering_expression = F.col(order_by_column).asc()
    else:
        ordering_expression = F.col(order_by_column).desc()

    window = Window.partitionBy(column_identity).orderBy(ordering_expression)

    return (
        df.withColumn(
            column_name_to_assign,
            F.when(F.col(reference_column).isNull(), F.last(F.col(reference_column), ignorenulls=True).over(window)),
        )
        .orderBy(ordering_expression, "id")
        .drop("id")
    )


def merge_previous_imputed_values(
    df: DataFrame,
    imputed_value_lookup_df: DataFrame,
    id_column_name: str,
) -> DataFrame:
    """
    Retrieve and coalesce imputed values and associated flags from a lookup table.
    Includes the imputed value, imputation status and imputation method.

    Parameters
    ----------
    df
        dataframe to impute values onto
    imputed_value_lookup_df
        previously imputed values to carry forward
    id_column_name
        column that should be used to join previously imputed values on
    """
    imputed_value_lookup_df = imputed_value_lookup_df.toDF(
        *[f"_{column}" if column != id_column_name else column for column in imputed_value_lookup_df.columns]
    )  # _ prevents ambiguity in join, but is sliced out when referencing the original columns

    df = df.join(imputed_value_lookup_df, on=id_column_name, how="left")
    columns_for_editing = [
        (column.replace("_imputation_method", ""), column.replace("_imputation_method", "_is_imputed"), column)
        for column in imputed_value_lookup_df.columns
        if column.endswith("_imputation_method")
    ]

    for value_column, status_column, method_column in columns_for_editing:
        fill_condition = F.col(value_column[1:]).isNull() & F.col(value_column).isNotNull()
        df = df.withColumn(status_column[1:], F.when(fill_condition, F.lit(1)).otherwise(F.lit(0)))
        df = df.withColumn(
            method_column[1:], F.when(fill_condition, F.col(method_column)).otherwise(F.lit(None)).cast("string")
        )
        df = df.withColumn(
            value_column[1:], F.when(fill_condition, F.col(value_column)).otherwise(F.col(value_column[1:]))
        )

    return df.drop(*[name for name in imputed_value_lookup_df.columns if name != id_column_name])


def impute_known_or_suspected_covid_latest_date(
    df: DataFrame,
    column_name_to_assign: str,
    order_by_columns: List[str],
    contact_covid_column: str,
    contact_covid_date_column: str,
    visit_date_column: str,
):
    window = Window.orderBy(*order_by_columns)
    df = df.withColumn("LAG", F.lag(contact_covid_column, 1).over(window))
    df = df.withColumn("DATE_LAG", F.lag(contact_covid_date_column, 1).over(window))
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(contact_covid_column) == 1)
            | (F.col("LAG") == 1)
            | (F.col(contact_covid_date_column).isNull())
            | (
                (F.col(contact_covid_date_column) < F.col("DATE_LAG")) & (F.col(visit_date_column) >= F.col("DATE_LAG"))
            ),
            1,
        ).otherwise(None),
    )
    return df.drop("LAG", "DATE_LAG")
