import logging
import sys
from datetime import datetime
from typing import Callable
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window



def impute_visit_datetime(df: DataFrame, visit_datetime_column: str, sampled_datetime_column: str) -> DataFrame:
    df = df.withColumn(
        visit_datetime_column, F.when(F.col(visit_datetime_column).isNull(), F.col(sampled_datetime_column))
    )
    return df

def fill_forward_work_columns(
    df: DataFrame,
    fill_forward_columns: List[str],
    participant_id_column: str,
    visit_date_column: str,
    main_job_changed_column: str,
) -> DataFrame:
    """ """
    window = Window.partitionBy(participant_id_column).orderBy(visit_date_column)
    for col in fill_forward_columns:
        df = df.withColumn(
            col,
            F.when(
                F.col(main_job_changed_column) != "Yes", F.first(F.col(col), ignorenulls=True).over(window)
            ).otherwise(F.col(col)),
        )
    return df


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
        The column that will be created with the imputed values
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
    Wrapper function for calling imputations, flagging imputed records and recording imputation methods.

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
    Get imputation value from given column by most commonly occuring value.
    Results in None when multiple modes are found.

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
    if order_type == "asc":
        ordering_expression = F.col(order_by_column).asc()
    else:
        ordering_expression = F.col(order_by_column).desc()

    window = Window.partitionBy(column_identity).orderBy(ordering_expression)
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(reference_column).isNull(), F.last(F.col(reference_column), ignorenulls=True).over(window)),
    )
    return df


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


def _create_log(start_time: datetime, log_path: str):
    """Create logger for logging KNN imputation details"""
    log = log_path + "/KNN_imputation_" + start_time.strftime("%d-%m-%Y %H:%M:%S") + ".log"
    logging.basicConfig(
        filename=log, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%d/%m/%Y %H:%M:%S"
    )
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.info("\n KNN imputation started")


def _validate_donor_group_variables(
    df, reference_column, donor_group_columns, donor_group_variable_weights, donor_group_variable_conditions
):
    """
    Validate that donor group column values are within given bounds and data type conditions.
    Also reports summary statistics for group variables.
    """
    # variable to impute not in required impute variables
    if reference_column in donor_group_columns:
        message = "Imputed variable should not be in given impute variables."
        logging.warning(message)
        raise ValueError(message)

    # impute variables and weights are the same length
    if len(donor_group_columns) != len(donor_group_variable_weights):
        message = "Impute weights needs to be the same length as given impute variables."
        logging.warning(message)
        raise ValueError(message)

    df_dtypes = dict(df.dtypes)
    for var in donor_group_variable_conditions.keys():
        if len(donor_group_variable_conditions[var]) != 3:
            message = f"Missing boundary conditions for {var}. Needs to be in format [Min, Max, Dtype]"
            logging.warning(message)
            raise ValueError(message)

        var_min, var_max, var_dtype = donor_group_variable_conditions[var]
        if var_dtype is not None:
            if var_dtype != df_dtypes[var]:
                logging.warning(f"{var} dtype is {df_dtypes[var]} and not the required {var_dtype}")

        if var_min is not None:
            var_min_count = df.filter(F.col(var) > var_min).count()
            if var_min_count > 0:
                logging.warning(f"{var_min_count} rows have {var} below {var_min}" % (var_min_count, var, var_min))

        if var_max is not None:
            var_max_count = df.filter(F.col(var) < var_max).count()
            if var_max_count > 0:
                logging.warning(f"{var_max_count} rows have {var} above {var_max}")

    logging.info("Summary statistics for donor group variables:")
    logging.info(df.select(donor_group_columns).summary().toPandas())


def weighted_distance(df, group_id, donor_group_columns, donor_group_column_weights):
    """
    Calculate weighted distance between donors and records to be imputed.
    Expects corresponding donor group columns to be prefixed with 'don_'.

    Parameters
    ----------
    df
    group_id
        ID for donor group
    donor_group_columns
        Columns used to assign donor group
    donor_group_variable_weights
        Weight to apply to distance of each group column
    """
    df = df.withColumn(
        "distance",
        sum(
            [
                (~F.col(var).eqNullSafe(F.col("don_" + var))).cast(DoubleType()) * donor_group_column_weights[i]
                for i, var in enumerate(donor_group_columns)
            ]
        ),
    )
    distance_window = Window.partitionBy(group_id).orderBy("distance")
    df = df.withColumn("min_distance", F.first("distance").over(distance_window))
    df = df.filter(F.col("distance") <= F.col("min_distance")).drop("min_distance")
    return df


def impute_by_k_nearest_neighbours(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    donor_group_columns: list,
    log_file_path: str,
    minimum_donors: int = 1,
    donor_group_column_weights: list = None,
    donor_group_column_conditions: dict = None,
    maximum_distance: int = 4999,
):
    """
    Minimal PySpark implementation of RBEIS, for K-nearest neighbours imputation.
    Uses a weighted distance to select unique groups of donors. Then applies probability
    proportional to size (PPS) sampling from final pool of donors.

    Parameters
    ----------
    df
    column_name_to_assign
        column to store imputed values
    reference_column
        column that missing values should be imputed for
    donor_group_columns
        variables used to form unique donor groups to impute from
    donor_group_column_weights
        list of weights per ``donor_group_variables``
    donor_group_column_conditions
        list of boundary and data type conditions per ``donor_group_variables``
        in the form "variable": [minimum, maximum, "dtype"]
    log_path
        location the log file is written to
    minimum_donors
        minimum number of donors required in each imputation pool, must be >= 0
    maximum_distance
        maximum sum weighted distance for a valid donor. Set to None for no maximum.

    Note
    ----
    Uses a broadcast join to apply selected values for imputation, so assumes that the number of values to be imputed
    is relatively low.
    """

    if reference_column not in df.columns:
        message = f"Variable to impute ({reference_column}) is not in in columns."
        raise ValueError(message)

    if not all(column in df.columns for column in donor_group_columns):
        message = f"Imputation columns ({donor_group_columns}) are not all found in input dataset."
        raise ValueError(message)

    to_impute_condition = F.col(reference_column).isNull()
    donor_df = df.filter(~to_impute_condition)
    donor_df = donor_df.withColumn("unique_donor_group", F.concat_ws("-", *donor_group_columns))
    # Leave this on the original df, for joining imputed values on
    df = df.withColumn("unique_imputation_group", F.when(to_impute_condition, F.concat_ws("-", *donor_group_columns)))
    imputing_df = df.filter(to_impute_condition)

    input_df_length = df.count()
    impute_count = imputing_df.count()
    donor_count = donor_df.count()

    assert impute_count + donor_count == input_df_length, "Donor and imputing records don't sum to the whole df length"

    if impute_count == 0:
        return df.withColumn(column_name_to_assign, F.lit(None).cast(df.schema[reference_column].dataType))
    _create_log(start_time=datetime.now(), log_path=log_file_path)
    logging.info(f"Function parameters:\n{locals()}")

    logging.info(f"Input dataframe length: {input_df_length}")
    logging.info(f"Records to impute: {impute_count}")
    logging.info(f"Donor records: {donor_count}")

    if donor_count < impute_count:
        message = "Overall number of donor records is less than the number of records to impute."
        logging.warning(message)
        raise ValueError(message)

    if donor_group_column_weights is None:
        donor_group_column_weights = [1] * len(donor_group_columns)
        logging.warning(f"No imputation weights specified, using default: {donor_group_column_weights}")

    if donor_group_column_conditions is None:
        donor_group_column_conditions = {var: [None, None, None] for var in donor_group_columns}
        logging.warning(f"No bounds for impute variables specified, using default: {donor_group_column_conditions}")

    _validate_donor_group_variables(
        df, reference_column, donor_group_columns, donor_group_column_weights, donor_group_column_conditions
    )

    imputing_df_unique = imputing_df.dropDuplicates(donor_group_columns).select(
        donor_group_columns + ["unique_imputation_group"]
    )
    donor_df_unique = donor_df.dropDuplicates(donor_group_columns).select(donor_group_columns + ["unique_donor_group"])

    for var in donor_group_columns + [reference_column]:
        donor_df_unique = donor_df_unique.withColumnRenamed(var, "don_" + var)
        donor_df = donor_df.withColumnRenamed(var, "don_" + var)

    joined_uniques = imputing_df_unique.crossJoin(donor_df_unique)
    joined_uniques = joined_uniques.repartition("unique_imputation_group")

    candidates = weighted_distance(
        joined_uniques, "unique_imputation_group", donor_group_columns, donor_group_column_weights
    ).select("unique_imputation_group", "unique_donor_group")
    if maximum_distance is not None:
        candidates = candidates.where(F.col("distance") <= maximum_distance)

    frequencies = donor_df.groupby("unique_donor_group", "don_" + reference_column).agg(
        F.count("*").alias("donor_group_value_frequency")
    )
    frequencies = frequencies.join(candidates, on="unique_donor_group")
    frequencies = frequencies.join(
        imputing_df.groupby("unique_imputation_group").agg(F.count("*").alias("imputation_group_size")),
        on="unique_imputation_group",
    )

    frequencies.cache().count()

    no_donors = imputing_df_unique.join(frequencies, on="unique_imputation_group", how="left_anti")
    no_donors_count = no_donors.count()
    if no_donors_count != 0:
        message = f"{no_donors_count} donor pools with no donors"
        logging.error(message)
        logging.error(no_donors.toPandas())
        raise ValueError(message)

    unique_imputation_group_window = Window.partitionBy("unique_imputation_group")
    frequencies = frequencies.withColumn(
        "total_donor_pool_size", F.sum("donor_group_value_frequency").over(unique_imputation_group_window)
    )

    below_minimum_donor_count = frequencies.filter(F.col("total_donor_pool_size") < minimum_donors)
    below_minimum_donor_count_count = below_minimum_donor_count.count()
    if below_minimum_donor_count_count > 0:
        message = (
            f"{below_minimum_donor_count_count} donor pools found with less than the required {minimum_donors} "
            "minimum donor(s)"
        )
        logging.error(message)
        logging.error(frequencies.filter(F.col("donor_group_value_frequency") < minimum_donors).toPandas())
        raise ValueError(message)

    frequencies = frequencies.withColumn(
        "probability", F.col("donor_group_value_frequency") / F.col("total_donor_pool_size")
    )
    frequencies = frequencies.withColumn("expected_frequency", F.col("probability") * F.col("imputation_group_size"))
    frequencies = frequencies.withColumn("expected_frequency_integer_part", F.floor(F.col("expected_frequency")))
    frequencies = frequencies.withColumn(
        "expected_frequency_decimal_part", F.col("expected_frequency") - F.col("expected_frequency_integer_part")
    )

    frequencies = frequencies.withColumn(
        "required_decimal_donor_count", F.col("imputation_group_size") - F.col("expected_frequency_integer_part")
    )
    integer_part_donors = frequencies.select(
        "expected_frequency_integer_part", "unique_imputation_group", "don_" + reference_column
    )
    integer_part_donors = integer_part_donors.filter(F.col("expected_frequency_integer_part") >= 1)
    integer_part_donors = integer_part_donors.withColumn(
        "expected_frequency_integer_part",
        F.expr("explode(array_repeat(expected_frequency_integer_part, int(expected_frequency_integer_part)))"),
    )

    decimal_part_donors = frequencies.filter(F.col("expected_frequency_decimal_part") > 0).select(
        "probability", "unique_imputation_group", "don_" + reference_column, "required_decimal_donor_count"
    )

    random_uniques_window = Window.partitionBy("unique_imputation_group").orderBy(
        (F.rand() * F.col("probability")).desc()
    )
    decimal_part_donors = decimal_part_donors.withColumn("donor_row_id", F.row_number().over(random_uniques_window))
    decimal_part_donors = decimal_part_donors.filter(F.col("donor_row_id") <= F.col("required_decimal_donor_count"))

    to_impute_df = integer_part_donors.select("unique_imputation_group", "don_" + reference_column).unionByName(
        decimal_part_donors.select("unique_imputation_group", "don_" + reference_column)
    )
    rand_uniques_window = Window.partitionBy("unique_imputation_group").orderBy(F.rand())
    to_impute_df = to_impute_df.withColumn("donor_row_id", F.row_number().over(rand_uniques_window))

    to_impute_df = to_impute_df.withColumnRenamed("don_" + reference_column, column_name_to_assign)

    unique_imputation_group_window = Window.partitionBy("unique_imputation_group").orderBy(F.lit(None))
    df = df.withColumn("donor_row_id", F.row_number().over(unique_imputation_group_window))
    df = df.join(F.broadcast(to_impute_df), on=["unique_imputation_group", "donor_row_id"], how="left").drop(
        "unique_imputation_group", "donor_row_id"
    )

    output_df_length = df.cache().count()
    logging.info(
        f"Summary statistics for imputed values ({column_name_to_assign}) and donor values ({reference_column}):"
    )
    logging.info(df.select(column_name_to_assign, reference_column).summary().toPandas())
    if output_df_length != input_df_length:
        raise ValueError(
            f"{output_df_length} records are found in the output, which is not equal to {input_df_length} in the input."
        )

    missing_count = df.filter(F.col(reference_column).isNull() & F.col(column_name_to_assign).isNull()).count()
    if missing_count != 0:
        raise ValueError(f"{missing_count} records still have missing '{reference_column}' after imputation.")

    logging.info("KNN imputation completed\n")
    return df


def impute_latest_date_flag(
    df: DataFrame,
    participant_id_column: str,
    visit_date_column: str,
    visit_id_column: str,
    contact_any_covid_column: str,
    contact_any_covid_date_column: str,
) -> DataFrame:
    """
    Parameters
    ----------
    df
    participant_id_column
    visit_date_column
    visit_id_column
    contact_any_covid_column
    contact_any_covid_date_column
    """
    window = Window.partitionBy(participant_id_column).orderBy(
        F.desc(contact_any_covid_date_column),
        F.desc(visit_date_column),
        F.desc(visit_id_column),
    )

    df = df.withColumn(
        "imputation_flag",
        F.when(
            (
                (F.lag(contact_any_covid_column, 1).over(window) == 1)
                & (F.col(contact_any_covid_column) == 1)
                & (F.col(contact_any_covid_date_column).isNull())
            )
            | (
                (F.col(contact_any_covid_date_column) < F.lag(contact_any_covid_date_column, 1).over(window))
                & (F.col(visit_date_column) >= F.lag(contact_any_covid_date_column, 1).over(window))
            ),
            1,
        ).otherwise(0),
    )
    df = df.withColumn(
        contact_any_covid_date_column,
        F.when(F.col("imputation_flag") == 1, F.first(contact_any_covid_date_column).over(window)).otherwise(
            F.col(contact_any_covid_date_column)
        ),
    )

    return df.drop("imputation_flag")
