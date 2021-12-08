from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from cishouseholds.derive import assign_from_lookup
from cishouseholds.derive import assign_named_buckets
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.load import extract_from_table


# 1167 - test done stefen
def chose_scenario_of_dweight_for_antibody_different_household(
    df: DataFrame,
    tranche_eligible_indicator: bool,  # TODO: what format is it?
) -> Union[str, None]:
    """
    Decide what scenario to use for calculation of the design weights
    for antibodies for different households
    Parameters
    ----------
    df
    eligibility_pct
    tranche_eligible_indicator
    household_samples_dataframe
    n_eligible_hh_tranche_bystrata_column
    n_sampled_hh_tranche_bystrata_column
    """
    df = df.withColumn(
        "eligibility_pct",
        F.when(
            F.col("number_eligible_households_tranche_bystrata").isNull()
            & F.col("number_sampled_households_tranche_bystrata").isNull(),
            0,
        )
        .when(
            F.col("number_eligible_households_tranche_bystrata").isNotNull()
            & F.col("number_sampled_households_tranche_bystrata").isNotNull()
            & (F.col("number_sampled_households_tranche_bystrata") > 0),
            (
                100
                * (
                    F.col("number_eligible_households_tranche_bystrata")
                    - F.col("number_sampled_households_tranche_bystrata")
                )
                / F.col("number_sampled_households_tranche_bystrata")
            ),
        )
        .otherwise(None),  # TODO: check this
    )
    if not tranche_eligible_indicator:  # TODO: not in household_samples_dataframe?
        return "A"
    else:
        if df.select("eligibility_pct").collect()[0][0] == 0.0:
            return "B"
        else:
            return "C"


# 1168 - test done stefen
def raw_dweight_for_AB_scenario_for_antibody(
    df: DataFrame,
    hh_dweight_antibodies_column,
    raw_dweight_antibodies_column,
    sample_new_previous_column,
    scaled_dweight_swab_nonadjusted_column,
) -> DataFrame:
    """
    Parameters
    ----------
    df
    hh_dweight_antibodies_column
    raw_dweight_antibodies_column
    sample_new_previous_column
    scaled_dweight_swab_nonadjusted_column
    """
    df = df.withColumn(
        raw_dweight_antibodies_column,
        F.when(F.col(hh_dweight_antibodies_column).isNotNull(), F.col(hh_dweight_antibodies_column)),
    )
    df = df.withColumn(
        raw_dweight_antibodies_column + "_b",  # TODO: should this be AB
        F.when(
            (F.col(sample_new_previous_column) == "previous") & (F.col(hh_dweight_antibodies_column).isNotNull()),
            F.col(hh_dweight_antibodies_column),
        ).when(
            (F.col(sample_new_previous_column) == "new") & (F.col(hh_dweight_antibodies_column).isNull()),
            F.col(scaled_dweight_swab_nonadjusted_column),
        ),
    )
    df = df.withColumn(
        raw_dweight_antibodies_column,
        F.when(hh_dweight_antibodies_column).isNull(),
        F.col(scaled_dweight_swab_nonadjusted_column),
    )
    return df


# 1169 - test done stefen
def function_name_1169(
    df: DataFrame,
    sample_new_previous: str,
    tranche_eligible_hh: str,
    tranche_n_indicator: str,
    raw_dweight_antibodies_c: str,
    scaled_dweight_swab_nonadjusted: str,
    tranche_factor: str,
    hh_dweight_antibodies_c: str,
    dweights_swab: str,
) -> DataFrame:
    """
    1 step: for cases with sample_new_previous = "previous" AND tranche_eligible_households=yes(1)
    AND  tranche_number_indicator = max value, calculate raw design weight antibodies by using
    tranche_factor
    2 step: for cases with sample_new_previous = "previous";  tranche_number_indicator != max value;
    tranche_ eligible_households != yes(1), calculate the  raw design weight antibodies by using
    previous dweight antibodies
    3 step: for cases with sample_new_previous = new, calculate  raw design weights antibodies
    by using  design weights for swab

    Parameters
    ----------
    df
    sample_new_previous
    tranche_eligible_hh
    tranche_n_indicator
    raw_dweight_antibodies_c
    scaled_dweight_swab_nonadjusted
    tranche_factor
    hh_dweight_antibodies_c
    dweights_swab
    """
    max_value = df.agg({"tranche": "max"}).first()[0]

    df = df.withColumn(
        "raw_design_weight_antibodies",
        F.when(
            (F.col(sample_new_previous) == "previous")
            & (F.col(tranche_eligible_hh) == "Yes")
            & (F.col(tranche_n_indicator) == max_value),
            F.col(scaled_dweight_swab_nonadjusted) * F.col(tranche_factor),
        )
        .when(
            (F.col(sample_new_previous) == "previous")
            & (F.col(tranche_n_indicator) != max_value)
            & (F.col(tranche_eligible_hh) != "Yes"),
            F.col(hh_dweight_antibodies_c),
        )
        .when(
            (F.col(sample_new_previous) == "new")
            & (F.col(raw_dweight_antibodies_c) == F.col(scaled_dweight_swab_nonadjusted)),
            F.col(dweights_swab),
        ),
    )
    return df


# 1170 test done stefen
def function_name_1170(df: DataFrame, dweights_column: str, carryforward_dweights_antibodies_column: str) -> DataFrame:
    """
    Bring the design weights calculated up until this point  into same variable: carryforward_dweights_antibodies.
    Scaled up to population level  carryforward design weight antibodies.
    Parameters
    ----------
    df
    dweights_column
    carryforward_dweights_antibodies_column
    """
    # part 1: bring the design weights calculated up until this point  into same variable:
    #  carryforward_dweights_antibodies
    df = df.withColumn(carryforward_dweights_antibodies_column, F.col(dweights_column))  # TODO

    # part 2: scaled up to population level  carryforward design weight antibodies

    return df


# 1171 stefen
def function_name_1171(
    df: DataFrame,
    dweights_antibodies_column: str,
    total_population_column: str,
    cis_area_code_20_column: str,
) -> DataFrame:
    """
    check the dweights_antibodies_column are adding up to total_population_column.
    check the design weights antibodies are all are positive.
    check there are no missing design weights antibodies.
    check if they are the same across cis_area_code_20 by sample groups (by sample_source).

    Parameters
    ----------
    df
    """
    # part 1: check the dweights_antibodies_column are adding up to total_population_column
    assert sum(list(df.select(dweights_antibodies_column).toPandas()[dweights_antibodies_column])) == sum(
        list(df.select(total_population_column).toPandas()[total_population_column])
    )

    df = df.withColumn(
        "check_2-dweights_positive_notnull",
        F.when(F.col(total_population_column) > 1, 1).when(F.col(total_population_column).isNotNull(), 1),
    )

    # TODO part 4: check if they are the same across cis_area_code_20 by sample groups (by sample_source)

    return df


# 1178
def function_name_1178(
    hh_samples_df: DataFrame,
    df_extract_by_country: DataFrame,
    table_name: str,
    required_extracts_column_list: List[str],
    mandatory_extracts_column_list: List[str],
    population_join_column: str,
) -> DataFrame:
    """
    Parameters
    ----------
    hh_samples_df
    df_extract_by_country
    table_name
    required_extracts_column_list
    mandatory_extracts_column_list
    population_join_column: Only swab/antibody
    """
    # STEP 1 - create: extract_dataset as per requirements
    # (i.e extract dataset for calibration from survey_data dataset (individual level))
    df = extract_from_table(table_name).select(*required_extracts_column_list)

    # STEP 2 -  check there are no missing values
    for var in mandatory_extracts_column_list:
        df = df.withColumn(
            "check_if_missing", F.when(F.col(var).isNull(), 1)
        )  # TODO: check if multiple columns are needed

    # STEP 3 -  create household level of the extract to be used in calculation of non response factor
    df = (
        df.withColumn("response_indicator", F.when(F.col("participant_id").isNotNull(), 1).otherwise(None))
        .drop("participant_id")
        .dropDuplicates("ons_household_id")
    )

    # STEP 4 - merge hh_samples_df (36 processing step) and household level extract (69 processing step)
    # TODO: check if population_by_country comes by country separated or together
    hh_samples_df = hh_samples_df.join(
        df=df.select(*required_extracts_column_list, "check_if_missing"), on="ons_household_id", how="left"
    )

    hh_samples_df = hh_samples_df.join(
        df=df_extract_by_country, on="population_country_{population_join_column}", how="left"
    )

    return df


# Jamie refactoring and test done
def function1179_1(df: DataFrame):
    """
    Parameters
    ----------
    df
    """
    # A.1 group households  considering  values to index of multiple deprivation
    map_index_multiple_deprivation_group_country = {
        "england": {0: 1, 6570: 2, 13139: 3, 19708: 4, 26277: 5},
        "wales": {0: 1, 383: 2, 765: 3, 1147: 4, 1529: 5},
        "scotland": {0: 1, 1396: 2, 2791: 3, 4186: 4, 5581: 5},
        "northen_ireland": {0: 1, 179: 2, 357: 3, 535: 4, 713: 5},
    }

    # test
    df = union_multiple_tables(
        [
            assign_named_buckets(
                df=df.where(F.col("country_name") == country),
                reference_column="index_multiple_deprivation",
                column_name_to_assign="index_multiple_deprivation_group",
                map=map_index_multiple_deprivation_group_country[country],
            )
            for country in map_index_multiple_deprivation_group_country.keys()
        ]
    )
    # TODO run without filtering 4 times, 1 per country, using the dictionary for the country
    # then use withcolumn() and f.when( country is england use england column)
    return df


# 1179 - Jamie refactoring - test?
def assign_total_sampled_households_cis_imd_addressbase(df: DataFrame, country_column: str) -> DataFrame:
    """
    Parameters
    ----------
    df
    country_column: For northen_ireland, use country, otherwise use cis_area_code_20
    """
    window_list = ["sample_addressbase_indicator", country_column, "index_multiple_deprivation_group"]

    w = Window.partitionBy(*window_list)

    df = df.withColumn("total_sampled_households_cis_imd_addressbase", F.count(F.col("ons_household_id")).over(w))

    df = df.withColumn(
        "total_responded_households_cis_imd_addressbase",
        F.when(F.col("interim_participant_id") == 1, F.count(F.col("ons_household_id"))).over(w),
    )

    # B.1 calculate raw non_response_factor by dividing total respondent household
    # to total sampled households (calculated above A.2, A.3)
    df = df.withColumn(
        "raw_non_response_factor",
        (
            F.col("total_sampled_households_cis_imd_addressbase")
            / F.col("total_responded_households_cis_imd_addressbase")
        ),
    )
    return df


# Jamie test done
def function1179_2(df: DataFrame, country_column: str) -> DataFrame:
    """
    Parameters
    ----------
    df
    country_column: For northen_ireland, use country, otherwise use cis_area_code_20
    """

    window_list_nni = ["sample_addressbase_indicator", "cis_area_code", "index_multiple_deprivation_group"]
    window_list_ni = ["sample_addressbase_indicator", country_column, "index_multiple_deprivation_group"]

    w1_nni = Window.partitionBy(*window_list_nni)
    w1_ni = Window.partitionBy(*window_list_ni)

    import pdb

    pdb.set_trace()

    df = df.withColumn(
        "total_sampled_households_cis_imd_addressbase",
        F.when(
            F.col("country_name") != "northern_ireland", F.count(F.col("ons_household_id")).over(w1_nni).cast("int")
        ).otherwise(F.count(F.col("ons_household_id")).over(w1_ni).cast("int")),
    )

    w2_nni = Window.partitionBy(*window_list_nni, "interim_participant_id")
    w2_ni = Window.partitionBy(*window_list_ni, "interim_participant_id")
    df = df.withColumn(
        "total_responded_households_cis_imd_addressbase",
        F.when(
            F.col("country_name") != "northern_ireland", F.count(F.col("ons_household_id")).over(w2_nni).cast("int")
        ).otherwise(F.count(F.col("ons_household_id")).over(w2_ni).cast("int")),
    )

    df = df.withColumn(
        "total_responded_households_cis_imd_addressbase",
        F.when(F.col("interim_participant_id") != 1, 0).otherwise(
            F.col("total_responded_households_cis_imd_addressbase")
        ),
    )

    df = df.withColumn(
        "auxiliary",
        F.when(
            F.col("country_name") != "northern_ireland",
            F.max(F.col("total_responded_households_cis_imd_addressbase")).over(w1_nni),
        ).otherwise(F.max(F.col("total_responded_households_cis_imd_addressbase")).over(w1_ni)),
    )
    df = df.withColumn("total_responded_households_cis_imd_addressbase", F.col("auxiliary")).drop("auxiliary")

    return df


# jamie refactoring WIP
def function1179_3(df: DataFrame, country_list: List[str], test_type: str) -> DataFrame:
    """
    Parameters
    ----------
    """
    # B.2 calculate scaled non_response_factor by dividing raw_non_response_factor
    # to the mean of raw_non_response_factor
    w_country = Window.partitionBy(country_list)

    raw_non_response_factor_mean = df.select(F.mean(F.col("raw_non_response_factor"))).collect()[0][0]

    df = df.withColumn(
        "mean_raw_non_response_factor",
        (F.col("raw_non_response_factor") / raw_non_response_factor_mean).over(w_country),
    )

    df = df.withColumn("scaled_non_response_factor", F.col("raw_non_response_factor") / raw_non_response_factor_mean)

    # B.3 calculate bounded non_response_factor by adjusting the values of the scaled
    # non_response factor to be contained in a certain, pre-defined range
    df = df.withColumn(
        "bounded_non_response_factor",
        F.when(F.col("scaled_non_response_factor") < 0.5, 0.6).when(F.col("scaled_non_response_factor") > 2.0, 1.8),
    )  # TODO: is this in a country window ?

    # C1. adjust design weights by the non_response_factor by multiplying the desing
    # weights with the non_response factor
    df = df.withColumn(
        "household_level_designweight_adjusted_swab",
        F.when(
            # TODO: when  data extracted is a swab or longcovid type of dataset
            True & F.col("response_indicator") == 1,
            F.col("household_level_designweight_swab") * F.col("bounded_non_response_factor"),
        ).over(w_country),
    )

    df = df.withColumn(
        "household_level_designweight_adjusted_antibodies",
        F.when(
            # TODO: when  data extracted is a antibodies type of dataset
            True & F.col("response_indicator") == 1,
            F.col("household_level_designweight_antibodies") * F.col("bounded_non_response_factor"),
        ).over(w_country),
    )

    # D.1 calculate the scaling factor and then apply the scaling factor the
    # design weights adjusted by the non response factor.
    # If there is a swab or long covid type dataset, then use the population
    # totlas for swab and the naming will have "swab" incorporated.
    # Same for antibodies type dataset

    # sum_adjusted_design_weight_swab or antibodies
    df = df.withColumn(
        "sum_adjusted_design_weight_" + test_type,
        F.when(F.col("response_indicator") == 1, F.col("household_level_designweight_adjusted_" + test_type)).over(
            w_country
        ),
    )

    # scaling_factor_adjusted_design_weight_swab or antibodies
    sum_adjusted_design_weight = df.select(F.sum(F.col("adjusted_design_weight_" + test_type))).collect()[0][0]

    df = df.withColumn(
        "scaling_factor_adjusted_design_weight_" + test_type,
        F.when(
            F.col("response_indicator") == 1, (F.col("population_country_" + test_type) / sum_adjusted_design_weight)
        ).over(w_country),
    )

    # scaled_design_weight_adjusted_swab or antibodies
    df = df.withColumn(
        "scaled_design_weight_adjusted_" + test_type,
        F.when(
            F.col("response_indicator") == 1,
            (
                F.col("scaling_factor_adjusted_design_weight_" + test_type)
                * F.col("household_level_designweight_adjusted_" + test_type)
            ),
        ).over(w_country),
    )
    return df


# 1179 test done
def precalibration_checkpoints(df: DataFrame, test_type: str, dweight_list: List[str]) -> DataFrame:
    """
    Parameters
    ----------
    df
    test_type
    population_totals
    dweight_list
    """
    # TODO: use validate_design_weights() stefen's function
    # check_1: The design weights are adding up to total population
    check_1 = (
        df.select(F.sum(F.col("scaled_design_weight_adjusted_" + test_type))).collect()[0][0]
        == df.select("number_of_households_population_by_cis").collect()[0][0]
    )

    # check_2 and check_3: The  design weights are all are positive AND check there are no missing design weights
    for dweight in dweight_list:
        df = df.withColumn(
            "not_positive_or_null",
            F.when((F.col(dweight) < 0) | (F.col(dweight).isNull()), 1).otherwise(F.col("not_positive_or_null")),
        )

    check_2_3 = 0 == len(
        df.distinct().where(F.col("not_positive_or_null") == 1).select("not_positive_or_null").collect()
    )

    # check_4: if they are the same across cis_area_code_20 by sample groups (by sample_source)
    # TODO: testdata - create a column done for sampling then filter out to extract the singular samplings.
    # These should have the same dweights when window function is applied.
    check_4 = True

    return check_1, check_2_3, check_4


# 1180 test done
def function_1180(df):
    """
    Parameters
    ----------
    df
    """

    # A.1 re-code the region_code values, by replacing the alphanumeric code with numbers from 1 to 12
    spark = SparkSession.builder.getOrCreate()
    region_code_lookup_df = spark.createDataFrame(
        data=[
            ("E12000001", 1),
            ("E12000002", 2),
            ("E12000003", 3),
            ("E12000004", 4),
            ("E12000005", 5),
            ("E12000006", 6),
            ("E12000007", 7),
            ("E12000008", 8),
            ("E12000009", 9),
            ("W99999999", 10),
            ("S99999999", 11),
            ("N99999999", 12),
        ],
        schema="interim_region string, interim_region_code integer",
    )
    df = assign_from_lookup(
        df=df,
        column_name_to_assign="interim_region_code",
        reference_columns=["interim_region"],
        lookup_df=region_code_lookup_df,
    )
    # import pdb; pdb.set_trace()
    # A.2 re-code sex variable replacing string with integers
    sex_code_lookup_df = spark.createDataFrame(
        data=[
            ("male", 1),
            ("female", 2),
        ],
        schema="sex string, interim_sex integer",
    )
    df = assign_from_lookup(
        df=df,
        column_name_to_assign="interim_sex",
        reference_columns=["sex"],
        lookup_df=sex_code_lookup_df,
    )

    # A.3 create age groups considering certain age boundaries,
    # as needed for calibration weighting of swab data
    map_age_at_visit_swab = {2: 1, 12: 2, 17: 3, 25: 4, 35: 5, 50: 6, 70: 7}
    df = assign_named_buckets(
        df=df,
        column_name_to_assign="age_group_swab",
        reference_column="age_at_visit",
        map=map_age_at_visit_swab,
    )

    # A.4 create age groups considering certain age boundaries,
    # needed for calibration weighting of antibodies data
    map_age_at_visit_antibodies = {16: 1, 25: 2, 35: 3, 50: 4, 70: 5}
    df = assign_named_buckets(
        df=df,
        column_name_to_assign="age_group_antibodies",
        reference_column="age_at_visit",
        map=map_age_at_visit_antibodies,
    )

    # A.5 Creating first partition(p1)/calibration variable
    return df


# 1180 - TEST DONE - check with team
def create_calibration_var(
    df: DataFrame,
) -> DataFrame:
    """
    Parameters
    ----------
    df
    dataset
    calibration_type:
        allowed values:
            p1_swab_longcovid_england
            p1_swab_longcovid_wales_scot_ni
            p1_for_antibodies_evernever_engl
            p1_for_antibodies_28daysto_engl
            p1_for_antibodies_wales_scot_ni
            p2_for_antibodies
            p3_for_antibodies_28daysto_engl
    dataset_type - allowed values:
        swab_evernever
        swab_14days
        long_covid_24days
        long_covid_42days
        long_covid__24days
        antibodies_evernever
        antibodies_28daysto
    """
    calibration_dic = {
        "p1_swab_longcovid_england": {
            "dataset": [
                "swab_evernever",
                "swab_14days",
                "long_covid_24days",
                "long_covid_42days",
            ],
            "country_name": ["england"],
            "condition": ((F.col("country_name") == "england")),
            "operation": (
                (F.col("interim_region_code") - 1) * 14 + (F.col("interim_sex") - 1) * 7 + F.col("age_group_swab")
            ),
        },
        "p1_swab_longcovid_wales_scot_ni": {
            "dataset": [
                "long_covid_24days",
                "long_covid_42days",
                "swab_evernever",
            ],
            "country_name": ["wales", "scotland", "northen_ireland"],
            "condition": (
                (F.col("country_name") == "wales")
                | (F.col("country_name") == "scotland")
                | (F.col("country_name") == "northern_ireland")  # TODO: double-check name
            ),
            "operation": ((F.col("interim_sex") - 1) * 7 + F.col("age_group_swab")),
        },
        "p1_for_antibodies_evernever_engl": {
            "dataset": ["antibodies_evernever"],
            "country_name": ["england"],
            "condition": (F.col("country_name") == "england"),
            "operation": (
                (F.col("interim_region_code") - 1) * 10 + (F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies")
            ),
        },
        "p1_for_antibodies_28daysto_engl": {
            "dataset": ["antibodies_28daysto"],
            "country_name": ["england"],
            "condition": F.col("country_name") == "england",
            "operation": (F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies"),
        },
        "p1_for_antibodies_wales_scot_ni": {
            "dataset": ["antibodies_evernever", "antibodies_28daysto"],
            "country_name": ["northern_ireland"],
            "condition": (
                (F.col("country_name") == "wales")
                | (F.col("country_name") == "scotland")
                | (F.col("country_name") == "northern_ireland")  # TODO: double-check name
            ),
            "operation": ((F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies")),
        },
        "p2_for_antibodies": {
            "dataset": [
                "antibodies_evernever",
                "antibodies_28daysto",
            ],
            "country_name": ["england", "wales"],
            "condition": (((F.col("country_name") == "wales")) | ((F.col("country_name") == "england"))),
            "operation": (F.col("ethnicity_white") + 1),
        },
        "p3_for_antibodies_28daysto_engl": {
            "dataset": ["antibodies_28daysto"],
            "country_name": ["england"],
            "condition": ((F.col("country_name") == "england") & (F.col("age_at_visit") >= 16)),
            "operation": (F.col("interim_region_code")),
        },
    }
    # TODO-QUESTION: are the dataframes organised by country so that column country isnt needed?

    # A.6 Create second partition (p2)/calibration variable

    dataset_column = [
        "swab_evernever",
        "swab_14days",
        "long_covid_24days",
        "long_covid_42days",
        "long_covid_24days",
        "antibodies_evernever",
        "antibodies_28daysto",
    ]
    for column in dataset_column:
        df = df.withColumn(column, F.lit(None))

    for calibration_type in calibration_dic.keys():
        df = df.withColumn(
            calibration_type,
            F.when(
                calibration_dic[calibration_type]["condition"],
                calibration_dic[calibration_type]["operation"],
            ),
        )
        for column_dataset in calibration_dic[calibration_type]["dataset"]:
            df = df.withColumn(
                column_dataset,
                F.when(calibration_dic[calibration_type]["condition"], F.lit(1)).otherwise(F.col(column_dataset)),
            )
    return df


# 1180 - TEST DONE
def generate_datasets_to_be_weighted_for_calibration(
    df: DataFrame,
    processing_step: int
    # dataset,
    # dataset_type:str
):
    """
    Parameters
    ----------
    df
    processing_step:
        1 for
            england_swab_evernever, england_swab_14days, england_long_covid_28days, england_long_covid_42days
        2 for
            wales_swab_evernever, wales_swab_14days, wales_long_covid_28days, wales_long_covid_42days,
            scotland_swab_evernever, scotland_swab_14days, scotland_long_covid_28days, scotland_long_covid_42days,
            northen_ireland_swab_evernever, northen_ireland_swab_14days, northen_ireland_long_covid_28days,
            northen_ireland_long_covid_42days
        3 for
            england_antibodies_evernever
        4 for
            england_antibodies_28daysto
        5 for
            wales_antibodies_evernever
            wales_antibodies_28daysto
        6 for
            scotland_antibodies_evernever
            scotland_antibodies_28daysto
            northen_ireland_antibodies_evernever
            northen_ireland_antibodies_28daysto
    """
    dataset_dict = {
        1: {
            "variable": ["england"],
            "keep_var": [
                "country_name",
                "participant_id",
                "scaled_design_weight_adjusted_swab",
                "p1_swab_longcovid_england",
            ],
            "create_dataset": [
                "england_swab_evernever",
                "england_swab_14days",
                "england_long_covid_24days",
                "england_long_covid_42days",
            ],
        },
        2: {
            "variable": ["wales", "scotland", "northen_ireland"],
            "keep_var": [
                "country_name",
                "participant_id",
                "scaled_design_weight_adjusted_swab",
                "p1_swab_longcovid_wales_scot_ni",
            ],
            "create_dataset": [
                "wales_swab_evernever",
                "wales_swab_14days",
                "wales_long_covid_24days",
                "wales_long_covid_42days",
                "scotland_swab_evernever",
                "scotland_swab_14days",
                "scotland_long_covid__24days",
                "scotland_long_covid_42days",
                "northen_ireland_swab_evernever",
                "northen_ireland_swab_14days",
                "northen_ireland_long_covid_24days",
                "northen_ireland_long_covid_42days",
            ],
        },
        3: {
            "variable": ["england"],
            "keep_var": [
                "country_name",
                "participant_id",
                "scaled_design_weight_adjusted_antibodies",
                "p1_for_antibodies_evernever_engl",
                "p2_for_antibodies",
            ],
            "create_dataset": ["england_antibodies_evernever"],
        },
        4: {
            "variable": ["england"],
            "keep_var": [
                "country_name",
                "participant_id",
                "scaled_design_weight_adjusted_antibodies",
                "p1_for_antibodies_28daysto_engl",
                "p2_for_antibodies",
                "p3_for_antibodies",
            ],
            "create_dataset": ["england_antibodies_28daysto"],
        },
        5: {
            "variable": ["wales"],
            "keep_var": [
                "country_name",
                "participant_id",
                "scaled_design_weight_adjusted_antibodies",
                "p1_for_antibodies_wales_scot_ni",
                "p2_for_antibodies",
            ],
            "create_dataset": ["wales_antibodies_evernever", "wales_antibodies_28daysto"],
        },
        6: {
            "variable": ["scotland", "northen_ireland"],
            "keep_var": [
                "country_name",
                "participant_id",
                "scaled_design_weight_adjusted_antibodies",
                "p1_for_antibodies_wales_scot_ni",
            ],
            "create_dataset": [
                "scotland_antibodies_evernever",
                "scotland_antibodies_28daysto",
                "northen_ireland_antibodies_evernever",
                "northen_ireland_antibodies_28daysto",
            ],
        },
    }

    df = df.where(F.col("country_name").isin(dataset_dict[processing_step]["variable"])).select(
        *dataset_dict[processing_step]["keep_var"]
    )

    # df.where(F.col('country_name').isin(dataset_dict[processing_step]['variable']))
    # TODO: create datasets dataset_dict[processing_step]['create_dataset']

    # TODO: no need to create multiple df
    return df
