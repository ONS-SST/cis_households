from chispa import assert_df_equality
from pyspark.sql.window import Window

from cishouseholds.weights.design_weights import scale_swab_design_weight


def test_calculate_combined_design_weight_swabs(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, 1, 1),
            (2, 1, 2),
        ],
        schema="""
            combined_design_weight_swab integer,
            number_of_households_by_cis_area integer,
            window integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[(2, 1, 2, 2, 0.5, 1.0), (1, 1, 1, 1, 1.0, 1.0)],
        schema="""
           combined_design_weight_swab integer,
           number_of_households_by_cis_area integer,
           window integer,
           sum_combined_design_weight_swab long,
           scaling_factor_combined_design_weight_swab double,
           scaled_design_weight_swab_non_adjusted double
        """,
    )
    window = Window.partitionBy("window")
    output_df = scale_swab_design_weight(
        df=input_df,
        design_weight_column="combined_design_weight_swab",
        household_count_column="number_of_households_by_cis_area",
        cis_window=window,
    )

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
