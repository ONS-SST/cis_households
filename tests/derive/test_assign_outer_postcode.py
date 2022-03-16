from chispa import assert_df_equality

from cishouseholds.derive import assign_outward_postcode


def test_assign_outer_postcode(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("E26 4LB", "E26"), ("e6 2pc", "E6"), ("ig12 0db", "IG12"), ("ig124 0pxb", None)],
        schema=["ref", "outer_postcode"],
    )
    output_df = assign_outward_postcode(expected_df.drop("outer_postcode"), "outer_postcode", "ref")
    assert_df_equality(expected_df, output_df)