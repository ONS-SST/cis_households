from chispa import assert_df_equality

from cishouseholds.merge import many_to_one_antibody_flag


def test_many_to_one_antibody_flag(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            ("ABC123", None, 1, 2, None),
            ("ABC456", None, 1, 2, 1),
            ("ABC456", None, 1, 2, 1),
        ],
        schema="antibody_barcode_cleaned string, out_of_date_range_antibody integer, count_barcode_antibody integer, \
                count_barcode_voyager integer, \
                drop_mto1_antibody_flag integer",
    )

    input_df = expected_df.drop("drop_mto1_antibody_flag")

    output_df = many_to_one_antibody_flag(input_df, "drop_mto1_antibody_flag", "antibody_barcode_cleaned")

    assert_df_equality(output_df, expected_df, ignore_row_order=True)
