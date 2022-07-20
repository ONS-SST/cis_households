from chispa import assert_df_equality

from cishouseholds.weights.derive import assign_tranche_factor


def test_assign_tranche_factor(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "A", "J1", 1, "Yes", 2, 0, None),
            ("B", "B", "J2", 2, "Yes", 2, 2, 1.0),
            ("C", "C", "J2", 2, "Yes", 2, 2, 1.0),
            ("D", "D", "J1", 2, "Yes", 2, 1, 2.0),
            ("E", None, "J1", 1, "No", 0, 0, None),
        ],
        schema="""
            barcode string,
            barcode_ref string,
            groupby string,
            tranche_number_indicator integer,
            tranche_eligible_households string,
            number_eligible_households_tranche_by_strata_enrolment integer,
            number_sampled_households_tranche_by_strata_enrolment integer,
            tranche_factor double""",
    )
    output_df = assign_tranche_factor(
        df=expected_df.drop(
            "tranche_factor",
            "tranche_eligible_households",
            "number_eligible_households_tranche_by_strata_enrolment",
            "number_sampled_households_tranche_by_strata_enrolment",
        ),
        column_name_to_assign="tranche_factor",
        barcode_ref_column="barcode_ref",
        tranche_column="tranche_number_indicator",
        strata_columns=["groupby"],
        household_id_column="barcode",
    )
    assert_df_equality(
        output_df,
        expected_df.drop("barcode_ref"),
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
